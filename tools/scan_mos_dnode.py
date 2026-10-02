#!/usr/bin/env python3
"""Find a historical DSL dataset dnode in log-spacemap FREE ranges.

The pool remains offline. Matching 16 KiB MOS dnode blocks are copied to an
output directory for later inspection. Candidate validation uses the dataset
object slot, creation TXG, and optionally the DSL directory object.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from recovery_common import DNODE_SIZE, open_raidz

from zfs.dnode import BonusDataset, DNode
from zfs.lz4zfs import lz4zfs_decompress

LOG_SPACEMAP_RANGE = re.compile(r"^(\d+).*F range: ([0-9a-fA-F]+)-([0-9a-fA-F]+)")
MOS_DNODE_BLOCK_SIZE = 16 * 1024


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--disks", required=True, help="files block-provider table")
    parser.add_argument("--label", required=True, help="device containing a ZFS label")
    parser.add_argument("--ranges", required=True, help="log-spacemap FREE range file")
    parser.add_argument("--target-txg", required=True, type=int)
    parser.add_argument("--dataset-object", required=True, type=int)
    parser.add_argument("--creation-txg", required=True, type=int)
    parser.add_argument("--directory-object", type=int)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--scan-all-offsets",
        action="store_true",
        help="scan every ashift-aligned offset instead of only each range start",
    )
    parser.add_argument(
        "--keep-going",
        action="store_true",
        help="save all strong matches instead of stopping at the first",
    )
    return parser.parse_args()


def load_ranges(path: Path, target_txg: int) -> list[tuple[int, int]]:
    ranges = []
    with path.open(encoding="ascii", errors="replace") as stream:
        for line in stream:
            match = LOG_SPACEMAP_RANGE.search(line)
            if match and int(match.group(1)) == target_txg:
                ranges.append((int(match.group(2), 16), int(match.group(3), 16) + 1))
    return ranges


def inspect_candidate(
    data: bytes,
    *,
    slot: int,
    creation_txg: int,
    directory_object: int | None,
) -> tuple[DNode, BonusDataset] | None:
    if len(data) < MOS_DNODE_BLOCK_SIZE:
        return None
    raw = data[slot * DNODE_SIZE : (slot + 1) * DNODE_SIZE]
    if len(raw) != DNODE_SIZE or raw[0] not in (16, 196) or raw[4] != 16:
        return None
    try:
        dnode = DNode(raw)
    except Exception:
        return None
    if not isinstance(dnode.bonus, BonusDataset):
        return None
    bonus = dnode.bonus
    if bonus.ds_creation_txg != creation_txg:
        return None
    if directory_object is not None and bonus.ds_dir_obj != directory_object:
        return None
    return dnode, bonus


def main() -> int:
    args = parse_args()
    ranges_path = Path(args.ranges)
    output_dir = Path(args.output_dir)
    if not ranges_path.is_file():
        raise FileNotFoundError(ranges_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    ranges = load_ranges(ranges_path, args.target_txg)
    if not ranges:
        print(f"no FREE ranges found for TXG {args.target_txg}", file=sys.stderr)
        return 2

    vdev, label = open_raidz(args.disks, args.label)
    sector_size = 1 << label._ashift
    physical_sizes = range(sector_size, MOS_DNODE_BLOCK_SIZE + sector_size, sector_size)
    slot = args.dataset_object % 32
    total_bytes = sum(end - start for start, end in ranges)
    print(
        f"TXG {args.target_txg}: {len(ranges)} ranges, "
        f"{total_bytes / (1024**3):.3f} GiB; "
        f"object={args.dataset_object}, slot={slot}, ashift={label._ashift}",
        flush=True,
    )

    offsets = reads = read_errors = matches = 0
    for range_number, (start, end) in enumerate(ranges, 1):
        candidate_offsets = (
            range(start, end, sector_size) if args.scan_all_offsets else (start,)
        )
        for offset in candidate_offsets:
            offsets += 1
            available = end - offset
            for physical_size in physical_sizes:
                columns, _, _ = vdev._map_alloc(offset, physical_size, label._ashift)
                allocated_size = sum(column["rc_size"] for column in columns)
                if allocated_size > available:
                    continue
                try:
                    physical = bytes(
                        vdev._read_physical(offset, physical_size, False, "")
                    )
                except Exception:
                    read_errors += 1
                    continue
                reads += 1
                variants = []
                if physical_size == MOS_DNODE_BLOCK_SIZE:
                    variants.append(("raw", physical[:MOS_DNODE_BLOCK_SIZE]))
                decompressed = lz4zfs_decompress(physical, MOS_DNODE_BLOCK_SIZE)
                if decompressed is not None:
                    variants.append(("lz4", bytes(decompressed)))

                for encoding, data in variants:
                    result = inspect_candidate(
                        data,
                        slot=slot,
                        creation_txg=args.creation_txg,
                        directory_object=args.directory_object,
                    )
                    if result is None:
                        continue
                    dnode, bonus = result
                    matches += 1
                    output = output_dir / (
                        f"mos-dnode-object-{args.dataset_object}-"
                        f"txg-{args.target_txg}-offset-{offset:x}-"
                        f"psize-{physical_size:x}-{encoding}.bin"
                    )
                    with output.open("xb") as stream:
                        stream.write(data[:MOS_DNODE_BLOCK_SIZE])
                    print("\n*** STRONG CANDIDATE FOUND ***", flush=True)
                    print(
                        f"offset=0x{offset:x} psize=0x{physical_size:x} "
                        f"encoding={encoding} creation_txg={bonus.ds_creation_txg} "
                        f"directory_object={bonus.ds_dir_obj}",
                        flush=True,
                    )
                    print(dnode, flush=True)
                    print(f"saved: {output}", flush=True)
                    if not args.keep_going:
                        return 0

        if range_number % 1000 == 0:
            print(
                f"progress: ranges={range_number}/{len(ranges)} "
                f"offsets={offsets} reads={reads} read_errors={read_errors} "
                f"matches={matches}",
                flush=True,
            )

    print(
        f"no candidate found; offsets={offsets} reads={reads} "
        f"read_errors={read_errors} matches={matches}",
        flush=True,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
