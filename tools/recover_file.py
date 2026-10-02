#!/usr/bin/env python3
"""Recover selected files with checksum-guided RAIDZ1 reconstruction.

For every file block the tool tries all non-null DVAs, one failed data column,
and finally a different failed member for each RAIDZ stripe row. Only Fletcher4
matches against the original block pointer are accepted. The pool is never
imported, and no duplicate-file search is performed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
import os
import time
import zlib
from collections import Counter
from pathlib import Path, PurePosixPath

from recovery_common import (
    CandidateSpec,
    RecoveryError,
    open_dataset,
    parse_candidate_spec,
)

from zfs.blockptr import fletcher4
from zfs.blocktree import BlockTree
from zfs.lz4zfs import lz4zfs_decompress
from zfs.lzjb import lzjb_decompress
from zfs.zio import GenericDevice, RaidzDevice, roundup


def log(kind: str, message: str) -> None:
    stamp = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    print(f"{stamp} [{kind}] {message}", flush=True)


def normalize_path(value: str) -> str:
    value = value.strip().replace("\\", "/").lstrip("/")
    pure = PurePosixPath(value)
    if not value or any(part in ("", ".", "..") for part in pure.parts):
        raise ValueError(f"unsafe or empty target path: {value!r}")
    return str(pure)


def candidate_argument(value: str) -> CandidateSpec:
    try:
        return parse_candidate_spec(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def load_file_list(path: Path) -> dict[str, tuple[int, int]]:
    result = {}
    with path.open(newline="", encoding="utf-8", errors="surrogateescape") as stream:
        for row in csv.reader(stream, dialect="excel-tab"):
            if len(row) < 3 or row[2].endswith("/") or " -> " in row[2]:
                continue
            try:
                result[normalize_path(row[2])] = (int(row[0]), int(row[1]))
            except (ValueError, IndexError):
                continue
    return result


def load_targets(paths: list[str], manifest: str | None) -> list[str]:
    values = list(paths)
    if manifest:
        with open(manifest, encoding="utf-8", errors="surrogateescape") as stream:
            for line in stream:
                line = line.strip()
                if line and not line.startswith("#"):
                    values.append(line.split("\t", 1)[0].split(",", 1)[0])
    result = []
    seen = set()
    for value in values:
        normalized = normalize_path(value)
        if normalized not in seen:
            result.append(normalized)
            seen.add(normalized)
    if not result:
        raise RecoveryError("specify at least one --path or --manifest")
    return result


def verify_fletcher4(block_pointer, physical: bytes | bytearray) -> bool:
    if block_pointer._cksum != 7:
        return False
    payload = bytes(physical[: block_pointer.psize])
    return tuple(fletcher4(payload)) == tuple(block_pointer._checksum)


def decode_physical(block_pointer, physical: bytes | bytearray) -> bytes | None:
    data = bytes(physical[: block_pointer.psize])
    try:
        if block_pointer.compressed:
            if block_pointer.comp_alg == GenericDevice.COMP_TYPE_LZ4:
                data = bytes(lz4zfs_decompress(data, block_pointer.lsize))
            elif (
                GenericDevice.COMP_TYPE_GZIP_1
                <= block_pointer.comp_alg
                <= GenericDevice.COMP_TYPE_GZIP_9
            ):
                data = zlib.decompress(data)
            elif block_pointer.comp_alg in (
                GenericDevice.COMP_TYPE_ON,
                GenericDevice.COMP_TYPE_LZJB,
            ):
                data = bytes(lzjb_decompress(data, block_pointer.lsize))
            else:
                return None
    except Exception:
        return None
    if len(data) < block_pointer.lsize:
        data += b"\0" * (block_pointer.lsize - len(data))
    return data[: block_pointer.lsize]


class RaidzForensics:
    """Read an allocation once and test parity-derived variants."""

    def __init__(self, vdev: RaidzDevice, max_combinations: int):
        self.vdev = vdev
        self.max_combinations = max_combinations
        self.sector_size = 1 << vdev._ashift

    def read_columns(self, block_pointer, dva: int) -> tuple[list[bytearray], int]:
        pointer = block_pointer.get_dva(dva)
        read_size = roundup(block_pointer.psize, self.sector_size)
        columns, first_data_column, _ = self.vdev._map_alloc(
            pointer.offset, read_size, self.vdev._ashift
        )
        requests = [
            (
                self.vdev._devs[column["rc_devidx"]],
                column["rc_offset"] + 0x400000,
                column["rc_size"],
            )
            for column in columns
        ]
        raw = self.vdev._bp.readv(requests)
        result = []
        offset = 0
        for column in columns:
            size = column["rc_size"]
            result.append(bytearray(raw[offset : offset + size]))
            offset += size
        return result, first_data_column

    @staticmethod
    def assemble(
        columns: list[bytearray], first_data_column: int, physical_size: int
    ) -> bytes:
        return bytes(b"".join(columns[first_data_column:])[:physical_size])

    @staticmethod
    def xor_into(target: bytearray, source: bytes | bytearray) -> None:
        for index, value in enumerate(source[: len(target)]):
            target[index] ^= value

    def reconstruct_whole_column(
        self, block_pointer, columns: list[bytearray], first_data_column: int
    ) -> tuple[bytes | None, int | None]:
        parity = columns[0]
        for target_column in range(first_data_column, len(columns)):
            candidate = [bytearray(column) for column in columns]
            rebuilt = bytearray(parity[: len(candidate[target_column])])
            for index in range(first_data_column, len(candidate)):
                if index != target_column:
                    self.xor_into(rebuilt, candidate[index])
            candidate[target_column][:] = rebuilt
            physical = self.assemble(candidate, first_data_column, block_pointer.psize)
            if verify_fletcher4(block_pointer, physical):
                return physical, target_column
        return None, None

    def mismatching_rows(
        self, columns: list[bytearray], first_data_column: int
    ) -> list[int]:
        row_count = (max(map(len, columns)) + self.sector_size - 1) // self.sector_size
        mismatches = []
        for row in range(row_count):
            start = row * self.sector_size
            end = min(start + self.sector_size, len(columns[0]))
            if start >= end:
                continue
            syndrome = bytearray(columns[0][start:end])
            for index in range(first_data_column, len(columns)):
                self.xor_into(syndrome, columns[index][start:end])
            if any(syndrome):
                mismatches.append(row)
        return mismatches

    def reconstruct_by_row(
        self, block_pointer, columns: list[bytearray], first_data_column: int
    ) -> tuple[bytes | None, dict[str, object]]:
        bad_rows = self.mismatching_rows(columns, first_data_column)
        diagnostics: dict[str, object] = {"parity_mismatch_rows": bad_rows}
        if not bad_rows:
            diagnostics.update(
                reason="parity-consistent-but-checksum-wrong", combinations=0
            )
            return None, diagnostics

        choices = []
        for row in bad_rows:
            start = row * self.sector_size
            present = [
                index
                for index in range(first_data_column, len(columns))
                if start < len(columns[index])
            ]
            choices.append([None, *present])

        combination_count = 1
        for row_choices in choices:
            combination_count *= len(row_choices)
        diagnostics["candidate_combinations"] = combination_count
        if combination_count > self.max_combinations:
            diagnostics.update(
                reason="combination-limit",
                combination_limit=self.max_combinations,
            )
            return None, diagnostics

        for attempt, selection in enumerate(itertools.product(*choices), 1):
            candidate = [bytearray(column) for column in columns]
            for row, target_column in zip(bad_rows, selection, strict=True):
                if target_column is None:
                    continue
                start = row * self.sector_size
                length = min(self.sector_size, len(candidate[target_column]) - start)
                rebuilt = bytearray(candidate[0][start : start + length])
                for index in range(first_data_column, len(candidate)):
                    if index != target_column:
                        self.xor_into(rebuilt, candidate[index][start : start + length])
                candidate[target_column][start : start + length] = rebuilt
            physical = self.assemble(candidate, first_data_column, block_pointer.psize)
            if verify_fletcher4(block_pointer, physical):
                diagnostics.update(
                    combinations=attempt,
                    selection=list(selection),
                )
                return physical, diagnostics

        diagnostics.update(
            combinations=combination_count,
            reason="no-checksum-match",
        )
        return None, diagnostics

    def recover(self, block_pointer, dva: int) -> tuple[bytes | None, str, dict]:
        try:
            columns, first_data_column = self.read_columns(block_pointer, dva)
        except Exception as error:
            return (
                None,
                "raidz-read-error",
                {"error": f"{type(error).__name__}: {error}"},
            )

        physical, column = self.reconstruct_whole_column(
            block_pointer, columns, first_data_column
        )
        if physical is not None:
            decoded = decode_physical(block_pointer, physical)
            if decoded is not None:
                return decoded, "raidz-whole-column", {"raidz_column": column}

        physical, diagnostics = self.reconstruct_by_row(
            block_pointer, columns, first_data_column
        )
        if physical is not None:
            decoded = decode_physical(block_pointer, physical)
            if decoded is not None:
                return decoded, "raidz-per-row", diagnostics
        return None, "unresolved", diagnostics


class DatasetSource:
    def __init__(
        self,
        spec: CandidateSpec,
        disks: str,
        label: str,
        max_combinations: int,
    ):
        self.spec = spec
        self.dataset, self.vdev, dnode = open_dataset(spec, disks, label)
        self.forensics = RaidzForensics(self.vdev, max_combinations)
        bonus = dnode.bonus
        log(
            "SOURCE",
            f"candidate={str(spec.path)!r} object={spec.dataset_object} "
            f"creation_txg={bonus.ds_creation_txg} "
            f"directory_object={bonus.ds_dir_obj}",
        )

    def recover_block(self, block_pointer) -> tuple[bytes | None, dict]:
        if block_pointer is None:
            return None, {"method": "missing-block-pointer"}
        if block_pointer.empty:
            return b"\0" * block_pointer.lsize, {"method": "hole"}
        if block_pointer._cksum != 7:
            return None, {
                "method": "unsupported-checksum",
                "checksum_type": block_pointer._cksum,
            }

        direct_errors = []
        for dva in range(3):
            if block_pointer.get_dva(dva).null:
                continue
            try:
                data, checksum_ok = self.vdev.read_block(block_pointer, dva=dva)
                if data is not None and checksum_ok:
                    return bytes(data), {"method": "direct", "dva": dva}
                direct_errors.append(f"DVA{dva}: checksum/decode failure")
            except Exception as error:
                direct_errors.append(f"DVA{dva}: {type(error).__name__}: {error}")

        parity = []
        for dva in range(3):
            if block_pointer.get_dva(dva).null:
                continue
            data, method, diagnostics = self.forensics.recover(block_pointer, dva)
            if data is not None:
                return data, {
                    "method": method,
                    "dva": dva,
                    "direct_errors": direct_errors,
                    **diagnostics,
                }
            parity.append({"dva": dva, "method": method, **diagnostics})
        return None, {
            "method": "unresolved",
            "direct_errors": direct_errors,
            "parity": parity,
        }

    def recover_file(self, object_id: int, expected_size: int) -> dict:
        dnode = self.dataset[object_id]
        if dnode is None:
            return {"status": "unresolved", "reason": "dnode-unreadable"}
        size = int(dnode.bonus.zp_size)
        if expected_size >= 0 and size != expected_size:
            return {
                "status": "unresolved",
                "reason": "dnode-size-mismatch",
                "expected_size": expected_size,
                "actual_size": size,
            }
        if size == 0:
            return {"status": "recovered", "size": 0, "content": b"", "blocks": []}
        if not dnode.blkptrs:
            return {"status": "unresolved", "reason": "no-block-pointers"}
        try:
            tree = BlockTree(dnode.levels, self.vdev, dnode.blkptrs[0])
        except Exception as error:
            return {
                "status": "unresolved",
                "reason": "block-tree-open-failed",
                "error": f"{type(error).__name__}: {error}",
            }

        content = bytearray()
        blocks = []
        remaining = size
        for block_number in range(dnode.maxblkid + 1):
            if not remaining:
                break
            try:
                block_pointer = tree[block_number]
            except Exception as error:
                blocks.append(
                    {
                        "block": block_number,
                        "method": "block-pointer-lookup-error",
                        "error": f"{type(error).__name__}: {error}",
                    }
                )
                return {
                    "status": "unresolved",
                    "reason": "block-pointer-lookup-error",
                    "size": size,
                    "blocks": blocks,
                }
            data, details = self.recover_block(block_pointer)
            blocks.append({"block": block_number, **details})
            if data is None:
                return {
                    "status": "unresolved",
                    "reason": "block-unresolved",
                    "size": size,
                    "blocks": blocks,
                }
            wanted = min(int(dnode.datablksize), remaining)
            if len(data) < wanted:
                return {
                    "status": "unresolved",
                    "reason": "short-decoded-block",
                    "size": size,
                    "blocks": blocks,
                }
            content.extend(data[:wanted])
            remaining -= wanted

        if remaining:
            return {
                "status": "unresolved",
                "reason": "file-blocks-missing",
                "size": size,
                "missing_bytes": remaining,
                "blocks": blocks,
            }
        return {
            "status": "recovered",
            "size": size,
            "content": bytes(content),
            "blocks": blocks,
        }


def output_path(root: Path, relative: str) -> Path:
    root = root.resolve()
    target = root.joinpath(*PurePosixPath(relative).parts)
    parent = target.parent.resolve()
    if parent != root and root not in parent.parents:
        raise RecoveryError(f"target escapes output directory: {relative!r}")
    return target


def atomic_write(path: Path, data: bytes, overwrite: bool) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    if (path.exists() or path.is_symlink()) and not overwrite:
        raise FileExistsError(path)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    digest = hashlib.sha256()
    try:
        with temporary.open("xb") as stream:
            stream.write(data)
            digest.update(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if temporary.exists():
            temporary.unlink()
    return digest.hexdigest()


def append_report(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--disks", required=True, help="files block-provider table")
    parser.add_argument("--label", required=True, help="device containing a ZFS label")
    parser.add_argument(
        "--file-list", required=True, help="output from inspect_dataset"
    )
    parser.add_argument(
        "--candidate",
        required=True,
        action="append",
        type=candidate_argument,
        help="PATH,OBJECT[,CREATION_TXG[,DIRECTORY_OBJECT]]; repeatable",
    )
    parser.add_argument("--path", action="append", default=[])
    parser.add_argument(
        "--manifest", help="one path per line; TSV/CSV first field is used"
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--report", help="JSONL report; defaults inside output-dir")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument(
        "--max-sector-combinations",
        type=int,
        default=100_000,
        help="bound checksum-guided parity combinations per block",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    file_list = Path(args.file_list)
    if not file_list.is_file():
        raise FileNotFoundError(file_list)
    targets = load_targets(args.path, args.manifest)
    index = load_file_list(file_list)
    output_root = Path(args.output_dir)
    report = Path(args.report) if args.report else output_root / "recovery-report.jsonl"
    sources = [
        DatasetSource(
            candidate,
            args.disks,
            args.label,
            args.max_sector_combinations,
        )
        for candidate in args.candidate
    ]

    counts: Counter[str] = Counter()
    started_all = time.monotonic()
    for target in targets:
        started = time.monotonic()
        record: dict[str, object] = {"path": target}
        indexed = index.get(target)
        if indexed is None:
            record.update(
                status="not-found",
                reason="path-not-present-in-file-list",
                elapsed_seconds=round(time.monotonic() - started, 6),
            )
            append_report(report, record)
            counts["not-found"] += 1
            log("RESULT", f"status=not-found path={target!r}")
            continue

        object_id, expected_size = indexed
        attempts = []
        recovered = used_source = None
        for source in sources:
            result = source.recover_file(object_id, expected_size)
            public = {key: value for key, value in result.items() if key != "content"}
            public["candidate"] = str(source.spec.path)
            attempts.append(public)
            if result["status"] == "recovered":
                recovered, used_source = result, source
                break

        if recovered is None:
            record.update(
                status="unresolved",
                object_id=object_id,
                expected_size=expected_size,
                attempts=attempts,
                elapsed_seconds=round(time.monotonic() - started, 6),
            )
            append_report(report, record)
            counts["unresolved"] += 1
            log("RESULT", f"status=unresolved object={object_id} path={target!r}")
            continue

        target_path = output_path(output_root, target)
        digest = atomic_write(target_path, recovered["content"], args.overwrite)
        methods = Counter(block["method"] for block in recovered["blocks"])
        record.update(
            status="recovered",
            object_id=object_id,
            expected_size=expected_size,
            size=recovered["size"],
            sha256=digest,
            output=str(target_path),
            candidate=str(used_source.spec.path),
            block_methods=dict(methods),
            blocks=recovered["blocks"],
            attempts=attempts,
            elapsed_seconds=round(time.monotonic() - started, 6),
        )
        append_report(report, record)
        counts["recovered"] += 1
        log(
            "RESULT",
            f"status=recovered object={object_id} size={recovered['size']} "
            f"methods={dict(methods)} sha256={digest} path={target!r}",
        )

    summary = {
        "targets": len(targets),
        "counts": dict(counts),
        "recovery_percent": round(100 * counts["recovered"] / len(targets), 2),
        "elapsed_seconds": round(time.monotonic() - started_all, 3),
    }
    log("SUMMARY", json.dumps(summary, sort_keys=True))
    return 0 if counts["recovered"] == len(targets) else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        log("STOP", "interrupted")
        raise SystemExit(130) from None
