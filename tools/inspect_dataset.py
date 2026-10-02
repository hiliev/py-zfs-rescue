#!/usr/bin/env python3
"""Open a saved MOS dataset dnode and inspect its ZPL tree read-only."""

from __future__ import annotations

import argparse
import contextlib
import os
from pathlib import Path

from recovery_common import CandidateSpec, open_dataset


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--disks", required=True, help="files block-provider table")
    parser.add_argument("--label", required=True, help="device containing a ZFS label")
    parser.add_argument("--candidate", required=True, help="saved MOS dnode block")
    parser.add_argument("--dataset-object", required=True, type=int)
    parser.add_argument("--creation-txg", type=int)
    parser.add_argument("--directory-object", type=int)
    parser.add_argument("--depth", type=int, default=2)
    parser.add_argument(
        "--file-list", help="write a tab-separated object/size/path list"
    )
    parser.add_argument(
        "--quiet-file-list",
        action="store_true",
        help="suppress one progress line per entry while exporting the file list",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    spec = CandidateSpec(
        path=Path(args.candidate),
        dataset_object=args.dataset_object,
        creation_txg=args.creation_txg,
        directory_object=args.directory_object,
    )
    dataset, _, dnode = open_dataset(spec, args.disks, args.label)
    print("[+] Recovered dataset dnode:")
    print(dnode)
    print(f"[+] Traversing root object {dataset._rootdir_id}, depth={args.depth}...")
    dataset.traverse_dir(dataset._rootdir_id, depth=args.depth)
    if args.file_list:
        if args.quiet_file_list:
            with open(os.devnull, "w") as sink, contextlib.redirect_stdout(sink):
                dataset.export_file_list(args.file_list)
        else:
            dataset.export_file_list(args.file_list)
        print(f"[+] File list saved: {args.file_list}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
