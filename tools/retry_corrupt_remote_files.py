#!/usr/bin/env python3
"""Quarantine corrupt receiver outputs and make them eligible for retry.

The remote receiver deliberately skips every path recorded in its durable
ledger, including source-corrupt files. After improving the source reader, stop
the receiver and use this helper to move only corrupt outputs into a recoverable
quarantine and remove only their ledger rows. The default mode validates and
prints the plan without changing anything.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import sys
from pathlib import Path, PurePosixPath


def safe_target(base: Path, relative: str) -> Path:
    pure = PurePosixPath(relative)
    if (
        not relative
        or pure.is_absolute()
        or any(part in ("", ".", "..") for part in pure.parts)
    ):
        raise ValueError(f"unsafe relative path: {relative!r}")
    target = base.joinpath(*pure.parts)
    if os.path.commonpath((str(base), str(target.absolute()))) != str(base):
        raise ValueError(f"path escapes base directory: {relative!r}")
    return target


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def corrupt_rows(db: sqlite3.Connection) -> list[dict]:
    db.row_factory = sqlite3.Row
    rows = db.execute(
        """
        SELECT path, status, size, sha256, stored_path, object_id,
               mode, mtime, completed_at
        FROM files
        WHERE status = 'corrupt'
        ORDER BY path
        """
    ).fetchall()
    return [dict(row) for row in rows]


def validate_rows(root: Path, quarantine: Path, rows: list[dict]) -> int:
    total = 0
    for row in rows:
        source = safe_target(root, row["stored_path"])
        original = safe_target(root, row["path"])
        quarantined = safe_target(quarantine / "files", row["stored_path"])
        if row["stored_path"] == row["path"]:
            raise RuntimeError(f"corrupt row points at original path: {row['path']!r}")
        if source.is_symlink() or not source.is_file():
            raise RuntimeError(
                f"recorded corrupt file is missing/not regular: {source}"
            )
        actual_size = source.stat().st_size
        if actual_size != row["size"]:
            raise RuntimeError(
                f"size mismatch for {source}: ledger={row['size']} actual={actual_size}"
            )
        if original.exists() or original.is_symlink():
            raise RuntimeError(f"original destination unexpectedly exists: {original}")
        if quarantined.exists() or quarantined.is_symlink():
            raise RuntimeError(f"quarantine destination already exists: {quarantined}")
        total += actual_size
    return total


def write_manifest(path: Path, payload: dict) -> None:
    with path.open("x", encoding="utf-8") as output:
        json.dump(payload, output, ensure_ascii=False, indent=2, sort_keys=True)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    fsync_directory(path.parent)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, help="receiver destination root")
    parser.add_argument("--state-dir", required=True, help="receiver durable state")
    parser.add_argument(
        "--quarantine",
        help="destination for rollback data; defaults under state-dir",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="perform the move and ledger reset; default is validation only",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()
    state_dir = Path(args.state_dir).resolve()
    database = state_dir / "receiver-state.sqlite3"
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    quarantine = Path(
        args.quarantine or state_dir / f"corrupt-retry-{timestamp}"
    ).resolve()

    if not root.is_dir():
        raise RuntimeError(f"receiver root does not exist: {root}")
    if not state_dir.is_dir():
        raise RuntimeError(f"state directory does not exist: {state_dir}")
    if not database.is_file():
        raise RuntimeError(f"receiver database does not exist: {database}")
    if root == quarantine or root in quarantine.parents:
        raise RuntimeError("quarantine must not be inside the recovered-data root")

    db = sqlite3.connect(database, timeout=30)
    try:
        rows = corrupt_rows(db)
        total = validate_rows(root, quarantine, rows)
        summary = {
            "mode": "apply" if args.apply else "dry-run",
            "rows": len(rows),
            "bytes": total,
            "root": str(root),
            "state_dir": str(state_dir),
            "quarantine": str(quarantine),
        }
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True), flush=True)
        for row in rows:
            print(
                json.dumps(
                    {
                        "path": row["path"],
                        "stored_path": row["stored_path"],
                        "size": row["size"],
                        "sha256": row["sha256"],
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                flush=True,
            )
        if not args.apply or not rows:
            return 0

        quarantine.mkdir(parents=True, exist_ok=False)
        files_dir = quarantine / "files"
        files_dir.mkdir()
        if root.stat().st_dev != files_dir.stat().st_dev:
            raise RuntimeError("quarantine and recovered data must share a filesystem")

        backup_path = quarantine / "receiver-state-before.sqlite3"
        with sqlite3.connect(backup_path) as backup_db:
            db.backup(backup_db)
        with backup_path.open("rb") as backup_file:
            os.fsync(backup_file.fileno())

        manifest = {
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            **summary,
            "database_backup": str(backup_path),
            "entries": rows,
        }
        write_manifest(quarantine / "manifest.json", manifest)

        db.execute("BEGIN IMMEDIATE")
        locked_rows = corrupt_rows(db)
        if locked_rows != rows:
            raise RuntimeError("corrupt ledger rows changed after validation")

        for row in rows:
            source = safe_target(root, row["stored_path"])
            target = safe_target(files_dir, row["stored_path"])
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(source, target)
            fsync_directory(source.parent)
            fsync_directory(target.parent)

        deleted = db.execute("DELETE FROM files WHERE status = 'corrupt'").rowcount
        if deleted != len(rows):
            raise RuntimeError(
                f"ledger changed during reset: expected={len(rows)} deleted={deleted}"
            )
        db.commit()
        fsync_directory(state_dir)
        print(
            json.dumps(
                {
                    "status": "complete",
                    "quarantined": len(rows),
                    "bytes": total,
                    "quarantine": str(quarantine),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            flush=True,
        )
        return 0
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1) from None
