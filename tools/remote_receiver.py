#!/usr/bin/env python3
"""Receive a recovered file tree over stdin/stdout with durable validation.

Each file is staged on the destination filesystem, hashed independently, and
atomically installed only after the sender's SHA-256 agrees. A synchronous
SQLite ledger makes completed files safely skippable after a disconnect or
restart. Files the source could not verify are retained with a suffix and a
``corrupt`` ledger status for later targeted recovery.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import sqlite3
import stat
import sys
import time
from pathlib import Path, PurePosixPath

from remote_protocol import (
    PROTOCOL_VERSION,
    ProtocolError,
    read_exact,
    read_message,
    write_message,
)

COPY_BUFFER = 1024 * 1024
CORRUPT_SUFFIX = "._corrupted"


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class Receiver:
    def __init__(self, root: str, state_dir: str, reserve_bytes: int):
        self.root = Path(root).resolve()
        self.state_dir = Path(state_dir).resolve()
        self.reserve_bytes = reserve_bytes
        self.root.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.partials = self.state_dir / "partials"
        self.partials.mkdir(parents=True, exist_ok=True)
        if self.root.stat().st_dev != self.partials.stat().st_dev:
            raise RuntimeError(
                "destination root and state directory must be on the same filesystem"
            )

        self.db = sqlite3.connect(self.state_dir / "receiver-state.sqlite3")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                size INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                stored_path TEXT NOT NULL,
                object_id INTEGER,
                mode INTEGER,
                mtime INTEGER,
                completed_at INTEGER NOT NULL
            )
            """
        )
        self.db.commit()
        self.session_stored = 0
        self.session_skipped = 0
        self.session_corrupt = 0

    def close(self) -> None:
        self.db.close()

    def target(self, relative: str) -> Path:
        if not isinstance(relative, str) or not relative or "\0" in relative:
            raise ProtocolError("path must be a non-empty string without NUL")
        pure = PurePosixPath(relative)
        if pure.is_absolute() or any(part in ("", ".", "..") for part in pure.parts):
            raise ProtocolError(f"unsafe relative path: {relative!r}")
        target = self.root.joinpath(*pure.parts)
        if os.path.commonpath((str(self.root), str(target.absolute()))) != str(
            self.root
        ):
            raise ProtocolError(f"path escapes destination root: {relative!r}")
        return target

    def ensure_parents(self, target: Path) -> None:
        current = self.root
        for part in target.relative_to(self.root).parts[:-1]:
            child = current / part
            try:
                mode = child.lstat().st_mode
            except FileNotFoundError:
                child.mkdir()
                fsync_directory(current)
            else:
                if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
                    raise ProtocolError(f"unsafe parent component: {child}")
            current = child

    def validate_parents(self, target: Path) -> None:
        current = self.root
        for part in target.relative_to(self.root).parts[:-1]:
            current = current / part
            try:
                mode = current.lstat().st_mode
            except FileNotFoundError:
                return
            if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
                raise ProtocolError(f"unsafe parent component: {current}")

    def partial_path(self, relative: str) -> Path:
        name = hashlib.sha256(relative.encode("utf-8")).hexdigest() + ".part"
        return self.partials / name

    @staticmethod
    def apply_metadata(
        path: Path, mode, mtime, *, follow_symlinks: bool = True
    ) -> None:
        try:
            if mode is not None and follow_symlinks:
                os.chmod(path, int(mode) & 0o7777)
            if mtime is not None:
                os.utime(
                    path,
                    (int(mtime), int(mtime)),
                    follow_symlinks=follow_symlinks,
                )
        except (NotImplementedError, PermissionError, OSError) as error:
            print(
                f"[receiver] metadata warning for {path}: {error}",
                file=sys.stderr,
                flush=True,
            )

    def hello(self, message: dict) -> dict:
        if message.get("version") != PROTOCOL_VERSION:
            raise ProtocolError(
                f"protocol mismatch: sender={message.get('version')} "
                f"receiver={PROTOCOL_VERSION}"
            )
        usage = shutil.disk_usage(self.root)
        return {
            "status": "hello",
            "version": PROTOCOL_VERSION,
            "root": str(self.root),
            "state_dir": str(self.state_dir),
            "free_bytes": usage.free,
        }

    def mkdir(self, message: dict) -> dict:
        relative = message["path"]
        target = self.target(relative)
        self.ensure_parents(target)
        try:
            mode = target.lstat().st_mode
        except FileNotFoundError:
            target.mkdir()
            fsync_directory(target.parent)
        else:
            if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
                raise ProtocolError(
                    f"directory conflicts with existing object: {relative!r}"
                )
        return {"status": "directory", "path": relative}

    def directory_metadata(self, message: dict) -> dict:
        relative = message["path"]
        target = self.target(relative)
        self.validate_parents(target)
        try:
            mode = target.lstat().st_mode
        except FileNotFoundError as error:
            raise ProtocolError(f"directory is missing: {relative!r}") from error
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            raise ProtocolError(f"not a safe directory: {relative!r}")
        self.apply_metadata(target, message.get("mode"), message.get("mtime"))
        return {"status": "directory-metadata", "path": relative}

    def symlink(self, message: dict) -> dict:
        relative = message["path"]
        link_target = message["target"]
        if not isinstance(link_target, str) or "\0" in link_target:
            raise ProtocolError("symlink target must be a string without NUL")
        target = self.target(relative)
        self.ensure_parents(target)
        if target.is_symlink() and os.readlink(target) == link_target:
            return {"status": "skipped", "path": relative, "kind": "symlink"}
        if target.exists() or target.is_symlink():
            raise ProtocolError(f"symlink conflicts with existing object: {relative!r}")
        os.symlink(link_target, target)
        self.apply_metadata(target, None, message.get("mtime"), follow_symlinks=False)
        fsync_directory(target.parent)
        return {"status": "stored", "path": relative, "kind": "symlink"}

    def known_file(self, relative: str, size: int) -> dict | None:
        row = self.db.execute(
            "SELECT status, size, sha256, stored_path FROM files WHERE path = ?",
            (relative,),
        ).fetchone()
        if row is None:
            return None
        status, recorded_size, digest, stored_path = row
        if status not in ("ok", "corrupt"):
            raise ProtocolError(f"unknown ledger status for {relative!r}: {status!r}")
        actual = self.target(stored_path)
        self.validate_parents(actual)
        if actual.is_symlink():
            raise ProtocolError(f"ledger file is a symlink: {stored_path!r}")
        if recorded_size == size and actual.is_file() and actual.stat().st_size == size:
            return {
                "status": "skip-corrupt" if status == "corrupt" else "skip",
                "path": relative,
                "size": size,
                "sha256": digest,
            }
        return None

    def begin_file(self, message: dict, input_stream) -> dict | None:
        relative = message["path"]
        size = int(message["size"])
        if size < 0:
            raise ProtocolError(f"negative file size: {size}")
        target = self.target(relative)
        known = self.known_file(relative, size)
        if known is not None:
            self.session_skipped += 1
            write_message(sys.stdout.buffer, known)
            return None

        self.ensure_parents(target)
        if target.exists() or target.is_symlink():
            raise ProtocolError(f"untracked destination already exists: {relative!r}")
        free_bytes = shutil.disk_usage(self.root).free
        if free_bytes < size + self.reserve_bytes:
            raise ProtocolError(
                f"insufficient free space for {relative!r}: "
                f"need {size + self.reserve_bytes}, have {free_bytes}"
            )

        partial = self.partial_path(relative)
        write_message(sys.stdout.buffer, {"status": "ready", "path": relative})
        digest = hashlib.sha256()
        remaining = size
        with partial.open("wb") as output:
            while remaining:
                chunk = input_stream.read(min(COPY_BUFFER, remaining))
                if not chunk:
                    raise EOFError(
                        f"connection ended while receiving {relative!r}; "
                        f"{remaining} bytes missing"
                    )
                output.write(chunk)
                digest.update(chunk)
                remaining -= len(chunk)
            output.flush()
            os.fsync(output.fileno())

        trailer = read_exact(input_stream, 33)
        if trailer[0] not in (0, 1):
            raise ProtocolError(f"invalid source status byte: {trailer[0]}")
        source_ok = trailer[0] == 1
        sender_digest = trailer[1:].hex()
        receiver_digest = digest.hexdigest()
        if sender_digest != receiver_digest:
            raise ProtocolError(
                f"end-to-end SHA-256 mismatch for {relative!r}: "
                f"sender={sender_digest} receiver={receiver_digest}"
            )

        stored_relative = relative if source_ok else relative + CORRUPT_SUFFIX
        stored_target = self.target(stored_relative)
        self.ensure_parents(stored_target)
        if stored_target.exists() or stored_target.is_symlink():
            raise ProtocolError(
                f"destination appeared during transfer: {stored_relative!r}"
            )
        os.replace(partial, stored_target)
        self.apply_metadata(stored_target, message.get("mode"), message.get("mtime"))
        fsync_directory(stored_target.parent)

        status = "ok" if source_ok else "corrupt"
        self.db.execute(
            """
            INSERT INTO files
                (path, status, size, sha256, stored_path, object_id,
                 mode, mtime, completed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                status=excluded.status,
                size=excluded.size,
                sha256=excluded.sha256,
                stored_path=excluded.stored_path,
                object_id=excluded.object_id,
                mode=excluded.mode,
                mtime=excluded.mtime,
                completed_at=excluded.completed_at
            """,
            (
                relative,
                status,
                size,
                receiver_digest,
                stored_relative,
                message.get("object_id"),
                message.get("mode"),
                message.get("mtime"),
                int(time.time()),
            ),
        )
        self.db.commit()
        if source_ok:
            self.session_stored += 1
        else:
            self.session_corrupt += 1
        return {
            "status": "stored" if source_ok else "stored-corrupt",
            "path": relative,
            "stored_path": stored_relative,
            "size": size,
            "sha256": receiver_digest,
        }

    def run(self) -> int:
        input_stream = sys.stdin.buffer
        output_stream = sys.stdout.buffer
        while True:
            message = read_message(input_stream, allow_eof=True)
            if message is None:
                return 0
            operation = message.get("op")
            try:
                if operation == "hello":
                    response = self.hello(message)
                elif operation == "mkdir":
                    response = self.mkdir(message)
                elif operation == "directory-metadata":
                    response = self.directory_metadata(message)
                elif operation == "symlink":
                    response = self.symlink(message)
                elif operation == "file":
                    response = self.begin_file(message, input_stream)
                    if response is None:
                        continue
                elif operation == "end":
                    response = {
                        "status": "complete",
                        "stored": self.session_stored,
                        "skipped": self.session_skipped,
                        "corrupt": self.session_corrupt,
                    }
                    write_message(output_stream, response)
                    return 0
                else:
                    raise ProtocolError(f"unknown operation: {operation!r}")
                write_message(output_stream, response)
            except Exception as error:
                write_message(
                    output_stream,
                    {
                        "status": "error",
                        "operation": operation,
                        "path": message.get("path"),
                        "error": f"{type(error).__name__}: {error}",
                    },
                )
                print(
                    f"[receiver] fatal: {type(error).__name__}: {error}",
                    file=sys.stderr,
                    flush=True,
                )
                return 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument(
        "--reserve-bytes",
        type=int,
        default=1024**3,
        help="free space to retain in addition to the incoming file",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.reserve_bytes < 0:
        raise ValueError("--reserve-bytes cannot be negative")
    receiver = Receiver(args.root, args.state_dir, args.reserve_bytes)
    print(
        f"[receiver] ready root={receiver.root} state={receiver.state_dir}",
        file=sys.stderr,
        flush=True,
    )
    try:
        return receiver.run()
    finally:
        receiver.close()


if __name__ == "__main__":
    raise SystemExit(main())
