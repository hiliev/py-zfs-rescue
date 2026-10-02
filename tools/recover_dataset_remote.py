#!/usr/bin/env python3
"""Recover a ZFS dataset to a remote, validated, resumable file tree.

Files travel over one persistent SSH connection. Both ends calculate SHA-256;
the receiver atomically installs a file only after the digests agree. Completed
files are recorded in a remote SQLite ledger and skipped after a restart.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import posixpath
import shlex
import subprocess
import time

from recovery_common import CandidateSpec, open_dataset, parse_candidate_spec
from remote_protocol import PROTOCOL_VERSION, read_message, write_all, write_message

from zfs.blocktree import BlockTree
from zfs.zap import TYPECODES, safe_decode_string, zap_factory


class RemoteFatalError(RuntimeError):
    pass


class LimitReached(Exception):
    pass


def log(kind: str, message: str) -> None:
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    print(f"{timestamp} [{kind}] {message}", flush=True)


def candidate_argument(value: str) -> CandidateSpec:
    try:
        return parse_candidate_spec(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


class RemoteSession:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.process: subprocess.Popen | None = None

    def ssh_command(self) -> list[str]:
        remote_command = shlex.join(
            [
                self.args.remote_python,
                "-u",
                self.args.receiver,
                "--root",
                self.args.destination,
                "--state-dir",
                self.args.state_dir,
                "--reserve-bytes",
                str(self.args.reserve_bytes),
            ]
        )
        command = [
            "ssh",
            "-T",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            f"ConnectTimeout={self.args.connect_timeout}",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=3",
        ]
        if self.args.identity:
            command.extend(["-i", self.args.identity, "-o", "IdentitiesOnly=yes"])
        if self.args.known_hosts:
            command.extend(["-o", f"UserKnownHostsFile={self.args.known_hosts}"])
        for option in self.args.ssh_option:
            command.extend(["-o", option])
        command.extend([self.args.remote, remote_command])
        return command

    def connect(self) -> None:
        self.close()
        log("NETWORK", f"connecting to {self.args.remote}")
        self.process = subprocess.Popen(
            self.ssh_command(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None,
            bufsize=0,
        )
        if self.process.stdin is None or self.process.stdout is None:
            raise RuntimeError("failed to open SSH pipes")
        write_message(
            self.process.stdin,
            {"op": "hello", "version": PROTOCOL_VERSION},
        )
        response = self.response()
        if response.get("status") != "hello":
            raise RemoteFatalError(f"unexpected receiver handshake: {response}")
        log(
            "NETWORK",
            f"receiver ready root={response['root']} "
            f"free={response['free_bytes'] / (1024**3):.2f} GiB",
        )

    def close(self) -> None:
        if self.process is None:
            return
        try:
            if self.process.stdin:
                self.process.stdin.close()
        except OSError:
            pass
        try:
            self.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.process = None

    def abort(self) -> None:
        if self.process is not None:
            self.process.kill()
            self.process.wait()
            self.process = None

    def response(self) -> dict:
        if self.process is None or self.process.stdout is None:
            raise ConnectionError("receiver is not connected")
        response = read_message(self.process.stdout)
        if response.get("status") == "error":
            raise RemoteFatalError(
                f"remote {response.get('operation')} failed for "
                f"{response.get('path')!r}: {response.get('error')}"
            )
        return response

    def request(self, message: dict) -> dict:
        if self.process is None or self.process.stdin is None:
            raise ConnectionError("receiver is not connected")
        write_message(self.process.stdin, message)
        return self.response()


class RemoteRecovery:
    def __init__(self, args: argparse.Namespace, dataset, vdev):
        self.args = args
        self.dataset = dataset
        self.vdev = vdev
        self.remote = RemoteSession(args)
        self.stats = {
            "directories": 0,
            "files": 0,
            "stored": 0,
            "skipped": 0,
            "corrupt": 0,
            "unreadable_entries": 0,
            "special_entries": 0,
            "bytes_sent": 0,
        }

    def with_reconnect(self, operation, description: str):
        attempts = 0
        while True:
            try:
                if self.remote.process is None:
                    self.remote.connect()
                return operation()
            except RemoteFatalError:
                raise
            except (BrokenPipeError, ConnectionError, EOFError, OSError) as error:
                attempts += 1
                self.remote.abort()
                if self.args.max_retries and attempts > self.args.max_retries:
                    raise RuntimeError(
                        f"network retries exhausted for {description}: {error}"
                    ) from error
                log(
                    "RETRY",
                    f"{description}: {type(error).__name__}: {error}; "
                    f"retrying in {self.args.retry_delay}s (attempt {attempts})",
                )
                time.sleep(self.args.retry_delay)

    def request(self, message: dict, description: str) -> dict:
        return self.with_reconnect(lambda: self.remote.request(message), description)

    def best_block(self, block_pointer) -> tuple[bytes | None, bool]:
        last_error = None
        for dva in range(3):
            if block_pointer.get_dva(dva).null:
                continue
            try:
                data, checksum_ok = self.vdev.read_block(block_pointer, dva=dva)
                if data is not None and checksum_ok:
                    return bytes(data), True
            except Exception as error:
                last_error = error
        if last_error is not None:
            log("SOURCE", f"all DVAs failed; last error: {last_error}")
        return None, False

    def stream_file_content(self, dnode, output, digest) -> tuple[bool, int]:
        remaining = int(dnode.bonus.zp_size)
        if remaining == 0:
            return True, 0
        tree = BlockTree(dnode.levels, self.vdev, dnode.blkptrs[0])
        block_size = int(dnode.datablksize)
        source_ok = True
        sent = 0

        for block_number in range(dnode.maxblkid + 1):
            if remaining <= 0:
                break
            wanted = min(block_size, remaining)
            block_data = None
            block_ok = True
            try:
                block_pointer = tree[block_number]
                if block_pointer is None:
                    block_ok = False
                elif block_pointer.empty:
                    block_data = b"\0" * wanted
                else:
                    block_data, block_ok = self.best_block(block_pointer)
            except Exception as error:
                log("SOURCE", f"block {block_number} lookup failed: {error}")
                block_ok = False

            if not block_ok or block_data is None:
                source_ok = False
                block_data = b"\0" * wanted
            elif len(block_data) < wanted:
                source_ok = False
                block_data += b"\0" * (wanted - len(block_data))
            else:
                block_data = block_data[:wanted]

            write_all(output, block_data)
            digest.update(block_data)
            sent += len(block_data)
            remaining -= len(block_data)

        while remaining > 0:
            source_ok = False
            chunk = b"\0" * min(block_size, remaining)
            write_all(output, chunk)
            digest.update(chunk)
            sent += len(chunk)
            remaining -= len(chunk)
        return source_ok, sent

    def transfer_file_once(self, relative: str, object_id: int, dnode):
        if self.remote.process is None or self.remote.process.stdin is None:
            raise ConnectionError("receiver is not connected")
        size = int(dnode.bonus.zp_size)
        response = self.remote.request(
            {
                "op": "file",
                "path": relative,
                "size": size,
                "object_id": object_id,
                "mode": int(dnode.bonus.zp_mode),
                "mtime": int(dnode.bonus.mtime()),
            }
        )
        status = response.get("status")
        if status in ("skip", "skip-corrupt"):
            return response, 0
        if status != "ready":
            raise RemoteFatalError(
                f"unexpected file response for {relative!r}: {response}"
            )

        digest = hashlib.sha256()
        source_ok, sent = self.stream_file_content(
            dnode, self.remote.process.stdin, digest
        )
        write_all(
            self.remote.process.stdin,
            (b"\x01" if source_ok else b"\x00") + digest.digest(),
        )
        self.remote.process.stdin.flush()
        response = self.remote.response()
        if response.get("sha256") != digest.hexdigest():
            raise RemoteFatalError(
                f"receiver acknowledgement digest differs for {relative!r}"
            )
        if int(response.get("size", -1)) != size:
            raise RemoteFatalError(
                f"receiver acknowledgement size differs for {relative!r}"
            )
        return response, sent

    def transfer_file(self, relative: str, object_id: int, dnode) -> None:
        if self.args.limit_files and self.stats["files"] >= self.args.limit_files:
            raise LimitReached
        self.stats["files"] += 1
        response, sent = self.with_reconnect(
            lambda: self.transfer_file_once(relative, object_id, dnode),
            f"file {relative!r}",
        )
        self.stats["bytes_sent"] += sent
        status = response["status"]
        if status == "stored":
            self.stats["stored"] += 1
        elif status == "stored-corrupt":
            self.stats["corrupt"] += 1
        elif status in ("skip", "skip-corrupt"):
            self.stats["skipped"] += 1
            if status == "skip-corrupt":
                self.stats["corrupt"] += 1
        log(
            "FILE",
            f"status={status} object={object_id} size={dnode.bonus.zp_size} "
            f"sha256={response.get('sha256', '-')} path={relative!r}",
        )

    def read_symlink(self, dnode) -> tuple[str, bool]:
        size = int(dnode.bonus.zp_size)
        inline = dnode.bonus.zp_inline_content
        if inline is not None and size <= len(inline):
            return safe_decode_string(bytes(inline[:size])), True

        class Buffer:
            def __init__(self):
                self.data = bytearray()

            def write(self, value):
                self.data.extend(value)

        output = Buffer()
        digest = hashlib.sha256()
        source_ok, _ = self.stream_file_content(dnode, output, digest)
        return safe_decode_string(bytes(output.data[:size])), source_ok

    def traverse(self, directory_object: int, prefix: str = "") -> None:
        directory_dnode = self.dataset[directory_object]
        if directory_dnode is None:
            self.stats["unreadable_entries"] += 1
            log(
                "ERROR",
                f"unreadable directory object={directory_object} path={prefix!r}",
            )
            return
        zap = zap_factory(self.vdev, directory_dnode)
        if zap is None:
            self.stats["unreadable_entries"] += 1
            log(
                "ERROR",
                f"unreadable directory ZAP object={directory_object} path={prefix!r}",
            )
            return

        for name in sorted(zap.keys()):
            value = zap[name]
            type_code = value >> 60
            object_id = value & ~(15 << 60)
            kind = TYPECODES[type_code]
            relative = posixpath.join(prefix, name)
            dnode = self.dataset[object_id]
            if dnode is None:
                self.stats["unreadable_entries"] += 1
                log("ERROR", f"unreadable object={object_id} path={relative!r}")
                continue

            if kind == "d":
                self.request(
                    {"op": "mkdir", "path": relative},
                    f"directory {relative!r}",
                )
                self.stats["directories"] += 1
                self.traverse(object_id, relative)
                self.request(
                    {
                        "op": "directory-metadata",
                        "path": relative,
                        "mode": int(dnode.bonus.zp_mode),
                        "mtime": int(dnode.bonus.mtime()),
                    },
                    f"directory metadata {relative!r}",
                )
            elif kind == "f":
                self.transfer_file(relative, object_id, dnode)
            elif kind == "l":
                target, source_ok = self.read_symlink(dnode)
                if not source_ok:
                    self.stats["unreadable_entries"] += 1
                    log(
                        "ERROR",
                        f"corrupt symlink object={object_id} path={relative!r}",
                    )
                    continue
                response = self.request(
                    {
                        "op": "symlink",
                        "path": relative,
                        "target": target,
                        "mtime": int(dnode.bonus.mtime()),
                    },
                    f"symlink {relative!r}",
                )
                log(
                    "SYMLINK",
                    f"status={response['status']} path={relative!r} target={target!r}",
                )
            else:
                self.stats["special_entries"] += 1
                log(
                    "SKIP",
                    f"special kind={kind!r} object={object_id} path={relative!r}",
                )

    def run(self) -> int:
        self.remote.connect()
        limited = False
        try:
            self.traverse(self.dataset._rootdir_id)
        except LimitReached:
            limited = True
            log("TEST", f"file limit {self.args.limit_files} reached")
        response = self.request({"op": "end"}, "end of session")
        log("REMOTE", f"receiver session summary: {response}")
        log("SUMMARY", json.dumps(self.stats, sort_keys=True))
        if limited:
            log("SUMMARY", "stopped at the requested file limit")
        return 0

    def close(self) -> None:
        self.remote.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--disks", required=True, help="files block-provider table")
    parser.add_argument("--label", required=True, help="device containing a ZFS label")
    parser.add_argument(
        "--candidate",
        required=True,
        type=candidate_argument,
        help="PATH,OBJECT[,CREATION_TXG[,DIRECTORY_OBJECT]]",
    )
    parser.add_argument("--bad-disk", action="append", type=int, default=[])
    parser.add_argument(
        "--remote", required=True, help="SSH host, optionally user@host"
    )
    parser.add_argument(
        "--identity", help="SSH private key; omit to use normal SSH auth"
    )
    parser.add_argument("--known-hosts", help="alternate known_hosts file")
    parser.add_argument(
        "--ssh-option",
        action="append",
        default=[],
        help="additional ssh -o option; repeatable",
    )
    parser.add_argument("--connect-timeout", type=int, default=10)
    parser.add_argument("--remote-python", default="python3")
    parser.add_argument("--receiver", required=True, help="remote receiver script path")
    parser.add_argument("--destination", required=True, help="remote output root")
    parser.add_argument("--state-dir", required=True, help="remote durable state")
    parser.add_argument("--reserve-bytes", type=int, default=1024**3)
    parser.add_argument("--limit-files", type=int, default=0)
    parser.add_argument("--retry-delay", type=int, default=15)
    parser.add_argument(
        "--max-retries",
        type=int,
        default=0,
        help="zero retries forever; positive values are useful for tests",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if (
        min(
            args.connect_timeout,
            args.reserve_bytes,
            args.limit_files,
            args.retry_delay,
            args.max_retries,
        )
        < 0
    ):
        raise ValueError("numeric limits cannot be negative")
    dataset, vdev, dnode = open_dataset(
        args.candidate,
        args.disks,
        args.label,
        bad_disks=args.bad_disk,
    )
    log("SOURCE", f"validated recovered dataset dnode: {dnode}")
    log("SOURCE", f"dataset root object={dataset._rootdir_id}")
    recovery = RemoteRecovery(args, dataset, vdev)
    try:
        return recovery.run()
    finally:
        recovery.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        log("STOP", "interrupted")
        raise SystemExit(130) from None
