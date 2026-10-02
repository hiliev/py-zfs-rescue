#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import sqlite3
import struct
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"
for entry in (ROOT, TOOLS):
    if str(entry) not in sys.path:
        sys.path.insert(0, str(entry))

from remote_protocol import read_message, write_all, write_message  # noqa: E402

from zfs.blockptr import BlockPtr  # noqa: E402
from zfs.dataset import Dataset  # noqa: E402
from zfs.lz4zfs import lz4zfs_decompress  # noqa: E402
from zfs.zio import GenericDevice  # noqa: E402


class CoreRegressionTest(unittest.TestCase):
    def test_embedded_block_pointer_sizes_are_size_minus_one(self):
        qwords = [0] * 16
        qwords[6] = 511 | (63 << 25) | (1 << 39)
        pointer = BlockPtr(struct.pack("=16Q", *qwords))
        self.assertEqual(pointer.lsize, 512)
        self.assertEqual(pointer.psize, 64)

    def test_lz4_accepts_final_literal_only_sequence(self):
        payload = b"\x50hello"
        source = struct.pack(">I", len(payload)) + payload
        self.assertEqual(lz4zfs_decompress(source, 5), b"hello")

    def test_physical_read_is_rounded_to_ashift(self):
        class CapturingDevice(GenericDevice):
            def __init__(self):
                self._ashift = 12
                self._verbose = 0
                self.requested = None

            def _read_physical(self, offset, size, debug_dump, debug_prefix):
                self.requested = (offset, size)
                return b"x" * size

        dva = types.SimpleNamespace(offset=1234, _asize=8192, gang=False)
        pointer = types.SimpleNamespace(
            empty=False,
            get_dva=lambda _index: dva,
            psize=4608,
            lsize=4608,
            _embeded=False,
            _cksum=0,
            compressed=False,
        )
        device = CapturingDevice()
        data, checksum_ok = device.read_block(pointer)
        self.assertTrue(checksum_ok)
        self.assertEqual(device.requested, (1234, 8192))
        self.assertEqual(len(data), 4608)

    def test_sparse_file_holes_are_zero_filled_without_corruption(self):
        empty = types.SimpleNamespace(empty=True)
        allocated = types.SimpleNamespace(empty=False)

        class FakeTree:
            def __init__(self, *_args):
                pass

            def __getitem__(self, index):
                return (empty, allocated)[index]

        dnode = types.SimpleNamespace(
            levels=1,
            blkptrs=[object()],
            maxblkid=1,
            datablksize=4,
            bonus=types.SimpleNamespace(zp_size=6),
        )
        dataset = Dataset.__new__(Dataset)
        dataset._vdev = types.SimpleNamespace(
            read_block=lambda _pointer, dva=0: (b"abcd", True)
        )
        dataset.__getitem__ = lambda _object_id: dnode

        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "sparse.bin"
            with (
                mock.patch("zfs.dataset.BlockTree", FakeTree),
                mock.patch.object(Dataset, "__getitem__", return_value=dnode),
            ):
                self.assertTrue(dataset.extract_file(7, target))
            self.assertEqual(target.read_bytes(), b"\0\0\0\0ab")


class ReceiverProtocolTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        base = Path(self.tempdir.name)
        self.root = base / "destination"
        self.state = base / "state"

    def tearDown(self):
        self.tempdir.cleanup()

    def start_receiver(self):
        return subprocess.Popen(
            [
                sys.executable,
                str(TOOLS / "remote_receiver.py"),
                "--root",
                str(self.root),
                "--state-dir",
                str(self.state),
                "--reserve-bytes",
                "0",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )

    def hello(self, process):
        write_message(process.stdin, {"op": "hello", "version": 1})
        self.assertEqual(read_message(process.stdout)["status"], "hello")

    def send_file(self, process, relative, content, source_ok=True, digest=None):
        write_message(
            process.stdin,
            {
                "op": "file",
                "path": relative,
                "size": len(content),
                "object_id": 42,
                "mode": 0o640,
                "mtime": 1700000000,
            },
        )
        first = read_message(process.stdout)
        if first["status"].startswith("skip"):
            return first
        self.assertEqual(first["status"], "ready")
        for offset in range(0, len(content), 7919):
            write_all(process.stdin, content[offset : offset + 7919])
        trailer_digest = digest or hashlib.sha256(content).digest()
        write_all(
            process.stdin,
            (b"\x01" if source_ok else b"\x00") + trailer_digest,
        )
        process.stdin.flush()
        return read_message(process.stdout)

    def end(self, process):
        write_message(process.stdin, {"op": "end"})
        self.assertEqual(read_message(process.stdout)["status"], "complete")
        process.stdin.close()
        return_code = process.wait(timeout=5)
        stderr = process.stderr.read().decode()
        process.stdout.close()
        process.stderr.close()
        self.assertEqual(return_code, 0, stderr)

    def test_verified_store_restart_skip_and_interrupted_retry(self):
        content = (b"validated-zfs-recovery\0" * 50000)[:1000000]
        digest = hashlib.sha256(content).hexdigest()

        process = self.start_receiver()
        self.hello(process)
        write_message(process.stdin, {"op": "mkdir", "path": "tree"})
        self.assertEqual(read_message(process.stdout)["status"], "directory")
        stored = self.send_file(process, "tree/good.bin", content)
        self.assertEqual(stored["status"], "stored")
        self.assertEqual(stored["sha256"], digest)
        self.end(process)
        self.assertEqual((self.root / "tree/good.bin").read_bytes(), content)

        process = self.start_receiver()
        self.hello(process)
        skipped = self.send_file(process, "tree/good.bin", content)
        self.assertEqual(skipped["status"], "skip")
        self.end(process)

        process = self.start_receiver()
        self.hello(process)
        relative = "tree/interrupted.bin"
        write_message(
            process.stdin,
            {"op": "file", "path": relative, "size": len(content)},
        )
        self.assertEqual(read_message(process.stdout)["status"], "ready")
        write_all(process.stdin, content[:12345])
        process.stdin.close()
        self.assertNotEqual(process.wait(timeout=5), 0)
        process.stdout.close()
        process.stderr.close()
        self.assertFalse((self.root / relative).exists())

        process = self.start_receiver()
        self.hello(process)
        self.assertEqual(self.send_file(process, relative, content)["status"], "stored")
        self.end(process)

    def test_corrupt_output_can_be_quarantined_and_retried(self):
        content = b"recoverable source content"
        relative = "tree/damaged.bin"
        process = self.start_receiver()
        self.hello(process)
        result = self.send_file(process, relative, content, source_ok=False)
        self.assertEqual(result["status"], "stored-corrupt")
        self.end(process)
        corrupt = self.root / (relative + "._corrupted")
        self.assertEqual(corrupt.read_bytes(), content)

        quarantine = Path(self.tempdir.name) / "quarantine"
        command = [
            sys.executable,
            str(TOOLS / "retry_corrupt_remote_files.py"),
            "--root",
            str(self.root),
            "--state-dir",
            str(self.state),
            "--quarantine",
            str(quarantine),
        ]
        subprocess.run(command, check=True, capture_output=True, text=True)
        self.assertTrue(corrupt.exists())
        subprocess.run(
            [*command, "--apply"], check=True, capture_output=True, text=True
        )
        self.assertFalse(corrupt.exists())
        self.assertTrue((quarantine / "manifest.json").is_file())
        self.assertTrue((quarantine / "receiver-state-before.sqlite3").is_file())
        with sqlite3.connect(self.state / "receiver-state.sqlite3") as db:
            self.assertEqual(db.execute("SELECT count(*) FROM files").fetchone()[0], 0)

        process = self.start_receiver()
        self.hello(process)
        self.assertEqual(self.send_file(process, relative, content)["status"], "stored")
        self.end(process)
        self.assertEqual((self.root / relative).read_bytes(), content)

    def test_receiver_rejects_path_traversal_and_symlink_parent(self):
        process = self.start_receiver()
        self.hello(process)
        write_message(process.stdin, {"op": "mkdir", "path": "../escape"})
        response = read_message(process.stdout)
        self.assertEqual(response["status"], "error")
        process.stdin.close()
        self.assertEqual(process.wait(timeout=5), 2)
        process.stdout.close()
        process.stderr.close()

        outside = Path(self.tempdir.name) / "outside"
        outside.mkdir()
        self.root.mkdir(exist_ok=True)
        (self.root / "linked").symlink_to(outside, target_is_directory=True)
        process = self.start_receiver()
        self.hello(process)
        write_message(process.stdin, {"op": "mkdir", "path": "linked/escape"})
        response = read_message(process.stdout)
        self.assertEqual(response["status"], "error")
        process.stdin.close()
        self.assertEqual(process.wait(timeout=5), 2)
        process.stdout.close()
        process.stderr.close()
        self.assertFalse((outside / "escape").exists())

    def test_receiver_does_not_install_digest_mismatch(self):
        process = self.start_receiver()
        self.hello(process)
        relative = "tree/mismatch.bin"
        result = self.send_file(
            process,
            relative,
            b"payload",
            digest=b"\0" * hashlib.sha256().digest_size,
        )
        self.assertEqual(result["status"], "error")
        process.stdin.close()
        self.assertEqual(process.wait(timeout=5), 2)
        process.stdout.close()
        process.stderr.close()
        self.assertFalse((self.root / relative).exists())


if __name__ == "__main__":
    unittest.main()
