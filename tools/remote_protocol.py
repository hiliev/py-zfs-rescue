"""Framing helpers shared by the remote recovery sender and receiver."""

from __future__ import annotations

import json
import struct

PROTOCOL_VERSION = 1
MAX_MESSAGE = 1024 * 1024


class ProtocolError(RuntimeError):
    pass


def read_exact(stream, size: int) -> bytes:
    chunks = []
    remaining = size
    while remaining:
        chunk = stream.read(remaining)
        if not chunk:
            raise EOFError(f"unexpected EOF with {remaining} bytes remaining")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_message(stream, *, allow_eof: bool = False):
    length_raw = stream.read(4)
    if not length_raw and allow_eof:
        return None
    if len(length_raw) != 4:
        raise EOFError("short message length")
    (length,) = struct.unpack(">I", length_raw)
    if length > MAX_MESSAGE:
        raise ProtocolError(f"message too large: {length}")
    try:
        message = json.loads(read_exact(stream, length).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProtocolError(f"invalid JSON message: {error}") from error
    if not isinstance(message, dict):
        raise ProtocolError("protocol message must be a JSON object")
    return message


def write_all(stream, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = stream.write(view)
        if written is None:
            written = len(view)
        if written <= 0:
            raise BrokenPipeError("short protocol write")
        view = view[written:]


def write_message(stream, message: dict) -> None:
    payload = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    if len(payload) > MAX_MESSAGE:
        raise ProtocolError(f"message too large: {len(payload)}")
    write_all(stream, struct.pack(">I", len(payload)))
    write_all(stream, payload)
    stream.flush()
