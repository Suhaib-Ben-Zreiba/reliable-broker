"""Wire framing for the broker protocol.

See PROTOCOL.md for the design rationale. This module only knows about
bytes and framing; it has no idea what CONNECT/PUBLISH/etc. mean. Keeping
framing separate from message handling means the framing logic can be
tested and trusted in complete isolation from broker behavior.
"""

from __future__ import annotations

import asyncio
import json
import struct

LENGTH_PREFIX_SIZE = 4
MAX_FRAME_SIZE = 10 * 1024 * 1024  # 10 MiB; prevents a bad length prefix from causing an unbounded allocation


class FrameTooLargeError(ValueError):
    """Raised when a peer claims a frame larger than MAX_FRAME_SIZE."""


class ConnectionClosedError(ConnectionError):
    """Raised when the peer closes the connection mid-frame."""


def encode_frame(payload: dict) -> bytes:
    """Encode a JSON-serializable dict into a length-prefixed frame."""
    body = json.dumps(payload).encode("utf-8")
    if len(body) > MAX_FRAME_SIZE:
        raise FrameTooLargeError(f"payload of {len(body)} bytes exceeds MAX_FRAME_SIZE={MAX_FRAME_SIZE}")
    return struct.pack(">I", len(body)) + body


async def read_frame(reader: asyncio.StreamReader) -> dict:
    """Read exactly one frame from an asyncio stream and decode it.

    Raises ConnectionClosedError if the peer disconnects before a full
    frame arrives (including a clean disconnect between frames, which is
    the normal way a client signals "I'm done").
    """
    length_bytes = await _read_exactly(reader, LENGTH_PREFIX_SIZE)
    (length,) = struct.unpack(">I", length_bytes)
    if length > MAX_FRAME_SIZE:
        raise FrameTooLargeError(f"peer declared frame of {length} bytes, exceeds MAX_FRAME_SIZE={MAX_FRAME_SIZE}")
    body = await _read_exactly(reader, length)
    return json.loads(body.decode("utf-8"))


async def _read_exactly(reader: asyncio.StreamReader, n: int) -> bytes:
    try:
        data = await reader.readexactly(n)
    except asyncio.IncompleteReadError as exc:
        raise ConnectionClosedError("peer closed connection mid-frame") from exc
    return data
