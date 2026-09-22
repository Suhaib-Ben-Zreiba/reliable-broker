"""Unit tests for wire framing. No sockets involved here -- these test the
pure encode/decode logic in isolation, per PROTOCOL.md."""

import asyncio
import struct

import pytest

from broker.protocol import (
    FrameTooLargeError,
    ConnectionClosedError,
    encode_frame,
    read_frame,
)


def test_encode_frame_has_correct_length_prefix():
    payload = {"type": "PUBLISH", "topic": "orders", "body": {"id": 1}}
    frame = encode_frame(payload)

    (declared_length,) = struct.unpack(">I", frame[:4])
    assert declared_length == len(frame) - 4


def test_encode_frame_rejects_oversized_payload(monkeypatch):
    monkeypatch.setattr("broker.protocol.MAX_FRAME_SIZE", 10)
    with pytest.raises(FrameTooLargeError):
        encode_frame({"type": "PUBLISH", "body": "this payload is longer than ten bytes"})


class _FakeStreamReader:
    """Minimal stand-in for asyncio.StreamReader for pure-async unit tests."""

    def __init__(self, data: bytes):
        self._data = data
        self._pos = 0

    async def readexactly(self, n: int) -> bytes:
        if self._pos + n > len(self._data):
            raise asyncio.IncompleteReadError(self._data[self._pos:], n)
        chunk = self._data[self._pos:self._pos + n]
        self._pos += n
        return chunk


@pytest.mark.asyncio
async def test_read_frame_round_trips_encode_frame():
    payload = {"type": "SUBSCRIBE", "topic": "orders"}
    reader = _FakeStreamReader(encode_frame(payload))

    result = await read_frame(reader)

    assert result == payload


@pytest.mark.asyncio
async def test_read_frame_raises_on_truncated_stream():
    # A length prefix promising 100 bytes, but only 3 bytes actually follow.
    truncated = struct.pack(">I", 100) + b"abc"
    reader = _FakeStreamReader(truncated)

    with pytest.raises(ConnectionClosedError):
        await read_frame(reader)


@pytest.mark.asyncio
async def test_read_frame_raises_on_empty_stream():
    reader = _FakeStreamReader(b"")

    with pytest.raises(ConnectionClosedError):
        await read_frame(reader)
