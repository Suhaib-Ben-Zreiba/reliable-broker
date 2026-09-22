"""Integration test for Milestone 1: real TCP sockets, real asyncio server,
real client connection. This is deliberately not mocked -- the whole point
of Milestone 1 is proving framing works over an actual socket, not just in
memory (see test_protocol.py for the pure unit tests)."""

import asyncio

import pytest

from broker.protocol import encode_frame, read_frame
from broker.server import run_server


@pytest.mark.asyncio
async def test_server_echoes_a_single_frame():
    server = await run_server(host="127.0.0.1", port=0)  # port 0 = OS picks a free port
    addr = server.sockets[0].getsockname()

    try:
        reader, writer = await asyncio.open_connection(addr[0], addr[1])
        sent = {"type": "PUBLISH", "topic": "orders", "body": {"id": 42}}
        writer.write(encode_frame(sent))
        await writer.drain()

        received = await asyncio.wait_for(read_frame(reader), timeout=2)

        assert received == sent

        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_server_handles_multiple_frames_on_one_connection():
    server = await run_server(host="127.0.0.1", port=0)
    addr = server.sockets[0].getsockname()

    try:
        reader, writer = await asyncio.open_connection(addr[0], addr[1])

        for i in range(5):
            sent = {"type": "PUBLISH", "topic": "orders", "body": {"id": i}}
            writer.write(encode_frame(sent))
            await writer.drain()
            received = await asyncio.wait_for(read_frame(reader), timeout=2)
            assert received == sent

        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_server_handles_concurrent_connections():
    server = await run_server(host="127.0.0.1", port=0)
    addr = server.sockets[0].getsockname()

    async def client(client_id: int):
        reader, writer = await asyncio.open_connection(addr[0], addr[1])
        sent = {"type": "PUBLISH", "topic": "orders", "body": {"client_id": client_id}}
        writer.write(encode_frame(sent))
        await writer.drain()
        received = await asyncio.wait_for(read_frame(reader), timeout=2)
        writer.close()
        await writer.wait_closed()
        return received

    try:
        results = await asyncio.gather(*[client(i) for i in range(10)])
        assert {r["body"]["client_id"] for r in results} == set(range(10))
    finally:
        server.close()
        await server.wait_closed()
