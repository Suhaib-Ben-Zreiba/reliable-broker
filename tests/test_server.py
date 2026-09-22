"""Integration tests for Milestone 2: real TCP sockets, real asyncio server,
real publish/subscribe fan-out end to end. The server no longer echoes
frames (that was Milestone 1 scaffolding to prove framing worked); these
tests exercise the actual CONNECT/PUBLISH/SUBSCRIBE behavior."""

import asyncio

import pytest

from broker.protocol import encode_frame, read_frame
from broker.server import run_server


async def _connect(addr):
    return await asyncio.open_connection(addr[0], addr[1])


@pytest.mark.asyncio
async def test_subscriber_receives_a_published_message():
    server = await run_server(host="127.0.0.1", port=0)
    addr = server.sockets[0].getsockname()

    try:
        sub_reader, sub_writer = await _connect(addr)
        sub_writer.write(encode_frame({"type": "SUBSCRIBE", "topic": "orders"}))
        await sub_writer.drain()

        pub_reader, pub_writer = await _connect(addr)
        pub_writer.write(encode_frame({"type": "PUBLISH", "topic": "orders", "body": {"id": 1}}))
        await pub_writer.drain()

        message = await asyncio.wait_for(read_frame(sub_reader), timeout=2)
        assert message["type"] == "MESSAGE"
        assert message["topic"] == "orders"
        assert message["body"] == {"id": 1}

        for w in (sub_writer, pub_writer):
            w.close()
            await w.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_multiple_subscribers_each_receive_the_message():
    server = await run_server(host="127.0.0.1", port=0)
    addr = server.sockets[0].getsockname()

    try:
        sub_readers = []
        sub_writers = []
        for _ in range(3):
            r, w = await _connect(addr)
            w.write(encode_frame({"type": "SUBSCRIBE", "topic": "orders"}))
            await w.drain()
            sub_readers.append(r)
            sub_writers.append(w)

        pub_reader, pub_writer = await _connect(addr)
        pub_writer.write(encode_frame({"type": "PUBLISH", "topic": "orders", "body": {"id": 7}}))
        await pub_writer.drain()

        for r in sub_readers:
            message = await asyncio.wait_for(read_frame(r), timeout=2)
            assert message["body"] == {"id": 7}

        for w in (*sub_writers, pub_writer):
            w.close()
            await w.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_subscriber_does_not_receive_messages_for_other_topics():
    server = await run_server(host="127.0.0.1", port=0)
    addr = server.sockets[0].getsockname()

    try:
        sub_reader, sub_writer = await _connect(addr)
        sub_writer.write(encode_frame({"type": "SUBSCRIBE", "topic": "orders"}))
        await sub_writer.drain()

        pub_reader, pub_writer = await _connect(addr)
        pub_writer.write(encode_frame({"type": "PUBLISH", "topic": "shipping", "body": {"id": 1}}))
        await pub_writer.drain()
        # also publish to the topic the subscriber cares about, so we have
        # something to wait for instead of racing an arbitrary sleep
        pub_writer.write(encode_frame({"type": "PUBLISH", "topic": "orders", "body": {"id": 2}}))
        await pub_writer.drain()

        message = await asyncio.wait_for(read_frame(sub_reader), timeout=2)
        assert message["topic"] == "orders"
        assert message["body"] == {"id": 2}

        for w in (sub_writer, pub_writer):
            w.close()
            await w.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_publish_with_no_subscribers_does_not_error():
    server = await run_server(host="127.0.0.1", port=0)
    addr = server.sockets[0].getsockname()

    try:
        reader, writer = await _connect(addr)
        writer.write(encode_frame({"type": "PUBLISH", "topic": "nobody-home", "body": {"id": 1}}))
        await writer.drain()

        # Follow up with something we CAN observe, to confirm the connection
        # is still alive and processing frames after the no-op publish.
        writer.write(encode_frame({"type": "SUBSCRIBE", "topic": "nobody-home"}))
        await writer.drain()
        writer.write(encode_frame({"type": "PUBLISH", "topic": "nobody-home", "body": {"id": 2}}))
        await writer.drain()

        message = await asyncio.wait_for(read_frame(reader), timeout=2)
        assert message["body"] == {"id": 2}

        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_unknown_message_type_gets_an_error_reply():
    server = await run_server(host="127.0.0.1", port=0)
    addr = server.sockets[0].getsockname()

    try:
        reader, writer = await _connect(addr)
        writer.write(encode_frame({"type": "NONSENSE"}))
        await writer.drain()

        reply = await asyncio.wait_for(read_frame(reader), timeout=2)
        assert reply["type"] == "ERROR"

        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_disconnecting_subscriber_does_not_crash_future_publishes():
    server = await run_server(host="127.0.0.1", port=0)
    addr = server.sockets[0].getsockname()

    try:
        sub_reader, sub_writer = await _connect(addr)
        sub_writer.write(encode_frame({"type": "SUBSCRIBE", "topic": "orders"}))
        await sub_writer.drain()
        sub_writer.close()
        await sub_writer.wait_closed()

        # Give the server a moment to notice the disconnect and clean up.
        await asyncio.sleep(0.1)

        pub_reader, pub_writer = await _connect(addr)
        pub_writer.write(encode_frame({"type": "PUBLISH", "topic": "orders", "body": {"id": 1}}))
        await pub_writer.drain()
        # If the server were still trying to write to the dead subscriber's
        # connection, this would hang or raise instead of completing cleanly.
        pub_writer.write(encode_frame({"type": "CONNECT", "role": "producer"}))
        await pub_writer.drain()

        pub_writer.close()
        await pub_writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()
