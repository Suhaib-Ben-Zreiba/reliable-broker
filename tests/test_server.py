"""Integration tests for Milestones 2, 3, and 4: real TCP sockets, real
asyncio server, real publish/subscribe fan-out, persistence, and
acknowledgement/redelivery end to end. The server no longer echoes frames
(that was Milestone 1 scaffolding to prove framing worked); these tests
exercise CONNECT/PUBLISH/SUBSCRIBE/ACK and durable replay across a
simulated restart."""

import asyncio

import pytest

from broker.delivery import AckTracker
from broker.protocol import encode_frame, read_frame
from broker.server import run_server
from broker.storage import LogStore
from broker.topics import TopicRegistry

ACK_TIMEOUT = 0.1
PAST_ACK_TIMEOUT = ACK_TIMEOUT * 1.5


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


@pytest.mark.asyncio
async def test_new_subscriber_receives_messages_published_before_it_subscribed(tmp_path):
    """The actual Milestone 2 gap this milestone exists to fix: a message
    published while nobody was subscribed used to be gone forever."""
    registry = TopicRegistry(store=LogStore(tmp_path))
    server = await run_server(host="127.0.0.1", port=0, registry=registry)
    addr = server.sockets[0].getsockname()

    try:
        pub_reader, pub_writer = await _connect(addr)
        pub_writer.write(encode_frame({"type": "PUBLISH", "topic": "orders", "body": {"id": 1}}))
        await pub_writer.drain()
        pub_writer.close()
        await pub_writer.wait_closed()

        # Nobody was subscribed when that was published. A new subscriber
        # arriving afterward must still see it.
        sub_reader, sub_writer = await _connect(addr)
        sub_writer.write(encode_frame({"type": "SUBSCRIBE", "topic": "orders"}))
        await sub_writer.drain()

        message = await asyncio.wait_for(read_frame(sub_reader), timeout=2)
        assert message["body"] == {"id": 1}

        sub_writer.close()
        await sub_writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_broker_restart_does_not_lose_messages(tmp_path):
    """The real end-to-end proof: stop the server entirely, start a brand
    new one pointed at the same data directory, and confirm a subscriber
    still sees what was published before the 'restart'."""
    first_registry = TopicRegistry(store=LogStore(tmp_path))
    first_server = await run_server(host="127.0.0.1", port=0, registry=first_registry)
    first_addr = first_server.sockets[0].getsockname()

    pub_reader, pub_writer = await _connect(first_addr)
    pub_writer.write(encode_frame({"type": "PUBLISH", "topic": "orders", "body": {"id": 1}}))
    await pub_writer.drain()
    pub_writer.close()
    await pub_writer.wait_closed()

    first_server.close()
    await first_server.wait_closed()

    # A genuinely new process would construct a new LogStore over the same
    # directory; simulated here by constructing fresh objects rather than
    # reusing anything from the first server.
    second_registry = TopicRegistry(store=LogStore(tmp_path))
    second_server = await run_server(host="127.0.0.1", port=0, registry=second_registry)
    second_addr = second_server.sockets[0].getsockname()

    try:
        sub_reader, sub_writer = await _connect(second_addr)
        sub_writer.write(encode_frame({"type": "SUBSCRIBE", "topic": "orders"}))
        await sub_writer.drain()

        message = await asyncio.wait_for(read_frame(sub_reader), timeout=2)
        assert message["body"] == {"id": 1}
        assert message["message_id"] == "m-1"

        sub_writer.close()
        await sub_writer.wait_closed()
    finally:
        second_server.close()
        await second_server.wait_closed()


@pytest.mark.asyncio
async def test_unacked_message_is_redelivered_to_the_same_connection():
    """The real end-to-end proof for Milestone 4: a subscriber that
    receives a message but never ACKs it gets sent that exact message
    again, over the same real socket, once the ack timeout elapses."""
    registry = TopicRegistry(ack_tracker=AckTracker(timeout=ACK_TIMEOUT))
    server = await run_server(host="127.0.0.1", port=0, registry=registry)
    addr = server.sockets[0].getsockname()

    try:
        sub_reader, sub_writer = await _connect(addr)
        sub_writer.write(encode_frame({"type": "SUBSCRIBE", "topic": "orders"}))
        await sub_writer.drain()

        pub_reader, pub_writer = await _connect(addr)
        pub_writer.write(encode_frame({"type": "PUBLISH", "topic": "orders", "body": {"id": 1}}))
        await pub_writer.drain()

        first_delivery = await asyncio.wait_for(read_frame(sub_reader), timeout=2)
        assert first_delivery["body"] == {"id": 1}
        # Deliberately not acking.

        redelivery = await asyncio.wait_for(read_frame(sub_reader), timeout=2)
        assert redelivery == first_delivery

        for w in (sub_writer, pub_writer):
            w.close()
            await w.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_acked_message_is_not_redelivered():
    registry = TopicRegistry(ack_tracker=AckTracker(timeout=ACK_TIMEOUT))
    server = await run_server(host="127.0.0.1", port=0, registry=registry)
    addr = server.sockets[0].getsockname()

    try:
        sub_reader, sub_writer = await _connect(addr)
        sub_writer.write(encode_frame({"type": "SUBSCRIBE", "topic": "orders"}))
        await sub_writer.drain()

        pub_reader, pub_writer = await _connect(addr)
        pub_writer.write(encode_frame({"type": "PUBLISH", "topic": "orders", "body": {"id": 1}}))
        await pub_writer.drain()

        delivered = await asyncio.wait_for(read_frame(sub_reader), timeout=2)
        sub_writer.write(encode_frame({"type": "ACK", "message_id": delivered["message_id"]}))
        await sub_writer.drain()

        # Give the ack a moment to be processed, then confirm nothing more
        # arrives within a window comfortably past the ack timeout.
        await asyncio.sleep(PAST_ACK_TIMEOUT)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(read_frame(sub_reader), timeout=PAST_ACK_TIMEOUT)

        for w in (sub_writer, pub_writer):
            w.close()
            await w.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_disconnected_subscriber_is_not_redelivered_to():
    """Ensures forget_queue() is actually wired into connection close: an
    unacked message must not keep being pushed into a closed connection's
    queue forever."""
    registry = TopicRegistry(ack_tracker=AckTracker(timeout=ACK_TIMEOUT))
    server = await run_server(host="127.0.0.1", port=0, registry=registry)
    addr = server.sockets[0].getsockname()

    try:
        sub_reader, sub_writer = await _connect(addr)
        sub_writer.write(encode_frame({"type": "SUBSCRIBE", "topic": "orders"}))
        await sub_writer.drain()

        pub_reader, pub_writer = await _connect(addr)
        pub_writer.write(encode_frame({"type": "PUBLISH", "topic": "orders", "body": {"id": 1}}))
        await pub_writer.drain()

        await asyncio.wait_for(read_frame(sub_reader), timeout=2)  # received, not acked
        sub_writer.close()
        await sub_writer.wait_closed()

        # Give the server a moment to notice the disconnect and clean up,
        # same pattern as the Milestone 2 disconnect test.
        await asyncio.sleep(0.1)

        # If forget_queue() weren't wired in, the ack timer would still be
        # armed and would try to redeliver into the now-closed connection's
        # queue forever. Nothing observable to assert on the client side
        # (the connection is closed) -- this test's real job is to run
        # cleanly to completion without hanging or raising.
        await asyncio.sleep(PAST_ACK_TIMEOUT)

        pub_writer.close()
        await pub_writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_reconnecting_with_same_consumer_id_recovers_pending_message():
    """The real end-to-end proof for Milestone 5: a consumer identifies
    itself, receives a message, disconnects without acking, and a NEW
    connection claiming the same consumer_id gets that message without it
    ever being republished."""
    registry = TopicRegistry(ack_tracker=AckTracker(timeout=ACK_TIMEOUT))
    server = await run_server(host="127.0.0.1", port=0, registry=registry)
    addr = server.sockets[0].getsockname()

    try:
        first_reader, first_writer = await _connect(addr)
        first_writer.write(encode_frame({"type": "SUBSCRIBE", "topic": "orders", "consumer_id": "worker-1"}))
        await first_writer.drain()

        pub_reader, pub_writer = await _connect(addr)
        pub_writer.write(encode_frame({"type": "PUBLISH", "topic": "orders", "body": {"id": 1}}))
        await pub_writer.drain()

        delivered = await asyncio.wait_for(read_frame(first_reader), timeout=2)
        assert delivered["body"] == {"id": 1}

        # Disconnect without acking, then reconnect as the same consumer.
        first_writer.close()
        await first_writer.wait_closed()
        await asyncio.sleep(0.1)  # let the server notice the disconnect

        second_reader, second_writer = await _connect(addr)
        second_writer.write(encode_frame({"type": "SUBSCRIBE", "topic": "orders", "consumer_id": "worker-1"}))
        await second_writer.drain()

        recovered = await asyncio.wait_for(read_frame(second_reader), timeout=2)
        assert recovered["body"] == {"id": 1}
        assert recovered["message_id"] == delivered["message_id"]

        for w in (second_writer, pub_writer):
            w.close()
            await w.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_consumer_group_splits_work_across_real_connections():
    registry = TopicRegistry()
    server = await run_server(host="127.0.0.1", port=0, registry=registry)
    addr = server.sockets[0].getsockname()

    try:
        worker_a_reader, worker_a_writer = await _connect(addr)
        worker_a_writer.write(encode_frame({"type": "SUBSCRIBE", "topic": "jobs", "group_id": "workers"}))
        await worker_a_writer.drain()

        worker_b_reader, worker_b_writer = await _connect(addr)
        worker_b_writer.write(encode_frame({"type": "SUBSCRIBE", "topic": "jobs", "group_id": "workers"}))
        await worker_b_writer.drain()

        pub_reader, pub_writer = await _connect(addr)
        for i in range(4):
            pub_writer.write(encode_frame({"type": "PUBLISH", "topic": "jobs", "body": {"id": i}}))
            await pub_writer.drain()

        received_by_a = [(await asyncio.wait_for(read_frame(worker_a_reader), timeout=2))["body"]["id"] for _ in range(2)]
        received_by_b = [(await asyncio.wait_for(read_frame(worker_b_reader), timeout=2))["body"]["id"] for _ in range(2)]

        assert set(received_by_a) | set(received_by_b) == {0, 1, 2, 3}
        assert set(received_by_a).isdisjoint(received_by_b)

        for w in (worker_a_writer, worker_b_writer, pub_writer):
            w.close()
            await w.wait_closed()
    finally:
        server.close()
        await server.wait_closed()
