"""Unit tests for the in-memory topic registry. No sockets or asyncio
streams here -- these exercise fan-out logic directly, per the isolation
approach used in test_protocol.py."""

import asyncio

import pytest

from broker.topics import TopicRegistry


@pytest.mark.asyncio
async def test_publish_with_no_subscribers_delivers_to_nobody():
    registry = TopicRegistry()

    delivered = await registry.publish("orders", {"id": 1})

    assert delivered == 0


@pytest.mark.asyncio
async def test_single_subscriber_receives_published_message():
    registry = TopicRegistry()
    queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue)

    delivered = await registry.publish("orders", {"id": 1})

    assert delivered == 1
    message = queue.get_nowait()
    assert message["type"] == "MESSAGE"
    assert message["topic"] == "orders"
    assert message["body"] == {"id": 1}
    assert "message_id" in message


@pytest.mark.asyncio
async def test_multiple_subscribers_all_receive_the_same_publish():
    registry = TopicRegistry()
    queue_a: asyncio.Queue = asyncio.Queue()
    queue_b: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue_a)
    registry.subscribe("orders", queue_b)

    delivered = await registry.publish("orders", {"id": 1})

    assert delivered == 2
    assert queue_a.get_nowait()["body"] == {"id": 1}
    assert queue_b.get_nowait()["body"] == {"id": 1}


@pytest.mark.asyncio
async def test_subscriber_on_different_topic_does_not_receive_message():
    registry = TopicRegistry()
    orders_queue: asyncio.Queue = asyncio.Queue()
    shipping_queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", orders_queue)
    registry.subscribe("shipping", shipping_queue)

    await registry.publish("orders", {"id": 1})

    assert orders_queue.qsize() == 1
    assert shipping_queue.qsize() == 0


@pytest.mark.asyncio
async def test_unsubscribe_stops_further_delivery():
    registry = TopicRegistry()
    queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue)
    registry.unsubscribe("orders", queue)

    delivered = await registry.publish("orders", {"id": 1})

    assert delivered == 0


@pytest.mark.asyncio
async def test_unsubscribe_all_removes_from_every_topic():
    registry = TopicRegistry()
    queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue)
    registry.subscribe("shipping", queue)

    registry.unsubscribe_all(queue)

    assert await registry.publish("orders", {"id": 1}) == 0
    assert await registry.publish("shipping", {"id": 1}) == 0


@pytest.mark.asyncio
async def test_each_delivery_gets_a_unique_message_id():
    registry = TopicRegistry()
    queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue)

    await registry.publish("orders", {"id": 1})
    await registry.publish("orders", {"id": 2})

    first = queue.get_nowait()
    second = queue.get_nowait()
    assert first["message_id"] != second["message_id"]
