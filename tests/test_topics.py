"""Unit tests for the topic registry. No sockets or asyncio streams here --
these exercise fan-out and persistence logic directly, per the isolation
approach used in test_protocol.py."""

import asyncio

import pytest

from broker.storage import LogStore
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


# --- Persistence: the actual point of Milestone 3 ---------------------


@pytest.mark.asyncio
async def test_publish_with_no_subscribers_is_still_persisted(tmp_path):
    """This is the exact Milestone 2 gap Milestone 3 exists to close:
    a message published to a topic with nobody subscribed used to be
    dropped forever. Now it must still be on disk afterward."""
    store = LogStore(tmp_path)
    registry = TopicRegistry(store=store)

    delivered = await registry.publish("orders", {"id": 1})

    assert delivered == 0  # still true: no *live* subscriber got it
    assert store.read_all("orders") == [
        {"type": "MESSAGE", "topic": "orders", "message_id": "m-1", "body": {"id": 1}}
    ]


@pytest.mark.asyncio
async def test_subscribe_replays_history_published_before_subscription(tmp_path):
    store = LogStore(tmp_path)
    registry = TopicRegistry(store=store)
    await registry.publish("orders", {"id": 1})
    await registry.publish("orders", {"id": 2})

    queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue)
    replayed = registry.replay("orders", queue)

    assert replayed == 2
    assert queue.get_nowait()["body"] == {"id": 1}
    assert queue.get_nowait()["body"] == {"id": 2}


@pytest.mark.asyncio
async def test_registry_survives_simulated_restart(tmp_path):
    """A new TopicRegistry backed by the same data directory must see
    everything an earlier instance wrote, and must not reuse message ids
    already on disk."""
    store = LogStore(tmp_path)
    first_registry = TopicRegistry(store=store)
    await first_registry.publish("orders", {"id": 1})
    await first_registry.publish("orders", {"id": 2})

    second_registry = TopicRegistry(store=LogStore(tmp_path))
    queue: asyncio.Queue = asyncio.Queue()
    second_registry.subscribe("orders", queue)
    second_registry.replay("orders", queue)

    assert queue.get_nowait()["message_id"] == "m-1"
    assert queue.get_nowait()["message_id"] == "m-2"

    # A publish on the "restarted" broker must not collide with ids
    # already written by the instance before the "restart".
    await second_registry.publish("orders", {"id": 3})
    third = queue.get_nowait()
    assert third["message_id"] == "m-3"


@pytest.mark.asyncio
async def test_registry_without_a_store_still_works_in_memory_only():
    """Backward-compatible with Milestone 2 usage: TopicRegistry() with no
    store still does in-memory fan-out, just without durability."""
    registry = TopicRegistry()
    queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue)

    delivered = await registry.publish("orders", {"id": 1})

    assert delivered == 1
    assert registry.replay("orders", asyncio.Queue()) == 0
