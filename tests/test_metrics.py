"""Unit tests for build_snapshot(). No sockets or HTTP here -- pure
function over TopicRegistry's own bookkeeping, same isolation pattern used
throughout this project."""

import asyncio

import pytest

from broker.delivery import AckTracker
from broker.metrics import build_snapshot
from broker.topics import TopicRegistry


def test_snapshot_of_empty_registry():
    registry = TopicRegistry()

    snapshot = build_snapshot(registry)

    assert snapshot == {"connected_clients": 0, "pending_acks": 0, "topics": {}}


@pytest.mark.asyncio
async def test_snapshot_reflects_publish_counts_and_subscribers():
    registry = TopicRegistry()
    queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue)
    await registry.publish("orders", {"id": 1})
    await registry.publish("orders", {"id": 2})

    snapshot = build_snapshot(registry)

    assert snapshot["topics"]["orders"]["messages_published"] == 2
    assert snapshot["topics"]["orders"]["broadcast_subscribers"] == 1
    assert snapshot["topics"]["orders"]["groups"] == {}


@pytest.mark.asyncio
async def test_snapshot_reflects_group_membership():
    registry = TopicRegistry()
    queue_a: asyncio.Queue = asyncio.Queue()
    queue_b: asyncio.Queue = asyncio.Queue()
    registry.subscribe("jobs", queue_a, group_id="workers")
    registry.subscribe("jobs", queue_b, group_id="workers")

    snapshot = build_snapshot(registry)

    assert snapshot["topics"]["jobs"]["groups"] == {"workers": 2}
    assert snapshot["topics"]["jobs"]["broadcast_subscribers"] == 0


@pytest.mark.asyncio
async def test_snapshot_reflects_pending_acks_and_connected_clients():
    tracker = AckTracker(timeout=5.0)
    registry = TopicRegistry(ack_tracker=tracker)
    registry.active_connections = 3
    queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue)
    await registry.publish("orders", {"id": 1})

    snapshot = build_snapshot(registry)

    assert snapshot["connected_clients"] == 3
    assert snapshot["pending_acks"] == 1


@pytest.mark.asyncio
async def test_known_topics_includes_a_topic_with_no_current_subscriber():
    """A topic that was published to but never had (or no longer has) a
    subscriber should still show up -- it's not gone, just quiet."""
    registry = TopicRegistry()

    await registry.publish("orders", {"id": 1})  # nobody is subscribed

    snapshot = build_snapshot(registry)

    assert "orders" in snapshot["topics"]
    assert snapshot["topics"]["orders"]["messages_published"] == 1
    assert snapshot["topics"]["orders"]["broadcast_subscribers"] == 0
