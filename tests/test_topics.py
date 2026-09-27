"""Unit tests for the topic registry. No sockets or asyncio streams here --
these exercise fan-out, persistence, and ack-tracking wiring directly, per
the isolation approach used in test_protocol.py."""

import asyncio

import pytest

from broker.delivery import AckTracker
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


# --- Acknowledgement wiring: the actual point of Milestone 4 -----------


TIMEOUT = 0.1
PAST_TIMEOUT = TIMEOUT * 1.5


@pytest.mark.asyncio
async def test_publish_registers_delivery_with_ack_tracker():
    tracker = AckTracker(timeout=TIMEOUT)
    registry = TopicRegistry(ack_tracker=tracker)
    queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue)

    await registry.publish("orders", {"id": 1})

    assert tracker.pending_count() == 1


@pytest.mark.asyncio
async def test_unacked_publish_is_redelivered_through_the_full_registry():
    tracker = AckTracker(timeout=TIMEOUT)
    registry = TopicRegistry(ack_tracker=tracker)
    queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue)

    await registry.publish("orders", {"id": 1})
    queue.get_nowait()  # simulate the consumer receiving it, but not acking

    await asyncio.sleep(PAST_TIMEOUT)

    assert queue.qsize() == 1
    assert queue.get_nowait()["body"] == {"id": 1}


@pytest.mark.asyncio
async def test_replay_registers_delivery_with_ack_tracker(tmp_path):
    tracker = AckTracker(timeout=TIMEOUT)
    store = LogStore(tmp_path)
    registry = TopicRegistry(store=store, ack_tracker=tracker)
    await registry.publish("orders", {"id": 1})

    queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue)
    registry.replay("orders", queue)

    assert tracker.pending_count() == 1


@pytest.mark.asyncio
async def test_unsubscribe_all_forgets_pending_acks_for_that_queue():
    tracker = AckTracker(timeout=TIMEOUT)
    registry = TopicRegistry(ack_tracker=tracker)
    queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue)
    await registry.publish("orders", {"id": 1})

    registry.unsubscribe_all(queue)

    assert tracker.pending_count() == 0
    await asyncio.sleep(PAST_TIMEOUT)
    assert queue.qsize() == 1  # the original delivery, not a redelivery


# --- Consumer identity and reconnect: Milestone 5 ----------------------


@pytest.mark.asyncio
async def test_disconnecting_an_anonymous_queue_still_forgets_pending_acks():
    """Regression guard: a plain subscriber with no consumer_id must keep
    behaving exactly like Milestone 4 -- disconnect forgets it completely,
    there is no reconnect story for an unidentified subscriber."""
    tracker = AckTracker(timeout=TIMEOUT)
    registry = TopicRegistry(ack_tracker=tracker)
    queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue)
    await registry.publish("orders", {"id": 1})

    registry.unsubscribe_all(queue)

    assert tracker.pending_count() == 0


@pytest.mark.asyncio
async def test_identified_consumer_disconnect_preserves_pending_acks():
    tracker = AckTracker(timeout=TIMEOUT)
    registry = TopicRegistry(ack_tracker=tracker)
    queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue, consumer_id="worker-1")
    await registry.publish("orders", {"id": 1})

    registry.unsubscribe_all(queue)

    # Unlike an anonymous disconnect, this must NOT be forgotten -- it's
    # waiting for worker-1 to reconnect and claim it.
    assert tracker.pending_count() == 1


@pytest.mark.asyncio
async def test_reconnect_with_same_consumer_id_reassigns_pending_work():
    tracker = AckTracker(timeout=TIMEOUT)
    registry = TopicRegistry(ack_tracker=tracker)
    old_queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", old_queue, consumer_id="worker-1")
    await registry.publish("orders", {"id": 1})
    old_queue.get_nowait()  # received, not acked, then "worker-1" disconnects
    registry.unsubscribe_all(old_queue)

    new_queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", new_queue, consumer_id="worker-1")

    # The message that was outstanding for worker-1 arrives on its new
    # connection without needing to be republished.
    assert new_queue.get_nowait()["body"] == {"id": 1}
    assert old_queue.qsize() == 0


@pytest.mark.asyncio
async def test_reconnected_consumer_still_receives_new_broadcasts():
    registry = TopicRegistry()
    old_queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", old_queue, consumer_id="worker-1")
    registry.unsubscribe_all(old_queue)

    new_queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", new_queue, consumer_id="worker-1")

    delivered = await registry.publish("orders", {"id": 2})

    assert delivered == 1
    assert new_queue.get_nowait()["body"] == {"id": 2}


# --- Consumer groups: competing consumers, Milestone 5 ------------------


@pytest.mark.asyncio
async def test_group_members_split_messages_round_robin():
    registry = TopicRegistry()
    queue_a: asyncio.Queue = asyncio.Queue()
    queue_b: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue_a, group_id="workers")
    registry.subscribe("orders", queue_b, group_id="workers")

    for i in range(4):
        await registry.publish("orders", {"id": i})

    # Round robin over 2 members and 4 messages: each gets exactly 2, and
    # nobody gets the same message as the other (no duplication).
    a_ids = {queue_a.get_nowait()["body"]["id"] for _ in range(2)}
    b_ids = {queue_b.get_nowait()["body"]["id"] for _ in range(2)}
    assert a_ids | b_ids == {0, 1, 2, 3}
    assert a_ids.isdisjoint(b_ids)
    assert queue_a.qsize() == 0
    assert queue_b.qsize() == 0


@pytest.mark.asyncio
async def test_group_members_do_not_also_receive_broadcast():
    registry = TopicRegistry()
    grouped: asyncio.Queue = asyncio.Queue()
    broadcast: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", grouped, group_id="workers")
    registry.subscribe("orders", broadcast)  # plain, ungrouped

    delivered = await registry.publish("orders", {"id": 1})

    # One copy to the group member, one to the broadcast subscriber -- not
    # the group member getting it twice via both paths.
    assert delivered == 2
    assert grouped.qsize() == 1
    assert broadcast.qsize() == 1


@pytest.mark.asyncio
async def test_different_groups_on_the_same_topic_each_get_their_own_copy():
    """Two independent worker pools consuming the same topic -- each pool
    processes every message once, same as Kafka consumer groups."""
    registry = TopicRegistry()
    pool_a: asyncio.Queue = asyncio.Queue()
    pool_b: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", pool_a, group_id="pool-a")
    registry.subscribe("orders", pool_b, group_id="pool-b")

    delivered = await registry.publish("orders", {"id": 1})

    assert delivered == 2
    assert pool_a.get_nowait()["body"] == {"id": 1}
    assert pool_b.get_nowait()["body"] == {"id": 1}


@pytest.mark.asyncio
async def test_group_subscription_does_not_replay_history(tmp_path):
    """Deliberate limitation, documented in topics.py and PROJECT_LOG.md:
    splitting historical backlog correctly across group members would
    need per-member offset tracking, which is out of scope here."""
    registry = TopicRegistry(store=LogStore(tmp_path))
    await registry.publish("orders", {"id": 1})

    queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue, group_id="workers")

    assert queue.qsize() == 0
