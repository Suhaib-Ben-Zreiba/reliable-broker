"""Unit tests for AckTracker, isolated from TopicRegistry and the network
layer -- same isolation approach as the other broker/*.py modules.

Uses real, very short timeouts (tens of milliseconds) and real
asyncio.sleep rather than a mocked clock, because the whole mechanism
being tested is loop.call_later's real interaction with the running event
loop -- mocking that away would test something other than the real thing.
"""

import asyncio

import pytest

from broker.delivery import AckTracker

TIMEOUT = 0.1
# Enough margin past one timeout to reliably observe that redelivery fired,
# but comfortably less than 2x TIMEOUT -- redelivery re-arms itself, so a
# window that long would let a *second* redelivery cycle fire too and make
# these tests wrong about what they're asserting, not just flaky.
PAST_TIMEOUT = TIMEOUT * 1.5
# For tests that sleep twice in a row to observe two separate redelivery
# cycles: a smaller margin, because two back-to-back PAST_TIMEOUT sleeps
# would compound past the second timer's own re-arm and risk catching a
# third redelivery too. This one is deliberately tighter, not just smaller.
CHAINED_MARGIN = TIMEOUT * 1.2


@pytest.mark.asyncio
async def test_ack_before_timeout_prevents_redelivery():
    tracker = AckTracker(timeout=TIMEOUT)
    queue: asyncio.Queue = asyncio.Queue()
    message = {"message_id": "m-1", "body": {}}

    tracker.record_delivery(queue, message)
    assert tracker.ack(queue, "m-1") is True

    await asyncio.sleep(PAST_TIMEOUT)

    assert queue.qsize() == 0
    assert tracker.pending_count() == 0


@pytest.mark.asyncio
async def test_unacked_message_is_redelivered_after_timeout():
    tracker = AckTracker(timeout=TIMEOUT)
    queue: asyncio.Queue = asyncio.Queue()
    message = {"message_id": "m-1", "body": {"id": 1}}

    tracker.record_delivery(queue, message)

    await asyncio.sleep(PAST_TIMEOUT)

    assert queue.qsize() == 1
    assert queue.get_nowait() == message
    # Still pending: an unacked message keeps being retried, not given up on.
    assert tracker.pending_count() == 1


@pytest.mark.asyncio
async def test_redelivery_repeats_until_acked():
    tracker = AckTracker(timeout=TIMEOUT)
    queue: asyncio.Queue = asyncio.Queue()
    message = {"message_id": "m-1", "body": {}}

    tracker.record_delivery(queue, message)
    await asyncio.sleep(PAST_TIMEOUT)
    queue.get_nowait()  # first redelivery

    await asyncio.sleep(CHAINED_MARGIN)
    assert queue.qsize() == 1  # redelivered a second time

    tracker.ack(queue, "m-1")
    queue.get_nowait()
    await asyncio.sleep(PAST_TIMEOUT)
    assert queue.qsize() == 0  # acked: no third redelivery


@pytest.mark.asyncio
async def test_forget_queue_cancels_pending_redelivery():
    tracker = AckTracker(timeout=TIMEOUT)
    queue: asyncio.Queue = asyncio.Queue()
    message = {"message_id": "m-1", "body": {}}

    tracker.record_delivery(queue, message)
    tracker.forget_queue(queue)

    await asyncio.sleep(PAST_TIMEOUT)

    assert queue.qsize() == 0
    assert tracker.pending_count() == 0


def test_ack_on_unknown_message_id_returns_false():
    tracker = AckTracker(timeout=TIMEOUT)
    queue: asyncio.Queue = asyncio.Queue()

    assert tracker.ack(queue, "never-delivered") is False


@pytest.mark.asyncio
async def test_multiple_unacked_messages_redeliver_independently():
    tracker = AckTracker(timeout=TIMEOUT)
    queue: asyncio.Queue = asyncio.Queue()
    message_a = {"message_id": "m-1", "body": {}}
    message_b = {"message_id": "m-2", "body": {}}

    tracker.record_delivery(queue, message_a)
    tracker.record_delivery(queue, message_b)
    tracker.ack(queue, "m-1")

    await asyncio.sleep(PAST_TIMEOUT)

    remaining = [queue.get_nowait() for _ in range(queue.qsize())]
    assert remaining == [message_b]


@pytest.mark.asyncio
async def test_forget_queue_only_affects_that_queue():
    tracker = AckTracker(timeout=TIMEOUT)
    queue_a: asyncio.Queue = asyncio.Queue()
    queue_b: asyncio.Queue = asyncio.Queue()
    tracker.record_delivery(queue_a, {"message_id": "m-1", "body": {}})
    tracker.record_delivery(queue_b, {"message_id": "m-2", "body": {}})

    tracker.forget_queue(queue_a)

    await asyncio.sleep(PAST_TIMEOUT)

    assert queue_a.qsize() == 0
    assert queue_b.qsize() == 1


# --- reassign(): the actual point of Milestone 5's reconnect handling --


def test_reassign_with_nothing_pending_is_a_noop():
    tracker = AckTracker(timeout=TIMEOUT)
    old_queue: asyncio.Queue = asyncio.Queue()
    new_queue: asyncio.Queue = asyncio.Queue()

    moved = tracker.reassign(old_queue, new_queue)

    assert moved == 0
    assert new_queue.qsize() == 0


@pytest.mark.asyncio
async def test_reassign_redelivers_immediately_to_new_queue():
    tracker = AckTracker(timeout=TIMEOUT)
    old_queue: asyncio.Queue = asyncio.Queue()
    new_queue: asyncio.Queue = asyncio.Queue()
    message = {"message_id": "m-1", "body": {"id": 1}}
    tracker.record_delivery(old_queue, message)

    moved = tracker.reassign(old_queue, new_queue)

    assert moved == 1
    assert new_queue.get_nowait() == message
    assert old_queue.qsize() == 0


@pytest.mark.asyncio
async def test_reassign_cancels_the_old_queues_timer():
    tracker = AckTracker(timeout=TIMEOUT)
    old_queue: asyncio.Queue = asyncio.Queue()
    new_queue: asyncio.Queue = asyncio.Queue()
    tracker.record_delivery(old_queue, {"message_id": "m-1", "body": {}})

    tracker.reassign(old_queue, new_queue)
    new_queue.get_nowait()  # drain the immediate redelivery from reassign itself

    await asyncio.sleep(PAST_TIMEOUT)

    # The message keeps retrying, but only on new_queue -- old_queue must
    # never receive anything again once it's been reassigned away from.
    assert old_queue.qsize() == 0
    assert new_queue.qsize() == 1


@pytest.mark.asyncio
async def test_reassign_moves_multiple_pending_messages():
    tracker = AckTracker(timeout=TIMEOUT)
    old_queue: asyncio.Queue = asyncio.Queue()
    new_queue: asyncio.Queue = asyncio.Queue()
    tracker.record_delivery(old_queue, {"message_id": "m-1", "body": {}})
    tracker.record_delivery(old_queue, {"message_id": "m-2", "body": {}})

    moved = tracker.reassign(old_queue, new_queue)

    assert moved == 2
    ids = {new_queue.get_nowait()["message_id"] for _ in range(2)}
    assert ids == {"m-1", "m-2"}
