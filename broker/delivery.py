"""Tracks messages delivered but not yet acknowledged, and redelivers them
after a per-message timeout.

Uses one asyncio timer per outstanding delivery (via loop.call_later)
rather than a periodic sweep that wakes up and checks everything. A sweep
needs a background task started when the broker starts and explicitly
cancelled when it stops -- one more thing to leak if that lifecycle isn't
managed carefully, the same category of bug as the asyncio.gather() task
leak documented for Milestone 2. A per-message timer is owned entirely by
the event loop: it fires exactly at its deadline (not up to one sweep
interval late), and acking a message is a single handle.cancel() call.
AckTracker itself needs no start()/stop() as a result.

Redelivery targets the SAME queue a message was originally delivered to.
Redelivering to a different connection after the original one disconnects
is a consumer-identity problem -- that's Milestone 5's reconnect handling,
not this one. For now, forget_queue() exists specifically so a closed
connection's outstanding deliveries stop being redelivered into a queue
nobody will ever drain again.
"""

from __future__ import annotations

import asyncio


class AckTracker:
    def __init__(self, timeout: float = 5.0) -> None:
        self._timeout = timeout
        self._pending: dict[tuple[asyncio.Queue, str], asyncio.TimerHandle] = {}

    def record_delivery(self, queue: asyncio.Queue, message: dict) -> None:
        """Start (or restart) the redelivery timer for this message on this
        queue. Called once per initial delivery, and again by _redeliver()
        each time a timeout fires, so an unacked message keeps being
        retried indefinitely rather than only once. There is currently no
        maximum retry count or dead-letter handling -- that's a documented
        future improvement, not an oversight.
        """
        key = (queue, message["message_id"])
        existing = self._pending.get(key)
        if existing is not None:
            existing.cancel()
        loop = asyncio.get_running_loop()
        self._pending[key] = loop.call_later(self._timeout, self._redeliver, queue, message)

    def _redeliver(self, queue: asyncio.Queue, message: dict) -> None:
        key = (queue, message["message_id"])
        self._pending.pop(key, None)
        queue.put_nowait(message)
        self.record_delivery(queue, message)

    def ack(self, queue: asyncio.Queue, message_id: str) -> bool:
        """Cancel the pending redelivery timer for (queue, message_id).
        Returns False for an unknown or already-acked id rather than
        raising -- a late or duplicate ACK arriving after redelivery
        already happened is a normal race, not an error condition."""
        key = (queue, message_id)
        handle = self._pending.pop(key, None)
        if handle is None:
            return False
        handle.cancel()
        return True

    def forget_queue(self, queue: asyncio.Queue) -> None:
        dead_keys = [key for key in self._pending if key[0] is queue]
        for key in dead_keys:
            self._pending.pop(key).cancel()

    def pending_count(self) -> int:
        return len(self._pending)
