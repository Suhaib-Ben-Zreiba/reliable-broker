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

Redelivery targets the SAME queue a message was originally delivered to,
UNLESS that queue is explicitly reassigned via reassign() -- which is what
Milestone 5 uses to move a reconnecting, identified consumer's outstanding
work from its old (dead) connection queue onto its new one.
"""

from __future__ import annotations

import asyncio


class AckTracker:
    def __init__(self, timeout: float = 5.0) -> None:
        self._timeout = timeout
        # key -> (timer handle, the message itself). The message is kept
        # alongside the handle, not just captured in call_later's closure,
        # specifically so reassign() can read it back out and redeliver it
        # to a different queue.
        self._pending: dict[tuple[asyncio.Queue, str], tuple[asyncio.TimerHandle, dict]] = {}

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
            existing[0].cancel()
        loop = asyncio.get_running_loop()
        handle = loop.call_later(self._timeout, self._redeliver, queue, message)
        self._pending[key] = (handle, message)

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
        entry = self._pending.pop(key, None)
        if entry is None:
            return False
        entry[0].cancel()
        return True

    def forget_queue(self, queue: asyncio.Queue) -> None:
        """Cancel and drop every pending delivery for queue, with no
        chance of recovery. Used for an anonymous (no consumer_id)
        connection closing -- there is no identity to reconnect as, so
        there is nothing to preserve."""
        dead_keys = [key for key in self._pending if key[0] is queue]
        for key in dead_keys:
            handle, _ = self._pending.pop(key)
            handle.cancel()

    def reassign(self, old_queue: asyncio.Queue, new_queue: asyncio.Queue) -> int:
        """Move every outstanding delivery for old_queue onto new_queue:
        redeliver each immediately and re-arm its timeout there. Used when
        a consumer identified by a stable consumer_id reconnects.

        old_queue's pending entries are the authoritative record of what's
        still outstanding for it. Whatever might already be sitting
        unread in old_queue's own buffer (from a redelivery that fired
        while the consumer was offline) is a stale duplicate of the same
        messages this moves over, and is simply abandoned along with
        old_queue itself -- nothing will ever read old_queue again once
        its owning connection is gone.

        Returns the number of deliveries moved.
        """
        keys_to_move = [key for key in self._pending if key[0] is old_queue]
        for key in keys_to_move:
            handle, message = self._pending.pop(key)
            handle.cancel()
            new_queue.put_nowait(message)
            self.record_delivery(new_queue, message)
        return len(keys_to_move)

    def pending_count(self) -> int:
        return len(self._pending)
