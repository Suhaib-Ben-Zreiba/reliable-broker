"""In-memory subscriber tracking plus durable, acknowledged fan-out, backed
by storage.py and delivery.py.

Deliberately has no dependency on sockets or asyncio streams -- a
subscriber is represented by an asyncio.Queue that the connection layer
drains and writes to the network. That separation is what lets fan-out,
replay, and ack-tracking logic be unit tested without opening a single
socket, the same way protocol.py is tested independently of server.py.
"""

from __future__ import annotations

import asyncio
import itertools

from broker.delivery import AckTracker
from broker.storage import LogStore


class TopicRegistry:
    def __init__(self, store: LogStore | None = None, ack_tracker: AckTracker | None = None) -> None:
        self._subscribers: dict[str, set[asyncio.Queue]] = {}
        self._store = store
        self.ack_tracker = ack_tracker
        start = (store.max_message_id_seen() + 1) if store is not None else 1
        self._message_ids = itertools.count(start)

    def subscribe(self, topic: str, queue: asyncio.Queue) -> None:
        self._subscribers.setdefault(topic, set()).add(queue)

    def replay(self, topic: str, queue: asyncio.Queue) -> int:
        """Push every historical message for topic onto queue.

        Deliberately synchronous (no `await` anywhere in this call): the
        server calls subscribe() immediately followed by replay() with no
        `await` between them, which makes "start receiving live messages"
        and "receive everything published before I subscribed" atomic with
        respect to other connections. If this used async file I/O, a
        PUBLISH from another connection could land in the gap and either
        be missed or be delivered twice (once live, once in a
        since-updated history read). See docs/PROJECT_LOG.md for the full
        reasoning.

        Replayed messages are tracked for acknowledgement exactly like a
        live delivery -- there is no reason a message a consumer missed
        the first time around should be exempt from the same at-least-once
        guarantee everything else gets.
        """
        if self._store is None:
            return 0
        records = self._store.read_all(topic)
        for record in records:
            queue.put_nowait(record)
            if self.ack_tracker is not None:
                self.ack_tracker.record_delivery(queue, record)
        return len(records)

    def unsubscribe(self, topic: str, queue: asyncio.Queue) -> None:
        subscribers = self._subscribers.get(topic)
        if subscribers:
            subscribers.discard(queue)

    def unsubscribe_all(self, queue: asyncio.Queue) -> None:
        for subscribers in self._subscribers.values():
            subscribers.discard(queue)
        if self.ack_tracker is not None:
            self.ack_tracker.forget_queue(queue)

    def subscriber_count(self, topic: str) -> int:
        return len(self._subscribers.get(topic, ()))

    async def publish(self, topic: str, body) -> int:
        """Persist the message durably, then fan it out to current live
        subscribers, registering each delivery for acknowledgement.

        Persistence happens first and can raise (disk full, permission
        error) before any subscriber is touched, so a failed publish never
        partially delivers. Still declared `async def` for API stability
        (a future storage backend might need real async I/O), but nothing
        in this implementation currently awaits, which is exactly what
        keeps it safe to call from within replay()'s atomic window.
        """
        message = {
            "type": "MESSAGE",
            "topic": topic,
            "message_id": f"m-{next(self._message_ids)}",
            "body": body,
        }
        if self._store is not None:
            self._store.append(topic, message)

        subscribers = self._subscribers.get(topic, ())
        for queue in subscribers:
            queue.put_nowait(message)
            if self.ack_tracker is not None:
                self.ack_tracker.record_delivery(queue, message)
        return len(subscribers)
