"""In-memory subscriber tracking plus durable, acknowledged, identity-aware
fan-out, backed by storage.py and delivery.py.

Deliberately has no dependency on sockets or asyncio streams -- a
subscriber is represented by an asyncio.Queue that the connection layer
drains and writes to the network. That separation is what lets fan-out,
replay, ack-tracking, and group/reconnect logic be unit tested without
opening a single socket, the same way protocol.py is tested independently
of server.py.

Two delivery modes coexist on a topic:

- Broadcast (no group_id): every subscribed queue gets every message.
  This is the pub/sub behavior from Milestone 2, unchanged.
- Consumer group (group_id given): the queues sharing a group_id on a
  topic compete for messages -- each publish goes to exactly one member,
  round-robin. This is the "competing consumers" pattern real message
  queues use to spread work across a pool of workers, distinct from
  broadcast fan-out.

Grouped subscriptions do NOT receive historical replay. Splitting a
topic's *history* correctly across group members would require tracking,
per member, which historical messages it has already seen -- a real
feature (Kafka calls this partition offset assignment) that is out of
scope here. A consumer joining a group only sees messages published from
the moment it joins onward. This is a deliberate, documented limitation,
not an oversight.
"""

from __future__ import annotations

import asyncio
import itertools

from broker.delivery import AckTracker
from broker.storage import LogStore


class TopicRegistry:
    def __init__(self, store: LogStore | None = None, ack_tracker: AckTracker | None = None) -> None:
        self._subscribers: dict[str, set[asyncio.Queue]] = {}
        self._groups: dict[tuple[str, str], list[asyncio.Queue]] = {}
        self._group_next_index: dict[tuple[str, str], int] = {}
        self._consumer_queues: dict[str, asyncio.Queue] = {}
        self._store = store
        self.ack_tracker = ack_tracker
        start = (store.max_message_id_seen() + 1) if store is not None else 1
        self._message_ids = itertools.count(start)

    def subscribe(
        self,
        topic: str,
        queue: asyncio.Queue,
        *,
        consumer_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        if group_id is not None:
            members = self._groups.setdefault((topic, group_id), [])
            if queue not in members:
                members.append(queue)
        else:
            self._subscribers.setdefault(topic, set()).add(queue)

        if consumer_id is not None:
            old_queue = self._consumer_queues.get(consumer_id)
            if old_queue is not None and old_queue is not queue and self.ack_tracker is not None:
                # This consumer_id was already associated with a different
                # (now presumably dead) queue -- a reconnect. Move whatever
                # work was still outstanding for it onto the new queue
                # rather than letting it be lost or stuck redelivering into
                # a connection nobody is reading anymore.
                self.ack_tracker.reassign(old_queue, queue)
            self._consumer_queues[consumer_id] = queue

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
        """Called when a connection closes.

        An anonymous queue (no consumer_id ever associated with it) is
        forgotten completely, including its pending acks -- there is no
        identity for it to reconnect as, so nothing can ever claim that
        outstanding work. An identified queue (a consumer_id points at it)
        is removed from live delivery -- it must not receive any *new*
        broadcasts or group turns while disconnected -- but its pending
        acks are deliberately left alone. They are only resolved later,
        either by the same consumer_id reconnecting (subscribe() then
        moves them via AckTracker.reassign) or, if it never reconnects, by
        retrying forever into a queue nobody reads -- a real, documented
        limitation; there is no expiry/grace-period cleanup for an
        identified consumer that abandons its session permanently.
        """
        for subscribers in self._subscribers.values():
            subscribers.discard(queue)
        for members in self._groups.values():
            if queue in members:
                members.remove(queue)

        is_identified = any(q is queue for q in self._consumer_queues.values())
        if not is_identified and self.ack_tracker is not None:
            self.ack_tracker.forget_queue(queue)

    def subscriber_count(self, topic: str) -> int:
        return len(self._subscribers.get(topic, ()))

    def group_member_count(self, topic: str, group_id: str) -> int:
        return len(self._groups.get((topic, group_id), ()))

    async def publish(self, topic: str, body) -> int:
        """Persist the message durably, then fan it out: every broadcast
        subscriber gets a copy, and each consumer group subscribed to this
        topic gets exactly one member selected round-robin. Every delivery
        is registered for acknowledgement.

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

        delivered = 0

        for queue in self._subscribers.get(topic, ()):
            queue.put_nowait(message)
            if self.ack_tracker is not None:
                self.ack_tracker.record_delivery(queue, message)
            delivered += 1

        for (member_topic, group_id), members in self._groups.items():
            if member_topic != topic or not members:
                continue
            index = self._group_next_index.get((member_topic, group_id), 0) % len(members)
            self._group_next_index[(member_topic, group_id)] = index + 1
            queue = members[index]
            queue.put_nowait(message)
            if self.ack_tracker is not None:
                self.ack_tracker.record_delivery(queue, message)
            delivered += 1

        return delivered
