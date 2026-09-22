"""In-memory topic registry: tracks subscribers and fans out published
messages to them.

Deliberately has no dependency on sockets, asyncio streams, or the wire
protocol -- a subscriber is represented by an asyncio.Queue that the
connection layer drains and writes to the network. That separation is what
lets fan-out logic be unit tested without opening a single socket, the same
way protocol.py is tested independently of server.py.

No persistence at this milestone: publishing to a topic with no current
subscribers simply drops the message. A subscriber that connects after a
message was published never sees it. That gap is intentional and is what
Milestone 3's persistent log exists to close -- fixing it here would mean
building persistence and fan-out at the same time, which makes both harder
to get right and harder to test in isolation.
"""

from __future__ import annotations

import asyncio
import itertools


class TopicRegistry:
    def __init__(self) -> None:
        self._subscribers: dict[str, set[asyncio.Queue]] = {}
        self._message_ids = itertools.count(1)

    def subscribe(self, topic: str, queue: asyncio.Queue) -> None:
        self._subscribers.setdefault(topic, set()).add(queue)

    def unsubscribe(self, topic: str, queue: asyncio.Queue) -> None:
        subscribers = self._subscribers.get(topic)
        if subscribers:
            subscribers.discard(queue)

    def unsubscribe_all(self, queue: asyncio.Queue) -> None:
        """Remove a queue from every topic it was subscribed to. Called when
        a connection closes, so a dead connection's queue doesn't keep
        being handed messages that will never be read."""
        for subscribers in self._subscribers.values():
            subscribers.discard(queue)

    def subscriber_count(self, topic: str) -> int:
        return len(self._subscribers.get(topic, ()))

    async def publish(self, topic: str, body) -> int:
        """Deliver a message to every current subscriber of topic.

        Returns the number of subscribers the message was delivered to
        (0 if the topic has none right now). Each subscriber gets its own
        message dict with a unique, broker-assigned message_id -- shared
        mutable state across subscribers would be a bug waiting to happen
        once Milestone 4 adds per-subscriber ack tracking keyed by this id.
        """
        subscribers = self._subscribers.get(topic, ())
        delivered = 0
        for queue in subscribers:
            message = {
                "type": "MESSAGE",
                "topic": topic,
                "message_id": f"m-{next(self._message_ids)}",
                "body": body,
            }
            await queue.put(message)
            delivered += 1
        return delivered
