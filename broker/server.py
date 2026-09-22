"""TCP connection handling and command dispatch.

Milestone 1 proved framing works over real sockets by echoing frames back.
Milestone 2 replaces that echo with real command handling: CONNECT,
PUBLISH, SUBSCRIBE, routed through a shared TopicRegistry (see topics.py).

Each connection runs two concurrent loops: one reading incoming frames from
the client, one writing outgoing frames (messages fanned out from other
connections) from a per-connection queue. They're run as separate tasks and
raced with asyncio.wait(..., FIRST_COMPLETED) rather than asyncio.gather(),
because gather does not cancel the surviving coroutine when the other
raises -- that would leak a task that runs forever every time a client
disconnects while it had nothing queued to write.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from broker.protocol import ConnectionClosedError, encode_frame, read_frame
from broker.storage import LogStore
from broker.topics import TopicRegistry

logger = logging.getLogger("broker.server")


async def _reader_loop(
    reader: asyncio.StreamReader,
    registry: TopicRegistry,
    outbound: asyncio.Queue,
    subscribed_topics: set[str],
    peer,
) -> None:
    while True:
        frame = await read_frame(reader)
        await _handle_frame(frame, registry, outbound, subscribed_topics, peer)


async def _writer_loop(writer: asyncio.StreamWriter, outbound: asyncio.Queue) -> None:
    while True:
        message = await outbound.get()
        writer.write(encode_frame(message))
        await writer.drain()


async def _handle_frame(
    frame: dict,
    registry: TopicRegistry,
    outbound: asyncio.Queue,
    subscribed_topics: set[str],
    peer,
) -> None:
    msg_type = frame.get("type")

    if msg_type == "CONNECT":
        logger.info("%s CONNECT role=%s", peer, frame.get("role"))

    elif msg_type == "PUBLISH":
        topic = frame.get("topic")
        if not topic:
            await outbound.put({"type": "ERROR", "reason": "PUBLISH requires a topic"})
            return
        delivered = await registry.publish(topic, frame.get("body"))
        logger.info("%s PUBLISH topic=%s delivered_to=%d", peer, topic, delivered)

    elif msg_type == "SUBSCRIBE":
        topic = frame.get("topic")
        if not topic:
            await outbound.put({"type": "ERROR", "reason": "SUBSCRIBE requires a topic"})
            return
        # subscribe() then replay() with no `await` between them is
        # deliberate -- see TopicRegistry.replay() for why that ordering
        # is what makes this race-free against a concurrent PUBLISH.
        registry.subscribe(topic, outbound)
        replayed = registry.replay(topic, outbound)
        subscribed_topics.add(topic)
        logger.info("%s SUBSCRIBE topic=%s replayed=%d", peer, topic, replayed)

    else:
        await outbound.put({"type": "ERROR", "reason": f"unknown message type: {msg_type!r}"})


async def handle_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, registry: TopicRegistry) -> None:
    peer = writer.get_extra_info("peername")
    logger.info("connection opened: %s", peer)

    outbound: asyncio.Queue = asyncio.Queue()
    subscribed_topics: set[str] = set()

    reader_task = asyncio.create_task(_reader_loop(reader, registry, outbound, subscribed_topics, peer))
    writer_task = asyncio.create_task(_writer_loop(writer, outbound))

    done, pending = await asyncio.wait({reader_task, writer_task}, return_when=asyncio.FIRST_COMPLETED)

    for task in pending:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    for finished in done:
        exc = finished.exception()
        if exc is not None and not isinstance(exc, ConnectionClosedError):
            logger.warning("connection %s ended with unexpected error: %r", peer, exc)

    registry.unsubscribe_all(outbound)
    writer.close()
    await writer.wait_closed()
    logger.info("connection closed: %s", peer)


async def run_server(host: str = "127.0.0.1", port: int = 8765, registry: TopicRegistry | None = None) -> asyncio.AbstractServer:
    registry = registry if registry is not None else TopicRegistry()

    async def _handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await handle_connection(reader, writer, registry)

    server = await asyncio.start_server(_handler, host, port)
    addr = server.sockets[0].getsockname()
    logger.info("broker listening on %s:%s", addr[0], addr[1])
    return server


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    registry = TopicRegistry(store=LogStore("data"))
    server = await run_server(registry=registry)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
