"""Real throughput and latency measurement against a real broker.

Not a unit test: this starts an actual asyncio TCP server, connects real
client sockets to it, and measures wall-clock time -- the same "no
mocking the thing you're claiming to measure" rule the test suite
follows. Run it directly:

    python -m benchmarks.throughput

It prints a report and does not assert anything; there is no "pass/fail"
threshold, because there is no prior baseline or SLA to compare against.
The numbers are whatever this machine, right now, actually produces --
see docs/BENCHMARKS.md for the last captured run, environment notes, and
the honest limitations of this methodology.
"""

from __future__ import annotations

import asyncio
import statistics
import tempfile
import time
from pathlib import Path

from broker.protocol import encode_frame, read_frame
from broker.server import run_server
from broker.storage import LogStore
from broker.topics import TopicRegistry

LATENCY_ITERATIONS = 200
THROUGHPUT_MESSAGE_COUNT = 2000


async def _connect(addr):
    return await asyncio.open_connection(addr[0], addr[1])


async def _subscribe(reader, writer, topic: str) -> None:
    writer.write(encode_frame({"type": "SUBSCRIBE", "topic": topic}))
    await writer.drain()


async def measure_latency(addr, topic: str) -> list[float]:
    """One publisher, one subscriber, strictly sequential: publish a
    message, wait for the subscriber to receive it, only then publish the
    next one. This isolates per-message round-trip latency from any
    pipelining or batching effect -- exactly one message is ever in
    flight."""
    pub_reader, pub_writer = await _connect(addr)
    sub_reader, sub_writer = await _connect(addr)
    await _subscribe(sub_reader, sub_writer, topic)

    samples = []
    for i in range(LATENCY_ITERATIONS):
        start = time.perf_counter()
        pub_writer.write(encode_frame({"type": "PUBLISH", "topic": topic, "body": {"i": i}}))
        await pub_writer.drain()
        await read_frame(sub_reader)
        samples.append((time.perf_counter() - start) * 1000)  # ms

    for writer in (pub_writer, sub_writer):
        writer.close()
        await writer.wait_closed()
    return samples


async def measure_throughput(addr, topic: str, count: int) -> float:
    """One publisher writes `count` PUBLISH frames back-to-back without
    waiting for anything between them (no reply is expected for PUBLISH
    on this protocol), while one subscriber concurrently reads messages
    off its own connection. Returns messages/second, measured from the
    first byte sent to the last message received."""
    pub_reader, pub_writer = await _connect(addr)
    sub_reader, sub_writer = await _connect(addr)
    await _subscribe(sub_reader, sub_writer, topic)

    async def receive_all():
        for _ in range(count):
            await read_frame(sub_reader)

    receiver = asyncio.create_task(receive_all())

    start = time.perf_counter()
    for i in range(count):
        pub_writer.write(encode_frame({"type": "PUBLISH", "topic": topic, "body": {"i": i}}))
    await pub_writer.drain()
    await receiver
    elapsed = time.perf_counter() - start

    for writer in (pub_writer, sub_writer):
        writer.close()
        await writer.wait_closed()
    return count / elapsed


def _report_latency(label: str, samples: list[float]) -> None:
    sorted_samples = sorted(samples)
    p95_index = int(len(sorted_samples) * 0.95)
    print(f"  {label}:")
    print(f"    min    = {sorted_samples[0]:.3f} ms")
    print(f"    median = {statistics.median(sorted_samples):.3f} ms")
    print(f"    mean   = {statistics.mean(sorted_samples):.3f} ms")
    print(f"    p95    = {sorted_samples[p95_index]:.3f} ms")
    print(f"    max    = {sorted_samples[-1]:.3f} ms")


async def run_scenario(label: str, registry: TopicRegistry) -> None:
    server = await run_server(host="127.0.0.1", port=0, registry=registry)
    addr = server.sockets[0].getsockname()
    try:
        print(f"\n=== {label} ===")
        latency_samples = await measure_latency(addr, "bench-latency")
        _report_latency("per-message publish-to-receive latency", latency_samples)

        rate = await measure_throughput(addr, "bench-throughput", THROUGHPUT_MESSAGE_COUNT)
        print(f"  sustained throughput: {rate:.0f} messages/sec "
              f"({THROUGHPUT_MESSAGE_COUNT} messages, single publisher, single subscriber)")

        # Give each connection's server-side handle_connection() a moment to
        # notice the client closed and finish its own cleanup, so closing
        # the server below doesn't leave a dangling task for the event loop
        # to complain about at shutdown.
        await asyncio.sleep(0.05)
    finally:
        server.close()
        await server.wait_closed()


async def main() -> None:
    print(f"Latency: {LATENCY_ITERATIONS} sequential round trips.")
    print(f"Throughput: {THROUGHPUT_MESSAGE_COUNT} pipelined messages.")

    await run_scenario("in-memory only (no LogStore)", TopicRegistry())

    with tempfile.TemporaryDirectory() as tmp:
        registry = TopicRegistry(store=LogStore(Path(tmp)))
        await run_scenario("with persistence (LogStore, matches production default)", registry)


if __name__ == "__main__":
    asyncio.run(main())
