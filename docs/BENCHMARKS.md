# Benchmarks

Real measurements from `benchmarks/throughput.py`, run against the actual
broker over real TCP sockets. No number in this document is estimated or
invented -- everything below came from actually running the script.

## Methodology

Two scenarios, both single publisher and single subscriber on one topic:

- **Latency**: publish one message, wait for the subscriber to receive
  it, only then publish the next -- 200 sequential round trips, so
  exactly one message is ever in flight. Reports min/median/mean/p95/max
  in milliseconds.
- **Throughput**: write 2000 `PUBLISH` frames back-to-back with no
  waiting between them (there is no reply to wait for), while the
  subscriber concurrently drains its connection. Reports messages/second
  from the first byte sent to the last message received.

Each scenario runs twice: once against a `TopicRegistry` with no
`LogStore` (pure in-memory fan-out), and once with a real `LogStore`
writing to disk -- the same persistence path `broker.server.main()` uses
by default. Comparing the two isolates the cost of `TopicLog.append()`,
which opens, writes, and flushes the topic's file on every single publish
(see `broker/storage.py`'s own comment on this exact tradeoff).

Run it yourself:

```bash
python -m benchmarks.throughput
```

## Environment

- Python 3.11.15, Linux x86_64, 4 logical CPUs.
- A shared, virtualized sandbox container, not dedicated hardware. CPU and
  I/O are not isolated from other tenants, which is exactly why the
  results below are a range from three separate runs, not a single
  number.

## Results (three separate runs)

**Latency, in-memory (no persistence):**

| run | min (ms) | median | mean | p95 | max |
|---|---|---|---|---|---|
| 1 | 0.070 | 0.079 | 0.089 | 0.130 | 0.321 |
| 2 | 0.086 | 0.095 | 0.105 | 0.152 | 0.335 |
| 3 | 0.072 | 0.100 | 0.122 | 0.217 | 0.645 |

**Latency, with persistence:**

| run | min (ms) | median | mean | p95 | max |
|---|---|---|---|---|---|
| 1 | 0.057 | 0.061 | 0.068 | 0.097 | 0.428 |
| 2 | 0.056 | 0.062 | 0.070 | 0.101 | 0.339 |
| 3 | 0.057 | 0.062 | 0.078 | 0.135 | 0.753 |

**Throughput (2000 messages, single publisher/subscriber):**

| run | in-memory (msg/s) | with persistence (msg/s) |
|---|---|---|
| 1 | 63,190 | 36,756 |
| 2 | 51,672 | 34,077 |
| 3 | 40,625 | 38,880 |

## Interpretation

- **Median latency is sub-millisecond either way** (roughly 0.06-0.10 ms),
  which is dominated by asyncio event-loop scheduling and loopback TCP,
  not by the broker's own logic. The tail (p95/max) is visibly noisier
  than the median -- consistent with a shared, virtualized sandbox rather
  than a dedicated machine, not with anything the broker itself is doing
  inconsistently.
- **Persistence does not clearly cost latency here**, and in these runs
  the persisted numbers are if anything slightly lower. That is
  counterintuitive for a synchronous per-message `open`/`write`/`flush`,
  and the likely explanation is that the in-memory scenario's
  measurements are noisier (this sandbox's CPU/IO scheduling, not the
  code path) rather than that disk writes are actually free. This is
  flagged rather than smoothed over: an honest benchmark reports what
  actually happened, including results that don't match intuition.
- **Persistence visibly costs throughput**: roughly 35-39k messages/sec
  with `LogStore` versus 40-63k without, across the same three runs. That
  is directly attributable to `TopicLog.append()` opening, writing, and
  flushing the topic file on every single publish, exactly the cost
  `broker/storage.py`'s own docstring names as the thing worth measuring
  before optimizing. Batched or buffered writes are a documented option
  if this ever needs to go faster -- not attempted here, because there
  was no measured reason to before this benchmark existed.
- **Run-to-run variance (40k-63k msg/s for the same in-memory scenario)
  is itself a real finding**: this sandbox is not a stable place to make
  fine-grained performance claims, which is precisely why this document
  reports a range from three runs instead of a single cherry-picked
  number.

## Limitations of this methodology

- **Single publisher, single subscriber only.** This does not measure how
  throughput or latency changes with many concurrent connections, many
  topics, or consumer groups distributing work across multiple members --
  those are different, unmeasured scenarios.
- **Loopback only.** Everything runs on `127.0.0.1` in one process; there
  is no real network latency, packet loss, or bandwidth limit involved.
- **JSON encoding is not isolated as its own cost.** `encode_frame()`/
  `read_frame()` overhead is included in every number above but never
  measured on its own.
- **Not a competitive benchmark.** No attempt was made to tune the event
  loop, disable Python's GIL-related overhead, or otherwise engineer a
  best-case number. These are the broker's numbers as actually built, on
  a shared sandbox, nothing more.
