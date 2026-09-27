# Architecture

This is a plain-text diagram, not an image, because everything in this
project is meant to be readable straight out of the repository without
extra tooling -- the same reasoning behind JSONL logs over a binary
format and a hand-rolled HTTP server over a framework.

## Components and data flow

```
                       TCP, length-prefixed JSON frames (PROTOCOL.md)
                       CONNECT / PUBLISH / SUBSCRIBE / ACK
  Publisher  ────────┐
  (client)           │
                      ▼
              ┌───────────────────┐
              │   asyncio TCP      │   one _reader_loop + one _writer_loop
              │   server           │   task per connection, raced with
              │  (broker/server.py)│   asyncio.wait(FIRST_COMPLETED)
              └─────────┬──────────┘
                        │  dispatches by frame "type"
                        ▼
              ┌───────────────────┐        ┌───────────────────┐
              │   TopicRegistry    │──────▶│    LogStore        │
              │  (broker/topics.py)│ append │ (broker/storage.py)│
              │                    │        │  one JSONL file    │
              │  - subscribers     │        │  per topic on disk │
              │  - consumer groups │        └───────────────────┘
              │  - publish counts  │
              │  - active_conns    │──────▶┌───────────────────┐
              └─────────┬──────────┘ record │    AckTracker      │
                        │  put_nowait        │ (broker/delivery.py)│
                        ▼                   │  loop.call_later()  │
              ┌───────────────────┐         │  per-message timer, │
              │  per-connection    │         │  redeliver on       │
              │  asyncio.Queue     │◀────────│  timeout, cancel    │
              │  (outbound)        │  ACK    │  on ack             │
              └─────────┬──────────┘         └───────────────────┘
                        │  _writer_loop drains this queue
                        ▼
  Subscriber ◀──────────┘
  (client)

                                          separate port, separate protocol
              ┌───────────────────┐      (plain HTTP, not the broker's own
              │  metrics HTTP      │      framing -- see PROTOCOL.md)
              │  server            │
              │ (broker/metrics.py)│◀── reads TopicRegistry's own bookkeeping,
              └─────────┬──────────┘    nothing it doesn't already track
                        │  GET /metrics (JSON), GET / (dashboard HTML)
                        ▼
              ┌───────────────────┐
              │  dashboard/        │  vanilla JS, polls /metrics
              │  index.html        │  every 2s, no build step
              └───────────────────┘
```

## Why it's shaped this way

- **`TopicRegistry` has no import of `asyncio.StreamWriter` or sockets.**
  A subscriber is represented purely as an `asyncio.Queue`. This is the
  single decision that makes the rest of the architecture testable: every
  test in `tests/test_topics.py` and `tests/test_delivery.py` exercises
  fan-out, persistence wiring, and ack tracking without opening a socket,
  and the network layer (`server.py`) is a thin adapter on top rather
  than where the actual logic lives.
- **Two servers, two protocols, two ports.** The broker's own TCP
  protocol (`PROTOCOL.md`) and the metrics HTTP endpoint are unrelated
  concerns serving different audiences (a broker client vs. a browser or
  `curl`), so they are genuinely separate `asyncio` servers rather than
  one process multiplexing two protocols on one port.
- **Every arrow into `LogStore` and `AckTracker` originates from
  `TopicRegistry`, never from `server.py` directly.** The connection
  handler only ever calls `registry.publish()` / `registry.subscribe()` /
  `registry.ack_tracker.ack()` -- it has no direct knowledge of how
  persistence or redelivery work, which keeps those two concerns
  swappable (see `PROTOCOL.md` and `docs/PROJECT_LOG.md` for the
  documented, deliberate limitations of the current implementations of
  each).

## What this diagram intentionally leaves out

- Consumer groups' round-robin member selection (`_groups`,
  `_group_next_index` in `topics.py`) -- covered in the module's own
  docstring and `docs/PROJECT_LOG.md`'s Milestone 5 entry, not repeated
  here as a box.
- The exact reconnect/reassign path for an identified consumer
  (`_consumer_queues`, `AckTracker.reassign()`) -- same reason.

Both are real, tested, and documented elsewhere; this diagram is meant to
answer "what talks to what," not "every state transition."
