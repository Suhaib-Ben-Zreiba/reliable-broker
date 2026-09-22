# Reliable Broker

A small message broker built from scratch: TCP transport, a custom
length-prefixed framing protocol, persistent per-topic queues, and
at-least-once delivery via acknowledgement and retry.

**Status: Milestone 3 of 7 complete.** This project is being built and
documented milestone by milestone, not dumped in one commit. See
`docs/PROJECT_LOG.md` for what's done, what's in progress, and the design
decisions behind each piece.

## Why this exists

Most "message broker" portfolio projects wrap Kafka or Redis and call it a
day. This one implements the actual mechanics: framing, connection handling,
persistence, acknowledgement/retry, and concurrent delivery, so that every
piece can be explained and defended rather than treated as a black box.

## Current functionality (through Milestone 3)

- A custom binary framing protocol over TCP (see `PROTOCOL.md` for the
  design rationale).
- An asyncio TCP server handling arbitrarily many concurrent connections.
- Publish/subscribe: `SUBSCRIBE` to a topic, `PUBLISH` to a topic, every
  current subscriber gets the message immediately.
- Durable, replayable topics: every published message is written to an
  append-only per-topic log on disk before it's fanned out live, and every
  new `SUBSCRIBE` replays that topic's full history first. A broker
  restart does not lose messages -- `tests/test_server.py`'s
  `test_broker_restart_does_not_lose_messages` proves this by actually
  stopping one server and starting a second one against the same data
  directory.
- No acknowledgement/retry yet, and no per-subscriber resume offset --
  every subscribe replays from the beginning of history, every time.
  That's Milestone 4.

## Running it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m broker.server
```

## Testing

```bash
pytest
```

- `tests/test_protocol.py`: pure unit tests for frame encode/decode,
  including truncated-stream and oversized-frame failure cases.
- `tests/test_storage.py`: pure unit tests for the persistent log,
  including a simulated-restart test and the topic-name path-traversal
  guard.
- `tests/test_topics.py`: pure unit tests for publish/subscribe fan-out and
  replay (no sockets), including multi-subscriber delivery, cross-topic
  isolation, unsubscribe behavior, and message-id continuation across a
  simulated restart.
- `tests/test_server.py`: integration tests against a real TCP server and
  real client sockets (not mocked): pub/sub end to end, multiple
  subscribers, unknown-message-type error handling, a disconnect test that
  verifies a dead subscriber's connection doesn't break future publishes,
  and `test_broker_restart_does_not_lose_messages`, which stops a real
  server and starts a second one against the same data directory to prove
  persistence actually works end to end, not just at the unit level.

## Architecture

See `docs/architecture.md` (added once there's enough system to diagram
meaningfully -- a diagram of a TCP echo server isn't worth drawing yet).

## Roadmap

1. ~~Project scaffolding + framing protocol + TCP connection handling~~ (done)
2. ~~CONNECT / PUBLISH / SUBSCRIBE and in-memory topic fan-out~~ (done)
3. ~~Persistent per-topic append-only log~~ (done)
4. Acknowledgement and timeout-based redelivery
5. Concurrent multi-consumer delivery + reconnect handling
6. Metrics endpoint + dashboard
7. Throughput/latency measurement, final documentation pass
