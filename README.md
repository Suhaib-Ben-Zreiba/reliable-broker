# Reliable Broker

A small message broker built from scratch: TCP transport, a custom
length-prefixed framing protocol, persistent per-topic queues, and
at-least-once delivery via acknowledgement and retry.

**Status: Milestone 2 of 7 complete.** This project is being built and
documented milestone by milestone, not dumped in one commit. See
`docs/PROJECT_LOG.md` for what's done, what's in progress, and the design
decisions behind each piece.

## Why this exists

Most "message broker" portfolio projects wrap Kafka or Redis and call it a
day. This one implements the actual mechanics: framing, connection handling,
persistence, acknowledgement/retry, and concurrent delivery, so that every
piece can be explained and defended rather than treated as a black box.

## Current functionality (through Milestone 2)

- A custom binary framing protocol over TCP (see `PROTOCOL.md` for the
  design rationale).
- An asyncio TCP server handling arbitrarily many concurrent connections.
- Publish/subscribe: `SUBSCRIBE` to a topic, `PUBLISH` to a topic, every
  current subscriber gets the message immediately.
- No persistence yet: publishing to a topic with no subscribers drops the
  message rather than queuing it. That's Milestone 3.
- No acknowledgement/retry yet. That's Milestone 4.

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
- `tests/test_topics.py`: pure unit tests for publish/subscribe fan-out
  (no sockets), including multi-subscriber delivery, cross-topic isolation,
  and unsubscribe behavior.
- `tests/test_server.py`: integration tests against a real TCP server and
  real client sockets (not mocked): pub/sub end to end, multiple
  subscribers, unknown-message-type error handling, and a disconnect test
  that verifies a dead subscriber's connection doesn't break future
  publishes.

## Architecture

See `docs/architecture.md` (added once there's enough system to diagram
meaningfully -- a diagram of a TCP echo server isn't worth drawing yet).

## Roadmap

1. ~~Project scaffolding + framing protocol + TCP connection handling~~ (done)
2. ~~CONNECT / PUBLISH / SUBSCRIBE and in-memory topic fan-out~~ (done)
3. Persistent per-topic append-only log
4. Acknowledgement and timeout-based redelivery
5. Concurrent multi-consumer delivery + reconnect handling
6. Metrics endpoint + dashboard
7. Throughput/latency measurement, final documentation pass
