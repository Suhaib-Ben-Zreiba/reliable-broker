# Reliable Broker

A small message broker built from scratch: TCP transport, a custom
length-prefixed framing protocol, persistent per-topic queues, and
at-least-once delivery via acknowledgement and retry.

**Status: Milestone 1 of 7 complete.** This project is being built and
documented milestone by milestone, not dumped in one commit. See
`docs/PROJECT_LOG.md` for what's done, what's in progress, and the design
decisions behind each piece.

## Why this exists

Most "message broker" portfolio projects wrap Kafka or Redis and call it a
day. This one implements the actual mechanics: framing, connection handling,
persistence, acknowledgement/retry, and concurrent delivery, so that every
piece can be explained and defended rather than treated as a black box.

## Current functionality (Milestone 1)

- A custom binary framing protocol over TCP (see `PROTOCOL.md` for the
  design rationale).
- An asyncio TCP server that accepts arbitrarily many concurrent
  connections, reads and writes correctly framed messages, and handles
  disconnects cleanly.
- No message semantics yet (CONNECT/PUBLISH/SUBSCRIBE) -- that's
  Milestone 2. This milestone exists to prove the transport layer is
  correct in isolation before anything is built on top of it.

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
- `tests/test_server.py`: integration tests against a real TCP server and
  real client sockets (not mocked), including a concurrent-connections
  test with 10 simultaneous clients.

## Architecture

See `docs/architecture.md` (added once there's enough system to diagram
meaningfully -- a diagram of a TCP echo server isn't worth drawing yet).

## Roadmap

1. ~~Project scaffolding + framing protocol + TCP connection handling~~ (done)
2. CONNECT / PUBLISH / SUBSCRIBE and in-memory topic fan-out
3. Persistent per-topic append-only log
4. Acknowledgement and timeout-based redelivery
5. Concurrent multi-consumer delivery + reconnect handling
6. Metrics endpoint + dashboard
7. Throughput/latency measurement, final documentation pass
