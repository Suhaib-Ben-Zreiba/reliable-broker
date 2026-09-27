# Reliable Broker

A small message broker built from scratch: TCP transport, a custom
length-prefixed framing protocol, persistent per-topic queues, and
at-least-once delivery via acknowledgement and retry.

**Status: Milestone 7 of 7 complete.** This project is being built and
documented milestone by milestone, not dumped in one commit. See
`docs/PROJECT_LOG.md` for what's done, what's in progress, and the design
decisions behind each piece.

## Why this exists

Most "message broker" portfolio projects wrap Kafka or Redis and call it a
day. This one implements the actual mechanics: framing, connection handling,
persistence, acknowledgement/retry, and concurrent delivery, so that every
piece can be explained and defended rather than treated as a black box.

## Current functionality (through Milestone 7)

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
- At-least-once delivery: every delivery starts a per-message timer, and a
  consumer that doesn't `ACK` in time gets the exact same message
  redelivered on the exact same connection, repeating until acked. Uses
  per-message `asyncio` timers rather than a polling sweep, so there's no
  background task to manage or leak.
- Consumer identity and reconnect: a subscriber that identifies itself
  with a `consumer_id` and disconnects without acking gets its outstanding
  work redelivered on its next connection with the same id, instead of
  losing it.
- Consumer groups: subscribers sharing a `group_id` on a topic compete for
  messages round-robin (one copy per group, split across members) instead
  of every subscriber getting a full broadcast -- the "competing
  consumers" pattern used to scale work across a pool of workers.
- No per-subscriber resume offset yet -- every plain (non-grouped)
  subscribe still replays a topic's entire history, and grouped
  subscribers get no replay at all (a documented limitation, not a bug --
  see `PROTOCOL.md`). An identified consumer that disconnects and never
  reconnects also leaks its pending redelivery timer; there's no
  expiry/grace-period cleanup yet.
- A metrics endpoint and dashboard: a second, minimal HTTP server (no
  framework) exposes `GET /metrics` as JSON -- connected client count,
  pending ack count, and per-topic publish/subscriber/group counts -- and
  `GET /` serves a small dark-themed static dashboard that polls
  `/metrics` every 2 seconds and renders it as stat tiles and a topics
  table. This is a separate protocol and a separate port from the broker
  itself, not a new command on the wire protocol in `PROTOCOL.md`.

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
- `tests/test_delivery.py`: pure unit tests for ack tracking,
  timeout-based redelivery, and `reassign()` (no sockets), including that
  redelivery repeats until acked and that reassigning a queue moves its
  outstanding work without losing or duplicating it.
- `tests/test_server.py`: integration tests against a real TCP server and
  real client sockets (not mocked): pub/sub end to end, multiple
  subscribers, unknown-message-type error handling, a disconnect test that
  verifies a dead subscriber's connection doesn't break future publishes,
  `test_broker_restart_does_not_lose_messages` (stops a real server and
  starts a second one against the same data directory),
  `test_unacked_message_is_redelivered_to_the_same_connection`,
  `test_reconnecting_with_same_consumer_id_recovers_pending_message`
  (disconnect, reconnect with the same identity, get the exact same
  outstanding message back), and
  `test_consumer_group_splits_work_across_real_connections` (two real
  connections in a group each get their own half of four published
  messages, with no overlap).
- `tests/test_metrics.py`: pure unit tests for `build_snapshot()` (no
  sockets), covering an empty registry, publish/subscriber counts, group
  membership, and pending-ack/connected-client counts.
- `tests/test_metrics_server.py`: integration tests against the real
  metrics HTTP server over real sockets, including a valid-JSON
  `/metrics` response, the dashboard HTML route, a 404 for an unknown
  path, and `/metrics` reflecting a live connection count from an actual
  broker connection (not a manually-constructed registry).

## Architecture

See `docs/architecture.md` for a component diagram and the reasoning
behind the main structural decisions (why `TopicRegistry` has no
dependency on sockets, why metrics get a separate server on a separate
port).

## Benchmarks

```bash
python -m benchmarks.throughput
```

Real latency and throughput numbers, measured against the actual broker
over real sockets, not estimated. See `docs/BENCHMARKS.md` for
methodology, the last captured results, and this methodology's honest
limitations (single publisher/subscriber, loopback only, not a
competitive benchmark).

## Roadmap

1. ~~Project scaffolding + framing protocol + TCP connection handling~~ (done)
2. ~~CONNECT / PUBLISH / SUBSCRIBE and in-memory topic fan-out~~ (done)
3. ~~Persistent per-topic append-only log~~ (done)
4. ~~Acknowledgement and timeout-based redelivery~~ (done)
5. ~~Concurrent multi-consumer delivery + reconnect handling~~ (done)
6. ~~Metrics endpoint + dashboard~~ (done)
7. ~~Throughput/latency measurement, final documentation pass~~ (done)

All seven milestones are complete. What's explicitly NOT built, and why,
is documented where it's relevant rather than collected into a vague
"future work" list: see the "explicitly NOT done yet" section of each
milestone in `docs/PROJECT_LOG.md`, and the wire-protocol-level
limitations (no per-subscriber resume offset, no replay for consumer
groups) in `PROTOCOL.md`.
