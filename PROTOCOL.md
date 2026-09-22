# Wire Protocol (v0)

This document defines how clients and the broker talk to each other over TCP,
and why the format was chosen. Read this before touching `broker/protocol.py`
or `broker/server.py` -- every later milestone builds on this.

## Why not just send raw JSON lines?

The simplest possible design is: send a JSON object per line, separated by
`\n`, and read with `readline()`. That breaks the moment a message payload
contains a newline character, and it also makes it ambiguous how to detect
a partial read on a slow or congested connection -- you cannot tell "message
isn't finished yet" apart from "message has no more data" without scanning
byte by byte for the delimiter.

Real network protocols (HTTP/2, gRPC, most binary protocols) solve this with
**framing**: the sender always says how many bytes are coming *before*
sending them, so the receiver can allocate a buffer and know exactly when a
full message has arrived, regardless of what bytes are inside it.

## Frame format

Every message on the wire is:

```
+----------------+----------------------+
| length (4 bytes, big-endian uint32)   |
+----------------------------------------+
| payload (length bytes, UTF-8 JSON)     |
+----------------------------------------+
```

- The length prefix is fixed at 4 bytes, so the receiver always knows exactly
  how many bytes to read to get the length, before it knows anything else.
- The payload is UTF-8 encoded JSON. JSON was chosen over a fully custom
  binary payload format for v0 because it is human-debuggable (you can log a
  frame and read it) and because message throughput is not yet a proven
  bottleneck -- if benchmarking in Milestone 7 shows JSON encoding is the
  limiting factor, a binary payload format is a documented future option,
  not a redesign of the framing itself.
- Maximum frame size is capped (`MAX_FRAME_SIZE` in `protocol.py`) so a
  malformed or malicious length prefix can't make the broker allocate an
  unbounded buffer.

## Message envelope (JSON payload)

Every payload is a JSON object with at least a `"type"` field:

```json
{"type": "CONNECT", "role": "producer"}
{"type": "CONNECT", "role": "consumer", "topics": ["orders"]}
{"type": "PUBLISH", "topic": "orders", "body": {"order_id": 123}}
{"type": "SUBSCRIBE", "topic": "orders"}
{"type": "MESSAGE", "topic": "orders", "message_id": "m-1", "body": {"order_id": 123}}
{"type": "ACK", "message_id": "m-1"}
{"type": "ERROR", "reason": "unknown message type"}
```

`CONNECT`, `PUBLISH`, and `SUBSCRIBE` are implemented as of Milestone 2.
`ACK` (consumer to broker) and redelivery on timeout are implemented in
Milestone 4. This document is updated as each milestone adds message types --
it is meant to always describe the current protocol, not a final spec
written up front.

`ERROR` is returned for any frame whose `"type"` is not one of the above.

## What Milestone 2 actually implements

In-memory publish/subscribe fan-out:

- `SUBSCRIBE {topic}` registers the connection to receive `MESSAGE` frames
  published to that topic from this point forward.
- `PUBLISH {topic, body}` delivers a `MESSAGE {topic, message_id, body}`
  frame to every connection currently subscribed to that topic. `body` is
  passed through unchanged; the broker does not interpret it.
- `CONNECT {role}` is accepted and logged but does not yet gate behavior --
  a connection that never sends CONNECT can still PUBLISH and SUBSCRIBE.
  Enforcing role-appropriate behavior is deferred until there's an actual
  reason to (e.g. authentication, or producer/consumer-specific limits),
  rather than added speculatively now.
- `message_id` is assigned by the broker (not the publisher) so every
  delivered copy of a message can be referred to unambiguously once
  Milestone 4 adds per-subscriber acknowledgement.

## What Milestone 3 actually implements

Durable, replayable topics:

- Every `PUBLISH` is appended to that topic's on-disk log before being
  fanned out to any live subscriber, so a message survives even if the
  broker crashes immediately after accepting it.
- Every `SUBSCRIBE` now replays the topic's full history to the new
  subscriber before it starts receiving live messages -- the exact gap
  Milestone 2 left open (publishing to a topic with no current subscribers
  used to drop the message forever) is closed.
- A restarted broker resumes message id assignment after whatever was
  already on disk, so ids never collide across a restart.
- There is still no acknowledgement and no notion of "resume from where I
  left off" -- every SUBSCRIBE replays from the very beginning of the
  topic's history, every time. Per-subscriber offsets are Milestone 4/5
  territory, not this one.

## What Milestone 1 implemented

Only the framing layer: `encode_frame(payload_dict) -> bytes` and an async
`read_frame(reader) -> dict` that can read a frame off an `asyncio` stream
one byte-count at a time, plus a TCP server that accepted connections and
echoed frames back unchanged, to prove framing worked in isolation before
any protocol logic was built on top of it. The echo behavior no longer
exists as of Milestone 2 -- it was scaffolding, not a feature.
