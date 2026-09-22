# Wire Protocol (v0)

This document defines how clients and the broker talk to each other over TCP,
and why the format was chosen. Read this before touching `broker/protocol.py`
or `broker/server.py` — every later milestone builds on this.

## Why not just send raw JSON lines?

The simplest possible design is: send a JSON object per line, separated by
`\n`, and read with `readline()`. That breaks the moment a message payload
contains a newline character, and it also makes it ambiguous how to detect
a partial read on a slow or congested connection — you cannot tell "message
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
  bottleneck — if benchmarking in Milestone 7 shows JSON encoding is the
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

`CONNECT`, `PUBLISH`, and `SUBSCRIBE` are implemented starting in Milestone 2.
`MESSAGE` (broker to consumer) and `ACK` (consumer to broker) are implemented
in Milestone 4 along with redelivery. This document will be updated as each
milestone adds message types — it is meant to always describe the current
protocol, not a final spec written up front.

## What Milestone 1 actually implements

Only the framing layer: `encode_frame(payload_dict) -> bytes` and an async
`read_frame(reader) -> dict` that can read a frame off an `asyncio` stream
one byte-count at a time, plus a TCP server that accepts connections and can
read and log a single frame. No message types are interpreted yet — that is
deliberate, so the framing itself is tested and correct in isolation before
any protocol logic is built on top of it.
