# Project Log

Running record of design decisions, problems, and what's actually done vs.
remaining. Written as work happens, not reconstructed afterward.

## Milestone 1: Project scaffolding + framing protocol + TCP connection handling

**Decision: custom length-prefixed framing instead of newline-delimited JSON.**
Newline-delimited JSON is simpler but breaks if a payload ever contains a
literal newline, and makes "message incomplete" indistinguishable from
"message has no more bytes" without scanning for the delimiter. A 4-byte
length prefix removes both problems at the cost of a slightly more complex
reader. Full reasoning in `PROTOCOL.md`.

**Decision: JSON payload, not a fully custom binary format, for v0.**
JSON is human-debuggable (a captured frame can be read directly) and
throughput hasn't been measured yet, so optimizing payload encoding before
there's a number to justify it would be premature. If Milestone 7's
throughput measurements show JSON encoding is the bottleneck, that's a
documented, deliberate future change, not something guessed at up front.

**Decision: asyncio over threading for connection handling.**
The broker's job is fundamentally I/O-bound (waiting on network reads/writes
from many clients), which is exactly asyncio's design target, and it avoids
needing locks around shared state (topic registry, ack tracker) that a
thread-per-connection model would require starting in Milestone 2. This is
a decision I should be able to explain the tradeoffs of in an interview,
including when threading or multiprocessing would be the better choice
(CPU-bound work, which this broker doesn't have).

**What's tested:**
- Frame encode/decode round-trips correctly (unit test).
- Oversized payload is rejected before being sent (unit test).
- A truncated or empty stream raises a clear error instead of hanging or
  crashing (unit test, two cases).
- The server correctly echoes a single frame over a real socket
  (integration test).
- The server correctly handles multiple sequential frames on one
  connection (integration test).
- The server correctly handles 10 concurrent client connections at once
  (integration test) -- this is the first real evidence the asyncio
  connection-per-task model works under concurrency, not just in the
  single-client case.

**What's explicitly NOT done yet (by design, not oversight):**
- No message types are interpreted. The server only frames and echoes.
- No persistence.
- No acknowledgement/retry.
- No metrics or dashboard.
- Not deployed anywhere yet -- there is nothing meaningful to deploy until
  the broker actually brokers something.

**Open question carried into Milestone 2:** how should the topic registry
be structured so that publish/subscribe fan-out is easy to test in
isolation from the network layer, the way `protocol.py` is tested in
isolation from `server.py`? Likely answer: a plain in-memory class with no
asyncio/socket dependency, exercised directly in unit tests, and only
wired into the connection handler at the end. Will confirm this holds up
once Milestone 2 is actually written.
