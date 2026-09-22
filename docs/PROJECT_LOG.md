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

## Milestone 2: CONNECT / PUBLISH / SUBSCRIBE and in-memory topic fan-out

**The open question from Milestone 1 held up.** `broker/topics.py`'s
`TopicRegistry` has no import of `asyncio.StreamWriter` or sockets at all --
subscribers are represented as `asyncio.Queue` objects, and the registry
just puts messages onto them. That made `tests/test_topics.py` possible
without opening a single socket, and meant fan-out logic (multi-subscriber
delivery, cross-topic isolation, unsubscribe) was debugged and correct
*before* it was ever wired into the network layer.

**Decision: a connection needs two concurrent loops, not one.** A
connection has to simultaneously keep reading commands from its client and
be ready to push out a message the instant another connection publishes to
a topic it's subscribed to. One `await read_frame(...)` loop can't do both,
so each connection now runs a `_reader_loop` and a `_writer_loop` as
separate `asyncio` tasks, with the writer loop draining a per-connection
`asyncio.Queue` that `TopicRegistry.publish()` feeds.

**Decision: race the two loops with `asyncio.wait(..., FIRST_COMPLETED)`,
not `asyncio.gather()`.** The obvious-looking way to run two concurrent
tasks is `asyncio.gather(reader_task, writer_task)`. The problem: when a
client disconnects, `_reader_loop` raises `ConnectionClosedError` and
`gather` immediately propagates it to the caller -- but it does **not**
cancel `_writer_loop`, which is left awaiting `outbound.get()` forever.
Every single disconnect would leak one background task permanently, and
it's the kind of bug that "works" in casual testing and only shows up as a
slow task/memory leak under sustained real use. `asyncio.wait(...,
return_when=FIRST_COMPLETED)` plus explicitly cancelling whichever task is
still pending avoids this entirely.
`test_disconnecting_subscriber_does_not_crash_future_publishes` exists to
guard against a future change accidentally reintroducing this.

**Decision: message_id is assigned by the broker, not the publisher.**
Once Milestone 4 needs to track which specific delivered copy of a message
has been acknowledged, that id needs to be unambiguous and broker-controlled
-- trusting a publisher-supplied id would let a buggy or malicious producer
collide ids across messages. Assigning it now, even though nothing consumes
it yet, means the id scheme doesn't have to be retrofitted later.

**Decision: CONNECT is accepted but doesn't gate behavior yet.** A
connection can PUBLISH or SUBSCRIBE without ever sending CONNECT. This is
deliberate, not an oversight -- there is no current reason (auth, role
limits) that requires enforcement, and adding a permission check with
nothing behind it would be speculative complexity. It's noted here so the
gap is documented rather than silently assumed away.

**What's tested (18 tests total, 13 new this milestone):**
- Fan-out unit tests: no-subscriber publish, single and multi-subscriber
  delivery, cross-topic isolation, unsubscribe, unsubscribe_all, and that
  every delivered message gets a unique id.
- Integration tests over real sockets: end-to-end publish/subscribe,
  multiple simultaneous subscribers, cross-topic isolation at the network
  level, publish-with-no-subscribers not raising, unknown message type
  producing an `ERROR` reply, and the disconnect/leak regression test
  above.

**What's explicitly NOT done yet:**
- No persistence -- a subscriber must already be subscribed at publish
  time or the message is gone. Milestone 3.
- No acknowledgement, no redelivery, no at-least-once guarantee of any
  kind yet. Milestone 4.
- Still not deployed. Still nothing worth deploying yet.

## Milestone 3: persistent per-topic append-only log

**Decision: JSONL files, one per topic, not SQLite.** The access pattern is
purely sequential append + sequential full read, which is exactly what a
flat file is good at without adding a real dependency. If a later milestone
needs random access (resume from a specific offset without scanning from
the start), that becomes a concrete reason to revisit this -- not a
speculative upgrade made now. Full reasoning in `broker/storage.py`.

**The actual bug this milestone fixes, precisely stated:** in Milestone 2,
`registry.publish("orders", body)` with zero current subscribers silently
dropped the message. There was no error, no warning, nothing -- it just
vanished. `test_publish_with_no_subscribers_is_still_persisted` and
`test_new_subscriber_receives_messages_published_before_it_subscribed`
exist specifically to pin down that this no longer happens.

**Decision: replay must be synchronous, with zero `await` between
`subscribe()` and `replay()`.** This asyncio broker is single-threaded and
cooperative: a coroutine only yields control at an `await`. So if
"register this subscriber" and "hand it everything published before now"
happen with no `await` in between, no other coroutine can run a `PUBLISH`
in that window, and the two operations are atomic for free -- no lock
needed. Using async file I/O here (e.g. `aiofiles`) would have reintroduced
exactly the race it's meant to avoid: a `PUBLISH` landing in the gap could
be missed by both the live-fanout path and a since-stale history read.
This is a direct, deliberate consequence of choosing `asyncio` in
Milestone 1 -- worth being able to explain that connection in an interview.

**Decision: message ids must resume correctly after a restart.** A naive
implementation resets `itertools.count(1)` on every process start, which
would mean the *second* message published after any restart reuses an id
already sitting in yesterday's log. `LogStore.max_message_id_seen()` scans
every topic's log at startup and `TopicRegistry` resumes counting after
whatever it finds.
`test_registry_survives_simulated_restart` and
`test_broker_restart_does_not_lose_messages` (the latter stops and starts
a real server over real sockets, not just in-memory objects) both check
this directly, not just "does replay return something."

**Small security decision, easy to miss:** a topic name is client-supplied
data, and I'm using it to build a filename. Without `_safe_filename()`, a
client publishing to a topic named `"../../etc/passwd"` could make the
broker read or write outside its data directory.
`test_topic_name_cannot_escape_the_data_directory` exists because this
class of bug is exactly the kind that's obvious once pointed out and easy
to never think of otherwise.

**What's tested (31 tests total, 13 new this milestone):** persistence
round-trips, cross-topic isolation on disk, restart survival at both the
storage layer and the full registry layer, message-id continuation across
restart, the path-traversal guard, and -- the one that actually matters
most -- a real server process being stopped and a new one started against
the same data directory, over real sockets, with a subscriber still
receiving what was published before the "restart."

**What's explicitly NOT done yet:**
- No acknowledgement. A consumer can't tell the broker "I've processed
  this," and the broker has no notion of redelivering something that
  wasn't acknowledged. Milestone 4.
- No per-subscriber offset. Every SUBSCRIBE replays the *entire* history
  of a topic, every time, even for a consumer that already saw all of it
  five seconds ago. This is fine for now and would be wasteful at scale --
  exactly the kind of thing Milestone 4/5's ack tracking is meant to fix
  by letting a consumer resume from its own last-acknowledged point instead
  of from zero.
- Still not deployed.
