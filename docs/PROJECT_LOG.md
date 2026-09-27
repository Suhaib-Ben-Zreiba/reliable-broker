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
  exactly the kind of thing Milestone 4's ack tracking is meant to fix
  by letting a consumer resume from its own last-acknowledged point instead
  of from zero.
- Still not deployed.

## Milestone 4: acknowledgement and timeout-based redelivery

**Decision: per-message `loop.call_later` timers, not a periodic sweep.**
The obvious design is a background task that wakes up every N seconds and
checks every pending delivery for whether it's expired. I didn't build
that. A sweep needs its own lifecycle: something has to start it when the
broker starts and explicitly cancel it when the broker stops, which is
exactly the class of bug Milestone 2's `asyncio.gather()` task leak was --
one more background task that has to be managed correctly or it leaks. A
per-message timer via `loop.call_later()` is owned entirely by the event
loop: it fires at its exact deadline instead of up to one sweep-interval
late, and acking a message is one `handle.cancel()` call. `AckTracker`
ends up with no `start()`/`stop()` at all as a direct consequence.

**What actually failed on the first attempt, and why it wasn't a real
bug:** the first version of `tests/test_delivery.py` used
`PAST_TIMEOUT = TIMEOUT * 3` as the sleep margin after each redelivery
check. That's wrong on purpose-adjacent grounds: redelivery re-arms itself
for another full `TIMEOUT`, so sleeping 3x the timeout reliably let two or
three redelivery cycles fire instead of the one each test expected --
`qsize()` came back 2 or 3, not 1. This wasn't asyncio timing jitter, it
was the test's margin arithmetic not accounting for the fact that the
thing under test *repeats itself*. Fixed by using a margin just past one
timeout (1.5x) for single-cycle assertions, and a separate, tighter margin
(1.2x, on top of a larger base timeout of 0.1s instead of 0.05s to shrink
the relative effect of real scheduling jitter) for the one test that
needs to observe two separate redelivery cycles without also catching a
third.

**Decision: replayed messages get acknowledgement too.** A message a
consumer receives via history replay after Milestone 3 is exactly as
important as one delivered live -- there's no principled reason it should
be exempt from the same at-least-once guarantee. `TopicRegistry.replay()`
now calls `ack_tracker.record_delivery()` for every replayed record, the
same call `publish()` makes for a live one.

**Decision: an unknown or duplicate ACK is not an error.** A consumer's
ACK racing a redelivery that already happened (the timer fired a moment
before the ACK arrived) is an ordinary, expected condition in a
distributed-ish system, not a client mistake. `AckTracker.ack()` returns
`False` rather than raising, and the server logs it rather than sending
back an ERROR frame.

**Decision: disconnecting a subscriber must cancel its pending
redeliveries, not just its subscriptions.** `TopicRegistry.unsubscribe_all()`
now also calls `ack_tracker.forget_queue()`. Without this, a client that
disconnects mid-delivery would have its unacked messages redelivered
forever into a queue nobody will ever read from again -- the same shape of
resource leak as the Milestone 2 task-leak bug, just in a different
component. `test_disconnected_subscriber_is_not_redelivered_to` exists
specifically to exercise this path, even though there's nothing to assert
on the client side once the connection is closed; its job is to prove the
scenario runs cleanly rather than hanging.

**What's tested (45 tests total, 14 new this milestone):** ack-before-
timeout preventing redelivery, unacked messages being redelivered, repeat
redelivery until acked, forgetting a queue cancelling its timers, unknown
acks returning false rather than raising, multiple independent unacked
messages, registry-level wiring of ack tracking into both publish() and
replay(), and three real-socket integration tests: redelivery to the same
connection, no redelivery after a real ACK frame, and a disconnected
subscriber not being redelivered to.

**What's explicitly NOT done yet:**
- No maximum retry count or dead-letter queue -- an unacked message is
  retried forever. Documented as a future option, not a gap discovered by
  accident.
- No redelivery to a *different* consumer after the original connection
  disconnects. A disconnected consumer's pending work is simply dropped,
  not reassigned to another subscriber of the same topic. That requires a
  notion of consumer identity that survives a reconnect, which is
  Milestone 5.
- No per-subscriber resume offset -- SUBSCRIBE still replays everything,
  every time. Also Milestone 5.
- Still not deployed.

## Milestone 5: consumer identity, reconnect, and consumer groups

**Decision: `AckTracker._pending` had to change shape before reassign()
was possible.** It previously stored `key -> TimerHandle` only. Moving a
consumer's outstanding work to a new queue requires redelivering the
actual message, and a `TimerHandle` doesn't expose the arguments it was
scheduled with in any way worth relying on. Refactored to
`key -> (TimerHandle, message)`, kept the message right next to the
handle that will eventually fire it, so `reassign()` has what it needs
without reaching into implementation details of `call_later`.

**Decision: reassign() trusts `_pending` as the sole source of truth for
what to redeliver, and does not also drain the old queue's buffer.**
Every message that has ever fired past its first delivery has, at any
given moment, exactly one live entry in `_pending` (each firing
immediately re-arms via `record_delivery`), even though stale copies from
earlier firings may be sitting unread in the old queue's buffer if nobody
has been reading it. Since that old queue is being abandoned entirely
once reassigned away from, those stale duplicates are simply garbage
collected along with it -- there is no need to reconcile them, only to
move the authoritative pending entries.

**The ordering problem this milestone had to solve, precisely stated:**
`handle_connection`'s cleanup runs the instant a connection closes, well
before any reconnect could plausibly happen. A naive
`unsubscribe_all` -> `forget_queue` on every disconnect (Milestone 4's
behavior) would wipe out a reconnecting consumer's pending acks before it
ever got a chance to reconnect and claim them. Solved by giving
`unsubscribe_all` two different behaviors depending on whether the
disconnecting queue is identified: an anonymous queue is forgotten
immediately (unchanged from Milestone 4); an identified one (some
consumer_id points at it in `_consumer_queues`) is removed from live
delivery but its pending acks are deliberately left alone, to be resolved
later by `subscribe()`'s reassign-on-reconnect path. This is also exactly
why `_consumer_queues[consumer_id]` is NOT cleared on disconnect --
`subscribe()` needs to still find the old, dead queue there when the same
consumer_id comes back, specifically so it knows what to reassign from.

**Decision: consumer groups are a second, independent delivery path, not
a variant of the existing broadcast set.** A queue subscribed with a
`group_id` is never added to `_subscribers` (the broadcast set) at all --
it lives only in `_groups[(topic, group_id)]`, and `publish()` walks both
structures separately: every broadcast subscriber gets a copy, and every
distinct group gets exactly one member's copy via round robin. This
keeps the two delivery models -- "everyone gets everything" vs. "exactly
one of you gets each message" -- from ever being ambiguous about which
one a given queue is participating in.
`test_group_members_do_not_also_receive_broadcast` and
`test_different_groups_on_the_same_topic_each_get_their_own_copy` pin
down that these two models compose correctly rather than interfering.

**Decision, stated plainly so it isn't mistaken for an oversight: grouped
subscribers get no historical replay.** Splitting a topic's *history*
correctly across group members (so each historical message goes to
exactly one member, same as live ones do) requires tracking, per member,
which historical messages it has already been assigned -- real systems
call this partition/offset assignment, and it's a meaningfully larger
feature than round-robin live delivery. Building a half-correct version
of it (e.g. replaying full history to whichever group member happens to
subscribe first) would be actively worse than the current behavior,
since it would look like it worked while quietly duplicating or losing
historical work across a group. `test_group_subscription_does_not_replay_history`
exists to pin this down as intentional.

**What's tested (59 tests total, 14 new this milestone):** reassign()
moving pending work between queues at the unit level (including a
no-op-when-nothing-pending case and that the old queue never receives
anything again afterward), an anonymous disconnect still forgetting
pending acks immediately (regression guard against breaking Milestone
4's behavior), an identified disconnect preserving them, a full
reconnect cycle through the registry, round-robin group delivery,
groups and broadcast composing correctly, independent groups on the same
topic, the group-replay limitation, and two real-socket integration
tests: a genuine reconnect with the same consumer_id recovering a
pending message, and two real connections in a group splitting four
published messages with no overlap.

**What's explicitly NOT done yet:**
- No per-subscriber resume offset for plain (non-grouped) subscriptions --
  SUBSCRIBE still replays a topic's entire history every time.
- No replay at all for grouped subscriptions -- a documented limitation,
  not a gap to quietly work around later without noticing it was a
  decision.
- No expiry or grace-period cleanup for an identified consumer that
  disconnects and never reconnects -- its pending timer keeps firing into
  an abandoned queue forever. A production system would want a maximum
  retry count or a TTL on abandoned consumer state; noted here rather than
  built speculatively.
- Still not deployed.

## Milestone 6: metrics endpoint and dashboard

**Decision: a hand-rolled HTTP server, not a framework.** The whole point
of this project is implementing real mechanics instead of wrapping
something that already does it, and the metrics endpoint only ever needs
to answer two fixed routes: `GET /metrics` and `GET /`. That doesn't
justify pulling in `aiohttp` or similar for routing and middleware it
won't use. `broker/metrics.py`'s `_handle_http_connection()` reads a
request line and headers off a raw `asyncio.StreamReader` and writes a
response by hand -- no keep-alive, no chunked encoding, no request
bodies. That's a deliberate scope limit stated in the module docstring,
not an oversight.

**Decision: a second server on a second port, not a new command on the
existing wire protocol.** `PROTOCOL.md` documents the broker's own
length-prefixed framing protocol; HTTP is a different protocol serving a
different audience (a browser or `curl`, not a broker client), so it gets
its own `asyncio.start_server` and its own port (`run_metrics_server`)
rather than a `METRICS` message type bolted onto `protocol.py`. `main()`
now runs both servers concurrently with `asyncio.gather()` -- safe here,
unlike the reader/writer loops in `server.py`, because neither
`serve_forever()` call is expected to raise or finish first under normal
operation, so the gather-doesn't-cancel-siblings problem from Milestone 2
doesn't apply.

**Decision: `build_snapshot()` is a pure function over
`TopicRegistry`'s own bookkeeping.** It takes a registry and returns a
plain dict, with no `asyncio`, no sockets, no HTTP -- the same isolation
pattern as `topics.py` itself. That let `tests/test_metrics.py` unit-test
every case (empty registry, publish counts, group membership, pending
acks, connected clients) without opening a single connection, and kept
the HTTP layer in `_handle_http_connection()` responsible for nothing
more than calling it and serializing the result.

**Decision: `active_connections` is a plain public counter on
`TopicRegistry`, not a new component.** The registry already owns the
bookkeeping every other metric reads from (`_publish_counts`,
`_subscribers`, `_groups`), so a connected-client count belongs there too
rather than in a separate tracker that would need its own wiring.
`handle_connection()` in `server.py` increments it right after a
connection opens and decrements it in the same cleanup block that already
runs `unsubscribe_all()`, so the count can't drift out of sync with
connections that actually closed.

**What's tested (68 tests total, 9 new this milestone):** `build_snapshot()`
unit tests for an empty registry, publish/subscriber counts, group
membership, and pending-ack/connected-client counts; and real-socket
integration tests for a valid-JSON `/metrics` response, the dashboard
HTML route, a 404 for an unknown path, and `/metrics` reflecting a live
connection count from an actual broker connection rather than a
manually-constructed registry.

**Manually verified end-to-end, beyond the automated tests:** started the
real broker and metrics server together as a background process, used
`curl` to fetch the dashboard HTML and the `/metrics` JSON, then ran a
real Python client over a real socket to `SUBSCRIBE` and `PUBLISH`, and
confirmed `/metrics` correctly showed `messages_published: 1` and the
connected-client count changing as the client connected and disconnected.

**What's explicitly NOT done yet:**
- No historical/time-series metrics -- the dashboard shows current
  snapshot state only, polled every 2 seconds; there's no record of what
  the numbers were a minute ago.
- No authentication on the metrics endpoint -- acceptable for a
  local/portfolio deployment, not for a real production system.
- Still not deployed anywhere reachable outside this machine.

## Milestone 7: throughput/latency measurement and final documentation pass

**Decision: measure against a real broker over real sockets, not
estimate.** `benchmarks/throughput.py` starts an actual `asyncio` TCP
server and connects real client sockets to it -- the same "don't mock the
thing you're claiming to measure" rule the test suite has followed since
Milestone 1. There is no synthetic model of the broker's performance
anywhere in this project.

**Decision: measure both with and without persistence, not just the
production configuration.** `broker/storage.py`'s own docstring on
`TopicLog.append()` names per-message `open`/`write`/`flush` as a
plausible bottleneck worth measuring before optimizing. Milestone 7 is
that measurement: running the same benchmark against a `TopicRegistry`
with and without a `LogStore` isolates exactly what persistence costs,
instead of reporting one number and guessing at what's driving it.

**Decision: report a range from three runs, not one number.** The first
run of the throughput benchmark alone produced 63,190 msg/s for the
in-memory scenario; a second run produced 51,672; a third produced
40,625 -- all on the same code, same machine, same benchmark. That
spread is itself the honest finding: this is a shared, virtualized
sandbox, not dedicated hardware, and reporting a single cherry-picked
number would misrepresent the measurement's own precision.
`docs/BENCHMARKS.md` reports all three runs and says so explicitly,
rather than averaging away a result that doesn't fit a clean story.

**What the numbers actually showed:** median publish-to-receive latency
is sub-millisecond either way (roughly 0.06-0.10 ms across all six
latency runs), and persistence visibly costs throughput (roughly
35,000-39,000 msg/s with `LogStore` versus 40,000-63,000 without, across
the same three runs) -- consistent with the per-message synchronous disk
write `storage.py` already documented as the likely cost. Full numbers,
methodology, and limitations are in `docs/BENCHMARKS.md`, not
duplicated here.

**Decision: an ASCII diagram in `docs/architecture.md`, not a generated
image.** Every other design artifact in this repository (JSONL logs,
plain-text protocol docs) is readable directly from the repository
without extra tooling; a text diagram of components and data flow keeps
that property, and is trivial to keep in sync by hand as the system
changes.

**What's explicitly NOT done, project-wide, now that all seven milestones
are complete:** no per-subscriber resume offset, no replay for consumer
groups, no maximum retry count or dead-letter queue, no expiry for an
identified consumer that disconnects and never returns, no
authentication anywhere (wire protocol or metrics endpoint), and no
deployment beyond this machine. Each of these is documented at the
milestone where it was introduced, in `PROTOCOL.md`, or in this file --
listed together here once, at the end, so the full set of known gaps is
visible in one place without pretending any of them are secretly solved.
