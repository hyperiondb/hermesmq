# Security findings

Results of a fuzzing pass over hermesmq 0.8.1, run on 2026-09-04 against commit
`3e1ab8f`, and a record of what each finding does now.

**All nine are fixed.** Each section keeps the original description of the
problem and ends with a **Fixed by** note naming the change. The reproduction
harness was rewritten to run the same attacks against the current code and print
what happens, so the output below each finding is what you get today.

Every numbered finding below has a runnable reproduction:

```sh
cargo run -p hermesmq-core --example security_repro            # overflow checks on
cargo run -p hermesmq-core --example security_repro --release  # what a shipped binary does
```

The example is `crates/hermesmq-core/examples/security_repro.rs`. It opens no
sockets — each case drives the queue state machine, the snapshot decoder, the
rate limiter or the frame reader directly. The section numbers in its output
match the finding numbers here.

## Summary

| # | Finding | Severity | Found by | Status |
| --- | --- | --- | --- | --- |
| 1 | Snapshot contents are never validated, and four counters in the queue overflow | High | fuzz target `snapshot_install` + code reading | Fixed |
| 2 | Any client can grow the replicated state without bound | High | code reading | Fixed |
| 3 | The frame reader reserves the declared length before any body arrives | High | code reading | Fixed |
| 4 | Every control operation is unauthenticated | High | code reading (partly a documented caveat) | Fixed (opt-in token) |
| 5 | A panic inside the state machine disables the node permanently | Medium | reproduction of finding 1 | Fixed |
| 6 | A client-chosen visibility timeout has no upper bound | Medium | code reading | Fixed |
| 7 | An installed snapshot can leave the dedup table unable to evict | Medium | fuzz target `snapshot_install` | Fixed |
| 8 | Retention is not applied when a produce is deduplicated | Low | fuzz target `queue_ops` | Fixed |
| 9 | The rate limiter answers "retry in 0 ms" when asked for zero tokens | Low | fuzz target `rate_limiter` | Fixed |

### What changed, in one table

| Change | Where | Findings it closes |
| --- | --- | --- |
| Decoded snapshot state is validated before it replaces the queue, and refused if it is not self-consistent | `engine/state_machine.rs` `decode_state`, `queue.rs` `Queue::validate` | 1, 7 |
| Every counter the queue adds to uses `saturating_add`, and validation caps each one at 2^62 | `queue.rs` | 1 |
| Mutex guards recover from poisoning instead of panicking again | `lib.rs` `lock`, all call sites | 5 |
| Topic name, group name and producer id are capped at 256 bytes; topics at 10,000; consumer groups at 1,000 per topic | `queue.rs`, checked again on the client port | 2 |
| Retention drops dedup keys with the messages they point at | `queue.rs` `prune_dedup` | 2 |
| Retention and the group sweep run on a deduplicated produce too | `queue.rs` `maintain` | 2, 8 |
| A commit carries the leader clock, so the group it creates expires like any other, and running the sweep no longer needs a poll | `raft.rs` `CommitOffsetAt`, `queue.rs` `commit` | 2 |
| A commit offset is clamped to the end of its topic | `queue.rs` `commit` | 1, 2 |
| The frame reader grows its buffer as bytes arrive, and a body has 30 s to turn up | `frame.rs` | 3 |
| Each listener caps concurrent connections | `client.rs`, `engine/network.rs`, `http.rs` | 3 |
| Optional shared-secret authentication on the client port and the peer port | `client.rs`, `engine/network.rs`, `--client-token` / `--peer-token` | 4 |
| Visibility timeout is clamped to 12 hours | `queue.rs`, `client.rs` | 6 |
| A rate-limiter request for zero tokens is granted rather than refused with a zero delay | `client.rs` `RateLimiter::take` | 9 |

---

## 1. Snapshot contents are never validated, and four counters in the queue overflow

**Severity:** High.

**Where.** `crates/hermesmq-core/src/engine/state_machine.rs`, `install_snapshot`
and `StateMachineStore::new`, both of which call `decode_state`. `decode_state`
checks only that the bytes decode — as the current format when they start with
the two-byte marker `[0xFF, 0x01]`, otherwise as the pre-0.8 format. Nothing
checks what the decoded values mean. The whole decoded state then replaces the
queue.

Four places in `crates/hermesmq-core/src/queue.rs` add one to a counter that a
snapshot can set to any value:

| Line | Statement | Runs on |
| --- | --- | --- |
| `queue.rs:256` | `t.next_offset += 1` | every produce |
| `queue.rs:38` | `self.ack_watermark += 1` (in `mark_acked`) | every ack |
| `queue.rs:74` | `self.ack_watermark += 1` (in `advance_watermark`) | every commit and every retention purge |
| `queue.rs:509` | `*next_lease += 1` | every message delivered by a poll |
| `queue.rs:459` | `g.poll_count += 1` | every poll |

The `snapshot_install` fuzz target found two of these on its own — the message
offset counter at `queue.rs:256` and the poll counter at `queue.rs:459`, each
within about five minutes from an empty corpus. The other three came from
reading the code and were then confirmed the same way.

**How it is reached.** Two paths:

* The snapshot record stored under the key `sm:snapshot`. `StateMachineStore::new`
  reads and decodes it on every start, so a damaged or replaced record on disk
  reaches the decoder. This path is confirmed — it is what the reproduction uses.
* The peer TCP port. `handle_conn` in `crates/hermesmq-core/src/engine/network.rs`
  reads a frame, decodes it as a peer request and passes an `InstallSnapshot`
  variant to `raft.install_snapshot`, whose body ends up in `decode_state`. That
  port has no authentication and no transport encryption (see finding 4).
  openraft gates the RPC on the sender's vote term, which the sender chooses.
  **I did not confirm this path end to end over a socket** — treat it as likely
  rather than demonstrated.

**What happens.** The two build configurations behave differently, and both are
bad:

*Release build, which is what ships* (the workspace release profile does not turn
on overflow checks):

```
snapshot installed with next_offset = u64::MAX
produce returned Produced { offset: 18446744073709551615 }   <- wrapped, no error reported
second produce returned Produced { offset: 0 }
invariants after: Err("topic t: message offset 18446744073709551615 is at or above next_offset 1")
```

The counter wraps silently. Offsets go backwards, so newly produced messages
overwrite older ones at the same offset, the consumer group watermark no longer
means what the rest of the code assumes, and every node in the cluster applies
the same corrupted state because it is replicated.

*Build with overflow checks on* (debug builds, and the builds cargo-fuzz
produces): the addition panics inside `Queue::apply`, which runs while the state
machine mutex is held. See finding 5 for what that does to the node.

**Fuzz coverage.** `fuzz/fuzz_targets/snapshot_install.rs` installs both raw
attacker bytes and a structured legacy state with arbitrary counter values, then
applies ordinary requests on top. It fails on every run of a few minutes, and
will keep failing until the decoded state is validated.

**Fixed by.** `decode_state` now runs `Queue::normalize` and then
`Queue::validate` on the decoded state, and returns a storage error if the state
is not one the queue could have produced itself. Validation covers the counter
ceilings (`next_offset`, `next_lease_id`, `poll_count`, each group's
`ack_watermark`), the relationships between them (a lease id below
`next_lease_id`, a lease offset and an `acked_above` entry below `next_offset`,
a watermark no higher than `next_offset`), the message key matching the offset it
carries, and the dedup map matching its eviction queue. Both callers go through
it, so a damaged record on disk stops the node from starting rather than being
loaded, and a snapshot from a peer is refused before it is persisted.

Separately, all five counter increments now use `saturating_add`, so no build
configuration can wrap or panic even if a state slipped past validation.

`normalize` handles the one shape earlier releases could produce themselves: an
unclamped commit could park a group's watermark past the end of its topic, so
that watermark is lowered back to `next_offset` rather than rejected. Commits are
now clamped when they are applied, so no new state has it.

Running the reproduction:

```
snapshot record written: next_offset = u64::MAX
  refused: ... snapshot state is not self-consistent: topic t: next_offset is 18446744073709551615
snapshot record written: next_lease_id = u64::MAX
  refused: ... snapshot state is not self-consistent: next_lease_id is 18446744073709551615
```

and, on a state built by ordinary requests (8 produces, a poll, 4 acks and a
commit of `u64::MAX`), `validation of the resulting state: Ok(())`.

## 2. Any client can grow the replicated state without bound

**Severity:** High.

Retention bounds the retained *messages* of a topic and nothing else. Three
separate structures grow from ordinary client requests and are never reclaimed.
All three live in the Raft state machine, so they are replicated to every node
and written into every snapshot.

**2a. Consumer groups created by a commit are immortal.**
`commit` in `queue.rs:642` calls `group_entry`, which inserts a new group record
for any group name the request names. A group created this way has
`last_active_ms == 0`, and the idle sweep in `sweep_idle_groups` (`queue.rs:611`)
explicitly keeps groups whose `last_active_ms` is zero. The other reclaim path,
`gc_idle_groups` (`queue.rs:621`), only runs when the topic has no messages at
all. So neither path can ever remove such a group:

```
after 50000 commits with distinct 65-byte group names:
  groups held: 50000
  bytes held in topic+group names: 3438891
after a poll 40 days later (idle sweep + reclaim both run):
  groups still held: 50001
  retained messages: 1
```

Each of those group records also holds the low watermark of the topic, which
blocks consumption-based reclaim of messages for every other consumer.

**2b. The producer dedup table is not covered by retention.**
`produce_one` (`queue.rs:247`) stores each `(producer_id, seq)` key twice — once
in `dedup` and once in `dedup_order` — and caps only the *number* of entries at
100,000. The producer id has no length limit. Retention purges `messages` and
leaves the dedup table alone:

```
retention is set to 1 message; 20000 produces with 1024-byte producer ids:
  retained messages: 1
  dedup entries held: 20000
  bytes held in dedup keys: 41137780
```

The entry cap is 100,000 per topic. With the largest producer id a 2 MiB client
frame allows, that is roughly 400 GB of retained state per topic, held twice
over in the two structures. Filling it costs the attacker about 200 GB of
traffic, so the amplification is close to one to one — what makes it matter is
that the state is permanent, is replicated to every node, and is not covered by
any configured limit.

**2c. Topics are created by any produce and are never freed.**
`produce_one` and `poll` both do `topics.entry(name).or_default()`. Only an
explicit `DeleteTopic` removes a topic:

```
after 20000 produces to distinct topics:
  topics held: 20000
  bytes held in topic names: 1368890
```

**No length limits.** `MAX_PAYLOAD_BYTES` (1 MiB) is checked for the message
payload in `handle_produce` (`client.rs:411`). The topic name, the group name and
the producer id are not checked at all; the only bound on them is the 2 MiB
client frame cap in `crates/hermesmq-core/src/frame.rs`.

**Why authentication would not fix this.** Findings 1, 3 and 4 are about
unauthenticated access. This one is not: an ordinary, fully authorized client
doing nothing unusual — producing to topics it names, committing offsets for
groups it names — can exhaust memory on every node in the cluster and leave the
state permanently larger.

**Fixed by.** Four separate changes, because the finding is four leaks:

* **Names have lengths.** Topic name, group name and producer id are each capped
  at 256 bytes. The check runs in the state machine, which answers
  `AppResponse::Rejected`, and again on the client port, so a bad request costs a
  reply instead of a replicated write.
* **Topics and groups have counts.** 10,000 topics per cluster and 1,000
  consumer groups per topic. A request that would create one past the cap is
  rejected; a poll naming a group past the cap returns nothing rather than
  creating state.
* **Commit-created groups expire.** `CommitOffsetAt` carries the leader's clock,
  so a group created by a commit gets an activity stamp and falls under the same
  7-day idle expiry as one created by a poll. A group restored from a pre-0.8
  snapshot with no stamp is stamped on the next sweep instead of being exempt
  from it, which is what made those groups immortal. The sweep itself now runs on
  produce and commit as well as poll, so a topic nobody polls still reclaims.
* **Retention covers the dedup table.** `prune_dedup` drops `(producer_id, seq)`
  keys whose message retention has already removed, so the table is bounded by
  the retained messages rather than by a 100,000-entry cap on its own.

Running the reproduction:

```
after 50000 commits with distinct 65-byte group names:
  groups held: 1000
after a poll 40 days later (idle sweep + reclaim both run):
  groups still held: 1
a commit with a 1024-byte group name: Rejected { reason: "group name is 1024 bytes; max is 256" }

retention is set to 1 message; 20000 produces with 200-byte producer ids:
  retained messages: 1
  dedup entries held: 1
a produce with a 4096-byte producer id: Rejected { reason: "producer id is 4096 bytes; max is 256" }

after 20000 produces to distinct topics:
  topics held: 10000
the produce that would have made one more topic: Rejected { reason: "topic limit of 10000 reached" }
```

## 3. The frame reader reserves the declared length before any body arrives

**Severity:** High.

**Where.** `crates/hermesmq-core/src/frame.rs`, `read_frame`. It reads a
four-byte big-endian length, rejects it if it exceeds the cap, and then calls
`BytesMut::zeroed(n)` — allocating and zeroing the full declared length — before
reading a single byte of body.

```
4 bytes of header sent, no body at all
  bytes allocated by the reader while it waits: 67109120
  declared frame length: 67108864
```

Four bytes of input reserve up to 64 MiB on a peer connection
(`MAX_PEER_FRAME`) and up to 2 MiB on a client connection (`MAX_CLIENT_FRAME`).

**What makes it worse.** There is no limit on concurrent connections and no read
timeout anywhere. `serve_clients` (`client.rs:997`), `serve_peer`
(`network.rs:46`) and `serve_http` (`http.rs:9`) each accept in a loop and spawn
a task per connection with no semaphore and no deadline. A client that opens
connections and sends four bytes on each holds the memory for as long as it keeps
the socket open.

**Suggested direction** (not implemented): read the body incrementally into a
buffer that grows as bytes actually arrive, cap concurrent connections, and put a
read deadline on every connection.

**Fixed by.** `read_frame` reads the body in 64 KiB chunks into a buffer that
grows as bytes actually arrive, so a declared length costs nothing until the
sender backs it up. The servers call `read_frame_deadline`, which gives a body 30
seconds to arrive once its header has been read; the header itself still waits
indefinitely, because an idle but healthy connection legitimately sends nothing
for long stretches. Each listener also caps concurrent connections - 4,096
client, 256 peer, 256 HTTP - and refuses further ones rather than spawning
unboundedly. The HTTP request head has its own 10-second deadline.

Running the reproduction:

```
4 bytes of header sent, no body at all
  bytes allocated by the reader while it waits: 65920
  declared frame length: 67108864
```

## 4. Every control operation is unauthenticated

**Severity:** High. The README already carries the caveat that the TCP and peer
ports have no authentication and no transport encryption. This entry records what
that actually permits, because the consequences go past eavesdropping.

On the client port, `handle_request` (`client.rs:724`) dispatches every request
kind with no identity check of any sort. Anyone who can open the port can:

* `DeleteTopic` — drop any topic and everything in it.
* `SetRetention` with `max_messages: 1` — destroy retained messages on the next
  produce to that topic.
* `SetRateLimit` — set or remove any topic's rate limit.
* `Ack` / `Commit` — acknowledge or commit past another consumer's messages,
  which drops them.
* `Bootstrap` — `handle_bootstrap` (`client.rs:669`) passes a client-supplied
  member list to `raft.initialize`. On an already-initialized cluster this is
  refused. On a node that has not been bootstrapped yet it succeeds, so whoever
  reaches a fresh node first decides the cluster membership.

On the peer port, `handle_conn` (`network.rs:63`) feeds decoded frames straight
into `raft.append_entries`, `raft.vote` and `raft.install_snapshot`.

**Fixed by.** Both ports take an optional shared secret, off unless set.

* `--client-token` / `HERMESMQ_CLIENT_TOKEN`: a connection must send an `Auth`
  request carrying the token as its first frame. Anything else is answered
  `unauthorized` and the connection closes, so a wrong guess costs a new
  handshake. This gates every request listed above, `Bootstrap` included.
* `--peer-token` / `HERMESMQ_PEER_TOKEN`: a peer presents the token on each
  connection it opens, before any `AppendEntries`, `Vote` or `InstallSnapshot`.
  A connection that sends an RPC before authenticating is closed. The same flag
  sets what a node demands and what it presents, so every node needs the same
  value.

Tokens are compared without a byte-position-dependent early exit. Authentication
is opt-in rather than mandatory so that an existing deployment keeps working
across the upgrade; each port left open logs a warning at startup. The new
protobuf `Auth` message and the new `PeerRequest::Auth` variant are both appended
to their existing definitions, so a node or client on an older build still
decodes everything it used to.

There is still no transport encryption. A token stops an unauthorized host from
acting; it does not hide traffic from one that can read the wire.

## 5. A panic inside the state machine disables the node permanently

**Severity:** Medium (it is what turns finding 1 into a lasting outage).

`StateMachineStore` keeps its data behind `Arc<Mutex<Inner>>` and every accessor
does `.lock().unwrap()` — `apply`, `applied_state`, `metrics`, `rate_config`,
`has_deliverable`, `build_snapshot`, `install_snapshot`. A panic raised while the
lock is held poisons it, and `unwrap()` on a poisoned lock panics too. From then
on every read of the state machine, including the metrics endpoint and the
snapshot builder, panics:

```
snapshot installed with next_offset = u64::MAX
produce panicked (overflow check active in this build)
second produce panicked
state machine is now unusable: its lock was poisoned by the panic
```

Restarting does not help when the cause is the stored snapshot:
`StateMachineStore::new` decodes the same record again.

**Fixed by.** Every `.lock().unwrap()` was replaced with a helper that takes
the guard out of a poisoned lock (`lib.rs::lock`), so one panic no longer turns
every later read of the state machine into a panic. With finding 1 fixed, the
panic that reached the lock in the first place cannot happen from a snapshot, and
with the saturating counters it cannot happen from a counter either - this change
is the backstop for whatever else might panic while the lock is held.

## 6. A client-chosen visibility timeout has no upper bound

**Severity:** Medium.

`handle_poll` (`client.rs:593`) and `handle_subscribe` (`client.rs:809`) both
substitute a 30-second default when the client sends zero and otherwise pass the
value straight through. A lease deadline far in the future keeps its message
undeliverable to anyone else, and it keeps the whole consumer group alive through
the idle sweep, because `sweep_idle_groups` retains any group holding a lease
whose deadline has not passed:

```
one poll with visibility_timeout_ms = u64::MAX leased 1 message(s)
ten years later, after another consumer polls:
  groups still held: 2 (the parked one is not swept)
  messages still held: 1
```

One poll request is enough to park a message and its group for the life of the
cluster.

**Fixed by.** A visibility timeout is clamped to 12 hours, in the state machine
(`poll`) and again on the client port for both `poll` and `subscribe`. 12 hours
is well under the 7-day group idle expiry, so a parked lease can no longer hold
its group past the sweep. Running the reproduction, the parked group is gone ten
years later where it used to survive.

## 7. An installed snapshot can leave the dedup table unable to evict

**Severity:** Medium. Found by the `snapshot_install` fuzz target.

The fuzzer produced snapshot bytes that install a topic whose `dedup` map holds
one entry while its `dedup_order` queue holds none:

```
installed snapshot broke an invariant: topic : dedup has 1 entries but dedup_order has 0
```

The eviction loop in `produce_one` pops from `dedup_order` and removes the popped
key from `dedup`. Entries that are in `dedup` but not in `dedup_order` are
therefore never evicted: the dedup map grows past its 100,000-entry cap
permanently, and the `(producer_id, seq)` pairs it holds are silently treated as
duplicates forever, so those messages are dropped rather than stored.

This shares its root cause with finding 1 — an installed snapshot is trusted
without any consistency check — but the consequence is different, so it is listed
separately.

Note that this specific assertion has since been removed from the fuzz target,
because with no validation in place it fires on almost every run and hides
everything else. The target now checks the weaker property that ordinary requests
applied on top of an installed snapshot do not crash.

**Fixed by.** The same validation as finding 1. `Queue::validate` requires
`dedup` and `dedup_order` to hold the same number of entries, `dedup_order` to
hold no duplicate key, and every `dedup_order` key to be present in `dedup`, so a
snapshot whose eviction queue cannot reach part of the map is refused rather than
installed. The assertion the fuzz target dropped can go back: it now holds.

## 8. Retention is not applied when a produce is deduplicated

**Severity:** Low. Found by the `queue_ops` fuzz target.

`produce_one` returns early when the `(producer_id, seq)` key is already in the
dedup table, and `purge_retained` runs at the end of the function — so a
deduplicated produce never enforces retention:

```
3 messages stored under the default cap
retention lowered to 1 message
a produce that repeats producer p sequence 0 (a duplicate):
  messages retained: 3
a produce with a new sequence number:
  messages retained: 1
```

A topic whose producers only re-send duplicates stays above its configured
message cap indefinitely. Memory cannot grow from this on its own, which is why
it is rated low, but a retention limit set to bound memory does not take effect
when it is expected to.

**Fixed by.** `produce_one` runs the same upkeep on the deduplicated path as on
the storing path: the idle group sweep and `purge_retained`. A topic whose
producers only re-send now enforces its cap on every produce.

Running the reproduction, the duplicate produce leaves 1 retained message where
it used to leave 3.

## 9. The rate limiter answers "retry in 0 ms" when asked for zero tokens

**Severity:** Low, and not reachable from the network today. Found by the
`rate_limiter` fuzz target.

`RateLimiter::take` (`client.rs:185`) computes `grant` as
`tokens.floor().min(want)`. With `want == 0` the grant is zero even on a full
bucket, so it takes the rate-limited branch, where `needed = 1.0 - tokens` is
negative and `(negative as u64)` saturates to zero:

```
take(want=0) on a full bucket -> Err(0)
take(want=1) on the same bucket -> Ok(1)
```

A caller acting on that answer would retry immediately in a loop. Both current
call sites pass at least one — `handle_poll` replaces a zero maximum with a
default, and `handle_subscribe` only calls when the prefetch window is short — so
this is a latent contract violation, not a live one. It is recorded because the
guard is in the callers rather than in the limiter.

**Fixed by.** `RateLimiter::take` returns `Ok(0)` for a request of zero tokens
before it reaches the rate-limited branch, so the contract - a grant of no more
than was asked, or a nonzero retry delay - holds for every input rather than only
for the ones the current callers produce.

---

## What was tested and came back clean

These are the properties the fuzz targets assert that held up. Execution counts
are from a single run per target on one Windows machine; they are a rough measure
of effort, not a coverage guarantee.

| Property | Target | Executions |
| --- | --- | --- |
| The HTTP path parser cannot carry a CR, an LF or whitespace out of the request line, so the path cannot inject a response header | `http_request` | 3,210,684 |
| The peer RPC decoder does not crash or over-allocate on arbitrary bytes | `peer_frame_decode` | 3,140,610 |
| The client protobuf request decoder does not crash | `request_decode` | 2,738,966 |
| The server protobuf response decoder does not crash | `response_decode` | 2,206,924 |
| The rate limiter never grants more than asked, never goes negative or NaN, and never exceeds its burst | `rate_limiter` | 775,082 |
| The log entry decoder does not crash, and a request survives a postcard round trip unchanged | `log_entry_decode` | 799,876 |
| The startup repair leaves a log that starts at the applied base and a commit pointer within reach | `recovery_state` | 549,671 |
| Subscription lease bookkeeping stays consistent across deliver/resolve/count and drains fully | `sub_tracking` | 545,833 |
| The frame codec round-trips, rejects an oversized declared length, and errors on a truncated body | `frame_roundtrip` | 343,466 |
| A snapshot round trip preserves both behaviour and state | `snapshot_roundtrip` | 304,015 |
| Queue bookkeeping stays self-consistent after every request | `queue_ops` | 250,722 |
| The same operation sequence gives byte-identical state on two nodes, and across a snapshot boundary | `queue_determinism` | 216,634 |
| Batched produce and batched ack/nack match the same operations applied one at a time | `batch_equivalence` | 111,267 |
| The log store's merge of pending and flushed entries matches an ordered-map model | `store_model` | 4,402 |

The `queue_ops` and `rate_limiter` rows are clean only after their assertions
were narrowed to what the code actually promises; findings 8 and 9 record what
was narrowed and why. `store_model` runs at roughly fifteen executions per second
because the log store spawns a flusher thread per instance and every truncate and
purge waits on a cross-thread barrier.

## Reproducing

The two builds now print the same thing for every case: the release/debug split
in finding 1 came from overflow checks, and the counters no longer overflow.

```sh
# The deterministic reproductions for every finding above.
cargo run -p hermesmq-core --example security_repro
cargo run -p hermesmq-core --example security_repro --release

# The fuzz targets. See the fuzzing section of README.md for the full list.
# On Windows prefix each of these with RUSTFLAGS="-Clink-arg=/FORCE:UNRESOLVED".
cd fuzz
cargo +nightly fuzz run snapshot_install -- -max_total_time=300
cargo +nightly fuzz run queue_ops        -- -max_total_time=300
```

## Notes on the fuzzing environment

* The run was done on Windows with the MSVC toolchain. Building the fuzz targets
  there needs `RUSTFLAGS="-Clink-arg=/FORCE:UNRESOLVED"`, because `redb` declares
  `crate-type = ["cdylib", "rlib"]`, cargo builds every declared crate type for a
  dependency, and the resulting DLL has no `main` and no libFuzzer runtime to
  resolve the coverage symbols against. This affects only the unused DLL. On
  Linux the flag is unnecessary.
* On this platform libFuzzer did not write the crashing input to an artifact file
  when a target failed. The failures are reported with a source location only,
  which is why every finding here is backed by the deterministic example rather
  than by a corpus artifact.
* `--sanitizer=none` produces binaries that crash during libFuzzer startup on
  this platform. The default address-sanitizer build works and is what was used.
