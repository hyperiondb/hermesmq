# hermesmq — TODO

A **simple, Raft-replicated message queue**. A small cluster of nodes runs Raft
([openraft](https://github.com/datafuselabs/openraft)); produces, acks, and offset commits go
through the Raft log so the queue survives node failure. Clients talk over a minimal
length-prefixed **TCP + JSON** protocol. A napi-rs binding exposes it to Node.

## Decisions (locked)
- **Language / shape:** Rust core in `hermesmq` (lib + `hermesmqd` binary); napi-rs binding in `hermesmq-node`.
- **Consensus:** openraft (async, tokio-native; membership changes + snapshots built in).
- **Client protocol:** length-prefixed TCP frames carrying JSON (`{"op": ...}`).
- **Delivery target:** at-least-once (default) or at-most-once via per-subscription `ack_mode`; consumer groups (each message to one consumer in a group), explicit ack + redelivery.
- **QoS:** per-message **priority** (higher priority delivered first, with anti-starvation) + **rate limiting** (per client / topic / group).
- **redb**

---

## Architecture (what goes through Raft)
| Concern | Replicated via Raft? | Notes |
|---|---|---|
| Produce (append message to topic) | **Yes** | leader appends, replicates, then acks producer |
| Consumer offset / ack commit | **Yes** | so redelivery survives leader change |
| Multiple topics/ topic create/delete, group create | **Yes** | cluster metadata |
| Cluster membership (add/remove node) | **Yes** | openraft `change_membership` |
| Poll / fetch messages (read) | **No** | served from local applied state on the leader (followers redirect) |
| In-flight / "leased" message tracking | state machine | derived deterministically from applied log + time |
| Message priority ordering | **Yes** (state machine) | priority is part of the message; lease/poll picks highest first |
| Rate limiting (client/topic/group) | **Yes** | edge token-bucket; cluster-wide, set by `rate_limit`: boolean, `rate`: float per second, can be < 1 |

State machine = the queue: topics → ordered message log, per-group consumer offsets, in-flight
(leased) set with visibility timeout, and a dedup window for idempotent produce.

---

## Phase 0 — Scaffold
- [ ] `cargo init` in `hermesmq` (workspace: `hermesmq-core` lib + `hermesmqd` bin).
- [ ] Add deps: `openraft`, `tokio`, `serde`/`serde_json`, chosen storage crate, `tracing`, `anyhow`/`thiserror`.
- [ ] Define core types: `NodeId`, `TopicId`, `MessageId`, `Offset`, `GroupId`, the `AppRequest`/`AppResponse` Raft application types.
- [ ] Scaffold `hermesmq-node` (napi-rs), mirroring the `mqtt-broker` / `node-addon` layout. Wire cargo + napi build.

## Phase 1 — Storage layer (behind a trait)
- [ ] Define `Storage` trait: Raft log append/read-range/truncate/purge, save/read vote + committed, KV for metadata/offsets.
- [ ] Implement with **redb** (single data dir; tables for log, vote, meta, offsets).
- [ ] fsync/durability policy: configurable (per-append fsync vs. group-commit batching) — default group-commit.
- [ ] (Later) segmented append-only log for message payloads; expose read by `(topic, offset)`.

## Phase 2 — Raft engine (openraft)
- [ ] Implement `RaftLogStorage` over the `Storage` trait.
- [ ] Implement `RaftStateMachine`: `apply()` the queue ops; track last-applied; build/install snapshots.
- [ ] Implement `RaftNetwork` (AppendEntries / Vote / InstallSnapshot) over the inter-node transport (Phase 4).
- [ ] Wire `Raft::new(...)`, config (election timeout, heartbeat, snapshot policy, max payload entries).
- [ ] Single-node bootstrap (`initialize`) → verify it elects itself leader and applies an entry.

## Phase 3 — Queue state machine
- [ ] Topic log: append message → assign monotonic `Offset`; store in state machine.
- [ ] Consumer groups: per-`(topic, group)` committed offset + in-flight (leased) set.
- [ ] `produce`: dedup by `(producer_id, seq)` within a window → idempotent re-sends are no-ops.
- [ ] `poll(topic, group, max)`: lease the next N un-acked messages with a **visibility timeout**; return them + lease ids.
- [ ] **Priority:** messages carry a `priority`; `poll` leases highest-priority-first, then by offset within a level (stable, deterministic). See Phase 7 for levels + anti-starvation.
- [ ] `ack(lease_id)`: advance committed offset / remove from in-flight.
- [ ] `nack` / lease expiry: message becomes redeliverable (drives at-least-once + redelivery).
- [ ] Determinism: visibility timeout / expiry must be evaluated against a value carried **in the log** (e.g. leader's apply timestamp), not wall-clock at apply time, so all replicas agree.

## Phase 4 — Inter-node transport & membership
- [ ] Length-prefixed framing shared with the client protocol; separate listener/port for peer RPC.
- [ ] Serve openraft RPCs (append/vote/snapshot) over it; client `RaftNetwork` dials peers.
- [ ] `add_learner` + `change_membership` flow; config: `node_id`, `peers`, data dir, listen addrs.
- [ ] Cluster bootstrap (first node `initialize`) and join (new node as learner → voter).

## Phase 5 — Client protocol (TCP + JSON)
- [ ] Length-prefixed frame codec (`u32` len + JSON body); request/response + server-push for streaming consumers.
- [ ] Ops: `produce` (optional `priority`), `subscribe`/`poll` (`ack_mode`: `manual`|`auto`), `ack`, `nack`, `commit`, `create_topic`, `delete_topic`, `stats`.
- [ ] **Leader awareness:** writes must hit the leader. On a follower, return `NotLeader{leader_addr}` so the client redirects (or auto-forward). Client caches the leader.
- [ ] Backpressure / max in-flight per connection; bounded frame size.
- [ ] Errors as structured JSON (`{"error": {...}}`); never panic the connection task.
- [ ] On limit breach return `RateLimited{retry_after_ms}` (don't drop the connection); see Phase 7.

## Phase 6 — Delivery semantics (validate end-to-end)
- [ ] At-least-once confirmed: produce ack only after Raft commit; consumer ack only after Raft commit.
- [ ] `ack_mode`: `manual` = ack after processing (**at-least-once**, default); `auto` = lease acked on delivery before the handler runs (**at-most-once**, may lose on crash). Validate both: crash mid-handler → `manual` redelivers, `auto` does not.
- [ ] Consumer groups: with N consumers in a group, each message delivered to exactly one (load-balanced via leasing).
- [ ] Redelivery on consumer crash (lease expiry) → message handed to another consumer.
- [ ] Idempotency: duplicate produce (same `producer_id`+`seq`) does not create a second message.
- [ ] Ordering: per-topic FIFO for a single consumer; document the (relaxed) ordering under groups/redelivery.

## Phase 7 — QoS: priority & rate limiting
- [ ] **Priority levels:** fixed small set (e.g. `0=low..3=high`) so lease selection stays O(levels); define default.
- [ ] **Anti-starvation:** aging or reserved-fraction policy so low-priority still drains; must be deterministic if evaluated in `apply()`.
- [ ] **Rate limiting (token bucket):** per connection/client, per topic (produce), per group (consume); configurable rate + burst.
- [ ] **Scope:** local per-node by default (no coordination). Cluster-wide = either approximate (quota ÷ node count) or replicate counters via Raft — see open Q.
- [ ] **Enforcement:** over-limit producers get `RateLimited{retry_after_ms}`; over-limit consumers get throttled `poll` (backpressure), never dropped messages.
- [ ] Admin: set/update per-topic/group limits + priority policy (replicated as metadata via Raft).
- [ ] Per-priority + per-limit metrics.

## Phase 8 — Server binary (`hermesmqd`)
- [ ] Config via file + env + flags (node id, peers, data dir, client/peer listen addrs, durability, retention).
- [ ] Startup: open storage → start Raft → bind peer listener → bind client listener.
- [ ] Graceful shutdown (drain client conns, flush, step down if leader).
- [ ] `hermesctl` (or subcommands) for: bootstrap, add-node, remove-node, topic CRUD, stats.

## Phase 9 — Node addon (`hermesmq-node`)
- [ ] napi-rs client binding: `connect(addr)`, `produce({topic, body, key?})`, `subscribe({topic, group, ackMode?}, onMessage)`, `ack(id)`.
- [ ] `ackMode` defaults to `manual` (at-least-once; handler must call `ack(id)` when done); `auto` acks on delivery (at-most-once).
- [ ] Threadsafe JS callbacks for consumer delivery; async produce returns a Promise resolving on commit.
- [ ] Auto leader-redirect handled inside the addon (transparent to JS).
- [ ] Optional: `embed(config)` to run a `hermesmqd` node in-process (parity with mqtt-broker's embedded model).
- [ ] Prebuild for target platform(s).

## Phase 10 — Snapshots, compaction, retention
- [ ] openraft snapshot: serialize state machine (offsets, in-flight, topic logs or pointers); install on lagging followers.
- [ ] Log compaction / purge after snapshot.
- [ ] Message retention: by size and/or age per topic; purge acked-and-aged messages from payload storage.
- [ ] Verify a freshly-joined node catches up via snapshot + log tail.

## Phase 11 — Observability & ops
- [ ] `tracing` structured logs; per-op spans.
- [ ] Metrics: produce/consume throughput, commit latency, Raft term/leader, replication lag, in-flight count, redelivery rate, storage size.
- [ ] Health/readiness (storage open, Raft up, listeners bound, is-leader).
- [ ] Leader/term change events surfaced in logs + metrics.
- [] Docker setup

## Phase 12 — Testing
- [ ] Unit: state machine ops (produce/poll/ack/nack/expiry/dedup) are deterministic.
- [ ] Single-node integration over the real TCP protocol (produce → consume → ack).
- [ ] 3-node cluster: leader election, replication, produce on leader → consume after leader kill.
- [ ] Chaos/fault injection: kill leader mid-produce, partition a node, slow disk → assert no message loss / no double-ack-as-loss.
- [ ] Property test: at-least-once + dedup ⇒ every produced message consumed exactly once at the app layer.
- [ ] Restart durability: kill all nodes, restart, state recovers from storage + snapshot.
- [ ] Priority: high-priority drains before low under load; low-priority still drains (no starvation).
- [ ] Rate limiting: over-limit producer gets `RateLimited`; sustained throughput stays within the configured bucket.

---

## Security / correctness caveats
- **TCP + JSON has no auth/TLS by itself** — only safe on a trusted private network to start. Add auth (token) + optional TLS before any untrusted exposure. Treat the peer-RPC port as cluster-internal only; firewall it.
- **Determinism in the state machine is mandatory** — no wall-clock, no RNG, no map-iteration-order dependence during `apply()`; otherwise replicas diverge. Carry timestamps/randomness in the log entry.
- **Writes only via the leader** — reads from a follower can be stale; document read semantics (leader-only for read-your-writes).
- At-least-once means consumers must be **idempotent** at the app layer; we provide produce-side dedup, not exactly-once end-to-end.

## Open questions (confirm before building)
- [x] Storage: redb chosen for log/metadata/offsets with payload segment-log for high throughput
- [x] Priority: 8 levels; policy: reserved fraction
- [x] Rate limiting: cluster-wide
- [x] Topics: single ordered log per topic
- [x] Payloads go **through the Raft log**
- [x] pull (`poll`) as the primary consume model
- [x] Cluster size target:  3, no learners/observers are needed.
