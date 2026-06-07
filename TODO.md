# hermesmq — TODO

A **simple, Raft-replicated message queue**. A small cluster of nodes runs Raft
([openraft](https://github.com/datafuselabs/openraft)); produces, acks, and offset commits go
through the Raft log so the queue survives node failure. Clients talk over a minimal
length-prefixed **TCP + Protobuf** protocol. A napi-rs binding exposes it to Node.

## Decisions (locked)
- **Language / shape:** Rust core in `hermesmq` (lib + `hermesmqd` binary); napi-rs binding in `hermesmq-node`.
- **Consensus:** openraft (async, tokio-native; membership changes + snapshots built in).
- **Client protocol:** length-prefixed TCP frames carrying **Protobuf** messages (one `Request`/`Response` envelope; message payload is an opaque `bytes` field).
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
| Poll (lease) / ack / nack | **Yes** | poll leases for consumer-group exclusivity + visibility timeout → it is a replicated write; carries `ts_ms` for deterministic expiry. (Leader-local soft leases were the rejected optimization.) |
| In-flight / "leased" message tracking | state machine | derived deterministically from applied log + time |
| Message priority ordering | **Yes** (state machine) | priority is part of the message; lease/poll picks highest first |
| Rate limiting (client/topic/group) | **Yes** | edge token-bucket; cluster-wide, set by `rate_limit`: boolean, `rate`: float per second, can be < 1 |

State machine = the queue: topics → ordered message log, per-group consumer offsets, in-flight
(leased) set with visibility timeout, and a dedup window for idempotent produce.

---

## Unplanned for open source

- [ ] segmented append-only payload store
- [ ] mTLS, auth
- [ ] Granular metrics

---

- [] Optimization: peer client connects per RPC; add connection pooling/keepalive.
- [] Backpressure: per-conn requests are sequential (in-flight = 1); bounded frame size done. `RateLimited` field is in the proto, enforcement = Phase 7.
- [] subscribe side dedup
- [] perf tests (throughput on publish/ subscribe)

---

## Security / correctness caveats

- **TCP + Protobuf has no auth/TLS by itself** — only safe on a trusted private network to start. Add auth (token) + optional TLS before any untrusted exposure. Treat the peer-RPC port as cluster-internal only; firewall it.
- **Determinism in the state machine is mandatory** — no wall-clock, no RNG, no map-iteration-order dependence during `apply()`; otherwise replicas diverge. Carry timestamps/randomness in the log entry.
- **Writes only via the leader** — reads from a follower can be stale; document read semantics (leader-only for read-your-writes).
- At-least-once means consumers must be **idempotent** at the app layer; we provide produce-side dedup, not exactly-once end-to-end.

## Open questions (confirm before building)
- [x] Storage: redb chosen for log/metadata/offsets with payload segment-log for high throughput
- [x] Priority: 8 levels; policy: reserved fraction
- [x] Rate limiting: cluster-wide
- [x] Topics: single ordered log per topic
- [x] Payloads go **through the Raft log**
- [x] consume models: pull (`poll`, long-poll) **and** server push (`subscribe`, leader-driven, lease-based — preserves priority + redelivery; `prefetch`-bounded concurrent processing)
- [x] Cluster size target:  3, no learners/observers are needed.
