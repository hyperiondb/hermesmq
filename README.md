# HermesMQ

A **Raft-replicated message queue**. A cluster of nodes runs Raft
([openraft](https://github.com/datafuselabs/openraft)); produces, acks, and consumer offsets all go
through the replicated log, so the queue survives node failure. Clients talk a compact
length-prefixed **TCP + Protobuf** protocol.

Status: **in progress**

## Features

- [x] Easy, fast cluster setup — **client-driven bootstrap** (cluster forms from the node list the client supplies)
- [x] Ordered message log per topic (Kafka-like), payloads replicated through Raft
- [x] Consumer groups — one consumer per message within a group; fan-out across groups
- [x] Two consume styles — server **push** (`subscribe`, no client loop) or **pull** (`poll`, long-poll)
- [x] Per-group consumer offsets + in-flight (leased) tracking with visibility timeout
- [x] At-least-once (default) or at-most-once via per-subscription `ack_mode`; `ack`/`nack` + redelivery
- [x] 8-level priority with reserved-fraction anti-starvation
- [x] Cluster-wide rate limits (token bucket per topic; `rate` may be < 1/s)
- [x] Retention by message count and/or age
- [x] Idempotent produce (dedup by `producer_id` + `seq`)
- [x] Configuration via the client (topics, rate limits, retention)
- [x] Observability — `/health`, `/ready`, Prometheus `/metrics`
- [x] Pure-Rust build — no `protoc` (protobuf codegen via `protox` + `prost-build`)

## Workspace layout

| Crate | Role |
|---|---|
| `crates/hermesmq` | Umbrella library — re-exports `hermesmq-core` (`cargo add hermesmq`) |
| `crates/hermesmq-proto` | Protobuf wire types (`prost`), shared by server and clients |
| `crates/hermesmq-core` | Storage (redb), Raft engine, queue state machine, TCP/protobuf + HTTP servers |
| `crates/hermesmqd` | The server daemon binary |

## Build

```sh
cargo build --release
cargo test            # 33 tests: unit + client-protocol + cluster + durability + http + slow-disk
```

## Run a node

```sh
hermesmqd \
  --node-id 1 \
  --data-dir ./data1 \
  --client-addr 127.0.0.1:7600 \
  --peer-addr   127.0.0.1:7700 \
  --metrics-addr 127.0.0.1:9600
```

A freshly started node waits for a **client to bootstrap** it. For a multi-node cluster, start each
node (no special flags), then have a client send a `Bootstrap` with the full node list (the Node
addon's `connect()` does this automatically).

| Flag | Default | Purpose |
|---|---|---|
| `--node-id` | `1` | Unique node id |
| `--data-dir` | `data` | redb data directory |
| `--client-addr` | `127.0.0.1:7600` | Client protobuf/TCP listener |
| `--peer-addr` | `127.0.0.1:7700` | Inter-node Raft RPC listener |
| `--metrics-addr` | `127.0.0.1:9600` | HTTP `/health` `/ready` `/metrics` |

Environment variable `RUST_LOG` controls log verbosity (e.g. `RUST_LOG=info`). Example `- RUST_LOG=info,openraft=warn`

## Run a 3-node test cluster with Docker

```sh
docker compose up -d --build
docker compose ps          # all three healthy
```

This starts `hermesmq1/2/3` (client ports `7600/7601/7602`, metrics `9600/9601/9602`); a client
bootstraps them with the peer addresses (`hermesmq1:7700`, …).

## Node.js client

```sh
npm install hermesmq-node
```

`connect(nodes)` returns a `Client` and **auto-bootstraps** the cluster from the node list. Every
method takes a single options object and returns a `Promise`.

```js
import { connect } from "hermesmq-node";

const client = await connect([
  { id: 1, clientAddr: "127.0.0.1:7600", peerAddr: "hermesmq1:7700" },
  { id: 2, clientAddr: "127.0.0.1:7601", peerAddr: "hermesmq2:7700" },
  { id: 3, clientAddr: "127.0.0.1:7602", peerAddr: "hermesmq3:7700" },
]);
```

### Topics vs. consumer groups

You **produce to a topic** — there is no group on the produce side. A **consumer group** is a
consume-side label: each group reads the whole topic independently (fan-out *across* groups), while
consumers *within* one group split the messages between them (work queue). Groups aren't created
explicitly — a group springs into existence the first time you `poll` with that name (`"workers"`
below is just a name you picked). Same split as Kafka: produce → topic; consume → topic + group.

### Methods

**`createTopic(options)`** — create a topic (idempotent) and configure it. `rateLimit` and
`retention` are **per-topic** and optional; set them here once, not on every publish.
```js
await client.createTopic({
  topic: "orders",
  rateLimit: { ratePerSec: 100, burst: 200 },                 // optional: cluster-wide token bucket
  retention: { maxMessages: 1_000_000, maxAgeMs: 86_400_000 },// optional: keep <= 1M messages or <= 24h
});
```

**`produce(options) → offset`** — append a message to a topic; returns the assigned offset (string).
`priority` is **per-message** (each message carries its own).
```js
const offset = await client.produce({
  topic: "orders",
  body: Buffer.from("hello"),  // payload is opaque bytes
  priority: 0,                 // 0 = lowest .. 7 = highest (default 0)
});
```

**`poll(options) → messages[]`** — lease up to `max` deliverable messages for a `(topic, group)`.
With `waitMs > 0` it **long-polls**: the server parks the request (no Raft writes while idle) and
returns as soon as a message is available, or empty after `waitMs`. With `waitMs = 0` it returns
immediately. Each message is leased for `visibilityMs`; ack before it expires or it is redelivered.
```js
const msgs = await client.poll({
  topic: "orders",
  group: "workers",
  max: 10,               // optional (default 16)
  visibilityMs: 30_000,  // optional (default 30000)
  waitMs: 20_000,        // optional (default 0 = no wait); long-poll up to 20s
});
// each: { leaseId, offset, priority, contentType, payload: Buffer, tsMs }  (ids are strings)
```

**`subscribe(options, onMessage) → Subscription`** — server-driven **push**: the leader streams
deliverable messages to your handler as they arrive (priority-ordered, no client loop, no idle Raft
writes). Returns a `Subscription` with `unsubscribe()`. `onMessage` may be `async`.
```js
const sub = await client.subscribe(
  {
    topic: "orders",
    group: "workers",
    prefetch: 16,         // optional: max messages in flight before acks (default 16)
    visibilityMs: 30_000, // optional (default 30000)
    ackMode: "manual",    // optional: "manual" (default) acks after onMessage; "auto" acks on delivery
  },
  async (m) => {
    await handle(m.payload); // m: { leaseId, offset, priority, contentType, payload: Buffer, tsMs }
  },
);
// later:
sub.unsubscribe();
```

**`ack(lease)`** — mark a leased message done so it is not redelivered.
```js
await client.ack({ topic: "orders", group: "workers", leaseId: m.leaseId });
```

**`nack(lease)`** — release a lease now so the message is redelivered immediately (don't wait for the timeout).
```js
await client.nack({ topic: "orders", group: "workers", leaseId: m.leaseId });
```

**`stats() → { lastApplied, currentLeader }`** — Raft applied index + current leader node id.
```js
const { lastApplied, currentLeader } = await client.stats();
```

**`bootstrap()`** — (re)form the cluster from the node list. `connect()` already calls it; only needed
to re-bootstrap manually. Idempotent.
```js
await client.bootstrap();
```

### Writing a consumer

**Push (recommended)** — `subscribe`: the server streams messages to your handler. No loop, no
busy-spin, **no Raft writes while idle**. With `ackMode: "manual"` (default) the message is acked
after `onMessage` resolves and **nacked if it throws** (redelivered) — at-least-once. Up to
`prefetch` messages are processed concurrently, so one slow/stuck handler doesn't block the others.

```js
const sub = await client.subscribe(
  { topic: "orders", group: "workers", prefetch: 16 },
  async (m) => {
    await handle(m.payload); // ack on success, nack (redeliver) on throw — automatic
  },
);
// sub.unsubscribe() to stop.
```

For at-most-once push, pass `ackMode: "auto"` (acked on delivery; a crash mid-handler drops the message).

**Pull (alternative)** — long-poll with `waitMs`: the call blocks server-side until a message
arrives, then you `ack`/`nack` yourself. Useful when you want explicit control over fetching:

```js
while (running) {
  const msgs = await client.poll({ topic: "orders", group: "workers", waitMs: 20_000 });
  for (const m of msgs) {
    try {
      await handle(m.payload);
      await client.ack({ topic: "orders", group: "workers", leaseId: m.leaseId });
    } catch {
      await client.nack({ topic: "orders", group: "workers", leaseId: m.leaseId });
    }
  }
}
```

The client auto-discovers the leader (rotates through nodes on `not_leader`/unreachable). 64-bit ids
(`offset`, `leaseId`, `tsMs`) are returned as **strings** to avoid JS `2^53` precision loss.

## Protocol

Length-prefixed frames (`u32` big-endian length + Protobuf body). One `Request`/`Response` envelope
(`hermesmq.proto`); the message payload is an opaque `bytes` field. Ops: `bootstrap`, `produce`,
`poll`, `subscribe`, `ack`, `nack`, `commit`, `create_topic`, `delete_topic`, `set_rate_limit`,
`set_retention`, `stats`. `subscribe` takes over its connection: the leader pushes `Delivered`
frames and reads `ack`/`nack` frames back on the same socket. Inter-node Raft RPC uses the same
framing with postcard-encoded openraft messages.

## Observability

- `GET /health` → `200 ok` (liveness)
- `GET /ready`  → `200` if the node sees a leader, else `503` (readiness)
- `GET /metrics` → Prometheus text: Raft term/leader, last-applied, last-log-index, replication lag,
  topics, messages, in-flight.

## Delivery semantics

- **At-least-once** (default): `subscribe` (push) acks after your handler resolves, or `poll`+`ack`
  (pull). If a lease's visibility timeout expires without an ack, the message is redelivered — so
  consumers must be **idempotent**. Both paths preserve priority ordering and per-`(topic, group)`
  redelivery.
- **At-most-once**: `subscribe` with `ackMode: "auto"`, or `poll` with `ackMode: "auto"` — acked on
  delivery.
- **Dedup**: provide `producer_id`/`seq`; re-sends within the dedup window return the original offset.
- **Quorum**: a 3-node cluster tolerates 1 failure for full read/write/consume availability; losing 2
  stops writes by design (no split-brain). Run 5 nodes to tolerate 2 failures.

## Testing

`cargo test` covers: queue semantics (unit + a `proptest` property), the real TCP/protobuf protocol,
3-node replication, leader/follower loss, quorum-loss safety, network partition + heal, on-disk
restart durability, slow-disk tolerance, and the HTTP endpoints.

## Caveats

- The TCP and peer-RPC ports have **no auth/TLS** yet — only run on a trusted/private network. Add a
  token + TLS before any untrusted exposure, and firewall the peer port to cluster hosts only.
- Retention is **Kafka-style**: it drops messages by age/size regardless of consumption, so a lagging
  group can lose un-consumed messages. Set generous retention if that matters.
- Reads (`poll`) go through the leader; followers redirect.

## License

GPL-3.0-or-later
