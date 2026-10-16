# HermesMQ

[![Code health](https://api.repowise.dev/badge/health/hyperiondb/hermesmq.svg)](https://repowise.dev/repo/hyperiondb/hermesmq)

The simple af, performant, durable Kafka-like **Raft-replicated message queue**.

Status: **in production**

## Features

- [x] Easy, fast cluster setup — **client-driven bootstrap** (cluster forms from the node list the client supplies)
- [x] Ordered message log per topic (Kafka-like), payloads replicated through Raft
- [x] Consumer groups — one consumer per message within a group; fan-out across groups
- [x] Two consume styles — server **push** (`subscribe`, no client loop) or **pull** (`poll`, long-poll)
- [x] Per-group consumer offsets + in-flight (leased) tracking with visibility timeout
- [x] At-least-once (default) or at-most-once via per-subscription `ack_mode`; `ack`/`nack` + redelivery
- [x] Consumer-side dedup on `subscribe` — slow handlers get lease auto-refresh instead of same-connection duplicates; late acks still count
- [x] 8-level priority with reserved-fraction anti-starvation
- [x] Cluster-wide rate limits (token bucket per topic; `rate` may be < 1/s) — paces **delivery** only; produce is never throttled, the backlog absorbs bursts
- [x] Retention by message count and/or age
- [x] Idempotent produce (dedup by `producer_id` + `seq`)
- [x] Configuration via the client (topics, rate limits, retention)
- [x] Observability — `/health`, `/ready`, Prometheus `/metrics`
- [x] Pure-Rust build — no `protoc` (protobuf codegen via `protox` + `prost-build`)

## Highly subjective performance (0.2.0)

...testing on local machine, (i5, 32GB RAM) w/o network, etc. bottlenecks

![HermesMQ performance](https://github.com/hyperiondb/hermesmq/blob/main/performance.png?raw=true)

The chart is regenerated from the measured numbers on every `cargo perf` run.

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
cargo test # unit + client-protocol + cluster + durability + http + slow-disk
cargo e2e  # end-to-end against a 3-node Docker cluster (needs Docker + compose v2)
cargo perf # throughput and latency, release only; regenerates performance.png
```

`cargo test` skips the last two: both are marked `#[ignore]` so an ordinary test run does not
require Docker or spend time measuring. See [Testing](#testing) for what each one does.

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

| Flag | Env var | Default | Purpose |
|---|---|---|---|
| `--node-id` | `HERMESMQ_NODE_ID` | `1` | Unique node id |
| `--data-dir` | `HERMESMQ_DATA_DIR` | `data` | redb data directory |
| `--client-addr` | `HERMESMQ_CLIENT_ADDR` | `127.0.0.1:7600` | Client protobuf/TCP listener |
| `--peer-addr` | `HERMESMQ_PEER_ADDR` | `127.0.0.1:7700` | Inter-node Raft RPC listener |
| `--metrics-addr` | `HERMESMQ_METRICS_ADDR` | `127.0.0.1:9600` | HTTP `/health` `/ready` `/metrics` |
| `--metrics-enabled` | `HERMESMQ_METRICS_ENABLED` | `true` | `false` disables Prometheus `/metrics` (`/health` and `/ready` stay on) |
| `--client-token` | `HERMESMQ_CLIENT_TOKEN` | unset | Shared secret a client must send before any request is served |
| `--peer-token` | `HERMESMQ_PEER_TOKEN` | unset | Shared secret every peer connection must present, in both directions |

Every flag can also be set via its environment variable; a CLI flag takes precedence. The Docker
image bakes in container-appropriate defaults (`0.0.0.0` listeners, `/data` data dir), so a
container only needs `HERMESMQ_NODE_ID`.

Environment variable `RUST_LOG` controls log verbosity (e.g. `RUST_LOG=info`). Example `- RUST_LOG=info,openraft=warn`

## Run a 3-node test cluster with Docker

```sh
docker compose up -d --build
docker compose ps # all three healthy
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
`retention` are **per-topic** and optional; set them here once, not on every publish. The rate
limit applies to delivery (poll/subscribe), never to produce: bursts queue up and drain to
consumers at `ratePerSec`.
```js
await client.createTopic({
  topic: "orders",
  rateLimit: { ratePerSec: 100, burst: 200 },                 // optional: cluster-wide token bucket
  retention: { maxMessages: 1_000_000, maxAgeMs: 86_400_000 },// optional: keep <= 1M messages or <= 24h
});
```

**`produce(options) → offset`** — append a message to a topic; returns the assigned offset (string).
`priority` is **per-message** (each message carries its own). Optional `producerId` + `seq` (a
per-producer monotonic counter) make retries idempotent: a re-send with the same pair returns the
original offset instead of appending a duplicate. All produces share one pipelined connection to
the leader (up to 32 in flight) and are group-committed, so concurrent produces scale to thousands
of msg/s while a serial `await` loop is bound to one Raft round per message.
```js
const offset = await client.produce({
  topic: "orders",
  body: Buffer.from("hello"),  // payload is opaque bytes
  priority: 0,                 // 0 = lowest .. 7 = highest (default 0)
  producerId: "billing-7f3a",  // optional: enables dedup; requires seq
  seq: 42,                     // optional: per-producer monotonic counter
});
```

**`produceMany(items) → results[]`** — produce a batch concurrently through the pipeline; returns
per-item `{ offset?, error? }` aligned with the input, so partial failures are visible. Pair with
`producerId`/`seq` and retry only the failed items with their original seqs — items that already
committed dedup to their original offsets.
```js
const results = await client.produceMany(
  orders.map((order, i) => ({
    topic: "orders",
    body: Buffer.from(JSON.stringify(order)),
    producerId: "billing-7f3a",
    seq: base + i,
  })),
);
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
(`hermesmq.proto`); the message payload is an opaque `bytes` field. Ops: `auth`, `bootstrap`,
`produce`, `poll`, `subscribe`, `ack`, `nack`, `commit`, `create_topic`, `delete_topic`,
`set_rate_limit`, `set_retention`, `stats`. `auth` is only required when the server was started with
`--client-token`, and must then be the first frame on the connection; against a server with no token
it is answered `ok` and changes nothing. `subscribe` takes over its connection: the leader pushes `Delivered`
frames and reads `ack`/`nack` frames back on the same socket. Inter-node Raft RPC uses the same
framing with postcard-encoded openraft messages (one persistent connection per peer).

Server-side limits and defaults (applied when a field is `0`): client frames are capped at
**2 MiB** and peer-RPC frames at 64 MiB, produce payloads are capped at
**1 MiB** (`payload_too_large` otherwise), poll `max` defaults to 16 (capped at 1024),
`visibility_timeout_ms` defaults to 30 000 and is capped at 12 hours, `wait_ms` is capped at
300 000, and `priority` is clamped to 0–7. Topic names, group names and producer ids are capped at
256 bytes each (`bad_request` otherwise), and a request that would push the cluster past 10 000
topics or a topic past 1 000 consumer groups is answered `rejected`. `not_leader` errors include the
current leader's peer address in `leader_addr` when one is known.

Requests may be **pipelined**: a client can send further frames without waiting for responses
(up to 32 are processed concurrently per connection; beyond that, TCP backpressure applies).
Responses are always written in request order — there are no request ids. Pipelined produces are
processed concurrently, so their offsets may not match submission order; don't pipeline a
long-poll ahead of requests whose responses you need promptly. A `subscribe` frame waits for all
pending responses to drain, then takes over the connection as before.

Concurrent produces (pipelined or across connections) are **group-committed**: the node coalesces
them into a single replicated log entry and one fsync, so produce throughput scales with the
number of in-flight requests instead of paying a full Raft round per message. Up to 4 batches are
replicated concurrently, so a sustained burst pipelines Raft rounds instead of serializing them
(batches may commit out of order — per-produce offsets are unaffected). A produce only ever
returns after its batch is durable and replicated — semantics are unchanged.

## Observability

- `GET /health` → `200 ok` (liveness)
- `GET /ready`  → `200` if the node sees a leader, else `503` (readiness)
- `GET /metrics` → Prometheus text: Raft term/leader, last-applied, last-log-index, replication lag,
  topics, messages, in-flight. Disable with `HERMESMQ_METRICS_ENABLED=false` (or
  `--metrics-enabled false`) — the endpoint then returns `404` while `/health` and `/ready` keep
  working.

## Delivery semantics

- **At-least-once** (default): `subscribe` (push) acks after your handler resolves, or `poll`+`ack`
  (pull). If a lease's visibility timeout expires without an ack, the message is redelivered — so
  consumers must be **idempotent**. Both paths preserve priority ordering and per-`(topic, group)`
  redelivery.
- **Consumer-side dedup (`subscribe`)**: while a subscription connection is alive, a message whose
  visibility timeout expires mid-handler is **not** re-pushed to that connection — the server
  auto-refreshes the lease (up to 2 times) and a late ack for the original lease still completes
  the message. After the refresh cap (~3× `visibilityMs`) the message is redelivered as usual, and
  if the connection dies its leases expire normally — at-least-once is preserved, so consumers must
  still be idempotent across reconnects and consumer failover. Pull (`poll`) consumers can dedup by
  `offset`.
- **At-most-once**: `subscribe` with `ackMode: "auto"`, or `poll` with `ackMode: "auto"` — acked on
  delivery.
- **Dedup**: provide `producer_id`/`seq`; re-sends within the dedup window return the original offset.
- **Quorum**: a 3-node cluster tolerates 1 failure for full read/write/consume availability; losing 2
  stops writes by design (no split-brain). Run 5 nodes to tolerate 2 failures.

## Testing

```bash
cargo test
```

### End-to-end (Docker)

```sh
cargo e2e
```

This is **one test that runs a full cluster lifecycle** (so the runner reports `1 passed` —
that's expected), printing its progress step by step (`[e2e   12.3s] ...`): it builds the image,
starts a dedicated 3-node compose cluster (`docker-compose.e2e.yml`, host ports 17600-17602 /
19600-19602), bootstraps it over the wire, exercises produce/dedup/priority/poll/ack, kills the
leader container, verifies failover and that un-acked messages survive, restarts the killed node,
waits for catch-up, checks `/metrics`, and tears the cluster down (also on failure). Requires
Docker with compose v2. The first run builds the image and can take several minutes; later runs
reuse the Docker cache. (`cargo e2e` is an alias from `.cargo/config.toml` for
`cargo test -p hermesmq-core --test e2e_docker -- --ignored --nocapture`.)

### Performance

```sh
cargo perf
```

Prints throughput and latency percentiles, and asserts loose floors (release builds only) to catch
catastrophic regressions: queue state-machine ops, sequential / concurrent / pipelined produce
against a single fsync-backed node, poll/ack drain, subscribe push, produce-to-delivery push tail
latency (p50/p99/p99.9), and 3-node replicated writes.
(Alias for `cargo test -p hermesmq-core --release --test perf -- --ignored --nocapture --test-threads=1`.)

### Fuzzing

Nightly + [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz). Every target is bounded so it
exits. Run from the `fuzz/` directory.

Decoders fed untrusted bytes:

```sh
cargo +nightly fuzz run request_decode    -- -max_total_time=60   # client Request (protobuf, server-side)
cargo +nightly fuzz run response_decode   -- -max_total_time=60   # server Response (protobuf, client-side)
cargo +nightly fuzz run peer_frame_decode -- -max_total_time=60   # peer RPC (postcard, unauthenticated port)
cargo +nightly fuzz run log_entry_decode  -- -max_total_time=60   # one log entry read back out of redb
cargo +nightly fuzz run http_request      -- -max_total_time=60   # HTTP request-line parser
```

Queue state machine, driven by generated request sequences:

```sh
cargo +nightly fuzz run queue_ops          -- -max_total_time=60  # internal invariants after every step
cargo +nightly fuzz run queue_determinism  -- -max_total_time=60  # two nodes must reach identical state
cargo +nightly fuzz run snapshot_roundtrip -- -max_total_time=60  # a restored node must behave the same
cargo +nightly fuzz run snapshot_install   -- -max_total_time=60  # attacker-shaped snapshot, then ordinary requests
cargo +nightly fuzz run batch_equivalence  -- -max_total_time=60  # batched ops == the same ops one at a time
```

Storage, framing and per-connection bookkeeping:

```sh
cargo +nightly fuzz run store_model     -- -max_total_time=60     # log store vs. an ordered-map model
cargo +nightly fuzz run recovery_state  -- -max_total_time=60     # startup repair over a damaged store
cargo +nightly fuzz run frame_roundtrip -- -max_total_time=60     # length-prefixed codec
cargo +nightly fuzz run sub_tracking    -- -max_total_time=60     # subscription lease bookkeeping
cargo +nightly fuzz run rate_limiter    -- -max_total_time=60     # token bucket float math
```

For the targets that decode untrusted bytes, add `-rss_limit_mb=2048 -malloc_limit_mb=512` so an
oversized allocation is reported as a crash instead of an out-of-memory kill.

Findings from the last fuzzing run are in [VULN.md](VULN.md), together with what each one does now.
The reproduction harness runs every case against the current code:

```sh
cargo run -p hermesmq-core --example security_repro
```

**On Windows (MSVC)** the build needs `RUSTFLAGS="-Clink-arg=/FORCE:UNRESOLVED"`: `redb` declares
`crate-type = ["cdylib", "rlib"]`, cargo builds every declared crate type for a dependency, and that
unused DLL cannot resolve the coverage symbols or `main`. The flag is not needed on Linux, and
`--sanitizer=none` produces binaries that crash at libFuzzer startup on Windows — use the default
address-sanitizer build.

## Authentication

Both TCP ports take an optional shared secret. Authentication is **off unless a token is set**, and
the server logs a warning at startup for each port left open.

```sh
hermesmqd --client-token "$CLIENT_SECRET" --peer-token "$PEER_SECRET"
```

- **Client port.** With `--client-token` set, a connection must send an `Auth` request carrying the
  token as its first message. Every other request is answered `unauthorized` and the connection is
  closed, so guessing costs a new handshake each time.
- **Peer port.** With `--peer-token` set, a peer must present the token on each connection it opens,
  before any `AppendEntries`, `Vote` or `InstallSnapshot`. Set the **same value on every node**: the
  flag controls both what a node demands of incoming connections and what it presents on outgoing
  ones. A mismatch looks like an unreachable peer.

There is still **no transport encryption**. A token stops an unauthorized host from producing,
consuming, reconfiguring topics, bootstrapping an un-bootstrapped node or joining the Raft group; it
does not hide traffic from anyone who can read the wire. Terminate TLS in front of the ports, or keep
them on a private network, if confidentiality matters.

## Limits

These bound how much one client can add to the replicated state. They are compiled in rather than
configured, because every replica has to reach the same decision for the same request. A request over
a limit is answered with an error and changes nothing - except the visibility timeout, which is
clamped to the cap rather than refused.

| Limit | Value | Applies to |
|---|---|---|
| Payload size | 1 MiB | One message |
| Client frame size | 2 MiB | One request or response |
| Peer frame size | 64 MiB | One Raft RPC |
| Topic name, group name | 256 bytes each | Every request naming one |
| Producer id | 256 bytes | Produce |
| Topics | 10,000 | Whole cluster |
| Consumer groups | 1,000 | Per topic |
| Visibility timeout | 12 hours | Poll and subscribe; longer values are clamped |
| Concurrent connections | 4,096 client, 256 peer, 256 HTTP | Per listener |

A frame body has 30 seconds to arrive once its length header has been read, and an HTTP request head
10 seconds. Neither deadline applies to a connection that is simply idle.

## Caveats

- The TCP and peer-RPC ports have **no TLS**, and no authentication unless you set `--client-token`
  and `--peer-token` (see [Authentication](#authentication)). Firewall the peer port to cluster hosts.
- Memory is reclaimed two ways. Messages that **every** group has acked past are dropped
  automatically (consumption-based), so a fully-drained topic costs nothing. On top of that,
  retention is **Kafka-style**: it drops messages by age/size regardless of consumption, so a lagging
  group can lose un-consumed messages. Set generous retention if that matters.
- A consumer group that stops polling is **expired after 7 idle days** (once its leases have run
  out), so an abandoned group can't pin messages forever. A group that comes back after expiry is
  treated as new: it re-reads whatever is still retained (at-least-once, as always). The sweep runs
  on any produce, poll or commit against the topic.
- The **producer dedup table is bounded by retention**: a `(producer_id, seq)` pair is dropped once
  the message it points at has been retained out. Re-sending a produce for a message that is no
  longer retained stores it again under a new offset.
- A topic that sets **no** retention still gets a default safety cap of 1,000,000 messages, so an
  un-consumed (or never-acked) topic can't grow unbounded in RAM. Set explicit `retention` to raise,
  lower, or age-bound it.
- Reads (`poll`) go through the leader; followers redirect.

## License

AGPL-3.0-only
