mod chart;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hermesmq_core::client::proto::{self, request, response, Request, Response};
use hermesmq_core::engine::{build_raft, initialize_cluster, serve_peer};
use hermesmq_core::{
    serve_clients, AppRequest, AppResponse, ContentType, GroupId, HermesRaft, Priority, Queue,
    RedbStore, TopicId,
};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const RELEASE: bool = !cfg!(debug_assertions);

fn rate(n: usize, elapsed: Duration) -> f64 {
    n as f64 / elapsed.as_secs_f64()
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

fn produce_app(payload: Vec<u8>, priority: u8, ts_ms: u64) -> AppRequest {
    AppRequest::Produce {
        topic: TopicId::from("t"),
        priority: Priority(priority),
        content_type: ContentType::Raw,
        payload,
        producer_id: String::new(),
        seq: 0,
        ts_ms,
    }
}

#[test]
#[ignore = "performance test; run with: cargo test --release --test perf -- --ignored --nocapture"]
fn perf_queue_state_machine() {
    const N: usize = 20_000;
    const BATCH: u32 = 256;
    const HD_CALLS: usize = 200_000;

    let mut q = Queue::default();

    let t0 = Instant::now();
    for i in 0..N {
        q.apply(produce_app(vec![0u8; 64], (i % 8) as u8, i as u64));
    }
    let produce_dt = t0.elapsed();
    println!(
        "queue produce:            {N} msgs in {produce_dt:?} -> {:.0} ops/s",
        rate(N, produce_dt)
    );

    let t0 = Instant::now();
    let mut drained = 0usize;
    let mut ts = 1_000_000u64;
    while drained < N {
        let resp = q.apply(AppRequest::Poll {
            topic: TopicId::from("t"),
            group: GroupId::from("g"),
            max: BATCH,
            visibility_timeout_ms: 600_000,
            ts_ms: ts,
        });
        ts += 1;
        let items = match resp {
            AppResponse::Polled { items } => items,
            other => panic!("expected Polled, got {other:?}"),
        };
        assert!(!items.is_empty(), "drain stalled at {drained}/{N}");
        drained += items.len();
        let lease_ids = items.iter().map(|d| d.lease_id).collect();
        q.apply(AppRequest::AckMany {
            topic: TopicId::from("t"),
            group: GroupId::from("g"),
            lease_ids,
        });
    }
    let drain_dt = t0.elapsed();
    println!(
        "queue poll+ack drain:     {N} msgs (batch {BATCH}) in {drain_dt:?} -> {:.0} msg/s",
        rate(N, drain_dt)
    );

    let t0 = Instant::now();
    let mut deliverable = false;
    for _ in 0..HD_CALLS {
        deliverable |= q.has_deliverable("t", "g", ts);
    }
    assert!(!deliverable, "queue must be fully drained");
    let hd_dt = t0.elapsed();
    println!(
        "has_deliverable (drained backlog of {N} retained msgs): {HD_CALLS} calls in {hd_dt:?} -> {:.0} calls/s",
        rate(HD_CALLS, hd_dt)
    );

    if RELEASE {
        assert!(rate(N, produce_dt) > 50_000.0, "produce regressed catastrophically");
        assert!(rate(N, drain_dt) > 20_000.0, "poll/ack drain regressed catastrophically");
        assert!(rate(HD_CALLS, hd_dt) > 500_000.0, "has_deliverable must not scan consumed messages");
        chart::record("q_produce", rate(N, produce_dt), None);
        chart::record("q_drain", rate(N, drain_dt), None);
        chart::record("q_hd", rate(HD_CALLS, hd_dt), None);
        chart::render();
    }
}

async fn call(stream: &mut TcpStream, req: &Request) -> Response {
    write_req(stream, req).await;
    read_resp(stream).await
}

async fn write_req<W: AsyncWriteExt + Unpin>(stream: &mut W, req: &Request) {
    let bytes = req.encode_to_vec();
    stream.write_all(&(bytes.len() as u32).to_be_bytes()).await.unwrap();
    stream.write_all(&bytes).await.unwrap();
    stream.flush().await.unwrap();
}

async fn read_resp<R: AsyncReadExt + Unpin>(stream: &mut R) -> Response {
    let fut = async {
        let mut len = [0u8; 4];
        stream.read_exact(&mut len).await.unwrap();
        let n = u32::from_be_bytes(len) as usize;
        let mut buf = vec![0u8; n];
        stream.read_exact(&mut buf).await.unwrap();
        Response::decode(buf.as_slice()).unwrap()
    };
    tokio::time::timeout(Duration::from_secs(30), fut)
        .await
        .expect("response within 30s")
}

fn produce_req(topic: &str, payload: Vec<u8>) -> Request {
    Request {
        kind: Some(request::Kind::Produce(proto::Produce {
            topic: topic.to_string(),
            priority: 0,
            content_type: 0,
            payload,
            producer_id: String::new(),
            seq: 0,
        })),
    }
}

fn poll_req(topic: &str, group: &str, max: u32) -> Request {
    Request {
        kind: Some(request::Kind::Poll(proto::Poll {
            topic: topic.to_string(),
            group: group.to_string(),
            max,
            visibility_timeout_ms: 600_000,
            ack_mode: "manual".to_string(),
            wait_ms: 0,
        })),
    }
}

fn ack_req(topic: &str, group: &str, lease_id: u64) -> Request {
    Request {
        kind: Some(request::Kind::Ack(proto::Ack {
            topic: topic.to_string(),
            group: group.to_string(),
            lease_id,
        })),
    }
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let dir = std::env::temp_dir().join(format!("hermesmq-perf-{tag}-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn start_durable_node(dir: &TempDir) -> (HermesRaft, SocketAddr) {
    let db = Arc::new(RedbStore::open(dir.0.join("hermesmq.redb")).unwrap());
    let (raft, sm) = build_raft(1, db).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_clients(raft.clone(), sm, listener));
    initialize_cluster(&raft, &[(1, String::new())]).await.unwrap();
    for _ in 0..200 {
        if raft.current_leader().await.is_some() {
            return (raft, addr);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader elected");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "performance test; run with: cargo test --release --test perf -- --ignored --nocapture"]
async fn perf_protocol_single_node_durable() {
    const SEQ_N: usize = 300;
    const CONC_TASKS: usize = 4;
    const CONC_PER_TASK: usize = 250;
    const PAYLOAD: usize = 256;

    let dir = TempDir::new("proto");
    let (raft, addr) = start_durable_node(&dir).await;

    let mut s = TcpStream::connect(addr).await.unwrap();
    let mut latencies = Vec::with_capacity(SEQ_N);
    let t0 = Instant::now();
    for _ in 0..SEQ_N {
        let started = Instant::now();
        let resp = call(&mut s, &produce_req("t", vec![0u8; PAYLOAD])).await;
        assert!(matches!(resp.kind, Some(response::Kind::Produced(_))));
        latencies.push(started.elapsed());
    }
    let seq_dt = t0.elapsed();
    latencies.sort();
    println!(
        "produce sequential+fsync: {SEQ_N} msgs in {seq_dt:?} -> {:.0} msg/s; p50={:?} p95={:?} p99={:?}",
        rate(SEQ_N, seq_dt),
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.95),
        percentile(&latencies, 0.99),
    );

    let conc_n = CONC_TASKS * CONC_PER_TASK;
    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..CONC_TASKS {
        tasks.push(tokio::spawn(async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            for _ in 0..CONC_PER_TASK {
                let resp = call(&mut s, &produce_req("t", vec![0u8; PAYLOAD])).await;
                assert!(matches!(resp.kind, Some(response::Kind::Produced(_))));
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let conc_dt = t0.elapsed();
    println!(
        "produce {CONC_TASKS} concurrent conns: {conc_n} msgs in {conc_dt:?} -> {:.0} msg/s",
        rate(conc_n, conc_dt)
    );

    const PIPE_N: usize = 2_000;
    let pipe_stream = TcpStream::connect(addr).await.unwrap();
    let (mut read_half, mut write_half) = pipe_stream.into_split();
    let t0 = Instant::now();
    let pipe_writer = tokio::spawn(async move {
        for _ in 0..PIPE_N {
            write_req(&mut write_half, &produce_req("pipe", vec![0u8; PAYLOAD])).await;
        }
    });
    for _ in 0..PIPE_N {
        let resp = read_resp(&mut read_half).await;
        assert!(matches!(resp.kind, Some(response::Kind::Produced(_))));
    }
    pipe_writer.await.unwrap();
    let pipe_dt = t0.elapsed();
    println!(
        "produce pipelined 1 conn: {PIPE_N} msgs in {pipe_dt:?} -> {:.0} msg/s",
        rate(PIPE_N, pipe_dt)
    );

    let total = SEQ_N + conc_n;
    let t0 = Instant::now();
    let mut drained = 0usize;
    while drained < total {
        let items = match call(&mut s, &poll_req("t", "g", 256)).await.kind {
            Some(response::Kind::Polled(p)) => p.items,
            other => panic!("expected Polled, got {other:?}"),
        };
        assert!(!items.is_empty(), "drain stalled at {drained}/{total}");
        for item in &items {
            let resp = call(&mut s, &ack_req("t", "g", item.lease_id)).await;
            assert!(matches!(resp.kind, Some(response::Kind::Ok(_))));
        }
        drained += items.len();
    }
    let drain_dt = t0.elapsed();
    println!(
        "poll(batch 256)+ack each: {total} msgs in {drain_dt:?} -> {:.0} msg/s",
        rate(total, drain_dt)
    );

    if RELEASE {
        assert!(rate(SEQ_N, seq_dt) > 25.0, "sequential produce regressed catastrophically");
        assert!(rate(conc_n, conc_dt) > 50.0, "concurrent produce regressed catastrophically");
        assert!(
            rate(PIPE_N, pipe_dt) > 1_000.0,
            "pipelined produce must be batch-bound, not one-fsync-per-message"
        );
        assert!(rate(total, drain_dt) > 50.0, "poll/ack drain regressed catastrophically");
        let p50_ms = percentile(&latencies, 0.50).as_secs_f64() * 1000.0;
        chart::record("tcp_seq", rate(SEQ_N, seq_dt), Some(format!("p50 {p50_ms:.1} ms")));
        chart::record("tcp_conc", rate(conc_n, conc_dt), None);
        chart::record("tcp_pipe", rate(PIPE_N, pipe_dt), None);
        chart::record("tcp_drain", rate(total, drain_dt), None);
        chart::render();
    }

    raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "performance test; run with: cargo test --release --test perf -- --ignored --nocapture"]
async fn perf_subscribe_push_durable() {
    const N: usize = 1_000;
    const PRODUCERS: usize = 4;
    const PAYLOAD: usize = 256;

    let dir = TempDir::new("sub");
    let (raft, addr) = start_durable_node(&dir).await;

    let per_task = N / PRODUCERS;
    let mut tasks = Vec::new();
    for _ in 0..PRODUCERS {
        tasks.push(tokio::spawn(async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            for _ in 0..per_task {
                let resp = call(&mut s, &produce_req("t", vec![0u8; PAYLOAD])).await;
                assert!(matches!(resp.kind, Some(response::Kind::Produced(_))));
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let mut sub = TcpStream::connect(addr).await.unwrap();
    let subscribe = Request {
        kind: Some(request::Kind::Subscribe(proto::Subscribe {
            topic: "t".to_string(),
            group: "g".to_string(),
            prefetch: 64,
            visibility_timeout_ms: 600_000,
            ack_mode: "manual".to_string(),
        })),
    };
    write_req(&mut sub, &subscribe).await;

    let t0 = Instant::now();
    for received in 0..N {
        let items = match read_resp(&mut sub).await.kind {
            Some(response::Kind::Polled(p)) => p.items,
            other => panic!("expected pushed Polled at msg {received}, got {other:?}"),
        };
        assert_eq!(items.len(), 1);
        write_req(&mut sub, &ack_req("t", "g", items[0].lease_id)).await;
    }
    let dt = t0.elapsed();
    println!(
        "subscribe push+ack (prefetch 64): {N} msgs in {dt:?} -> {:.0} msg/s",
        rate(N, dt)
    );

    if RELEASE {
        assert!(
            rate(N, dt) > 500.0,
            "subscribe push must be batch-bound, not one-raft-round-per-ack"
        );
        chart::record("tcp_sub", rate(N, dt), Some("prefetch 64".to_string()));
        chart::render();
    }

    raft.shutdown().await.unwrap();
}

async fn spawn_cluster_node(id: u64) -> (HermesRaft, String) {
    let db = Arc::new(RedbStore::in_memory().unwrap());
    let (raft, _sm) = build_raft(id, db).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(serve_peer(raft.clone(), listener));
    (raft, addr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "performance test; run with: cargo test --release --test perf -- --ignored --nocapture"]
async fn perf_cluster_replicated_writes() {
    const SEQ_N: usize = 1_000;
    const CONC_TASKS: usize = 8;
    const CONC_PER_TASK: usize = 250;
    const PAYLOAD: usize = 256;

    let (r1, a1) = spawn_cluster_node(1).await;
    let (r2, a2) = spawn_cluster_node(2).await;
    let (r3, a3) = spawn_cluster_node(3).await;
    initialize_cluster(&r1, &[(1, a1), (2, a2), (3, a3)])
        .await
        .unwrap();

    let nodes = [(1u64, r1), (2, r2), (3, r3)];
    let mut leader = None;
    for _ in 0..300 {
        for (id, raft) in &nodes {
            if raft.current_leader().await == Some(*id) {
                leader = Some(raft.clone());
            }
        }
        if leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let leader = leader.expect("no leader elected");

    let t0 = Instant::now();
    for i in 0..SEQ_N {
        leader
            .client_write(produce_app(vec![0u8; PAYLOAD], 0, i as u64))
            .await
            .unwrap();
    }
    let seq_dt = t0.elapsed();
    println!(
        "3-node replicated produce (sequential):  {SEQ_N} msgs in {seq_dt:?} -> {:.0} msg/s",
        rate(SEQ_N, seq_dt)
    );

    let conc_n = CONC_TASKS * CONC_PER_TASK;
    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..CONC_TASKS {
        let raft = leader.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..CONC_PER_TASK {
                raft.client_write(produce_app(vec![0u8; PAYLOAD], 0, i as u64))
                    .await
                    .unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let conc_dt = t0.elapsed();
    println!(
        "3-node replicated produce ({CONC_TASKS} writers): {conc_n} msgs in {conc_dt:?} -> {:.0} msg/s",
        rate(conc_n, conc_dt)
    );

    if RELEASE {
        assert!(rate(SEQ_N, seq_dt) > 100.0, "replicated produce regressed catastrophically");
        assert!(rate(conc_n, conc_dt) > 200.0, "concurrent replicated produce regressed catastrophically");
        chart::record("cl_seq", rate(SEQ_N, seq_dt), None);
        chart::record("cl_conc", rate(conc_n, conc_dt), None);
        chart::render();
    }

    for (_, raft) in nodes {
        let _ = raft.shutdown().await;
    }
}
