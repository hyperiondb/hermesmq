use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hermesmq_core::engine::{
    add_learner, build_raft, build_raft_partitionable, build_raft_tuned, initialize_cluster,
    serve_peer, set_voters, PartitionControl,
};
use hermesmq_core::{AppRequest, AppResponse, ContentType, GroupId, HermesRaft, Priority, RedbStore, TopicId};
use tokio::net::TcpListener;

async fn spawn_node(id: u64) -> (HermesRaft, String) {
    let db = Arc::new(RedbStore::in_memory().unwrap());
    let (raft, _sm) = build_raft(id, db).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(serve_peer(raft.clone(), listener));
    (raft, addr)
}

async fn wait_for_leader(nodes: &[(u64, &HermesRaft)]) -> u64 {
    for _ in 0..300 {
        for (_, raft) in nodes {
            if let Some(leader) = raft.current_leader().await {
                if nodes.iter().any(|(id, _)| *id == leader) {
                    return leader;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader elected within timeout");
}

fn pick<'a>(nodes: &'a [(u64, &'a HermesRaft)], id: u64) -> &'a HermesRaft {
    nodes.iter().find(|(n, _)| *n == id).map(|(_, r)| *r).unwrap()
}

async fn spawn_partition_node(id: u64) -> (HermesRaft, String, PartitionControl) {
    let db = Arc::new(RedbStore::in_memory().unwrap());
    let (raft, _sm, blocked) = build_raft_partitionable(id, db).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(serve_peer(raft.clone(), listener));
    (raft, addr, blocked)
}

async fn wait_new_leader(nodes: &[(u64, &HermesRaft)], exclude: u64) -> u64 {
    for _ in 0..400 {
        for (_, raft) in nodes {
            if let Some(leader) = raft.current_leader().await {
                if leader != exclude && nodes.iter().any(|(id, _)| *id == leader) {
                    return leader;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no new leader elected in the majority partition");
}

fn produce_req(body: &str) -> AppRequest {
    AppRequest::Produce {
        topic: TopicId::from("t"),
        priority: Priority::default(),
        content_type: ContentType::Raw,
        payload: bytes::Bytes::copy_from_slice(body.as_bytes()),
        producer_id: String::new(),
        seq: 0,
        ts_ms: 0,
    }
}

fn last_applied_index(raft: &HermesRaft) -> u64 {
    raft.metrics()
        .borrow()
        .last_applied
        .as_ref()
        .map(|l| l.index)
        .unwrap_or(0)
}

async fn wait_applied(raft: &HermesRaft, target: u64) {
    for _ in 0..200 {
        if last_applied_index(raft) >= target {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("node did not catch up to applied index {target}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_replicates_and_survives_leader_loss() {
    let (r1, a1) = spawn_node(1).await;
    let (r2, a2) = spawn_node(2).await;
    let (r3, a3) = spawn_node(3).await;

    initialize_cluster(&r1, &[(1, a1), (2, a2), (3, a3)])
        .await
        .unwrap();

    let all = [(1u64, &r1), (2, &r2), (3, &r3)];
    let leader_id = wait_for_leader(&all).await;
    let leader = pick(&all, leader_id);

    leader
        .client_write(AppRequest::CreateTopic {
            topic: TopicId::from("t"),
        })
        .await
        .unwrap();

    leader
        .client_write(AppRequest::Produce {
            topic: TopicId::from("t"),
            priority: Priority::default(),
            content_type: ContentType::Raw,
            payload: bytes::Bytes::from_static(b"hello"),
            producer_id: "p1".to_string(),
            seq: 1,
            ts_ms: 0,
        })
        .await
        .unwrap();

    leader.shutdown().await.unwrap();

    let remaining: Vec<(u64, &HermesRaft)> =
        all.iter().copied().filter(|(id, _)| *id != leader_id).collect();
    let new_leader_id = wait_for_leader(&remaining).await;
    let new_leader = pick(&remaining, new_leader_id);

    let polled = new_leader
        .client_write(AppRequest::Poll {
            topic: TopicId::from("t"),
            group: GroupId::from("g"),
            max: 10,
            visibility_timeout_ms: 1000,
            ts_ms: 0,
        })
        .await
        .unwrap();

    match polled.data {
        AppResponse::Polled { items } => {
            assert_eq!(items.len(), 1, "produced message must survive leader loss");
            assert_eq!(items[0].payload, &b"hello"[..]);
        }
        other => panic!("expected Polled, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn learner_joins_and_catches_up() {
    let (r1, a1) = spawn_node(1).await;
    let (r2, a2) = spawn_node(2).await;
    let (r3, a3) = spawn_node(3).await;

    initialize_cluster(&r1, &[(1, a1), (2, a2), (3, a3)])
        .await
        .unwrap();

    let all = [(1u64, &r1), (2, &r2), (3, &r3)];
    let leader_id = wait_for_leader(&all).await;
    let leader = pick(&all, leader_id);

    leader
        .client_write(AppRequest::Produce {
            topic: TopicId::from("t"),
            priority: Priority::default(),
            content_type: ContentType::Raw,
            payload: bytes::Bytes::from_static(b"hello"),
            producer_id: "p1".to_string(),
            seq: 1,
            ts_ms: 0,
        })
        .await
        .unwrap();

    let target = last_applied_index(leader);

    let (r4, a4) = spawn_node(4).await;
    add_learner(leader, 4, a4).await.unwrap();
    set_voters(leader, &[1, 2, 3, 4]).await.unwrap();

    wait_applied(&r4, target).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_tolerates_follower_loss() {
    let (r1, a1) = spawn_node(1).await;
    let (r2, a2) = spawn_node(2).await;
    let (r3, a3) = spawn_node(3).await;
    initialize_cluster(&r1, &[(1, a1), (2, a2), (3, a3)])
        .await
        .unwrap();

    let all = [(1u64, &r1), (2, &r2), (3, &r3)];
    let leader_id = wait_for_leader(&all).await;
    let leader = pick(&all, leader_id);

    leader
        .client_write(AppRequest::CreateTopic {
            topic: TopicId::from("t"),
        })
        .await
        .unwrap();
    leader.client_write(produce_req("m1")).await.unwrap();

    let follower_id = [1u64, 2, 3].into_iter().find(|id| *id != leader_id).unwrap();
    pick(&all, follower_id).shutdown().await.unwrap();

    leader.client_write(produce_req("m2")).await.unwrap();

    let polled = leader
        .client_write(AppRequest::Poll {
            topic: TopicId::from("t"),
            group: GroupId::from("g"),
            max: 10,
            visibility_timeout_ms: 1000,
            ts_ms: 0,
        })
        .await
        .unwrap();
    match polled.data {
        AppResponse::Polled { items } => {
            assert_eq!(items.len(), 2, "both messages available after a follower is lost")
        }
        other => panic!("expected Polled, got {other:?}"),
    }
}

async fn snapshot_index(raft: &HermesRaft) -> u64 {
    for _ in 0..200 {
        if let Some(i) = raft.metrics().borrow().snapshot.as_ref().map(|l| l.index) {
            return i;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("snapshot was not built within timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wiped_node_recovers_via_snapshot_and_survives_restart() {
    let (r1, a1) = spawn_node(1).await;
    let (r2, a2) = spawn_node(2).await;
    initialize_cluster(&r1, &[(1, a1), (2, a2)]).await.unwrap();

    let voters = [(1u64, &r1), (2, &r2)];
    let leader_id = wait_for_leader(&voters).await;
    let leader = pick(&voters, leader_id);

    leader
        .client_write(AppRequest::CreateTopic { topic: TopicId::from("t") })
        .await
        .unwrap();
    for _ in 0..5 {
        leader.client_write(produce_req("snapshotted")).await.unwrap();
    }
    leader.trigger().snapshot().await.unwrap();
    let snap_idx = snapshot_index(leader).await;
    for _ in 0..5 {
        leader.client_write(produce_req("trailing")).await.unwrap();
    }
    let target = last_applied_index(leader);
    leader.trigger().purge_log(snap_idx).await.unwrap();

    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir: PathBuf = std::env::temp_dir().join(format!("hermesmq-wipe-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("hermesmq.redb");

    let a3 = {
        let db = Arc::new(RedbStore::open(&path).unwrap());
        let (r3, _sm3) = build_raft(3, db).await.unwrap();
        let l3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l3.local_addr().unwrap().to_string();
        let serve = tokio::spawn(serve_peer(r3.clone(), l3));
        add_learner(leader, 3, addr.clone()).await.unwrap();
        wait_applied(&r3, target).await;
        r3.shutdown().await.unwrap();
        serve.abort();
        addr
    };

    tokio::time::sleep(Duration::from_millis(200)).await;

    let db = Arc::new(RedbStore::open(&path).unwrap());
    let (r3c, _sm3c) = build_raft(3, db).await.unwrap();
    let l3 = {
        let mut bound = None;
        for _ in 0..100 {
            if let Ok(l) = TcpListener::bind(&a3).await {
                bound = Some(l);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        bound.expect("port frees up after the dead node releases its listener")
    };
    tokio::spawn(serve_peer(r3c.clone(), l3));

    for _ in 0..3 {
        leader.client_write(produce_req("after-restart")).await.unwrap();
    }
    let target2 = last_applied_index(leader);

    for _ in 0..200 {
        if last_applied_index(&r3c) >= target2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let reached = last_applied_index(&r3c);
    let snap = r3c.metrics().borrow().snapshot.as_ref().map(|l| l.index);
    r3c.shutdown().await.ok();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        reached >= target2,
        "restarted node stuck: applied={reached}, snapshot={snap:?}, snap_idx={snap_idx}, target2={target2}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wiped_node_recovers_through_snapshot_overlap() {
    const SNAP_LOGS: u64 = 4;
    const KEEP: u64 = 0;

    async fn spawn_tuned(id: u64, snap: u64, keep: u64) -> (HermesRaft, String) {
        let db = Arc::new(RedbStore::in_memory().unwrap());
        let (raft, _sm) = build_raft_tuned(id, db, snap, keep, 1024 * 1024).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(serve_peer(raft.clone(), listener));
        (raft, addr)
    }

    let (r1, a1) = spawn_tuned(1, SNAP_LOGS, KEEP).await;
    let (r2, a2) = spawn_tuned(2, SNAP_LOGS, KEEP).await;
    initialize_cluster(&r1, &[(1, a1), (2, a2)]).await.unwrap();
    let voters = [(1u64, &r1), (2, &r2)];
    let leader_id = wait_for_leader(&voters).await;
    let leader = pick(&voters, leader_id);

    leader
        .client_write(AppRequest::CreateTopic { topic: TopicId::from("t") })
        .await
        .unwrap();
    for _ in 0..40 {
        leader.client_write(produce_req("x")).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let db3 = Arc::new(RedbStore::in_memory().unwrap());
    let (r3, _sm3) = build_raft_tuned(3, db3, SNAP_LOGS, KEEP, 1024 * 1024).await.unwrap();
    let l3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a3 = l3.local_addr().unwrap().to_string();
    tokio::spawn(serve_peer(r3.clone(), l3));
    add_learner(leader, 3, a3).await.unwrap();

    for _ in 0..40 {
        leader.client_write(produce_req("y")).await.unwrap();
    }
    let target = last_applied_index(leader);

    for _ in 0..200 {
        if last_applied_index(&r3) >= target {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let reached = last_applied_index(&r3);
    assert!(
        reached >= target,
        "wiped node stuck recovering through snapshot overlap: applied={reached}, target={target}"
    );
}

/// A wiped node must catch up via a snapshot that spans MANY chunks.
///
/// With openraft's 3 MiB default `snapshot_max_chunk_size`, every snapshot in
/// these tests fit in a single chunk, so the multi-chunk transport path — and
/// the per-chunk `install_snapshot_timeout` — were never exercised. In
/// production a multi-megabyte snapshot shipped as one chunk with the 200ms
/// default timeout could not round-trip on the overlay network, so openraft
/// restarted the transfer from offset 0 forever and the node never recovered.
/// Here we force a small chunk size so the snapshot is split into many chunks
/// and assert the fresh node fully installs it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wiped_node_recovers_via_multichunk_snapshot() {
    const SNAP_LOGS: u64 = 4;
    const KEEP: u64 = 0;
    const CHUNK: u64 = 8 * 1024; // 8 KiB — far smaller than the snapshot below.

    async fn spawn_tuned_chunked(id: u64, chunk: u64) -> (HermesRaft, String) {
        let db = Arc::new(RedbStore::in_memory().unwrap());
        let (raft, _sm) = build_raft_tuned(id, db, SNAP_LOGS, KEEP, chunk).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(serve_peer(raft.clone(), listener));
        (raft, addr)
    }

    fn produce_big(n: usize) -> AppRequest {
        AppRequest::Produce {
            topic: TopicId::from("t"),
            priority: Priority::default(),
            content_type: ContentType::Raw,
            payload: bytes::Bytes::from(vec![(n & 0xff) as u8; 4096]), // 4 KiB each
            producer_id: String::new(),
            seq: 0,
            ts_ms: 0,
        }
    }

    let (r1, a1) = spawn_tuned_chunked(1, CHUNK).await;
    let (r2, a2) = spawn_tuned_chunked(2, CHUNK).await;
    initialize_cluster(&r1, &[(1, a1), (2, a2)]).await.unwrap();
    let voters = [(1u64, &r1), (2, &r2)];
    let leader_id = wait_for_leader(&voters).await;
    let leader = pick(&voters, leader_id);

    leader
        .client_write(AppRequest::CreateTopic { topic: TopicId::from("t") })
        .await
        .unwrap();
    // ~50 * 4 KiB of retained payload >> 8 KiB chunk -> snapshot spans many chunks.
    for i in 0..50 {
        leader.client_write(produce_big(i)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Fresh node: its log is empty and the leader purged its own, so the only way
    // to catch up is a full, multi-chunk snapshot install.
    let db3 = Arc::new(RedbStore::in_memory().unwrap());
    let (r3, _sm3) = build_raft_tuned(3, db3, SNAP_LOGS, KEEP, CHUNK).await.unwrap();
    let l3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a3 = l3.local_addr().unwrap().to_string();
    tokio::spawn(serve_peer(r3.clone(), l3));
    add_learner(leader, 3, a3).await.unwrap();

    for i in 50..70 {
        leader.client_write(produce_big(i)).await.unwrap();
    }
    let target = last_applied_index(leader);

    for _ in 0..200 {
        if last_applied_index(&r3) >= target {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let reached = last_applied_index(&r3);
    assert!(
        reached >= target,
        "node stuck installing a multi-chunk snapshot: applied={reached}, target={target}"
    );
}

/// PRODUCTION scenario: an EXISTING VOTER is wiped to an empty volume and
/// restarts under the SAME node id, while the leader has already purged its log
/// prefix. Unlike every other "wiped node" test, the node is NOT re-added with
/// `add_learner` — it is already in committed membership, so recovery is driven
/// entirely by the leader replicating to the reused address. This is the path
/// that crash-loops in production with
/// `LogIndexNotFound { want: 0, got: Some(<purged index>) }`.
static SAW_REVERSION_PANIC: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wiped_voter_rejoins_without_readd() {
    use std::path::Path;
    use std::sync::atomic::Ordering;

    // Without `loosen-follower-log-revert`, openraft's leader trips a
    // `debug_assert!` ("follower log reversion is not allowed") the moment a
    // previously replicated voter returns wiped. tokio isolates that panic to a
    // worker thread, so the test would otherwise report `ok`. Catch it via the
    // panic hook and fail explicitly — this is the regression guard.
    SAW_REVERSION_PANIC.store(false, Ordering::SeqCst);
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if format!("{info}").contains("log reversion") {
            SAW_REVERSION_PANIC.store(true, Ordering::SeqCst);
        }
        prev_hook(info);
    }));

    async fn open_node(
        id: u64,
        file: &Path,
        addr: Option<&str>,
    ) -> (HermesRaft, String, tokio::task::JoinHandle<()>) {
        let db = Arc::new(RedbStore::open(file).unwrap());
        let (raft, _sm) = build_raft(id, db).await.unwrap();
        let listener = match addr {
            Some(a) => {
                let mut bound = None;
                for _ in 0..100 {
                    if let Ok(l) = TcpListener::bind(a).await {
                        bound = Some(l);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                bound.expect("port frees up after the dead node releases its listener")
            }
            None => TcpListener::bind("127.0.0.1:0").await.unwrap(),
        };
        let real_addr = listener.local_addr().unwrap().to_string();
        let serve = tokio::spawn(serve_peer(raft.clone(), listener));
        (raft, real_addr, serve)
    }

    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let base: PathBuf = std::env::temp_dir().join(format!("hermesmq-rejoin-{nanos}"));
    let file_of = |id: u64| base.join(format!("n{id}")).join("hermesmq.redb");
    for id in [1u64, 2, 3] {
        std::fs::create_dir_all(file_of(id).parent().unwrap()).unwrap();
    }

    let (r1, a1, s1) = open_node(1, &file_of(1), None).await;
    let (r2, a2, s2) = open_node(2, &file_of(2), None).await;
    let (r3, a3, s3) = open_node(3, &file_of(3), None).await;

    initialize_cluster(&r1, &[(1, a1.clone()), (2, a2.clone()), (3, a3.clone())])
        .await
        .unwrap();

    let all = [(1u64, &r1), (2, &r2), (3, &r3)];
    let leader_id = wait_for_leader(&all).await;
    let leader = pick(&all, leader_id);

    // Wipe a follower (prefer node 2, matching prod) so the cluster keeps quorum.
    let victim_id = if leader_id != 2 { 2 } else { 1 };
    let victim_addr = match victim_id {
        1 => a1.clone(),
        2 => a2.clone(),
        _ => a3.clone(),
    };

    leader
        .client_write(AppRequest::CreateTopic { topic: TopicId::from("t") })
        .await
        .unwrap();
    for _ in 0..5 {
        leader.client_write(produce_req("snap")).await.unwrap();
    }
    leader.trigger().snapshot().await.unwrap();
    let snap_idx = snapshot_index(leader).await;
    for _ in 0..5 {
        leader.client_write(produce_req("tail")).await.unwrap();
    }
    // Purge the leader's prefix so a blank follower can ONLY recover via snapshot.
    leader.trigger().purge_log(snap_idx).await.unwrap();

    // --- WIPE: shut the voter down, delete its volume, restart empty, same id+addr ---
    pick(&all, victim_id).shutdown().await.unwrap();
    for (id, s) in [(1u64, s1), (2, s2), (3, s3)] {
        if id == victim_id {
            s.abort();
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    std::fs::remove_dir_all(file_of(victim_id).parent().unwrap()).unwrap();
    std::fs::create_dir_all(file_of(victim_id).parent().unwrap()).unwrap();

    // NOTE: no add_learner — the node is already a committed voter.
    let (r_victim, _va, _vs) = open_node(victim_id, &file_of(victim_id), Some(&victim_addr)).await;

    for _ in 0..3 {
        leader.client_write(produce_req("after")).await.unwrap();
    }
    let target = last_applied_index(leader);

    let mut reached = 0;
    for _ in 0..200 {
        reached = last_applied_index(&r_victim);
        if reached >= target {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let snap = r_victim.metrics().borrow().snapshot.as_ref().map(|l| l.index);
    r_victim.shutdown().await.ok();
    let _ = std::fs::remove_dir_all(&base);
    assert!(
        reached >= target,
        "wiped voter failed to rejoin: applied={reached}, target={target}, \
         snap_idx={snap_idx}, victim_snapshot={snap:?}"
    );
    assert!(
        !SAW_REVERSION_PANIC.load(Ordering::SeqCst),
        "leader hit openraft's follower-log-reversion assertion — \
         loosen-follower-log-revert is not effective"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_minority_cannot_commit() {
    let (r1, a1) = spawn_node(1).await;
    let (r2, a2) = spawn_node(2).await;
    let (r3, a3) = spawn_node(3).await;
    initialize_cluster(&r1, &[(1, a1), (2, a2), (3, a3)])
        .await
        .unwrap();

    let all = [(1u64, &r1), (2, &r2), (3, &r3)];
    let leader_id = wait_for_leader(&all).await;

    let others: Vec<u64> = [1u64, 2, 3].into_iter().filter(|id| *id != leader_id).collect();
    let survivor_id = others[0];
    let killed_follower = others[1];
    pick(&all, leader_id).shutdown().await.unwrap();
    pick(&all, killed_follower).shutdown().await.unwrap();

    let survivor = pick(&all, survivor_id);
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        survivor.client_write(produce_req("nope")),
    )
    .await;

    let committed = matches!(result, Ok(Ok(_)));
    assert!(
        !committed,
        "a lone minority node must not commit writes (got {result:?})"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn network_partition_isolates_leader_then_heals() {
    let (r1, a1, b1) = spawn_partition_node(1).await;
    let (r2, a2, b2) = spawn_partition_node(2).await;
    let (r3, a3, b3) = spawn_partition_node(3).await;
    initialize_cluster(&r1, &[(1, a1), (2, a2), (3, a3)])
        .await
        .unwrap();

    let all = [(1u64, &r1), (2, &r2), (3, &r3)];
    let blocks = [(1u64, &b1), (2, &b2), (3, &b3)];
    let old_leader = wait_for_leader(&all).await;
    let leader = pick(&all, old_leader);

    leader
        .client_write(AppRequest::CreateTopic {
            topic: TopicId::from("t"),
        })
        .await
        .unwrap();
    leader.client_write(produce_req("m1")).await.unwrap();

    let others: Vec<u64> = [1u64, 2, 3].into_iter().filter(|id| *id != old_leader).collect();
    let block_of = |id: u64| -> &PartitionControl {
        blocks.iter().find(|(n, _)| *n == id).map(|(_, b)| *b).unwrap()
    };
    block_of(old_leader).lock().unwrap().insert(others[0]);
    block_of(old_leader).lock().unwrap().insert(others[1]);
    block_of(others[0]).lock().unwrap().insert(old_leader);
    block_of(others[1]).lock().unwrap().insert(old_leader);

    let majority: Vec<(u64, &HermesRaft)> =
        all.iter().copied().filter(|(id, _)| *id != old_leader).collect();
    let new_leader_id = wait_new_leader(&majority, old_leader).await;
    let new_leader = pick(&majority, new_leader_id);

    let result = tokio::time::timeout(
        Duration::from_secs(3),
        leader.client_write(produce_req("dropped")),
    )
    .await;
    assert!(
        !matches!(result, Ok(Ok(_))),
        "isolated leader must not commit (got {result:?})"
    );

    new_leader.client_write(produce_req("m2")).await.unwrap();
    let target = last_applied_index(new_leader);

    for (_, b) in &blocks {
        b.lock().unwrap().clear();
    }

    wait_applied(leader, target).await;

    let polled = new_leader
        .client_write(AppRequest::Poll {
            topic: TopicId::from("t"),
            group: GroupId::from("g"),
            max: 10,
            visibility_timeout_ms: 1000,
            ts_ms: 0,
        })
        .await
        .unwrap();
    match polled.data {
        AppResponse::Polled { items } => {
            assert_eq!(items.len(), 2, "both messages survive partition + heal")
        }
        other => panic!("expected Polled, got {other:?}"),
    }
}
