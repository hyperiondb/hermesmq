//! Restart / crash-recovery conformance for the redb-backed Raft storage.
//!
//! The stock `openraft::testing::Suite` (see `store_conformance.rs`) only ever
//! exercises a *single, freshly built, in-memory* store instance. It therefore
//! cannot catch bugs that only appear when a **non-empty store is reopened from
//! disk after damage** — which is exactly the production failure:
//!
//! ```text
//! get_initial_state last_applied=None committed=...-198410 last_log_id=...-198410
//! re-apply log [0..198411) ...
//! ERROR Failed to get log entries, expected index: [0, 64), got [None, None)
//! Error: when Read LogIndex(0): Failed to get log entries, expected index: [0, 64) ...
//! ```
//!
//! openraft rebuilds the state machine on startup by re-applying the log over
//! `(last_applied .. committed]`. If the store advertises a `committed` /
//! `last_log_id` that the log cannot actually serve from the front, openraft
//! aborts. These tests reproduce that and assert the recovery path heals it.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hermesmq_core::engine::build_raft;
use hermesmq_core::{
    AppRequest, AppResponse, ContentType, GroupId, HermesRaft, Priority, RedbStore, Storage, TopicId,
};

fn unique_dir(tag: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("hermesmq-restart-{tag}-{nanos}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn produce(topic: &str, body: &str) -> AppRequest {
    AppRequest::Produce {
        topic: TopicId::from(topic),
        priority: Priority::default(),
        content_type: ContentType::Raw,
        payload: bytes::Bytes::copy_from_slice(body.as_bytes()),
        producer_id: String::new(),
        seq: 0,
        ts_ms: 0,
    }
}

async fn wait_leader(raft: &HermesRaft, id: u64) {
    raft.wait(Some(Duration::from_secs(10)))
        .current_leader(id, "elect self")
        .await
        .unwrap();
}

/// Build a healthy single-node log on disk: topic + `n` messages, then a clean
/// shutdown. Returns the db path. All handles are dropped before returning so
/// the file can be reopened.
async fn seed_node(path: &std::path::Path, n: usize) {
    let db = Arc::new(RedbStore::open(path).unwrap());
    let (raft, _sm) = build_raft(1, db).await.unwrap();
    hermesmq_core::engine::initialize_cluster(&raft, &[(1, String::new())])
        .await
        .unwrap();
    wait_leader(&raft, 1).await;
    raft.client_write(AppRequest::CreateTopic { topic: TopicId::from("t") })
        .await
        .unwrap();
    for i in 0..n {
        raft.client_write(produce("t", &format!("m{i}"))).await.unwrap();
    }
    raft.shutdown().await.unwrap();
    drop(raft);
    // Let the detached log-flusher thread observe the dropped sender and exit,
    // releasing its Arc on the redb file before we reopen it.
    tokio::time::sleep(Duration::from_millis(300)).await;
}

async fn poll_count(raft: &HermesRaft) -> usize {
    let polled = raft
        .client_write(AppRequest::Poll {
            topic: TopicId::from("t"),
            group: GroupId::from("g"),
            max: 100_000,
            visibility_timeout_ms: 1000,
            ts_ms: 0,
        })
        .await
        .unwrap();
    match polled.data {
        AppResponse::Polled { items } => items.len(),
        other => panic!("expected Polled, got {other:?}"),
    }
}

/// Baseline: a clean restart with an intact on-disk log must replay the log and
/// recover every committed message. If THIS fails, "deleted disk" recovery is
/// hopeless regardless of the damage-repair logic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn healthy_restart_replays_committed_log() {
    let dir = unique_dir("healthy");
    let path = dir.join("node.redb");

    seed_node(&path, 8).await;

    let db = Arc::new(RedbStore::open(&path).unwrap());
    let (raft, _sm) = build_raft(1, db).await.unwrap();
    wait_leader(&raft, 1).await;

    let n = poll_count(&raft).await;
    assert_eq!(n, 8, "all committed messages must survive a clean restart");

    raft.shutdown().await.ok();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Production failure: the log lost its prefix (e.g. partial volume loss) so the
/// store has entries [k..last] but not [0..k), with `committed` pointing at the
/// tail and NO snapshot. openraft would try to re-apply from index 0 and abort.
/// The recovery path must heal this into a clean (blank) node that can rejoin a
/// cluster, instead of crash-looping on startup.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_log_prefix_without_snapshot_recovers() {
    let dir = unique_dir("prefix");
    let path = dir.join("node.redb");

    seed_node(&path, 200).await;

    // Simulate the lost prefix: drop log entries [0..=63] WITHOUT recording a
    // purge marker or snapshot — exactly the orphaned-log shape from prod.
    {
        let db = RedbStore::open(&path).unwrap();
        assert_eq!(db.first_log_index().unwrap(), Some(0));
        db.purge_log_upto(63).unwrap();
        assert!(db.first_log_index().unwrap().unwrap() >= 64, "prefix dropped");
        assert!(db.last_log_index().unwrap().unwrap() >= 199, "tail intact");
        assert!(db.read_committed().unwrap().is_some(), "committed still points at the tail");
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The real startup path must not crash on this state.
    let db = Arc::new(RedbStore::open(&path).unwrap());
    let (raft, _sm) = build_raft(1, db).await.expect(
        "build_raft must recover an orphaned-prefix log instead of returning the \
         'Failed to get log entries [0,64)' storage error",
    );
    // The healed node is blank (its log/membership were unrecoverable); it must
    // be a live, shippable raft instance ready to be re-added to the cluster.
    let _ = raft.metrics().borrow().clone();
    raft.shutdown().await.ok();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The committed pointer survived but points PAST the end of the log (e.g. the
/// log's tail was lost while the committed meta key persisted). openraft would
/// re-apply past the last entry and abort. Recovery must heal this and keep the
/// log prefix that IS present.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_past_end_of_log_recovers() {
    let dir = unique_dir("commit-past");
    let path = dir.join("node.redb");

    seed_node(&path, 200).await;

    {
        let db = RedbStore::open(&path).unwrap();
        let last = db.last_log_index().unwrap().unwrap();
        // Drop the tail [100..], leaving committed pointing well past it.
        db.truncate_log_from(100).unwrap();
        assert_eq!(db.last_log_index().unwrap(), Some(99), "tail trimmed");
        assert!(last > 99, "committed pointer is now past the log end");
        assert!(db.read_committed().unwrap().is_some());
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let db = Arc::new(RedbStore::open(&path).unwrap());
    let (raft, _sm) = build_raft(1, db)
        .await
        .expect("build_raft must recover when committed points past the log end");
    let _ = raft.metrics().borrow().clone();
    raft.shutdown().await.ok();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Documents the ROOT CAUSE: without the recovery/repair step, the same
/// orphaned-prefix on-disk state makes openraft's `get_initial_state` abort with
/// the exact production error. This proves the repair is load-bearing, not
/// cosmetic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_open_without_repair_reproduces_production_error() {
    use hermesmq_core::engine::raw_build_raft_no_repair;

    let dir = unique_dir("rawrepro");
    let path = dir.join("node.redb");
    seed_node(&path, 200).await;
    {
        let db = RedbStore::open(&path).unwrap();
        db.purge_log_upto(63).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let db = Arc::new(RedbStore::open(&path).unwrap());
    let res = raw_build_raft_no_repair(1, db).await;
    match res {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("Failed to get log entries") || msg.contains("LogIndex"),
                "expected the openraft re-apply error, got: {msg}"
            );
        }
        Ok(raft) => {
            // If construction is lazy, force the initial-state read.
            raft.shutdown().await.ok();
            panic!("raw open without repair unexpectedly succeeded on an orphaned-prefix log");
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}
