use std::sync::Arc;
use std::time::Duration;

use hermesmq_core::engine::{build_raft, initialize_cluster};
use hermesmq_core::{
    AppRequest, AppResponse, ContentType, GroupId, HermesRaft, Priority, RedbStore, Result, Storage,
    TopicId,
};

struct LatencyStore {
    inner: RedbStore,
    write_delay: Duration,
}

impl LatencyStore {
    fn slow(&self) {
        std::thread::sleep(self.write_delay);
    }
}

impl Storage for LatencyStore {
    fn append_log(&self, entries: &[(u64, Vec<u8>)]) -> Result<()> {
        self.slow();
        self.inner.append_log(entries)
    }
    fn read_log(&self, start: u64, end: u64) -> Result<Vec<(u64, Vec<u8>)>> {
        self.inner.read_log(start, end)
    }
    fn truncate_log_from(&self, index: u64) -> Result<()> {
        self.slow();
        self.inner.truncate_log_from(index)
    }
    fn purge_log_upto(&self, index: u64) -> Result<()> {
        self.slow();
        self.inner.purge_log_upto(index)
    }
    fn first_log_index(&self) -> Result<Option<u64>> {
        self.inner.first_log_index()
    }
    fn last_log_index(&self) -> Result<Option<u64>> {
        self.inner.last_log_index()
    }
    fn save_vote(&self, vote: &[u8]) -> Result<()> {
        self.slow();
        self.inner.save_vote(vote)
    }
    fn read_vote(&self) -> Result<Option<Vec<u8>>> {
        self.inner.read_vote()
    }
    fn save_committed(&self, committed: &[u8]) -> Result<()> {
        self.slow();
        self.inner.save_committed(committed)
    }
    fn read_committed(&self) -> Result<Option<Vec<u8>>> {
        self.inner.read_committed()
    }
    fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        self.slow();
        self.inner.put(key, value)
    }
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn delete(&self, key: &str) -> Result<()> {
        self.slow();
        self.inner.delete(key)
    }
    fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        self.inner.scan_prefix(prefix)
    }
}

async fn wait_leader(raft: &HermesRaft) {
    for _ in 0..400 {
        if raft.current_leader().await.is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader elected under slow disk");
}

fn produce(body: &[u8]) -> AppRequest {
    AppRequest::Produce {
        topic: TopicId::from("t"),
        priority: Priority::default(),
        content_type: ContentType::Raw,
        payload: body.to_vec(),
        producer_id: String::new(),
        seq: 0,
        ts_ms: 0,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tolerates_slow_disk() {
    let store = LatencyStore {
        inner: RedbStore::in_memory().unwrap(),
        write_delay: Duration::from_millis(8),
    };
    let db = Arc::new(store);

    let (raft, _sm) = build_raft(1, db).await.unwrap();
    initialize_cluster(&raft, &[(1, "127.0.0.1:9".to_string())])
        .await
        .unwrap();
    wait_leader(&raft).await;

    raft.client_write(AppRequest::CreateTopic {
        topic: TopicId::from("t"),
    })
    .await
    .unwrap();

    for i in 0..5u8 {
        let r = raft.client_write(produce(&[i])).await.unwrap();
        assert!(matches!(r.data, AppResponse::Produced { .. }));
    }

    let polled = raft
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
            assert_eq!(items.len(), 5, "all messages durable + consumable despite slow disk")
        }
        other => panic!("expected Polled, got {other:?}"),
    }

    raft.shutdown().await.unwrap();
}
