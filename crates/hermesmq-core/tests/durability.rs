use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hermesmq_core::engine::{build_raft, initialize_cluster};
use hermesmq_core::{AppRequest, AppResponse, ContentType, GroupId, HermesRaft, Priority, RedbStore};

async fn wait_leader(raft: &HermesRaft) {
    for _ in 0..200 {
        if raft.current_leader().await.is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader elected");
}

async fn wait_snapshot(raft: &HermesRaft) {
    for _ in 0..200 {
        if raft.metrics().borrow().snapshot.is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no snapshot built");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_recovers_from_disk() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir: PathBuf = std::env::temp_dir().join(format!("hermesmq-durability-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("hermesmq.redb");

    {
        let db = Arc::new(RedbStore::open(&path).unwrap());
        let (raft, sm) = build_raft(1, db).await.unwrap();
        initialize_cluster(&raft, &[(1, "127.0.0.1:9".to_string())])
            .await
            .unwrap();
        wait_leader(&raft).await;

        raft.client_write(AppRequest::CreateTopic {
            topic: "t".into(),
        })
        .await
        .unwrap();
        let produced = raft
            .client_write(AppRequest::Produce {
                topic: "t".into(),
                priority: Priority::default(),
                content_type: ContentType::Raw,
                payload: bytes::Bytes::from_static(b"durable"),
                producer_id: "p1".to_string(),
                seq: 1,
                ts_ms: 0,
            })
            .await
            .unwrap();
        assert!(matches!(produced.data, AppResponse::Produced { offset: 0 }));

        raft.shutdown().await.unwrap();
        drop(raft);
        drop(sm);
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    {
        let db = Arc::new(RedbStore::open(&path).unwrap());
        let (raft, _sm) = build_raft(1, db).await.unwrap();
        wait_leader(&raft).await;

        let polled = raft
            .client_write(AppRequest::Poll {
                topic: "t".into(),
                group: GroupId::from("g"),
                max: 10,
                visibility_timeout_ms: 1000,
                ts_ms: 0,
            })
            .await
            .unwrap();
        match polled.data {
            AppResponse::Polled { items } => {
                assert_eq!(items.len(), 1, "message must survive restart");
                assert_eq!(items[0].payload, &b"durable"[..]);
            }
            other => panic!("expected Polled, got {other:?}"),
        }
        raft.shutdown().await.unwrap();
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_group_committed_writes_survive_restart() {
    const WRITERS: usize = 8;
    const PER_WRITER: usize = 16;

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir: PathBuf = std::env::temp_dir().join(format!("hermesmq-groupcommit-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("hermesmq.redb");

    {
        let db = Arc::new(RedbStore::open(&path).unwrap());
        let (raft, _sm) = build_raft(1, db).await.unwrap();
        initialize_cluster(&raft, &[(1, "127.0.0.1:9".to_string())])
            .await
            .unwrap();
        wait_leader(&raft).await;

        let mut tasks = Vec::new();
        for w in 0..WRITERS {
            let raft = raft.clone();
            tasks.push(tokio::spawn(async move {
                for i in 0..PER_WRITER {
                    raft.client_write(AppRequest::Produce {
                        topic: "t".into(),
                        priority: Priority::default(),
                        content_type: ContentType::Raw,
                        payload: bytes::Bytes::from(vec![w as u8, i as u8]),
                        producer_id: String::new(),
                        seq: 0,
                        ts_ms: 0,
                    })
                    .await
                    .unwrap();
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        raft.shutdown().await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    {
        let db = Arc::new(RedbStore::open(&path).unwrap());
        let (raft, _sm) = build_raft(1, db).await.unwrap();
        wait_leader(&raft).await;

        let polled = raft
            .client_write(AppRequest::Poll {
                topic: "t".into(),
                group: GroupId::from("g"),
                max: 1024,
                visibility_timeout_ms: 1000,
                ts_ms: 0,
            })
            .await
            .unwrap();
        match polled.data {
            AppResponse::Polled { items } => {
                assert_eq!(
                    items.len(),
                    WRITERS * PER_WRITER,
                    "every acknowledged concurrent write must survive restart"
                );
            }
            other => panic!("expected Polled, got {other:?}"),
        }
        raft.shutdown().await.unwrap();
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_then_restart_recovers_state() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir: PathBuf = std::env::temp_dir().join(format!("hermesmq-snapshot-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("hermesmq.redb");

    {
        let db = Arc::new(RedbStore::open(&path).unwrap());
        let (raft, _sm) = build_raft(1, db).await.unwrap();
        initialize_cluster(&raft, &[(1, "127.0.0.1:9".to_string())])
            .await
            .unwrap();
        wait_leader(&raft).await;

        for i in 0..3u8 {
            raft.client_write(AppRequest::Produce {
                topic: "t".into(),
                priority: Priority::default(),
                content_type: ContentType::Raw,
                payload: bytes::Bytes::from(vec![i]),
                producer_id: String::new(),
                seq: 0,
                ts_ms: 0,
            })
            .await
            .unwrap();
        }

        raft.trigger().snapshot().await.unwrap();
        wait_snapshot(&raft).await;
        raft.shutdown().await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    {
        let db = Arc::new(RedbStore::open(&path).unwrap());
        let (raft, _sm) = build_raft(1, db).await.unwrap();
        wait_leader(&raft).await;

        let polled = raft
            .client_write(AppRequest::Poll {
                topic: "t".into(),
                group: GroupId::from("g"),
                max: 10,
                visibility_timeout_ms: 1000,
                ts_ms: 0,
            })
            .await
            .unwrap();
        match polled.data {
            AppResponse::Polled { items } => {
                assert_eq!(items.len(), 3, "snapshotted messages must survive restart");
            }
            other => panic!("expected Polled, got {other:?}"),
        }
        raft.shutdown().await.unwrap();
    }

    let _ = std::fs::remove_dir_all(&dir);
}
