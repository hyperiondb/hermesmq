use std::sync::Arc;
use std::time::Duration;

use hermesmq_core::engine::{
    add_learner, build_raft, build_raft_partitionable, initialize_cluster, serve_peer, set_voters,
    PartitionControl,
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
        payload: body.as_bytes().to_vec(),
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
            payload: b"hello".to_vec(),
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
            assert_eq!(items[0].payload, b"hello");
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
            payload: b"hello".to_vec(),
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
