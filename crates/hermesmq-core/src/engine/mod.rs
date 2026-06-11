mod log_store;
mod network;
mod state_machine;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use openraft::{BasicNode, Config, Raft, StorageError, StorageIOError};
use serde::de::DeserializeOwned;

pub use log_store::LogStore;
pub use network::{serve_peer, PartitionControl, PeerConnection, PeerNetwork};
pub use state_machine::{SnapshotBuilder, StateMachineStore};

use crate::raft::TypeConfig;
use crate::storage::Storage;
use crate::types::NodeId;
use crate::RedbStore;

pub type HermesRaft = Raft<TypeConfig>;

fn ioerr<E: std::fmt::Display>(e: E) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

pub(crate) fn enc<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, StorageError<NodeId>> {
    postcard::to_stdvec(value).map_err(|e| StorageIOError::write_logs(&ioerr(e)).into())
}

pub(crate) fn dec<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError<NodeId>> {
    postcard::from_bytes(bytes).map_err(|e| StorageIOError::read_logs(&ioerr(e)).into())
}

pub(crate) fn sread<E: std::fmt::Display>(e: E) -> StorageError<NodeId> {
    StorageIOError::read_logs(&ioerr(e)).into()
}

pub(crate) fn swrite<E: std::fmt::Display>(e: E) -> StorageError<NodeId> {
    StorageIOError::write_logs(&ioerr(e)).into()
}

pub async fn build_raft_partitionable<S: Storage>(
    node_id: NodeId,
    db: Arc<S>,
) -> Result<(HermesRaft, StateMachineStore<S>, PartitionControl), Box<dyn std::error::Error + Send + Sync>>
{
    let config = Arc::new(Config {
        heartbeat_interval: 300,
        election_timeout_min: 1000,
        election_timeout_max: 2000,
        max_payload_entries: 32,
        ..Config::default()
    });
    let log = LogStore::new(db.clone());
    let state_machine = StateMachineStore::new(db)?;
    let sm_read = state_machine.clone();
    let network = PeerNetwork::default();
    let blocked = network.blocked_handle();
    let raft = Raft::new(node_id, config, network, log, state_machine).await?;
    Ok((raft, sm_read, blocked))
}

pub async fn build_raft<S: Storage>(
    node_id: NodeId,
    db: Arc<S>,
) -> Result<(HermesRaft, StateMachineStore<S>), Box<dyn std::error::Error + Send + Sync>> {
    let (raft, sm, _blocked) = build_raft_partitionable(node_id, db).await?;
    Ok((raft, sm))
}

pub async fn initialize_cluster(
    raft: &HermesRaft,
    members: &[(NodeId, String)],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let map: BTreeMap<NodeId, BasicNode> = members
        .iter()
        .map(|(id, addr)| (*id, BasicNode::new(addr)))
        .collect();
    raft.initialize(map).await?;
    Ok(())
}

pub async fn add_learner(
    raft: &HermesRaft,
    id: NodeId,
    addr: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    raft.add_learner(id, BasicNode::new(addr), true).await?;
    Ok(())
}

pub async fn set_voters(
    raft: &HermesRaft,
    voters: &[NodeId],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let set: BTreeSet<NodeId> = voters.iter().copied().collect();
    raft.change_membership(set, false).await?;
    Ok(())
}

pub async fn start_single_node(
    node_id: NodeId,
    db: Arc<RedbStore>,
) -> Result<HermesRaft, Box<dyn std::error::Error + Send + Sync>> {
    let (raft, _sm) = build_raft(node_id, db).await?;
    initialize_cluster(&raft, &[(node_id, String::new())]).await?;
    Ok(raft)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{AppRequest, AppResponse, ContentType, GroupId, Priority, TopicId};

    #[tokio::test]
    async fn single_node_bootstrap_and_write() {
        let db = Arc::new(RedbStore::in_memory().unwrap());
        let raft = start_single_node(1, db).await.unwrap();

        raft.wait(Some(Duration::from_secs(10)))
            .current_leader(1, "elect self as leader")
            .await
            .unwrap();

        let created = raft
            .client_write(AppRequest::CreateTopic {
                topic: TopicId::from("orders"),
            })
            .await
            .unwrap();
        assert!(matches!(created.data, AppResponse::TopicCreated));

        let first = raft
            .client_write(AppRequest::Produce {
                topic: TopicId::from("orders"),
                priority: Priority::default(),
                content_type: ContentType::Raw,
                payload: b"hello".to_vec(),
                producer_id: "p1".to_string(),
                seq: 1,
                ts_ms: 0,
            })
            .await
            .unwrap();
        assert!(matches!(first.data, AppResponse::Produced { offset: 0 }));

        let second = raft
            .client_write(AppRequest::Produce {
                topic: TopicId::from("orders"),
                priority: Priority::default(),
                content_type: ContentType::Raw,
                payload: b"world".to_vec(),
                producer_id: "p1".to_string(),
                seq: 2,
                ts_ms: 0,
            })
            .await
            .unwrap();
        assert!(matches!(second.data, AppResponse::Produced { offset: 1 }));

        let polled = raft
            .client_write(AppRequest::Poll {
                topic: TopicId::from("orders"),
                group: GroupId::from("workers"),
                max: 10,
                visibility_timeout_ms: 1000,
                ts_ms: 0,
            })
            .await
            .unwrap();
        let leases: Vec<_> = match polled.data {
            AppResponse::Polled { items } => items,
            other => panic!("expected Polled, got {other:?}"),
        };
        assert_eq!(leases.len(), 2);

        raft.client_write(AppRequest::Ack {
            topic: TopicId::from("orders"),
            group: GroupId::from("workers"),
            lease_id: leases[0].lease_id,
        })
        .await
        .unwrap();

        let after = raft
            .client_write(AppRequest::Poll {
                topic: TopicId::from("orders"),
                group: GroupId::from("workers"),
                max: 10,
                visibility_timeout_ms: 1000,
                ts_ms: 5000,
            })
            .await
            .unwrap();
        match after.data {
            AppResponse::Polled { items } => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].offset, leases[1].offset);
            }
            other => panic!("expected Polled, got {other:?}"),
        }

        raft.shutdown().await.unwrap();
    }
}
