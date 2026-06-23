mod log_store;
mod network;
mod state_machine;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use openraft::{BasicNode, Config, LogId, Raft, StorageError, StorageIOError};
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

/// Make the persisted state self-consistent before openraft reads it on startup.
///
/// openraft rebuilds the state machine by re-applying the log over
/// `(last_applied .. committed]` (see `openraft::storage::helper`). It trusts
/// `get_log_state()` and `read_committed()` to describe a log it can actually
/// serve. After a partial disk loss those can disagree with the bytes on disk,
/// and openraft aborts with `Failed to get log entries`. This heals the
/// reachable damage shapes:
///
/// * **Orphaned prefix** — the contiguous log no longer starts where replay must
///   begin (index 0, or `snapshot + 1`). Its tail can't be replayed from the
///   applied base, so drop the whole log and fall back to the snapshot (if any)
///   plus re-sync from the leader.
/// * **Unbacked commit** — `committed` points past everything we can serve (the
///   snapshot index or the last surviving log entry). Drop the pointer; openraft
///   re-derives it.
///
/// A healthy store — log starting at the applied base, commit within range — is
/// left untouched, so a normal restart still replays its full committed log.
fn recover_consistency<S: Storage>(db: &S) -> crate::Result<()> {
    let to_err = |e: StorageError<NodeId>| crate::Error::Storage(e.to_string());

    let snap_index = state_machine::snapshot_last_index(db).map_err(to_err)?;
    let base_next = snap_index.map(|i| i + 1).unwrap_or(0);

    // Orphaned prefix: the log's front is ahead of where replay must begin, so
    // the gap between the applied base and the log is unrecoverable locally.
    if let Some(first) = db.first_log_index()? {
        if first > base_next {
            if let Some(last) = db.last_log_index()? {
                db.purge_log_upto(last)?;
            }
            db.delete(log_store::KEY_PURGED)?;
        }
    }

    // Unbacked commit: a committed pointer past the highest index we can serve
    // (snapshot or last log entry) would make openraft re-apply missing entries.
    let reachable = snap_index.into_iter().chain(db.last_log_index()?).max();
    if let Some(bytes) = db.read_committed()? {
        let committed: Option<LogId<NodeId>> = dec(&bytes).map_err(to_err)?;
        if let Some(c) = committed {
            if reachable.map(|r| c.index > r).unwrap_or(true) {
                db.delete(crate::storage::KEY_COMMITTED)?;
            }
        }
    }
    Ok(())
}

/// openraft's snapshot defaults (`install_snapshot_timeout` 200ms,
/// `snapshot_max_chunk_size` 3 MiB) ship a multi-megabyte snapshot as a *single*
/// chunk with a 200ms RPC deadline. On a real network — a container/overlay link
/// in particular — that chunk can't round-trip in time, so the leader times out
/// and openraft restarts the transfer from offset 0 *forever*; a wiped node
/// never catches up. Send the snapshot in small chunks with a generous per-chunk
/// timeout so progress is incremental, resumable, and bounded by chunk size
/// rather than total snapshot size.
const INSTALL_SNAPSHOT_TIMEOUT_MS: u64 = 30_000;
const SNAPSHOT_CHUNK_BYTES: u64 = 1024 * 1024;

pub async fn build_raft_partitionable<S: Storage>(
    node_id: NodeId,
    db: Arc<S>,
) -> Result<(HermesRaft, StateMachineStore<S>, PartitionControl), Box<dyn std::error::Error + Send + Sync>>
{
    recover_consistency(db.as_ref())?;
    let config = Arc::new(Config {
        heartbeat_interval: 300,
        election_timeout_min: 1000,
        election_timeout_max: 2000,
        max_payload_entries: 32,
        max_in_snapshot_log_to_keep: 0,
        install_snapshot_timeout: INSTALL_SNAPSHOT_TIMEOUT_MS,
        snapshot_max_chunk_size: SNAPSHOT_CHUNK_BYTES,
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

/// Test-support: construct a Raft node WITHOUT running `recover_consistency`.
///
/// Exposed (hidden) so the restart-conformance suite can demonstrate that an
/// inconsistent on-disk state aborts openraft's `get_initial_state` — i.e. that
/// the recovery step is load-bearing. Production code must use [`build_raft`].
#[doc(hidden)]
pub async fn raw_build_raft_no_repair<S: Storage>(
    node_id: NodeId,
    db: Arc<S>,
) -> Result<HermesRaft, Box<dyn std::error::Error + Send + Sync>> {
    let config = Arc::new(Config {
        heartbeat_interval: 300,
        election_timeout_min: 1000,
        election_timeout_max: 2000,
        max_payload_entries: 32,
        max_in_snapshot_log_to_keep: 0,
        ..Config::default()
    });
    let log = LogStore::new(db.clone());
    let state_machine = StateMachineStore::new(db)?;
    let network = PeerNetwork::default();
    let raft = Raft::new(node_id, config, network, log, state_machine).await?;
    Ok(raft)
}

pub async fn build_raft_tuned<S: Storage>(
    node_id: NodeId,
    db: Arc<S>,
    snapshot_logs_since_last: u64,
    max_in_snapshot_log_to_keep: u64,
    snapshot_chunk_bytes: u64,
) -> Result<(HermesRaft, StateMachineStore<S>), Box<dyn std::error::Error + Send + Sync>> {
    recover_consistency(db.as_ref())?;
    let config = Arc::new(Config {
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        max_payload_entries: 32,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(snapshot_logs_since_last),
        max_in_snapshot_log_to_keep,
        install_snapshot_timeout: INSTALL_SNAPSHOT_TIMEOUT_MS,
        snapshot_max_chunk_size: snapshot_chunk_bytes,
        ..Config::default()
    });
    let log = LogStore::new(db.clone());
    let state_machine = StateMachineStore::new(db)?;
    let sm_read = state_machine.clone();
    let network = PeerNetwork::default();
    let raft = Raft::new(node_id, config, network, log, state_machine).await?;
    Ok((raft, sm_read))
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
                payload: bytes::Bytes::from_static(b"hello"),
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
                payload: bytes::Bytes::from_static(b"world"),
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
