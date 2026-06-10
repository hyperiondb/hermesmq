use std::io::Cursor;
use std::sync::{Arc, Mutex};

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, Snapshot, SnapshotMeta, StorageError,
    StoredMembership,
};
use serde::{Deserialize, Serialize};

use super::{dec, enc, sread, swrite};
use crate::queue::Queue;
use crate::raft::TypeConfig;
use crate::storage::Storage;
use crate::types::NodeId;
use crate::{AppResponse, RedbStore};

const KEY_SNAPSHOT: &str = "sm:snapshot";

#[derive(Default, Clone, Serialize, Deserialize)]
struct StateMachineData {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    queue: Queue,
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

struct Inner {
    data: StateMachineData,
    snapshot_seq: u64,
}

pub struct StateMachineStore<S = RedbStore> {
    inner: Arc<Mutex<Inner>>,
    db: Arc<S>,
}

impl<S> Clone for StateMachineStore<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            db: Arc::clone(&self.db),
        }
    }
}

impl<S: Storage> StateMachineStore<S> {
    pub fn new(db: Arc<S>) -> Result<Self, StorageError<NodeId>> {
        let data = match db.get(KEY_SNAPSHOT).map_err(sread)? {
            Some(bytes) => {
                let stored: StoredSnapshot = dec(&bytes)?;
                dec(&stored.data)?
            }
            None => StateMachineData::default(),
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                data,
                snapshot_seq: 0,
            })),
            db,
        })
    }

    pub fn rate_config(&self, topic: &str) -> Option<(u64, u32)> {
        self.inner.lock().unwrap().data.queue.rate_config(topic)
    }

    pub fn metrics(&self) -> crate::queue::QueueMetrics {
        self.inner.lock().unwrap().data.queue.metrics()
    }

    pub fn has_deliverable(&self, topic: &str, group: &str, now_ms: u64) -> bool {
        self.inner
            .lock()
            .unwrap()
            .data
            .queue
            .has_deliverable(topic, group, now_ms)
    }
}

fn persist_snapshot<S: Storage>(
    db: &S,
    stored: &StoredSnapshot,
) -> Result<(), StorageError<NodeId>> {
    let bytes = enc(stored)?;
    db.put(KEY_SNAPSHOT, &bytes).map_err(swrite)
}

impl<S: Storage> RaftStateMachine<TypeConfig> for StateMachineStore<S> {
    type SnapshotBuilder = SnapshotBuilder<S>;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let inner = self.inner.lock().unwrap();
        Ok((inner.data.last_applied, inner.data.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<AppResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = self.inner.lock().unwrap();
        let mut responses = Vec::new();
        for entry in entries {
            let log_id = entry.log_id;
            inner.data.last_applied = Some(log_id);
            let response = match entry.payload {
                EntryPayload::Blank => AppResponse::NoOp,
                EntryPayload::Membership(m) => {
                    inner.data.last_membership = StoredMembership::new(Some(log_id), m);
                    AppResponse::NoOp
                }
                EntryPayload::Normal(req) => inner.data.queue.apply(req),
            };
            responses.push(response);
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            inner: Arc::clone(&self.inner),
            db: Arc::clone(&self.db),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = (*snapshot).into_inner();
        let data: StateMachineData = dec(&bytes)?;
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: bytes,
        };
        persist_snapshot(self.db.as_ref(), &stored)?;
        self.inner.lock().unwrap().data = data;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match self.db.get(KEY_SNAPSHOT).map_err(sread)? {
            Some(bytes) => {
                let stored: StoredSnapshot = dec(&bytes)?;
                Ok(Some(Snapshot {
                    meta: stored.meta,
                    snapshot: Box::new(Cursor::new(stored.data)),
                }))
            }
            None => Ok(None),
        }
    }
}

pub struct SnapshotBuilder<S = RedbStore> {
    inner: Arc<Mutex<Inner>>,
    db: Arc<S>,
}

impl<S: Storage> RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder<S> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (meta, bytes) = {
            let mut inner = self.inner.lock().unwrap();
            let bytes = enc(&inner.data)?;
            inner.snapshot_seq += 1;
            let last = inner.data.last_applied;
            let snapshot_id = match &last {
                Some(log_id) => format!("{}-{}", log_id.index, inner.snapshot_seq),
                None => format!("none-{}", inner.snapshot_seq),
            };
            let meta = SnapshotMeta {
                last_log_id: last,
                last_membership: inner.data.last_membership.clone(),
                snapshot_id,
            };
            (meta, bytes)
        };
        let stored = StoredSnapshot { meta, data: bytes };
        persist_snapshot(self.db.as_ref(), &stored)?;
        Ok(Snapshot {
            meta: stored.meta,
            snapshot: Box::new(Cursor::new(stored.data)),
        })
    }
}
