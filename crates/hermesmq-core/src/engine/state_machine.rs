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

#[doc(hidden)]
pub const KEY_SNAPSHOT: &str = "sm:snapshot";

// Legacy (pre-0.8) snapshot bytes start with an Option tag (0x00/0x01), so a
// 0xFF prefix unambiguously marks the versioned format.
const SNAPSHOT_FORMAT: [u8; 2] = [0xFF, 1];

#[derive(Default, Clone, Serialize, Deserialize)]
struct StateMachineData {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    queue: Queue,
}

#[derive(Deserialize)]
struct StateMachineDataV0 {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    queue: crate::queue::compat::QueueV0,
}

fn encode_state(data: &StateMachineData) -> Result<Vec<u8>, StorageError<NodeId>> {
    let mut bytes = SNAPSHOT_FORMAT.to_vec();
    bytes.extend(enc(data)?);
    Ok(bytes)
}

/// Decode snapshot bytes and refuse the ones the queue could not have produced.
///
/// Snapshot bytes arrive two ways: the record on disk, read on every start, and
/// the peer port, which hands whatever a peer sends to the Raft snapshot
/// installer. Decoding used to be the only check, so any state postcard could
/// parse replaced the queue wholesale — including states with counters at the
/// top of their range (which then wrap or panic on the next request) and with a
/// dedup map its eviction queue cannot reach.
fn decode_state(bytes: &[u8]) -> Result<StateMachineData, StorageError<NodeId>> {
    let mut data: StateMachineData = match bytes.strip_prefix(&SNAPSHOT_FORMAT) {
        Some(rest) => dec(rest)?,
        None => {
            let v0: StateMachineDataV0 = dec(bytes)?;
            StateMachineData {
                last_applied: v0.last_applied,
                last_membership: v0.last_membership,
                queue: v0.queue.into(),
            }
        }
    };
    data.queue.normalize();
    if let Err(reason) = data.queue.validate() {
        return Err(sread(format!("snapshot state is not self-consistent: {reason}")));
    }
    Ok(data)
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

/// Decode of snapshot body bytes, exposed for the fuzz targets.
#[doc(hidden)]
pub fn fuzz_decode_state(bytes: &[u8]) -> bool {
    decode_state(bytes).is_ok()
}

/// Write a snapshot record straight into the store, exposed for the fuzz targets.
#[doc(hidden)]
pub fn fuzz_store_snapshot<S: Storage>(
    db: &S,
    last_log_id: Option<LogId<NodeId>>,
    data: Vec<u8>,
) -> Result<(), StorageError<NodeId>> {
    let stored = StoredSnapshot {
        meta: SnapshotMeta {
            last_log_id,
            last_membership: StoredMembership::default(),
            snapshot_id: "fuzz".to_string(),
        },
        data,
    };
    persist_snapshot(db, &stored)
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
                decode_state(&stored.data)?
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
        crate::lock(&self.inner).data.queue.rate_config(topic)
    }

    pub fn metrics(&self) -> crate::queue::QueueMetrics {
        crate::lock(&self.inner).data.queue.metrics()
    }

    pub fn has_deliverable(&self, topic: &str, group: &str, now_ms: u64) -> bool {
        crate::lock(&self.inner)
            .data
            .queue
            .has_deliverable(topic, group, now_ms)
    }

    /// Whether a request naming this topic can proceed, so the client port can
    /// turn one away before it becomes a replicated write.
    pub fn topic_admits(&self, topic: &str) -> bool {
        crate::lock(&self.inner).data.queue.topic_admits(topic)
    }

    /// Apply one request directly to the installed queue, exposed for the fuzz targets.
    #[doc(hidden)]
    pub fn debug_apply(&self, req: crate::AppRequest) -> AppResponse {
        crate::lock(&self.inner).data.queue.apply(req)
    }

    /// Run [`Queue::debug_check_invariants`] over the installed queue.
    #[doc(hidden)]
    pub fn debug_check_invariants(&self) -> Result<(), String> {
        crate::lock(&self.inner).data.queue.debug_check_invariants()
    }
}

fn persist_snapshot<S: Storage>(
    db: &S,
    stored: &StoredSnapshot,
) -> Result<(), StorageError<NodeId>> {
    let bytes = enc(stored)?;
    db.put(KEY_SNAPSHOT, &bytes).map_err(swrite)
}

/// The highest log index covered by the persisted snapshot, if any.
///
/// This is the "applied base" the log must be contiguous with after a restart:
/// with a snapshot at index `S`, a replayable log starts at `S + 1`. Used by the
/// startup recovery step to detect a log whose prefix was lost.
pub(crate) fn snapshot_last_index<S: Storage>(
    db: &S,
) -> Result<Option<u64>, StorageError<NodeId>> {
    match db.get(KEY_SNAPSHOT).map_err(sread)? {
        Some(bytes) => {
            let stored: StoredSnapshot = dec(&bytes)?;
            Ok(stored.meta.last_log_id.map(|l| l.index))
        }
        None => Ok(None),
    }
}

impl<S: Storage> RaftStateMachine<TypeConfig> for StateMachineStore<S> {
    type SnapshotBuilder = SnapshotBuilder<S>;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let inner = crate::lock(&self.inner);
        Ok((inner.data.last_applied, inner.data.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<AppResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = crate::lock(&self.inner);
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
        let data = decode_state(&bytes)?;
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: bytes,
        };
        persist_snapshot(self.db.as_ref(), &stored)?;
        crate::lock(&self.inner).data = data;
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
        let (meta, data) = {
            let mut inner = crate::lock(&self.inner);
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
            (meta, inner.data.clone())
        };
        let bytes = encode_state(&data)?;
        let stored = StoredSnapshot { meta, data: bytes };
        persist_snapshot(self.db.as_ref(), &stored)?;
        Ok(Snapshot {
            meta: stored.meta,
            snapshot: Box::new(Cursor::new(stored.data)),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, VecDeque};

    use bytes::Bytes;
    use openraft::CommittedLeaderId;
    use serde::Serialize;

    use super::*;
    use crate::types::{ContentType, Message, Priority};
    use crate::AppRequest;

    #[derive(Serialize)]
    struct OldGroup {
        ack_watermark: u64,
        acked_above: BTreeSet<u64>,
        in_flight: BTreeMap<u64, (u64, u64)>,
        leased_offsets: BTreeMap<u64, u64>,
        poll_count: u64,
    }

    #[derive(Serialize)]
    struct OldTopic {
        next_offset: u64,
        messages: BTreeMap<u64, Message>,
        dedup: BTreeMap<(String, u64), u64>,
        dedup_order: VecDeque<(String, u64)>,
        groups: BTreeMap<String, OldGroup>,
        rate_milli_per_sec: u64,
        burst: u32,
        retain_max_messages: u64,
        retain_max_age_ms: u64,
    }

    #[derive(Serialize)]
    struct OldQueue {
        topics: BTreeMap<String, OldTopic>,
        next_lease_id: u64,
    }

    #[derive(Serialize)]
    struct OldData {
        last_applied: Option<LogId<NodeId>>,
        last_membership: StoredMembership<NodeId, BasicNode>,
        queue: OldQueue,
    }

    #[test]
    fn legacy_snapshot_bytes_decode_and_convert() {
        let mut groups = BTreeMap::new();
        groups.insert(
            "g".to_string(),
            OldGroup {
                ack_watermark: 1,
                acked_above: BTreeSet::new(),
                in_flight: BTreeMap::from([(7u64, (1u64, 500u64))]),
                leased_offsets: BTreeMap::from([(1u64, 500u64)]),
                poll_count: 3,
            },
        );
        let mut topics = BTreeMap::new();
        topics.insert(
            "t".to_string(),
            OldTopic {
                next_offset: 2,
                messages: BTreeMap::from([(
                    1u64,
                    Message {
                        offset: 1,
                        priority: Priority(0),
                        content_type: ContentType::Raw,
                        payload: Bytes::from_static(b"x"),
                        ts_ms: 0,
                    },
                )]),
                dedup: BTreeMap::new(),
                dedup_order: VecDeque::new(),
                groups,
                rate_milli_per_sec: 0,
                burst: 0,
                retain_max_messages: 0,
                retain_max_age_ms: 0,
            },
        );
        let old = OldData {
            last_applied: Some(LogId::new(CommittedLeaderId::new(2, 1), 9)),
            last_membership: StoredMembership::default(),
            queue: OldQueue {
                topics,
                next_lease_id: 8,
            },
        };
        let bytes = postcard::to_stdvec(&old).unwrap();

        let data = decode_state(&bytes).expect("legacy snapshot bytes must still decode");
        assert_eq!(data.last_applied.map(|l| l.index), Some(9));
        let m = data.queue.metrics();
        assert_eq!((m.topics, m.messages, m.in_flight), (1, 1, 1));
        assert!(!data.queue.has_deliverable("t", "g", 100), "lease still active");
        assert!(data.queue.has_deliverable("t", "g", 1_000), "lease expired at 500");
    }

    fn legacy_bytes(topic: OldTopic, next_lease_id: u64) -> Vec<u8> {
        postcard::to_stdvec(&OldData {
            last_applied: None,
            last_membership: StoredMembership::default(),
            queue: OldQueue {
                topics: BTreeMap::from([("t".to_string(), topic)]),
                next_lease_id,
            },
        })
        .unwrap()
    }

    fn empty_topic(next_offset: u64) -> OldTopic {
        OldTopic {
            next_offset,
            messages: BTreeMap::new(),
            dedup: BTreeMap::new(),
            dedup_order: VecDeque::new(),
            groups: BTreeMap::new(),
            rate_milli_per_sec: 0,
            burst: 0,
            retain_max_messages: 0,
            retain_max_age_ms: 0,
        }
    }

    #[test]
    fn a_snapshot_with_an_impossible_counter_is_refused() {
        let bytes = legacy_bytes(empty_topic(u64::MAX), 0);
        assert!(
            decode_state(&bytes).is_err(),
            "an offset counter at the top of its range must not be installed"
        );
    }

    #[test]
    fn a_snapshot_whose_dedup_queue_cannot_evict_is_refused() {
        let mut topic = empty_topic(1);
        topic.dedup = BTreeMap::from([(("p".to_string(), 0u64), 0u64)]);
        let bytes = legacy_bytes(topic, 0);
        assert!(
            decode_state(&bytes).is_err(),
            "a dedup entry the eviction queue cannot reach must not be installed"
        );
    }

    #[test]
    fn a_legacy_watermark_past_the_end_is_normalized() {
        let mut topic = empty_topic(2);
        topic.groups = BTreeMap::from([(
            "g".to_string(),
            OldGroup {
                ack_watermark: u64::MAX,
                acked_above: BTreeSet::new(),
                in_flight: BTreeMap::new(),
                leased_offsets: BTreeMap::new(),
                poll_count: 0,
            },
        )]);
        let bytes = legacy_bytes(topic, 0);
        let data = decode_state(&bytes).expect("a watermark an old commit could set must still load");
        assert_eq!(data.queue.validate(), Ok(()));
    }

    #[test]
    fn snapshot_state_roundtrips_in_versioned_format() {
        let mut data = StateMachineData::default();
        data.queue.apply(AppRequest::Produce {
            topic: crate::TopicId::from("t"),
            priority: Priority(0),
            content_type: ContentType::Raw,
            payload: Bytes::from_static(b"hello"),
            producer_id: String::new(),
            seq: 0,
            ts_ms: 0,
        });
        let bytes = encode_state(&data).unwrap();
        assert_eq!(&bytes[..2], &SNAPSHOT_FORMAT);
        let back = decode_state(&bytes).unwrap();
        assert_eq!(back.queue.metrics().messages, 1);
    }
}
