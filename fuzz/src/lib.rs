//! Shared input model for the structure-aware hermesmq fuzz targets.
//!
//! Everything here is bounded on purpose: small name alphabets so operations
//! collide, short payloads, and a capped operation count, so one iteration stays
//! cheap enough for coverage to grow.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use arbitrary::Arbitrary;
use bytes::Bytes;
use hermesmq_core::raft::{AppRequest, AppResponse, ProduceItem};
use hermesmq_core::types::{ContentType, LeaseId, Message, Offset};
use hermesmq_core::{GroupId, Priority, Queue, TopicId};
use serde::Serialize;

pub const MAX_OPS: usize = 200;
pub const MAX_BATCH: usize = 8;
pub const TOPIC_ALPHABET: u8 = 4;
pub const GROUP_ALPHABET: u8 = 4;
const RECENT_LEASES: usize = 64;

pub fn topic_name(n: u8) -> String {
    format!("t{}", n % TOPIC_ALPHABET)
}

pub fn group_name(n: u8) -> String {
    format!("g{}", n % GROUP_ALPHABET)
}

#[derive(Arbitrary, Debug, Clone)]
pub enum Ref {
    /// One of the lease ids (or offsets) the run has actually seen.
    Recent(u8),
    /// An arbitrary value, including ones no operation ever produced.
    Raw(u64),
}

#[derive(Arbitrary, Debug, Clone)]
pub struct Item {
    pub topic: u8,
    pub priority: u8,
    pub content_type: u8,
    pub producer: u8,
    pub seq: u8,
    pub payload_len: u8,
}

#[derive(Arbitrary, Debug, Clone)]
pub enum Op {
    CreateTopic(u8),
    DeleteTopic(u8),
    Produce(Item),
    ProduceMany(Vec<Item>),
    Poll { topic: u8, group: u8, max: u8, vis: u16 },
    PollAuto { topic: u8, group: u8, max: u8, vis: u16 },
    Ack { topic: u8, group: u8, lease: Ref },
    Nack { topic: u8, group: u8, lease: Ref },
    AckMany { topic: u8, group: u8, leases: Vec<Ref> },
    NackMany { topic: u8, group: u8, leases: Vec<Ref> },
    Commit { topic: u8, group: u8, offset: Ref },
    SetRateLimit { topic: u8, rate_milli: u64, burst: u32 },
    SetRetention { topic: u8, max_messages: u8, max_age_ms: u16 },
    Advance(u16),
    SetClock(u32),
}

fn content_type(n: u8) -> ContentType {
    match n % 4 {
        1 => ContentType::Text,
        2 => ContentType::Json,
        3 => ContentType::MsgPack,
        _ => ContentType::Raw,
    }
}

/// Turns [`Op`] values into [`AppRequest`] values, carrying the clock and the
/// lease/offset ids the run has observed so acks and commits actually land.
#[derive(Default)]
pub struct Driver {
    pub now: u64,
    leases: VecDeque<LeaseId>,
    offsets: VecDeque<Offset>,
    pub retention: BTreeMap<String, u64>,
}

impl Driver {
    pub fn new() -> Self {
        Self::default()
    }

    fn pick(pool: &VecDeque<u64>, r: &Ref) -> u64 {
        match r {
            Ref::Recent(i) => {
                if pool.is_empty() {
                    0
                } else {
                    pool[*i as usize % pool.len()]
                }
            }
            Ref::Raw(v) => *v,
        }
    }

    fn remember(pool: &mut VecDeque<u64>, v: u64) {
        if pool.len() == RECENT_LEASES {
            pool.pop_front();
        }
        pool.push_back(v);
    }

    fn produce_item(&self, i: &Item) -> ProduceItem {
        ProduceItem {
            topic: TopicId(topic_name(i.topic)),
            priority: Priority(i.priority % 8),
            content_type: content_type(i.content_type),
            payload: Bytes::from(vec![i.payload_len; i.payload_len as usize]),
            producer_id: if i.producer == 0 {
                String::new()
            } else {
                format!("p{}", i.producer % 4)
            },
            seq: (i.seq % 8) as u64,
            ts_ms: self.now,
        }
    }

    /// Maps one operation to a request. Clock operations return `None`.
    pub fn request(&mut self, op: &Op) -> Option<AppRequest> {
        Some(match op {
            Op::Advance(dt) => {
                self.now = self.now.saturating_add(*dt as u64);
                return None;
            }
            Op::SetClock(t) => {
                self.now = *t as u64;
                return None;
            }
            Op::CreateTopic(t) => AppRequest::CreateTopic {
                topic: TopicId(topic_name(*t)),
            },
            Op::DeleteTopic(t) => {
                self.retention.remove(&topic_name(*t));
                AppRequest::DeleteTopic {
                    topic: TopicId(topic_name(*t)),
                }
            }
            Op::Produce(i) => {
                let item = self.produce_item(i);
                AppRequest::Produce {
                    topic: item.topic,
                    priority: item.priority,
                    content_type: item.content_type,
                    payload: item.payload,
                    producer_id: item.producer_id,
                    seq: item.seq,
                    ts_ms: item.ts_ms,
                }
            }
            Op::ProduceMany(items) => AppRequest::ProduceMany {
                items: items
                    .iter()
                    .take(MAX_BATCH)
                    .map(|i| self.produce_item(i))
                    .collect(),
            },
            Op::Poll {
                topic,
                group,
                max,
                vis,
            } => AppRequest::Poll {
                topic: TopicId(topic_name(*topic)),
                group: GroupId(group_name(*group)),
                max: *max as u32,
                visibility_timeout_ms: *vis as u64,
                ts_ms: self.now,
            },
            Op::PollAuto {
                topic,
                group,
                max,
                vis,
            } => AppRequest::PollAuto {
                topic: TopicId(topic_name(*topic)),
                group: GroupId(group_name(*group)),
                max: *max as u32,
                visibility_timeout_ms: *vis as u64,
                ts_ms: self.now,
            },
            Op::Ack {
                topic,
                group,
                lease,
            } => AppRequest::Ack {
                topic: TopicId(topic_name(*topic)),
                group: GroupId(group_name(*group)),
                lease_id: Self::pick(&self.leases, lease),
            },
            Op::Nack {
                topic,
                group,
                lease,
            } => AppRequest::Nack {
                topic: TopicId(topic_name(*topic)),
                group: GroupId(group_name(*group)),
                lease_id: Self::pick(&self.leases, lease),
            },
            Op::AckMany {
                topic,
                group,
                leases,
            } => AppRequest::AckMany {
                topic: TopicId(topic_name(*topic)),
                group: GroupId(group_name(*group)),
                lease_ids: leases
                    .iter()
                    .take(MAX_BATCH)
                    .map(|r| Self::pick(&self.leases, r))
                    .collect(),
            },
            Op::NackMany {
                topic,
                group,
                leases,
            } => AppRequest::NackMany {
                topic: TopicId(topic_name(*topic)),
                group: GroupId(group_name(*group)),
                lease_ids: leases
                    .iter()
                    .take(MAX_BATCH)
                    .map(|r| Self::pick(&self.leases, r))
                    .collect(),
            },
            Op::Commit {
                topic,
                group,
                offset,
            } => AppRequest::CommitOffset {
                topic: TopicId(topic_name(*topic)),
                group: GroupId(group_name(*group)),
                offset: Self::pick(&self.offsets, offset),
            },
            Op::SetRateLimit {
                topic,
                rate_milli,
                burst,
            } => AppRequest::SetRateLimit {
                topic: TopicId(topic_name(*topic)),
                rate_milli_per_sec: *rate_milli,
                burst: *burst,
            },
            Op::SetRetention {
                topic,
                max_messages,
                max_age_ms,
            } => {
                self.retention
                    .insert(topic_name(*topic), *max_messages as u64);
                AppRequest::SetRetention {
                    topic: TopicId(topic_name(*topic)),
                    max_messages: *max_messages as u64,
                    max_age_ms: *max_age_ms as u64,
                }
            }
        })
    }

    pub fn observe(&mut self, resp: &AppResponse) {
        match resp {
            AppResponse::Polled { items } => {
                for d in items {
                    Self::remember(&mut self.leases, d.lease_id);
                    Self::remember(&mut self.offsets, d.offset);
                }
            }
            AppResponse::Produced { offset } => {
                Self::remember(&mut self.offsets, offset.saturating_add(1));
                Self::remember(&mut self.offsets, *offset);
            }
            AppResponse::ProducedMany { offsets } => {
                for o in offsets {
                    Self::remember(&mut self.offsets, *o);
                }
            }
            _ => {}
        }
    }
}

/// Applies a bounded operation sequence to a queue and returns every response.
pub fn run(q: &mut Queue, ops: &[Op]) -> Vec<AppResponse> {
    let mut driver = Driver::new();
    let mut out = Vec::new();
    for op in ops.iter().take(MAX_OPS) {
        if let Some(req) = driver.request(op) {
            let resp = q.apply(req);
            driver.observe(&resp);
            out.push(resp);
        }
    }
    out
}

pub fn encode(q: &Queue) -> Vec<u8> {
    postcard::to_stdvec(q).expect("a queue must always encode")
}

// ---------------------------------------------------------------------------
// Pre-0.8 snapshot shape.
//
// `hermesmq_core::queue::compat::QueueV0` is private, and by design accepts
// anything postcard can decode. These mirrors let a target hand the decoder a
// structurally valid but semantically impossible state, which is what a peer on
// the (unauthenticated) peer port or a damaged snapshot record can do.
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct V0Group {
    pub ack_watermark: u64,
    pub acked_above: BTreeSet<u64>,
    pub in_flight: BTreeMap<u64, (u64, u64)>,
    pub leased_offsets: BTreeMap<u64, u64>,
    pub poll_count: u64,
}

#[derive(Serialize)]
pub struct V0Topic {
    pub next_offset: u64,
    pub messages: BTreeMap<u64, Message>,
    pub dedup: BTreeMap<(String, u64), u64>,
    pub dedup_order: VecDeque<(String, u64)>,
    pub groups: BTreeMap<String, V0Group>,
    pub rate_milli_per_sec: u64,
    pub burst: u32,
    pub retain_max_messages: u64,
    pub retain_max_age_ms: u64,
}

#[derive(Serialize)]
pub struct V0Queue {
    pub topics: BTreeMap<String, V0Topic>,
    pub next_lease_id: u64,
}

#[derive(Serialize)]
pub struct V0Data {
    pub last_applied: Option<openraft::LogId<u64>>,
    pub last_membership: openraft::StoredMembership<u64, openraft::BasicNode>,
    pub queue: V0Queue,
}

/// The shape of an adversarial legacy snapshot, kept small and fully specified.
#[derive(Arbitrary, Debug, Clone)]
pub struct SnapshotShape {
    pub next_offset: u64,
    pub message_offsets: Vec<u64>,
    pub ack_watermark: u64,
    pub acked_above: Vec<u64>,
    pub leases: Vec<(u64, u64, u64)>,
    pub leased_offsets: Vec<(u64, u64)>,
    pub next_lease_id: u64,
    pub retain_max_messages: u64,
    pub retain_max_age_ms: u64,
    pub poll_count: u64,
}

impl SnapshotShape {
    pub fn encode(&self) -> Vec<u8> {
        let mut messages = BTreeMap::new();
        for offset in self.message_offsets.iter().take(MAX_BATCH) {
            messages.insert(
                *offset,
                Message {
                    offset: *offset,
                    priority: Priority(0),
                    content_type: ContentType::Raw,
                    payload: Bytes::from_static(b"x"),
                    ts_ms: 0,
                },
            );
        }
        let group = V0Group {
            ack_watermark: self.ack_watermark,
            acked_above: self.acked_above.iter().take(MAX_BATCH).copied().collect(),
            in_flight: self
                .leases
                .iter()
                .take(MAX_BATCH)
                .map(|(id, offset, deadline)| (*id, (*offset, *deadline)))
                .collect(),
            leased_offsets: self.leased_offsets.iter().take(MAX_BATCH).copied().collect(),
            poll_count: self.poll_count,
        };
        let topic = V0Topic {
            next_offset: self.next_offset,
            messages,
            dedup: BTreeMap::new(),
            dedup_order: VecDeque::new(),
            groups: BTreeMap::from([(group_name(0), group)]),
            rate_milli_per_sec: 0,
            burst: 0,
            retain_max_messages: self.retain_max_messages,
            retain_max_age_ms: self.retain_max_age_ms,
        };
        let data = V0Data {
            last_applied: None,
            last_membership: openraft::StoredMembership::default(),
            queue: V0Queue {
                topics: BTreeMap::from([(topic_name(0), topic)]),
                next_lease_id: self.next_lease_id,
            },
        };
        postcard::to_stdvec(&data).expect("the legacy shape must always encode")
    }
}

// ---------------------------------------------------------------------------
// A plain in-memory `Storage`.
//
// `RedbStore::in_memory()` still runs redb's page manager and a write
// transaction per call, which holds the store-shaped targets to single-digit
// executions per second. This backend keeps those targets fast enough for
// coverage to grow; `store_model` still runs against the real redb store on
// half of its inputs.
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct MemStore {
    log: std::sync::Mutex<BTreeMap<u64, Vec<u8>>>,
    meta: std::sync::Mutex<BTreeMap<String, Vec<u8>>>,
}

const MEM_KEY_VOTE: &str = "sys:vote";
const MEM_KEY_COMMITTED: &str = "sys:committed";

impl hermesmq_core::Storage for MemStore {
    fn append_log(&self, entries: &[(u64, Bytes)]) -> hermesmq_core::Result<()> {
        let mut log = self.log.lock().unwrap();
        for (index, bytes) in entries {
            log.insert(*index, bytes.to_vec());
        }
        Ok(())
    }

    fn read_log(&self, start: u64, end: u64) -> hermesmq_core::Result<Vec<(u64, Vec<u8>)>> {
        Ok(self
            .log
            .lock()
            .unwrap()
            .range(start..end)
            .map(|(k, v)| (*k, v.clone()))
            .collect())
    }

    fn truncate_log_from(&self, index: u64) -> hermesmq_core::Result<()> {
        self.log.lock().unwrap().retain(|k, _| *k < index);
        Ok(())
    }

    fn purge_log_upto(&self, index: u64) -> hermesmq_core::Result<()> {
        self.log.lock().unwrap().retain(|k, _| *k > index);
        Ok(())
    }

    fn first_log_index(&self) -> hermesmq_core::Result<Option<u64>> {
        Ok(self.log.lock().unwrap().keys().next().copied())
    }

    fn last_log_index(&self) -> hermesmq_core::Result<Option<u64>> {
        Ok(self.log.lock().unwrap().keys().next_back().copied())
    }

    fn save_vote(&self, vote: &[u8]) -> hermesmq_core::Result<()> {
        self.put(MEM_KEY_VOTE, vote)
    }

    fn read_vote(&self) -> hermesmq_core::Result<Option<Vec<u8>>> {
        self.get(MEM_KEY_VOTE)
    }

    fn save_committed(&self, committed: &[u8]) -> hermesmq_core::Result<()> {
        self.put(MEM_KEY_COMMITTED, committed)
    }

    fn read_committed(&self) -> hermesmq_core::Result<Option<Vec<u8>>> {
        self.get(MEM_KEY_COMMITTED)
    }

    fn put(&self, key: &str, value: &[u8]) -> hermesmq_core::Result<()> {
        self.meta.lock().unwrap().insert(key.to_string(), value.to_vec());
        Ok(())
    }

    fn get(&self, key: &str) -> hermesmq_core::Result<Option<Vec<u8>>> {
        Ok(self.meta.lock().unwrap().get(key).cloned())
    }

    fn delete(&self, key: &str) -> hermesmq_core::Result<()> {
        self.meta.lock().unwrap().remove(key);
        Ok(())
    }

    fn scan_prefix(&self, prefix: &str) -> hermesmq_core::Result<Vec<(String, Vec<u8>)>> {
        Ok(self
            .meta
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
}
