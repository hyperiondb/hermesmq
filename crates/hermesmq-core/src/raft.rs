use std::io::Cursor;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::{ContentType, GroupId, LeaseId, Offset, Priority, TopicId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AppRequest {
    Produce {
        topic: TopicId,
        priority: Priority,
        content_type: ContentType,
        payload: Bytes,
        producer_id: String,
        seq: u64,
        ts_ms: u64,
    },
    Poll {
        topic: TopicId,
        group: GroupId,
        max: u32,
        visibility_timeout_ms: u64,
        ts_ms: u64,
    },
    Ack {
        topic: TopicId,
        group: GroupId,
        lease_id: LeaseId,
    },
    Nack {
        topic: TopicId,
        group: GroupId,
        lease_id: LeaseId,
    },
    CommitOffset {
        topic: TopicId,
        group: GroupId,
        offset: Offset,
    },
    CreateTopic {
        topic: TopicId,
    },
    DeleteTopic {
        topic: TopicId,
    },
    SetRateLimit {
        topic: TopicId,
        rate_milli_per_sec: u64,
        burst: u32,
    },
    SetRetention {
        topic: TopicId,
        max_messages: u64,
        max_age_ms: u64,
    },
    AckMany {
        topic: TopicId,
        group: GroupId,
        lease_ids: Vec<LeaseId>,
    },
    ProduceMany {
        items: Vec<ProduceItem>,
    },
    NackMany {
        topic: TopicId,
        group: GroupId,
        lease_ids: Vec<LeaseId>,
    },
    PollAuto {
        topic: TopicId,
        group: GroupId,
        max: u32,
        visibility_timeout_ms: u64,
        ts_ms: u64,
    },
    // Appended, never reordered: these variants are postcard-encoded by index
    // into the Raft log, so an existing entry must keep decoding as itself.
    /// A commit that carries the leader's clock, so the consumer group it
    /// creates is subject to the same idle expiry as one created by a poll.
    CommitOffsetAt {
        topic: TopicId,
        group: GroupId,
        offset: Offset,
        ts_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProduceItem {
    pub topic: TopicId,
    pub priority: Priority,
    pub content_type: ContentType,
    pub payload: Bytes,
    pub producer_id: String,
    pub seq: u64,
    pub ts_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Delivered {
    pub lease_id: LeaseId,
    pub offset: Offset,
    pub priority: Priority,
    pub content_type: ContentType,
    pub payload: Bytes,
    pub ts_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AppResponse {
    Produced { offset: Offset },
    Polled { items: Vec<Delivered> },
    Acked,
    Nacked,
    Committed,
    TopicCreated,
    TopicDeleted,
    RateLimitSet,
    RetentionSet,
    NoOp,
    ProducedMany { offsets: Vec<Offset> },
    /// The request was refused by the state machine because it broke a limit.
    Rejected { reason: String },
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = AppRequest,
        R = AppResponse,
);
