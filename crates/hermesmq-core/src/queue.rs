use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};

use serde::{Deserialize, Serialize};

use crate::raft::{AppRequest, AppResponse, Delivered, ProduceItem};
use crate::types::{LeaseId, Message, Offset, Priority};

const DEDUP_CAPACITY: usize = 100_000;
const RESERVED_DEN: u64 = 4;
const DEFAULT_RETAIN_MAX_MESSAGES: u64 = 1_000_000;
const GROUP_GC_THRESHOLD: usize = 1024;
const GROUP_IDLE_EXPIRE_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// Bounds on what one request can add to the replicated state. They are
/// compiled-in rather than configured because every replica must reach the same
/// decision for the same request.
pub const MAX_NAME_BYTES: usize = 256;
pub const MAX_PRODUCER_ID_BYTES: usize = 256;
pub const MAX_TOPICS: usize = 10_000;
pub const MAX_GROUPS_PER_TOPIC: usize = 1_000;
pub const MAX_VISIBILITY_MS: u64 = 12 * 60 * 60 * 1000;

/// Ceiling for every counter a snapshot can set. Well below `u64::MAX`, so the
/// increments below cannot reach the top of the range from a validated state.
const MAX_COUNTER: u64 = 1 << 62;

fn check_consumer(topic: &str, group: &str) -> Result<(), String> {
    check_name("topic", topic)?;
    check_name("group", group)
}

fn check_name(kind: &str, name: &str) -> Result<(), String> {
    if name.len() > MAX_NAME_BYTES {
        return Err(format!(
            "{kind} name is {} bytes; max is {MAX_NAME_BYTES}",
            name.len()
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Lease {
    offset: Offset,
    deadline_ms: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct GroupState {
    ack_watermark: Offset,
    acked_above: BTreeSet<Offset>,
    in_flight: BTreeMap<LeaseId, Lease>,
    leased_offsets: BTreeMap<Offset, u64>,
    poll_count: u64,
    last_active_ms: u64,
}

impl GroupState {
    fn mark_acked(&mut self, offset: Offset) {
        if offset < self.ack_watermark {
            return;
        }
        self.acked_above.insert(offset);
        while self.acked_above.remove(&self.ack_watermark) {
            self.ack_watermark = self.ack_watermark.saturating_add(1);
        }
    }

    fn expire(&mut self, now_ms: u64) {
        let expired: Vec<LeaseId> = self
            .in_flight
            .iter()
            .filter(|(_, l)| l.deadline_ms <= now_ms)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            if let Some(l) = self.in_flight.remove(&id) {
                self.leased_offsets.remove(&l.offset);
            }
        }
    }

    fn advance_watermark(&mut self, to: Offset) {
        if to > self.ack_watermark {
            self.ack_watermark = to;
        }
        let watermark = self.ack_watermark;
        self.acked_above.retain(|o| *o >= watermark);
        let drop: Vec<LeaseId> = self
            .in_flight
            .iter()
            .filter(|(_, l)| l.offset < watermark)
            .map(|(id, _)| *id)
            .collect();
        for id in drop {
            if let Some(l) = self.in_flight.remove(&id) {
                self.leased_offsets.remove(&l.offset);
            }
        }
        while self.acked_above.remove(&self.ack_watermark) {
            self.ack_watermark = self.ack_watermark.saturating_add(1);
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct TopicState {
    next_offset: Offset,
    messages: BTreeMap<Offset, Message>,
    dedup: BTreeMap<(String, u64), Offset>,
    dedup_order: VecDeque<(String, u64)>,
    groups: BTreeMap<String, GroupState>,
    rate_milli_per_sec: u64,
    burst: u32,
    retain_max_messages: u64,
    retain_max_age_ms: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Queue {
    topics: BTreeMap<String, TopicState>,
    next_lease_id: LeaseId,
}

impl Queue {
    pub fn apply(&mut self, req: AppRequest) -> AppResponse {
        match req {
            AppRequest::CreateTopic { topic } => {
                if let Err(reason) = self.check_topic(&topic.0) {
                    return AppResponse::Rejected { reason };
                }
                self.topics.entry(topic.0).or_default();
                AppResponse::TopicCreated
            }
            AppRequest::DeleteTopic { topic } => {
                self.topics.remove(&topic.0);
                AppResponse::TopicDeleted
            }
            AppRequest::Produce {
                topic,
                priority,
                content_type,
                payload,
                producer_id,
                seq,
                ts_ms,
            } => {
                let item = ProduceItem {
                    topic,
                    priority,
                    content_type,
                    payload,
                    producer_id,
                    seq,
                    ts_ms,
                };
                if let Err(reason) = self.check_produce(std::slice::from_ref(&item)) {
                    return AppResponse::Rejected { reason };
                }
                let offset = self.produce_one(item);
                AppResponse::Produced { offset }
            }
            AppRequest::ProduceMany { items } => {
                if let Err(reason) = self.check_produce(&items) {
                    return AppResponse::Rejected { reason };
                }
                let offsets = items.into_iter().map(|item| self.produce_one(item)).collect();
                AppResponse::ProducedMany { offsets }
            }
            AppRequest::Poll {
                topic,
                group,
                max,
                visibility_timeout_ms,
                ts_ms,
            } => {
                if let Err(reason) = check_consumer(&topic.0, &group.0) {
                    return AppResponse::Rejected { reason };
                }
                let mut next_lease = self.next_lease_id;
                let items = poll(
                    &mut self.topics,
                    &topic.0,
                    &group.0,
                    max,
                    visibility_timeout_ms,
                    ts_ms,
                    &mut next_lease,
                );
                self.next_lease_id = next_lease;
                AppResponse::Polled { items }
            }
            AppRequest::PollAuto {
                topic,
                group,
                max,
                visibility_timeout_ms,
                ts_ms,
            } => {
                if let Err(reason) = check_consumer(&topic.0, &group.0) {
                    return AppResponse::Rejected { reason };
                }
                let mut next_lease = self.next_lease_id;
                let items = poll(
                    &mut self.topics,
                    &topic.0,
                    &group.0,
                    max,
                    visibility_timeout_ms,
                    ts_ms,
                    &mut next_lease,
                );
                self.next_lease_id = next_lease;
                for d in &items {
                    ack(&mut self.topics, &topic.0, &group.0, d.lease_id);
                }
                reclaim_topic(&mut self.topics, &topic.0);
                AppResponse::Polled { items }
            }
            AppRequest::Ack {
                topic,
                group,
                lease_id,
            } => {
                ack(&mut self.topics, &topic.0, &group.0, lease_id);
                reclaim_topic(&mut self.topics, &topic.0);
                AppResponse::Acked
            }
            AppRequest::AckMany {
                topic,
                group,
                lease_ids,
            } => {
                for lease_id in lease_ids {
                    ack(&mut self.topics, &topic.0, &group.0, lease_id);
                }
                reclaim_topic(&mut self.topics, &topic.0);
                AppResponse::Acked
            }
            AppRequest::NackMany {
                topic,
                group,
                lease_ids,
            } => {
                for lease_id in lease_ids {
                    nack(&mut self.topics, &topic.0, &group.0, lease_id);
                }
                AppResponse::Nacked
            }
            AppRequest::Nack {
                topic,
                group,
                lease_id,
            } => {
                nack(&mut self.topics, &topic.0, &group.0, lease_id);
                AppResponse::Nacked
            }
            AppRequest::CommitOffset {
                topic,
                group,
                offset,
            } => {
                if let Err(reason) = check_consumer(&topic.0, &group.0) {
                    return AppResponse::Rejected { reason };
                }
                commit(&mut self.topics, &topic.0, &group.0, offset, None);
                reclaim_topic(&mut self.topics, &topic.0);
                AppResponse::Committed
            }
            AppRequest::CommitOffsetAt {
                topic,
                group,
                offset,
                ts_ms,
            } => {
                if let Err(reason) = check_consumer(&topic.0, &group.0) {
                    return AppResponse::Rejected { reason };
                }
                commit(&mut self.topics, &topic.0, &group.0, offset, Some(ts_ms));
                reclaim_topic(&mut self.topics, &topic.0);
                AppResponse::Committed
            }
            AppRequest::SetRateLimit {
                topic,
                rate_milli_per_sec,
                burst,
            } => {
                if let Err(reason) = self.check_topic(&topic.0) {
                    return AppResponse::Rejected { reason };
                }
                let t = self.topics.entry(topic.0).or_default();
                t.rate_milli_per_sec = rate_milli_per_sec;
                t.burst = burst;
                AppResponse::RateLimitSet
            }
            AppRequest::SetRetention {
                topic,
                max_messages,
                max_age_ms,
            } => {
                if let Err(reason) = self.check_topic(&topic.0) {
                    return AppResponse::Rejected { reason };
                }
                let t = self.topics.entry(topic.0).or_default();
                t.retain_max_messages = max_messages;
                t.retain_max_age_ms = max_age_ms;
                AppResponse::RetentionSet
            }
        }
    }

    /// Name and capacity checks for a produce, run over the whole batch before
    /// any of it is applied so a rejected batch leaves no partial state.
    fn check_produce(&self, items: &[ProduceItem]) -> Result<(), String> {
        let mut fresh: BTreeSet<&str> = BTreeSet::new();
        for item in items {
            check_name("topic", &item.topic.0)?;
            if item.producer_id.len() > MAX_PRODUCER_ID_BYTES {
                return Err(format!(
                    "producer id is {} bytes; max is {MAX_PRODUCER_ID_BYTES}",
                    item.producer_id.len()
                ));
            }
            if !self.topics.contains_key(&item.topic.0) {
                fresh.insert(item.topic.0.as_str());
            }
        }
        if self.topics.len() + fresh.len() > MAX_TOPICS {
            return Err(format!("topic limit of {MAX_TOPICS} reached"));
        }
        Ok(())
    }

    /// Name and capacity checks for a request that may create one topic.
    fn check_topic(&self, topic: &str) -> Result<(), String> {
        check_name("topic", topic)?;
        if !self.topics.contains_key(topic) && self.topics.len() >= MAX_TOPICS {
            return Err(format!("topic limit of {MAX_TOPICS} reached"));
        }
        Ok(())
    }

    fn produce_one(&mut self, item: ProduceItem) -> Offset {
        let t = self.topics.entry(item.topic.0).or_default();
        let dedup = !item.producer_id.is_empty();
        if dedup {
            if let Some(offset) = t.dedup.get(&(item.producer_id.clone(), item.seq)).copied() {
                // Retention runs on the duplicate too, so a producer that only
                // re-sends cannot hold a topic above its cap.
                purge_retained(t, item.ts_ms);
                return offset;
            }
        }
        let offset = t.next_offset;
        t.next_offset = t.next_offset.saturating_add(1);
        t.messages.insert(
            offset,
            Message {
                offset,
                priority: item.priority,
                content_type: item.content_type,
                payload: item.payload,
                ts_ms: item.ts_ms,
            },
        );
        if dedup {
            let key = (item.producer_id, item.seq);
            t.dedup.insert(key.clone(), offset);
            t.dedup_order.push_back(key);
            while t.dedup_order.len() > DEDUP_CAPACITY {
                if let Some(old) = t.dedup_order.pop_front() {
                    t.dedup.remove(&old);
                }
            }
        }
        purge_retained(t, item.ts_ms);
        offset
    }

    /// Whether a request naming this topic can proceed: either the topic is
    /// already there, or there is room for one more.
    pub fn topic_admits(&self, topic: &str) -> bool {
        self.topics.contains_key(topic) || self.topics.len() < MAX_TOPICS
    }

    pub fn rate_config(&self, topic: &str) -> Option<(u64, u32)> {
        self.topics.get(topic).and_then(|t| {
            if t.rate_milli_per_sec > 0 {
                Some((t.rate_milli_per_sec, t.burst))
            } else {
                None
            }
        })
    }

    pub fn has_deliverable(&self, topic: &str, group: &str, now_ms: u64) -> bool {
        let Some(t) = self.topics.get(topic) else {
            return false;
        };
        match t.groups.get(group) {
            None => !t.messages.is_empty(),
            Some(g) => t.messages.range(g.ack_watermark..).any(|(offset, _)| {
                !g.acked_above.contains(offset)
                    && g.leased_offsets
                        .get(offset)
                        .is_none_or(|deadline| *deadline <= now_ms)
            }),
        }
    }

    pub fn metrics(&self) -> QueueMetrics {
        let mut metrics = QueueMetrics {
            topics: self.topics.len() as u64,
            messages: 0,
            in_flight: 0,
        };
        for t in self.topics.values() {
            metrics.messages += t.messages.len() as u64;
            for g in t.groups.values() {
                metrics.in_flight += g.in_flight.len() as u64;
            }
        }
        metrics
    }

    /// Bring a decoded state back inside the range ordinary operation now keeps
    /// it in, for the one shape earlier releases could produce themselves.
    ///
    /// Before commits were clamped, a client could commit any offset and park a
    /// group's watermark past the end of its topic. Lowering it back to
    /// `next_offset` is where those groups would have been left had the commit
    /// been clamped, and it costs nothing: there are no messages up there.
    /// Everything else [`Queue::validate`] rejects is damage, not history.
    pub fn normalize(&mut self) {
        for t in self.topics.values_mut() {
            for g in t.groups.values_mut() {
                if g.ack_watermark > t.next_offset {
                    g.ack_watermark = t.next_offset;
                }
            }
        }
    }

    /// Whether a decoded state is one the queue could have produced itself.
    ///
    /// Snapshot bytes reach the decoder from the peer port and from the record
    /// on disk, and the decoded state replaces the whole queue, so a state that
    /// breaks one of these is what makes counters wrap, dedup entries become
    /// un-evictable, or messages become invisible.
    pub fn validate(&self) -> Result<(), String> {
        if self.next_lease_id > MAX_COUNTER {
            return Err(format!("next_lease_id is {}", self.next_lease_id));
        }
        for (name, t) in &self.topics {
            if t.next_offset > MAX_COUNTER {
                return Err(format!("topic {name}: next_offset is {}", t.next_offset));
            }
            for (offset, message) in &t.messages {
                if message.offset != *offset {
                    return Err(format!(
                        "topic {name}: message stored at {offset} carries offset {}",
                        message.offset
                    ));
                }
            }
            if let Some(highest) = t.messages.keys().next_back() {
                if *highest >= t.next_offset {
                    return Err(format!(
                        "topic {name}: message offset {highest} is at or above next_offset {}",
                        t.next_offset
                    ));
                }
            }
            if t.dedup.len() != t.dedup_order.len() {
                return Err(format!(
                    "topic {name}: dedup has {} entries but dedup_order has {}",
                    t.dedup.len(),
                    t.dedup_order.len()
                ));
            }
            if t.dedup.len() > DEDUP_CAPACITY {
                return Err(format!("topic {name}: dedup holds {} entries", t.dedup.len()));
            }
            let distinct: BTreeSet<&(String, u64)> = t.dedup_order.iter().collect();
            if distinct.len() != t.dedup_order.len() {
                return Err(format!("topic {name}: dedup_order holds a duplicate key"));
            }
            for key in &t.dedup_order {
                if !t.dedup.contains_key(key) {
                    return Err(format!("topic {name}: dedup_order key is missing from dedup"));
                }
            }
            for (group, g) in &t.groups {
                if g.poll_count > MAX_COUNTER {
                    return Err(format!("topic {name} group {group}: poll_count is {}", g.poll_count));
                }
                if g.ack_watermark > t.next_offset {
                    return Err(format!(
                        "topic {name} group {group}: watermark {} is above next_offset {}",
                        g.ack_watermark, t.next_offset
                    ));
                }
                if let Some(highest) = g.acked_above.iter().next_back() {
                    if *highest >= t.next_offset {
                        return Err(format!(
                            "topic {name} group {group}: acked_above holds {highest} at or above next_offset {}",
                            t.next_offset
                        ));
                    }
                }
                for (lease_id, lease) in &g.in_flight {
                    if *lease_id >= self.next_lease_id {
                        return Err(format!(
                            "topic {name} group {group}: lease id {lease_id} is at or above next_lease_id {}",
                            self.next_lease_id
                        ));
                    }
                    if lease.offset >= t.next_offset {
                        return Err(format!(
                            "topic {name} group {group}: lease holds offset {} at or above next_offset {}",
                            lease.offset, t.next_offset
                        ));
                    }
                }
                let by_lease: BTreeMap<Offset, u64> =
                    g.in_flight.values().map(|l| (l.offset, l.deadline_ms)).collect();
                if by_lease.len() != g.in_flight.len() {
                    return Err(format!("topic {name} group {group}: two leases share one offset"));
                }
                if by_lease != g.leased_offsets {
                    return Err(format!(
                        "topic {name} group {group}: in_flight and leased_offsets disagree"
                    ));
                }
                if g.acked_above.contains(&g.ack_watermark) {
                    return Err(format!(
                        "topic {name} group {group}: acked_above still holds the watermark {}",
                        g.ack_watermark
                    ));
                }
                if let Some(lowest) = g.acked_above.iter().next() {
                    if *lowest < g.ack_watermark {
                        return Err(format!(
                            "topic {name} group {group}: acked_above holds {lowest} below watermark {}",
                            g.ack_watermark
                        ));
                    }
                }
                if let Some(lowest) = g.leased_offsets.keys().next() {
                    if *lowest < g.ack_watermark {
                        return Err(format!(
                            "topic {name} group {group}: lease at {lowest} is below watermark {}",
                            g.ack_watermark
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Internal consistency check, exposed for the fuzz targets and unit tests.
    #[doc(hidden)]
    pub fn debug_check_invariants(&self) -> Result<(), String> {
        self.validate()
    }

    /// Retained message count for one topic, or `None` if the topic is unknown.
    #[doc(hidden)]
    pub fn debug_topic_len(&self, topic: &str) -> Option<usize> {
        self.topics.get(topic).map(|t| t.messages.len())
    }

    /// Counts that bound how much memory the replicated state holds.
    #[doc(hidden)]
    pub fn debug_footprint(&self) -> QueueFootprint {
        let mut f = QueueFootprint {
            topics: self.topics.len(),
            ..QueueFootprint::default()
        };
        for (name, t) in &self.topics {
            f.name_bytes += name.len();
            f.messages += t.messages.len();
            f.payload_bytes += t.messages.values().map(|m| m.payload.len()).sum::<usize>();
            f.groups += t.groups.len();
            f.name_bytes += t.groups.keys().map(|g| g.len()).sum::<usize>();
            f.dedup_entries += t.dedup.len();
            f.dedup_bytes += t.dedup.keys().map(|(p, _)| p.len()).sum::<usize>();
            f.dedup_bytes += t.dedup_order.iter().map(|(p, _)| p.len()).sum::<usize>();
        }
        f
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[doc(hidden)]
pub struct QueueFootprint {
    pub topics: usize,
    pub groups: usize,
    pub messages: usize,
    pub payload_bytes: usize,
    pub dedup_entries: usize,
    pub dedup_bytes: usize,
    pub name_bytes: usize,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct QueueMetrics {
    pub topics: u64,
    pub messages: u64,
    pub in_flight: u64,
}

fn poll(
    topics: &mut BTreeMap<String, TopicState>,
    topic: &str,
    group: &str,
    max: u32,
    visibility_timeout_ms: u64,
    ts_ms: u64,
    next_lease: &mut LeaseId,
) -> Vec<Delivered> {
    let Some(t) = topics.get_mut(topic) else {
        return Vec::new();
    };
    let visibility_timeout_ms = visibility_timeout_ms.min(MAX_VISIBILITY_MS);
    if sweep_idle_groups(t, ts_ms) {
        reclaim(t);
    }
    if !t.groups.contains_key(group) && t.groups.len() >= MAX_GROUPS_PER_TOPIC {
        return Vec::new();
    }
    let first = t.messages.keys().next().copied().unwrap_or(t.next_offset);
    let g = t.groups.entry(group.to_string()).or_insert_with(|| GroupState {
        ack_watermark: first,
        last_active_ms: ts_ms,
        ..GroupState::default()
    });
    g.expire(ts_ms);
    g.last_active_ms = g.last_active_ms.max(ts_ms);

    let max = max as usize;
    let poll_count = g.poll_count;
    g.poll_count = g.poll_count.saturating_add(1);

    if max == 0 {
        return Vec::new();
    }

    let base = max / RESERVED_DEN as usize;
    let bonus = usize::from((poll_count + 1) % RESERVED_DEN == 0);
    let reserved = (base + bonus).min(max);
    let priority_slots = max - reserved;

    let mut earliest: Vec<(Priority, Offset)> = Vec::new();
    let mut top: BinaryHeap<Reverse<(Priority, Reverse<Offset>)>> = BinaryHeap::new();
    for (offset, message) in t.messages.range(g.ack_watermark..) {
        if g.acked_above.contains(offset) || g.leased_offsets.contains_key(offset) {
            continue;
        }
        if earliest.len() < max {
            earliest.push((message.priority, *offset));
        } else if priority_slots == 0 {
            break;
        }
        if priority_slots > 0 {
            top.push(Reverse((message.priority, Reverse(*offset))));
            if top.len() > priority_slots {
                top.pop();
            }
        }
    }

    let mut chosen_set: BTreeSet<Offset> = BTreeSet::new();
    let mut chosen: Vec<(Priority, Offset)> = Vec::new();
    for Reverse((priority, Reverse(offset))) in top {
        if chosen_set.insert(offset) {
            chosen.push((priority, offset));
        }
    }
    for item in earliest {
        if chosen.len() >= max {
            break;
        }
        if chosen_set.insert(item.1) {
            chosen.push(item);
        }
    }
    chosen.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));

    let mut items = Vec::new();
    for (_, offset) in chosen {
        let lease_id = *next_lease;
        *next_lease = next_lease.saturating_add(1);
        let deadline_ms = ts_ms.saturating_add(visibility_timeout_ms);
        g.in_flight.insert(lease_id, Lease { offset, deadline_ms });
        g.leased_offsets.insert(offset, deadline_ms);
        let message = &t.messages[&offset];
        items.push(Delivered {
            lease_id,
            offset,
            priority: message.priority,
            content_type: message.content_type,
            payload: message.payload.clone(),
            ts_ms: message.ts_ms,
        });
    }
    items
}

fn ack(topics: &mut BTreeMap<String, TopicState>, topic: &str, group: &str, lease_id: LeaseId) {
    if let Some(t) = topics.get_mut(topic) {
        if let Some(g) = t.groups.get_mut(group) {
            if let Some(lease) = g.in_flight.remove(&lease_id) {
                g.leased_offsets.remove(&lease.offset);
                g.mark_acked(lease.offset);
            }
        }
    }
}

fn nack(topics: &mut BTreeMap<String, TopicState>, topic: &str, group: &str, lease_id: LeaseId) {
    if let Some(t) = topics.get_mut(topic) {
        if let Some(g) = t.groups.get_mut(group) {
            if let Some(lease) = g.in_flight.remove(&lease_id) {
                g.leased_offsets.remove(&lease.offset);
            }
        }
    }
}

fn effective_retention(t: &TopicState) -> (u64, u64) {
    if t.retain_max_messages == 0 && t.retain_max_age_ms == 0 {
        (DEFAULT_RETAIN_MAX_MESSAGES, 0)
    } else {
        (t.retain_max_messages, t.retain_max_age_ms)
    }
}

fn purge_retained(t: &mut TopicState, now_ms: u64) {
    let (max_messages, max_age_ms) = effective_retention(t);
    let mut purge: BTreeSet<Offset> = BTreeSet::new();
    if max_age_ms > 0 {
        for (offset, message) in t.messages.iter() {
            if now_ms.saturating_sub(message.ts_ms) > max_age_ms {
                purge.insert(*offset);
            } else {
                break;
            }
        }
    }
    if max_messages > 0 {
        let target = max_messages as usize;
        let kept = t.messages.len() - purge.len();
        if kept > target {
            let mut need = kept - target;
            for offset in t.messages.keys() {
                if need == 0 {
                    break;
                }
                if purge.insert(*offset) {
                    need -= 1;
                }
            }
        }
    }
    if purge.is_empty() {
        return;
    }
    for offset in &purge {
        t.messages.remove(offset);
    }
    let boundary = t.messages.keys().next().copied().unwrap_or(t.next_offset);
    for g in t.groups.values_mut() {
        if boundary > g.ack_watermark {
            g.advance_watermark(boundary);
        }
    }
    prune_dedup(t, boundary);
}

/// Drop dedup keys for messages retention has already removed.
///
/// Without this the dedup table is the one part of a topic retention does not
/// bound: it keeps a `(producer_id, seq)` pair per produce whether or not the
/// message it points at still exists. `dedup_order` is in insertion order,
/// which is offset order, so this stops at the first key still in range.
fn prune_dedup(t: &mut TopicState, boundary: Offset) {
    while t
        .dedup_order
        .front()
        .is_some_and(|key| t.dedup.get(key).is_none_or(|offset| *offset < boundary))
    {
        if let Some(key) = t.dedup_order.pop_front() {
            t.dedup.remove(&key);
        }
    }
}

fn reclaim_topic(topics: &mut BTreeMap<String, TopicState>, topic: &str) {
    if let Some(t) = topics.get_mut(topic) {
        reclaim(t);
    }
}

fn reclaim(t: &mut TopicState) {
    if let Some(boundary) = t.groups.values().map(|g| g.ack_watermark).min() {
        if t.messages.keys().next().is_some_and(|first| boundary > *first) {
            t.messages = t.messages.split_off(&boundary);
        }
    }
    gc_idle_groups(t);
}

/// Drop consumer groups that have been idle past the expiry. Runs on poll and on
/// commit - the two requests that create a group - so a topic reclaims whichever
/// of them its clients use.
fn sweep_idle_groups(t: &mut TopicState, now_ms: u64) -> bool {
    // A group restored from a pre-0.8 snapshot, or created by a commit that
    // carried no clock, has no activity stamp. Stamp it now rather than exempt
    // it: an unstamped group used to be immortal, which is how a run of commits
    // with fresh group names could grow the state and never give it back.
    for g in t.groups.values_mut() {
        if g.last_active_ms == 0 {
            g.last_active_ms = now_ms;
        }
    }
    let before = t.groups.len();
    t.groups.retain(|_, g| {
        now_ms.saturating_sub(g.last_active_ms) <= GROUP_IDLE_EXPIRE_MS
            || g.in_flight.values().any(|l| l.deadline_ms > now_ms)
    });
    t.groups.len() != before
}

fn gc_idle_groups(t: &mut TopicState) {
    if t.groups.len() <= GROUP_GC_THRESHOLD || !t.messages.is_empty() {
        return;
    }
    let next = t.next_offset;
    t.groups.retain(|_, g| {
        !(g.ack_watermark >= next
            && g.in_flight.is_empty()
            && g.leased_offsets.is_empty()
            && g.acked_above.is_empty())
    });
}

/// The group's state, creating it if the topic has room for another one.
///
/// `None` means the topic is already holding [`MAX_GROUPS_PER_TOPIC`] groups
/// and this name is not one of them.
fn group_entry<'a>(
    t: &'a mut TopicState,
    group: &str,
    now_ms: Option<u64>,
) -> Option<&'a mut GroupState> {
    if !t.groups.contains_key(group) && t.groups.len() >= MAX_GROUPS_PER_TOPIC {
        return None;
    }
    let first = t.messages.keys().next().copied().unwrap_or(t.next_offset);
    Some(t.groups.entry(group.to_string()).or_insert_with(|| GroupState {
        ack_watermark: first,
        last_active_ms: now_ms.unwrap_or(0),
        ..GroupState::default()
    }))
}

fn commit(
    topics: &mut BTreeMap<String, TopicState>,
    topic: &str,
    group: &str,
    offset: Offset,
    now_ms: Option<u64>,
) {
    if let Some(t) = topics.get_mut(topic) {
        if let Some(now_ms) = now_ms {
            if sweep_idle_groups(t, now_ms) {
                reclaim(t);
            }
        }
        // A watermark past the end of the topic would hide every message
        // produced afterwards from this group, and leaves the counter free to
        // run to the top of its range.
        let offset = offset.min(t.next_offset);
        let Some(g) = group_entry(t, group, now_ms) else {
            return;
        };
        g.advance_watermark(offset);
        if let Some(now_ms) = now_ms {
            g.last_active_ms = g.last_active_ms.max(now_ms);
        }
    }
}

pub(crate) mod compat {
    use super::*;

    #[derive(Deserialize)]
    pub(crate) struct QueueV0 {
        topics: BTreeMap<String, TopicStateV0>,
        next_lease_id: LeaseId,
    }

    #[derive(Deserialize)]
    struct TopicStateV0 {
        next_offset: Offset,
        messages: BTreeMap<Offset, Message>,
        dedup: BTreeMap<(String, u64), Offset>,
        dedup_order: VecDeque<(String, u64)>,
        groups: BTreeMap<String, GroupStateV0>,
        rate_milli_per_sec: u64,
        burst: u32,
        retain_max_messages: u64,
        retain_max_age_ms: u64,
    }

    #[derive(Deserialize)]
    struct GroupStateV0 {
        ack_watermark: Offset,
        acked_above: BTreeSet<Offset>,
        in_flight: BTreeMap<LeaseId, Lease>,
        leased_offsets: BTreeMap<Offset, u64>,
        poll_count: u64,
    }

    impl From<GroupStateV0> for GroupState {
        fn from(g: GroupStateV0) -> Self {
            GroupState {
                ack_watermark: g.ack_watermark,
                acked_above: g.acked_above,
                in_flight: g.in_flight,
                leased_offsets: g.leased_offsets,
                poll_count: g.poll_count,
                last_active_ms: 0,
            }
        }
    }

    impl From<TopicStateV0> for TopicState {
        fn from(t: TopicStateV0) -> Self {
            TopicState {
                next_offset: t.next_offset,
                messages: t.messages,
                dedup: t.dedup,
                dedup_order: t.dedup_order,
                groups: t.groups.into_iter().map(|(k, g)| (k, g.into())).collect(),
                rate_milli_per_sec: t.rate_milli_per_sec,
                burst: t.burst,
                retain_max_messages: t.retain_max_messages,
                retain_max_age_ms: t.retain_max_age_ms,
            }
        }
    }

    impl From<QueueV0> for Queue {
        fn from(q: QueueV0) -> Self {
            Queue {
                topics: q.topics.into_iter().map(|(k, t)| (k, t.into())).collect(),
                next_lease_id: q.next_lease_id,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ContentType, GroupId, Priority, TopicId};
    use bytes::Bytes;
    use proptest::prelude::*;

    fn produce(q: &mut Queue, topic: &str, priority: u8, body: &[u8], producer: &str, seq: u64) -> Offset {
        match q.apply(AppRequest::Produce {
            topic: TopicId::from(topic),
            priority: Priority(priority),
            content_type: ContentType::Raw,
            payload: Bytes::copy_from_slice(body),
            producer_id: producer.to_string(),
            seq,
            ts_ms: 0,
        }) {
            AppResponse::Produced { offset } => offset,
            other => panic!("expected Produced, got {other:?}"),
        }
    }

    fn poll_offsets(q: &mut Queue, topic: &str, group: &str, max: u32, vis: u64, ts: u64) -> Vec<(LeaseId, Offset)> {
        match q.apply(AppRequest::Poll {
            topic: TopicId::from(topic),
            group: GroupId::from(group),
            max,
            visibility_timeout_ms: vis,
            ts_ms: ts,
        }) {
            AppResponse::Polled { items } => items.into_iter().map(|d| (d.lease_id, d.offset)).collect(),
            other => panic!("expected Polled, got {other:?}"),
        }
    }

    #[test]
    fn a_commit_past_the_end_cannot_hide_later_messages() {
        let mut q = Queue::default();
        produce(&mut q, "t", 0, b"a", "", 0);
        q.apply(AppRequest::CommitOffsetAt {
            topic: TopicId::from("t"),
            group: GroupId::from("g"),
            offset: u64::MAX,
            ts_ms: 1,
        });
        produce(&mut q, "t", 0, b"b", "", 0);
        let got: Vec<Offset> = poll_offsets(&mut q, "t", "g", 10, 1000, 2)
            .into_iter()
            .map(|(_, o)| o)
            .collect();
        assert_eq!(got, vec![1], "the message produced after the commit must still be served");
        assert_eq!(q.validate(), Ok(()));
    }

    #[test]
    fn over_long_names_are_rejected() {
        let mut q = Queue::default();
        let long = "x".repeat(MAX_NAME_BYTES + 1);
        assert!(matches!(
            q.apply(AppRequest::CreateTopic {
                topic: TopicId(long.clone())
            }),
            AppResponse::Rejected { .. }
        ));
        assert!(matches!(
            q.apply(AppRequest::Poll {
                topic: TopicId::from("t"),
                group: GroupId(long),
                max: 1,
                visibility_timeout_ms: 1000,
                ts_ms: 0,
            }),
            AppResponse::Rejected { .. }
        ));
        assert_eq!(q.metrics().topics, 0);
    }

    #[test]
    fn an_over_long_producer_id_is_rejected() {
        let mut q = Queue::default();
        let long = "p".repeat(MAX_PRODUCER_ID_BYTES + 1);
        assert!(matches!(
            q.apply(AppRequest::Produce {
                topic: TopicId::from("t"),
                priority: Priority(0),
                content_type: ContentType::Raw,
                payload: Bytes::from_static(b"x"),
                producer_id: long,
                seq: 0,
                ts_ms: 0,
            }),
            AppResponse::Rejected { .. }
        ));
        assert_eq!(q.metrics().messages, 0);
    }

    #[test]
    fn the_topic_limit_is_enforced() {
        let mut q = Queue::default();
        for i in 0..MAX_TOPICS {
            produce(&mut q, &format!("t{i}"), 0, b"a", "", 0);
        }
        assert_eq!(q.metrics().topics as usize, MAX_TOPICS);
        assert!(matches!(
            q.apply(AppRequest::CreateTopic {
                topic: TopicId::from("one-more")
            }),
            AppResponse::Rejected { .. }
        ));
        assert_eq!(q.metrics().topics as usize, MAX_TOPICS);
    }

    #[test]
    fn the_group_limit_is_enforced() {
        let mut q = Queue::default();
        produce(&mut q, "t", 0, b"a", "", 0);
        for i in 0..MAX_GROUPS_PER_TOPIC {
            q.apply(AppRequest::CommitOffsetAt {
                topic: TopicId::from("t"),
                group: GroupId(format!("g{i}")),
                offset: 0,
                ts_ms: 1,
            });
        }
        assert_eq!(q.debug_footprint().groups, MAX_GROUPS_PER_TOPIC);
        assert!(
            poll_offsets(&mut q, "t", "one-more", 10, 1000, 1).is_empty(),
            "a group past the cap gets no state and no delivery"
        );
        assert_eq!(q.debug_footprint().groups, MAX_GROUPS_PER_TOPIC);
    }

    #[test]
    fn a_commit_created_group_expires_when_idle() {
        let mut q = Queue::default();
        produce(&mut q, "t", 0, b"a", "", 0);
        q.apply(AppRequest::CommitOffsetAt {
            topic: TopicId::from("t"),
            group: GroupId::from("ghost"),
            offset: 0,
            ts_ms: 1,
        });
        assert_eq!(q.debug_footprint().groups, 1);
        q.apply(AppRequest::CommitOffsetAt {
            topic: TopicId::from("t"),
            group: GroupId::from("live"),
            offset: 0,
            ts_ms: 1 + GROUP_IDLE_EXPIRE_MS + 1,
        });
        assert_eq!(
            q.debug_footprint().groups,
            1,
            "the idle group is swept by the later commit"
        );
    }

    #[test]
    fn retention_prunes_the_dedup_table() {
        let mut q = Queue::default();
        q.apply(AppRequest::SetRetention {
            topic: TopicId::from("t"),
            max_messages: 2,
            max_age_ms: 0,
        });
        for seq in 0..50 {
            produce(&mut q, "t", 0, b"a", "p", seq);
        }
        let f = q.debug_footprint();
        assert_eq!(f.messages, 2);
        assert_eq!(f.dedup_entries, 2, "dedup keys outlived the messages they point at");
        assert_eq!(q.validate(), Ok(()));
    }

    #[test]
    fn a_deduplicated_produce_still_enforces_retention() {
        let mut q = Queue::default();
        for seq in 0..3 {
            produce(&mut q, "t", 0, b"a", "p", seq);
        }
        q.apply(AppRequest::SetRetention {
            topic: TopicId::from("t"),
            max_messages: 1,
            max_age_ms: 0,
        });
        produce(&mut q, "t", 0, b"a", "p", 0);
        assert_eq!(q.debug_topic_len("t"), Some(1));
    }

    #[test]
    fn the_visibility_timeout_is_clamped() {
        let mut q = Queue::default();
        produce(&mut q, "t", 0, b"a", "", 0);
        assert_eq!(poll_offsets(&mut q, "t", "g", 1, u64::MAX, 0).len(), 1);
        assert_eq!(
            poll_offsets(&mut q, "t", "g", 1, 1000, MAX_VISIBILITY_MS + 1).len(),
            1,
            "a lease taken with an unbounded timeout must still lapse at the cap"
        );
    }

    #[test]
    fn produce_assigns_monotonic_offsets() {
        let mut q = Queue::default();
        assert_eq!(produce(&mut q, "t", 0, b"a", "p", 1), 0);
        assert_eq!(produce(&mut q, "t", 0, b"b", "p", 2), 1);
        assert_eq!(produce(&mut q, "t", 0, b"c", "p", 3), 2);
    }

    #[test]
    fn produce_dedup_returns_same_offset() {
        let mut q = Queue::default();
        let first = produce(&mut q, "t", 0, b"a", "p", 1);
        let dup = produce(&mut q, "t", 0, b"a-again", "p", 1);
        assert_eq!(first, dup);
        assert_eq!(produce(&mut q, "t", 0, b"b", "p", 2), 1);
    }

    #[test]
    fn empty_producer_id_disables_dedup() {
        let mut q = Queue::default();
        assert_eq!(produce(&mut q, "t", 0, b"a", "", 0), 0);
        assert_eq!(produce(&mut q, "t", 0, b"b", "", 0), 1);
        assert_eq!(produce(&mut q, "t", 0, b"c", "", 0), 2);
    }

    #[test]
    fn poll_orders_by_priority_then_offset() {
        let mut q = Queue::default();
        produce(&mut q, "t", 0, b"a", "p", 1);
        produce(&mut q, "t", 5, b"b", "p", 2);
        produce(&mut q, "t", 3, b"c", "p", 3);
        let got: Vec<Offset> = poll_offsets(&mut q, "t", "g", 10, 1000, 0)
            .into_iter()
            .map(|(_, o)| o)
            .collect();
        assert_eq!(got, vec![1, 2, 0]);
    }

    #[test]
    fn leased_messages_are_not_redelivered_until_expiry() {
        let mut q = Queue::default();
        produce(&mut q, "t", 0, b"a", "p", 1);
        let first = poll_offsets(&mut q, "t", "g", 10, 1000, 0);
        assert_eq!(first.len(), 1);
        let again = poll_offsets(&mut q, "t", "g", 10, 1000, 500);
        assert!(again.is_empty());
        let after_expiry = poll_offsets(&mut q, "t", "g", 10, 1000, 2000);
        assert_eq!(after_expiry.len(), 1);
    }

    #[test]
    fn ack_makes_message_done() {
        let mut q = Queue::default();
        produce(&mut q, "t", 0, b"a", "p", 1);
        let leased = poll_offsets(&mut q, "t", "g", 10, 1000, 0);
        let (lease_id, _) = leased[0];
        q.apply(AppRequest::Ack {
            topic: TopicId::from("t"),
            group: GroupId::from("g"),
            lease_id,
        });
        let after = poll_offsets(&mut q, "t", "g", 10, 1000, 5000);
        assert!(after.is_empty());
    }

    #[test]
    fn nack_makes_message_immediately_redeliverable() {
        let mut q = Queue::default();
        produce(&mut q, "t", 0, b"a", "p", 1);
        let leased = poll_offsets(&mut q, "t", "g", 10, 1000, 0);
        let (lease_id, _) = leased[0];
        q.apply(AppRequest::Nack {
            topic: TopicId::from("t"),
            group: GroupId::from("g"),
            lease_id,
        });
        let after = poll_offsets(&mut q, "t", "g", 10, 1000, 1);
        assert_eq!(after.len(), 1);
    }

    #[test]
    fn anti_starvation_serves_oldest_within_reserved_cadence() {
        let mut q = Queue::default();
        produce(&mut q, "t", 0, b"low", "", 0);
        for _ in 0..5 {
            produce(&mut q, "t", 7, b"high", "", 0);
        }
        let mut delivered = Vec::new();
        for k in 0..4u64 {
            let items = poll_offsets(&mut q, "t", "g", 1, 1_000_000, k);
            if let Some((_, offset)) = items.first() {
                delivered.push(*offset);
            }
        }
        assert!(
            delivered.contains(&0),
            "low-priority oldest message must be served within the reserved cadence, got {delivered:?}"
        );
    }

    #[test]
    fn retention_by_count_keeps_newest() {
        let mut q = Queue::default();
        q.apply(AppRequest::SetRetention {
            topic: TopicId::from("t"),
            max_messages: 3,
            max_age_ms: 0,
        });
        for i in 0..5 {
            produce(&mut q, "t", 0, &[i], "", 0);
        }
        let offsets: Vec<Offset> = poll_offsets(&mut q, "t", "g", 10, 1000, 0)
            .into_iter()
            .map(|(_, o)| o)
            .collect();
        assert_eq!(offsets, vec![2, 3, 4]);
    }

    #[test]
    fn retention_by_age_drops_old() {
        let mut q = Queue::default();
        q.apply(AppRequest::SetRetention {
            topic: TopicId::from("t"),
            max_messages: 0,
            max_age_ms: 1000,
        });
        q.apply(AppRequest::Produce {
            topic: TopicId::from("t"),
            priority: Priority(0),
            content_type: ContentType::Raw,
            payload: Bytes::from_static(b"old"),
            producer_id: String::new(),
            seq: 0,
            ts_ms: 0,
        });
        q.apply(AppRequest::Produce {
            topic: TopicId::from("t"),
            priority: Priority(0),
            content_type: ContentType::Raw,
            payload: Bytes::from_static(b"new"),
            producer_id: String::new(),
            seq: 0,
            ts_ms: 5000,
        });
        let offsets: Vec<Offset> = poll_offsets(&mut q, "t", "g", 10, 1000, 5000)
            .into_iter()
            .map(|(_, o)| o)
            .collect();
        assert_eq!(offsets, vec![1]);
    }

    #[test]
    fn retention_purge_advances_watermark_and_releases_ack_tracking() {
        let mut q = Queue::default();
        q.apply(AppRequest::SetRetention {
            topic: TopicId::from("t"),
            max_messages: 3,
            max_age_ms: 0,
        });
        produce(&mut q, "t", 0, b"seed", "", 0);
        poll_offsets(&mut q, "t", "g", 1, 1, 0);
        for i in 0..5 {
            produce(&mut q, "t", 0, &[i], "", 0);
        }
        let leased = poll_offsets(&mut q, "t", "g", 10, 1000, 10);
        let offsets: Vec<Offset> = leased.iter().map(|(_, o)| *o).collect();
        assert_eq!(offsets, vec![3, 4, 5]);
        for (lease_id, _) in leased {
            q.apply(AppRequest::Ack {
                topic: TopicId::from("t"),
                group: GroupId::from("g"),
                lease_id,
            });
        }
        let g = &q.topics["t"].groups["g"];
        assert_eq!(g.ack_watermark, 6, "watermark must advance past purged offsets");
        assert!(g.acked_above.is_empty(), "acked_above must drain once the watermark advances");
        assert!(g.in_flight.is_empty());
        assert!(g.leased_offsets.is_empty());
    }

    #[test]
    fn fully_acked_messages_are_reclaimed_from_memory() {
        let mut q = Queue::default();
        for i in 0..10 {
            produce(&mut q, "t", 0, &[i], "", 0);
        }
        assert_eq!(q.metrics().messages, 10);
        let leased = poll_offsets(&mut q, "t", "g", 100, 1000, 0);
        for (lease_id, _) in leased {
            q.apply(AppRequest::Ack {
                topic: TopicId::from("t"),
                group: GroupId::from("g"),
                lease_id,
            });
        }
        assert_eq!(q.metrics().messages, 0, "fully acked messages must be freed from RAM");
    }

    #[test]
    fn reclaim_holds_the_line_for_the_slowest_group() {
        let mut q = Queue::default();
        for i in 0..10 {
            produce(&mut q, "t", 0, &[i], "", 0);
        }
        let _g2_registers = poll_offsets(&mut q, "t", "g2", 1, 1_000_000, 0);
        let leased = poll_offsets(&mut q, "t", "g1", 100, 1000, 0);
        for (lease_id, _) in leased {
            q.apply(AppRequest::Ack {
                topic: TopicId::from("t"),
                group: GroupId::from("g1"),
                lease_id,
            });
        }
        assert_eq!(
            q.metrics().messages,
            10,
            "messages must be retained while g2 still lags"
        );
        q.apply(AppRequest::CommitOffset {
            topic: TopicId::from("t"),
            group: GroupId::from("g2"),
            offset: 10,
        });
        assert_eq!(
            q.metrics().messages,
            0,
            "memory is freed once every group has consumed past the messages"
        );
    }

    #[test]
    fn unconsumed_topic_is_never_reclaimed() {
        let mut q = Queue::default();
        for i in 0..5 {
            produce(&mut q, "t", 0, &[i], "", 0);
        }
        assert_eq!(q.metrics().messages, 5, "no groups means nothing is consumed yet");
    }

    #[test]
    fn drained_topic_sheds_idle_caught_up_groups() {
        let mut t = TopicState {
            next_offset: 10,
            ..TopicState::default()
        };
        for i in 0..(GROUP_GC_THRESHOLD + 5) {
            t.groups.insert(
                format!("g{i}"),
                GroupState {
                    ack_watermark: 10,
                    ..GroupState::default()
                },
            );
        }
        gc_idle_groups(&mut t);
        assert!(t.groups.is_empty(), "a drained topic must forget caught-up idle groups");
    }

    #[test]
    fn small_group_counts_are_left_alone() {
        let mut t = TopicState {
            next_offset: 10,
            ..TopicState::default()
        };
        t.groups.insert(
            "g".to_string(),
            GroupState {
                ack_watermark: 10,
                ..GroupState::default()
            },
        );
        gc_idle_groups(&mut t);
        assert_eq!(t.groups.len(), 1, "below the threshold nothing is touched");
    }

    #[test]
    fn group_holding_a_lease_or_lagging_survives_gc() {
        let mut t = TopicState {
            next_offset: 10,
            ..TopicState::default()
        };
        for i in 0..(GROUP_GC_THRESHOLD + 1) {
            t.groups.insert(
                format!("g{i}"),
                GroupState {
                    ack_watermark: 10,
                    ..GroupState::default()
                },
            );
        }
        let mut leased = GroupState {
            ack_watermark: 10,
            ..GroupState::default()
        };
        leased.in_flight.insert(1, Lease { offset: 5, deadline_ms: 0 });
        t.groups.insert("leased".to_string(), leased);
        t.groups.insert(
            "lagging".to_string(),
            GroupState {
                ack_watermark: 3,
                ..GroupState::default()
            },
        );
        gc_idle_groups(&mut t);
        assert!(t.groups.contains_key("leased"), "a group with an in-flight lease must be kept");
        assert!(t.groups.contains_key("lagging"), "a lagging group must be kept");
        assert_eq!(t.groups.len(), 2);
    }

    #[test]
    fn default_retention_cap_applies_when_unset() {
        let t = TopicState::default();
        assert_eq!(effective_retention(&t), (DEFAULT_RETAIN_MAX_MESSAGES, 0));
    }

    #[test]
    fn explicit_retention_overrides_default_cap() {
        let by_count = TopicState {
            retain_max_messages: 50,
            ..TopicState::default()
        };
        assert_eq!(effective_retention(&by_count), (50, 0));

        let by_age = TopicState {
            retain_max_age_ms: 1000,
            ..TopicState::default()
        };
        assert_eq!(effective_retention(&by_age), (0, 1000));
    }

    #[test]
    fn default_cap_bounds_an_unconsumed_topic() {
        let mut t = TopicState::default();
        for offset in 0..(DEFAULT_RETAIN_MAX_MESSAGES + 5) {
            t.messages.insert(
                offset,
                Message {
                    offset,
                    priority: Priority(0),
                    content_type: ContentType::Raw,
                    payload: Bytes::new(),
                    ts_ms: 0,
                },
            );
            t.next_offset = offset + 1;
        }
        purge_retained(&mut t, 0);
        assert_eq!(t.messages.len() as u64, DEFAULT_RETAIN_MAX_MESSAGES);
        assert_eq!(t.messages.keys().next().copied(), Some(5));
    }

    #[test]
    fn produce_many_assigns_offsets_in_order_and_dedups() {
        let mut q = Queue::default();
        let item = |payload: &[u8], producer: &str, seq: u64| ProduceItem {
            topic: TopicId::from("t"),
            priority: Priority(0),
            content_type: ContentType::Raw,
            payload: Bytes::copy_from_slice(payload),
            producer_id: producer.to_string(),
            seq,
            ts_ms: 0,
        };
        let resp = q.apply(AppRequest::ProduceMany {
            items: vec![
                item(b"a", "p", 1),
                item(b"b", "p", 2),
                item(b"a-retry", "p", 1),
                item(b"c", "", 0),
            ],
        });
        match resp {
            AppResponse::ProducedMany { offsets } => {
                assert_eq!(offsets, vec![0, 1, 0, 2], "dedup inside a batch must return the original offset");
            }
            other => panic!("expected ProducedMany, got {other:?}"),
        }
        let polled = poll_offsets(&mut q, "t", "g", 10, 1000, 0);
        assert_eq!(polled.len(), 3, "the dedup re-send must not create a fourth message");
    }

    #[test]
    fn ack_many_acks_all_leases() {
        let mut q = Queue::default();
        for i in 0..3 {
            produce(&mut q, "t", 0, &[i], "", 0);
        }
        let leased = poll_offsets(&mut q, "t", "g", 10, 1000, 0);
        let lease_ids: Vec<LeaseId> = leased.iter().map(|(id, _)| *id).collect();
        q.apply(AppRequest::AckMany {
            topic: TopicId::from("t"),
            group: GroupId::from("g"),
            lease_ids,
        });
        let after = poll_offsets(&mut q, "t", "g", 10, 1000, 5000);
        assert!(after.is_empty(), "all leases must be acked in one request");
    }

    #[test]
    fn nack_many_releases_all_leases() {
        let mut q = Queue::default();
        for i in 0..3 {
            produce(&mut q, "t", 0, &[i], "", 0);
        }
        let leased = poll_offsets(&mut q, "t", "g", 10, 1000, 0);
        let lease_ids: Vec<LeaseId> = leased.iter().map(|(id, _)| *id).collect();
        q.apply(AppRequest::NackMany {
            topic: TopicId::from("t"),
            group: GroupId::from("g"),
            lease_ids,
        });
        let again = poll_offsets(&mut q, "t", "g", 10, 1000, 1);
        assert_eq!(again.len(), 3, "nacked messages must be immediately redeliverable");
    }

    #[test]
    fn separate_groups_each_see_every_message() {
        let mut q = Queue::default();
        produce(&mut q, "t", 0, b"a", "p", 1);
        let g1 = poll_offsets(&mut q, "t", "g1", 10, 1000, 0);
        let g2 = poll_offsets(&mut q, "t", "g2", 10, 1000, 0);
        assert_eq!(g1.len(), 1);
        assert_eq!(g2.len(), 1);
    }

    #[test]
    fn competing_consumers_in_group_split_work() {
        let mut q = Queue::default();
        for i in 0..4 {
            produce(&mut q, "t", 0, &[i], "", 0);
        }
        let a = poll_offsets(&mut q, "t", "g", 2, 100_000, 0);
        let b = poll_offsets(&mut q, "t", "g", 2, 100_000, 1);
        assert_eq!(a.len(), 2);
        assert_eq!(b.len(), 2);
        let mut all: Vec<Offset> = a.iter().chain(b.iter()).map(|(_, o)| *o).collect();
        all.sort();
        assert_eq!(all, vec![0, 1, 2, 3], "each message delivered to exactly one consumer");
    }

    #[test]
    fn single_consumer_sees_fifo() {
        let mut q = Queue::default();
        for i in 0..5 {
            produce(&mut q, "t", 0, &[i], "", 0);
        }
        let offsets: Vec<Offset> = poll_offsets(&mut q, "t", "g", 10, 1000, 0)
            .into_iter()
            .map(|(_, o)| o)
            .collect();
        assert_eq!(offsets, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn poll_auto_acks_in_one_apply() {
        let mut q = Queue::default();
        for i in 0..3 {
            produce(&mut q, "t", 0, &[i], "", 0);
        }
        let resp = q.apply(AppRequest::PollAuto {
            topic: TopicId::from("t"),
            group: GroupId::from("g"),
            max: 10,
            visibility_timeout_ms: 1000,
            ts_ms: 0,
        });
        let items = match resp {
            AppResponse::Polled { items } => items,
            other => panic!("expected Polled, got {other:?}"),
        };
        assert_eq!(items.len(), 3);
        assert_eq!(q.metrics().in_flight, 0, "auto-acked leases must not stay in flight");
        assert_eq!(q.metrics().messages, 0, "acked messages are reclaimed in the same apply");
        let again = poll_offsets(&mut q, "t", "g", 10, 1000, 5000);
        assert!(again.is_empty(), "auto-acked messages must not be redelivered");
    }

    #[test]
    fn idle_group_expires_and_unpins_memory() {
        let mut q = Queue::default();
        for i in 0..3 {
            produce(&mut q, "t", 0, &[i], "", 0);
        }
        poll_offsets(&mut q, "t", "dead", 1, 10, 1_000);
        let leased = poll_offsets(&mut q, "t", "live", 10, 1000, GROUP_IDLE_EXPIRE_MS);
        assert_eq!(leased.len(), 3);
        for (lease_id, _) in leased {
            q.apply(AppRequest::Ack {
                topic: TopicId::from("t"),
                group: GroupId::from("live"),
                lease_id,
            });
        }
        assert_eq!(q.metrics().messages, 3, "the dead group still pins the messages");
        poll_offsets(&mut q, "t", "live", 10, 1000, GROUP_IDLE_EXPIRE_MS + 1_002);
        let groups = &q.topics["t"].groups;
        assert!(!groups.contains_key("dead"), "an idle group must expire after the TTL");
        assert!(groups.contains_key("live"), "an active group must survive the sweep");
        assert_eq!(q.metrics().messages, 0, "expiring the idle group frees the messages");
    }

    #[test]
    fn idle_group_with_unexpired_lease_survives() {
        let mut q = Queue::default();
        produce(&mut q, "t", 0, b"a", "", 0);
        poll_offsets(&mut q, "t", "dead", 1, MAX_VISIBILITY_MS, 1_000);
        // A poll can no longer set a deadline past the idle expiry, because the
        // visibility timeout is capped well below it. Put one there by hand, the
        // way a state restored from an older release can carry it, so the sweep
        // still has a lease to respect.
        let far = GROUP_IDLE_EXPIRE_MS * 3;
        let g = q.topics.get_mut("t").unwrap().groups.get_mut("dead").unwrap();
        for lease in g.in_flight.values_mut() {
            lease.deadline_ms = far;
        }
        for deadline in g.leased_offsets.values_mut() {
            *deadline = far;
        }
        poll_offsets(&mut q, "t", "live", 10, 1000, GROUP_IDLE_EXPIRE_MS + 2_000);
        assert!(
            q.topics["t"].groups.contains_key("dead"),
            "a group holding an unexpired lease must not be swept"
        );
    }

    #[test]
    fn a_parked_lease_cannot_outlive_the_group_expiry() {
        let mut q = Queue::default();
        produce(&mut q, "t", 0, b"a", "", 0);
        poll_offsets(&mut q, "t", "parked", 1, u64::MAX, 1_000);
        poll_offsets(&mut q, "t", "live", 10, 1000, GROUP_IDLE_EXPIRE_MS + 2_000);
        assert!(
            !q.topics["t"].groups.contains_key("parked"),
            "one poll must not be able to hold a group past the idle expiry"
        );
    }

    #[test]
    fn a_group_without_an_activity_timestamp_is_stamped_then_expires() {
        let mut q = Queue::default();
        produce(&mut q, "t", 0, b"a", "", 0);
        q.apply(AppRequest::CommitOffset {
            topic: TopicId::from("t"),
            group: GroupId::from("commit-only"),
            offset: 0,
        });
        let first = GROUP_IDLE_EXPIRE_MS * 10;
        poll_offsets(&mut q, "t", "live", 10, 1000, first);
        assert!(
            q.topics["t"].groups.contains_key("commit-only"),
            "the first sweep stamps an unstamped group rather than dropping it"
        );
        poll_offsets(&mut q, "t", "live", 10, 1000, first + GROUP_IDLE_EXPIRE_MS + 1);
        assert!(
            !q.topics["t"].groups.contains_key("commit-only"),
            "once stamped, it expires on the ordinary idle schedule"
        );
    }

    fn reference_selection(
        candidates: &[(Priority, Offset)],
        max: usize,
        poll_count: u64,
    ) -> Vec<Offset> {
        if candidates.is_empty() || max == 0 {
            return Vec::new();
        }
        let base = max / RESERVED_DEN as usize;
        let bonus = usize::from((poll_count + 1).is_multiple_of(RESERVED_DEN));
        let reserved = (base + bonus).min(max);
        let priority_slots = max - reserved;
        let by_offset = candidates.to_vec();
        let mut by_priority = candidates.to_vec();
        by_priority.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        let mut chosen_set: BTreeSet<Offset> = BTreeSet::new();
        let mut chosen: Vec<(Priority, Offset)> = Vec::new();
        for item in by_priority.iter() {
            if chosen.len() >= priority_slots {
                break;
            }
            if chosen_set.insert(item.1) {
                chosen.push(*item);
            }
        }
        for item in by_offset.iter() {
            if chosen.len() >= max {
                break;
            }
            if chosen_set.insert(item.1) {
                chosen.push(*item);
            }
        }
        for item in by_priority.iter() {
            if chosen.len() >= max {
                break;
            }
            if chosen_set.insert(item.1) {
                chosen.push(*item);
            }
        }
        chosen.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        chosen.into_iter().map(|(_, o)| o).collect()
    }

    proptest! {
        #[test]
        fn bounded_selection_matches_reference(
            priorities in prop::collection::vec(0u8..8, 1..120),
            max in 1u32..20,
            rounds in 1usize..6,
        ) {
            let mut q = Queue::default();
            let mut remaining: Vec<(Priority, Offset)> = Vec::new();
            for (i, p) in priorities.iter().enumerate() {
                produce(&mut q, "t", *p, &[*p], "", 0);
                remaining.push((Priority(*p), i as Offset));
            }
            for round in 0..rounds {
                let expected = reference_selection(&remaining, max as usize, round as u64);
                let got: Vec<Offset> = poll_offsets(&mut q, "t", "g", max, 1_000_000, round as u64)
                    .into_iter()
                    .map(|(_, o)| o)
                    .collect();
                prop_assert_eq!(&got, &expected, "round {} diverged from the old algorithm", round);
                remaining.retain(|(_, o)| !expected.contains(o));
            }
        }
    }

    proptest! {
        #[test]
        fn dedup_then_drain_is_exactly_once(keys in prop::collection::vec(0u8..6, 1..40)) {
            let mut q = Queue::default();
            let mut distinct = BTreeSet::new();
            for (i, k) in keys.iter().enumerate() {
                let producer = format!("p{k}");
                distinct.insert(producer.clone());
                q.apply(AppRequest::Produce {
                    topic: TopicId::from("t"),
                    priority: Priority(0),
                    content_type: ContentType::Raw,
                    payload: Bytes::from(vec![*k]),
                    producer_id: producer,
                    seq: 0,
                    ts_ms: i as u64,
                });
            }

            let mut drained = 0usize;
            let mut ts = 1_000u64;
            loop {
                let resp = q.apply(AppRequest::Poll {
                    topic: TopicId::from("t"),
                    group: GroupId::from("g"),
                    max: 8,
                    visibility_timeout_ms: 1000,
                    ts_ms: ts,
                });
                ts += 2000;
                let items = match resp {
                    AppResponse::Polled { items } => items,
                    other => panic!("expected Polled, got {other:?}"),
                };
                if items.is_empty() {
                    break;
                }
                for d in items {
                    drained += 1;
                    q.apply(AppRequest::Ack {
                        topic: TopicId::from("t"),
                        group: GroupId::from("g"),
                        lease_id: d.lease_id,
                    });
                }
                prop_assert!(ts < 10_000_000);
            }
            prop_assert_eq!(drained, distinct.len());
        }
    }
}
