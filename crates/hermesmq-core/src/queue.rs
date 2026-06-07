use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::raft::{AppRequest, AppResponse, Delivered};
use crate::types::{LeaseId, Message, Offset, Priority};

const DEDUP_CAPACITY: usize = 100_000;
const RESERVED_DEN: u64 = 4;

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
    leased_offsets: BTreeSet<Offset>,
    poll_count: u64,
}

impl GroupState {
    fn is_done(&self, offset: Offset) -> bool {
        offset < self.ack_watermark || self.acked_above.contains(&offset)
    }

    fn mark_acked(&mut self, offset: Offset) {
        if offset < self.ack_watermark {
            return;
        }
        self.acked_above.insert(offset);
        while self.acked_above.remove(&self.ack_watermark) {
            self.ack_watermark += 1;
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
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct TopicState {
    exists: bool,
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
                self.topics.entry(topic.0).or_default().exists = true;
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
                let t = self.topics.entry(topic.0).or_default();
                t.exists = true;
                let dedup = !producer_id.is_empty();
                if dedup {
                    if let Some(offset) = t.dedup.get(&(producer_id.clone(), seq)) {
                        return AppResponse::Produced { offset: *offset };
                    }
                }
                let offset = t.next_offset;
                t.next_offset += 1;
                t.messages.insert(
                    offset,
                    Message {
                        offset,
                        priority,
                        content_type,
                        payload,
                        ts_ms,
                    },
                );
                if dedup {
                    let key = (producer_id, seq);
                    t.dedup.insert(key.clone(), offset);
                    t.dedup_order.push_back(key);
                    while t.dedup_order.len() > DEDUP_CAPACITY {
                        if let Some(old) = t.dedup_order.pop_front() {
                            t.dedup.remove(&old);
                        }
                    }
                }
                purge_retained(t, ts_ms);
                AppResponse::Produced { offset }
            }
            AppRequest::Poll {
                topic,
                group,
                max,
                visibility_timeout_ms,
                ts_ms,
            } => {
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
            AppRequest::Ack {
                topic,
                group,
                lease_id,
            } => {
                ack(&mut self.topics, &topic.0, &group.0, lease_id);
                AppResponse::Acked
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
                commit(&mut self.topics, &topic.0, &group.0, offset);
                AppResponse::Committed
            }
            AppRequest::SetRateLimit {
                topic,
                rate_milli_per_sec,
                burst,
            } => {
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
                let t = self.topics.entry(topic.0).or_default();
                t.retain_max_messages = max_messages;
                t.retain_max_age_ms = max_age_ms;
                AppResponse::RetentionSet
            }
        }
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
            Some(g) => t.messages.keys().any(|offset| {
                !g.is_done(*offset)
                    && !g
                        .in_flight
                        .values()
                        .any(|lease| lease.offset == *offset && lease.deadline_ms > now_ms)
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
    let g = t.groups.entry(group.to_string()).or_default();
    g.expire(ts_ms);

    let max = max as usize;
    let mut candidates: Vec<(Priority, Offset)> = Vec::new();
    for (offset, message) in t.messages.iter() {
        if !g.is_done(*offset) && !g.leased_offsets.contains(offset) {
            candidates.push((message.priority, *offset));
        }
    }

    let poll_count = g.poll_count;
    g.poll_count += 1;

    if candidates.is_empty() || max == 0 {
        return Vec::new();
    }

    let base = max / RESERVED_DEN as usize;
    let bonus = usize::from((poll_count + 1) % RESERVED_DEN == 0);
    let reserved = (base + bonus).min(max);
    let priority_slots = max - reserved;

    let mut by_priority = candidates.clone();
    by_priority.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let mut by_offset = candidates;
    by_offset.sort_by_key(|(_, offset)| *offset);

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

    let mut items = Vec::new();
    for (_, offset) in chosen {
        let lease_id = *next_lease;
        *next_lease += 1;
        g.in_flight.insert(
            lease_id,
            Lease {
                offset,
                deadline_ms: ts_ms + visibility_timeout_ms,
            },
        );
        g.leased_offsets.insert(offset);
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

fn purge_retained(t: &mut TopicState, now_ms: u64) {
    if t.retain_max_age_ms == 0 && t.retain_max_messages == 0 {
        return;
    }
    let mut purge: BTreeSet<Offset> = BTreeSet::new();
    if t.retain_max_age_ms > 0 {
        for (offset, message) in t.messages.iter() {
            if now_ms.saturating_sub(message.ts_ms) > t.retain_max_age_ms {
                purge.insert(*offset);
            } else {
                break;
            }
        }
    }
    if t.retain_max_messages > 0 {
        let target = t.retain_max_messages as usize;
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
    for offset in &purge {
        t.messages.remove(offset);
        for g in t.groups.values_mut() {
            let leases: Vec<LeaseId> = g
                .in_flight
                .iter()
                .filter(|(_, lease)| lease.offset == *offset)
                .map(|(id, _)| *id)
                .collect();
            for id in leases {
                g.in_flight.remove(&id);
            }
            g.leased_offsets.remove(offset);
            g.acked_above.remove(offset);
        }
    }
}

fn commit(topics: &mut BTreeMap<String, TopicState>, topic: &str, group: &str, offset: Offset) {
    if let Some(t) = topics.get_mut(topic) {
        let g = t.groups.entry(group.to_string()).or_default();
        if offset > g.ack_watermark {
            g.ack_watermark = offset;
        }
        let watermark = g.ack_watermark;
        g.acked_above.retain(|o| *o >= watermark);
        let drop: Vec<LeaseId> = g
            .in_flight
            .iter()
            .filter(|(_, l)| l.offset < watermark)
            .map(|(id, _)| *id)
            .collect();
        for id in drop {
            if let Some(l) = g.in_flight.remove(&id) {
                g.leased_offsets.remove(&l.offset);
            }
        }
        while g.acked_above.remove(&g.ack_watermark) {
            g.ack_watermark += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ContentType, GroupId, Priority, TopicId};
    use proptest::prelude::*;

    fn produce(q: &mut Queue, topic: &str, priority: u8, body: &[u8], producer: &str, seq: u64) -> Offset {
        match q.apply(AppRequest::Produce {
            topic: TopicId::from(topic),
            priority: Priority(priority),
            content_type: ContentType::Raw,
            payload: body.to_vec(),
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
            payload: b"old".to_vec(),
            producer_id: String::new(),
            seq: 0,
            ts_ms: 0,
        });
        q.apply(AppRequest::Produce {
            topic: TopicId::from("t"),
            priority: Priority(0),
            content_type: ContentType::Raw,
            payload: b"new".to_vec(),
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
                    payload: vec![*k],
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
