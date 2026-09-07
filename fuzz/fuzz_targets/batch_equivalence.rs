#![no_main]

use hermesmq_core::raft::{AppRequest, AppResponse, ProduceItem};
use hermesmq_core::{GroupId, Queue, TopicId};
use hermesmq_fuzz::{encode, group_name, topic_name, Driver, Item, Op};
use libfuzzer_sys::fuzz_target;

// The client batches aggressively: up to 256 produces fold into one ProduceMany
// and up to 1024 acks into one AckMany or NackMany. A batched path that diverges
// from the single-op path only shows up under load, so compare them directly.
fuzz_target!(|input: (Vec<Item>, u8, u8, bool)| {
    let (items, topic, group, nack) = input;
    if items.is_empty() {
        return;
    }
    let items: Vec<Item> = items.into_iter().take(32).collect();

    let mut batched = Queue::default();
    let mut single = Queue::default();
    let mut d = Driver::new();

    let many: Vec<ProduceItem> = items
        .iter()
        .map(|i| match d.request(&Op::Produce(i.clone())) {
            Some(AppRequest::Produce {
                topic,
                priority,
                content_type,
                payload,
                producer_id,
                seq,
                ts_ms,
            }) => ProduceItem {
                topic,
                priority,
                content_type,
                payload,
                producer_id,
                seq,
                ts_ms,
            },
            _ => unreachable!("a Produce op maps to a Produce request"),
        })
        .collect();

    let batch_resp = batched.apply(AppRequest::ProduceMany { items: many.clone() });
    let mut one_by_one = Vec::new();
    for item in many {
        match single.apply(AppRequest::Produce {
            topic: item.topic,
            priority: item.priority,
            content_type: item.content_type,
            payload: item.payload,
            producer_id: item.producer_id,
            seq: item.seq,
            ts_ms: item.ts_ms,
        }) {
            AppResponse::Produced { offset } => one_by_one.push(offset),
            other => panic!("expected Produced, got {other:?}"),
        }
    }
    assert_eq!(
        batch_resp,
        AppResponse::ProducedMany { offsets: one_by_one },
        "ProduceMany assigned different offsets than repeated Produce"
    );
    assert_eq!(
        encode(&batched),
        encode(&single),
        "ProduceMany left different state than repeated Produce"
    );

    let t = TopicId(topic_name(topic));
    let g = GroupId(group_name(group));
    let poll = |q: &mut Queue| -> Vec<u64> {
        match q.apply(AppRequest::Poll {
            topic: t.clone(),
            group: g.clone(),
            max: 64,
            visibility_timeout_ms: 1_000,
            ts_ms: 1,
        }) {
            AppResponse::Polled { items } => items.into_iter().map(|d| d.lease_id).collect(),
            other => panic!("expected Polled, got {other:?}"),
        }
    };
    let batch_leases = poll(&mut batched);
    let single_leases = poll(&mut single);
    assert_eq!(batch_leases, single_leases, "the polls diverged before the ack test");

    if nack {
        batched.apply(AppRequest::NackMany {
            topic: t.clone(),
            group: g.clone(),
            lease_ids: batch_leases,
        });
        for lease_id in single_leases {
            single.apply(AppRequest::Nack {
                topic: t.clone(),
                group: g.clone(),
                lease_id,
            });
        }
    } else {
        batched.apply(AppRequest::AckMany {
            topic: t.clone(),
            group: g.clone(),
            lease_ids: batch_leases,
        });
        for lease_id in single_leases {
            single.apply(AppRequest::Ack {
                topic: t.clone(),
                group: g.clone(),
                lease_id,
            });
        }
    }

    // Ack reclaims once per operation while AckMany reclaims once at the end, so
    // settle both sides with the same trailing reclaim before comparing.
    for q in [&mut batched, &mut single] {
        q.apply(AppRequest::Ack {
            topic: t.clone(),
            group: g.clone(),
            lease_id: u64::MAX,
        });
    }
    assert_eq!(
        encode(&batched),
        encode(&single),
        "the batched ack path left different state than repeated single acks"
    );
});
