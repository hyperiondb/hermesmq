//! Reproductions for the findings recorded in VULN.md at the repository root.
//!
//! Each case drives the same attack the finding described and prints what the
//! code does about it now. Run it both ways - overflow checks changed what the
//! unfixed code did, so the two builds are still worth comparing:
//!
//! ```sh
//! cargo run -p hermesmq-core --example security_repro            # overflow checks on
//! cargo run -p hermesmq-core --example security_repro --release  # what a shipped binary does
//! ```
//!
//! Nothing here opens a socket; every case drives the queue state machine, the
//! snapshot decoder, the rate limiter or the frame reader directly.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use hermesmq_core::engine::{fuzz_store_snapshot, StateMachineStore};
use hermesmq_core::raft::AppRequest;
use hermesmq_core::types::{ContentType, Message, Priority};
use hermesmq_core::{GroupId, Queue, RedbStore, TopicId};
use serde::Serialize;

struct Counting;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(l.size(), Ordering::Relaxed);
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        if new > l.size() {
            ALLOCATED.fetch_add(new - l.size(), Ordering::Relaxed);
        }
        System.realloc(p, l, new)
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(l.size(), Ordering::Relaxed);
        System.alloc_zeroed(l)
    }
}

#[global_allocator]
static A: Counting = Counting;

#[derive(Serialize)]
struct V0Group {
    ack_watermark: u64,
    acked_above: BTreeSet<u64>,
    in_flight: BTreeMap<u64, (u64, u64)>,
    leased_offsets: BTreeMap<u64, u64>,
    poll_count: u64,
}

#[derive(Serialize)]
struct V0Topic {
    next_offset: u64,
    messages: BTreeMap<u64, Message>,
    dedup: BTreeMap<(String, u64), u64>,
    dedup_order: VecDeque<(String, u64)>,
    groups: BTreeMap<String, V0Group>,
    rate_milli_per_sec: u64,
    burst: u32,
    retain_max_messages: u64,
    retain_max_age_ms: u64,
}

#[derive(Serialize)]
struct V0Queue {
    topics: BTreeMap<String, V0Topic>,
    next_lease_id: u64,
}

#[derive(Serialize)]
struct V0Data {
    last_applied: Option<openraft::LogId<u64>>,
    last_membership: openraft::StoredMembership<u64, openraft::BasicNode>,
    queue: V0Queue,
}

fn snapshot_with(next_offset: u64, ack_watermark: u64, next_lease_id: u64) -> Vec<u8> {
    snapshot_full(next_offset, ack_watermark, next_lease_id, BTreeSet::new(), BTreeMap::new(), BTreeMap::new())
}

fn snapshot_full(
    next_offset: u64,
    ack_watermark: u64,
    next_lease_id: u64,
    acked_above: BTreeSet<u64>,
    in_flight: BTreeMap<u64, (u64, u64)>,
    leased_offsets: BTreeMap<u64, u64>,
) -> Vec<u8> {
    let topic = V0Topic {
        next_offset,
        messages: BTreeMap::new(),
        dedup: BTreeMap::new(),
        dedup_order: VecDeque::new(),
        groups: BTreeMap::from([(
            "g".to_string(),
            V0Group {
                ack_watermark,
                acked_above,
                in_flight,
                leased_offsets,
                poll_count: 0,
            },
        )]),
        rate_milli_per_sec: 0,
        burst: 0,
        retain_max_messages: 0,
        retain_max_age_ms: 0,
    };
    let data = V0Data {
        last_applied: None,
        last_membership: openraft::StoredMembership::default(),
        queue: V0Queue {
            topics: BTreeMap::from([("t".to_string(), topic)]),
            next_lease_id,
        },
    };
    postcard::to_stdvec(&data).unwrap()
}

fn snapshot_poll_count(poll_count: u64) -> Vec<u8> {
    let topic = V0Topic {
        next_offset: 0,
        messages: BTreeMap::new(),
        dedup: BTreeMap::new(),
        dedup_order: VecDeque::new(),
        groups: BTreeMap::from([(
            "g".to_string(),
            V0Group {
                ack_watermark: 0,
                acked_above: BTreeSet::new(),
                in_flight: BTreeMap::new(),
                leased_offsets: BTreeMap::new(),
                poll_count,
            },
        )]),
        rate_milli_per_sec: 0,
        burst: 0,
        retain_max_messages: 0,
        retain_max_age_ms: 0,
    };
    postcard::to_stdvec(&V0Data {
        last_applied: None,
        last_membership: openraft::StoredMembership::default(),
        queue: V0Queue {
            topics: BTreeMap::from([("t".to_string(), topic)]),
            next_lease_id: 0,
        },
    })
    .unwrap()
}

fn produce(topic: &str, producer: &str, seq: u64, payload: usize) -> AppRequest {
    AppRequest::Produce {
        topic: TopicId::from(topic),
        priority: Priority(0),
        content_type: ContentType::Raw,
        payload: Bytes::from(vec![0u8; payload]),
        producer_id: producer.to_string(),
        seq,
        ts_ms: 0,
    }
}

fn section(n: &str) {
    println!("\n================ {n} ================");
}

/// Write a snapshot record, then report whether opening the store accepts it.
fn install(label: &str, bytes: Vec<u8>) {
    let db = Arc::new(RedbStore::in_memory().unwrap());
    fuzz_store_snapshot(db.as_ref(), None, bytes).unwrap();
    println!("snapshot record written: {label}");
    match StateMachineStore::new(db) {
        Ok(sm) => {
            println!("  the store opened it; invariants: {:?}", sm.debug_check_invariants());
        }
        Err(e) => println!("  refused: {e}"),
    }
}

fn overflow_from_snapshot() {
    section("1. offset counter taken from a snapshot");
    install("next_offset = u64::MAX", snapshot_with(u64::MAX, 0, 0));
}

fn overflow_watermark_from_snapshot() {
    section("2. ack watermark taken from a snapshot");
    install(
        "ack_watermark = u64::MAX with a lease at offset u64::MAX",
        snapshot_full(
            5,
            u64::MAX,
            0,
            BTreeSet::new(),
            BTreeMap::from([(1u64, (u64::MAX, 10_000u64))]),
            BTreeMap::from([(u64::MAX, 10_000u64)]),
        ),
    );
}

fn overflow_lease_from_snapshot() {
    section("3. lease counter taken from a snapshot");
    install("next_lease_id = u64::MAX", snapshot_with(0, 0, u64::MAX));
}

fn overflow_poll_count_from_snapshot() {
    section("3b. poll counter taken from a snapshot");
    install("group poll counter = u64::MAX", snapshot_poll_count(u64::MAX));
}

fn ordinary_state_still_loads() {
    section("3c. a snapshot the queue produced itself is still accepted");
    let db = Arc::new(RedbStore::in_memory().unwrap());
    let sm = StateMachineStore::new(db).unwrap();
    for seq in 0..8 {
        sm.debug_apply(produce("t", "p", seq, 16));
    }
    let leased = match sm.debug_apply(AppRequest::Poll {
        topic: TopicId::from("t"),
        group: GroupId::from("g"),
        max: 4,
        visibility_timeout_ms: 1_000,
        ts_ms: 10,
    }) {
        hermesmq_core::raft::AppResponse::Polled { items } => items,
        other => panic!("expected Polled, got {other:?}"),
    };
    for d in &leased {
        sm.debug_apply(AppRequest::Ack {
            topic: TopicId::from("t"),
            group: GroupId::from("g"),
            lease_id: d.lease_id,
        });
    }
    sm.debug_apply(AppRequest::CommitOffsetAt {
        topic: TopicId::from("t"),
        group: GroupId::from("g"),
        offset: u64::MAX,
        ts_ms: 20,
    });
    println!("8 produces, one poll of 4, 4 acks, and a commit of u64::MAX");
    println!("  validation of the resulting state: {:?}", sm.debug_check_invariants());
}

fn group_namespace_growth() {
    section("4. consumer groups created by ordinary requests are never freed");
    let mut q = Queue::default();
    q.apply(produce("t", "", 0, 1));
    let n = 50_000;
    for i in 0..n {
        // What the client port sends for a commit: it carries the leader clock,
        // so the group it creates has an activity stamp like any other.
        q.apply(AppRequest::CommitOffsetAt {
            topic: TopicId::from("t"),
            group: GroupId(format!("{}{i}", "g".repeat(64))),
            offset: 0,
            ts_ms: 1,
        });
    }
    let f = q.debug_footprint();
    println!("after {n} commits with distinct 65-byte group names:");
    println!("  groups held: {}", f.groups);
    println!("  bytes held in topic+group names: {}", f.name_bytes);
    // A poll from a real consumer runs the idle sweep and the reclaim pass.
    q.apply(AppRequest::Poll {
        topic: TopicId::from("t"),
        group: GroupId::from("real"),
        max: 1,
        visibility_timeout_ms: 1,
        ts_ms: 40 * 24 * 60 * 60 * 1000,
    });
    let f = q.debug_footprint();
    println!("after a poll 40 days later (idle sweep + reclaim both run):");
    println!("  groups still held: {}", f.groups);
    println!("  retained messages: {}", f.messages);
    let long = "g".repeat(1024);
    println!(
        "a commit with a {}-byte group name: {:?}",
        long.len(),
        q.apply(AppRequest::CommitOffsetAt {
            topic: TopicId::from("t"),
            group: GroupId(long),
            offset: 0,
            ts_ms: 2,
        })
    );
}

fn dedup_growth() {
    section("5. producer dedup table is not covered by retention");
    let mut q = Queue::default();
    q.apply(AppRequest::SetRetention {
        topic: TopicId::from("t"),
        max_messages: 1,
        max_age_ms: 0,
    });
    let id_len = 200;
    let n = 20_000;
    for i in 0..n {
        let producer = format!("{}{i}", "p".repeat(id_len));
        q.apply(produce("t", &producer, 0, 1));
    }
    let f = q.debug_footprint();
    println!("retention is set to 1 message; {n} produces with {id_len}-byte producer ids:");
    println!("  retained messages: {}", f.messages);
    println!("  dedup entries held: {}", f.dedup_entries);
    println!("  bytes held in dedup keys: {}", f.dedup_bytes);
    let long = "p".repeat(4096);
    println!(
        "a produce with a {}-byte producer id: {:?}",
        long.len(),
        q.apply(produce("t", &long, 0, 1))
    );
}

fn topic_namespace_growth() {
    section("6. topics are created by any produce and are never freed");
    let mut q = Queue::default();
    let n = 20_000;
    for i in 0..n {
        q.apply(produce(&format!("{}{i}", "t".repeat(64)), "", 0, 1));
    }
    let f = q.debug_footprint();
    println!("after {n} produces to distinct topics:");
    println!("  topics held: {}", f.topics);
    println!("  bytes held in topic names: {}", f.name_bytes);
    println!(
        "the produce that would have made one more topic: {:?}",
        q.apply(produce("one-more", "", 0, 1))
    );
}

fn unbounded_visibility_timeout() {
    section("7. a client-chosen visibility timeout has no upper bound");
    use hermesmq_core::raft::AppResponse;
    let mut q = Queue::default();
    q.apply(produce("t", "", 0, 1));
    let resp = q.apply(AppRequest::Poll {
        topic: TopicId::from("t"),
        group: GroupId::from("parked"),
        max: 1,
        visibility_timeout_ms: u64::MAX,
        ts_ms: 1,
    });
    let leased = match resp {
        AppResponse::Polled { items } => items.len(),
        _ => 0,
    };
    let day = 24 * 60 * 60 * 1000u64;
    q.apply(AppRequest::Poll {
        topic: TopicId::from("t"),
        group: GroupId::from("real"),
        max: 10,
        visibility_timeout_ms: 1000,
        ts_ms: 3650 * day,
    });
    let f = q.debug_footprint();
    println!("one poll with visibility_timeout_ms = u64::MAX leased {leased} message(s)");
    println!("ten years later, after another consumer polls:");
    println!("  groups still held: {} (the parked one is swept once its lease lapses)", f.groups);
    println!("  messages still held: {}", f.messages);
}

fn rate_limiter_zero_retry() {
    section("8. rate limiter returns a zero retry delay when asked for zero tokens");
    use hermesmq_core::client::RateLimiter;
    let l = RateLimiter::default();
    println!("take(want=0) on a full bucket -> {:?}", l.take("t", 10.0, 100.0, 0));
    println!("take(want=1) on the same bucket -> {:?}", l.take("t", 10.0, 100.0, 1));
    let drained = RateLimiter::default();
    let _ = drained.take("t", 10.0, 1.0, 1);
    println!("take(want=1) on a drained bucket -> {:?}", drained.take("t", 10.0, 1.0, 1));
    println!("take(want=0) on a drained bucket -> {:?}", drained.take("t", 10.0, 1.0, 0));
}

fn retention_skipped_on_duplicate() {
    section("10. retention is not applied when a produce is deduplicated");
    let mut q = Queue::default();
    for seq in 0..3 {
        q.apply(produce("t", "p", seq, 1));
    }
    println!("3 messages stored under the default cap");
    q.apply(AppRequest::SetRetention {
        topic: TopicId::from("t"),
        max_messages: 1,
        max_age_ms: 0,
    });
    println!("retention lowered to 1 message");
    q.apply(produce("t", "p", 0, 1));
    println!("a produce that repeats producer p sequence 0 (a duplicate):");
    println!("  messages retained: {}", q.debug_topic_len("t").unwrap());
    q.apply(produce("t", "p", 99, 1));
    println!("a produce with a new sequence number:");
    println!("  messages retained: {}", q.debug_topic_len("t").unwrap());
}

async fn frame_preallocation() {
    section("9. frame reader reserves the declared length before the body arrives");
    use hermesmq_core::frame::{read_frame, MAX_PEER_FRAME};
    use tokio::io::AsyncWriteExt;

    let (mut writer, mut reader) = tokio::io::duplex(64);
    let header = (MAX_PEER_FRAME as u32).to_be_bytes();
    writer.write_all(&header).await.unwrap();
    writer.flush().await.unwrap();

    let before = ALLOCATED.load(Ordering::Relaxed);
    let task = tokio::spawn(async move {
        let _ = read_frame(&mut reader, MAX_PEER_FRAME).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let after = ALLOCATED.load(Ordering::Relaxed);
    println!("4 bytes of header sent, no body at all");
    println!("  bytes allocated by the reader while it waits: {}", after - before);
    println!("  declared frame length: {MAX_PEER_FRAME}");
    task.abort();
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    overflow_from_snapshot();
    overflow_watermark_from_snapshot();
    overflow_lease_from_snapshot();
    overflow_poll_count_from_snapshot();
    ordinary_state_still_loads();
    group_namespace_growth();
    dedup_growth();
    topic_namespace_growth();
    unbounded_visibility_timeout();
    rate_limiter_zero_retry();
    retention_skipped_on_duplicate();
    frame_preallocation().await;
}
