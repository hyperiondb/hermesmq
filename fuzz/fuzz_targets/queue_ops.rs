#![no_main]

use hermesmq_core::raft::AppRequest;
use hermesmq_core::Queue;
use hermesmq_fuzz::{Driver, Op, MAX_OPS};
use libfuzzer_sys::fuzz_target;

// The queue state machine under a generated request sequence. Every step must
// leave the internal bookkeeping self-consistent: these are the invariants the
// poll/ack/reclaim paths rely on, and a snapshot or a crafted request that
// breaks one of them corrupts state on every node in the cluster.
fuzz_target!(|ops: Vec<Op>| {
    let mut q = Queue::default();
    let mut driver = Driver::new();
    for op in ops.iter().take(MAX_OPS) {
        let Some(req) = driver.request(op) else {
            continue;
        };
        let touched = stored_topics(&req);
        let resp = q.apply(req);
        driver.observe(&resp);
        if let Err(e) = q.debug_check_invariants() {
            panic!("invariant broken after {op:?}: {e}");
        }
        for topic in touched {
            let Some(cap) = driver.retention.get(&topic).copied() else {
                continue;
            };
            if cap == 0 {
                continue;
            }
            if let Some(len) = q.debug_topic_len(&topic) {
                assert!(
                    len as u64 <= cap,
                    "topic {topic} kept {len} messages with a retention cap of {cap}"
                );
            }
        }
    }
});

/// Topics this request produced to.
///
/// Retention runs on every produce, including one the dedup table answers from
/// cache, so every topic named by a produce must be at or under its cap
/// afterwards.
fn stored_topics(req: &AppRequest) -> Vec<String> {
    match req {
        AppRequest::Produce { topic, .. } => vec![topic.0.clone()],
        AppRequest::ProduceMany { items } => {
            let mut out: Vec<String> = items.iter().map(|i| i.topic.0.clone()).collect();
            out.sort();
            out.dedup();
            out
        }
        _ => Vec::new(),
    }
}
