#![no_main]

use hermesmq_core::Queue;
use hermesmq_fuzz::{encode, run, Driver, Op, MAX_OPS};
use libfuzzer_sys::fuzz_target;

// Raft only converges if applying identical log entries yields identical state.
// Two things are checked: two fresh queues fed the same sequence agree step for
// step, and a queue that is encoded and decoded halfway through keeps agreeing.
fuzz_target!(|ops: Vec<Op>| {
    let mut a = Queue::default();
    let mut b = Queue::default();
    let ra = run(&mut a, &ops);
    let rb = run(&mut b, &ops);
    assert_eq!(ra, rb, "the same op sequence produced different responses");
    assert_eq!(
        encode(&a),
        encode(&b),
        "the same op sequence produced different state"
    );

    let bounded: Vec<&Op> = ops.iter().take(MAX_OPS).collect();
    let split = bounded.len() / 2;
    let mut c = Queue::default();
    let mut driver = Driver::new();
    let mut rc = Vec::new();
    for (i, op) in bounded.iter().enumerate() {
        if i == split {
            c = postcard::from_bytes(&encode(&c)).expect("a queue must survive a snapshot");
        }
        if let Some(req) = driver.request(op) {
            let resp = c.apply(req);
            driver.observe(&resp);
            rc.push(resp);
        }
    }
    assert_eq!(ra, rc, "a snapshot boundary changed the responses");
    assert_eq!(encode(&a), encode(&c), "a snapshot boundary changed the state");
});
