#![no_main]

use hermesmq_core::Queue;
use hermesmq_fuzz::{encode, run, Op};
use libfuzzer_sys::fuzz_target;

// A node that was wiped rejoins by taking a snapshot from the leader. After the
// transfer both nodes must behave identically: same responses to the same
// requests, same resulting state.
fuzz_target!(|input: (Vec<Op>, Vec<Op>)| {
    let (build, then) = input;
    let mut a = Queue::default();
    run(&mut a, &build);

    let bytes = encode(&a);
    let mut b: Queue = postcard::from_bytes(&bytes).expect("a snapshot must decode");
    assert_eq!(encode(&b), bytes, "snapshot encode/decode is not stable");
    if let Err(e) = b.debug_check_invariants() {
        panic!("a decoded snapshot broke an invariant: {e}");
    }

    let ra = run(&mut a, &then);
    let rb = run(&mut b, &then);
    assert_eq!(ra, rb, "the restored node answered differently");
    assert_eq!(encode(&a), encode(&b), "the restored node diverged");
});
