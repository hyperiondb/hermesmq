#![no_main]

use hermesmq_core::raft::{AppRequest, TypeConfig};
use hermesmq_fuzz::{Driver, Op};
use libfuzzer_sys::fuzz_target;
use openraft::Entry;

// Bytes read back out of redb. Reachable after partial disk damage, and the
// same decoder openraft drives during startup replay.
fuzz_target!(|input: (&[u8], Vec<Op>)| {
    let (data, ops) = input;
    let _ = postcard::from_bytes::<Entry<TypeConfig>>(data);

    // The on-disk log format must round-trip: a serde or field-order change that
    // silently broke it would make every existing log unreadable after an upgrade.
    let mut driver = Driver::new();
    for op in ops.iter().take(32) {
        let Some(req) = driver.request(op) else {
            continue;
        };
        let bytes = postcard::to_stdvec(&req).expect("a request must encode");
        let back: AppRequest = postcard::from_bytes(&bytes).expect("a request must decode");
        assert_eq!(req, back, "AppRequest did not survive a postcard round trip");
    }
});
