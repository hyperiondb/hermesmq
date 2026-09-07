#![no_main]

use std::sync::Arc;

use hermesmq_core::engine::{fuzz_decode_state, fuzz_store_snapshot, StateMachineStore, KEY_SNAPSHOT};
use hermesmq_core::Storage;
use hermesmq_fuzz::MemStore;
use hermesmq_fuzz::{Driver, Op, SnapshotShape};
use libfuzzer_sys::fuzz_target;
use openraft::storage::RaftStateMachine;
use openraft::SnapshotMeta;

thread_local! {
    static RT: tokio::runtime::Runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
}

// Snapshot bytes arrive over the peer socket and are also read back off disk on
// every restart. Two shapes are exercised: raw bytes against the decoder, and a
// structurally valid but semantically impossible legacy state followed by
// ordinary requests, which is where an offset counter parked near u64::MAX would
// show up.
// The decoder validates what it decoded, so an install that succeeds must leave
// a self-consistent state, and ordinary requests applied on top must keep it
// that way. An install that fails must leave the stored snapshot untouched.
fuzz_target!(|input: (&[u8], SnapshotShape, Vec<Op>)| {
    let (raw, shape, ops) = input;

    let _ = fuzz_decode_state(raw);

    let db = Arc::new(MemStore::default());
    let before = db.get(KEY_SNAPSHOT).unwrap();
    RT.with(|rt| rt.block_on(async {
        let mut sm = StateMachineStore::new(db.clone()).expect("empty store opens");
        let meta = SnapshotMeta::default();
        let installed = sm
            .install_snapshot(&meta, Box::new(std::io::Cursor::new(raw.to_vec())))
            .await;
        if installed.is_err() {
            assert_eq!(
                db.get(KEY_SNAPSHOT).unwrap(),
                before,
                "a snapshot that failed to decode must not be persisted"
            );
            return;
        }
        if let Err(e) = sm.debug_check_invariants() {
            panic!("an installed snapshot broke an invariant: {e}");
        }
        let mut driver = Driver::new();
        for op in ops.iter().take(64) {
            if let Some(req) = driver.request(op) {
                driver.observe(&sm.debug_apply(req));
                if let Err(e) = sm.debug_check_invariants() {
                    panic!("a request applied over an installed snapshot broke an invariant: {e}");
                }
            }
        }
    }));

    let bytes = shape.encode();
    let db = Arc::new(MemStore::default());
    fuzz_store_snapshot(db.as_ref(), None, bytes).expect("snapshot record written");
    let Ok(sm) = StateMachineStore::new(db) else {
        return;
    };
    if let Err(e) = sm.debug_check_invariants() {
        panic!("a stored snapshot the store accepted broke an invariant: {e}");
    }
    let mut driver = Driver::new();
    for op in ops.iter().take(64) {
        let Some(req) = driver.request(op) else {
            continue;
        };
        let resp = sm.debug_apply(req);
        driver.observe(&resp);
        if let Err(e) = sm.debug_check_invariants() {
            panic!("a request applied over a stored snapshot broke an invariant: {e}");
        }
    }
});
