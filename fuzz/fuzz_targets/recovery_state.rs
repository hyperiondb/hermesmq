#![no_main]

use arbitrary::Arbitrary;
use bytes::Bytes;
use hermesmq_core::engine::{fuzz_recover_consistency, fuzz_snapshot_last_index, fuzz_store_snapshot, KEY_PURGED};
use hermesmq_core::Storage;
use hermesmq_fuzz::MemStore;
use libfuzzer_sys::fuzz_target;
use openraft::{CommittedLeaderId, LogId};

#[derive(Arbitrary, Debug)]
struct Damage {
    log_indices: Vec<u8>,
    snapshot_index: Option<u8>,
    purged_index: Option<u8>,
    committed_index: Option<u8>,
    committed_garbage: bool,
}

fn log_id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(1, 1), index)
}

// A node that lost part of its disk. The startup repair has to leave the store
// in a shape openraft can actually read, or return an error - never panic and
// never leave a committed pointer that points at entries nobody has.
fuzz_target!(|damage: Damage| {
    let db = MemStore::default();

    let entries: Vec<(u64, Bytes)> = damage
        .log_indices
        .iter()
        .take(32)
        .map(|i| (*i as u64, Bytes::from_static(b"entry")))
        .collect();
    if !entries.is_empty() {
        db.append_log(&entries).expect("seed log");
    }
    if let Some(index) = damage.snapshot_index {
        fuzz_store_snapshot(&db, Some(log_id(index as u64)), Vec::new()).expect("seed snapshot");
    }
    if let Some(index) = damage.purged_index {
        let bytes = postcard::to_stdvec(&log_id(index as u64)).expect("encode purged marker");
        db.put(KEY_PURGED, &bytes).expect("seed purged marker");
    }
    if damage.committed_garbage {
        db.save_committed(b"\xff\xff\xff\xff\xff\xff").expect("seed committed");
    } else if let Some(index) = damage.committed_index {
        let bytes = postcard::to_stdvec(&Some(log_id(index as u64))).expect("encode committed");
        db.save_committed(&bytes).expect("seed committed");
    }

    if fuzz_recover_consistency(&db).is_err() {
        return;
    }

    let snap = fuzz_snapshot_last_index(&db).expect("snapshot index readable after repair");
    let base_next = snap.map(|i| i + 1).unwrap_or(0);
    if let Some(first) = db.first_log_index().expect("first index") {
        assert!(
            first <= base_next,
            "repair left a log starting at {first}, past the applied base {base_next}"
        );
    }

    let reachable = snap
        .into_iter()
        .chain(db.last_log_index().expect("last index"))
        .max();
    if let Some(bytes) = db.read_committed().expect("committed readable") {
        let committed: Option<LogId<u64>> =
            postcard::from_bytes(&bytes).expect("repair must leave decodable committed bytes");
        if let Some(c) = committed {
            let reach = reachable.expect("a committed pointer needs something behind it");
            assert!(
                c.index <= reach,
                "repair left a committed pointer at {} past the reachable index {reach}",
                c.index
            );
        }
    }
});
