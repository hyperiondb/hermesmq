#![no_main]

use std::collections::BTreeMap;
use std::sync::Arc;

use arbitrary::Arbitrary;
use hermesmq_core::raft::TypeConfig;
use hermesmq_core::{LogStore, RedbStore, Storage};
use hermesmq_fuzz::MemStore;
use libfuzzer_sys::fuzz_target;
use openraft::storage::{RaftLogReader, RaftLogStorage};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

#[derive(Arbitrary, Debug)]
enum Step {
    Append(Vec<u8>),
    Truncate(u8),
    Purge(u8),
    Read(u8, u8),
    State,
    Flush,
}

fn log_id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(1, 1), index)
}

fn entry(index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(index),
        payload: EntryPayload::Blank,
    }
}

async fn drive<S: Storage>(db: Arc<S>, steps: &[Step]) {
    let mut log = LogStore::new(db);
    let mut model: BTreeMap<u64, LogId<u64>> = BTreeMap::new();
    let mut purged: Option<LogId<u64>> = None;

    for step in steps.iter().take(48) {
        match step {
            Step::Append(indices) => {
                let entries: Vec<Entry<TypeConfig>> =
                    indices.iter().take(16).map(|i| entry(*i as u64)).collect();
                log.fuzz_append(&entries).expect("append must be accepted");
                for e in &entries {
                    model.insert(e.log_id.index, e.log_id);
                }
            }
            Step::Truncate(index) => {
                let index = *index as u64;
                log.truncate(log_id(index)).await.expect("truncate");
                model.retain(|k, _| *k < index);
            }
            Step::Purge(index) => {
                let index = *index as u64;
                log.purge(log_id(index)).await.expect("purge");
                model.retain(|k, _| *k > index);
                if purged.map(|p| p.index < index).unwrap_or(true) {
                    purged = Some(log_id(index));
                }
            }
            Step::Read(lo, hi) => {
                let (lo, hi) = (*lo as u64, *hi as u64);
                if lo >= hi {
                    continue;
                }
                let got = log.try_get_log_entries(lo..hi).await.expect("read");
                let want: Vec<u64> = model.range(lo..hi).map(|(k, _)| *k).collect();
                let got: Vec<u64> = got.into_iter().map(|e| e.log_id.index).collect();
                assert_eq!(got, want, "read of {lo}..{hi} did not match the model");
            }
            Step::State => {
                let state = log.get_log_state().await.expect("state");
                assert_eq!(
                    state.last_purged_log_id, purged,
                    "last_purged_log_id did not match the model"
                );
                let want = model.values().next_back().copied().or(purged);
                assert_eq!(state.last_log_id, want, "last_log_id did not match the model");
            }
            Step::Flush => {
                log.fuzz_barrier().await;
                assert!(
                    log.fuzz_pending_indices().is_empty(),
                    "entries were still pending after a barrier"
                );
            }
        }
    }

    // Drop every handle so the flusher channel closes and its OS thread exits;
    // otherwise a long run accumulates threads until it dies for its own reasons.
    drop(log);
}

// The log store merges an in-memory pending map with whatever the background
// flusher thread has already written. That merge has to look exactly like one
// ordered map to openraft, whether or not a flush is in flight. Half the inputs
// A small share of the inputs runs against the real redb store and the rest
// against a plain map: creating a redb instance costs about a hundred times more
// than the whole rest of one iteration, so an even split would starve the
// merge logic this target exists to exercise.
fuzz_target!(|input: (u8, Vec<Step>)| {
    let (selector, steps) = input;
    let use_redb = selector < 8;
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    rt.block_on(async {
        if use_redb {
            drive(Arc::new(RedbStore::in_memory().expect("in-memory redb")), &steps).await;
        } else {
            drive(Arc::new(MemStore::default()), &steps).await;
        }
    });
});
