use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use openraft::storage::{LogFlushed, RaftLogReader, RaftLogStorage};
use openraft::{Entry, LogId, LogState, OptionalSend, StorageError, Vote};
use tokio::sync::{mpsc, oneshot};

use super::{dec, enc, sread, swrite};
use crate::raft::TypeConfig;
use crate::storage::Storage;
use crate::types::NodeId;
use crate::RedbStore;

pub(crate) const KEY_PURGED: &str = "log:purged";
const FLUSH_MAX_ENTRIES: usize = 512;
const FLUSH_MAX_BYTES: usize = 8 * 1024 * 1024;

enum FlushJob {
    Append(Vec<(u64, Bytes)>, LogFlushed<TypeConfig>),
    Barrier(oneshot::Sender<()>),
}

struct Shared<S> {
    db: Arc<S>,
    pending: Mutex<BTreeMap<u64, Bytes>>,
}

pub struct LogStore<S = RedbStore> {
    shared: Arc<Shared<S>>,
    jobs: mpsc::UnboundedSender<FlushJob>,
}

impl<S> Clone for LogStore<S> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            jobs: self.jobs.clone(),
        }
    }
}

impl<S: Storage> LogStore<S> {
    pub fn new(db: Arc<S>) -> Self {
        let shared = Arc::new(Shared {
            db,
            pending: Mutex::new(BTreeMap::new()),
        });
        let (jobs, rx) = mpsc::unbounded_channel();
        let flusher = Arc::clone(&shared);
        std::thread::spawn(move || run_flusher(flusher, rx));
        Self { shared, jobs }
    }

    async fn barrier(&self) {
        let (tx, rx) = oneshot::channel();
        if self.jobs.send(FlushJob::Barrier(tx)).is_ok() {
            let _ = rx.await;
        }
    }

    fn pending_last(&self) -> Option<u64> {
        self.shared
            .pending
            .lock()
            .unwrap()
            .keys()
            .next_back()
            .copied()
    }
}

fn run_flusher<S: Storage>(shared: Arc<Shared<S>>, mut rx: mpsc::UnboundedReceiver<FlushJob>) {
    while let Some(first) = rx.blocking_recv() {
        let mut batch: Vec<(u64, Bytes)> = Vec::new();
        let mut callbacks = Vec::new();
        let mut barriers = Vec::new();
        let mut bytes = 0usize;
        let mut job = Some(first);
        loop {
            match job {
                Some(FlushJob::Append(entries, callback)) => {
                    bytes += entries.iter().map(|(_, b)| b.len()).sum::<usize>();
                    batch.extend(entries);
                    callbacks.push(callback);
                }
                Some(FlushJob::Barrier(done)) => {
                    barriers.push(done);
                    break;
                }
                None => break,
            }
            if batch.len() >= FLUSH_MAX_ENTRIES || bytes >= FLUSH_MAX_BYTES {
                break;
            }
            job = rx.try_recv().ok();
        }

        let result = if batch.is_empty() {
            Ok(())
        } else {
            shared.db.append_log(&batch)
        };
        match result {
            Ok(()) => {
                if !batch.is_empty() {
                    let mut pending = shared.pending.lock().unwrap();
                    for (index, _) in &batch {
                        pending.remove(index);
                    }
                }
                for callback in callbacks {
                    callback.log_io_completed(Ok(()));
                }
            }
            Err(e) => {
                let msg = e.to_string();
                for callback in callbacks {
                    callback.log_io_completed(Err(std::io::Error::other(msg.clone())));
                }
            }
        }
        for done in barriers {
            let _ = done.send(());
        }
    }
}

pub(crate) fn mark_purged<S: Storage>(
    db: &S,
    upto: &LogId<NodeId>,
) -> Result<(), StorageError<NodeId>> {
    if let Some(bytes) = db.get(KEY_PURGED).map_err(sread)? {
        let current: LogId<NodeId> = dec(&bytes)?;
        if current.index >= upto.index {
            return Ok(());
        }
    }
    db.put(KEY_PURGED, &enc(upto)?).map_err(swrite)?;
    db.purge_log_upto(upto.index).map_err(swrite)
}

impl<S: Storage> RaftLogReader<TypeConfig> for LogStore<S> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let start = match range.start_bound() {
            Bound::Included(x) => *x,
            Bound::Excluded(x) => *x + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(x) => *x + 1,
            Bound::Excluded(x) => *x,
            Bound::Unbounded => {
                let db_last = self.shared.db.last_log_index().map_err(sread)?;
                db_last
                    .into_iter()
                    .chain(self.pending_last())
                    .max()
                    .map(|i| i + 1)
                    .unwrap_or(0)
            }
        };

        let mut merged: BTreeMap<u64, Bytes> = BTreeMap::new();
        for (index, bytes) in self.shared.db.read_log(start, end).map_err(sread)? {
            merged.insert(index, Bytes::from(bytes));
        }
        {
            let pending = self.shared.pending.lock().unwrap();
            for (index, bytes) in pending.range(start..end) {
                merged.insert(*index, bytes.clone());
            }
        }
        let mut out = Vec::with_capacity(merged.len());
        for bytes in merged.values() {
            out.push(dec::<Entry<TypeConfig>>(bytes)?);
        }
        Ok(out)
    }
}

impl<S: Storage> RaftLogStorage<TypeConfig> for LogStore<S> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last_purged: Option<LogId<NodeId>> = match self.shared.db.get(KEY_PURGED).map_err(sread)? {
            Some(b) => Some(dec(&b)?),
            None => None,
        };

        let db_last = self.shared.db.last_log_index().map_err(sread)?;
        let last_index = db_last.into_iter().chain(self.pending_last()).max();
        let last_log_id = match last_index {
            Some(index) => {
                let bytes = {
                    let pending = self.shared.pending.lock().unwrap();
                    pending.get(&index).cloned()
                };
                let bytes = match bytes {
                    Some(b) => Some(b),
                    None => self
                        .shared
                        .db
                        .read_log(index, index + 1)
                        .map_err(sread)?
                        .into_iter()
                        .next()
                        .map(|(_, b)| Bytes::from(b)),
                };
                match bytes {
                    Some(b) => Some(dec::<Entry<TypeConfig>>(&b)?.log_id),
                    None => last_purged,
                }
            }
            None => last_purged,
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let bytes = enc(vote)?;
        self.shared.db.save_vote(&bytes).map_err(swrite)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        match self.shared.db.read_vote().map_err(sread)? {
            Some(b) => Ok(Some(dec(&b)?)),
            None => Ok(None),
        }
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = enc(&committed)?;
        self.shared.db.save_committed(&bytes).map_err(swrite)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let stored: Option<LogId<NodeId>> = match self.shared.db.read_committed().map_err(sread)? {
            Some(b) => dec(&b)?,
            None => return Ok(None),
        };
        let Some(committed) = stored else { return Ok(None) };

        // Never advertise a commit the log can't serve. If `committed` points
        // past the last persisted entry, the tail was lost; returning it would
        // make openraft re-apply `(last_applied .. committed]` over entries that
        // aren't on disk and abort startup with "Failed to get log entries".
        // The committed pointer is a recoverable optimization: a follower
        // re-learns it from the leader, and a node that re-wins leadership
        // re-commits its own log.
        let last = self
            .shared
            .db
            .last_log_index()
            .map_err(sread)?
            .into_iter()
            .chain(self.pending_last())
            .max();
        match last {
            Some(last_index) if committed.index <= last_index => Ok(Some(committed)),
            _ => Ok(None),
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut batch = Vec::new();
        for entry in entries {
            batch.push((entry.log_id.index, Bytes::from(enc(&entry)?)));
        }
        {
            let mut pending = self.shared.pending.lock().unwrap();
            for (index, bytes) in &batch {
                pending.insert(*index, bytes.clone());
            }
        }
        if self.jobs.send(FlushJob::Append(batch, callback)).is_err() {
            return Err(swrite("log flusher thread is gone"));
        }
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.barrier().await;
        self.shared
            .pending
            .lock()
            .unwrap()
            .split_off(&log_id.index);
        self.shared
            .db
            .truncate_log_from(log_id.index)
            .map_err(swrite)
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.barrier().await;
        {
            let mut pending = self.shared.pending.lock().unwrap();
            *pending = pending.split_off(&(log_id.index + 1));
        }
        mark_purged(self.shared.db.as_ref(), &log_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::CommittedLeaderId;

    fn log_id(index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(3615, 3), index)
    }

    #[tokio::test]
    async fn read_committed_drops_a_pointer_past_the_available_log() {
        let db = Arc::new(RedbStore::in_memory().unwrap());
        db.save_committed(&enc(&Some(log_id(196780))).unwrap()).unwrap();
        let mut log = LogStore::new(db);
        assert_eq!(
            log.read_committed().await.unwrap(),
            None,
            "a committed pointer with no backing log must not be trusted"
        );
    }

    #[tokio::test]
    async fn read_committed_keeps_a_pointer_covered_by_the_log() {
        let db = Arc::new(RedbStore::in_memory().unwrap());
        db.append_log(&[(1, Bytes::from_static(b"a")), (2, Bytes::from_static(b"b"))])
            .unwrap();
        db.save_committed(&enc(&Some(log_id(2))).unwrap()).unwrap();
        let mut log = LogStore::new(db);
        assert_eq!(log.read_committed().await.unwrap(), Some(log_id(2)));
    }

    #[tokio::test]
    async fn build_raft_starts_despite_committed_past_a_lost_log() {
        let db = Arc::new(RedbStore::in_memory().unwrap());
        db.save_committed(&enc(&Some(log_id(196780))).unwrap()).unwrap();
        let (raft, _sm) = super::super::build_raft(3, db).await.unwrap();
        raft.shutdown().await.unwrap();
    }
}
