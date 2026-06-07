use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogReader, RaftLogStorage};
use openraft::{Entry, LogId, LogState, OptionalSend, StorageError, Vote};

use super::{dec, enc, sread, swrite};
use crate::raft::TypeConfig;
use crate::storage::Storage;
use crate::types::NodeId;
use crate::RedbStore;

const KEY_PURGED: &str = "log:purged";

pub struct LogStore<S = RedbStore> {
    db: Arc<S>,
}

impl<S> Clone for LogStore<S> {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
        }
    }
}

impl<S> LogStore<S> {
    pub fn new(db: Arc<S>) -> Self {
        Self { db }
    }
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
            Bound::Unbounded => self
                .db
                .last_log_index()
                .map_err(sread)?
                .map(|i| i + 1)
                .unwrap_or(0),
        };

        let raw = self.db.read_log(start, end).map_err(sread)?;
        let mut out = Vec::with_capacity(raw.len());
        for (_, bytes) in raw {
            out.push(dec::<Entry<TypeConfig>>(&bytes)?);
        }
        Ok(out)
    }
}

impl<S: Storage> RaftLogStorage<TypeConfig> for LogStore<S> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last_purged: Option<LogId<NodeId>> = match self.db.get(KEY_PURGED).map_err(sread)? {
            Some(b) => Some(dec(&b)?),
            None => None,
        };

        let last_log_id = match self.db.last_log_index().map_err(sread)? {
            Some(i) => {
                let raw = self.db.read_log(i, i + 1).map_err(sread)?;
                match raw.into_iter().next() {
                    Some((_, bytes)) => {
                        let entry: Entry<TypeConfig> = dec(&bytes)?;
                        Some(entry.log_id)
                    }
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
        self.db.save_vote(&bytes).map_err(swrite)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        match self.db.read_vote().map_err(sread)? {
            Some(b) => Ok(Some(dec(&b)?)),
            None => Ok(None),
        }
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = enc(&committed)?;
        self.db.save_committed(&bytes).map_err(swrite)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        match self.db.read_committed().map_err(sread)? {
            Some(b) => dec::<Option<LogId<NodeId>>>(&b),
            None => Ok(None),
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
            let index = entry.log_id.index;
            batch.push((index, enc(&entry)?));
        }
        self.db.append_log(&batch).map_err(swrite)?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.db.truncate_log_from(log_id.index).map_err(swrite)
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let bytes = enc(&log_id)?;
        self.db.put(KEY_PURGED, &bytes).map_err(swrite)?;
        self.db.purge_log_upto(log_id.index).map_err(swrite)
    }
}
