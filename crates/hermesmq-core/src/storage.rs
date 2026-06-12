use std::path::Path;

use bytes::Bytes;
use redb::{Database, Durability, ReadableTable, TableDefinition};

use crate::error::{Error, Result};

const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

const KEY_VOTE: &str = "sys:vote";
const KEY_COMMITTED: &str = "sys:committed";

fn st<E: std::fmt::Display>(e: E) -> Error {
    Error::Storage(e.to_string())
}

pub trait Storage: Send + Sync + 'static {
    fn append_log(&self, entries: &[(u64, Bytes)]) -> Result<()>;
    fn read_log(&self, start: u64, end: u64) -> Result<Vec<(u64, Vec<u8>)>>;
    fn truncate_log_from(&self, index: u64) -> Result<()>;
    fn purge_log_upto(&self, index: u64) -> Result<()>;
    fn first_log_index(&self) -> Result<Option<u64>>;
    fn last_log_index(&self) -> Result<Option<u64>>;

    fn save_vote(&self, vote: &[u8]) -> Result<()>;
    fn read_vote(&self) -> Result<Option<Vec<u8>>>;
    fn save_committed(&self, committed: &[u8]) -> Result<()>;
    fn read_committed(&self) -> Result<Option<Vec<u8>>>;

    fn put(&self, key: &str, value: &[u8]) -> Result<()>;
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    fn delete(&self, key: &str) -> Result<()>;
    fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>>;
}

pub struct RedbStore {
    db: Database,
    durability: Durability,
}

impl RedbStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::create(path).map_err(st)?;
        let store = Self {
            db,
            durability: Durability::Immediate,
        };
        store.init_tables()?;
        Ok(store)
    }

    pub fn in_memory() -> Result<Self> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(st)?;
        let store = Self {
            db,
            durability: Durability::Immediate,
        };
        store.init_tables()?;
        Ok(store)
    }

    pub fn with_durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    fn init_tables(&self) -> Result<()> {
        let wtx = self.db.begin_write().map_err(st)?;
        {
            wtx.open_table(LOG).map_err(st)?;
            wtx.open_table(META).map_err(st)?;
        }
        wtx.commit().map_err(st)?;
        Ok(())
    }

    fn begin_write(&self) -> Result<redb::WriteTransaction> {
        let mut wtx = self.db.begin_write().map_err(st)?;
        wtx.set_durability(self.durability);
        Ok(wtx)
    }

    fn remove_log_keys(&self, keys: Vec<u64>) -> Result<()> {
        let wtx = self.begin_write()?;
        {
            let mut table = wtx.open_table(LOG).map_err(st)?;
            for key in keys {
                table.remove(key).map_err(st)?;
            }
        }
        wtx.commit().map_err(st)?;
        Ok(())
    }
}

impl Storage for RedbStore {
    fn append_log(&self, entries: &[(u64, Bytes)]) -> Result<()> {
        let wtx = self.begin_write()?;
        {
            let mut table = wtx.open_table(LOG).map_err(st)?;
            for (index, bytes) in entries {
                table.insert(*index, bytes.as_ref()).map_err(st)?;
            }
        }
        wtx.commit().map_err(st)?;
        Ok(())
    }

    fn read_log(&self, start: u64, end: u64) -> Result<Vec<(u64, Vec<u8>)>> {
        let rtx = self.db.begin_read().map_err(st)?;
        let table = rtx.open_table(LOG).map_err(st)?;
        let mut out = Vec::new();
        for item in table.range(start..end).map_err(st)? {
            let (k, v) = item.map_err(st)?;
            out.push((k.value(), v.value().to_vec()));
        }
        Ok(out)
    }

    fn truncate_log_from(&self, index: u64) -> Result<()> {
        let keys = {
            let rtx = self.db.begin_read().map_err(st)?;
            let table = rtx.open_table(LOG).map_err(st)?;
            let mut keys = Vec::new();
            for item in table.range(index..).map_err(st)? {
                keys.push(item.map_err(st)?.0.value());
            }
            keys
        };
        self.remove_log_keys(keys)
    }

    fn purge_log_upto(&self, index: u64) -> Result<()> {
        let keys = {
            let rtx = self.db.begin_read().map_err(st)?;
            let table = rtx.open_table(LOG).map_err(st)?;
            let mut keys = Vec::new();
            for item in table.range(..=index).map_err(st)? {
                keys.push(item.map_err(st)?.0.value());
            }
            keys
        };
        self.remove_log_keys(keys)
    }

    fn first_log_index(&self) -> Result<Option<u64>> {
        let rtx = self.db.begin_read().map_err(st)?;
        let table = rtx.open_table(LOG).map_err(st)?;
        let index = table.first().map_err(st)?.map(|(k, _)| k.value());
        Ok(index)
    }

    fn last_log_index(&self) -> Result<Option<u64>> {
        let rtx = self.db.begin_read().map_err(st)?;
        let table = rtx.open_table(LOG).map_err(st)?;
        let index = table.last().map_err(st)?.map(|(k, _)| k.value());
        Ok(index)
    }

    fn save_vote(&self, vote: &[u8]) -> Result<()> {
        self.put(KEY_VOTE, vote)
    }

    fn read_vote(&self) -> Result<Option<Vec<u8>>> {
        self.get(KEY_VOTE)
    }

    fn save_committed(&self, committed: &[u8]) -> Result<()> {
        let mut wtx = self.db.begin_write().map_err(st)?;
        wtx.set_durability(Durability::Eventual);
        {
            let mut table = wtx.open_table(META).map_err(st)?;
            table.insert(KEY_COMMITTED, committed).map_err(st)?;
        }
        wtx.commit().map_err(st)?;
        Ok(())
    }

    fn read_committed(&self) -> Result<Option<Vec<u8>>> {
        self.get(KEY_COMMITTED)
    }

    fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        let wtx = self.begin_write()?;
        {
            let mut table = wtx.open_table(META).map_err(st)?;
            table.insert(key, value).map_err(st)?;
        }
        wtx.commit().map_err(st)?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let rtx = self.db.begin_read().map_err(st)?;
        let table = rtx.open_table(META).map_err(st)?;
        Ok(table.get(key).map_err(st)?.map(|v| v.value().to_vec()))
    }

    fn delete(&self, key: &str) -> Result<()> {
        let wtx = self.begin_write()?;
        {
            let mut table = wtx.open_table(META).map_err(st)?;
            table.remove(key).map_err(st)?;
        }
        wtx.commit().map_err(st)?;
        Ok(())
    }

    fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let rtx = self.db.begin_read().map_err(st)?;
        let table = rtx.open_table(META).map_err(st)?;
        let mut out = Vec::new();
        for item in table.iter().map_err(st)? {
            let (k, v) = item.map_err(st)?;
            let key = k.value().to_string();
            if key.starts_with(prefix) {
                out.push((key, v.value().to_vec()));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_append_read_first_last() {
        let s = RedbStore::in_memory().unwrap();
        s.append_log(&[
            (1, Bytes::from_static(b"a")),
            (2, Bytes::from_static(b"b")),
            (3, Bytes::from_static(b"c")),
        ])
        .unwrap();

        assert_eq!(s.first_log_index().unwrap(), Some(1));
        assert_eq!(s.last_log_index().unwrap(), Some(3));

        let r = s.read_log(1, 3).unwrap();
        assert_eq!(r, vec![(1, b"a".to_vec()), (2, b"b".to_vec())]);
    }

    #[test]
    fn truncate_and_purge() {
        let s = RedbStore::in_memory().unwrap();
        for i in 1..=5u64 {
            s.append_log(&[(i, Bytes::from(vec![i as u8]))]).unwrap();
        }

        s.truncate_log_from(4).unwrap();
        assert_eq!(s.last_log_index().unwrap(), Some(3));

        s.purge_log_upto(1).unwrap();
        assert_eq!(s.first_log_index().unwrap(), Some(2));

        let r = s.read_log(0, 100).unwrap();
        assert_eq!(r, vec![(2, vec![2]), (3, vec![3])]);
    }

    #[test]
    fn vote_committed_roundtrip() {
        let s = RedbStore::in_memory().unwrap();
        assert_eq!(s.read_vote().unwrap(), None);
        s.save_vote(b"vote-1").unwrap();
        assert_eq!(s.read_vote().unwrap(), Some(b"vote-1".to_vec()));

        s.save_committed(b"c-1").unwrap();
        assert_eq!(s.read_committed().unwrap(), Some(b"c-1".to_vec()));
    }

    #[test]
    fn meta_put_get_scan_delete() {
        let s = RedbStore::in_memory().unwrap();
        s.put("off:t:g", b"42").unwrap();
        s.put("off:t:h", b"7").unwrap();
        s.put("other", b"x").unwrap();

        assert_eq!(s.get("off:t:g").unwrap(), Some(b"42".to_vec()));

        let mut scan = s.scan_prefix("off:").unwrap();
        scan.sort();
        assert_eq!(
            scan,
            vec![
                ("off:t:g".to_string(), b"42".to_vec()),
                ("off:t:h".to_string(), b"7".to_vec()),
            ]
        );

        s.delete("off:t:g").unwrap();
        assert_eq!(s.get("off:t:g").unwrap(), None);
    }
}
