// openraft's `StorageError` is intentionally large; the stock suite signature
// returns it by value, matching the lib's crate-wide allow.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use hermesmq_core::{LogStore, NodeId, RedbStore, StateMachineStore, TypeConfig};
use openraft::testing::{StoreBuilder, Suite};
use openraft::StorageError;

struct Builder;

impl StoreBuilder<TypeConfig, LogStore, StateMachineStore, ()> for Builder {
    async fn build(&self) -> Result<((), LogStore, StateMachineStore), StorageError<NodeId>> {
        let db = Arc::new(RedbStore::in_memory().unwrap());
        let log = LogStore::new(db.clone());
        let sm = StateMachineStore::new(db)?;
        Ok(((), log, sm))
    }
}

#[test]
fn store_conformance_suite() -> Result<(), StorageError<NodeId>> {
    Suite::test_all(Builder)
}
