#![allow(clippy::result_large_err)]

pub mod client;
pub mod engine;
pub mod error;
#[doc(hidden)]
pub mod frame;
pub mod http;
pub mod queue;
pub mod raft;
pub mod storage;
pub mod types;

pub(crate) fn lock<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) fn secret_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub use client::{serve_clients, serve_clients_authenticated, MAX_PAYLOAD_BYTES};
pub use http::serve_http;
pub use engine::{
    add_learner, build_raft, build_raft_authenticated, build_raft_partitionable, initialize_cluster,
    serve_peer, serve_peer_authenticated, set_voters, start_single_node, HermesRaft, LogStore,
    PartitionControl, PeerNetwork, StateMachineStore,
};
pub use error::{Error, Result};
pub use queue::Queue;
pub use raft::{AppRequest, AppResponse, Delivered, TypeConfig};
pub use storage::{RedbStore, Storage};
pub use types::{ContentType, GroupId, LeaseId, Message, NodeId, Offset, Priority, TopicId};
