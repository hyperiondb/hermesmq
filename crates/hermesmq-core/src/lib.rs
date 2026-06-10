#![allow(clippy::result_large_err)]

pub mod client;
pub mod engine;
pub mod error;
mod frame;
pub mod http;
pub mod queue;
pub mod raft;
pub mod storage;
pub mod types;

pub use client::{serve_clients, MAX_PAYLOAD_BYTES};
pub use http::serve_http;
pub use engine::{
    add_learner, build_raft, build_raft_partitionable, initialize_cluster, serve_peer, set_voters,
    start_single_node, HermesRaft, LogStore, PartitionControl, PeerNetwork, StateMachineStore,
};
pub use error::{Error, Result};
pub use queue::Queue;
pub use raft::{AppRequest, AppResponse, Delivered, TypeConfig};
pub use storage::{RedbStore, Storage};
pub use types::{ContentType, GroupId, LeaseId, Message, NodeId, Offset, Priority, TopicId};
