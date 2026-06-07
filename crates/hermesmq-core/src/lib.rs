#![allow(clippy::result_large_err)]

pub mod client;
pub mod config;
pub mod engine;
pub mod error;
mod frame;
pub mod http;
pub mod queue;
pub mod raft;
pub mod storage;
pub mod types;

pub use client::serve_clients;
pub use http::serve_http;
pub use config::NodeConfig;
pub use engine::{
    add_learner, build_raft, build_raft_partitionable, initialize_cluster, serve_peer, set_voters,
    start_single_node, HermesRaft, LogStore, PartitionControl, PeerNetwork, StateMachineStore,
};
pub use error::{Error, Result};
pub use queue::Queue;
pub use raft::{AppRequest, AppResponse, Delivered, TypeConfig};
pub use storage::{RedbStore, Storage};
pub use types::{
    AckMode, AckModeKind, ContentType, GroupId, LeaseId, Message, MessageId, NodeId, Offset,
    Priority, TopicId,
};
