use std::net::SocketAddr;
use std::path::PathBuf;

use crate::types::NodeId;

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: NodeId,
    pub data_dir: PathBuf,
    pub client_addr: SocketAddr,
    pub peer_addr: SocketAddr,
}
