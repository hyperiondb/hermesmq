use std::collections::HashSet;
use std::io;
use std::sync::{Arc, Mutex};

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};

use super::HermesRaft;
use crate::frame::{read_frame, to_io, write_frame};
use crate::raft::TypeConfig;
use crate::types::NodeId;

#[derive(Serialize, Deserialize)]
enum PeerRequest {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<NodeId>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
}

#[derive(Serialize, Deserialize)]
enum PeerResponse {
    AppendEntries(Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>),
    Vote(Result<VoteResponse<NodeId>, RaftError<NodeId>>),
    InstallSnapshot(Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>),
}

pub async fn serve_peer(raft: HermesRaft, listener: TcpListener) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let raft = raft.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_conn(raft, stream).await {
                        tracing::debug!("peer connection closed: {e}");
                    }
                });
            }
            Err(e) => tracing::warn!("peer accept error: {e}"),
        }
    }
}

async fn handle_conn(raft: HermesRaft, mut stream: TcpStream) -> io::Result<()> {
    loop {
        let req_bytes = read_frame(&mut stream).await?;
        let req: PeerRequest = postcard::from_bytes(&req_bytes).map_err(to_io)?;
        let resp = match req {
            PeerRequest::AppendEntries(r) => PeerResponse::AppendEntries(raft.append_entries(r).await),
            PeerRequest::Vote(r) => PeerResponse::Vote(raft.vote(r).await),
            PeerRequest::InstallSnapshot(r) => {
                PeerResponse::InstallSnapshot(raft.install_snapshot(r).await)
            }
        };
        let resp_bytes = postcard::to_stdvec(&resp).map_err(to_io)?;
        write_frame(&mut stream, &resp_bytes).await?;
    }
}

pub type PartitionControl = Arc<Mutex<HashSet<NodeId>>>;

#[derive(Clone, Default)]
pub struct PeerNetwork {
    blocked: PartitionControl,
}

impl PeerNetwork {
    pub fn blocked_handle(&self) -> PartitionControl {
        self.blocked.clone()
    }
}

impl RaftNetworkFactory<TypeConfig> for PeerNetwork {
    type Network = PeerConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        PeerConnection {
            target,
            addr: node.addr.clone(),
            blocked: self.blocked.clone(),
        }
    }
}

pub struct PeerConnection {
    target: NodeId,
    addr: String,
    blocked: PartitionControl,
}

impl PeerConnection {
    async fn call(&self, req: PeerRequest) -> Result<PeerResponse, Unreachable> {
        if self.blocked.lock().unwrap().contains(&self.target) {
            return Err(Unreachable::new(&to_io("peer is partitioned")));
        }
        let mut stream = TcpStream::connect(&self.addr)
            .await
            .map_err(|e| Unreachable::new(&e))?;
        let bytes = postcard::to_stdvec(&req).map_err(|e| Unreachable::new(&to_io(e)))?;
        write_frame(&mut stream, &bytes)
            .await
            .map_err(|e| Unreachable::new(&e))?;
        let resp_bytes = read_frame(&mut stream)
            .await
            .map_err(|e| Unreachable::new(&e))?;
        postcard::from_bytes(&resp_bytes).map_err(|e| Unreachable::new(&to_io(e)))
    }
}

fn variant_mismatch() -> io::Error {
    to_io("peer returned a response for a different RPC")
}

impl RaftNetwork<TypeConfig> for PeerConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        match self.call(PeerRequest::AppendEntries(rpc)).await? {
            PeerResponse::AppendEntries(res) => {
                res.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            _ => Err(RPCError::Network(NetworkError::new(&variant_mismatch()))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        match self.call(PeerRequest::Vote(rpc)).await? {
            PeerResponse::Vote(res) => {
                res.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            _ => Err(RPCError::Network(NetworkError::new(&variant_mismatch()))),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        match self.call(PeerRequest::InstallSnapshot(rpc)).await? {
            PeerResponse::InstallSnapshot(res) => {
                res.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            _ => Err(RPCError::Network(NetworkError::new(&variant_mismatch()))),
        }
    }
}
