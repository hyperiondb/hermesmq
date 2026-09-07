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
use tokio::sync::Semaphore;

use super::HermesRaft;
use crate::frame::{read_frame, read_frame_deadline, to_io, write_frame, BODY_TIMEOUT, MAX_PEER_FRAME};
use crate::raft::TypeConfig;
use crate::types::NodeId;

/// Peer connections are long-lived and one per peer, so this only ever bites an
/// address that is opening sockets it does not use.
const MAX_PEER_CONNS: usize = 256;

/// How long a peer has to answer the token handshake.
const AUTH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Serialize, Deserialize)]
enum PeerRequest {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<NodeId>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
    // Appended, never reordered: postcard encodes the variant by index, so a
    // node running an older build must keep decoding the first three.
    Auth(String),
}

#[derive(Serialize, Deserialize)]
enum PeerResponse {
    AppendEntries(Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>),
    Vote(Result<VoteResponse<NodeId>, RaftError<NodeId>>),
    InstallSnapshot(Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>),
    Auth(bool),
}

/// Decode of a peer RPC frame, exposed so the fuzz target does not need a socket.
#[doc(hidden)]
pub fn fuzz_decode_peer_request(bytes: &[u8]) -> bool {
    postcard::from_bytes::<PeerRequest>(bytes).is_ok()
}

/// Decode of a peer RPC response frame (what a client node accepts back).
#[doc(hidden)]
pub fn fuzz_decode_peer_response(bytes: &[u8]) -> bool {
    postcard::from_bytes::<PeerResponse>(bytes).is_ok()
}

/// Serve the peer RPC port with no authentication: any address that can reach
/// it can append entries, vote and install snapshots.
pub async fn serve_peer(raft: HermesRaft, listener: TcpListener) {
    serve_peer_authenticated(raft, listener, None).await
}

/// Serve the peer RPC port, requiring `token` on every connection when one is
/// set. A connection stays unauthenticated until it sends the right token, and
/// is closed on the first RPC it tries before that.
pub async fn serve_peer_authenticated(
    raft: HermesRaft,
    listener: TcpListener,
    token: Option<String>,
) {
    if token.is_none() {
        tracing::warn!("peer port has no token: any host that can reach it can join the cluster");
    }
    let token = Arc::new(token);
    let permits = Arc::new(Semaphore::new(MAX_PEER_CONNS));
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    tracing::warn!("peer connection from {addr} refused: {MAX_PEER_CONNS} already open");
                    continue;
                };
                let _ = stream.set_nodelay(true);
                let raft = raft.clone();
                let token = token.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_conn(raft, stream, token.as_ref().as_deref()).await {
                        tracing::debug!("peer connection closed: {e}");
                    }
                    drop(permit);
                });
            }
            Err(e) => tracing::warn!("peer accept error: {e}"),
        }
    }
}

async fn handle_conn(
    raft: HermesRaft,
    mut stream: TcpStream,
    token: Option<&str>,
) -> io::Result<()> {
    let mut authed = token.is_none();
    loop {
        let req_bytes = read_frame_deadline(&mut stream, MAX_PEER_FRAME, BODY_TIMEOUT).await?;
        let req: PeerRequest = postcard::from_bytes(&req_bytes).map_err(to_io)?;
        if let PeerRequest::Auth(offered) = &req {
            authed = match token {
                Some(expected) => crate::secret_eq(offered.as_bytes(), expected.as_bytes()),
                None => true,
            };
            let bytes = postcard::to_stdvec(&PeerResponse::Auth(authed)).map_err(to_io)?;
            write_frame(&mut stream, &bytes).await?;
            if !authed {
                return Err(to_io("peer offered the wrong token"));
            }
            continue;
        }
        if !authed {
            return Err(to_io("peer sent an RPC before authenticating"));
        }
        let resp = match req {
            PeerRequest::AppendEntries(r) => PeerResponse::AppendEntries(raft.append_entries(r).await),
            PeerRequest::Vote(r) => PeerResponse::Vote(raft.vote(r).await),
            PeerRequest::InstallSnapshot(r) => {
                PeerResponse::InstallSnapshot(raft.install_snapshot(r).await)
            }
            PeerRequest::Auth(_) => unreachable!("handled above"),
        };
        let stopped = matches!(
            &resp,
            PeerResponse::AppendEntries(Err(RaftError::Fatal(_)))
                | PeerResponse::Vote(Err(RaftError::Fatal(_)))
                | PeerResponse::InstallSnapshot(Err(RaftError::Fatal(_)))
        );
        let resp_bytes = postcard::to_stdvec(&resp).map_err(to_io)?;
        write_frame(&mut stream, &resp_bytes).await?;
        if stopped {
            return Ok(());
        }
    }
}

pub type PartitionControl = Arc<Mutex<HashSet<NodeId>>>;

#[derive(Clone, Default)]
pub struct PeerNetwork {
    blocked: PartitionControl,
    token: Option<Arc<String>>,
}

impl PeerNetwork {
    /// A peer network that presents `token` on every connection it opens.
    pub fn with_token(token: Option<String>) -> Self {
        Self {
            blocked: PartitionControl::default(),
            token: token.map(Arc::new),
        }
    }

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
            token: self.token.clone(),
            stream: None,
        }
    }
}

pub struct PeerConnection {
    target: NodeId,
    addr: String,
    blocked: PartitionControl,
    token: Option<Arc<String>>,
    stream: Option<TcpStream>,
}

impl PeerConnection {
    async fn call(&mut self, req: PeerRequest) -> Result<PeerResponse, Unreachable> {
        if crate::lock(&self.blocked).contains(&self.target) {
            self.stream = None;
            return Err(Unreachable::new(&to_io("peer is partitioned")));
        }
        let bytes = postcard::to_stdvec(&req).map_err(|e| Unreachable::new(&to_io(e)))?;
        let mut stream = match self.stream.take() {
            Some(stream) => stream,
            None => {
                let mut stream = TcpStream::connect(&self.addr)
                    .await
                    .map_err(|e| Unreachable::new(&e))?;
                stream.set_nodelay(true).map_err(|e| Unreachable::new(&e))?;
                if let Some(token) = &self.token {
                    authenticate(&mut stream, token).await?;
                }
                stream
            }
        };
        write_frame(&mut stream, &bytes)
            .await
            .map_err(|e| Unreachable::new(&e))?;
        let resp_bytes = read_frame(&mut stream, MAX_PEER_FRAME)
            .await
            .map_err(|e| Unreachable::new(&e))?;
        let resp = postcard::from_bytes(&resp_bytes).map_err(|e| Unreachable::new(&to_io(e)))?;
        self.stream = Some(stream);
        Ok(resp)
    }
}

/// Present the shared token on a freshly opened peer connection.
///
/// Bounded end to end: a peer that accepts the connection and then says nothing
/// would otherwise pin this replication stream for as long as the socket lives.
async fn authenticate(stream: &mut TcpStream, token: &str) -> Result<(), Unreachable> {
    match tokio::time::timeout(AUTH_TIMEOUT, exchange_token(stream, token)).await {
        Ok(res) => res,
        Err(_) => Err(Unreachable::new(&to_io("peer did not answer the handshake"))),
    }
}

async fn exchange_token(stream: &mut TcpStream, token: &str) -> Result<(), Unreachable> {
    let bytes = postcard::to_stdvec(&PeerRequest::Auth(token.to_string()))
        .map_err(|e| Unreachable::new(&to_io(e)))?;
    write_frame(stream, &bytes)
        .await
        .map_err(|e| Unreachable::new(&e))?;
    let resp_bytes = read_frame(stream, MAX_PEER_FRAME)
        .await
        .map_err(|e| Unreachable::new(&e))?;
    match postcard::from_bytes(&resp_bytes) {
        Ok(PeerResponse::Auth(true)) => Ok(()),
        Ok(PeerResponse::Auth(false)) => Err(Unreachable::new(&to_io("peer rejected our token"))),
        Ok(_) => Err(Unreachable::new(&variant_mismatch())),
        Err(e) => Err(Unreachable::new(&to_io(e))),
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
