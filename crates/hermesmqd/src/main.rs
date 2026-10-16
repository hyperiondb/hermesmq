use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use hermesmq_core::{
    build_raft_authenticated, serve_clients_authenticated, serve_http, serve_peer_authenticated,
    RedbStore,
};
use tokio::net::TcpListener;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "hermesmqd", version, about = "Raft-replicated message queue")]
struct Cli {
    #[arg(long, env = "HERMESMQ_NODE_ID", default_value_t = 1)]
    node_id: u64,

    #[arg(long, env = "HERMESMQ_DATA_DIR", default_value = "data")]
    data_dir: PathBuf,

    #[arg(long, env = "HERMESMQ_CLIENT_ADDR", default_value = "127.0.0.1:7600")]
    client_addr: SocketAddr,

    #[arg(long, env = "HERMESMQ_PEER_ADDR", default_value = "127.0.0.1:7700")]
    peer_addr: SocketAddr,

    #[arg(long, env = "HERMESMQ_METRICS_ADDR", default_value = "127.0.0.1:9600")]
    metrics_addr: SocketAddr,

    #[arg(
        long,
        env = "HERMESMQ_METRICS_ENABLED",
        default_value_t = true,
        action = clap::ArgAction::Set
    )]
    metrics_enabled: bool,

    /// Shared secret every client connection must present before it is served.
    /// Unset leaves the client port open to anyone who can reach it.
    #[arg(long, env = "HERMESMQ_CLIENT_TOKEN")]
    client_token: Option<String>,

    /// Shared secret every peer connection must present, in both directions.
    /// Set the same value on every node in the cluster.
    #[arg(long, env = "HERMESMQ_PEER_TOKEN")]
    peer_token: Option<String>,
}

#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate())?;
    tokio::select! {
        r = tokio::signal::ctrl_c() => r,
        _ = term.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    // A variable a compose file leaves unset arrives as an empty string,
    // which must mean "no token", not "the empty token".
    let client_token = cli.client_token.clone().filter(|t| !t.is_empty());
    let peer_token = cli.peer_token.clone().filter(|t| !t.is_empty());

    std::fs::create_dir_all(&cli.data_dir)?;
    let db_path = cli.data_dir.join("hermesmq.redb");
    let db = Arc::new(RedbStore::open(&db_path)?);

    let (raft, sm) = build_raft_authenticated(cli.node_id, db, peer_token.clone())
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let peer_listener = TcpListener::bind(cli.peer_addr).await?;
    let client_listener = TcpListener::bind(cli.client_addr).await?;
    let metrics_listener = TcpListener::bind(cli.metrics_addr).await?;
    tracing::info!(
        node_id = cli.node_id,
        peer = %cli.peer_addr,
        client = %cli.client_addr,
        metrics = %cli.metrics_addr,
        metrics_enabled = cli.metrics_enabled,
        client_auth = client_token.is_some(),
        peer_auth = peer_token.is_some(),
        data_dir = %cli.data_dir.display(),
        "hermesmqd listening (waiting for client bootstrap)"
    );

    tokio::spawn(serve_peer_authenticated(
        raft.clone(),
        peer_listener,
        peer_token,
    ));
    tokio::spawn(serve_clients_authenticated(
        raft.clone(),
        sm.clone(),
        client_listener,
        client_token,
    ));
    tokio::spawn(serve_http(raft.clone(), sm, metrics_listener, cli.metrics_enabled));

    shutdown_signal().await?;
    tracing::info!("shutdown signal received; stopping raft node");
    raft.shutdown().await?;
    Ok(())
}
