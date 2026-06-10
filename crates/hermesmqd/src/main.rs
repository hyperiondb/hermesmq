use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use hermesmq_core::{build_raft, serve_clients, serve_http, serve_peer, RedbStore};
use tokio::net::TcpListener;

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

    std::fs::create_dir_all(&cli.data_dir)?;
    let db_path = cli.data_dir.join("hermesmq.redb");
    let db = Arc::new(RedbStore::open(&db_path)?);

    let (raft, sm) = build_raft(cli.node_id, db)
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
        data_dir = %cli.data_dir.display(),
        "hermesmqd listening (waiting for client bootstrap)"
    );

    tokio::spawn(serve_peer(raft.clone(), peer_listener));
    tokio::spawn(serve_clients(raft.clone(), sm.clone(), client_listener));
    tokio::spawn(serve_http(raft.clone(), sm, metrics_listener));

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown signal received; stopping raft node");
    raft.shutdown().await?;
    Ok(())
}
