use std::sync::Arc;
use std::time::Duration;

use hermesmq_core::engine::{build_raft, initialize_cluster};
use hermesmq_core::{serve_http, RedbStore};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn get(addr: std::net::SocketAddr, path: &str) -> (String, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let status = response.lines().next().unwrap_or("").to_string();
    let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

async fn start_node() -> (hermesmq_core::HermesRaft, hermesmq_core::StateMachineStore) {
    let db = Arc::new(RedbStore::in_memory().unwrap());
    let (raft, sm) = build_raft(1, db).await.unwrap();
    initialize_cluster(&raft, &[(1, "127.0.0.1:9".to_string())])
        .await
        .unwrap();
    for _ in 0..200 {
        if raft.current_leader().await.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (raft, sm)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_health_ready_metrics() {
    let (raft, sm) = start_node().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_http(raft.clone(), sm, listener, true));

    let (status, body) = get(addr, "/health").await;
    assert!(status.contains("200"), "health status: {status}");
    assert!(body.contains("ok"));

    let (status, _) = get(addr, "/ready").await;
    assert!(status.contains("200"), "ready status: {status}");

    let (status, body) = get(addr, "/metrics").await;
    assert!(status.contains("200"), "metrics status: {status}");
    assert!(body.contains("hermesmq_raft_is_leader"), "metrics body: {body}");
    assert!(body.contains("hermesmq_topics"));

    let (status, _) = get(addr, "/nope").await;
    assert!(status.contains("404"), "unknown path status: {status}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_metrics_can_be_disabled() {
    let (raft, sm) = start_node().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_http(raft.clone(), sm, listener, false));

    let (status, body) = get(addr, "/metrics").await;
    assert!(status.contains("404"), "disabled metrics status: {status}");
    assert!(body.contains("metrics disabled"));

    let (status, _) = get(addr, "/health").await;
    assert!(status.contains("200"), "health must stay available: {status}");

    let (status, _) = get(addr, "/ready").await;
    assert!(status.contains("200"), "ready must stay available: {status}");
}
