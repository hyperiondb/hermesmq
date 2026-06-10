use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::engine::HermesRaft;
use crate::engine::StateMachineStore;

pub async fn serve_http(
    raft: HermesRaft,
    sm: StateMachineStore,
    listener: TcpListener,
    metrics_enabled: bool,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let raft = raft.clone();
                let sm = sm.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle(raft, sm, stream, metrics_enabled).await {
                        tracing::debug!("http connection closed: {e}");
                    }
                });
            }
            Err(e) => tracing::warn!("http accept error: {e}"),
        }
    }
}

async fn handle(
    raft: HermesRaft,
    sm: StateMachineStore,
    mut stream: TcpStream,
    metrics_enabled: bool,
) -> io::Result<()> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(2).any(|w| w == b"\r\n") || buf.len() >= 8192 {
            break;
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let request_line = head.lines().next().unwrap_or("");
    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .split('?')
        .next()
        .unwrap_or("/")
        .to_string();

    let (status, content_type, body) = route(&raft, &sm, &path, metrics_enabled);
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

fn route(
    raft: &HermesRaft,
    sm: &StateMachineStore,
    path: &str,
    metrics_enabled: bool,
) -> (&'static str, &'static str, String) {
    match path {
        "/health" => ("200 OK", "text/plain", "ok\n".to_string()),
        "/ready" => {
            let metrics = raft.metrics();
            let ready = metrics.borrow().current_leader.is_some();
            if ready {
                ("200 OK", "text/plain", "ready\n".to_string())
            } else {
                ("503 Service Unavailable", "text/plain", "not ready\n".to_string())
            }
        }
        "/metrics" if !metrics_enabled => {
            ("404 Not Found", "text/plain", "metrics disabled\n".to_string())
        }
        "/metrics" => ("200 OK", "text/plain; version=0.0.4", metrics_text(raft, sm)),
        _ => ("404 Not Found", "text/plain", "not found\n".to_string()),
    }
}

fn metrics_text(raft: &HermesRaft, sm: &StateMachineStore) -> String {
    let metrics = raft.metrics();
    let m = metrics.borrow();
    let last_applied = m.last_applied.as_ref().map(|l| l.index).unwrap_or(0);
    let last_log_index = m.last_log_index.unwrap_or(0);
    let current_leader = m.current_leader.unwrap_or(0);
    let is_leader = i32::from(m.current_leader == Some(m.id));
    let term = m.current_term;
    let quorum_ack_ms = m.millis_since_quorum_ack.unwrap_or(0);
    let node_id = m.id;
    drop(m);
    let q = sm.metrics();
    format!(
        "# HELP hermesmq_raft_term Current Raft term\n\
         # TYPE hermesmq_raft_term gauge\n\
         hermesmq_raft_term{{node=\"{node_id}\"}} {term}\n\
         # HELP hermesmq_raft_is_leader 1 if this node is the leader\n\
         # TYPE hermesmq_raft_is_leader gauge\n\
         hermesmq_raft_is_leader{{node=\"{node_id}\"}} {is_leader}\n\
         # HELP hermesmq_raft_current_leader Current leader node id (0 if none)\n\
         # TYPE hermesmq_raft_current_leader gauge\n\
         hermesmq_raft_current_leader{{node=\"{node_id}\"}} {current_leader}\n\
         # HELP hermesmq_raft_last_log_index Last log index appended\n\
         # TYPE hermesmq_raft_last_log_index gauge\n\
         hermesmq_raft_last_log_index{{node=\"{node_id}\"}} {last_log_index}\n\
         # HELP hermesmq_raft_last_applied Last log index applied to the state machine\n\
         # TYPE hermesmq_raft_last_applied gauge\n\
         hermesmq_raft_last_applied{{node=\"{node_id}\"}} {last_applied}\n\
         # HELP hermesmq_raft_millis_since_quorum_ack Replication lag: ms since last quorum ack (leader only)\n\
         # TYPE hermesmq_raft_millis_since_quorum_ack gauge\n\
         hermesmq_raft_millis_since_quorum_ack{{node=\"{node_id}\"}} {quorum_ack_ms}\n\
         # HELP hermesmq_topics Number of topics\n\
         # TYPE hermesmq_topics gauge\n\
         hermesmq_topics{{node=\"{node_id}\"}} {}\n\
         # HELP hermesmq_messages Number of retained messages across topics\n\
         # TYPE hermesmq_messages gauge\n\
         hermesmq_messages{{node=\"{node_id}\"}} {}\n\
         # HELP hermesmq_in_flight Number of leased (in-flight) messages\n\
         # TYPE hermesmq_in_flight gauge\n\
         hermesmq_in_flight{{node=\"{node_id}\"}} {}\n",
        q.topics, q.messages, q.in_flight
    )
}
