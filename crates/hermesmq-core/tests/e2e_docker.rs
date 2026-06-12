use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use hermesmq_core::client::proto::{self, request, response, Request, Response};
use prost::Message;

const PROJECT: &str = "hermesmq-e2e";
const COMPOSE_FILE: &str = "docker-compose.e2e.yml";
const CLIENT_PORTS: [u16; 3] = [17600, 17601, 17602];
const METRICS_PORTS: [u16; 3] = [19600, 19601, 19602];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..").join("..")
}

fn compose(args: &[&str]) -> Output {
    let root = repo_root();
    Command::new("docker")
        .args(["compose", "-p", PROJECT, "-f", COMPOSE_FILE])
        .args(args)
        .current_dir(&root)
        .output()
        .expect("could not run `docker`; this test requires Docker with compose v2 (run it via: cargo e2e)")
}

fn compose_ok(args: &[&str]) {
    let out = compose(args);
    assert!(
        out.status.success(),
        "docker compose {args:?} failed:\n--- stdout ---\n{}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

struct ComposeGuard;

impl Drop for ComposeGuard {
    fn drop(&mut self) {
        println!("[e2e] tearing down the cluster (docker compose down)");
        let _ = compose(&["down", "--remove-orphans", "-t", "5"]);
    }
}

fn connect(port: u16) -> std::io::Result<TcpStream> {
    let stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(15)))?;
    stream.set_write_timeout(Some(Duration::from_secs(15)))?;
    Ok(stream)
}

fn call(stream: &mut TcpStream, req: &Request) -> std::io::Result<Response> {
    let bytes = req.encode_to_vec();
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(&bytes)?;
    let mut len = [0u8; 4];
    stream.read_exact(&mut len)?;
    let n = u32::from_be_bytes(len) as usize;
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf)?;
    Response::decode(buf.as_slice())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn http_get(port: u16, path: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    stream.set_write_timeout(Some(Duration::from_secs(5))).ok()?;
    write!(stream, "GET {path} HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n").ok()?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw).ok()?;
    let status: u16 = raw.split_whitespace().nth(1)?.parse().ok()?;
    let body = raw.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    Some((status, body))
}

fn wait_until(what: &str, timeout: Duration, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check() {
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!("timed out after {timeout:?} waiting for {what}");
}

fn stats(port: u16) -> Option<proto::StatsResult> {
    let mut s = connect(port).ok()?;
    let req = Request {
        kind: Some(request::Kind::Stats(proto::Stats {})),
    };
    match call(&mut s, &req).ok()?.kind {
        Some(response::Kind::Stats(st)) => Some(st),
        _ => None,
    }
}

fn leader() -> Option<(u64, u16)> {
    CLIENT_PORTS.iter().enumerate().find_map(|(i, port)| {
        let st = stats(*port)?;
        st.is_leader.then_some(((i + 1) as u64, *port))
    })
}

fn wait_for_leader(exclude: Option<u64>) -> (u64, u16) {
    let mut found = None;
    wait_until("a cluster leader", Duration::from_secs(60), || {
        found = leader().filter(|(id, _)| Some(*id) != exclude);
        found.is_some()
    });
    found.unwrap()
}

fn request_leader(req: &Request) -> Response {
    for _ in 0..60 {
        let (_, port) = wait_for_leader(None);
        let Ok(mut s) = connect(port) else {
            std::thread::sleep(Duration::from_millis(250));
            continue;
        };
        let Ok(resp) = call(&mut s, req) else {
            std::thread::sleep(Duration::from_millis(250));
            continue;
        };
        match &resp.kind {
            Some(response::Kind::Error(e)) if e.code == "not_leader" => {
                std::thread::sleep(Duration::from_millis(250));
            }
            _ => return resp,
        }
    }
    panic!("no node accepted the request as leader after retries");
}

fn produce_req(topic: &str, payload: &[u8], priority: u32, producer_id: &str, seq: u64) -> Request {
    Request {
        kind: Some(request::Kind::Produce(proto::Produce {
            topic: topic.to_string(),
            priority,
            content_type: 0,
            payload: bytes::Bytes::copy_from_slice(payload),
            producer_id: producer_id.to_string(),
            seq,
        })),
    }
}

fn poll_req(topic: &str, group: &str, max: u32, wait_ms: u64) -> Request {
    Request {
        kind: Some(request::Kind::Poll(proto::Poll {
            topic: topic.to_string(),
            group: group.to_string(),
            max,
            visibility_timeout_ms: 30_000,
            ack_mode: "manual".to_string(),
            wait_ms,
        })),
    }
}

fn ack_req(topic: &str, group: &str, lease_id: u64) -> Request {
    Request {
        kind: Some(request::Kind::Ack(proto::Ack {
            topic: topic.to_string(),
            group: group.to_string(),
            lease_id,
        })),
    }
}

fn produced_offset(resp: Response) -> u64 {
    match resp.kind {
        Some(response::Kind::Produced(p)) => p.offset,
        other => panic!("expected Produced, got {other:?}"),
    }
}

fn polled_items(resp: Response) -> Vec<proto::Delivered> {
    match resp.kind {
        Some(response::Kind::Polled(p)) => p.items,
        other => panic!("expected Polled, got {other:?}"),
    }
}

fn assert_ok(resp: Response) {
    assert!(
        matches!(resp.kind, Some(response::Kind::Ok(_))),
        "expected Ok, got {:?}",
        resp.kind
    );
}

#[test]
#[ignore = "requires Docker; run with: cargo e2e"]
fn e2e_docker_three_node_cluster() {
    let started = Instant::now();
    let step = |msg: &str| {
        println!("[e2e {:>6.1}s] {msg}", started.elapsed().as_secs_f32());
    };

    step("cleaning up any leftover cluster from a previous run");
    let _ = compose(&["down", "--remove-orphans", "-t", "2"]);
    let _guard = ComposeGuard;
    step("building the image and starting the 3-node cluster (first build can take several minutes)");
    compose_ok(&["up", "-d", "--build"]);
    step("cluster is up; waiting for /health on all three nodes");

    for port in METRICS_PORTS {
        wait_until(
            &format!("node on metrics port {port} to report /health"),
            Duration::from_secs(120),
            || http_get(port, "/health").map(|(s, _)| s == 200).unwrap_or(false),
        );
    }

    step("bootstrapping the cluster over the client protocol");
    let bootstrap = Request {
        kind: Some(request::Kind::Bootstrap(proto::Bootstrap {
            nodes: (1..=3u64)
                .map(|id| proto::Node {
                    id,
                    peer_addr: format!("hermesmq{id}:7700"),
                })
                .collect(),
        })),
    };
    wait_until("bootstrap to be accepted", Duration::from_secs(60), || {
        let Ok(mut s) = connect(CLIENT_PORTS[0]) else {
            return false;
        };
        matches!(
            call(&mut s, &bootstrap),
            Ok(Response {
                kind: Some(response::Kind::Ok(_))
            })
        )
    });

    step("waiting for /ready (leader elected) on all three nodes");
    for port in METRICS_PORTS {
        wait_until(
            &format!("node on metrics port {port} to report /ready"),
            Duration::from_secs(60),
            || http_get(port, "/ready").map(|(s, _)| s == 200).unwrap_or(false),
        );
    }

    step("create topic + produce (idempotent re-send, priorities)");
    let create = Request {
        kind: Some(request::Kind::CreateTopic(proto::CreateTopic {
            topic: "orders".to_string(),
        })),
    };
    assert_ok(request_leader(&create));

    let off_low = produced_offset(request_leader(&produce_req("orders", b"low", 0, "p1", 1)));
    let off_high = produced_offset(request_leader(&produce_req("orders", b"high", 7, "p1", 2)));
    let off_dup = produced_offset(request_leader(&produce_req("orders", b"low-retry", 0, "p1", 1)));
    assert_eq!(off_dup, off_low, "idempotent produce must return the original offset");
    assert_ne!(off_low, off_high);

    step("poll: priority order + dedup, then ack");
    let items = polled_items(request_leader(&poll_req("orders", "workers", 10, 5_000)));
    assert_eq!(items.len(), 2, "dedup re-send must not create a third message");
    assert_eq!(items[0].payload, &b"high"[..], "higher priority must be delivered first");
    assert_eq!(items[1].payload, &b"low"[..]);
    for item in &items {
        assert_ok(request_leader(&ack_req("orders", "workers", item.lease_id)));
    }

    let survivor_off = produced_offset(request_leader(&produce_req("orders", b"survivor", 0, "", 0)));
    let (old_leader, _) = wait_for_leader(None);
    step(&format!("failover: killing the leader container hermesmq{old_leader} (SIGKILL)"));
    compose_ok(&["kill", &format!("hermesmq{old_leader}")]);

    let (new_leader, new_leader_port) = wait_for_leader(Some(old_leader));
    assert_ne!(new_leader, old_leader);
    step(&format!("new leader elected: node {new_leader}"));

    let items = polled_items(request_leader(&poll_req("orders", "workers", 10, 10_000)));
    assert_eq!(items.len(), 1, "the un-acked message must survive leader loss");
    assert_eq!(items[0].payload, &b"survivor"[..]);
    assert_eq!(items[0].offset, survivor_off);
    assert_ok(request_leader(&ack_req("orders", "workers", items[0].lease_id)));
    step("un-acked message survived the leader loss; writes still flowing with 2/3 nodes");

    let off_after = produced_offset(request_leader(&produce_req("orders", b"after-failover", 0, "", 0)));
    assert!(off_after > survivor_off, "writes must keep flowing with 2/3 nodes");

    step(&format!("restarting hermesmq{old_leader} and waiting for log catch-up"));
    compose_ok(&["start", &format!("hermesmq{old_leader}")]);
    let target = stats(new_leader_port)
        .expect("leader stats")
        .last_applied;
    let restarted_port = CLIENT_PORTS[(old_leader - 1) as usize];
    wait_until(
        "the restarted node to catch up with the leader",
        Duration::from_secs(120),
        || {
            stats(restarted_port)
                .map(|st| st.last_applied >= target)
                .unwrap_or(false)
        },
    );
    wait_until(
        "the restarted node to report /ready",
        Duration::from_secs(60),
        || {
            let port = METRICS_PORTS[(old_leader - 1) as usize];
            http_get(port, "/ready").map(|(s, _)| s == 200).unwrap_or(false)
        },
    );

    step("checking prometheus /metrics on all three nodes");
    for port in METRICS_PORTS {
        let (status, body) = http_get(port, "/metrics").expect("metrics endpoint reachable");
        assert_eq!(status, 200);
        assert!(body.contains("hermesmq_raft_is_leader"), "prometheus output missing on {port}");
        assert!(body.contains("hermesmq_messages"));
    }
    step("all checks passed");
}
