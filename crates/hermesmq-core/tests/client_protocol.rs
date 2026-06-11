use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hermesmq_core::client::proto;
use hermesmq_core::client::proto::{request, response, Request, Response};
use hermesmq_core::engine::build_raft;
use hermesmq_core::{serve_clients, HermesRaft, RedbStore};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn call(stream: &mut TcpStream, req: Request) -> Response {
    let bytes = req.encode_to_vec();
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&bytes).await.unwrap();
    stream.flush().await.unwrap();

    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.unwrap();
    let n = u32::from_be_bytes(len) as usize;
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf).await.unwrap();
    Response::decode(buf.as_slice()).unwrap()
}

async fn write_req(stream: &mut TcpStream, req: Request) {
    let bytes = req.encode_to_vec();
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&bytes).await.unwrap();
    stream.flush().await.unwrap();
}

async fn read_resp(stream: &mut TcpStream) -> Response {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.unwrap();
    let n = u32::from_be_bytes(len) as usize;
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf).await.unwrap();
    Response::decode(buf.as_slice()).unwrap()
}

fn subscribe_req(topic: &str, group: &str, prefetch: u32, ack_mode: &str) -> Request {
    subscribe_req_vis(topic, group, prefetch, ack_mode, 30_000)
}

fn subscribe_req_vis(
    topic: &str,
    group: &str,
    prefetch: u32,
    ack_mode: &str,
    visibility_timeout_ms: u64,
) -> Request {
    Request {
        kind: Some(request::Kind::Subscribe(proto::Subscribe {
            topic: topic.to_string(),
            group: group.to_string(),
            prefetch,
            visibility_timeout_ms,
            ack_mode: ack_mode.to_string(),
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

async fn start() -> (HermesRaft, SocketAddr) {
    let db = Arc::new(RedbStore::in_memory().unwrap());
    let (raft, sm) = build_raft(1, db).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_clients(raft.clone(), sm, listener));
    (raft, addr)
}

fn bootstrap_req() -> Request {
    Request {
        kind: Some(request::Kind::Bootstrap(proto::Bootstrap {
            nodes: vec![proto::Node {
                id: 1,
                peer_addr: "127.0.0.1:9".to_string(),
            }],
        })),
    }
}

fn create_topic(topic: &str) -> Request {
    Request {
        kind: Some(request::Kind::CreateTopic(proto::CreateTopic {
            topic: topic.to_string(),
        })),
    }
}

fn produce(topic: &str, payload: &[u8]) -> Request {
    Request {
        kind: Some(request::Kind::Produce(proto::Produce {
            topic: topic.to_string(),
            priority: 0,
            content_type: 0,
            payload: payload.to_vec(),
            producer_id: String::new(),
            seq: 0,
        })),
    }
}

fn poll(topic: &str, group: &str) -> Request {
    poll_mode(topic, group, "manual", 1000)
}

fn poll_mode(topic: &str, group: &str, ack_mode: &str, visibility_timeout_ms: u64) -> Request {
    Request {
        kind: Some(request::Kind::Poll(proto::Poll {
            topic: topic.to_string(),
            group: group.to_string(),
            max: 10,
            visibility_timeout_ms,
            ack_mode: ack_mode.to_string(),
            wait_ms: 0,
        })),
    }
}

fn poll_wait(topic: &str, group: &str, wait_ms: u64) -> Request {
    Request {
        kind: Some(request::Kind::Poll(proto::Poll {
            topic: topic.to_string(),
            group: group.to_string(),
            max: 10,
            visibility_timeout_ms: 30_000,
            ack_mode: "manual".to_string(),
            wait_ms,
        })),
    }
}

fn polled_len(resp: Response) -> usize {
    match resp.kind {
        Some(response::Kind::Polled(p)) => p.items.len(),
        other => panic!("expected Polled, got {other:?}"),
    }
}

async fn bootstrap_and_wait(stream: &mut TcpStream, raft: &HermesRaft) {
    let r = call(stream, bootstrap_req()).await;
    assert!(matches!(r.kind, Some(response::Kind::Ok(_))));
    for _ in 0..200 {
        if raft.current_leader().await.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_protocol_bootstrap_produce_poll_ack() {
    let (raft, addr) = start().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    bootstrap_and_wait(&mut s, &raft).await;

    assert!(matches!(
        call(&mut s, create_topic("t")).await.kind,
        Some(response::Kind::Ok(_))
    ));

    let offset = match call(&mut s, produce("t", b"hello")).await.kind {
        Some(response::Kind::Produced(p)) => p.offset,
        other => panic!("expected Produced, got {other:?}"),
    };
    assert_eq!(offset, 0);

    let items = match call(&mut s, poll("t", "g")).await.kind {
        Some(response::Kind::Polled(p)) => p.items,
        other => panic!("expected Polled, got {other:?}"),
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].payload, b"hello");

    let ack = Request {
        kind: Some(request::Kind::Ack(proto::Ack {
            topic: "t".to_string(),
            group: "g".to_string(),
            lease_id: items[0].lease_id,
        })),
    };
    assert!(matches!(
        call(&mut s, ack).await.kind,
        Some(response::Kind::Ok(_))
    ));

    let items = match call(&mut s, poll("t", "g")).await.kind {
        Some(response::Kind::Polled(p)) => p.items,
        other => panic!("expected Polled, got {other:?}"),
    };
    assert!(items.is_empty(), "acked message must not be redelivered");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_protocol_long_poll_wakes_on_produce() {
    let (raft, addr) = start().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    bootstrap_and_wait(&mut s, &raft).await;
    call(&mut s, create_topic("lp")).await;

    let poller = tokio::spawn(async move {
        let mut ps = TcpStream::connect(addr).await.unwrap();
        call(&mut ps, poll_wait("lp", "g", 5_000)).await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(matches!(
        call(&mut s, produce("lp", b"wakeup")).await.kind,
        Some(response::Kind::Produced(_))
    ));

    let resp = poller.await.unwrap();
    match resp.kind {
        Some(response::Kind::Polled(p)) => {
            assert_eq!(p.items.len(), 1, "long-poll must wake and deliver on produce");
            assert_eq!(p.items[0].payload, b"wakeup");
        }
        other => panic!("expected Polled, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_protocol_push_subscribe_manual() {
    let (raft, addr) = start().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    bootstrap_and_wait(&mut s, &raft).await;
    call(&mut s, create_topic("ps")).await;
    for p in [b"a".as_ref(), b"b", b"c"] {
        assert!(matches!(
            call(&mut s, produce("ps", p)).await.kind,
            Some(response::Kind::Produced(_))
        ));
    }

    let mut sub = TcpStream::connect(addr).await.unwrap();
    write_req(&mut sub, subscribe_req("ps", "g", 10, "manual")).await;

    let mut got: Vec<Vec<u8>> = Vec::new();
    for _ in 0..3 {
        let item = match read_resp(&mut sub).await.kind {
            Some(response::Kind::Polled(p)) => p.items.into_iter().next().unwrap(),
            other => panic!("expected pushed Polled, got {other:?}"),
        };
        got.push(item.payload.clone());
        write_req(
            &mut sub,
            Request {
                kind: Some(request::Kind::Ack(proto::Ack {
                    topic: "ps".to_string(),
                    group: "g".to_string(),
                    lease_id: item.lease_id,
                })),
            },
        )
        .await;
    }
    got.sort();
    assert_eq!(got, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_protocol_push_subscribe_auto_wakes_on_produce() {
    let (raft, addr) = start().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    bootstrap_and_wait(&mut s, &raft).await;
    call(&mut s, create_topic("ps2")).await;

    let mut sub = TcpStream::connect(addr).await.unwrap();
    write_req(&mut sub, subscribe_req("ps2", "g", 10, "auto")).await;

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(matches!(
        call(&mut s, produce("ps2", b"later")).await.kind,
        Some(response::Kind::Produced(_))
    ));

    match read_resp(&mut sub).await.kind {
        Some(response::Kind::Polled(p)) => {
            assert_eq!(p.items.len(), 1);
            assert_eq!(p.items[0].payload, b"later");
        }
        other => panic!("expected pushed Polled, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_protocol_subscribe_duplicate_ack_does_not_stall() {
    let (raft, addr) = start().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    bootstrap_and_wait(&mut s, &raft).await;
    call(&mut s, create_topic("dup")).await;
    call(&mut s, produce("dup", b"first")).await;
    call(&mut s, produce("dup", b"second")).await;

    let mut sub = TcpStream::connect(addr).await.unwrap();
    write_req(&mut sub, subscribe_req("dup", "g", 1, "manual")).await;

    let m1 = match read_resp(&mut sub).await.kind {
        Some(response::Kind::Polled(p)) => p.items.into_iter().next().unwrap(),
        other => panic!("expected pushed Polled, got {other:?}"),
    };
    write_req(&mut sub, ack_req("dup", "g", m1.lease_id)).await;
    write_req(&mut sub, ack_req("dup", "g", m1.lease_id)).await;

    let m2 = tokio::time::timeout(Duration::from_secs(5), read_resp(&mut sub))
        .await
        .expect("subscription must keep delivering after a duplicate ack");
    match m2.kind {
        Some(response::Kind::Polled(p)) => {
            assert_eq!(p.items[0].payload, b"second");
        }
        other => panic!("expected pushed Polled, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_protocol_subscribe_reclaims_slot_after_visibility_expiry() {
    let (raft, addr) = start().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    bootstrap_and_wait(&mut s, &raft).await;
    call(&mut s, create_topic("vt")).await;
    call(&mut s, produce("vt", b"stuck")).await;

    let mut sub = TcpStream::connect(addr).await.unwrap();
    write_req(&mut sub, subscribe_req_vis("vt", "g", 1, "manual", 300)).await;

    let first = tokio::time::timeout(Duration::from_secs(5), read_resp(&mut sub))
        .await
        .expect("first delivery");
    let m1 = match first.kind {
        Some(response::Kind::Polled(p)) => p.items.into_iter().next().unwrap(),
        other => panic!("expected pushed Polled, got {other:?}"),
    };

    let second = tokio::time::timeout(Duration::from_secs(5), read_resp(&mut sub))
        .await
        .expect("un-acked message must be redelivered on the same subscription after expiry");
    match second.kind {
        Some(response::Kind::Polled(p)) => {
            let m2 = &p.items[0];
            assert_eq!(m2.payload, b"stuck");
            assert_ne!(m2.lease_id, m1.lease_id, "redelivery must carry a fresh lease");
        }
        other => panic!("expected pushed Polled, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_protocol_subscribe_dedups_slow_handler_and_accepts_late_ack() {
    let (raft, addr) = start().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    bootstrap_and_wait(&mut s, &raft).await;
    call(&mut s, create_topic("slowh")).await;
    call(&mut s, produce("slowh", b"slow")).await;

    let mut sub = TcpStream::connect(addr).await.unwrap();
    write_req(&mut sub, subscribe_req_vis("slowh", "g", 4, "manual", 300)).await;

    let first = tokio::time::timeout(Duration::from_secs(5), read_resp(&mut sub))
        .await
        .expect("first delivery");
    let m1 = match first.kind {
        Some(response::Kind::Polled(p)) => p.items.into_iter().next().unwrap(),
        other => panic!("expected pushed Polled, got {other:?}"),
    };
    assert_eq!(m1.payload, b"slow");

    let dup = tokio::time::timeout(Duration::from_millis(800), read_resp(&mut sub)).await;
    assert!(
        dup.is_err(),
        "no duplicate may be pushed to the same connection while its lease is auto-refreshed"
    );

    write_req(&mut sub, ack_req("slowh", "g", m1.lease_id)).await;
    call(&mut s, produce("slowh", b"next")).await;

    let second = tokio::time::timeout(Duration::from_secs(5), read_resp(&mut sub))
        .await
        .expect("delivery after late ack");
    match second.kind {
        Some(response::Kind::Polled(p)) => {
            assert_eq!(
                p.items[0].payload, b"next",
                "a late ack must complete the first message via lease translation"
            );
        }
        other => panic!("expected pushed Polled, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_protocol_poll_zero_values_are_normalized() {
    let (raft, addr) = start().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    bootstrap_and_wait(&mut s, &raft).await;
    call(&mut s, create_topic("zz")).await;
    call(&mut s, produce("zz", b"m")).await;

    let req = Request {
        kind: Some(request::Kind::Poll(proto::Poll {
            topic: "zz".to_string(),
            group: "g".to_string(),
            max: 0,
            visibility_timeout_ms: 0,
            ack_mode: "manual".to_string(),
            wait_ms: 0,
        })),
    };
    assert_eq!(polled_len(call(&mut s, req).await), 1, "max=0 must fall back to the default batch size");

    assert_eq!(
        polled_len(call(&mut s, poll("zz", "g")).await),
        0,
        "visibility=0 must fall back to the default lease, not an instantly-expired one"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_protocol_pipelined_requests_get_in_order_responses() {
    let (raft, addr) = start().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    bootstrap_and_wait(&mut s, &raft).await;

    write_req(&mut s, create_topic("pipe")).await;
    for payload in [b"a".as_ref(), b"b", b"c"] {
        write_req(&mut s, produce("pipe", payload)).await;
    }
    write_req(
        &mut s,
        Request {
            kind: Some(request::Kind::Stats(proto::Stats {})),
        },
    )
    .await;

    assert!(matches!(
        read_resp(&mut s).await.kind,
        Some(response::Kind::Ok(_))
    ));
    let mut offsets = Vec::new();
    for _ in 0..3 {
        match read_resp(&mut s).await.kind {
            Some(response::Kind::Produced(p)) => offsets.push(p.offset),
            other => panic!("expected Produced in position, got {other:?}"),
        }
    }
    offsets.sort();
    assert_eq!(offsets, vec![0, 1, 2], "three produces must yield three distinct offsets");
    assert!(matches!(
        read_resp(&mut s).await.kind,
        Some(response::Kind::Stats(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_protocol_pipelined_subscribe_drains_pending_responses_first() {
    let (raft, addr) = start().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    bootstrap_and_wait(&mut s, &raft).await;
    call(&mut s, create_topic("ps3")).await;

    let mut c = TcpStream::connect(addr).await.unwrap();
    write_req(&mut c, produce("ps3", b"one")).await;
    write_req(&mut c, produce("ps3", b"two")).await;
    write_req(&mut c, subscribe_req("ps3", "g", 10, "manual")).await;

    for _ in 0..2 {
        assert!(
            matches!(read_resp(&mut c).await.kind, Some(response::Kind::Produced(_))),
            "pipelined produce responses must drain before the subscription takes over"
        );
    }

    let mut got = Vec::new();
    for _ in 0..2 {
        let pushed = tokio::time::timeout(Duration::from_secs(5), read_resp(&mut c))
            .await
            .expect("pushed delivery after handover");
        match pushed.kind {
            Some(response::Kind::Polled(p)) => {
                let item = p.items.into_iter().next().unwrap();
                got.push(item.payload.clone());
                write_req(&mut c, ack_req("ps3", "g", item.lease_id)).await;
            }
            other => panic!("expected pushed Polled, got {other:?}"),
        }
    }
    got.sort();
    assert_eq!(got, vec![b"one".to_vec(), b"two".to_vec()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_protocol_rejects_oversized_payload() {
    let (raft, addr) = start().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    bootstrap_and_wait(&mut s, &raft).await;
    call(&mut s, create_topic("big")).await;

    let body = vec![0u8; hermesmq_core::MAX_PAYLOAD_BYTES + 1];
    match call(&mut s, produce("big", &body)).await.kind {
        Some(response::Kind::Error(e)) => assert_eq!(e.code, "payload_too_large"),
        other => panic!("expected payload_too_large error, got {other:?}"),
    }

    assert!(matches!(
        call(&mut s, produce("big", b"small")).await.kind,
        Some(response::Kind::Produced(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_protocol_rate_limit() {
    let (raft, addr) = start().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    bootstrap_and_wait(&mut s, &raft).await;

    assert!(matches!(
        call(&mut s, create_topic("rl")).await.kind,
        Some(response::Kind::Ok(_))
    ));

    let set = Request {
        kind: Some(request::Kind::SetRateLimit(proto::SetRateLimit {
            topic: "rl".to_string(),
            rate_per_sec: 1.0,
            burst: 1,
        })),
    };
    assert!(matches!(
        call(&mut s, set).await.kind,
        Some(response::Kind::Ok(_))
    ));

    for payload in [b"a", b"b", b"c"] {
        assert!(
            matches!(
                call(&mut s, produce("rl", payload)).await.kind,
                Some(response::Kind::Produced(_))
            ),
            "produce must never be rate limited"
        );
    }

    assert_eq!(
        polled_len(call(&mut s, poll("rl", "g")).await),
        1,
        "delivery must be paced to the available tokens (burst = 1)"
    );

    match call(&mut s, poll("rl", "g")).await.kind {
        Some(response::Kind::Error(e)) => assert_eq!(e.code, "rate_limited"),
        other => panic!("expected rate_limited error, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_protocol_ack_mode_auto_vs_manual() {
    let (raft, addr) = start().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    bootstrap_and_wait(&mut s, &raft).await;

    // auto: message is acked on delivery, so it is gone after the visibility timeout
    call(&mut s, create_topic("auto")).await;
    call(&mut s, produce("auto", b"x")).await;
    assert_eq!(polled_len(call(&mut s, poll_mode("auto", "g", "auto", 100)).await), 1);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        polled_len(call(&mut s, poll_mode("auto", "g", "manual", 100)).await),
        0,
        "auto-acked message must not be redelivered after expiry"
    );

    // manual without ack: redelivered after the visibility timeout
    call(&mut s, create_topic("man")).await;
    call(&mut s, produce("man", b"y")).await;
    assert_eq!(polled_len(call(&mut s, poll_mode("man", "g", "manual", 100)).await), 1);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        polled_len(call(&mut s, poll_mode("man", "g", "manual", 100)).await),
        1,
        "un-acked manual message must be redelivered after expiry"
    );
}
