use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use openraft::error::{ClientWriteError, InitializeError, RaftError};
use openraft::BasicNode;
use prost::Message;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

use crate::engine::{HermesRaft, StateMachineStore};
use crate::frame::{read_frame, write_frame};
use crate::raft::{AppRequest, AppResponse, Delivered};
use crate::types::{ContentType, GroupId, NodeId, Priority, TopicId};

struct Bucket {
    tokens: f64,
    last: Instant,
}

#[derive(Default)]
struct RateLimiter {
    buckets: StdMutex<HashMap<String, Bucket>>,
}

impl RateLimiter {
    fn check(&self, topic: &str, rate_per_sec: f64, burst: f64) -> std::result::Result<(), u64> {
        let mut buckets = self.buckets.lock().unwrap();
        let now = Instant::now();
        let bucket = buckets.entry(topic.to_string()).or_insert(Bucket {
            tokens: burst,
            last: now,
        });
        let elapsed = now.duration_since(bucket.last).as_secs_f64();
        bucket.last = now;
        bucket.tokens = (bucket.tokens + elapsed * rate_per_sec).min(burst);
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            let needed = 1.0 - bucket.tokens;
            let retry_ms = if rate_per_sec > 0.0 {
                (needed / rate_per_sec * 1000.0).ceil() as u64
            } else {
                1000
            };
            Err(retry_ms)
        }
    }
}

#[derive(Clone)]
struct Ctx {
    raft: HermesRaft,
    sm: StateMachineStore,
    limiter: Arc<RateLimiter>,
    notifiers: Arc<StdMutex<HashMap<String, Arc<Notify>>>>,
}

impl Ctx {
    fn notifier(&self, topic: &str) -> Arc<Notify> {
        self.notifiers
            .lock()
            .unwrap()
            .entry(topic.to_string())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }
}

pub use hermesmq_proto as proto;

use proto::{request, response, Request, Response};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn ct_from_u32(v: u32) -> ContentType {
    match v {
        1 => ContentType::Text,
        2 => ContentType::Json,
        3 => ContentType::MsgPack,
        _ => ContentType::Raw,
    }
}

fn ct_to_u32(c: ContentType) -> u32 {
    match c {
        ContentType::Raw => 0,
        ContentType::Text => 1,
        ContentType::Json => 2,
        ContentType::MsgPack => 3,
    }
}

fn resp_ok() -> Response {
    Response {
        kind: Some(response::Kind::Ok(proto::Ok {})),
    }
}

fn resp_error(code: &str, message: String) -> Response {
    Response {
        kind: Some(response::Kind::Error(proto::Error {
            code: code.to_string(),
            message,
            leader_addr: String::new(),
            retry_after_ms: 0,
        })),
    }
}

fn resp_not_leader(addr: String) -> Response {
    Response {
        kind: Some(response::Kind::Error(proto::Error {
            code: "not_leader".to_string(),
            message: "not the leader; retry against leader_addr".to_string(),
            leader_addr: addr,
            retry_after_ms: 0,
        })),
    }
}

fn resp_rate_limited(retry_after_ms: u64) -> Response {
    Response {
        kind: Some(response::Kind::Error(proto::Error {
            code: "rate_limited".to_string(),
            message: "rate limit exceeded".to_string(),
            leader_addr: String::new(),
            retry_after_ms,
        })),
    }
}

fn resp_polled(items: Vec<Delivered>) -> Response {
    let items = items
        .into_iter()
        .map(|d| proto::Delivered {
            lease_id: d.lease_id,
            offset: d.offset,
            priority: d.priority.0 as u32,
            content_type: ct_to_u32(d.content_type),
            payload: d.payload,
            ts_ms: d.ts_ms,
        })
        .collect();
    Response {
        kind: Some(response::Kind::Polled(proto::Polled { items })),
    }
}

fn map_raft_error(e: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>) -> Response {
    match e {
        RaftError::APIError(ClientWriteError::ForwardToLeader(f)) => {
            resp_not_leader(f.leader_node.map(|n| n.addr).unwrap_or_default())
        }
        other => resp_error("internal", other.to_string()),
    }
}

fn app_response_to_proto(r: AppResponse) -> Response {
    match r {
        AppResponse::Produced { offset } => Response {
            kind: Some(response::Kind::Produced(proto::Produced { offset })),
        },
        AppResponse::Polled { items } => resp_polled(items),
        AppResponse::Acked
        | AppResponse::Nacked
        | AppResponse::Committed
        | AppResponse::TopicCreated
        | AppResponse::TopicDeleted
        | AppResponse::RateLimitSet
        | AppResponse::RetentionSet
        | AppResponse::NoOp => resp_ok(),
    }
}

async fn write_and_map(raft: &HermesRaft, app: AppRequest) -> Response {
    match raft.client_write(app).await {
        Ok(resp) => app_response_to_proto(resp.data),
        Err(e) => map_raft_error(e),
    }
}

async fn handle_produce(ctx: &Ctx, p: proto::Produce) -> Response {
    let topic = p.topic.clone();
    let resp = write_and_map(
        &ctx.raft,
        AppRequest::Produce {
            topic: TopicId(p.topic),
            priority: Priority(p.priority as u8),
            content_type: ct_from_u32(p.content_type),
            payload: p.payload,
            producer_id: p.producer_id,
            seq: p.seq,
            ts_ms: now_ms(),
        },
    )
    .await;
    if matches!(resp.kind, Some(response::Kind::Produced(_))) {
        ctx.notifier(&topic).notify_waiters();
    }
    resp
}

async fn handle_poll(ctx: &Ctx, p: proto::Poll) -> Response {
    {
        let metrics = ctx.raft.metrics();
        let m = metrics.borrow();
        if m.current_leader != Some(m.id) {
            return resp_not_leader(String::new());
        }
    }

    let topic = TopicId(p.topic.clone());
    let group = GroupId(p.group.clone());
    let deadline = Instant::now() + Duration::from_millis(p.wait_ms);

    loop {
        if ctx.sm.has_deliverable(&p.topic, &p.group, now_ms()) {
            if let Some((rate_milli, burst)) = ctx.sm.rate_config(&p.topic) {
                let rate = rate_milli as f64 / 1000.0;
                if let Err(retry) = ctx.limiter.check(&p.topic, rate, burst.max(1) as f64) {
                    return resp_rate_limited(retry);
                }
            }
            let app = AppRequest::Poll {
                topic: topic.clone(),
                group: group.clone(),
                max: p.max,
                visibility_timeout_ms: p.visibility_timeout_ms,
                ts_ms: now_ms(),
            };
            let resp = match ctx.raft.client_write(app).await {
                Ok(r) => r,
                Err(e) => return map_raft_error(e),
            };
            let items = match resp.data {
                AppResponse::Polled { items } => items,
                other => return app_response_to_proto(other),
            };
            if !items.is_empty() {
                if p.ack_mode == "auto" {
                    for d in &items {
                        let _ = ctx
                            .raft
                            .client_write(AppRequest::Ack {
                                topic: topic.clone(),
                                group: group.clone(),
                                lease_id: d.lease_id,
                            })
                            .await;
                    }
                }
                return resp_polled(items);
            }
        }

        if p.wait_ms == 0 || Instant::now() >= deadline {
            return resp_polled(Vec::new());
        }
        let wait = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(200));
        let notifier = ctx.notifier(&p.topic);
        tokio::select! {
            _ = notifier.notified() => {}
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

async fn handle_bootstrap(raft: &HermesRaft, b: proto::Bootstrap) -> Response {
    let members: BTreeMap<NodeId, BasicNode> = b
        .nodes
        .into_iter()
        .map(|n| (n.id, BasicNode::new(n.peer_addr)))
        .collect();
    match raft.initialize(members).await {
        Ok(()) => resp_ok(),
        Err(RaftError::APIError(InitializeError::NotAllowed(_))) => resp_ok(),
        Err(e) => resp_error("bootstrap_failed", e.to_string()),
    }
}

fn stats_response(raft: &HermesRaft, sm: &StateMachineStore) -> Response {
    let metrics = raft.metrics();
    let m = metrics.borrow();
    let last_applied = m.last_applied.as_ref().map(|l| l.index).unwrap_or(0);
    let current_leader = m.current_leader.unwrap_or(0);
    let current_term = m.current_term;
    let last_log_index = m.last_log_index.unwrap_or(0);
    let is_leader = m.current_leader == Some(m.id);
    drop(m);
    let qm = sm.metrics();
    Response {
        kind: Some(response::Kind::Stats(proto::StatsResult {
            last_applied,
            current_leader,
            current_term,
            last_log_index,
            is_leader,
            topics: qm.topics,
            messages: qm.messages,
            in_flight: qm.in_flight,
        })),
    }
}

async fn handle_request(ctx: &Ctx, req: Request) -> Response {
    let Some(kind) = req.kind else {
        return resp_error("bad_request", "empty request".to_string());
    };

    if let request::Kind::Produce(p) = &kind {
        if let Some((rate_milli, burst)) = ctx.sm.rate_config(&p.topic) {
            let rate = rate_milli as f64 / 1000.0;
            if let Err(retry_after_ms) = ctx.limiter.check(&p.topic, rate, burst.max(1) as f64) {
                return resp_rate_limited(retry_after_ms);
            }
        }
    }

    let raft = &ctx.raft;
    match kind {
        request::Kind::Bootstrap(b) => handle_bootstrap(raft, b).await,
        request::Kind::Stats(_) => stats_response(raft, &ctx.sm),
        request::Kind::Produce(p) => handle_produce(ctx, p).await,
        request::Kind::Poll(p) => handle_poll(ctx, p).await,
        request::Kind::SetRateLimit(s) => {
            write_and_map(
                raft,
                AppRequest::SetRateLimit {
                    topic: TopicId(s.topic),
                    rate_milli_per_sec: (s.rate_per_sec * 1000.0).round() as u64,
                    burst: s.burst,
                },
            )
            .await
        }
        request::Kind::SetRetention(s) => {
            write_and_map(
                raft,
                AppRequest::SetRetention {
                    topic: TopicId(s.topic),
                    max_messages: s.max_messages,
                    max_age_ms: s.max_age_ms,
                },
            )
            .await
        }
        request::Kind::Ack(a) => {
            write_and_map(
                raft,
                AppRequest::Ack {
                    topic: TopicId(a.topic),
                    group: GroupId(a.group),
                    lease_id: a.lease_id,
                },
            )
            .await
        }
        request::Kind::Nack(a) => {
            write_and_map(
                raft,
                AppRequest::Nack {
                    topic: TopicId(a.topic),
                    group: GroupId(a.group),
                    lease_id: a.lease_id,
                },
            )
            .await
        }
        request::Kind::Commit(c) => {
            write_and_map(
                raft,
                AppRequest::CommitOffset {
                    topic: TopicId(c.topic),
                    group: GroupId(c.group),
                    offset: c.offset,
                },
            )
            .await
        }
        request::Kind::CreateTopic(c) => {
            write_and_map(raft, AppRequest::CreateTopic { topic: TopicId(c.topic) }).await
        }
        request::Kind::DeleteTopic(c) => {
            write_and_map(raft, AppRequest::DeleteTopic { topic: TopicId(c.topic) }).await
        }
        request::Kind::Subscribe(_) => {
            resp_error("bad_request", "subscribe must be the first message on its own connection".to_string())
        }
    }
}

async fn handle_subscribe(ctx: Ctx, stream: TcpStream, sub: proto::Subscribe) -> io::Result<()> {
    let (mut read_half, mut write_half) = stream.into_split();

    let is_leader = {
        let metrics = ctx.raft.metrics();
        let m = metrics.borrow();
        m.current_leader == Some(m.id)
    };
    if !is_leader {
        let resp = resp_not_leader(String::new());
        write_frame(&mut write_half, &resp.encode_to_vec()).await?;
        return Ok(());
    }

    let topic = TopicId(sub.topic.clone());
    let group = GroupId(sub.group.clone());
    let prefetch = sub.prefetch.max(1) as usize;
    let auto = sub.ack_mode == "auto";
    let visibility = if sub.visibility_timeout_ms == 0 {
        30_000
    } else {
        sub.visibility_timeout_ms
    };

    let in_flight = Arc::new(AtomicUsize::new(0));
    let freed = Arc::new(Notify::new());

    let reader = if auto {
        None
    } else {
        let ctx = ctx.clone();
        let topic = topic.clone();
        let group = group.clone();
        let in_flight = in_flight.clone();
        let freed = freed.clone();
        Some(tokio::spawn(async move {
            loop {
                let bytes = match read_frame(&mut read_half).await {
                    Ok(b) => b,
                    Err(_) => break,
                };
                let Ok(req) = Request::decode(bytes.as_slice()) else {
                    continue;
                };
                match req.kind {
                    Some(request::Kind::Ack(a)) => {
                        let _ = ctx
                            .raft
                            .client_write(AppRequest::Ack {
                                topic: topic.clone(),
                                group: group.clone(),
                                lease_id: a.lease_id,
                            })
                            .await;
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                        freed.notify_one();
                    }
                    Some(request::Kind::Nack(a)) => {
                        let _ = ctx
                            .raft
                            .client_write(AppRequest::Nack {
                                topic: topic.clone(),
                                group: group.clone(),
                                lease_id: a.lease_id,
                            })
                            .await;
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                        freed.notify_one();
                    }
                    _ => {}
                }
            }
        }))
    };

    loop {
        {
            let metrics = ctx.raft.metrics();
            let m = metrics.borrow();
            if m.current_leader != Some(m.id) {
                break;
            }
        }

        let cur = in_flight.load(Ordering::SeqCst);
        let mut pushed_any = false;
        if cur < prefetch && ctx.sm.has_deliverable(&sub.topic, &sub.group, now_ms()) {
            let allowed = match ctx.sm.rate_config(&sub.topic) {
                Some((rate_milli, burst)) => ctx
                    .limiter
                    .check(&sub.topic, rate_milli as f64 / 1000.0, burst.max(1) as f64)
                    .is_ok(),
                None => true,
            };
            if allowed {
                let app = AppRequest::Poll {
                    topic: topic.clone(),
                    group: group.clone(),
                    max: (prefetch - cur) as u32,
                    visibility_timeout_ms: visibility,
                    ts_ms: now_ms(),
                };
                match ctx.raft.client_write(app).await {
                    Ok(r) => {
                        if let AppResponse::Polled { items } = r.data {
                            for d in items {
                                pushed_any = true;
                                if auto {
                                    let _ = ctx
                                        .raft
                                        .client_write(AppRequest::Ack {
                                            topic: topic.clone(),
                                            group: group.clone(),
                                            lease_id: d.lease_id,
                                        })
                                        .await;
                                } else {
                                    in_flight.fetch_add(1, Ordering::SeqCst);
                                }
                                let frame = resp_polled(vec![d]);
                                if write_frame(&mut write_half, &frame.encode_to_vec())
                                    .await
                                    .is_err()
                                {
                                    if let Some(r) = &reader {
                                        r.abort();
                                    }
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        }

        if pushed_any {
            continue;
        }
        let notifier = ctx.notifier(&sub.topic);
        tokio::select! {
            _ = freed.notified() => {}
            _ = notifier.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }

    if let Some(r) = &reader {
        r.abort();
    }
    Ok(())
}

pub async fn serve_clients(raft: HermesRaft, sm: StateMachineStore, listener: TcpListener) {
    let ctx = Ctx {
        raft,
        sm,
        limiter: Arc::new(RateLimiter::default()),
        notifiers: Arc::new(StdMutex::new(HashMap::new())),
    };
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_client(ctx, stream).await {
                        tracing::debug!("client connection closed: {e}");
                    }
                });
            }
            Err(e) => tracing::warn!("client accept error: {e}"),
        }
    }
}

async fn handle_client(ctx: Ctx, mut stream: TcpStream) -> io::Result<()> {
    loop {
        let bytes = read_frame(&mut stream).await?;
        let req = match Request::decode(bytes.as_slice()) {
            Ok(req) => req,
            Err(e) => {
                let resp = resp_error("bad_request", e.to_string());
                write_frame(&mut stream, &resp.encode_to_vec()).await?;
                continue;
            }
        };
        if let Some(request::Kind::Subscribe(sub)) = req.kind {
            return handle_subscribe(ctx, stream, sub).await;
        }
        let response = handle_request(&ctx, req).await;
        write_frame(&mut stream, &response.encode_to_vec()).await?;
    }
}
