use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use openraft::error::{ClientWriteError, InitializeError, RaftError};
use openraft::BasicNode;
use prost::Message;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Notify};

use crate::engine::{HermesRaft, StateMachineStore};
use crate::frame::{read_frame, write_frame};
use crate::raft::{AppRequest, AppResponse, Delivered, ProduceItem};
use crate::types::{ContentType, GroupId, LeaseId, NodeId, Offset, Priority, TopicId};

pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const DEFAULT_POLL_MAX: u32 = 16;
const MAX_POLL_MAX: u32 = 1024;
const DEFAULT_VISIBILITY_MS: u64 = 30_000;
const MAX_WAIT_MS: u64 = 300_000;
const MAX_PRIORITY: u32 = 7;
const SUBSCRIBE_MAX_LEASE_REFRESHES: u32 = 2;
const SUBSCRIBE_MAX_LEASE_ALIASES: usize = 8;
const PIPELINE_DEPTH: usize = 32;
const PRODUCE_QUEUE_DEPTH: usize = 1024;
const PRODUCE_BATCH_MAX_ITEMS: usize = 256;
const PRODUCE_BATCH_MAX_BYTES: usize = 512 * 1024;
const ACK_QUEUE_DEPTH: usize = 1024;
const ACK_BATCH_MAX: usize = 1024;
const ACK_GRACE_MS: u64 = 5_000;

struct OffsetTrack {
    lease_id: LeaseId,
    deadline_ms: u64,
    refreshes: u32,
    leases: Vec<LeaseId>,
}

enum Delivery {
    Push,
    Refreshed,
    AlreadyAcked,
}

#[derive(Default)]
struct SubTracking {
    by_offset: HashMap<Offset, OffsetTrack>,
    lease_to_offset: HashMap<LeaseId, Offset>,
    acked_recently: HashMap<Offset, u64>,
}

impl SubTracking {
    fn deliver(&mut self, offset: Offset, lease_id: LeaseId, deadline_ms: u64, now_ms: u64) -> Delivery {
        if let Some(until) = self.acked_recently.get(&offset) {
            if *until > now_ms {
                return Delivery::AlreadyAcked;
            }
            self.acked_recently.remove(&offset);
        }
        match self.by_offset.get_mut(&offset) {
            Some(track) => {
                let refresh = track.refreshes < SUBSCRIBE_MAX_LEASE_REFRESHES;
                track.refreshes = if refresh { track.refreshes + 1 } else { 0 };
                track.lease_id = lease_id;
                track.deadline_ms = deadline_ms;
                track.leases.push(lease_id);
                if track.leases.len() > SUBSCRIBE_MAX_LEASE_ALIASES {
                    let old = track.leases.remove(0);
                    self.lease_to_offset.remove(&old);
                }
                self.lease_to_offset.insert(lease_id, offset);
                if refresh {
                    Delivery::Refreshed
                } else {
                    Delivery::Push
                }
            }
            None => {
                self.by_offset.insert(
                    offset,
                    OffsetTrack {
                        lease_id,
                        deadline_ms,
                        refreshes: 0,
                        leases: vec![lease_id],
                    },
                );
                self.lease_to_offset.insert(lease_id, offset);
                Delivery::Push
            }
        }
    }

    fn resolve(&mut self, lease_id: LeaseId, acked_until: Option<u64>) -> Option<LeaseId> {
        let offset = self.lease_to_offset.get(&lease_id).copied()?;
        let track = self.by_offset.remove(&offset)?;
        for lease in &track.leases {
            self.lease_to_offset.remove(lease);
        }
        if let Some(until) = acked_until {
            self.acked_recently.insert(offset, until);
        }
        Some(track.lease_id)
    }

    fn live_count(&mut self, now_ms: u64, grace_ms: u64) -> usize {
        self.acked_recently.retain(|_, until| *until > now_ms);
        let stale: Vec<Offset> = self
            .by_offset
            .iter()
            .filter(|(_, t)| t.deadline_ms.saturating_add(grace_ms) <= now_ms)
            .map(|(offset, _)| *offset)
            .collect();
        for offset in stale {
            if let Some(track) = self.by_offset.remove(&offset) {
                for lease in &track.leases {
                    self.lease_to_offset.remove(lease);
                }
            }
        }
        self.by_offset
            .values()
            .filter(|t| t.deadline_ms > now_ms)
            .count()
    }
}

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

    fn forget(&self, topic: &str) {
        self.buckets.lock().unwrap().remove(topic);
    }
}

struct ProduceJob {
    p: proto::Produce,
    reply: oneshot::Sender<Response>,
}

struct AckJob {
    topic: String,
    group: String,
    lease_id: LeaseId,
    nack: bool,
    reply: Option<oneshot::Sender<Response>>,
}

#[derive(Clone)]
struct Ctx {
    raft: HermesRaft,
    sm: StateMachineStore,
    limiter: Arc<RateLimiter>,
    notifiers: Arc<StdMutex<HashMap<String, Arc<Notify>>>>,
    produce_tx: mpsc::Sender<ProduceJob>,
    ack_tx: mpsc::Sender<AckJob>,
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
        AppResponse::ProducedMany { .. } => {
            resp_error("internal", "unexpected batched produce response".to_string())
        }
    }
}

async fn write_and_map(raft: &HermesRaft, app: AppRequest) -> Response {
    match raft.client_write(app).await {
        Ok(resp) => app_response_to_proto(resp.data),
        Err(e) => map_raft_error(e),
    }
}

fn leader_addr(raft: &HermesRaft) -> String {
    let metrics = raft.metrics();
    let m = metrics.borrow();
    m.current_leader
        .and_then(|id| {
            m.membership_config
                .membership()
                .get_node(&id)
                .map(|n| n.addr.clone())
        })
        .unwrap_or_default()
}

async fn handle_produce(ctx: &Ctx, p: proto::Produce) -> Response {
    if p.payload.len() > MAX_PAYLOAD_BYTES {
        return resp_error(
            "payload_too_large",
            format!("payload is {} bytes; max is {MAX_PAYLOAD_BYTES}", p.payload.len()),
        );
    }
    if let Some((rate_milli, burst)) = ctx.sm.rate_config(&p.topic) {
        let rate = rate_milli as f64 / 1000.0;
        if let Err(retry_after_ms) = ctx.limiter.check(&p.topic, rate, burst.max(1) as f64) {
            return resp_rate_limited(retry_after_ms);
        }
    }
    let (reply, reply_rx) = oneshot::channel();
    if ctx.produce_tx.send(ProduceJob { p, reply }).await.is_err() {
        return resp_error("internal", "produce batcher unavailable".to_string());
    }
    match reply_rx.await {
        Ok(resp) => resp,
        Err(_) => resp_error("internal", "produce batcher dropped the request".to_string()),
    }
}

async fn produce_batcher(
    raft: HermesRaft,
    notifiers: Arc<StdMutex<HashMap<String, Arc<Notify>>>>,
    mut rx: mpsc::Receiver<ProduceJob>,
) {
    while let Some(first) = rx.recv().await {
        let mut jobs = vec![first];
        let mut bytes = jobs[0].p.payload.len();
        while jobs.len() < PRODUCE_BATCH_MAX_ITEMS && bytes < PRODUCE_BATCH_MAX_BYTES {
            match rx.try_recv() {
                Ok(job) => {
                    bytes += job.p.payload.len();
                    jobs.push(job);
                }
                Err(_) => break,
            }
        }

        let ts_ms = now_ms();
        let mut items = Vec::with_capacity(jobs.len());
        let mut replies = Vec::with_capacity(jobs.len());
        let mut topics: Vec<String> = Vec::new();
        for job in jobs {
            if !topics.contains(&job.p.topic) {
                topics.push(job.p.topic.clone());
            }
            items.push(ProduceItem {
                topic: TopicId(job.p.topic),
                priority: Priority(job.p.priority.min(MAX_PRIORITY) as u8),
                content_type: ct_from_u32(job.p.content_type),
                payload: job.p.payload,
                producer_id: job.p.producer_id,
                seq: job.p.seq,
                ts_ms,
            });
            replies.push(job.reply);
        }

        match raft.client_write(AppRequest::ProduceMany { items }).await {
            Ok(resp) => match resp.data {
                AppResponse::ProducedMany { offsets } => {
                    for (reply, offset) in replies.into_iter().zip(offsets) {
                        let _ = reply.send(Response {
                            kind: Some(response::Kind::Produced(proto::Produced { offset })),
                        });
                    }
                    let map = notifiers.lock().unwrap();
                    for topic in &topics {
                        if let Some(notifier) = map.get(topic) {
                            notifier.notify_waiters();
                        }
                    }
                }
                other => {
                    let resp = resp_error("internal", format!("unexpected batch response: {other:?}"));
                    for reply in replies {
                        let _ = reply.send(resp.clone());
                    }
                }
            },
            Err(e) => {
                let resp = map_raft_error(e);
                for reply in replies {
                    let _ = reply.send(resp.clone());
                }
            }
        }
    }
}

type AckGroup = (Vec<LeaseId>, Vec<oneshot::Sender<Response>>);

async fn ack_batcher(raft: HermesRaft, mut rx: mpsc::Receiver<AckJob>) {
    while let Some(first) = rx.recv().await {
        let mut jobs = vec![first];
        while jobs.len() < ACK_BATCH_MAX {
            match rx.try_recv() {
                Ok(job) => jobs.push(job),
                Err(_) => break,
            }
        }

        let mut groups: BTreeMap<(String, String, bool), AckGroup> = BTreeMap::new();
        for job in jobs {
            let entry = groups.entry((job.topic, job.group, job.nack)).or_default();
            entry.0.push(job.lease_id);
            if let Some(reply) = job.reply {
                entry.1.push(reply);
            }
        }

        for ((topic, group, nack), (lease_ids, replies)) in groups {
            let app = if nack {
                AppRequest::NackMany {
                    topic: TopicId(topic),
                    group: GroupId(group),
                    lease_ids,
                }
            } else {
                AppRequest::AckMany {
                    topic: TopicId(topic),
                    group: GroupId(group),
                    lease_ids,
                }
            };
            let resp = write_and_map(&raft, app).await;
            for reply in replies {
                let _ = reply.send(resp.clone());
            }
        }
    }
}

async fn handle_poll(ctx: &Ctx, p: proto::Poll) -> Response {
    let is_leader = {
        let metrics = ctx.raft.metrics();
        let m = metrics.borrow();
        m.current_leader == Some(m.id)
    };
    if !is_leader {
        return resp_not_leader(leader_addr(&ctx.raft));
    }

    let topic = TopicId(p.topic.clone());
    let group = GroupId(p.group.clone());
    let max = if p.max == 0 { DEFAULT_POLL_MAX } else { p.max.min(MAX_POLL_MAX) };
    let visibility = if p.visibility_timeout_ms == 0 {
        DEFAULT_VISIBILITY_MS
    } else {
        p.visibility_timeout_ms
    };
    let wait_ms = p.wait_ms.min(MAX_WAIT_MS);
    let deadline = Instant::now() + Duration::from_millis(wait_ms);

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
                max,
                visibility_timeout_ms: visibility,
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
                    let lease_ids = items.iter().map(|d| d.lease_id).collect();
                    let _ = ctx
                        .raft
                        .client_write(AppRequest::AckMany {
                            topic: topic.clone(),
                            group: group.clone(),
                            lease_ids,
                        })
                        .await;
                }
                return resp_polled(items);
            }
        }

        if wait_ms == 0 || Instant::now() >= deadline {
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

async fn enqueue_ack(ctx: &Ctx, topic: String, group: String, lease_id: u64, nack: bool) -> Response {
    let (reply, reply_rx) = oneshot::channel();
    let job = AckJob {
        topic,
        group,
        lease_id,
        nack,
        reply: Some(reply),
    };
    if ctx.ack_tx.send(job).await.is_err() {
        return resp_error("internal", "ack batcher unavailable".to_string());
    }
    match reply_rx.await {
        Ok(resp) => resp,
        Err(_) => resp_error("internal", "ack batcher dropped the request".to_string()),
    }
}

async fn handle_request(ctx: &Ctx, req: Request) -> Response {
    let Some(kind) = req.kind else {
        return resp_error("bad_request", "empty request".to_string());
    };

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
        request::Kind::Ack(a) => enqueue_ack(ctx, a.topic, a.group, a.lease_id, false).await,
        request::Kind::Nack(a) => enqueue_ack(ctx, a.topic, a.group, a.lease_id, true).await,
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
            let topic = c.topic.clone();
            let resp = write_and_map(raft, AppRequest::DeleteTopic { topic: TopicId(c.topic) }).await;
            if matches!(resp.kind, Some(response::Kind::Ok(_))) {
                ctx.notifiers.lock().unwrap().remove(&topic);
                ctx.limiter.forget(&topic);
            }
            resp
        }
        request::Kind::Subscribe(_) => {
            resp_error("bad_request", "subscribe takes over its connection".to_string())
        }
    }
}

async fn handle_subscribe(
    ctx: Ctx,
    mut read_half: OwnedReadHalf,
    mut write_half: OwnedWriteHalf,
    sub: proto::Subscribe,
) -> io::Result<()> {
    let is_leader = {
        let metrics = ctx.raft.metrics();
        let m = metrics.borrow();
        m.current_leader == Some(m.id)
    };
    if !is_leader {
        let resp = resp_not_leader(leader_addr(&ctx.raft));
        write_frame(&mut write_half, &resp.encode_to_vec()).await?;
        return Ok(());
    }

    let topic = TopicId(sub.topic.clone());
    let group = GroupId(sub.group.clone());
    let prefetch = sub.prefetch.clamp(1, MAX_POLL_MAX) as usize;
    let auto = sub.ack_mode == "auto";
    let visibility = if sub.visibility_timeout_ms == 0 {
        DEFAULT_VISIBILITY_MS
    } else {
        sub.visibility_timeout_ms
    };

    let tracking: Arc<StdMutex<SubTracking>> = Arc::new(StdMutex::new(SubTracking::default()));
    let freed = Arc::new(Notify::new());
    let closed = Arc::new(AtomicBool::new(false));

    let reader = {
        let ctx = ctx.clone();
        let topic = topic.clone();
        let group = group.clone();
        let tracking = tracking.clone();
        let freed = freed.clone();
        let closed = closed.clone();
        tokio::spawn(async move {
            loop {
                let bytes = match read_frame(&mut read_half).await {
                    Ok(b) => b,
                    Err(_) => break,
                };
                if auto {
                    continue;
                }
                let Ok(req) = Request::decode(bytes.as_slice()) else {
                    continue;
                };
                match req.kind {
                    Some(request::Kind::Ack(a)) => {
                        let acked_until = now_ms().saturating_add(ACK_GRACE_MS);
                        let resolved = tracking
                            .lock()
                            .unwrap()
                            .resolve(a.lease_id, Some(acked_until));
                        let _ = ctx
                            .ack_tx
                            .send(AckJob {
                                topic: topic.0.clone(),
                                group: group.0.clone(),
                                lease_id: resolved.unwrap_or(a.lease_id),
                                nack: false,
                                reply: None,
                            })
                            .await;
                        if resolved.is_some() {
                            freed.notify_one();
                        }
                    }
                    Some(request::Kind::Nack(a)) => {
                        let resolved = tracking.lock().unwrap().resolve(a.lease_id, None);
                        let _ = ctx
                            .ack_tx
                            .send(AckJob {
                                topic: topic.0.clone(),
                                group: group.0.clone(),
                                lease_id: resolved.unwrap_or(a.lease_id),
                                nack: true,
                                reply: None,
                            })
                            .await;
                        if resolved.is_some() {
                            freed.notify_one();
                        }
                    }
                    _ => {}
                }
            }
            closed.store(true, Ordering::SeqCst);
            freed.notify_one();
        })
    };

    loop {
        if closed.load(Ordering::SeqCst) {
            break;
        }
        {
            let metrics = ctx.raft.metrics();
            let m = metrics.borrow();
            if m.current_leader != Some(m.id) {
                break;
            }
        }

        let cur = tracking.lock().unwrap().live_count(now_ms(), visibility);
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
                            if auto && !items.is_empty() {
                                let lease_ids = items.iter().map(|d| d.lease_id).collect();
                                let _ = ctx
                                    .raft
                                    .client_write(AppRequest::AckMany {
                                        topic: topic.clone(),
                                        group: group.clone(),
                                        lease_ids,
                                    })
                                    .await;
                            }
                            for d in items {
                                pushed_any = true;
                                if !auto {
                                    let now = now_ms();
                                    let deadline = now.saturating_add(visibility);
                                    let delivery = tracking
                                        .lock()
                                        .unwrap()
                                        .deliver(d.offset, d.lease_id, deadline, now);
                                    match delivery {
                                        Delivery::Refreshed => continue,
                                        Delivery::AlreadyAcked => {
                                            let _ = ctx
                                                .ack_tx
                                                .send(AckJob {
                                                    topic: topic.0.clone(),
                                                    group: group.0.clone(),
                                                    lease_id: d.lease_id,
                                                    nack: false,
                                                    reply: None,
                                                })
                                                .await;
                                            continue;
                                        }
                                        Delivery::Push => {}
                                    }
                                }
                                let frame = resp_polled(vec![d]);
                                if write_frame(&mut write_half, &frame.encode_to_vec())
                                    .await
                                    .is_err()
                                {
                                    reader.abort();
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

    reader.abort();
    Ok(())
}

pub async fn serve_clients(raft: HermesRaft, sm: StateMachineStore, listener: TcpListener) {
    let notifiers: Arc<StdMutex<HashMap<String, Arc<Notify>>>> =
        Arc::new(StdMutex::new(HashMap::new()));
    let (produce_tx, produce_rx) = mpsc::channel(PRODUCE_QUEUE_DEPTH);
    tokio::spawn(produce_batcher(raft.clone(), notifiers.clone(), produce_rx));
    let (ack_tx, ack_rx) = mpsc::channel(ACK_QUEUE_DEPTH);
    tokio::spawn(ack_batcher(raft.clone(), ack_rx));
    let ctx = Ctx {
        raft,
        sm,
        limiter: Arc::new(RateLimiter::default()),
        notifiers,
        produce_tx,
        ack_tx,
    };
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let _ = stream.set_nodelay(true);
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

async fn handle_client(ctx: Ctx, stream: TcpStream) -> io::Result<()> {
    let (mut read_half, write_half) = stream.into_split();
    let (tx, mut rx) = mpsc::channel::<oneshot::Receiver<Response>>(PIPELINE_DEPTH);

    let writer = tokio::spawn(async move {
        let mut write_half = write_half;
        while let Some(pending) = rx.recv().await {
            let Ok(resp) = pending.await else {
                break;
            };
            if write_frame(&mut write_half, &resp.encode_to_vec()).await.is_err() {
                break;
            }
        }
        write_half
    });

    let sub = loop {
        let bytes = match read_frame(&mut read_half).await {
            Ok(b) => b,
            Err(_) => break None,
        };
        let req = match Request::decode(bytes.as_slice()) {
            Ok(req) => req,
            Err(e) => {
                let (otx, orx) = oneshot::channel();
                let _ = otx.send(resp_error("bad_request", e.to_string()));
                if tx.send(orx).await.is_err() {
                    break None;
                }
                continue;
            }
        };
        if let Some(request::Kind::Subscribe(sub)) = req.kind {
            break Some(sub);
        }
        let (otx, orx) = oneshot::channel();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let _ = otx.send(handle_request(&ctx, req).await);
        });
        if tx.send(orx).await.is_err() {
            break None;
        }
    };

    drop(tx);
    let Ok(write_half) = writer.await else {
        return Ok(());
    };
    match sub {
        Some(sub) => handle_subscribe(ctx, read_half, write_half, sub).await,
        None => Ok(()),
    }
}
