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
use tokio::sync::{mpsc, oneshot, Notify, Semaphore};

use crate::engine::{HermesRaft, StateMachineStore};
use crate::frame::{read_frame_deadline, write_frame, BODY_TIMEOUT, MAX_CLIENT_FRAME};
use crate::queue::{MAX_NAME_BYTES, MAX_PRODUCER_ID_BYTES, MAX_TOPICS, MAX_VISIBILITY_MS};
use crate::raft::{AppRequest, AppResponse, Delivered, ProduceItem};
use crate::types::{ContentType, GroupId, LeaseId, NodeId, Offset, Priority, TopicId};

pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const DEFAULT_POLL_MAX: u32 = 16;
const MAX_POLL_MAX: u32 = 1024;
const DEFAULT_VISIBILITY_MS: u64 = 30_000;
const MAX_WAIT_MS: u64 = 300_000;
/// A connection costs a task, a socket and a read buffer, so cap how many one
/// listener will hold at once.
const MAX_CLIENT_CONNS: usize = 4096;
const MAX_PRIORITY: u32 = 7;
#[doc(hidden)]
pub const SUBSCRIBE_MAX_LEASE_REFRESHES: u32 = 2;
#[doc(hidden)]
pub const SUBSCRIBE_MAX_LEASE_ALIASES: usize = 8;
const PIPELINE_DEPTH: usize = 32;
const PRODUCE_QUEUE_DEPTH: usize = 1024;
const PRODUCE_BATCH_MAX_ITEMS: usize = 256;
const PRODUCE_BATCH_MAX_BYTES: usize = 512 * 1024;
const PRODUCE_WRITE_PIPELINE: usize = 4;
const ACK_QUEUE_DEPTH: usize = 1024;
const ACK_BATCH_MAX: usize = 1024;
const ACK_WRITE_PIPELINE: usize = 4;
const ACK_GRACE_MS: u64 = 5_000;

struct OffsetTrack {
    lease_id: LeaseId,
    deadline_ms: u64,
    refreshes: u32,
    leases: Vec<LeaseId>,
}

#[doc(hidden)]
#[derive(Debug, PartialEq, Eq)]
pub enum Delivery {
    Push,
    Refreshed,
    AlreadyAcked,
}

#[doc(hidden)]
#[derive(Default)]
pub struct SubTracking {
    by_offset: HashMap<Offset, OffsetTrack>,
    lease_to_offset: HashMap<LeaseId, Offset>,
    acked_recently: HashMap<Offset, u64>,
}

impl SubTracking {
    pub fn deliver(&mut self, offset: Offset, lease_id: LeaseId, deadline_ms: u64, now_ms: u64) -> Delivery {
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

    pub fn resolve(&mut self, lease_id: LeaseId, acked_until: Option<u64>) -> Option<LeaseId> {
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

    pub fn live_count(&mut self, now_ms: u64, grace_ms: u64) -> usize {
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

    /// Internal consistency check over the three maps, exposed for the fuzz targets.
    #[doc(hidden)]
    pub fn debug_check_invariants(&self) -> Result<(), String> {
        for (lease, offset) in &self.lease_to_offset {
            let Some(track) = self.by_offset.get(offset) else {
                return Err(format!("lease {lease} points at unknown offset {offset}"));
            };
            if !track.leases.contains(lease) {
                return Err(format!("offset {offset} does not list its alias lease {lease}"));
            }
        }
        for (offset, track) in &self.by_offset {
            if track.leases.len() > SUBSCRIBE_MAX_LEASE_ALIASES + 1 {
                return Err(format!(
                    "offset {offset} holds {} lease aliases",
                    track.leases.len()
                ));
            }
            if !track.leases.contains(&track.lease_id) {
                return Err(format!("offset {offset} current lease is not in its alias list"));
            }
        }
        Ok(())
    }

    /// Entry counts of the three maps, exposed for the fuzz targets.
    #[doc(hidden)]
    pub fn debug_sizes(&self) -> (usize, usize, usize) {
        (
            self.by_offset.len(),
            self.lease_to_offset.len(),
            self.acked_recently.len(),
        )
    }
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

#[doc(hidden)]
#[derive(Default)]
pub struct RateLimiter {
    buckets: StdMutex<HashMap<String, Bucket>>,
}

impl RateLimiter {
    pub fn take(
        &self,
        topic: &str,
        rate_per_sec: f64,
        burst: f64,
        want: u32,
    ) -> std::result::Result<u32, u64> {
        // Asking for nothing is granted, not rate limited: the rate-limited
        // branch below would compute a zero retry delay and invite a caller to
        // spin.
        if want == 0 {
            return Ok(0);
        }
        let mut buckets = crate::lock(&self.buckets);
        let now = Instant::now();
        let bucket = buckets.entry(topic.to_string()).or_insert(Bucket {
            tokens: burst,
            last: now,
        });
        let elapsed = now.duration_since(bucket.last).as_secs_f64();
        bucket.last = now;
        bucket.tokens = (bucket.tokens + elapsed * rate_per_sec).min(burst);
        let grant = (bucket.tokens.floor() as u32).min(want);
        if grant == 0 {
            let needed = 1.0 - bucket.tokens;
            let retry_ms = if rate_per_sec > 0.0 {
                (needed / rate_per_sec * 1000.0).ceil() as u64
            } else {
                1000
            };
            return Err(retry_ms);
        }
        bucket.tokens -= grant as f64;
        Ok(grant)
    }

    pub fn refund(&self, topic: &str, amount: u32, burst: f64) {
        if amount == 0 {
            return;
        }
        let mut buckets = crate::lock(&self.buckets);
        if let Some(bucket) = buckets.get_mut(topic) {
            bucket.tokens = (bucket.tokens + amount as f64).min(burst);
        }
    }

    fn forget(&self, topic: &str) {
        crate::lock(&self.buckets).remove(topic);
    }

    /// Current token count for a topic, exposed for the fuzz targets.
    #[doc(hidden)]
    pub fn debug_tokens(&self, topic: &str) -> Option<f64> {
        crate::lock(&self.buckets).get(topic).map(|b| b.tokens)
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
        let mut map = crate::lock(&self.notifiers);
        gc_notifiers(&mut map);
        map.entry(topic.to_string())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }
}

const NOTIFIER_GC_THRESHOLD: usize = 4096;

fn gc_notifiers(map: &mut HashMap<String, Arc<Notify>>) {
    if map.len() > NOTIFIER_GC_THRESHOLD {
        map.retain(|_, n| Arc::strong_count(n) > 1);
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
        AppResponse::Rejected { reason } => resp_error("rejected", reason),
        AppResponse::ProducedMany { .. } => {
            resp_error("internal", "unexpected batched produce response".to_string())
        }
    }
}

fn check_len(kind: &str, value: &str, max: usize) -> Result<(), Response> {
    if value.len() > max {
        return Err(resp_error(
            "bad_request",
            format!("{kind} is {} bytes; max is {max}", value.len()),
        ));
    }
    Ok(())
}

/// Turn away a request whose names the state machine would refuse anyway, so a
/// bad one costs a reply instead of a replicated write.
fn check_names(topic: &str, group: &str) -> Option<Response> {
    check_len("topic name", topic, MAX_NAME_BYTES)
        .and_then(|()| check_len("group name", group, MAX_NAME_BYTES))
        .err()
}

/// [`check_names`] plus the topic limit, for the requests that can create one.
/// Poll, commit, ack and nack are not among them: they only ever look a topic
/// up, so they pay for the name check alone.
fn check_new_topic(ctx: &Ctx, topic: &str) -> Option<Response> {
    if let Some(resp) = check_names(topic, "") {
        return Some(resp);
    }
    if !ctx.sm.topic_admits(topic) {
        return Some(resp_error(
            "rejected",
            format!("topic limit of {MAX_TOPICS} reached"),
        ));
    }
    None
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
    if let Err(resp) = check_len("producer id", &p.producer_id, MAX_PRODUCER_ID_BYTES) {
        return resp;
    }
    if let Some(resp) = check_new_topic(ctx, &p.topic) {
        return resp;
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
    let permits = Arc::new(Semaphore::new(PRODUCE_WRITE_PIPELINE));
    while let Some(first) = rx.recv().await {
        let permit = permits.clone().acquire_owned().await.unwrap();
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
        let raft = raft.clone();
        let notifiers = notifiers.clone();
        tokio::spawn(async move {
            write_produce_batch(&raft, &notifiers, jobs).await;
            drop(permit);
        });
    }
}

async fn write_produce_batch(
    raft: &HermesRaft,
    notifiers: &StdMutex<HashMap<String, Arc<Notify>>>,
    jobs: Vec<ProduceJob>,
) {
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
                let map = crate::lock(notifiers);
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

type AckGroup = (Vec<LeaseId>, Vec<oneshot::Sender<Response>>);

async fn ack_batcher(
    raft: HermesRaft,
    notifiers: Arc<StdMutex<HashMap<String, Arc<Notify>>>>,
    mut rx: mpsc::Receiver<AckJob>,
) {
    let permits = Arc::new(Semaphore::new(ACK_WRITE_PIPELINE));
    while let Some(first) = rx.recv().await {
        let permit = permits.clone().acquire_owned().await.unwrap();
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

        let raft = raft.clone();
        let notifiers = notifiers.clone();
        tokio::spawn(async move {
            write_ack_groups(&raft, &notifiers, groups).await;
            drop(permit);
        });
    }
}

async fn write_ack_groups(
    raft: &HermesRaft,
    notifiers: &StdMutex<HashMap<String, Arc<Notify>>>,
    groups: BTreeMap<(String, String, bool), AckGroup>,
) {
    for ((topic, group, nack), (lease_ids, replies)) in groups {
        let app = if nack {
            AppRequest::NackMany {
                topic: TopicId(topic.clone()),
                group: GroupId(group),
                lease_ids,
            }
        } else {
            AppRequest::AckMany {
                topic: TopicId(topic.clone()),
                group: GroupId(group),
                lease_ids,
            }
        };
        let resp = write_and_map(raft, app).await;
        let ok = matches!(resp.kind, Some(response::Kind::Ok(_)));
        for reply in replies {
            let _ = reply.send(resp.clone());
        }
        if nack && ok {
            if let Some(notifier) = crate::lock(notifiers).get(&topic) {
                notifier.notify_waiters();
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

    if let Some(resp) = check_names(&p.topic, &p.group) {
        return resp;
    }
    let topic = TopicId(p.topic.clone());
    let group = GroupId(p.group.clone());
    let max = if p.max == 0 { DEFAULT_POLL_MAX } else { p.max.min(MAX_POLL_MAX) };
    let visibility = if p.visibility_timeout_ms == 0 {
        DEFAULT_VISIBILITY_MS
    } else {
        p.visibility_timeout_ms.min(MAX_VISIBILITY_MS)
    };
    let wait_ms = p.wait_ms.min(MAX_WAIT_MS);
    let deadline = Instant::now() + Duration::from_millis(wait_ms);

    loop {
        let notifier = ctx.notifier(&p.topic);
        let notified = notifier.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        if ctx.sm.has_deliverable(&p.topic, &p.group, now_ms()) {
            let rate_cfg = ctx.sm.rate_config(&p.topic);
            let grant = match rate_cfg {
                Some((rate_milli, burst)) => {
                    let rate = rate_milli as f64 / 1000.0;
                    match ctx.limiter.take(&p.topic, rate, burst.max(1) as f64, max) {
                        Ok(g) => g,
                        Err(retry) => return resp_rate_limited(retry),
                    }
                }
                None => max,
            };
            let app = if p.ack_mode == "auto" {
                AppRequest::PollAuto {
                    topic: topic.clone(),
                    group: group.clone(),
                    max: grant,
                    visibility_timeout_ms: visibility,
                    ts_ms: now_ms(),
                }
            } else {
                AppRequest::Poll {
                    topic: topic.clone(),
                    group: group.clone(),
                    max: grant,
                    visibility_timeout_ms: visibility,
                    ts_ms: now_ms(),
                }
            };
            let resp = match ctx.raft.client_write(app).await {
                Ok(r) => r,
                Err(e) => return map_raft_error(e),
            };
            let items = match resp.data {
                AppResponse::Polled { items } => items,
                other => return app_response_to_proto(other),
            };
            if let Some((_, burst)) = rate_cfg {
                ctx.limiter.refund(
                    &p.topic,
                    grant.saturating_sub(items.len() as u32),
                    burst.max(1) as f64,
                );
            }
            if !items.is_empty() {
                return resp_polled(items);
            }
        }

        if wait_ms == 0 || Instant::now() >= deadline {
            return resp_polled(Vec::new());
        }
        let wait = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(200));
        tokio::select! {
            _ = notified => {}
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
        request::Kind::Auth(_) => resp_ok(),
        request::Kind::SetRateLimit(s) => {
            if let Some(resp) = check_new_topic(ctx, &s.topic) {
                return resp;
            }
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
            if let Some(resp) = check_new_topic(ctx, &s.topic) {
                return resp;
            }
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
            if let Some(resp) = check_names(&a.topic, &a.group) {
                return resp;
            }
            enqueue_ack(ctx, a.topic, a.group, a.lease_id, false).await
        }
        request::Kind::Nack(a) => {
            if let Some(resp) = check_names(&a.topic, &a.group) {
                return resp;
            }
            enqueue_ack(ctx, a.topic, a.group, a.lease_id, true).await
        }
        request::Kind::Commit(c) => {
            if let Some(resp) = check_names(&c.topic, &c.group) {
                return resp;
            }
            write_and_map(
                raft,
                AppRequest::CommitOffsetAt {
                    topic: TopicId(c.topic),
                    group: GroupId(c.group),
                    offset: c.offset,
                    ts_ms: now_ms(),
                },
            )
            .await
        }
        request::Kind::CreateTopic(c) => {
            if let Some(resp) = check_new_topic(ctx, &c.topic) {
                return resp;
            }
            write_and_map(raft, AppRequest::CreateTopic { topic: TopicId(c.topic) }).await
        }
        request::Kind::DeleteTopic(c) => {
            let topic = c.topic.clone();
            let resp = write_and_map(raft, AppRequest::DeleteTopic { topic: TopicId(c.topic) }).await;
            if matches!(resp.kind, Some(response::Kind::Ok(_))) {
                crate::lock(&ctx.notifiers).remove(&topic);
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

    if let Some(resp) = check_names(&sub.topic, &sub.group) {
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
        sub.visibility_timeout_ms.min(MAX_VISIBILITY_MS)
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
                let bytes =
                    match read_frame_deadline(&mut read_half, MAX_CLIENT_FRAME, BODY_TIMEOUT).await {
                        Ok(b) => b,
                        Err(_) => break,
                    };
                if auto {
                    continue;
                }
                let Ok(req) = Request::decode(bytes) else {
                    continue;
                };
                match req.kind {
                    Some(request::Kind::Ack(a)) => {
                        let acked_until = now_ms().saturating_add(ACK_GRACE_MS);
                        let resolved = crate::lock(&tracking).resolve(a.lease_id, Some(acked_until));
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
                        let resolved = crate::lock(&tracking).resolve(a.lease_id, None);
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

        let notifier = ctx.notifier(&sub.topic);
        let notified = notifier.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let cur = crate::lock(&tracking).live_count(now_ms(), visibility);
        let mut pushed_any = false;
        if cur < prefetch && ctx.sm.has_deliverable(&sub.topic, &sub.group, now_ms()) {
            let want = (prefetch - cur) as u32;
            let rate_cfg = ctx.sm.rate_config(&sub.topic);
            let grant = match rate_cfg {
                Some((rate_milli, burst)) => ctx
                    .limiter
                    .take(&sub.topic, rate_milli as f64 / 1000.0, burst.max(1) as f64, want)
                    .ok(),
                None => Some(want),
            };
            if let Some(grant) = grant {
                let app = if auto {
                    AppRequest::PollAuto {
                        topic: topic.clone(),
                        group: group.clone(),
                        max: grant,
                        visibility_timeout_ms: visibility,
                        ts_ms: now_ms(),
                    }
                } else {
                    AppRequest::Poll {
                        topic: topic.clone(),
                        group: group.clone(),
                        max: grant,
                        visibility_timeout_ms: visibility,
                        ts_ms: now_ms(),
                    }
                };
                match ctx.raft.client_write(app).await {
                    Ok(r) => {
                        if let AppResponse::Polled { items } = r.data {
                            if let Some((_, burst)) = rate_cfg {
                                ctx.limiter.refund(
                                    &sub.topic,
                                    grant.saturating_sub(items.len() as u32),
                                    burst.max(1) as f64,
                                );
                            }
                            for d in items {
                                pushed_any = true;
                                if !auto {
                                    let now = now_ms();
                                    let deadline = now.saturating_add(visibility);
                                    let delivery =
                                        crate::lock(&tracking).deliver(d.offset, d.lease_id, deadline, now);
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
        tokio::select! {
            _ = freed.notified() => {}
            _ = notified => {}
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }

    reader.abort();
    Ok(())
}

/// Serve the client port with no authentication: anyone who can open it can
/// produce, consume, reconfigure topics and bootstrap an un-bootstrapped node.
pub async fn serve_clients(raft: HermesRaft, sm: StateMachineStore, listener: TcpListener) {
    serve_clients_authenticated(raft, sm, listener, None).await
}

/// Serve the client port, requiring `token` when one is set. A connection must
/// send a matching `Auth` request before any other request is served.
pub async fn serve_clients_authenticated(
    raft: HermesRaft,
    sm: StateMachineStore,
    listener: TcpListener,
    token: Option<String>,
) {
    if token.is_none() {
        tracing::warn!(
            "client port has no token: any host that can reach it can read, write and reconfigure"
        );
    }
    let token = Arc::new(token);
    let notifiers: Arc<StdMutex<HashMap<String, Arc<Notify>>>> =
        Arc::new(StdMutex::new(HashMap::new()));
    let (produce_tx, produce_rx) = mpsc::channel(PRODUCE_QUEUE_DEPTH);
    tokio::spawn(produce_batcher(raft.clone(), notifiers.clone(), produce_rx));
    let (ack_tx, ack_rx) = mpsc::channel(ACK_QUEUE_DEPTH);
    tokio::spawn(ack_batcher(raft.clone(), notifiers.clone(), ack_rx));
    let ctx = Ctx {
        raft,
        sm,
        limiter: Arc::new(RateLimiter::default()),
        notifiers,
        produce_tx,
        ack_tx,
    };
    let permits = Arc::new(Semaphore::new(MAX_CLIENT_CONNS));
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    tracing::warn!(
                        "client connection from {addr} refused: {MAX_CLIENT_CONNS} already open"
                    );
                    continue;
                };
                let _ = stream.set_nodelay(true);
                let ctx = ctx.clone();
                let token = token.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_client(ctx, stream, token.as_ref().as_deref()).await {
                        tracing::debug!("client connection closed: {e}");
                    }
                    drop(permit);
                });
            }
            Err(e) => tracing::warn!("client accept error: {e}"),
        }
    }
}

async fn handle_client(ctx: Ctx, stream: TcpStream, token: Option<&str>) -> io::Result<()> {
    let (mut read_half, write_half) = stream.into_split();
    let (tx, mut rx) = mpsc::channel::<oneshot::Receiver<Response>>(PIPELINE_DEPTH);
    let mut authed = token.is_none();

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
        let bytes = match read_frame_deadline(&mut read_half, MAX_CLIENT_FRAME, BODY_TIMEOUT).await {
            Ok(b) => b,
            Err(_) => break None,
        };
        let req = match Request::decode(bytes) {
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
        if !authed {
            // One chance per connection: a wrong or missing token answers once
            // and the connection closes, so guessing costs a new handshake.
            let offered = match &req.kind {
                Some(request::Kind::Auth(a)) => Some(a.token.as_str()),
                _ => None,
            };
            let ok = offered.is_some_and(|offered| {
                token.is_some_and(|expected| {
                    crate::secret_eq(offered.as_bytes(), expected.as_bytes())
                })
            });
            let (otx, orx) = oneshot::channel();
            let _ = otx.send(if ok {
                resp_ok()
            } else {
                resp_error("unauthorized", "authenticate first".to_string())
            });
            let _ = tx.send(orx).await;
            if !ok {
                break None;
            }
            authed = true;
            continue;
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_for_zero_tokens_is_granted_not_rate_limited() {
        let limiter = RateLimiter::default();
        assert_eq!(
            limiter.take("t", 10.0, 100.0, 0),
            Ok(0),
            "asking for nothing must not answer with a zero retry delay"
        );
        assert_eq!(limiter.take("t", 10.0, 100.0, 1), Ok(1));
    }

    #[test]
    fn secret_eq_matches_only_the_same_bytes() {
        assert!(crate::secret_eq(b"token", b"token"));
        assert!(!crate::secret_eq(b"token", b"tokeN"));
        assert!(!crate::secret_eq(b"token", b"token "));
        assert!(!crate::secret_eq(b"", b"token"));
    }

    #[test]
    fn gc_notifiers_drops_entries_without_waiters() {
        let mut map: HashMap<String, Arc<Notify>> = HashMap::new();
        for i in 0..=NOTIFIER_GC_THRESHOLD {
            map.insert(format!("t{i}"), Arc::new(Notify::new()));
        }
        let live = map.get("t0").cloned().unwrap();
        gc_notifiers(&mut map);
        assert!(map.contains_key("t0"), "a topic with a live waiter must survive");
        assert_eq!(map.len(), 1, "notifiers with no waiters are reclaimed");
        drop(live);
    }

    #[test]
    fn gc_notifiers_is_a_noop_below_threshold() {
        let mut map: HashMap<String, Arc<Notify>> = HashMap::new();
        map.insert("t".to_string(), Arc::new(Notify::new()));
        gc_notifiers(&mut map);
        assert_eq!(map.len(), 1);
    }
}
