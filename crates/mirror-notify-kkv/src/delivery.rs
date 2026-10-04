//! Per-address delivery for the `source-consume` trigger.
//!
//! A kkv-v1 push is one-shot and unacknowledged on the consumer side, and
//! kafka-keyvalue's 2-6 replicas each pushed every update, so a push one
//! replica lost was healed by a sibling's. A single mirror-v3 replica has
//! no sibling, so it must deliver reliably itself:
//!
//! - Every address of every target keeps its own undelivered key set. A
//!   batch is merged into each address's set; an address that fails keeps
//!   its set (merged with newer batches) and is retried until it accepts,
//!   or until it has left discovery for [`ADDRESS_GONE_AFTER`].
//! - Any status other than 2xx is a failure.
//! - Addresses that are not ready yet are kept, not skipped: a target
//!   Service that publishes not-ready addresses lets a starting consumer
//!   get every update from its first resolution on, retried until its
//!   server listens.
//! - Delivery runs on its own task, never on the consume loop, so a slow
//!   or dead consumer cannot stall the cache.
//! - Nothing is sent before the mirror has caught up to its bootstrap
//!   watermark: a notified consumer re-reads the key at once, and the
//!   cache answers 503 until then.
//!
//! The outcome matrix still decides what a failure means: `final: fail`
//! ends the mirror after the retry budget, `final: accept` counts the
//! batch as delivered after it. `final: skip` no longer drops a batch: it
//! keeps it for the address, retried with backoff while the address
//! exists (the default; it replaces the drop because
//! with one replica a dropped push is never healed).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::{FuturesUnordered, StreamExt};
use indexmap::{IndexMap, IndexSet};
use mirror_config::{FinalAction, NotifyOutcomes, NotifyRetry};
use mirror_core::{AckSink, CacheState, NotifyError};
use reqwest::Client;
use tokio::sync::{mpsc, Mutex as TokioMutex, Notify as TokioNotify};
use url::Url;

use crate::{classify, DnsAResolver, Endpoint, FanOutMode, KkvV1Payload, Outcome, OutcomesLookup};

/// An address absent from every resolution for this long leaves
/// delivery, and its undelivered keys with it: the pod is gone, and its
/// replacement reads the cache when it starts. The grace keeps a DNS
/// hiccup or a resolution that briefly misses a pod from dropping keys.
pub const ADDRESS_GONE_AFTER: Duration = Duration::from_secs(30);

/// Re-resolve a `dns-a` target at most this often while there is work,
/// so a consumer that starts gets the updates from its first resolution
/// on.
pub const RESOLVE_INTERVAL: Duration = Duration::from_secs(1);

/// A resolution that takes longer counts as failed for this round.
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(2);

/// The longest wait between two attempts to one address.
pub const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Bodies are cut below this size: the Node client's body parser
/// rejects bodies over 100 kB with 413.
pub const MAX_BODY_BYTES: usize = 64 * 1024;

/// How long a graceful shutdown keeps delivering what is pending.
/// What is left is re-delivered after the restart (the committed offset
/// only covers delivered batches).
pub const SHUTDOWN_DELIVERY_BUDGET: Duration = Duration::from_secs(10);

/// Poll interval while waiting for the mirror to catch up.
const CATCH_UP_POLL: Duration = Duration::from_millis(200);

/// Keys to notify and the highest source offset among their records.
#[derive(Debug, Clone, Default)]
pub(crate) struct Batch {
    pub keys: IndexSet<String>,
    pub high: u64,
}

impl Batch {
    fn merge(&mut self, other: &Batch) {
        for k in &other.keys {
            self.keys.insert(k.clone());
        }
        self.high = self.high.max(other.high);
    }
}

/// What the notifier shares with the deliverer task.
pub(crate) struct Shared {
    pub topic: String,
    pub partition: i32,
    pub outcomes: NotifyOutcomes,
    pub retry: NotifyRetry,
    pub resolver: Arc<dyn DnsAResolver>,
    pub cache_state: Arc<CacheState>,
    pub mirror_name: String,
    pub ack_sink: std::sync::OnceLock<Arc<dyn AckSink>>,
    pub error_state: Arc<TokioMutex<Option<NotifyError>>>,
    pub error_signal: Arc<TokioNotify>,
}

impl Shared {
    fn labels(&self) -> (String, String) {
        (self.topic.clone(), self.partition.to_string())
    }
}

struct AddrState {
    url: Url,
    label: String,
    /// Keys not yet sent to this address.
    pending: Option<Batch>,
    /// Keys being sent, and whether they are all it had pending.
    inflight: Option<(Batch, bool)>,
    /// Every record below this offset is delivered to this address, or
    /// was before its time.
    acked: u64,
    attempt: u32,
    next_attempt: Instant,
    missing_since: Option<Instant>,
}

impl AddrState {
    fn new(url: Url, label: String, acked: u64) -> Self {
        Self {
            url,
            label,
            pending: None,
            inflight: None,
            acked,
            attempt: 0,
            next_attempt: Instant::now(),
            missing_since: None,
        }
    }

    fn merge(&mut self, batch: &Batch) {
        match self.pending.as_mut() {
            Some(p) => p.merge(batch),
            None => self.pending = Some(batch.clone()),
        }
    }

    fn idle(&self) -> bool {
        self.pending.is_none() && self.inflight.is_none()
    }
}

struct TargetState {
    endpoint: Endpoint,
    addrs: HashMap<String, AddrState>,
    last_resolve: Option<Instant>,
}

struct PostResult {
    target: usize,
    addr: String,
    outcome: Outcome,
    error: String,
}

pub(crate) struct Deliverer {
    shared: Arc<Shared>,
    targets: Vec<TargetState>,
    /// One past the highest offset of any batch received.
    high_next: u64,
    acked_reported: u64,
}

impl Deliverer {
    pub fn new(shared: Arc<Shared>, endpoints: Vec<Endpoint>) -> Self {
        let targets = endpoints
            .into_iter()
            .map(|endpoint| TargetState {
                endpoint,
                addrs: HashMap::new(),
                last_resolve: None,
            })
            .collect();
        Self {
            shared,
            targets,
            high_next: 0,
            acked_reported: 0,
        }
    }

    /// Run until the channel closes and pending batches are delivered
    /// (or the shutdown budget is spent), or until a `final: fail`
    /// outcome ends delivery with an error, which is stashed for the
    /// supervisor.
    pub async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Batch>) {
        let mut inflight: FuturesUnordered<
            std::pin::Pin<Box<dyn std::future::Future<Output = PostResult> + Send>>,
        > = FuturesUnordered::new();
        let mut closed_at: Option<Instant> = None;
        loop {
            if let Some(at) = closed_at {
                let all_done = inflight.is_empty()
                    && self
                        .targets
                        .iter()
                        .all(|t| t.addrs.values().all(AddrState::idle));
                if all_done {
                    return;
                }
                if at.elapsed() >= SHUTDOWN_DELIVERY_BUDGET {
                    tracing::warn!(
                        topic = %self.shared.topic,
                        "notify: shutdown budget spent with undelivered keys; they are re-sent after the restart"
                    );
                    return;
                }
            }
            let wake = self.next_wakeup(closed_at.is_some());
            tokio::select! {
                batch = rx.recv(), if closed_at.is_none() => match batch {
                    Some(b) => self.accept(b).await,
                    None => closed_at = Some(Instant::now()),
                },
                Some(done) = inflight.next(), if !inflight.is_empty() => {
                    if let Err(e) = self.complete(done) {
                        *self.shared.error_state.lock().await = Some(e);
                        self.shared.error_signal.notify_one();
                        return;
                    }
                }
                _ = tokio::time::sleep_until(wake.into()) => {}
            }
            self.refresh_discovery(false).await;
            if self.caught_up() {
                for fut in self.start_due() {
                    inflight.push(fut);
                }
            }
            self.report_ack();
        }
    }

    fn caught_up(&self) -> bool {
        self.shared
            .cache_state
            .has_caught_up(&self.shared.mirror_name)
    }

    fn next_wakeup(&self, closing: bool) -> Instant {
        let now = Instant::now();
        let mut wake = now
            + if closing {
                CATCH_UP_POLL
            } else {
                RESOLVE_INTERVAL
            };
        if !self.caught_up() {
            return now + CATCH_UP_POLL;
        }
        for t in &self.targets {
            for a in t.addrs.values() {
                if a.pending.is_some() && a.inflight.is_none() {
                    wake = wake.min(a.next_attempt);
                }
                if let Some(since) = a.missing_since {
                    wake = wake.min(since + ADDRESS_GONE_AFTER);
                }
            }
        }
        wake.max(now)
    }

    async fn accept(&mut self, batch: Batch) {
        // Resolve first, so an address that appeared since the last
        // round gets this batch.
        self.refresh_discovery(true).await;
        self.high_next = self.high_next.max(batch.high + 1);
        for t in self.targets.iter_mut() {
            for a in t.addrs.values_mut() {
                a.merge(&batch);
            }
        }
        if self.targets.iter().all(|t| t.addrs.is_empty()) {
            tracing::debug!(
                topic = %self.shared.topic,
                high = batch.high,
                "notify: no target address; nothing to deliver"
            );
        }
    }

    /// Update each target's address set. `fan-out: none` has one static
    /// address. `dns-a` resolves at most every [`RESOLVE_INTERVAL`]
    /// (always when `force`, unless resolved this very instant); a failed
    /// resolution keeps the known set, an address missing from a good one
    /// is dropped after [`ADDRESS_GONE_AFTER`].
    async fn refresh_discovery(&mut self, force: bool) {
        let high_next = self.high_next;
        let shared = Arc::clone(&self.shared);
        for t in self.targets.iter_mut() {
            match &t.endpoint.fan_out {
                FanOutMode::None => {
                    let key = t.endpoint.url.to_string();
                    t.addrs.entry(key).or_insert_with(|| {
                        AddrState::new(
                            t.endpoint.url.clone(),
                            t.endpoint.target_host.clone(),
                            high_next,
                        )
                    });
                }
                FanOutMode::DnsA(state) => {
                    let due = match t.last_resolve {
                        None => true,
                        Some(at) => {
                            let since = at.elapsed();
                            since >= RESOLVE_INTERVAL || (force && since > Duration::ZERO)
                        }
                    };
                    if !due {
                        continue;
                    }
                    t.last_resolve = Some(Instant::now());
                    let resolved = tokio::time::timeout(
                        RESOLVE_TIMEOUT,
                        shared.resolver.resolve(&state.host, state.port),
                    )
                    .await;
                    let addrs: Vec<SocketAddr> = match resolved {
                        Ok(Ok(a)) => a,
                        Ok(Err(e)) => {
                            tracing::warn!(
                                host = %state.host,
                                error = %e,
                                known = t.addrs.len(),
                                "notify: dns-a resolution failed; keeping the known addresses"
                            );
                            continue;
                        }
                        Err(_) => {
                            tracing::warn!(
                                host = %state.host,
                                known = t.addrs.len(),
                                "notify: dns-a resolution timed out; keeping the known addresses"
                            );
                            continue;
                        }
                    };
                    let now = Instant::now();
                    let mut seen = std::collections::HashSet::new();
                    for sa in addrs {
                        let key = sa.to_string();
                        if !seen.insert(key.clone()) {
                            continue;
                        }
                        match t.addrs.get_mut(&key) {
                            Some(a) => a.missing_since = None,
                            None => {
                                let mut url = t.endpoint.url.clone();
                                url.set_ip_host(sa.ip())
                                    .expect("an IP is a valid host for an http(s) URL");
                                url.set_port(Some(sa.port()))
                                    .expect("an http(s) URL takes a port");
                                tracing::info!(
                                    host = %state.host,
                                    address = %key,
                                    "notify: new target address"
                                );
                                t.addrs
                                    .insert(key.clone(), AddrState::new(url, key, high_next));
                            }
                        }
                    }
                    t.addrs.retain(|key, a| {
                        if seen.contains(key) {
                            return true;
                        }
                        let since = *a.missing_since.get_or_insert(now);
                        if now.duration_since(since) < ADDRESS_GONE_AFTER || a.inflight.is_some() {
                            return true;
                        }
                        let undelivered = a.pending.as_ref().map(|p| p.keys.len()).unwrap_or(0);
                        tracing::info!(
                            host = %state.host,
                            address = %key,
                            undelivered,
                            "notify: target address left discovery"
                        );
                        false
                    });
                }
            }
        }
    }

    fn start_due(
        &mut self,
    ) -> Vec<std::pin::Pin<Box<dyn std::future::Future<Output = PostResult> + Send>>> {
        let now = Instant::now();
        let mut futs = Vec::new();
        for (ti, t) in self.targets.iter_mut().enumerate() {
            for (key, a) in t.addrs.iter_mut() {
                if a.inflight.is_some() || a.pending.is_none() || a.next_attempt > now {
                    continue;
                }
                let pending = a.pending.take().expect("checked above");
                let (chunk, rest) =
                    cut_to_body_limit(pending, &self.shared.topic, self.shared.partition);
                let complete = rest.is_none();
                a.pending = rest;
                a.attempt += 1;
                let body = payload_body(&chunk, &self.shared.topic, self.shared.partition);
                a.inflight = Some((chunk, complete));
                futs.push(post(
                    Arc::clone(&self.shared),
                    t.endpoint.client.clone(),
                    a.url.clone(),
                    a.label.clone(),
                    body,
                    a.attempt,
                    ti,
                    key.clone(),
                ));
            }
        }
        futs
    }

    fn complete(&mut self, r: PostResult) -> Result<(), NotifyError> {
        let shared = Arc::clone(&self.shared);
        let (topic_l, partition_l) = shared.labels();
        let t = &mut self.targets[r.target];
        let Some(a) = t.addrs.get_mut(&r.addr) else {
            return Ok(());
        };
        let (batch, complete) = a.inflight.take().expect("a result has its in-flight batch");
        let delivered = |a: &mut AddrState, batch: &Batch, complete: bool| {
            if complete {
                a.acked = a.acked.max(batch.high + 1);
            }
            a.attempt = 0;
            a.next_attempt = Instant::now();
        };
        if r.outcome == Outcome::TwoXx {
            delivered(a, &batch, complete);
            metrics::counter!(
                "mirror_v3_notify_batches_total",
                "topic" => topic_l,
                "partition" => partition_l,
                "result" => "ok",
            )
            .increment(1);
            return Ok(());
        }
        let policy = shared.outcomes.for_outcome(r.outcome);
        let budget_left = policy.retry && a.attempt < shared.retry.max_attempts;
        let keep = |a: &mut AddrState, batch: Batch| {
            let mut back = batch;
            if let Some(p) = a.pending.take() {
                back.merge(&p);
            }
            a.pending = Some(back);
            a.next_attempt = Instant::now() + backoff(shared.retry.backoff_ms, a.attempt);
        };
        match (policy.final_, budget_left) {
            (_, true) | (FinalAction::Skip, false) => {
                tracing::warn!(
                    address = %a.label,
                    attempt = a.attempt,
                    outcome = ?r.outcome,
                    reason = %r.error,
                    keys = batch.keys.len(),
                    "notify: delivery failed; kept for this address and retried"
                );
                keep(a, batch);
                metrics::counter!(
                    "mirror_v3_notify_batches_total",
                    "topic" => topic_l,
                    "partition" => partition_l,
                    "result" => "retry",
                )
                .increment(1);
                Ok(())
            }
            (FinalAction::Accept, false) => {
                tracing::info!(
                    address = %a.label,
                    attempt = a.attempt,
                    outcome = ?r.outcome,
                    "notify outcome resolved to accept (treated as delivered)"
                );
                delivered(a, &batch, complete);
                metrics::counter!(
                    "mirror_v3_notify_batches_total",
                    "topic" => topic_l,
                    "partition" => partition_l,
                    "result" => "ok",
                )
                .increment(1);
                Ok(())
            }
            (FinalAction::Fail, false) => {
                tracing::error!(
                    address = %a.label,
                    attempt = a.attempt,
                    outcome = ?r.outcome,
                    reason = %r.error,
                    "notify exhausted; mirror will exit"
                );
                metrics::counter!(
                    "mirror_v3_notify_batches_total",
                    "topic" => topic_l,
                    "partition" => partition_l,
                    "result" => "fail",
                )
                .increment(1);
                Err(NotifyError::Exhausted {
                    attempts: a.attempt,
                    last_error: format!("{}: {}", a.label, r.error),
                })
            }
        }
    }

    /// Tell the ack sink how far every address has been delivered: the
    /// committed offset, from which a restart re-delivers.
    fn report_ack(&mut self) {
        let mut ack = self.high_next;
        for t in &self.targets {
            for a in t.addrs.values() {
                if !a.idle() {
                    ack = ack.min(a.acked);
                }
            }
        }
        if ack > self.acked_reported {
            self.acked_reported = ack;
            if let Some(sink) = self.shared.ack_sink.get() {
                sink.note_through(ack);
            }
        }
        let (topic_l, partition_l) = self.shared.labels();
        let undelivered: usize = self
            .targets
            .iter()
            .flat_map(|t| t.addrs.values())
            .map(|a| {
                a.pending.as_ref().map(|p| p.keys.len()).unwrap_or(0)
                    + a.inflight.as_ref().map(|(b, _)| b.keys.len()).unwrap_or(0)
            })
            .sum();
        metrics::gauge!(
            "mirror_v3_notify_undelivered_keys",
            "topic" => topic_l.clone(),
            "partition" => partition_l.clone(),
        )
        .set(undelivered as f64);
        let addresses: usize = self.targets.iter().map(|t| t.addrs.len()).sum();
        metrics::gauge!(
            "mirror_v3_notify_target_addresses",
            "topic" => topic_l,
            "partition" => partition_l,
        )
        .set(addresses as f64);
    }
}

fn backoff(base_ms: u64, attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1).min(20);
    Duration::from_millis(base_ms.saturating_mul(1u64 << shift)).min(RETRY_BACKOFF_MAX)
}

pub(crate) fn payload_body(batch: &Batch, topic: &str, partition: i32) -> (Vec<u8>, String) {
    let mut offsets = IndexMap::with_capacity(1);
    offsets.insert(partition.to_string(), batch.high);
    let updates: IndexMap<String, serde_json::Value> = batch
        .keys
        .iter()
        .map(|k| (k.clone(), serde_json::Value::Null))
        .collect();
    let payload = KkvV1Payload::new(topic, offsets, updates);
    let body = serde_json::to_vec(&payload).expect("a kkv-v1 payload serializes");
    let offsets_header = serde_json::to_string(&payload.offsets).expect("offsets serialize");
    (body, offsets_header)
}

/// Split a batch so the first part's body stays under
/// [`MAX_BODY_BYTES`]; each part carries the batch's high offset. A
/// single key larger than the limit is sent alone.
fn cut_to_body_limit(batch: Batch, topic: &str, partition: i32) -> (Batch, Option<Batch>) {
    let envelope = payload_body(&Batch::default(), topic, partition).0.len();
    let mut size = envelope;
    let mut first = Batch {
        keys: IndexSet::new(),
        high: batch.high,
    };
    let mut rest = Batch {
        keys: IndexSet::new(),
        high: batch.high,
    };
    for k in batch.keys {
        // "key":null, plus quotes and escapes; serde_json escapes at
        // most 6 bytes per char, so measure the key as encoded.
        let encoded = serde_json::to_string(&k)
            .expect("a string serializes")
            .len()
            + 6;
        if rest.keys.is_empty() && (first.keys.is_empty() || size + encoded <= MAX_BODY_BYTES) {
            size += encoded;
            first.keys.insert(k);
        } else {
            rest.keys.insert(k);
        }
    }
    let rest = (!rest.keys.is_empty()).then_some(rest);
    (first, rest)
}

#[allow(clippy::too_many_arguments)]
fn post(
    shared: Arc<Shared>,
    client: Client,
    url: Url,
    label: String,
    (body, offsets_header): (Vec<u8>, String),
    attempt: u32,
    target: usize,
    addr: String,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = PostResult> + Send>> {
    Box::pin(async move {
        let (topic_l, partition_l) = shared.labels();
        metrics::gauge!(
            "mirror_v3_notify_inflight_retry",
            "topic" => topic_l.clone(),
            "partition" => partition_l.clone(),
            "target_host" => label.clone(),
        )
        .set(attempt as f64);
        let start = Instant::now();
        let result = client
            .post(url)
            .header("content-type", "application/json")
            .header("x-kkv-topic", &shared.topic)
            .header("x-kkv-offsets", offsets_header)
            .body(body)
            .send()
            .await;
        metrics::histogram!(
            "mirror_v3_notify_post_duration_seconds",
            "topic" => topic_l.clone(),
            "partition" => partition_l.clone(),
            "target_host" => label.clone(),
        )
        .record(start.elapsed().as_secs_f64());
        let mut error = String::new();
        let outcome = classify(result, &mut error);
        if outcome == Outcome::TwoXx {
            metrics::gauge!(
                "mirror_v3_notify_inflight_retry",
                "topic" => topic_l,
                "partition" => partition_l,
                "target_host" => label,
            )
            .set(0.0);
        }
        PostResult {
            target,
            addr,
            outcome,
            error,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cut_keeps_bodies_under_the_limit_and_the_high_offset() {
        let mut keys = IndexSet::new();
        for i in 0..10_000 {
            keys.insert(format!("user-{i:08}-with-a-longish-key"));
        }
        let mut rest = Some(Batch { keys, high: 42 });
        let mut parts = 0;
        let mut total = 0;
        while let Some(b) = rest.take() {
            let (first, r) = cut_to_body_limit(b, "userstate", 0);
            let (body, _) = payload_body(&first, "userstate", 0);
            assert!(body.len() <= MAX_BODY_BYTES, "{}", body.len());
            assert_eq!(first.high, 42);
            total += first.keys.len();
            parts += 1;
            rest = r;
        }
        assert_eq!(total, 10_000);
        assert!(parts > 1);
    }

    #[test]
    fn backoff_doubles_up_to_the_cap() {
        assert_eq!(backoff(100, 1), Duration::from_millis(100));
        assert_eq!(backoff(100, 3), Duration::from_millis(400));
        assert_eq!(backoff(100, 30), RETRY_BACKOFF_MAX);
    }
}
