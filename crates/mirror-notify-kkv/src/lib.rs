//! Outbound `kkv-v1` webhook notifier — drop-in replacement for the
//! push side of `Yolean/kafka-keyvalue`.
//!
//! Wire contract (matches the legacy `@yolean/kafka-keyvalue` Node
//! client unmodified; see `WEBHOOKS.md`):
//!   * `POST /kafka-keyvalue/v1/updates`
//!   * Headers: `x-kkv-topic`, `x-kkv-offsets`
//!   * Body: `{ "topic": "...", "offsets": {"<partition>": <offset>}, "updates": { "<key>": null } }`
//!
//! Trigger model (`trigger.on: source-consume`):
//!   * Every accepted record is fed to [`KkvV1Notifier::on_record`]
//!     by the mirror loop. Records accumulate in an in-memory buffer
//!     (key set with the highest source offset across the batch).
//!   * The buffer is drained — i.e. POSTed and reset — when either
//!     `debounce.max-records` records have arrived since the last
//!     drain, or `debounce.max-time-ms` has elapsed since the *first*
//!     record of the current batch landed.
//!   * The max-records trigger drains inline (`on_record` awaits the
//!     dispatch); the max-time-ms trigger drains from a background
//!     timer task. Errors from the timer-task drain are surfaced on
//!     the next `on_record` / `shutdown` call.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::future::join_all;
use indexmap::IndexMap;
use mirror_config::{
    FanOut, FinalAction, NotifyApi, NotifyOutcome, NotifyOutcomes, NotifyRetry, NotifyTarget,
};
use mirror_core::{current_labels, Notifier, NotifyError, Record};
use reqwest::Client;
use serde::Serialize;
use thiserror::Error;
use tokio::sync::{Mutex as TokioMutex, Notify as TokioNotify};
use tokio::task::JoinHandle;
use url::Url;

mod buffer;
mod resolver;

use buffer::{Buffer, DrainedBatch};
pub use resolver::{DnsAResolver, SystemDnsResolver};

/// How long a `fan-out: dns-a` resolution is reused before a
/// re-resolve. The legacy kkv had no caching (it watched K8s
/// Endpoints continuously); for the DNS-A replacement path we cache
/// for 30s — matches the spec's "default 30 s if no TTL is
/// published". Failure invalidates the cache early (per spec) so
/// scale-down recovery doesn't wait the full window.
const DNS_A_CACHE_TTL: Duration = Duration::from_secs(30);

/// Default path component when a target's URL has no explicit path.
/// Matches the legacy `@yolean/kafka-keyvalue` Node client's
/// `ON_UPDATE_DEFAULT_PATH`.
pub const KKV_V1_DEFAULT_PATH: &str = "/kafka-keyvalue/v1/updates";

/// Errors produced while constructing a [`KkvV1Notifier`] from config.
/// Surfaced once at startup so the supervisor can refuse to launch a
/// mirror whose notify block can't possibly work, instead of crashing
/// on the first record.
#[derive(Debug, Error)]
pub enum BuildError {
    #[error("notify.targets must be non-empty")]
    NoTargets,
    #[error("notify.target url {url:?} is not a valid URL: {source}")]
    InvalidUrl {
        url: String,
        #[source]
        source: url::ParseError,
    },
    #[error("notify.target url {url} must use http:// or https://; got scheme {scheme:?}")]
    UnsupportedScheme { url: String, scheme: String },
    #[error("notify.target url {url} has no host")]
    NoHost { url: String },
    #[error("failed to build reqwest client: {0}")]
    ClientBuild(String),
}

/// Per-target dispatcher state. One target = one `Endpoint`. The
/// `fan_out` mode decides whether dispatch goes to the URL's host
/// (resolved transparently by reqwest) or to every A/AAAA record the
/// configured resolver returns (one POST per address).
#[derive(Debug)]
struct Endpoint {
    /// Fully-resolved URL the POST goes to. `kkv-v1` default path is
    /// applied here at build time so the per-request hot path stays
    /// allocation-free.
    url: Url,
    /// Pre-rendered `target_host` metric label (`url.host_str()`).
    /// For fan-out: dns-a this is the *configured* hostname; the
    /// per-address dispatch uses the resolved IP as its
    /// `target_host` label instead.
    target_host: String,
    client: Client,
    fan_out: FanOutMode,
}

/// Per-endpoint fan-out behaviour. `None` is the default,
/// single-address path; `DnsA` resolves the URL's host to all
/// A/AAAA records and POSTs every address concurrently.
#[derive(Debug)]
enum FanOutMode {
    /// Single POST to the URL as-is. reqwest handles DNS internally.
    None,
    /// Resolve `host:port` via [`DnsAResolver`], dispatch one POST
    /// per returned address. Resolutions cached for
    /// [`DNS_A_CACHE_TTL`] and invalidated on any per-address
    /// failure (matches the spec's "re-resolve on any failure"
    /// recommendation).
    DnsA(DnsAState),
}

/// Cached resolver state for one `fan-out: dns-a` endpoint.
#[derive(Debug)]
struct DnsAState {
    /// Hostname we resolve.
    host: String,
    /// Port carried by every resolved `SocketAddr` (production: the
    /// URL's port or scheme default; tests: whatever the stub
    /// resolver returns).
    port: u16,
    cached: TokioMutex<Option<(Vec<SocketAddr>, Instant)>>,
}

/// Stateless dispatcher: takes a built batch payload, runs it through
/// the per-outcome retry/final-action state machine, against each
/// configured endpoint in turn. Lives behind an `Arc` so the buffer's
/// inline-drain path and the background timer task can both invoke it.
struct Inner {
    endpoints: Vec<Endpoint>,
    outcomes: NotifyOutcomes,
    retry: NotifyRetry,
    topic: String,
    partition: i32,
    resolver: Arc<dyn DnsAResolver>,
}

/// Shared notifier state. `buffer` holds the in-progress batch;
/// `new_data` wakes the timer task when on_record adds to an empty
/// buffer; `shutting_down` lets shutdown signal the timer to exit
/// even if it's mid-sleep; `error_state` lets the timer surface a
/// terminal error to whichever of on_record / shutdown polls next.
struct NotifierState {
    buffer: TokioMutex<Buffer>,
    new_data: TokioNotify,
    shutting_down: AtomicBool,
    error_state: TokioMutex<Option<NotifyError>>,
}

/// Notifier implementing the kkv-v1 wire contract. One instance per
/// mirror (per `(topic, partition)`).
pub struct KkvV1Notifier {
    inner: Arc<Inner>,
    state: Arc<NotifierState>,
    timer_task: Option<JoinHandle<()>>,
    max_records: u64,
}

impl KkvV1Notifier {
    /// Build a notifier from a validated [`mirror_config::Notify`]
    /// block. The caller is responsible for the higher-level
    /// validation (URL well-formedness, target non-empty, etc.) —
    /// `mirror-config` does that in `validate_notify_shared`. The
    /// checks here are the lighter-weight last-mile ones the runtime
    /// needs to actually open a `reqwest::Client`.
    ///
    /// Phase 3c: the trigger mode is read from `notify.trigger.on` and
    /// the debounce window from `notify.trigger.debounce`. For
    /// `trigger.on: destination-flush` the debounce config is
    /// ignored — that path will be added when the
    /// destination-flush callback hook is wired in a later phase.
    pub fn from_config(
        notify: &mirror_config::Notify,
        topic: String,
        partition: i32,
    ) -> Result<Self, BuildError> {
        Self::from_config_with_resolver(notify, topic, partition, Arc::new(SystemDnsResolver))
    }

    /// Same as [`Self::from_config`] but with a caller-supplied DNS
    /// resolver. Tests use this to inject a stub that returns canned
    /// `SocketAddr`s, exercising the `fan-out: dns-a` dispatch path
    /// against multiple axum servers without depending on the system
    /// resolver or `/etc/hosts`.
    pub fn from_config_with_resolver(
        notify: &mirror_config::Notify,
        topic: String,
        partition: i32,
        resolver: Arc<dyn DnsAResolver>,
    ) -> Result<Self, BuildError> {
        let inner = Arc::new(build_inner(notify, topic, partition, resolver)?);

        // Debounce config lives on the trigger block. Defaults come
        // from `NotifyTrigger::default()` (`Some({100, 250})` for
        // source-consume); validator rejects missing debounce for
        // source-consume so the `expect` here is unreachable for any
        // legit config.
        let debounce = notify
            .trigger
            .debounce
            .unwrap_or(mirror_config::NotifyDebounce {
                max_records: 1,
                max_time_ms: u64::MAX,
            });
        let max_records = debounce.max_records;
        let max_time = Duration::from_millis(debounce.max_time_ms);
        let state = Arc::new(NotifierState {
            buffer: TokioMutex::new(Buffer::default()),
            new_data: TokioNotify::new(),
            shutting_down: AtomicBool::new(false),
            error_state: TokioMutex::new(None),
        });

        // Always spawn the timer task. For `max_records: 1` it just
        // never fires (every drain is inline from on_record), and the
        // sleeping task costs ~nothing.
        let timer_task = tokio::spawn(timer_loop(Arc::clone(&inner), Arc::clone(&state), max_time));

        Ok(Self {
            inner,
            state,
            timer_task: Some(timer_task),
            max_records,
        })
    }

    /// Drain the current buffer (if any) and dispatch it. Used from
    /// both the on_record max-records path and shutdown.
    async fn drain_now(&self) -> Result<(), NotifyError> {
        let batch = {
            let mut buf = self.state.buffer.lock().await;
            buf.take(self.inner.partition)
        };
        let Some(batch) = batch else {
            return Ok(());
        };
        self.inner.dispatch_drained(batch).await
    }
}

impl Inner {
    async fn dispatch_drained(&self, batch: DrainedBatch) -> Result<(), NotifyError> {
        let payload = KkvV1Payload {
            topic: &self.topic,
            offsets: batch.offsets,
            updates: batch.updates,
        };
        self.dispatch_batch(&payload).await
    }

    /// POST a single batch payload to every configured endpoint
    /// serially. Per-endpoint fan-out is internal to
    /// [`Self::dispatch_endpoint`].
    async fn dispatch_batch(&self, payload: &KkvV1Payload<'_>) -> Result<(), NotifyError> {
        for endpoint in &self.endpoints {
            self.dispatch_endpoint(endpoint, payload).await?;
        }
        Ok(())
    }

    /// One endpoint = one configured `notify.targets[]` entry.
    /// Dispatch behaviour branches on the endpoint's fan-out mode:
    /// `none` POSTs to the URL as-is (one address, reqwest does DNS
    /// internally); `dns-a` resolves the URL's host via
    /// [`DnsAResolver`] and POSTs to every returned address
    /// concurrently. Per the spec, any per-address outcome that
    /// resolves to `final: fail` fails the whole batch.
    async fn dispatch_endpoint(
        &self,
        endpoint: &Endpoint,
        payload: &KkvV1Payload<'_>,
    ) -> Result<(), NotifyError> {
        match &endpoint.fan_out {
            FanOutMode::None => {
                self.dispatch_to_address(
                    &endpoint.client,
                    endpoint.url.clone(),
                    &endpoint.target_host,
                    payload,
                )
                .await
            }
            FanOutMode::DnsA(state) => self.dispatch_dns_a(endpoint, state, payload).await,
        }
    }

    /// Fan-out dispatch: resolve, then concurrent POSTs per address.
    /// First per-address error wins (subsequent results are still
    /// awaited so we don't leak in-flight requests).
    async fn dispatch_dns_a(
        &self,
        endpoint: &Endpoint,
        state: &DnsAState,
        payload: &KkvV1Payload<'_>,
    ) -> Result<(), NotifyError> {
        let addrs = state.resolve_or_cached(self.resolver.as_ref()).await?;
        if addrs.is_empty() {
            return Err(NotifyError::Transport(format!(
                "dns-a resolution of {} returned 0 addresses",
                state.host
            )));
        }
        let futures = addrs.iter().map(|sa| {
            let mut per_addr_url = endpoint.url.clone();
            // Set host to the IP literal; set port to the resolved
            // socket's port (matches the URL's port in production,
            // but lets test stubs aim at arbitrary axum servers).
            // Both setters return `Result<(), …>` for malformed
            // inputs; IPs and small ports never fail here so unwrap
            // is justified.
            per_addr_url
                .set_ip_host(sa.ip())
                .expect("set_ip_host on a valid URL always succeeds for an IpAddr");
            per_addr_url
                .set_port(Some(sa.port()))
                .expect("set_port on a valid URL with an http(s) scheme succeeds");
            let host_label = sa.to_string();
            async move {
                self.dispatch_to_address(&endpoint.client, per_addr_url, &host_label, payload)
                    .await
            }
        });
        let results = join_all(futures).await;
        let mut first_err: Option<NotifyError> = None;
        for r in results {
            if let Err(e) = r {
                first_err.get_or_insert(e);
            }
        }
        match first_err {
            Some(e) => {
                // Per-spec: "Re-resolve when the cache TTL expires
                // OR when an address fails repeatedly." Failure
                // invalidates the cached set immediately so the next
                // dispatch (after the supervisor restarts the
                // mirror) picks up any K8s scale-down that happened
                // mid-batch.
                state.invalidate_cache().await;
                Err(e)
            }
            None => Ok(()),
        }
    }

    /// Run the per-attempt retry / outcome / final-action loop
    /// against ONE address. Used by both `fan-out: none` (with the
    /// endpoint's URL/host) and `fan-out: dns-a` (with a per-address
    /// rewritten URL and the IP literal as the metric label).
    async fn dispatch_to_address(
        self: &Inner,
        client: &Client,
        url: Url,
        target_host: &str,
        payload: &KkvV1Payload<'_>,
    ) -> Result<(), NotifyError> {
        let body = serde_json::to_vec(payload).map_err(|e| {
            // Body serialization failure is a programming error, not
            // a webhook-receiver problem; surface as transport so the
            // operator sees a loud, distinct line.
            NotifyError::Transport(format!("payload serialization failed: {e}"))
        })?;
        let offsets_header = serde_json::to_string(&payload.offsets).map_err(|e| {
            NotifyError::Transport(format!("offsets header serialization failed: {e}"))
        })?;

        let mut attempt: u32 = 1;
        let mut last_error: String = String::new();
        loop {
            let (topic_l, partition_l) = current_labels();
            // Per-attempt retry gauge; spec says 1-based, 0 when idle.
            metrics::gauge!(
                "mirror_v3_notify_inflight_retry",
                "topic" => topic_l.clone(),
                "partition" => partition_l.clone(),
                "target_host" => target_host.to_string(),
            )
            .set(attempt as f64);

            let start = std::time::Instant::now();
            let result = client
                .post(url.clone())
                .header("content-type", "application/json")
                .header("x-kkv-topic", &self.topic)
                .header("x-kkv-offsets", &offsets_header)
                .body(body.clone())
                .send()
                .await;

            metrics::histogram!(
                "mirror_v3_notify_post_duration_seconds",
                "topic" => topic_l.clone(),
                "partition" => partition_l.clone(),
                "target_host" => target_host.to_string(),
            )
            .record(start.elapsed().as_secs_f64());

            let outcome = classify(result, &mut last_error);
            let policy = self.outcomes.for_outcome(outcome);

            tracing::debug!(
                target = %url,
                attempt,
                max_attempts = self.retry.max_attempts,
                ?outcome,
                policy_retry = policy.retry,
                policy_final = ?policy.final_,
                "notify post attempt"
            );

            if matches!(outcome, Outcome::TwoXx) {
                // Reset retry gauge on success.
                metrics::gauge!(
                    "mirror_v3_notify_inflight_retry",
                    "topic" => topic_l.clone(),
                    "partition" => partition_l.clone(),
                    "target_host" => target_host.to_string(),
                )
                .set(0.0);
                metrics::counter!(
                    "mirror_v3_notify_batches_total",
                    "topic" => topic_l,
                    "partition" => partition_l,
                    "result" => "ok",
                )
                .increment(1);
                return Ok(());
            }

            if policy.retry && attempt < self.retry.max_attempts {
                tracing::warn!(
                    target = %url,
                    attempt,
                    max_attempts = self.retry.max_attempts,
                    reason = %last_error,
                    "notify retry"
                );
                let backoff = backoff_for_attempt(self.retry.backoff_ms, attempt);
                tokio::time::sleep(backoff).await;
                attempt += 1;
                continue;
            }

            // Either retry: false (one attempt only) or we've used
            // the retry budget. Apply the final action.
            return self
                .apply_final_action(
                    &url,
                    target_host,
                    outcome,
                    policy,
                    attempt,
                    std::mem::take(&mut last_error),
                )
                .await;
        }
    }

    async fn apply_final_action(
        self: &Inner,
        url: &Url,
        target_host: &str,
        outcome: Outcome,
        policy: NotifyOutcome,
        attempts: u32,
        last_error: String,
    ) -> Result<(), NotifyError> {
        let (topic_l, partition_l) = current_labels();
        // Reset retry gauge regardless of outcome — the request is
        // no longer in flight.
        metrics::gauge!(
            "mirror_v3_notify_inflight_retry",
            "topic" => topic_l.clone(),
            "partition" => partition_l.clone(),
            "target_host" => target_host.to_string(),
        )
        .set(0.0);

        match policy.final_ {
            FinalAction::Accept => {
                tracing::info!(
                    target = %url,
                    ?outcome,
                    attempts,
                    "notify outcome resolved to accept (treated as delivered)"
                );
                metrics::counter!(
                    "mirror_v3_notify_batches_total",
                    "topic" => topic_l,
                    "partition" => partition_l,
                    "result" => "ok",
                )
                .increment(1);
                Ok(())
            }
            FinalAction::Skip => {
                tracing::warn!(
                    target = %url,
                    ?outcome,
                    attempts,
                    reason = %last_error,
                    "notify outcome resolved to skip; dropping batch"
                );
                metrics::counter!(
                    "mirror_v3_notify_batches_total",
                    "topic" => topic_l,
                    "partition" => partition_l,
                    "result" => "skip",
                )
                .increment(1);
                Ok(())
            }
            FinalAction::Fail => {
                tracing::error!(
                    target = %url,
                    ?outcome,
                    attempts,
                    reason = %last_error,
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
                    attempts,
                    last_error,
                })
            }
        }
    }
}

impl DnsAState {
    async fn resolve_or_cached(
        &self,
        resolver: &dyn DnsAResolver,
    ) -> Result<Vec<SocketAddr>, NotifyError> {
        {
            let cached = self.cached.lock().await;
            if let Some((addrs, at)) = cached.as_ref() {
                if at.elapsed() < DNS_A_CACHE_TTL {
                    return Ok(addrs.clone());
                }
            }
        }
        let addrs = resolver.resolve(&self.host, self.port).await.map_err(|e| {
            NotifyError::Transport(format!("dns-a resolution failed for {}: {e}", self.host))
        })?;
        // Dedupe in case the resolver returned the same SocketAddr
        // twice (lookup_host can yield both IPv4 + IPv4-mapped IPv6,
        // for example). Preserve order.
        let mut seen = std::collections::HashSet::new();
        let unique: Vec<SocketAddr> = addrs.into_iter().filter(|a| seen.insert(*a)).collect();
        *self.cached.lock().await = Some((unique.clone(), Instant::now()));
        Ok(unique)
    }

    async fn invalidate_cache(&self) {
        *self.cached.lock().await = None;
    }
}

#[async_trait]
impl Notifier for KkvV1Notifier {
    async fn on_record(&mut self, record: &Record) -> Result<(), NotifyError> {
        // First: surface any terminal error the timer task accumulated
        // since the last call. Once an error is observed we still let
        // the run loop hand us further records — they'll just keep
        // returning the same error until the loop aborts. Take() so
        // we only return it once.
        if let Some(err) = self.state.error_state.lock().await.take() {
            return Err(err);
        }

        // Keys may be missing or non-UTF-8. Legacy kkv emits whatever
        // string repr the consumer expects; mirror-v3 chooses
        // lossy-UTF-8 on bytes and `""` on missing key. Real
        // deployments use UTF-8 keys; this keeps the surface working
        // on edge cases instead of crashing.
        let key_str = render_key(record.key.as_deref());

        let (topic_l, partition_l) = current_labels();
        metrics::counter!(
            "mirror_v3_notify_records_total",
            "topic" => topic_l.clone(),
            "partition" => partition_l.clone(),
        )
        .increment(1);

        let drain_now;
        let buffer_depth;
        {
            let mut buf = self.state.buffer.lock().await;
            let was_empty = buf.is_empty();
            buf.append(key_str, record.source_offset);
            drain_now = buf.seen_records() >= self.max_records;
            buffer_depth = buf.seen_records();
            // Wake the timer when the buffer transitions empty →
            // non-empty so the max-time-ms clock starts running.
            if was_empty {
                self.state.new_data.notify_one();
            }
        }
        metrics::gauge!(
            "mirror_v3_notify_buffer_records",
            "topic" => topic_l,
            "partition" => partition_l,
        )
        .set(buffer_depth as f64);

        if drain_now {
            // Inline drain: caller (the consume loop) blocks on the
            // POST + retry cycle. This is the natural backpressure
            // mechanism from the spec's failure-modes table.
            self.drain_now().await
        } else {
            Ok(())
        }
    }

    async fn shutdown(&mut self) -> Result<(), NotifyError> {
        // Signal the timer task to exit even if it's mid-sleep, then
        // drain any pending batch synchronously so we can surface the
        // result to the supervisor before returning.
        self.state.shutting_down.store(true, Ordering::SeqCst);
        self.state.new_data.notify_one();

        let drain_result = self.drain_now().await;

        if let Some(t) = self.timer_task.take() {
            // Abort before await — the task may currently be in a
            // `sleep` we can't easily interrupt otherwise. The task
            // does no externally-visible work past the shutting_down
            // check, so aborting is safe.
            t.abort();
            let _ = t.await;
        }

        // Prefer the just-now drain error over any older one the
        // timer task might have stashed.
        drain_result?;
        if let Some(err) = self.state.error_state.lock().await.take() {
            return Err(err);
        }
        Ok(())
    }
}

/// Background drain loop. Waits for `state.new_data` to signal that
/// the buffer transitioned empty → non-empty, then sleeps for the
/// remaining time before the buffer's `first_at + max_time` deadline
/// and drains. The on_record path may have drained inline in the
/// meantime — in that case the take() returns None and we go back to
/// waiting.
async fn timer_loop(inner: Arc<Inner>, state: Arc<NotifierState>, max_time: Duration) {
    loop {
        state.new_data.notified().await;
        if state.shutting_down.load(Ordering::SeqCst) {
            return;
        }
        // Compute the actual remaining time relative to the buffer's
        // first_at — between notify_one() and our wake-up, on_record
        // could have drained inline (first_at = None) or there could
        // simply be no data left.
        let remaining = {
            let buf = state.buffer.lock().await;
            match buf.first_at() {
                Some(t) => max_time.saturating_sub(t.elapsed()),
                None => continue,
            }
        };
        tokio::time::sleep(remaining).await;
        if state.shutting_down.load(Ordering::SeqCst) {
            return;
        }
        let batch = {
            let mut buf = state.buffer.lock().await;
            buf.take(inner.partition)
        };
        if let Some(batch) = batch {
            if let Err(e) = inner.dispatch_drained(batch).await {
                // Stash for the next on_record / shutdown to surface;
                // exit so the buffer doesn't grow further behind a
                // broken receiver.
                *state.error_state.lock().await = Some(e);
                return;
            }
        }
    }
}

/// Build the per-mirror dispatcher state shared by both
/// [`KkvV1Notifier`] (source-consume trigger) and [`FlushDispatcher`]
/// (destination-flush trigger). Validates targets, opens the
/// reqwest client, and resolves each target into an [`Endpoint`].
fn build_inner(
    notify: &mirror_config::Notify,
    topic: String,
    partition: i32,
    resolver: Arc<dyn DnsAResolver>,
) -> Result<Inner, BuildError> {
    assert_eq!(notify.api, NotifyApi::KkvV1, "only kkv-v1 supported today");
    if notify.targets.is_empty() {
        return Err(BuildError::NoTargets);
    }
    let timeout = Duration::from_millis(notify.timeout_ms);
    let client = Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| BuildError::ClientBuild(e.to_string()))?;
    let mut endpoints = Vec::with_capacity(notify.targets.len());
    for t in &notify.targets {
        endpoints.push(build_endpoint(t, client.clone())?);
    }
    Ok(Inner {
        endpoints,
        outcomes: notify.outcomes,
        retry: notify.retry,
        topic,
        partition,
        resolver,
    })
}

/// Webhook dispatcher for the `trigger.on: destination-flush` mode.
/// Implements [`mirror_core::FlushObserver`]: each `on_flushed(from,
/// to)` enqueues a [`FlushEvent`] into an unbounded channel; the
/// drainer task pulls events and POSTs a kkv-v1 body per event
/// (`offsets: {partition: to}`, `updates: {}`).
///
/// Separate type from [`KkvV1Notifier`] because the two trigger
/// modes' lifecycles don't overlap: source-consume builds a
/// notifier and uses `NoOpNotifier`-shaped destination behaviour;
/// destination-flush builds a dispatcher and uses
/// `NoOpNotifier` in the run loop. The supervisor picks one or the
/// other based on `notify.trigger.on`.
pub struct FlushDispatcher {
    /// Held so the drainer task can be addressed via
    /// `error_state` / `tx` for shutdown signalling; otherwise
    /// untouched at runtime. (`#[allow(dead_code)]` quiets the
    /// linter — the field exists so callers can extend the type
    /// without re-deriving the shared state from the channel.)
    #[allow(dead_code)]
    inner: Arc<Inner>,
    tx: tokio::sync::mpsc::UnboundedSender<FlushEvent>,
    drainer: Option<JoinHandle<()>>,
    error_state: Arc<TokioMutex<Option<NotifyError>>>,
}

enum FlushEvent {
    Flushed { to: u64 },
    Shutdown,
}

impl FlushDispatcher {
    pub fn from_config(
        notify: &mirror_config::Notify,
        topic: String,
        partition: i32,
    ) -> Result<Self, BuildError> {
        Self::from_config_with_resolver(notify, topic, partition, Arc::new(SystemDnsResolver))
    }

    pub fn from_config_with_resolver(
        notify: &mirror_config::Notify,
        topic: String,
        partition: i32,
        resolver: Arc<dyn DnsAResolver>,
    ) -> Result<Self, BuildError> {
        let inner = Arc::new(build_inner(notify, topic, partition, resolver)?);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let error_state = Arc::new(TokioMutex::new(None));
        let drainer = tokio::spawn(flush_drainer_loop(
            Arc::clone(&inner),
            rx,
            Arc::clone(&error_state),
        ));
        Ok(Self {
            inner,
            tx,
            drainer: Some(drainer),
            error_state,
        })
    }

    /// Drain pending events and stop the background task. Returns
    /// any error the drainer accumulated before exit. Idempotent —
    /// calling twice is safe (the second call is a no-op).
    pub async fn shutdown(&mut self) -> Result<(), NotifyError> {
        let _ = self.tx.send(FlushEvent::Shutdown);
        if let Some(handle) = self.drainer.take() {
            handle.abort();
            let _ = handle.await;
        }
        if let Some(err) = self.error_state.lock().await.take() {
            return Err(err);
        }
        Ok(())
    }

    /// Snapshot the drainer's latest error without consuming the
    /// dispatcher. Used by `mirror-bin`'s status / supervision loop
    /// to detect a fatal dispatch failure without waiting for
    /// shutdown.
    pub async fn last_error(&self) -> Option<NotifyError> {
        self.error_state.lock().await.take()
    }
}

impl mirror_core::FlushObserver for FlushDispatcher {
    fn on_flushed(&self, _from: u64, to: u64) {
        // Fire-and-forget into the channel. If the drainer has
        // already exited (error_state is set), the send fails — and
        // that's fine; the supervisor will see the error on the
        // next `last_error` / `shutdown` call. `from` is intentionally
        // dropped: the kkv-v1 body only carries the high-water `to`
        // in its `offsets` field (consumer's `requireOffset`
        // semantic).
        let _ = self.tx.send(FlushEvent::Flushed { to });
    }
}

/// Background task that pulls flush events off the channel and
/// dispatches one kkv-v1 POST per event. Exits on `Shutdown` or
/// channel close, or stashes the first fatal dispatch error and
/// exits.
async fn flush_drainer_loop(
    inner: Arc<Inner>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<FlushEvent>,
    error_state: Arc<TokioMutex<Option<NotifyError>>>,
) {
    while let Some(event) = rx.recv().await {
        let to = match event {
            FlushEvent::Shutdown => return,
            FlushEvent::Flushed { to } => to,
        };
        let mut offsets = IndexMap::new();
        offsets.insert(inner.partition.to_string(), to);
        let payload = KkvV1Payload {
            topic: &inner.topic,
            // Empty `updates` per WEBHOOKS.md open-question #2:
            // destination-flush is the "tell me a file landed" use
            // case, not cache invalidation, so the consumer doesn't
            // need a key set. The `offsets` field gives them the
            // high-water mark.
            offsets,
            updates: IndexMap::new(),
        };
        if let Err(e) = inner.dispatch_batch(&payload).await {
            *error_state.lock().await = Some(e);
            return;
        }
    }
}

fn build_endpoint(target: &NotifyTarget, client: Client) -> Result<Endpoint, BuildError> {
    let mut url = Url::parse(&target.url).map_err(|e| BuildError::InvalidUrl {
        url: target.url.clone(),
        source: e,
    })?;
    match url.scheme() {
        "http" | "https" => {}
        other => {
            return Err(BuildError::UnsupportedScheme {
                url: target.url.clone(),
                scheme: other.to_string(),
            });
        }
    }
    if url.host_str().is_none() {
        return Err(BuildError::NoHost {
            url: target.url.clone(),
        });
    }
    // Apply the api-default path when the operator left it implicit.
    // An explicit `path:` override wins; a URL whose path is `/` (the
    // default url crate emits for hostname-only inputs) is treated as
    // "no path specified".
    let explicit_path = target.path.as_deref();
    let url_has_path = !matches!(url.path(), "" | "/");
    let path_to_set: Option<&str> = explicit_path.or({
        if url_has_path {
            None
        } else {
            Some(KKV_V1_DEFAULT_PATH)
        }
    });
    if let Some(p) = path_to_set {
        url.set_path(p);
    }
    let target_host = url.host_str().unwrap_or("").to_string();
    let fan_out = match target.fan_out {
        FanOut::None => FanOutMode::None,
        FanOut::DnsA => {
            // Port comes from the URL; `port_or_known_default` falls
            // back to 80/443 per scheme. This is the port the
            // resolver appends to every A/AAAA address it returns —
            // matches the K8s headless-Service expectation (all pods
            // listen on the same port).
            let port =
                url.port_or_known_default()
                    .ok_or_else(|| BuildError::UnsupportedScheme {
                        url: target.url.clone(),
                        scheme: url.scheme().to_string(),
                    })?;
            FanOutMode::DnsA(DnsAState {
                host: target_host.clone(),
                port,
                cached: TokioMutex::new(None),
            })
        }
    };
    Ok(Endpoint {
        url,
        target_host,
        client,
        fan_out,
    })
}

fn render_key(key: Option<&[u8]>) -> String {
    match key {
        None => String::new(),
        Some(bytes) => String::from_utf8_lossy(bytes).into_owned(),
    }
}

/// Exponential backoff capped at 30s. `base * 2^(attempt-1)`. Attempt
/// 1 (first retry) is one base interval; attempt 5 is 16×.
fn backoff_for_attempt(base_ms: u64, attempt: u32) -> Duration {
    // attempt is 1-based on the just-finished failure; backoff is the
    // wait before the next attempt. Cap at 30 s so a misconfigured
    // multi-day backoff doesn't silently stall a mirror.
    let shift = (attempt - 1).min(20);
    let ms = base_ms.saturating_mul(1u64 << shift).min(30_000);
    Duration::from_millis(ms)
}

/// Strongly-typed outcome bucket. Maps `reqwest::Result<Response>`
/// onto one of the six spec-defined outcomes (`§ Outcomes and retry
/// policy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Timeout,
    ConnRefused,
    TwoXx,
    ThreeXx,
    FourXx,
    FiveXx,
}

/// Per-outcome lookup. Centralises the `NotifyOutcomes` mapping so the
/// dispatcher just deals with [`Outcome`] values.
trait OutcomesLookup {
    fn for_outcome(&self, o: Outcome) -> NotifyOutcome;
}

impl OutcomesLookup for NotifyOutcomes {
    fn for_outcome(&self, o: Outcome) -> NotifyOutcome {
        match o {
            Outcome::Timeout => self.timeout,
            Outcome::ConnRefused => self.connrefused,
            Outcome::TwoXx => self.two_xx,
            Outcome::ThreeXx => self.three_xx,
            Outcome::FourXx => self.four_xx,
            Outcome::FiveXx => self.five_xx,
        }
    }
}

/// Decide which outcome bucket a reqwest result falls into. `error`
/// is populated with a human-readable reason whenever the outcome is
/// not 2xx, so the eventual `tracing::warn!` / `NotifyError::Exhausted`
/// carries the underlying failure.
fn classify(result: reqwest::Result<reqwest::Response>, error: &mut String) -> Outcome {
    match result {
        Ok(resp) => {
            let status = resp.status();
            // Drop body promptly — outcome decision is status-only.
            // (reqwest will close the connection if we don't consume,
            // hurting keep-alive reuse.) Spawned task isn't needed:
            // the body is small for kkv 2xx (typically empty) and we
            // hold the future at the call site.
            drop(resp);
            if status.is_success() {
                Outcome::TwoXx
            } else if status.is_redirection() {
                *error = format!("HTTP {status}");
                Outcome::ThreeXx
            } else if status.is_client_error() {
                *error = format!("HTTP {status}");
                Outcome::FourXx
            } else if status.is_server_error() {
                *error = format!("HTTP {status}");
                Outcome::FiveXx
            } else {
                // 1xx — informational. Treat as 2xx (spec doesn't
                // enumerate; reqwest already filters most of these).
                Outcome::TwoXx
            }
        }
        Err(e) => {
            if e.is_timeout() {
                *error = format!("timeout: {e}");
                Outcome::Timeout
            } else if is_connection_refused(&e) {
                *error = format!("connection refused: {e}");
                Outcome::ConnRefused
            } else {
                // Other transport-layer errors (DNS resolution, TLS,
                // mid-stream EOF, etc.) are spec-treated like
                // connection-refused — they're "couldn't reach the
                // receiver", same retry/final policy expectations.
                *error = format!("connection error: {e}");
                Outcome::ConnRefused
            }
        }
    }
}

fn is_connection_refused(e: &reqwest::Error) -> bool {
    // reqwest doesn't surface a "connrefused" predicate; walk the
    // source chain looking for the io::ErrorKind::ConnectionRefused.
    let mut source: Option<&dyn std::error::Error> = Some(e);
    while let Some(err) = source {
        if let Some(io) = err.downcast_ref::<std::io::Error>() {
            if io.kind() == std::io::ErrorKind::ConnectionRefused {
                return true;
            }
        }
        source = err.source();
    }
    false
}

/// On-wire body shape for `api: kkv-v1`. Mirrors the legacy
/// `@yolean/kafka-keyvalue` Node client's `KafkaKeyValue.js` parser.
///
/// `topic` and `offsets` are duplicated in the headers
/// (`x-kkv-topic`, `x-kkv-offsets`) so misrouted requests are easy to
/// debug from the body alone. `updates` is a key → `null` map; the
/// consumer re-fetches every key via `GET /cache/v1/raw/<key>`.
#[derive(Debug, Serialize)]
struct KkvV1Payload<'a> {
    topic: &'a str,
    /// `IndexMap` to preserve insertion order on the wire; the legacy
    /// kkv consumer doesn't care about key order but stable output
    /// makes integration tests deterministic.
    offsets: IndexMap<String, u64>,
    updates: IndexMap<String, serde_json::Value>,
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn backoff_doubles_per_attempt_capped_at_30s() {
        assert_eq!(backoff_for_attempt(100, 1), Duration::from_millis(100));
        assert_eq!(backoff_for_attempt(100, 2), Duration::from_millis(200));
        assert_eq!(backoff_for_attempt(100, 3), Duration::from_millis(400));
        assert_eq!(backoff_for_attempt(100, 4), Duration::from_millis(800));
        // 100 << 19 = 52_428_800, capped at 30_000.
        assert_eq!(backoff_for_attempt(100, 20), Duration::from_millis(30_000));
    }

    #[test]
    fn render_key_handles_none_and_lossy_utf8() {
        assert_eq!(render_key(None), "");
        assert_eq!(render_key(Some(b"hello")), "hello");
        // 0xff is not valid UTF-8; lossy substitution should produce
        // the replacement character rather than panicking.
        let s = render_key(Some(&[b'a', 0xff, b'b']));
        assert!(s.starts_with('a') && s.ends_with('b'));
    }

    #[test]
    fn build_endpoint_applies_default_kkv_path_when_url_is_host_only() {
        let target = NotifyTarget {
            url: "http://kkv-target.example".into(),
            path: None,
            fan_out: mirror_config::FanOut::None,
        };
        let ep = build_endpoint(&target, Client::new()).unwrap();
        assert_eq!(ep.url.path(), KKV_V1_DEFAULT_PATH);
    }

    #[test]
    fn build_endpoint_respects_explicit_path_override() {
        let target = NotifyTarget {
            url: "http://kkv-target.example".into(),
            path: Some("/custom/route".into()),
            fan_out: mirror_config::FanOut::None,
        };
        let ep = build_endpoint(&target, Client::new()).unwrap();
        assert_eq!(ep.url.path(), "/custom/route");
    }

    #[test]
    fn build_endpoint_respects_path_in_url_when_no_override() {
        let target = NotifyTarget {
            url: "http://kkv-target.example/already/has/path".into(),
            path: None,
            fan_out: mirror_config::FanOut::None,
        };
        let ep = build_endpoint(&target, Client::new()).unwrap();
        assert_eq!(ep.url.path(), "/already/has/path");
    }

    #[test]
    fn build_endpoint_rejects_non_http_scheme() {
        let target = NotifyTarget {
            url: "file:///etc/passwd".into(),
            path: None,
            fan_out: mirror_config::FanOut::None,
        };
        let err = build_endpoint(&target, Client::new()).unwrap_err();
        assert!(
            matches!(err, BuildError::UnsupportedScheme { .. }),
            "got {err:?}"
        );
    }
}
