//! Outbound `kkv-v1` webhook notifier. Drop-in replacement for the
//! push side of `Yolean/kafka-keyvalue`.
//!
//! Wire contract (matches the `@yolean/kafka-keyvalue` Node client
//! unmodified; see `WEBHOOKS.md`):
//!   * `POST /kafka-keyvalue/v1/updates`
//!   * Headers: `x-kkv-topic`, `x-kkv-offsets`
//!   * Body: `{ "topic": "...", "offsets": {"<partition>": <offset>}, "updates": { "<key>": null } }`
//!
//! Trigger model (`trigger.on: source-consume`):
//!   * Every accepted record is fed to [`KkvV1Notifier::on_record`]
//!     by the mirror loop. Records accumulate in an in-memory buffer
//!     (key set with the highest source offset across the batch).
//!   * The buffer becomes a batch when either `debounce.max-records`
//!     records have arrived since the last batch, or
//!     `debounce.max-time-ms` has elapsed since the *first* record of
//!     the current batch landed (a timer task, so a batch tail never
//!     waits for the next record).
//!   * Batches go to the deliverer task ([`delivery`]), which keeps an
//!     undelivered key set per target address and retries it until the
//!     address accepts. `on_record` never waits for HTTP.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::future::join_all;
use indexmap::IndexMap;
use mirror_config::{
    FanOut, FinalAction, NotifyApi, NotifyOutcome, NotifyOutcomes, NotifyRetry, NotifyTarget,
};
use mirror_core::{AckSink, CacheState, Notifier, NotifyError, Record};
use reqwest::Client;
use serde::Serialize;
use thiserror::Error;
use tokio::sync::{Mutex as TokioMutex, Notify as TokioNotify};
use tokio::task::JoinHandle;
use url::Url;

mod buffer;
pub mod delivery;
mod resolver;

use buffer::Buffer;
pub use resolver::{DnsAResolver, SystemDnsResolver};

/// How long a `fan-out: dns-a` resolution is reused before a
/// re-resolve. 30s matches the spec's "default 30 s if no TTL is
/// published". Failure invalidates the cache early (per spec) so
/// scale-down recovery doesn't wait the full window.
const DNS_A_CACHE_TTL: Duration = Duration::from_secs(30);

/// Default path component when a target's URL has no explicit path.
/// Matches `@yolean/kafka-keyvalue` Node client's
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
    #[error("mirror {0:?} is not registered in the cache state; register it before building its notifier")]
    UnregisteredMirror(String),
}

/// Per-target dispatcher state. One target maps to one `Endpoint`. The
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

/// Notifier state shared between `on_record` and the debounce timer.
struct NotifierState {
    buffer: TokioMutex<Buffer>,
    /// Wakes the timer task when `on_record` adds to an empty buffer.
    new_data: TokioNotify,
    shutting_down: AtomicBool,
    /// Batches to the deliverer task; `None` once shut down.
    tx: std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<delivery::Batch>>>,
}

impl NotifierState {
    /// Hand the buffered records to the deliverer as one batch.
    async fn flush_buffer(&self) {
        let batch = self.buffer.lock().await.take();
        if let Some(batch) = batch {
            if let Some(tx) = self.tx.lock().expect("notifier tx poisoned").as_ref() {
                // The deliverer only stops after the sender is dropped
                // or after a terminal error, which `on_record` surfaces.
                let _ = tx.send(batch);
            }
        }
    }
}

/// Notifier implementing the kkv-v1 wire contract. One instance per
/// mirror (per `(topic, partition)`).
pub struct KkvV1Notifier {
    shared: Arc<delivery::Shared>,
    state: Arc<NotifierState>,
    timer_task: Option<JoinHandle<()>>,
    deliverer: Option<JoinHandle<()>>,
    max_records: u64,
}

impl KkvV1Notifier {
    /// Build a notifier from a validated [`mirror_config::Notify`]
    /// block (`mirror-config` validates URLs and limits). The mirror
    /// must be registered in `cache_state`: its suppression threshold
    /// and catch-up state gate what is sent and when.
    pub fn from_config(
        notify: &mirror_config::Notify,
        topic: String,
        partition: i32,
        cache_state: Arc<CacheState>,
        mirror_name: String,
    ) -> Result<Self, BuildError> {
        Self::from_config_with_resolver(
            notify,
            topic,
            partition,
            cache_state,
            mirror_name,
            Arc::new(SystemDnsResolver),
        )
    }

    /// Same as [`Self::from_config`] with a caller-supplied DNS
    /// resolver (tests).
    pub fn from_config_with_resolver(
        notify: &mirror_config::Notify,
        topic: String,
        partition: i32,
        cache_state: Arc<CacheState>,
        mirror_name: String,
        resolver: Arc<dyn DnsAResolver>,
    ) -> Result<Self, BuildError> {
        assert_eq!(notify.api, NotifyApi::KkvV1, "only kkv-v1 supported today");
        if cache_state.status_for(&mirror_name).is_none() {
            return Err(BuildError::UnregisteredMirror(mirror_name));
        }
        let endpoints = build_endpoints(notify)?;
        let debounce = notify.trigger.debounce.ok_or_else(|| {
            BuildError::ClientBuild("notify.trigger.debounce is required for source-consume".into())
        })?;
        let shared = Arc::new(delivery::Shared {
            topic,
            partition,
            outcomes: notify.outcomes,
            retry: notify.retry,
            resolver,
            cache_state,
            mirror_name,
            ack_sink: OnceLock::new(),
            error_state: Arc::new(TokioMutex::new(None)),
            error_signal: Arc::new(TokioNotify::new()),
        });
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let state = Arc::new(NotifierState {
            buffer: TokioMutex::new(Buffer::default()),
            new_data: TokioNotify::new(),
            shutting_down: AtomicBool::new(false),
            tx: std::sync::Mutex::new(Some(tx)),
        });
        let deliverer =
            tokio::spawn(delivery::Deliverer::new(Arc::clone(&shared), endpoints).run(rx));
        let timer_task = tokio::spawn(timer_loop(
            Arc::clone(&state),
            Duration::from_millis(debounce.max_time_ms),
        ));
        Ok(Self {
            shared,
            state,
            timer_task: Some(timer_task),
            deliverer: Some(deliverer),
            max_records: debounce.max_records,
        })
    }

    /// Install the [`AckSink`] that learns how far every target address
    /// has been delivered (`note_through(offset)`: everything below is
    /// delivered). The first install wins; call before the first record.
    pub fn with_ack_sink(self, ack: Arc<dyn AckSink>) -> Self {
        let _ = self.shared.ack_sink.set(ack);
        self
    }

    /// Handle for the supervisor to observe a terminal delivery error
    /// (a `final: fail` outcome) without owning the notifier: an idle
    /// topic never calls `on_record` again to surface it.
    pub fn terminal_error_watch(&self) -> TerminalErrorWatch {
        TerminalErrorWatch {
            error_state: Arc::clone(&self.shared.error_state),
            signal: Arc::clone(&self.shared.error_signal),
        }
    }
}

impl Inner {
    /// Metric labels from the construction-time mirror identity.
    /// The `MIRROR_LABELS` task-local is not available here: the
    /// timer task and the flush drainer are `tokio::spawn`ed at
    /// construction time, outside the run loop's scope, so
    /// `current_labels()` would report `unknown/0` for every drain
    /// they dispatch.
    fn labels(&self) -> (String, String) {
        (self.topic.clone(), self.partition.to_string())
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

    /// Fan-out dispatch. The retry loop sits at the endpoint level:
    /// each attempt resolves the address set fresh (the cache is
    /// invalidated on any failure), POSTs once per address
    /// concurrently, and retries the whole set on any retryable
    /// per-address failure. Retrying a pinned per-address IP instead
    /// would turn every receiver rolling restart into a mirror
    /// crash: the dead pod IP eats the full retry budget while the
    /// replacement pod is one re-resolve away. Addresses that
    /// already accepted the batch get it again on a retry round;
    /// kkv invalidations are idempotent and this is within
    /// at-least-once.
    async fn dispatch_dns_a(
        &self,
        endpoint: &Endpoint,
        state: &DnsAState,
        payload: &KkvV1Payload<'_>,
    ) -> Result<(), NotifyError> {
        let body = serde_json::to_vec(payload)
            .map_err(|e| NotifyError::Transport(format!("payload serialization failed: {e}")))?;
        let offsets_header = serde_json::to_string(&payload.offsets).map_err(|e| {
            NotifyError::Transport(format!("offsets header serialization failed: {e}"))
        })?;

        let mut attempt: u32 = 1;
        loop {
            // A failed resolution goes through the `connrefused` policy
            // (WEBHOOKS.md failure table); an empty one means there is
            // no target to deliver to (a Service scaled
            // to zero used to fail the mirror, bypassing the policy).
            let addrs = match state.resolve_or_cached(self.resolver.as_ref()).await {
                Ok(a) => a,
                Err(e) => {
                    let policy = self.outcomes.for_outcome(Outcome::ConnRefused);
                    if policy.retry && attempt < self.retry.max_attempts {
                        tracing::warn!(host = %state.host, attempt, error = %e, "notify dns-a resolution failed; retrying");
                        tokio::time::sleep(backoff_for_attempt(self.retry.backoff_ms, attempt))
                            .await;
                        attempt += 1;
                        continue;
                    }
                    return self
                        .apply_final_action(
                            &endpoint.url,
                            &endpoint.target_host,
                            Outcome::ConnRefused,
                            policy,
                            attempt,
                            e.to_string(),
                        )
                        .await;
                }
            };
            if addrs.is_empty() {
                tracing::info!(host = %state.host, "notify dns-a: no address; nothing to deliver");
                return Ok(());
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
                let body = &body;
                let offsets_header = &offsets_header;
                async move {
                    let mut last_error = String::new();
                    let outcome = self
                        .post_once(
                            &endpoint.client,
                            &per_addr_url,
                            &host_label,
                            body,
                            offsets_header,
                            attempt,
                            &mut last_error,
                        )
                        .await;
                    (per_addr_url, host_label, outcome, last_error)
                }
            });
            let results = join_all(futures).await;

            let mut want_retry = false;
            let mut failures: Vec<(Url, String, Outcome, NotifyOutcome, String)> = Vec::new();
            for (url, host_label, outcome, last_error) in results {
                if matches!(outcome, Outcome::TwoXx) {
                    continue;
                }
                let policy = self.outcomes.for_outcome(outcome);
                if policy.retry {
                    want_retry = true;
                }
                failures.push((url, host_label, outcome, policy, last_error));
            }
            if failures.is_empty() {
                return Ok(());
            }

            // Any failure means the cached set may be stale (K8s
            // scale-down or rollout mid-batch); the next attempt (or
            // the next batch) re-resolves.
            state.invalidate_cache().await;

            if want_retry && attempt < self.retry.max_attempts {
                let reasons: Vec<String> = failures
                    .iter()
                    .map(|(_, host, outcome, _, err)| format!("{host}: {outcome:?} {err}"))
                    .collect();
                tracing::warn!(
                    endpoint = %endpoint.url,
                    attempt,
                    max_attempts = self.retry.max_attempts,
                    failed_addresses = %reasons.join("; "),
                    "notify dns-a retry with fresh resolution"
                );
                let backoff = backoff_for_attempt(self.retry.backoff_ms, attempt);
                tokio::time::sleep(backoff).await;
                attempt += 1;
                continue;
            }

            // Terminal: apply each failing address's final action;
            // any `fail` fails the batch.
            let mut first_err: Option<NotifyError> = None;
            for (url, host_label, outcome, policy, last_error) in failures {
                if let Err(e) = self
                    .apply_final_action(&url, &host_label, outcome, policy, attempt, last_error)
                    .await
                {
                    first_err.get_or_insert(e);
                }
            }
            return match first_err {
                Some(e) => Err(e),
                None => Ok(()),
            };
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
            let outcome = self
                .post_once(
                    client,
                    &url,
                    target_host,
                    &body,
                    &offsets_header,
                    attempt,
                    &mut last_error,
                )
                .await;
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

    /// One POST attempt against one address: emits the per-attempt
    /// retry gauge and duration histogram, classifies the response,
    /// and on 2xx resets the gauge and counts the batch as ok.
    /// Shared by the fan-out: none per-address retry loop and the
    /// dns-a endpoint-level retry loop.
    #[allow(clippy::too_many_arguments)]
    async fn post_once(
        self: &Inner,
        client: &Client,
        url: &Url,
        target_host: &str,
        body: &[u8],
        offsets_header: &str,
        attempt: u32,
        last_error: &mut String,
    ) -> Outcome {
        let (topic_l, partition_l) = self.labels();
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
            .header("x-kkv-offsets", offsets_header)
            .body(body.to_vec())
            .send()
            .await;

        metrics::histogram!(
            "mirror_v3_notify_post_duration_seconds",
            "topic" => topic_l.clone(),
            "partition" => partition_l.clone(),
            "target_host" => target_host.to_string(),
        )
        .record(start.elapsed().as_secs_f64());

        let outcome = classify(result, last_error);
        if matches!(outcome, Outcome::TwoXx) {
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
        }
        outcome
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
        let (topic_l, partition_l) = self.labels();
        // Reset retry gauge regardless of outcome; the request is
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
        // Surface a terminal delivery error (`final: fail`) once.
        if let Some(err) = self.shared.error_state.lock().await.take() {
            return Err(err);
        }
        let (topic_l, partition_l) = self.shared_labels();

        // Records below the suppression threshold were notified by the
        // previous pod (committed offset), or predate this deploy
        // (bootstrap watermark on a fresh group).
        if self
            .shared
            .cache_state
            .is_record_suppressed(&self.shared.mirror_name, record.source_offset)
        {
            metrics::counter!(
                "mirror_v3_notify_suppressed_records_total",
                "topic" => topic_l,
                "partition" => partition_l,
            )
            .increment(1);
            return Ok(());
        }

        // kafka-keyvalue does not notify a record without a key; nor a
        // key that is not UTF-8 here (the consumer re-reads it as a
        // /raw/{key} path, and the cache skipped it too).
        let key_str = match record.key.as_deref().map(std::str::from_utf8) {
            Some(Ok(k)) => k.to_string(),
            other => {
                let reason = if other.is_none() {
                    "null_key"
                } else {
                    "non_utf8_key"
                };
                metrics::counter!(
                    "mirror_v3_notify_skipped_records_total",
                    "topic" => topic_l,
                    "partition" => partition_l,
                    "reason" => reason,
                )
                .increment(1);
                return Ok(());
            }
        };
        metrics::counter!(
            "mirror_v3_notify_records_total",
            "topic" => topic_l.clone(),
            "partition" => partition_l.clone(),
        )
        .increment(1);

        let full;
        let buffer_depth;
        {
            let mut buf = self.state.buffer.lock().await;
            let was_empty = buf.is_empty();
            buf.append(key_str, record.source_offset);
            full = buf.seen_records() >= self.max_records;
            buffer_depth = buf.seen_records();
            // Start the max-time-ms clock when the buffer goes from
            // empty to non-empty.
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
        if full {
            self.state.flush_buffer().await;
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), NotifyError> {
        self.state.shutting_down.store(true, Ordering::SeqCst);
        self.state.new_data.notify_one();
        if let Some(t) = self.timer_task.take() {
            t.abort();
            let _ = t.await;
        }
        self.state.flush_buffer().await;
        // Closing the channel tells the deliverer to finish what is
        // pending (within its shutdown budget) and stop.
        self.state.tx.lock().expect("notifier tx poisoned").take();
        if let Some(d) = self.deliverer.take() {
            let _ = d.await;
        }
        if let Some(err) = self.shared.error_state.lock().await.take() {
            return Err(err);
        }
        Ok(())
    }
}

impl KkvV1Notifier {
    fn shared_labels(&self) -> (String, String) {
        (self.shared.topic.clone(), self.shared.partition.to_string())
    }
}

/// Debounce timer: once the buffer goes non-empty, sleep until its
/// first record is `max_time` old and hand the batch to the deliverer.
async fn timer_loop(state: Arc<NotifierState>, max_time: Duration) {
    loop {
        state.new_data.notified().await;
        if state.shutting_down.load(Ordering::SeqCst) {
            return;
        }
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
        state.flush_buffer().await;
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
    Ok(Inner {
        endpoints: build_endpoints(notify)?,
        outcomes: notify.outcomes,
        retry: notify.retry,
        topic,
        partition,
        resolver,
    })
}

/// One [`Endpoint`] per `notify.targets[]` entry, sharing one HTTP
/// client with the configured per-request timeout.
fn build_endpoints(notify: &mirror_config::Notify) -> Result<Vec<Endpoint>, BuildError> {
    if notify.targets.is_empty() {
        return Err(BuildError::NoTargets);
    }
    let timeout = Duration::from_millis(notify.timeout_ms);
    let client = Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| BuildError::ClientBuild(e.to_string()))?;
    notify
        .targets
        .iter()
        .map(|t| build_endpoint(t, client.clone()))
        .collect()
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
    /// linter; the field exists so callers can extend the type
    /// without re-deriving the shared state from the channel.)
    #[allow(dead_code)]
    inner: Arc<Inner>,
    tx: tokio::sync::mpsc::UnboundedSender<FlushEvent>,
    /// Behind a mutex so [`Self::drain_and_stop`] can join the task
    /// through `&self`: in production the dispatcher lives inside
    /// the tee as an `Arc<dyn FlushObserver>`, and the supervisor
    /// holds a second `Arc` clone for the shutdown drain.
    drainer: TokioMutex<Option<JoinHandle<()>>>,
    error_state: Arc<TokioMutex<Option<NotifyError>>>,
    /// Signalled when the drainer stashes a terminal error; see
    /// [`TerminalErrorWatch`]. Without a watcher a drainer death is
    /// otherwise invisible in production: nothing calls
    /// `last_error`/`shutdown`, later `on_flushed` sends fail
    /// silently, and flush events (unlike source records) are not
    /// regenerated by a restart.
    error_signal: Arc<TokioNotify>,
    /// Per-mirror suppression handle. `on_flushed` consults
    /// `cache_state.is_record_suppressed(&mirror_name, to)` and
    /// drops events below the mirror's suppression threshold.
    /// Matches the source-consume gate on [`KkvV1Notifier`].
    cache_state: Arc<CacheState>,
    mirror_name: String,
    topic: String,
    partition: i32,
    /// Set once via [`Self::with_ack_sink`]. Shared with the drainer
    /// task at construction; the drainer calls
    /// `note_through(to + 1)` after a successful POST so the
    /// supervisor's per-mirror ack tracker can advance.
    ack_sink: Arc<OnceLock<Arc<dyn AckSink>>>,
}

enum FlushEvent {
    Flushed { to: u64 },
    Shutdown,
}

/// Awaitable handle onto a notifier's / dispatcher's terminal
/// dispatch error. The supervisor races this against the mirror run
/// loop so a dead webhook pipeline errors the mirror (and thereby
/// the process) instead of going silent: the orchestrator restarts,
/// and the post-restart replay-from-committed-offset re-delivers
/// what the dead pipeline dropped.
pub struct TerminalErrorWatch {
    error_state: Arc<TokioMutex<Option<NotifyError>>>,
    signal: Arc<TokioNotify>,
}

impl TerminalErrorWatch {
    /// Resolve once a terminal error is stashed, consuming it.
    /// Pending forever if dispatch never fails terminally. If the
    /// run loop consumes the error first (the `on_record` path),
    /// this stays pending; the run loop's own error wins the race,
    /// which is fine because either way the mirror errors exactly
    /// once.
    pub async fn wait(self) -> NotifyError {
        loop {
            if let Some(err) = self.error_state.lock().await.take() {
                return err;
            }
            // notify_one stores a permit when there's no waiter yet,
            // so a stash happening between the check above and this
            // await cannot be missed.
            self.signal.notified().await;
        }
    }
}

impl FlushDispatcher {
    pub fn from_config(
        notify: &mirror_config::Notify,
        topic: String,
        partition: i32,
        cache_state: Arc<CacheState>,
        mirror_name: String,
    ) -> Result<Self, BuildError> {
        Self::from_config_with_resolver(
            notify,
            topic,
            partition,
            cache_state,
            mirror_name,
            Arc::new(SystemDnsResolver),
        )
    }

    pub fn from_config_with_resolver(
        notify: &mirror_config::Notify,
        topic: String,
        partition: i32,
        cache_state: Arc<CacheState>,
        mirror_name: String,
        resolver: Arc<dyn DnsAResolver>,
    ) -> Result<Self, BuildError> {
        let inner = Arc::new(build_inner(notify, topic.clone(), partition, resolver)?);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let error_state = Arc::new(TokioMutex::new(None));
        let error_signal = Arc::new(TokioNotify::new());
        let ack_sink: Arc<OnceLock<Arc<dyn AckSink>>> = Arc::new(OnceLock::new());
        let drainer = tokio::spawn(flush_drainer_loop(
            Arc::clone(&inner),
            rx,
            Arc::clone(&error_state),
            Arc::clone(&error_signal),
            Arc::clone(&ack_sink),
        ));
        Ok(Self {
            inner,
            tx,
            drainer: TokioMutex::new(Some(drainer)),
            error_state,
            error_signal,
            cache_state,
            mirror_name,
            topic,
            partition,
            ack_sink,
        })
    }

    /// Install an [`AckSink`]. The drainer calls
    /// `ack.note_through(to + 1)` after every successful POST,
    /// where `to` is the high-water offset of the flushed batch the
    /// blob sink reported. Idempotent if called twice; the first
    /// install wins.
    pub fn with_ack_sink(self, ack: Arc<dyn AckSink>) -> Self {
        let _ = self.ack_sink.set(ack);
        self
    }

    /// Drain pending events and stop the background task. Returns
    /// any error the drainer accumulated before exit. Idempotent -
    /// calling twice is safe (the second call is a no-op).
    ///
    /// The channel is FIFO, so awaiting the drainer after queueing
    /// the Shutdown marker lets every already-queued flush event
    /// dispatch first - aborting instead would silently drop the
    /// final flush notification of a graceful shutdown, and flush
    /// events are not regenerated on restart. A dispatch stuck in
    /// retries holds this up for at most the retry budget
    /// (max-attempts x (timeout + backoff)); beyond that the
    /// orchestrator's termination grace period is the backstop.
    pub async fn shutdown(&mut self) -> Result<(), NotifyError> {
        self.drain_and_stop().await
    }

    /// `&self` version of [`Self::shutdown`] for the supervisor,
    /// which holds the dispatcher behind an `Arc` (the tee owns it
    /// as its `FlushObserver`). Idempotent.
    pub async fn drain_and_stop(&self) -> Result<(), NotifyError> {
        let _ = self.tx.send(FlushEvent::Shutdown);
        if let Some(handle) = self.drainer.lock().await.take() {
            let _ = handle.await;
        }
        if let Some(err) = self.error_state.lock().await.take() {
            return Err(err);
        }
        Ok(())
    }

    /// Snapshot the drainer's latest error without consuming the
    /// dispatcher. Prefer [`Self::terminal_error_watch`] for
    /// supervision; this polling accessor remains for tests and
    /// one-shot status checks.
    pub async fn last_error(&self) -> Option<NotifyError> {
        self.error_state.lock().await.take()
    }

    /// Handle for the supervisor to observe the drainer's terminal
    /// error while the dispatcher itself is owned by the sink as a
    /// `FlushObserver`. See [`TerminalErrorWatch`].
    pub fn terminal_error_watch(&self) -> TerminalErrorWatch {
        TerminalErrorWatch {
            error_state: Arc::clone(&self.error_state),
            signal: Arc::clone(&self.error_signal),
        }
    }
}

impl mirror_core::FlushObserver for FlushDispatcher {
    fn on_flushed(&self, _from: u64, to: u64) {
        // Suppress flush events whose high-water offset hasn't
        // reached this mirror's `suppression_threshold`. The
        // threshold compares against `to` (the flush event's high
        // offset): if `to < threshold` the whole flushed batch is
        // in the suppression window. `on_flushed` is a sync trait
        // method outside the `MIRROR_LABELS` task-local scope, so
        // labels come from the fields populated at construction.
        if self.cache_state.is_record_suppressed(&self.mirror_name, to) {
            metrics::counter!(
                "mirror_v3_notify_suppressed_records_total",
                "topic" => self.topic.clone(),
                "partition" => self.partition.to_string(),
            )
            .increment(1);
            return;
        }
        // Fire-and-forget into the channel. If the drainer has
        // already exited (error_state is set), the send fails; and
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
    error_signal: Arc<TokioNotify>,
    ack_sink: Arc<OnceLock<Arc<dyn AckSink>>>,
) {
    while let Some(event) = rx.recv().await {
        let to = match event {
            FlushEvent::Shutdown => return,
            FlushEvent::Flushed { to } => to,
        };
        let mut offsets = IndexMap::new();
        offsets.insert(inner.partition.to_string(), to);
        // Empty `updates` per WEBHOOKS.md open-question #2:
        // destination-flush is the "tell me a file landed" use case,
        // not cache invalidation, so the consumer doesn't need a key
        // set. The `offsets` field gives them the high-water mark.
        let payload = KkvV1Payload::new(&inner.topic, offsets, IndexMap::new());
        if let Err(e) = inner.dispatch_batch(&payload).await {
            *error_state.lock().await = Some(e);
            error_signal.notify_one();
            return;
        }
        // Successful POST => the batch is delivered. The flush event
        // already represents a durable destination boundary on the
        // blob sink side, so this also reflects the supervisor's
        // notion of "highest offset acked through every gating
        // pathway" for the destination-flush trigger.
        if let Some(ack) = ack_sink.get() {
            ack.note_through(to + 1);
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
            // resolver appends to every A/AAAA address it returns -
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
            // Drop body promptly; outcome decision is status-only.
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
            } else {
                // 5xx, and the 1xx a final response cannot be (reqwest
                // consumes interim ones): not a delivery.
                *error = format!("HTTP {status}");
                Outcome::FiveXx
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
                // connection-refused; they're "couldn't reach the
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
///
/// The `v: 1` field is a load-bearing protocol-version marker.
/// `@yolean/kafka-keyvalue` v1.8.3's `updateListener` (CJS and ESM
/// builds) checks `if (requestBody.v !== 1) throw new Error(...)`
/// before any other parsing; a missing field surfaces as `undefined`,
/// the throw lands inside an Express middleware as an unhandled
/// rejection, and the consumer pod crashloops. The legacy Quarkus
/// kkv server sends this field on every POST.
#[derive(Debug, Serialize)]
struct KkvV1Payload<'a> {
    /// Protocol version. Always 1 for `notify.api: kkv-v1`.
    v: u8,
    topic: &'a str,
    /// `IndexMap` to preserve insertion order on the wire; the legacy
    /// kkv consumer doesn't care about key order but stable output
    /// makes integration tests deterministic.
    offsets: IndexMap<String, u64>,
    updates: IndexMap<String, serde_json::Value>,
}

impl<'a> KkvV1Payload<'a> {
    /// Construct a body with the protocol-version field pinned to 1.
    /// New call sites should use this rather than constructing the
    /// struct directly so the `v: 1` invariant can't be bypassed.
    fn new(
        topic: &'a str,
        offsets: IndexMap<String, u64>,
        updates: IndexMap<String, serde_json::Value>,
    ) -> Self {
        Self {
            v: 1,
            topic,
            offsets,
            updates,
        }
    }
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
