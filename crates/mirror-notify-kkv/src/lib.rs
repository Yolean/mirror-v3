//! Outbound `kkv-v1` webhook notifier — drop-in replacement for the
//! push side of `Yolean/kafka-keyvalue`.
//!
//! Wire contract (matches the legacy `@yolean/kafka-keyvalue` Node
//! client unmodified; see `WEBHOOKS.md`):
//!   * `POST /kafka-keyvalue/v1/updates`
//!   * Headers: `x-kkv-topic`, `x-kkv-offsets`
//!   * Body: `{ "topic": "...", "offsets": {"<partition>": <offset>}, "updates": { "<key>": null } }`
//!
//! Phase 3a scope: per-record POST (no debounce, no fan-out) wired
//! through the per-outcome retry × final-action state machine from
//! `WEBHOOKS.md` § "Outcomes and retry policy". The buffer that
//! coalesces records into batches per `notify.trigger.debounce` is
//! added on top in Phase 3c.

use std::time::Duration;

use async_trait::async_trait;
use indexmap::IndexMap;
use mirror_config::{
    FinalAction, NotifyApi, NotifyOutcome, NotifyOutcomes, NotifyRetry, NotifyTarget,
};
use mirror_core::{current_labels, Notifier, NotifyError, Record};
use reqwest::Client;
use serde::Serialize;
use thiserror::Error;
use url::Url;

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

/// Per-target dispatcher state. One target = one `Endpoint`. Phase 3a
/// is fan-out: none only; the fan-out: dns-a path will allocate
/// multiple `Endpoint`s per target (one per resolved address) in a
/// later phase.
#[derive(Debug)]
struct Endpoint {
    /// Fully-resolved URL the POST goes to. `kkv-v1` default path is
    /// applied here at build time so the per-request hot path stays
    /// allocation-free.
    url: Url,
    /// Pre-rendered `target_host` metric label (`url.host_str()`).
    target_host: String,
    client: Client,
}

/// Notifier implementing the kkv-v1 wire contract. One instance per
/// mirror (per `(topic, partition)`). Each instance owns its own
/// reqwest client and outcome table.
pub struct KkvV1Notifier {
    endpoints: Vec<Endpoint>,
    outcomes: NotifyOutcomes,
    retry: NotifyRetry,
    topic: String,
    partition: i32,
}

impl KkvV1Notifier {
    /// Build a notifier from a validated [`mirror_config::Notify`]
    /// block. The caller is responsible for the higher-level
    /// validation (URL well-formedness, target non-empty, etc.) —
    /// `mirror-config` does that in `validate_notify_shared`. The
    /// checks here are the lighter-weight last-mile ones the runtime
    /// needs to actually open a `reqwest::Client`.
    ///
    /// Phase 3a/3b: the trigger mode (`source-consume` vs
    /// `destination-flush`) is read by the supervisor but doesn't
    /// alter the dispatcher's behaviour — per-record POST is
    /// equivalent to a max-records=1 debounce. The 3c batch-and-
    /// debounce path will live on this same notifier.
    pub fn from_config(
        notify: &mirror_config::Notify,
        topic: String,
        partition: i32,
    ) -> Result<Self, BuildError> {
        assert_eq!(notify.api, NotifyApi::KkvV1, "only kkv-v1 supported today");
        if notify.targets.is_empty() {
            return Err(BuildError::NoTargets);
        }

        let timeout = Duration::from_millis(notify.timeout_ms);
        // One client per notifier; reqwest's connection pool handles
        // keep-alive across requests to the same host. A future
        // multi-target / fan-out: dns-a path may want per-endpoint
        // clients for size-bounding the pool.
        let client = Client::builder()
            .timeout(timeout)
            // No global redirect-following — 3xx is a documented
            // outcome bucket and must surface as a status code, not
            // get silently followed.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| BuildError::ClientBuild(e.to_string()))?;

        let mut endpoints = Vec::with_capacity(notify.targets.len());
        for t in &notify.targets {
            endpoints.push(build_endpoint(t, client.clone())?);
        }

        Ok(Self {
            endpoints,
            outcomes: notify.outcomes,
            retry: notify.retry,
            topic,
            partition,
        })
    }

    /// POST a single batch payload to every configured endpoint
    /// serially. Used by both the per-record path (Phase 3a) and the
    /// debounced batch path (Phase 3c).
    async fn dispatch_batch(&self, payload: &KkvV1Payload<'_>) -> Result<(), NotifyError> {
        // Serial per endpoint: keeps the dispatch deterministic, makes
        // partial-failure ordering simple, and matches Phase 3a's
        // "one target most of the time" reality. A future fan-out
        // implementation will parallelize across resolved addresses.
        for endpoint in &self.endpoints {
            self.dispatch_one(endpoint, payload).await?;
        }
        Ok(())
    }

    /// Resolve outcome → retry/final-action for a single endpoint.
    async fn dispatch_one(
        &self,
        endpoint: &Endpoint,
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
                "target_host" => endpoint.target_host.clone(),
            )
            .set(attempt as f64);

            let start = std::time::Instant::now();
            let result = endpoint
                .client
                .post(endpoint.url.clone())
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
                "target_host" => endpoint.target_host.clone(),
            )
            .record(start.elapsed().as_secs_f64());

            let outcome = classify(result, &mut last_error);
            let policy = self.outcomes.for_outcome(outcome);

            tracing::debug!(
                target = %endpoint.url,
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
                    "target_host" => endpoint.target_host.clone(),
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
                    target = %endpoint.url,
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
                    endpoint,
                    outcome,
                    policy,
                    attempt,
                    std::mem::take(&mut last_error),
                )
                .await;
        }
    }

    async fn apply_final_action(
        &self,
        endpoint: &Endpoint,
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
            "target_host" => endpoint.target_host.clone(),
        )
        .set(0.0);

        match policy.final_ {
            FinalAction::Accept => {
                tracing::info!(
                    target = %endpoint.url,
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
                    target = %endpoint.url,
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
                    target = %endpoint.url,
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

#[async_trait]
impl Notifier for KkvV1Notifier {
    async fn on_record(&mut self, record: &Record) -> Result<(), NotifyError> {
        // Phase 3a: per-record dispatch. One record → one POST per
        // endpoint. The debounce buffer that coalesces records into
        // batches comes in Phase 3c; until then, `max-records: 1`
        // is the effective config and the per-record HTTP overhead
        // is acceptable at low rates.
        let mut updates = IndexMap::new();
        // Keys may be missing or non-UTF-8. Legacy kkv emits whatever
        // string repr the consumer expects; mirror-v3 chooses
        // lossy-UTF-8 on bytes and `""` on missing key. Real
        // deployments use UTF-8 keys; this keeps the surface working
        // on edge cases instead of crashing.
        let key_str = render_key(record.key.as_deref());
        updates.insert(key_str, serde_json::Value::Null);

        let mut offsets = IndexMap::new();
        offsets.insert(self.partition.to_string(), record.source_offset);

        let payload = KkvV1Payload {
            topic: &self.topic,
            offsets,
            updates,
        };

        let (topic_l, partition_l) = current_labels();
        metrics::counter!(
            "mirror_v3_notify_records_total",
            "topic" => topic_l,
            "partition" => partition_l,
        )
        .increment(1);

        self.dispatch_batch(&payload).await
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
    Ok(Endpoint {
        url,
        target_host,
        client,
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
