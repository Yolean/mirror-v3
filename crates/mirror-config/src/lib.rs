//! Configuration model for mirror-v3.
//!
//! Stable surface:
//! - [`Config`] is the root type; see [`load_from_str`] / [`load_from_path`].
//! - [`schema`] returns the JSON Schema for [`Config`], committed to
//!   `schemas/mirror-v3.config.schema.json` in the repo and gated in CI.
//!
//! ## Shape
//!
//! - Each [`Mirror`] declares one source `(topic, partition)` and a
//!   non-empty list of [`Destination`]s. A mirror with more than one
//!   destination fans the single source consumer through a tee
//!   ([`mirror_core::TeeSink`]) with per-sink heads, so divergent
//!   buffer/flush cadences and heterogeneous-state restart stay
//!   correct.
//! - Per-mirror encoding settings (`format`, `compression`, `keys`,
//!   `values`, `compaction`, `flush`, `timestamp-mode`,
//!   `http-access`) apply to every blob-shaped destination in the
//!   mirror's tee. Kafka destinations ignore the blob-only settings
//!   and honour `timestamp-mode` only. A process that needs two
//!   destinations with different encoding profiles writes them as
//!   two separate mirrors (each its own tee).
//! - Destinations carry only the transport identity: where bytes
//!   land. Per-destination `name` is optional (defaults to
//!   `mirror.name`) and becomes the on-disk / S3-prefix subdirectory.
//!   Kafka destinations also accept an optional `topic` (defaults to
//!   `mirror.topic`, i.e. same-topic-name mirroring).

use std::path::{Path, PathBuf};

use schemars::{schema_for, JsonSchema, Schema};
use serde::{Deserialize, Serialize};

pub mod envsubst;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Config {
    /// One mirror per (source topic, partition). Every mirror runs
    /// in its own task; failures terminate the whole process.
    pub mirrors: Vec<Mirror>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Destination {
    Kafka(KafkaDestination),
    Filesystem(FilesystemDestination),
    S3(S3Destination),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct KafkaDestination {
    /// Identifier for this destination (used in logs / metrics labels
    /// and as a tie-breaker when a mirror has multiple destinations).
    /// Defaults to the enclosing mirror's `name`; required to be set
    /// explicitly when a mirror has more than one destination
    /// (otherwise two destinations would share the same identifier).
    #[serde(default)]
    pub name: Option<String>,
    /// `bootstrap.servers` for the destination cluster.
    pub bootstrap_servers: String,
    /// Destination topic name. Defaults to the enclosing mirror's
    /// `topic` (i.e. same-name mirroring to a different broker).
    /// Override when the destination topic is named differently from
    /// the source.
    #[serde(default)]
    pub topic: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct FilesystemDestination {
    /// Identifier for this destination; see [`KafkaDestination::name`].
    /// Also the subdirectory under `root` where this destination's
    /// files land: `<root>/<name>/<partition>/`.
    #[serde(default)]
    pub name: Option<String>,
    /// Absolute path to the destination root directory.
    pub root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct S3Destination {
    /// Identifier for this destination; see [`KafkaDestination::name`].
    /// Also the subdirectory of the object key prefix where this
    /// destination's objects land:
    /// `<prefix?>/<name>/<partition>/<file>`.
    #[serde(default)]
    pub name: Option<String>,
    /// S3 endpoint URL. Required for non-AWS S3 (e.g. VersityGW);
    /// omit for AWS regional endpoints.
    #[serde(default)]
    pub endpoint: Option<String>,
    pub region: String,
    pub bucket: String,
    /// Key prefix prepended to all written object keys.
    #[serde(default)]
    pub prefix: Option<String>,
}

impl Destination {
    /// Effective identifier for this destination, falling back to
    /// the enclosing mirror's `name` when none was set in YAML.
    pub fn effective_name(&self, mirror_name: &str) -> String {
        match self {
            Destination::Kafka(k) => k.name.clone().unwrap_or_else(|| mirror_name.to_string()),
            Destination::Filesystem(fs) => {
                fs.name.clone().unwrap_or_else(|| mirror_name.to_string())
            }
            Destination::S3(s3) => s3.name.clone().unwrap_or_else(|| mirror_name.to_string()),
        }
    }

    /// True when this destination type buffers in memory between
    /// flushes (filesystem, S3) and therefore needs `flush:` /
    /// `format:` / `compression:` settings on the mirror. False for
    /// Kafka destinations, which commit per-record.
    pub fn is_blob(&self) -> bool {
        !matches!(self, Destination::Kafka(_))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Mirror {
    /// Identifier for the mirror. Also the default `name` for each
    /// destination in this mirror (operators only need to set
    /// per-destination `name` explicitly when a mirror has more than
    /// one destination). Must be unique across mirrors in the same
    /// process.
    pub name: String,

    pub source: KafkaSource,

    /// Source Kafka topic name.
    pub topic: String,

    /// Source Kafka partition. Required, no default.
    pub partition: u32,

    /// Destinations this mirror fans into. Required; non-empty. With
    /// more than one entry, a [`mirror_core::TeeSink`] sits between
    /// the source consumer and the sinks, preserving each inner
    /// sink's end-offset gate under divergent buffering and
    /// heterogeneous-state restart.
    pub destinations: Vec<Destination>,

    /// Envelope format for written files. Required (defaults to
    /// `parquet`) when any destination is filesystem/s3; ignored by
    /// Kafka destinations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<DestinationFormat>,

    /// Parquet compression. Only meaningful when `format = parquet`.
    /// Defaults to `zstd-1`. Ignored by Kafka destinations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<ParquetCompression>,

    /// Topic schema for the record `key`. Defaults to `{ type: utf8 }`.
    /// For Kafka destinations this is purely a validation contract
    /// (the record passes through unchanged); for filesystem/s3 +
    /// parquet it also selects the column encoding. See [`ColumnType`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys: Option<ColumnConfig>,

    /// Topic schema for the record `value`. Same semantics as `keys`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<ColumnConfig>,

    /// Optional log-compaction mode. When `log`, each Parquet file
    /// is a full materialised snapshot of the latest value per key.
    /// Requires `format = parquet`. Forbidden when *every*
    /// destination is Kafka.
    #[serde(default)]
    pub compaction: Option<Compaction>,

    /// Flush triggers for blob destinations. Required when any
    /// destination is filesystem/s3; forbidden when every destination
    /// is Kafka (which never buffers).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush: Option<FlushTriggers>,

    /// Which timestamp lands on Kafka destination records. Defaults
    /// to `source`. Forbidden when no destination is Kafka.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_mode: Option<TimestampMode>,

    /// Opt-in HTTP read access for this mirror's materialized view.
    /// Requires at least one filesystem/s3 destination (the cache
    /// bootstraps from durable destination state). Multiple mirrors
    /// with the same `api` are unioned into a single keyspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_access: Option<HttpAccess>,

    /// Whether mirror-v3 should actually spawn this mirror at
    /// startup. Defaults to `true`. Plain YAML boolean only —
    /// `true` / `false` (and the YAML-1.2 case variants
    /// `True`/`False`/`TRUE`/`FALSE`). The YAML-1.1 truthy/falsy
    /// strings (`yes`/`no`/`on`/`off`) are deliberately NOT
    /// accepted; operators who want to flip a mirror via env
    /// interpolation should write the env value as `true` or
    /// `false`:
    ///
    /// ```yaml
    /// - name: requests
    ///   enabled: ${REQUESTS_ENABLED:-false}
    ///   ...
    /// ```
    ///
    /// Disabled mirrors are validated the same as enabled ones (so
    /// flipping `false` → `true` won't surface latent config bugs)
    /// but are not spawned, do not register with the cache-v1
    /// readiness gate, and do not contribute to source-broker reads.
    /// If *every* mirror in a process is disabled, startup fails
    /// loudly so a misconfigured deployment doesn't silently idle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,

    /// Opt-in outbound webhook notify. Closes the legacy
    /// `Yolean/kafka-keyvalue` (kkv) "onupdate" gap: when a record
    /// lands in the mirror's view, POST to one or more downstream
    /// services so their in-process caches can invalidate and
    /// re-fetch via `/cache/v1/raw/<key>`.
    ///
    /// Today the only `api` variant is `kkv-v1`, which matches the
    /// legacy kkv wire contract byte-for-byte so the upstream
    /// `@yolean/kafka-keyvalue` Node client works unmodified.
    ///
    /// See `WEBHOOKS.md` at the repo root for the full design,
    /// trigger modes, outcome matrix, and DNS-A fan-out semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notify: Option<Notify>,
}

impl Mirror {
    /// Whether this mirror is enabled (default `true` when the
    /// `enabled` field is omitted in YAML).
    pub fn is_enabled(&self) -> bool {
        self.enabled.unwrap_or(true)
    }
}

// ============================================================
//   Notify (outbound webhook) — kkv-v1 drop-in for now
// ============================================================

/// Per-mirror outbound notify block. Today only the `kkv-v1` API
/// variant is supported; future variants (e.g. `nats-v1`, a
/// `kkv-v2` with auth) hang off the same block without re-shaping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Notify {
    pub api: NotifyApi,
    /// One or more downstream targets. Each target carries its own
    /// URL and fan-out mode. Multi-target notify fan-out is parallel
    /// and per-target outcomes resolve independently.
    pub targets: Vec<NotifyTarget>,
    #[serde(default)]
    pub trigger: NotifyTrigger,
    /// Per-request HTTP timeout. Independent of retry policy: timing
    /// out is one of the six outcomes whose action is configurable.
    /// Spec default: 5000 ms.
    #[serde(default = "default_notify_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub retry: NotifyRetry,
    #[serde(default)]
    pub outcomes: NotifyOutcomes,
}

/// The wire-contract variant this notify block speaks. Today only
/// the legacy kkv shape exists. New variants must explicitly opt
/// in — kkv-v1 is not the default to avoid silently changing
/// behaviour if we ever add e.g. a kkv-v2 with auth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum NotifyApi {
    /// `POST /kafka-keyvalue/v1/updates` with the legacy kkv body:
    /// `{ topic, offsets, updates: { <key>: null } }`. Matches the
    /// `@yolean/kafka-keyvalue` Node client's
    /// `getOnUpdateRoute()` / `ON_UPDATE_DEFAULT_PATH`.
    KkvV1,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct NotifyTarget {
    /// Full URL of the target. Path defaults to
    /// `/kafka-keyvalue/v1/updates` under `api: kkv-v1` if `path`
    /// is unset; explicit override is allowed for non-kkv clients.
    pub url: String,
    /// Override the URL's path segment. Defaults to the
    /// api-variant-defined path (`/kafka-keyvalue/v1/updates`
    /// for kkv-v1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// How the URL's host is resolved. `none` (default) sends one
    /// POST to a single keep-alive connection; `dns-a` resolves
    /// the host to its full A/AAAA record set and POSTs to every
    /// returned address concurrently — the K8s-headless-Service
    /// fan-out path without a Kubernetes API dependency.
    #[serde(default)]
    pub fan_out: FanOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "kebab-case")]
pub enum FanOut {
    /// Standard DNS, single keep-alive connection. Adequate for a
    /// non-K8s target or a single-replica deployment.
    #[default]
    None,
    /// Resolve the URL's host to all A/AAAA records and POST to
    /// every address concurrently. Headless Kubernetes Services
    /// return one A-record per pod, giving the same fan-out the
    /// legacy kkv did via the Endpoints API.
    DnsA,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct NotifyTrigger {
    pub on: TriggerOn,
    /// Required when `on: source-consume`; forbidden when
    /// `on: destination-flush` (the destination's own flush
    /// triggers ARE the debounce in that mode). Defaults to
    /// `{ max-records: 100, max-time-ms: 250 }`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub debounce: Option<NotifyDebounce>,
}

impl Default for NotifyTrigger {
    fn default() -> Self {
        Self {
            on: TriggerOn::default(),
            // `Some(...)` so the YAML-omitted case still has the
            // spec-default {100, 250} window when source-consume
            // applies. Validator can still reject explicit
            // `destination-flush + debounce`.
            debounce: Some(NotifyDebounce::default()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "kebab-case")]
pub enum TriggerOn {
    /// POST as soon as the consume loop hands a record to the
    /// mirror — bounded by the `debounce` window. Default;
    /// matches legacy kkv behaviour.
    #[default]
    SourceConsume,
    /// POST when *every* destination has durably committed past
    /// the batch's high-water offset. The notify body's offset
    /// range matches the flushed snapshot's `from`–`to`. Wrong
    /// for cache invalidation; right for downstream archival
    /// hints.
    DestinationFlush,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct NotifyDebounce {
    pub max_records: u64,
    pub max_time_ms: u64,
}

impl Default for NotifyDebounce {
    fn default() -> Self {
        Self {
            max_records: 100,
            max_time_ms: 250,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct NotifyRetry {
    pub max_attempts: u32,
    pub backoff_ms: u64,
}

impl Default for NotifyRetry {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            backoff_ms: 100,
        }
    }
}

fn default_notify_timeout_ms() -> u64 {
    5000
}

/// The six request outcomes and what each one means for the mirror.
/// Per-field omission falls back to the spec-default for that
/// outcome only (one outcome being explicit doesn't force the
/// others to be).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct NotifyOutcomes {
    #[serde(default = "default_outcome_timeout")]
    pub timeout: NotifyOutcome,
    #[serde(default = "default_outcome_connrefused")]
    pub connrefused: NotifyOutcome,
    /// HTTP 2xx — the only success outcome.
    #[serde(rename = "2xx", default = "default_outcome_2xx")]
    pub two_xx: NotifyOutcome,
    /// HTTP 3xx — almost always misconfiguration on a webhook.
    #[serde(rename = "3xx", default = "default_outcome_3xx")]
    pub three_xx: NotifyOutcome,
    /// HTTP 4xx — receiver says "your request is wrong";
    /// retrying the same payload doesn't help.
    #[serde(rename = "4xx", default = "default_outcome_4xx")]
    pub four_xx: NotifyOutcome,
    /// HTTP 5xx — receiver is transiently broken; retry per
    /// policy and fail on exhaustion.
    #[serde(rename = "5xx", default = "default_outcome_5xx")]
    pub five_xx: NotifyOutcome,
}

impl Default for NotifyOutcomes {
    fn default() -> Self {
        Self {
            timeout: default_outcome_timeout(),
            connrefused: default_outcome_connrefused(),
            two_xx: default_outcome_2xx(),
            three_xx: default_outcome_3xx(),
            four_xx: default_outcome_4xx(),
            five_xx: default_outcome_5xx(),
        }
    }
}

fn default_outcome_timeout() -> NotifyOutcome {
    NotifyOutcome {
        retry: true,
        final_: FinalAction::Fail,
    }
}
fn default_outcome_connrefused() -> NotifyOutcome {
    NotifyOutcome {
        retry: true,
        final_: FinalAction::Fail,
    }
}
fn default_outcome_2xx() -> NotifyOutcome {
    NotifyOutcome {
        retry: false,
        final_: FinalAction::Accept,
    }
}
fn default_outcome_3xx() -> NotifyOutcome {
    NotifyOutcome {
        retry: false,
        final_: FinalAction::Fail,
    }
}
fn default_outcome_4xx() -> NotifyOutcome {
    NotifyOutcome {
        retry: false,
        final_: FinalAction::Fail,
    }
}
fn default_outcome_5xx() -> NotifyOutcome {
    NotifyOutcome {
        retry: true,
        final_: FinalAction::Fail,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct NotifyOutcome {
    /// If `true`, the request is retried per [`NotifyRetry`] before
    /// [`Self::final_`] is applied. If `false`, the action in
    /// [`Self::final_`] is taken on the first attempt.
    pub retry: bool,
    /// What happens once retries (if any) are exhausted.
    #[serde(rename = "final")]
    pub final_: FinalAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum FinalAction {
    /// Treat the batch as delivered, advance.
    Accept,
    /// Log WARN, drop the batch, advance.
    Skip,
    /// Mirror task errors out; orchestrator restarts; mirror
    /// replays from durable state on restart.
    Fail,
}

/// HTTP read-access block. Today the only variant is the KKV-compatible
/// `/cache/v1` surface; the field is grouped so future APIs can be
/// added without re-shaping the YAML.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct HttpAccess {
    pub api: HttpAccessApi,
}

/// Variants of the read API surface mirror-v3 will host. Each opt-in
/// mirror declares which one applies to it; today only `cache-v1`
/// exists (a drop-in for `Yolean/kafka-keyvalue`'s `/cache/v1`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum HttpAccessApi {
    /// `/cache/v1/raw/{key}`, `/cache/v1/keys`, `/cache/v1/values`,
    /// `/cache/v1/offset/{topic}/{partition}`. See the `mirror-cache`
    /// crate for behavior and the committed OpenAPI 3.1 spec in
    /// `schemas/mirror-v3.cache.openapi.json`.
    CacheV1,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct KafkaSource {
    pub bootstrap_servers: String,
    /// Optional consumer group id used for monitoring/back-pressure
    /// only. Restart correctness derives from the destination, never
    /// from committed group offsets.
    #[serde(default)]
    pub group_id: Option<String>,
}

/// Schema for a record column (`key` or `value`). `type` is the only
/// field today; future extensions (schema-registry refs, codecs) hang
/// off this block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct ColumnConfig {
    #[serde(rename = "type", default)]
    pub kind: ColumnType,
}

/// Topic-schema declaration for a record column (`key` or `value`).
///
/// The variant describes *what the data is* and the validation
/// contract the mirror enforces at encode. How the destination
/// represents the column is destination-specific:
///
/// - **Kafka destinations**: the record is passed through as-is; the
///   declared type acts as a validation gate (non-UTF-8 input under
///   `utf8`/`json`/`json-parseable`, unparseable JSON under
///   `json-parseable` → mirror fails with the offending offset).
/// - **Filesystem/S3 + parquet**: every variant lands as `Utf8`.
///   `bytes` is base64-encoded; `utf8`/`json`/`json-parseable` are
///   stored verbatim. Extension metadata tags the field for
///   downstream consumers:
///
///   | Variant           | Parquet | Field metadata                          |
///   | ----------------- | ------- | --------------------------------------- |
///   | `bytes`           | `Utf8`  | `ARROW:extension:name = mirror_v3.bytes_base64` |
///   | `utf8`            | `Utf8`  | (none)                                  |
///   | `json`            | `Utf8`  | `ARROW:extension:name = arrow.json`     |
///   | `json-parseable`  | `Utf8`  | `ARROW:extension:name = arrow.json`     |
///
/// - **Filesystem/S3 + ndjson**: only the default (`utf8`) is
///   accepted today; other variants are rejected at config load.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ColumnType {
    /// Arbitrary bytes. No validation. For Kafka destinations the
    /// record passes through unchanged; for Parquet destinations the
    /// bytes are base64-encoded into a `Utf8` column (storage
    /// detail). Use for protobuf-keyed topics or other binary
    /// payloads.
    Bytes,
    /// UTF-8 string. Non-UTF-8 input is a hard `Encode` error at the
    /// mirror, with the offending source offset.
    #[default]
    Utf8,
    /// UTF-8 string carrying a JSON document. Same encode contract as
    /// `utf8` (UTF-8 enforced, payload *not* parsed). On Parquet
    /// destinations the column field is tagged with the `arrow.json`
    /// canonical extension as a hint to downstream consumers.
    Json,
    /// UTF-8 string carrying a JSON document, *parseability-gated*.
    /// In addition to the UTF-8 check, the encoder feeds each
    /// non-null payload through `serde_json` and rejects any record
    /// that does not parse, with the offending source offset.
    JsonParseable,
}

/// Kafka-destination timestamp behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "kebab-case")]
pub enum TimestampMode {
    /// Pass `record.timestamp_ms` to the producer. Destination
    /// stores it as CreateTime.
    #[default]
    Source,
    /// Do not pass an explicit timestamp; the destination broker
    /// stamps the record itself.
    Destination,
}

/// Compaction strategy. Reserved for future variants (e.g. windowed,
/// range). Default is "no compaction" (omit the field).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum Compaction {
    /// Kafka-style log compaction: keep the latest value per key.
    /// Each Parquet file is a full materialized snapshot. Null-value
    /// records are interpreted as tombstones (the key is removed
    /// from the materialized view). Null keys and non-UTF-8 keys are
    /// hard errors at encode time.
    Log,
}

/// Envelope format for Filesystem and S3 destinations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "kebab-case")]
pub enum DestinationFormat {
    /// Apache Parquet. Columnar, embedded schema, compressed.
    /// Standard data-lake format — readable by DuckDB / Athena /
    /// Spark out of the box.
    #[default]
    Parquet,
    /// Newline-delimited JSON, one record per line, base64-encoded
    /// binary fields. Operator-friendly for `jq` debugging; larger
    /// on disk than Parquet. Incompatible with non-default `keys`,
    /// `values` and `compaction`.
    Ndjson,
}

/// Parquet compression codec. Only meaningful when [`DestinationFormat::Parquet`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
pub enum ParquetCompression {
    /// Zstd level 1: strong compression, fast decode. Default.
    #[default]
    #[serde(rename = "zstd-1")]
    Zstd1,
    /// Zstd level 3: smaller files, slower encode.
    #[serde(rename = "zstd-3")]
    Zstd3,
    /// Snappy: fast, larger files than zstd.
    #[serde(rename = "snappy")]
    Snappy,
    /// LZ4: fast, larger files than zstd.
    #[serde(rename = "lz4")]
    Lz4,
    /// No compression. Debug-friendly; not recommended in production.
    #[serde(rename = "uncompressed")]
    Uncompressed,
}

/// Flush triggers for blob-style mirrors (Filesystem, S3). The three
/// size/time triggers must be set; any one tripping causes a flush
/// (set a trigger to a very large number to effectively disable it).
/// The optional `daily` trigger adds a wall-clock-UTC boundary on top.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct FlushTriggers {
    /// Maximum time, in milliseconds, between flushes.
    pub max_time_ms: u64,
    /// Maximum buffered bytes before a flush.
    pub max_bytes: u64,
    /// Maximum buffered source offsets before a flush.
    pub max_offsets: u64,
    /// Optional once-per-day wall-clock-UTC flush boundary.
    #[serde(default)]
    pub daily: Option<DailyFlush>,
}

/// Wall-clock-UTC daily flush boundary. Naive implementation: on
/// startup, schedule the next future occurrence of `at_utc`; when
/// the clock crosses it, flush any buffered records and advance to
/// tomorrow's slot. If the buffer is empty at the boundary, the
/// slot is silently skipped (no zero-record file).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct DailyFlush {
    /// UTC wall-clock time of day, `HH:MM` or `HH:MM:SS`.
    pub at_utc: String,
}

#[derive(Debug, thiserror::Error)]
pub enum AtUtcError {
    #[error("at-utc must be HH:MM or HH:MM:SS, got {0:?}")]
    Format(String),
    #[error("at-utc out of range (HH 0-23, MM 0-59, SS 0-59), got {0:?}")]
    Range(String),
}

impl DailyFlush {
    /// Parse `at_utc` to seconds since UTC midnight, `0..86400`.
    pub fn parse_at_utc(&self) -> Result<u32, AtUtcError> {
        let parts: Vec<&str> = self.at_utc.split(':').collect();
        let (h_s, m_s, s_s) = match parts.as_slice() {
            [h, m] => (*h, *m, "0"),
            [h, m, s] => (*h, *m, *s),
            _ => return Err(AtUtcError::Format(self.at_utc.clone())),
        };
        let h: u32 = h_s
            .parse()
            .map_err(|_| AtUtcError::Format(self.at_utc.clone()))?;
        let m: u32 = m_s
            .parse()
            .map_err(|_| AtUtcError::Format(self.at_utc.clone()))?;
        let s: u32 = s_s
            .parse()
            .map_err(|_| AtUtcError::Format(self.at_utc.clone()))?;
        if h > 23 || m > 59 || s > 59 {
            return Err(AtUtcError::Range(self.at_utc.clone()));
        }
        Ok(h * 3600 + m * 60 + s)
    }
}

#[cfg(test)]
mod daily_tests {
    use super::*;

    fn d(s: &str) -> DailyFlush {
        DailyFlush { at_utc: s.into() }
    }

    #[test]
    fn parses_hh_mm_ss() {
        assert_eq!(d("00:00:00").parse_at_utc().unwrap(), 0);
        assert_eq!(d("00:00:01").parse_at_utc().unwrap(), 1);
        assert_eq!(d("01:02:03").parse_at_utc().unwrap(), 3723);
        assert_eq!(d("23:59:59").parse_at_utc().unwrap(), 86399);
    }

    #[test]
    fn parses_hh_mm() {
        assert_eq!(d("00:00").parse_at_utc().unwrap(), 0);
        assert_eq!(d("12:34").parse_at_utc().unwrap(), 45240);
        assert_eq!(d("23:59").parse_at_utc().unwrap(), 86340);
    }

    #[test]
    fn rejects_out_of_range() {
        assert!(matches!(
            d("24:00").parse_at_utc(),
            Err(AtUtcError::Range(_))
        ));
        assert!(matches!(
            d("00:60").parse_at_utc(),
            Err(AtUtcError::Range(_))
        ));
        assert!(matches!(
            d("00:00:60").parse_at_utc(),
            Err(AtUtcError::Range(_))
        ));
        assert!(matches!(
            d("99:99:99").parse_at_utc(),
            Err(AtUtcError::Range(_))
        ));
    }

    #[test]
    fn rejects_garbage() {
        assert!(matches!(d("").parse_at_utc(), Err(AtUtcError::Format(_))));
        assert!(matches!(
            d("abc").parse_at_utc(),
            Err(AtUtcError::Format(_))
        ));
        assert!(matches!(d("12").parse_at_utc(), Err(AtUtcError::Format(_))));
        assert!(matches!(
            d("1:2:3:4").parse_at_utc(),
            Err(AtUtcError::Format(_))
        ));
        assert!(matches!(
            d("12:").parse_at_utc(),
            Err(AtUtcError::Format(_))
        ));
    }
}

/// JSON Schema for [`Config`]. Use this from `xtask gen-schema` to
/// regenerate `schemas/mirror-v3.config.schema.json`.
pub fn schema() -> Schema {
    schema_for!(Config)
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("reading config file {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parsing config: {0}")]
    Parse(#[from] serde_yaml::Error),
    #[error("invalid config: {0}")]
    Validation(String),
    #[error("env interpolation: {0}")]
    EnvSubst(#[from] envsubst::EnvSubstError),
}

/// Load and validate config from a YAML string, expanding any
/// `${VAR}` / `${VAR:-default}` / `$$` references against the
/// process environment first. See [`envsubst`] for the syntax.
pub fn load_from_str(yaml: &str) -> Result<Config, LoadError> {
    load_from_str_with_env(yaml, &envsubst::OsEnv)
}

/// Test-friendly variant of [`load_from_str`] that resolves env-subst
/// references through a caller-supplied [`envsubst::Env`] instead of
/// the real process environment.
pub fn load_from_str_with_env(yaml: &str, env: &dyn envsubst::Env) -> Result<Config, LoadError> {
    let expanded = envsubst::expand(yaml, env)?;
    let cfg: Config = serde_yaml::from_str(&expanded)?;
    validate(&cfg)?;
    Ok(cfg)
}

pub fn load_from_path(path: &Path) -> Result<Config, LoadError> {
    let bytes = std::fs::read(path).map_err(|source| LoadError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let yaml = std::str::from_utf8(&bytes).map_err(|e| {
        LoadError::Validation(format!("config file {path:?} is not valid UTF-8: {e}"))
    })?;
    load_from_str(yaml)
}

/// Cross-field validation that can't be expressed in serde attributes.
fn validate(cfg: &Config) -> Result<(), LoadError> {
    let mut seen_names: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for m in &cfg.mirrors {
        if !seen_names.insert(&m.name) {
            return Err(LoadError::Validation(format!(
                "mirror name {:?} appears more than once",
                m.name
            )));
        }
        validate_mirror(m)?;
    }
    Ok(())
}

fn validate_mirror(m: &Mirror) -> Result<(), LoadError> {
    // Destinations-empty is allowed ONLY when notify is set with at
    // least one target (the "notify-only mirror" shape — see
    // WEBHOOKS.md). Other rules in this function are then either
    // skipped (everything destination-shaped) or applied with
    // tighter restrictions (e.g. http-access forbidden).
    if m.destinations.is_empty() {
        let Some(notify) = m.notify.as_ref() else {
            return Err(LoadError::Validation(format!(
                "mirror {:?}: `destinations` must contain at least one entry, \
                 unless `notify` is set (notify-only mirrors are allowed)",
                m.name
            )));
        };
        if notify.targets.is_empty() {
            return Err(LoadError::Validation(format!(
                "mirror {:?}: notify-only mirror requires `notify.targets` to be non-empty",
                m.name
            )));
        }
        return validate_notify_only(m, notify);
    }

    // Per-destination identifiers: explicit `name` is required when a
    // mirror has more than one destination (otherwise the default
    // `mirror.name` would collide). With exactly one destination,
    // the default is unambiguous and `name` is optional.
    let multi = m.destinations.len() > 1;
    let mut seen_dest_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (i, d) in m.destinations.iter().enumerate() {
        if multi && raw_destination_name(d).is_none() {
            return Err(LoadError::Validation(format!(
                "mirror {:?}: destination[{i}] requires an explicit `name` \
                 (mirrors with multiple destinations cannot share the default \
                 `name = mirror.name`)",
                m.name
            )));
        }
        let effective = d.effective_name(&m.name);
        if !seen_dest_names.insert(effective.clone()) {
            return Err(LoadError::Validation(format!(
                "mirror {:?}: destination name {:?} appears more than once",
                m.name, effective
            )));
        }
    }
    let has_blob = m.destinations.iter().any(|d| d.is_blob());
    let has_kafka = m.destinations.iter().any(|d| !d.is_blob());

    // Encoding/flush settings: required when any blob destination is
    // present; rejected when every destination is Kafka.
    if !has_blob {
        for (field, present) in [
            ("format", m.format.is_some()),
            ("compression", m.compression.is_some()),
            ("compaction", m.compaction.is_some()),
            ("flush", m.flush.is_some()),
            ("http-access", m.http_access.is_some()),
        ] {
            if present {
                return Err(LoadError::Validation(format!(
                    "mirror {:?}: `{field}` is only valid when at least one destination is \
                     filesystem/s3",
                    m.name
                )));
            }
        }
    }

    if !has_kafka && m.timestamp_mode.is_some() {
        return Err(LoadError::Validation(format!(
            "mirror {:?}: `timestamp-mode` only affects Kafka destinations; \
             this mirror has none",
            m.name
        )));
    }

    if has_blob {
        if m.flush.is_none() {
            return Err(LoadError::Validation(format!(
                "mirror {:?}: `flush` is required when any destination is filesystem/s3",
                m.name
            )));
        }
        let format = m.format.unwrap_or_default();
        let keys = m.keys.unwrap_or_default();
        let values = m.values.unwrap_or_default();
        if matches!(format, DestinationFormat::Ndjson) {
            if !matches!(keys.kind, ColumnType::Utf8) || !matches!(values.kind, ColumnType::Utf8) {
                return Err(LoadError::Validation(format!(
                    "mirror {:?}: ndjson does not honour `keys`/`values` types; \
                     remove them or switch to `format: parquet`",
                    m.name
                )));
            }
            if m.compaction.is_some() {
                return Err(LoadError::Validation(format!(
                    "mirror {:?}: `compaction` requires `format: parquet`",
                    m.name
                )));
            }
        }
        if m.http_access.is_some() && matches!(keys.kind, ColumnType::Bytes) {
            return Err(LoadError::Validation(format!(
                "mirror {:?}: `http-access` requires `keys.type` ∈ {{utf8, json, json-parseable}}; \
                 /cache/v1 routes keys through URL path segments",
                m.name
            )));
        }
    }

    // Notify on a mirror with destinations: per WEBHOOKS.md, the
    // notify body says "go re-read via /cache/v1/raw/<key>". That's
    // only meaningful when http-access is set.
    if let Some(notify) = m.notify.as_ref() {
        if m.http_access.is_none() {
            return Err(LoadError::Validation(format!(
                "mirror {:?}: `notify` requires `http-access: {{ api: cache-v1 }}` on the same \
                 mirror (the notify body tells consumers to re-read via /cache/v1)",
                m.name
            )));
        }
        validate_notify_shared(m, notify)?;
    }
    Ok(())
}

/// Validation rules that apply to every notify block regardless of
/// whether the mirror has destinations. URL parses, targets
/// non-empty, debounce sanity, retry sanity, timeout sanity.
fn validate_notify_shared(m: &Mirror, notify: &Notify) -> Result<(), LoadError> {
    if notify.targets.is_empty() {
        return Err(LoadError::Validation(format!(
            "mirror {:?}: `notify.targets` must contain at least one entry",
            m.name
        )));
    }
    for (i, t) in notify.targets.iter().enumerate() {
        match url::Url::parse(&t.url) {
            Ok(u) => {
                let scheme = u.scheme();
                if scheme != "http" && scheme != "https" {
                    return Err(LoadError::Validation(format!(
                        "mirror {:?}: notify.targets[{i}].url must use scheme http or https, \
                         got {scheme:?}",
                        m.name
                    )));
                }
                if u.host_str().map(str::is_empty).unwrap_or(true) {
                    return Err(LoadError::Validation(format!(
                        "mirror {:?}: notify.targets[{i}].url has no host",
                        m.name
                    )));
                }
            }
            Err(e) => {
                return Err(LoadError::Validation(format!(
                    "mirror {:?}: notify.targets[{i}].url is not a valid URL: {e}",
                    m.name
                )));
            }
        }
    }
    if notify.timeout_ms < 1 {
        return Err(LoadError::Validation(format!(
            "mirror {:?}: `notify.timeout-ms` must be >= 1",
            m.name
        )));
    }
    if notify.retry.max_attempts < 1 {
        return Err(LoadError::Validation(format!(
            "mirror {:?}: `notify.retry.max-attempts` must be >= 1",
            m.name
        )));
    }
    if notify.retry.backoff_ms < 1 {
        return Err(LoadError::Validation(format!(
            "mirror {:?}: `notify.retry.backoff-ms` must be >= 1",
            m.name
        )));
    }
    match notify.trigger.on {
        TriggerOn::SourceConsume => {
            // `debounce` is required (the constructor default
            // populates it; explicit `debounce: null` is rejected).
            let debounce = notify.trigger.debounce.as_ref().ok_or_else(|| {
                LoadError::Validation(format!(
                    "mirror {:?}: `notify.trigger.debounce` is required when \
                     `trigger.on: source-consume`",
                    m.name
                ))
            })?;
            if debounce.max_records < 1 {
                return Err(LoadError::Validation(format!(
                    "mirror {:?}: `notify.trigger.debounce.max-records` must be >= 1",
                    m.name
                )));
            }
            if debounce.max_time_ms < 1 {
                return Err(LoadError::Validation(format!(
                    "mirror {:?}: `notify.trigger.debounce.max-time-ms` must be >= 1",
                    m.name
                )));
            }
        }
        TriggerOn::DestinationFlush => {
            // The destination's own flush triggers ARE the debounce
            // in this mode. Explicit debounce is redundant noise; we
            // could tolerate it, but rejecting catches typos and
            // makes the spec's "no `debounce` block applies" rule
            // observable.
            if notify.trigger.debounce.is_some() {
                return Err(LoadError::Validation(format!(
                    "mirror {:?}: `notify.trigger.debounce` is forbidden when \
                     `trigger.on: destination-flush`; the destination flush triggers are the \
                     debounce in that mode",
                    m.name
                )));
            }
        }
    }
    Ok(())
}

/// Extra restrictions on top of [`validate_notify_shared`] when the
/// mirror has no destinations: notify is the only side-effect, so
/// destination-shaped fields are all forbidden, http-access is
/// forbidden, and trigger.on must be source-consume.
fn validate_notify_only(m: &Mirror, notify: &Notify) -> Result<(), LoadError> {
    for (field, present) in [
        ("format", m.format.is_some()),
        ("compression", m.compression.is_some()),
        ("keys", m.keys.is_some()),
        ("values", m.values.is_some()),
        ("compaction", m.compaction.is_some()),
        ("flush", m.flush.is_some()),
        ("timestamp-mode", m.timestamp_mode.is_some()),
        ("http-access", m.http_access.is_some()),
    ] {
        if present {
            return Err(LoadError::Validation(format!(
                "mirror {:?}: notify-only mirrors (no destinations) cannot set `{field}`; \
                 there is nothing for it to apply to",
                m.name
            )));
        }
    }
    if matches!(notify.trigger.on, TriggerOn::DestinationFlush) {
        return Err(LoadError::Validation(format!(
            "mirror {:?}: notify-only mirrors must use `trigger.on: source-consume` \
             (no destinations to flush)",
            m.name
        )));
    }
    validate_notify_shared(m, notify)
}

fn raw_destination_name(d: &Destination) -> Option<&str> {
    match d {
        Destination::Kafka(k) => k.name.as_deref(),
        Destination::Filesystem(fs) => fs.name.as_deref(),
        Destination::S3(s3) => s3.name.as_deref(),
    }
}
