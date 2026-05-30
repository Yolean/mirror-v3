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
}

pub fn load_from_str(yaml: &str) -> Result<Config, LoadError> {
    let cfg: Config = serde_yaml::from_str(yaml)?;
    validate(&cfg)?;
    Ok(cfg)
}

pub fn load_from_path(path: &Path) -> Result<Config, LoadError> {
    let bytes = std::fs::read(path).map_err(|source| LoadError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let cfg: Config = serde_yaml::from_slice(&bytes)?;
    validate(&cfg)?;
    Ok(cfg)
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
    if m.destinations.is_empty() {
        return Err(LoadError::Validation(format!(
            "mirror {:?}: `destinations` must contain at least one entry",
            m.name
        )));
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
    Ok(())
}

fn raw_destination_name(d: &Destination) -> Option<&str> {
    match d {
        Destination::Kafka(k) => k.name.as_deref(),
        Destination::Filesystem(fs) => fs.name.as_deref(),
        Destination::S3(s3) => s3.name.as_deref(),
    }
}
