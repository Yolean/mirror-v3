//! Configuration model for mirror-v3.
//!
//! Stable surface:
//! - [`Config`] is the root type; see [`load_from_str`] / [`load_from_path`].
//! - [`schema`] returns the JSON Schema for [`Config`], committed to
//!   `schemas/mirror-v3.config.schema.json` in the repo and gated in CI.
//!
//! ## Shape
//!
//! - The `destination` block is **transport only**: where bytes land
//!   (`type`, `bucket`, `endpoint`, `root`, `bootstrap-servers`, …).
//! - Every property that shapes a file or governs a single mirror's
//!   cadence — `format`, `compression`, `keys`, `values`, `flush`,
//!   `compaction`, `timestamp-mode` — lives on the **mirror** entry.
//!   A single process can run two mirrors with different encoding
//!   profiles against the same destination.

use std::path::{Path, PathBuf};

use schemars::{schema_for, JsonSchema, Schema};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Config {
    /// Shared transport. Every mirror writes through this destination.
    pub destination: Destination,

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
    /// `bootstrap.servers` for the destination cluster.
    pub bootstrap_servers: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct FilesystemDestination {
    /// Absolute path to the destination root directory. Each mirror
    /// writes under `<root>/<mirror.name>/<partition>/`.
    pub root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct S3Destination {
    /// S3 endpoint URL. Required for non-AWS S3 (e.g. VersityGW); omit
    /// for AWS regional endpoints.
    #[serde(default)]
    pub endpoint: Option<String>,
    pub region: String,
    pub bucket: String,
    /// Key prefix prepended to all written object keys. Each mirror
    /// writes under `<prefix?>/<mirror.name>/<partition>/<file>`.
    #[serde(default)]
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Mirror {
    /// Identifier for the mirror. Also the on-disk / S3-prefix segment
    /// under which this mirror's files land
    /// (`<root>/<name>/<partition>/`). Must be unique across mirrors
    /// inside the same process.
    pub name: String,

    pub source: KafkaSource,

    /// Source Kafka topic name.
    pub topic: String,

    /// Source Kafka partition. Required, no default.
    pub partition: u32,

    /// Envelope format for written files. Required for `filesystem` and
    /// `s3` destinations; forbidden for `kafka` destinations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<DestinationFormat>,

    /// Parquet compression. Only meaningful when `format = parquet`.
    /// Defaults to `zstd-1`. Forbidden for `kafka` destinations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<ParquetCompression>,

    /// Topic schema for the record `key`. Defaults to `{ type: utf8 }`.
    /// For Kafka mirrors this is purely a validation contract (the
    /// record passes through unchanged); for filesystem/s3 + parquet
    /// it also selects the column encoding. See [`ColumnType`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys: Option<ColumnConfig>,

    /// Topic schema for the record `value`. Same semantics as `keys`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<ColumnConfig>,

    /// Optional log-compaction mode. When `log`, each Parquet file is a
    /// full materialized snapshot of the latest value per key. Requires
    /// `format = parquet`. Forbidden for `kafka` destinations.
    #[serde(default)]
    pub compaction: Option<Compaction>,

    /// Flush triggers. Required for `filesystem` and `s3` destinations;
    /// forbidden for `kafka` destinations (which never buffer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush: Option<FlushTriggers>,

    /// Which timestamp lands on the destination record. Only meaningful
    /// for `kafka` destinations; forbidden for `filesystem` and `s3`.
    /// Defaults to `source`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_mode: Option<TimestampMode>,
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
/// - **Kafka mirrors**: the record is passed through as-is; the
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
///
/// The `key` and `value` columns in Parquet are always named `key`
/// and `value`.
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
    /// that does not parse, with the offending source offset. The
    /// parser uses `serde::de::IgnoredAny` so no `serde_json::Value`
    /// tree is allocated — the cost is one structure walk per
    /// payload. Same on-disk shape as `json`.
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
    // Future: jitter-ms, debounce-ms, and a smarter restart
    // schedule that consults the destination's most recent blob
    // (mtime / last record timestamp) to decide whether the
    // boundary was already honored across a process bounce.
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
    let is_kafka = matches!(cfg.destination, Destination::Kafka(_));
    let mut seen_names: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for m in &cfg.mirrors {
        if !seen_names.insert(&m.name) {
            return Err(LoadError::Validation(format!(
                "mirror name {:?} appears more than once",
                m.name
            )));
        }
        if is_kafka {
            for (field, present) in [
                ("format", m.format.is_some()),
                ("compression", m.compression.is_some()),
                ("compaction", m.compaction.is_some()),
                ("flush", m.flush.is_some()),
            ] {
                if present {
                    return Err(LoadError::Validation(format!(
                        "mirror {:?}: `{field}` is only valid for filesystem/s3 destinations",
                        m.name
                    )));
                }
            }
        } else {
            if m.timestamp_mode.is_some() {
                return Err(LoadError::Validation(format!(
                    "mirror {:?}: `timestamp-mode` is only valid for kafka destinations",
                    m.name
                )));
            }
            if m.flush.is_none() {
                return Err(LoadError::Validation(format!(
                    "mirror {:?}: `flush` is required for filesystem/s3 destinations",
                    m.name
                )));
            }
            let format = m.format.unwrap_or_default();
            let keys = m.keys.unwrap_or_default();
            let values = m.values.unwrap_or_default();
            if matches!(format, DestinationFormat::Ndjson) {
                if !matches!(keys.kind, ColumnType::Utf8)
                    || !matches!(values.kind, ColumnType::Utf8)
                {
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
        }
    }
    Ok(())
}
