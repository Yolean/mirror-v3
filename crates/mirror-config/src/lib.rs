//! Configuration model for mirror-v3.
//!
//! Stable surface:
//! - [`Config`] is the root type; see [`load_from_str`] / [`load_from_path`].
//! - [`schema`] returns the JSON Schema for [`Config`], committed to
//!   `schemas/mirror-v3.config.schema.json` in the repo and gated in CI.

use std::path::{Path, PathBuf};

use schemars::{schema_for, JsonSchema, Schema};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Config {
    /// Shared destination configuration. A mirror may override the
    /// destination *name* (the path/prefix segment) but not the type
    /// or transport.
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
    /// Which timestamp lands on the destination record. Defaults to
    /// `source` — preserves the source's `timestamp_ms` exactly. Set
    /// to `destination` to have the destination broker stamp the
    /// record on receipt (CreateTime = producer send-time, or
    /// LogAppendTime if the destination topic is configured that
    /// way).
    #[serde(default)]
    pub timestamp_mode: TimestampMode,
}

/// Per-Kafka-destination timestamp behaviour.
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct FilesystemDestination {
    /// Absolute path to the destination root directory.
    pub root: PathBuf,
    /// Envelope format for written files. Defaults to `parquet`.
    #[serde(default)]
    pub format: DestinationFormat,
    /// Parquet compression. Only meaningful when `format = parquet`.
    /// Defaults to `zstd-1`.
    #[serde(default)]
    pub compression: ParquetCompression,
    /// When `true` (and `format = parquet`), replace the binary
    /// `value` column with a UTF-8 `json` column carrying the
    /// Kafka value bytes verbatim. mirror-v3 does not parse or
    /// validate JSON; it only enforces UTF-8. A non-UTF-8 value is
    /// a hard error. Reject `json = true` if `format = ndjson`
    /// (ndjson already carries the value, base64-encoded).
    #[serde(default)]
    pub json: bool,
    /// Storage representation for the record `key`. Defaults to
    /// `utf8`: keys are validated UTF-8 at encode time and the
    /// Parquet column is `Utf8` (operator-friendly — DuckDB reads it
    /// as `VARCHAR`). Set to `binary` for raw bytes (no validation,
    /// `LargeBinary` column) — required for protobuf-keyed topics
    /// and similar.
    #[serde(default)]
    pub key_type: KeyType,
    pub flush: FlushTriggers,
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
    /// Key prefix prepended to all written object keys.
    #[serde(default)]
    pub prefix: Option<String>,
    /// Envelope format for written objects. Defaults to `parquet`.
    #[serde(default)]
    pub format: DestinationFormat,
    /// Parquet compression. Only meaningful when `format = parquet`.
    /// Defaults to `zstd-1`.
    #[serde(default)]
    pub compression: ParquetCompression,
    /// When `true` (and `format = parquet`), replace the binary
    /// `value` column with a UTF-8 `json` column. See the
    /// equivalent field on `FilesystemDestination` for the
    /// semantics; `json = true` with `format = ndjson` is
    /// rejected at config-load time.
    #[serde(default)]
    pub json: bool,
    /// Key storage representation. See `FilesystemDestination::key_type`.
    #[serde(default)]
    pub key_type: KeyType,
    pub flush: FlushTriggers,
}

/// How the record `key` is represented on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "kebab-case")]
pub enum KeyType {
    /// Keys are validated as UTF-8 at encode (hard error on non-UTF-8,
    /// with the offending source offset). Parquet column type: `Utf8`.
    #[default]
    Utf8,
    /// Keys are opaque bytes — no validation. Parquet column type:
    /// `LargeBinary`. Required for non-UTF-8 keys (e.g. protobuf-keyed
    /// topics).
    Binary,
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
    /// on disk than Parquet.
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

/// Flush triggers for blob-style destinations (Filesystem, S3). The
/// three size/time triggers must be set; any one tripping causes a
/// flush (set to a very large number to effectively disable). The
/// optional `daily` trigger adds a wall-clock-UTC boundary on top.
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Mirror {
    /// Human-readable identifier; appears in logs, metrics and in the
    /// destination naming when `destination_name_override` is unset.
    pub name: String,

    pub source: KafkaSource,

    /// Source Kafka topic name.
    pub topic: String,

    /// Source Kafka partition. Required, no default.
    pub partition: u32,

    /// Override the destination naming for this mirror. For
    /// Filesystem/S3 this replaces the leading path/prefix segment;
    /// for Kafka it overrides the destination topic name.
    #[serde(default)]
    pub destination_name_override: Option<String>,
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
/// Currently: `json = true` requires `format = parquet`; ndjson already
/// carries the value (base64), so the json column would be a no-op
/// silently — better to reject loudly at load time.
fn validate(cfg: &Config) -> Result<(), LoadError> {
    match &cfg.destination {
        Destination::Filesystem(fs) => {
            if fs.json && matches!(fs.format, DestinationFormat::Ndjson) {
                return Err(LoadError::Validation(
                    "filesystem.json = true requires filesystem.format = parquet".into(),
                ));
            }
        }
        Destination::S3(s3) => {
            if s3.json && matches!(s3.format, DestinationFormat::Ndjson) {
                return Err(LoadError::Validation(
                    "s3.json = true requires s3.format = parquet".into(),
                ));
            }
        }
        Destination::Kafka(_) => {}
    }
    Ok(())
}
