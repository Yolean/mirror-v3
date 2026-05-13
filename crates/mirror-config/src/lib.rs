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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct FilesystemDestination {
    /// Absolute path to the destination root directory.
    pub root: PathBuf,
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
    pub flush: FlushTriggers,
}

/// Flush triggers for blob-style destinations (Filesystem, S3). All
/// three must be set; any one tripping causes a flush. Set a value to
/// a very large number to effectively disable that trigger.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct FlushTriggers {
    /// Maximum time, in milliseconds, between flushes.
    pub max_time_ms: u64,
    /// Maximum buffered bytes before a flush.
    pub max_bytes: u64,
    /// Maximum buffered source offsets before a flush.
    pub max_offsets: u64,
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
}

pub fn load_from_str(yaml: &str) -> Result<Config, LoadError> {
    Ok(serde_yaml::from_str(yaml)?)
}

pub fn load_from_path(path: &Path) -> Result<Config, LoadError> {
    let bytes = std::fs::read(path).map_err(|source| LoadError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(serde_yaml::from_slice(&bytes)?)
}
