//! S3-compatible blob sink.
//!
//! ## Atomicity (two-layer)
//!
//! 1. **Preferred**: `PutMode::Create` (`If-None-Match: *`). On AWS S3
//!    this fails the second writer with 412 Precondition Failed.
//! 2. **Universal fallback**: single-writer-per-(topic,partition) by
//!    deployment + scan-validate on startup. If the underlying
//!    object store silently ignores `PutMode::Create`, a duplicate
//!    `from` at startup is detected and the sink refuses to open.
//!
//! ## Restart correctness
//!
//! On open, list every object under the prefix, parse `<from>-<to>.ndjson`
//! names, sort, and require a contiguous chain from 0. Anything else
//! is a hard error.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use mirror_core::{Record, Sink, SinkError};
use mirror_envelope::{Format, ParquetCompression};
use mirror_fs::naming;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};

#[derive(Debug, Clone, Copy)]
pub struct FlushTriggers {
    pub max_time: Duration,
    pub max_bytes: u64,
    pub max_offsets: u64,
}

pub struct S3SinkConfig {
    pub store: Arc<dyn ObjectStore>,
    /// Path prefix inside the store: `<prefix>/<destination_name>/<partition>/`.
    pub prefix: Option<Path>,
    pub destination_name: String,
    pub partition: u32,
    pub format: Format,
    pub compression: ParquetCompression,
    pub flush: FlushTriggers,
}

pub struct S3Sink {
    store: Arc<dyn ObjectStore>,
    partition_prefix: Path,
    format: Format,
    compression: ParquetCompression,
    flush: FlushTriggers,
    durable_position: u64,
    buffer: Vec<Record>,
    buffer_bytes: u64,
    buffer_started: Option<Instant>,
    last_flush_at: Option<Instant>,
}

impl S3Sink {
    pub async fn open(cfg: S3SinkConfig) -> Result<Self, S3Error> {
        let partition_prefix =
            build_prefix(cfg.prefix.as_ref(), &cfg.destination_name, cfg.partition);
        let durable_position =
            scan_validate(cfg.store.as_ref(), &partition_prefix, cfg.format).await?;
        Ok(Self {
            store: cfg.store,
            partition_prefix,
            format: cfg.format,
            compression: cfg.compression,
            flush: cfg.flush,
            durable_position,
            buffer: Vec::new(),
            buffer_bytes: 0,
            buffer_started: None,
            last_flush_at: None,
        })
    }

    pub async fn flush_now(&mut self) -> Result<(), SinkError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.flush_locked().await
    }

    fn should_flush(&self) -> bool {
        if self.buffer.is_empty() {
            return false;
        }
        self.buffer.len() as u64 >= self.flush.max_offsets
            || self.buffer_bytes >= self.flush.max_bytes
            || self
                .buffer_started
                .map(|t| t.elapsed() >= self.flush.max_time)
                .unwrap_or(false)
    }

    async fn flush_locked(&mut self) -> Result<(), SinkError> {
        debug_assert!(!self.buffer.is_empty());
        let flush_started = Instant::now();
        let from = self.durable_position;
        let to = self.durable_position + self.buffer.len() as u64 - 1;
        let count = self.buffer.len();
        let buffered_bytes = self.buffer_bytes;
        let name = naming::batch_filename(from, to, self.format.extension());
        let path = child_of(&self.partition_prefix, &name);

        let bytes = mirror_envelope::encode_batch(self.format, self.compression, &self.buffer)
            .map_err(|e| SinkError::Transport(format!("encode: {e}")))?;
        let encoded_bytes = bytes.len() as u64;

        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&path, PutPayload::from(Bytes::from(bytes)), opts)
            .await
        {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. }) => {
                return Err(SinkError::UnexpectedPosition {
                    expected: from,
                    actual: from,
                });
            }
            Err(e) => return Err(SinkError::Transport(format!("put_opts {path}: {e}"))),
        }

        self.durable_position = to + 1;
        self.buffer.clear();
        self.buffer_bytes = 0;
        self.buffer_started = None;
        let elapsed_ms = flush_started.elapsed().as_millis() as u64;
        let interval_ms = self
            .last_flush_at
            .map(|t| t.elapsed().as_millis() as u64)
            .unwrap_or(0);
        self.last_flush_at = Some(Instant::now());

        let (topic, partition) = mirror_core::current_labels();
        let unix_now_seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        metrics::gauge!(
            "mirror_v3_destination_offset_verified",
            "topic" => topic.clone(),
            "partition" => partition.clone(),
        )
        .set(self.durable_position as f64);
        metrics::gauge!(
            "mirror_v3_destination_last_flush_timestamp_seconds",
            "topic" => topic.clone(),
            "partition" => partition.clone(),
        )
        .set(unix_now_seconds as f64);
        metrics::counter!(
            "mirror_v3_destination_bytes_total",
            "topic" => topic.clone(),
            "partition" => partition.clone(),
        )
        .increment(encoded_bytes);
        metrics::counter!(
            "mirror_v3_destination_flushes_total",
            "topic" => topic,
            "partition" => partition,
        )
        .increment(1);

        tracing::info!(
            %path,
            from,
            to,
            count,
            buffered_bytes,
            encoded_bytes,
            elapsed_ms,
            interval_ms,
            "flushed batch"
        );
        Ok(())
    }
}

#[async_trait]
impl Sink for S3Sink {
    async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
        let on_remote = scan_validate(self.store.as_ref(), &self.partition_prefix, self.format)
            .await
            .map_err(|e| SinkError::Transport(e.to_string()))?;
        if on_remote != self.durable_position {
            return Err(SinkError::UnexpectedPosition {
                expected: self.durable_position,
                actual: on_remote,
            });
        }
        Ok(self.durable_position + self.buffer.len() as u64)
    }

    async fn write(&mut self, record: Record) -> Result<(), SinkError> {
        let expected = self.durable_position + self.buffer.len() as u64;
        if record.source_offset != expected {
            return Err(SinkError::UnexpectedPosition {
                expected,
                actual: record.source_offset,
            });
        }
        self.buffer_bytes += record_byte_size(&record);
        self.buffer.push(record);
        if self.buffer_started.is_none() {
            self.buffer_started = Some(Instant::now());
        }
        if self.should_flush() {
            self.flush_locked().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        self.flush_now().await
    }
}

fn record_byte_size(record: &Record) -> u64 {
    record.key.as_ref().map(|k| k.len()).unwrap_or(0) as u64
        + record.value.as_ref().map(|v| v.len()).unwrap_or(0) as u64
}

fn build_prefix(root: Option<&Path>, destination_name: &str, partition: u32) -> Path {
    let mut parts: Vec<String> = Vec::new();
    if let Some(p) = root {
        for part in p.parts() {
            parts.push(part.as_ref().to_string());
        }
    }
    parts.push(destination_name.to_string());
    parts.push(partition.to_string());
    Path::from_iter(parts)
}

fn child_of(prefix: &Path, name: &str) -> Path {
    let mut parts: Vec<String> = prefix.parts().map(|p| p.as_ref().to_string()).collect();
    parts.push(name.to_string());
    Path::from_iter(parts)
}

fn file_extension(name: &str) -> Option<&str> {
    let dot = name.rfind('.')?;
    Some(&name[dot + 1..])
}

async fn scan_validate(
    store: &dyn ObjectStore,
    prefix: &Path,
    format: Format,
) -> Result<u64, S3Error> {
    let expected_ext = format.extension();
    let mut entries: Vec<(u64, u64)> = Vec::new();
    let mut stream = store.list(Some(prefix));
    while let Some(meta) = stream.next().await {
        let meta = meta.map_err(|e| S3Error::Store(e.to_string()))?;
        let name = meta
            .location
            .filename()
            .map(|s| s.to_string())
            .unwrap_or_default();
        if name.is_empty() || name.contains(".tmp.") {
            continue;
        }
        if let Some(other_ext) = file_extension(&name) {
            if other_ext != expected_ext && naming::parse_filename(&name, other_ext).is_some() {
                return Err(S3Error::CorruptChain(format!(
                    "{name}: extension '{other_ext}' does not match configured format \
                     '{expected_ext}'"
                )));
            }
        }
        if let Some((from, to)) = naming::parse_filename(&name, expected_ext) {
            if to < from {
                return Err(S3Error::CorruptChain(format!("{name}: to < from")));
            }
            entries.push((from, to));
        }
    }
    entries.sort_unstable();
    let mut expected_next = 0u64;
    for (from, to) in &entries {
        if *from != expected_next {
            return Err(S3Error::CorruptChain(format!(
                "gap or overlap: expected from={expected_next}, found {from}-{to}"
            )));
        }
        expected_next = to + 1;
    }
    Ok(expected_next)
}

#[derive(Debug, thiserror::Error)]
pub enum S3Error {
    #[error("object store: {0}")]
    Store(String),
    #[error("destination chain is corrupt: {0}")]
    CorruptChain(String),
}
