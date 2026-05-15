//! Filesystem sink for mirror-v3.
//!
//! ## Atomicity
//!
//! Each flush writes the buffered records to a same-directory
//! temporary file (`<final>.tmp.<uuid>`), `fsync`s it, then
//! `rename(2)`s it to the canonical name `<from>-<to>.<ext>` where
//! `<ext>` matches the configured envelope format (parquet / ndjson).
//! POSIX rename is atomic; a pre-existing canonical name is a hard
//! error.
//!
//! ## Restart correctness
//!
//! On startup, [`FilesystemSink::open`] lists the partition
//! directory, parses every filename matching the configured
//! extension, and validates the chain forms a contiguous `from→to`
//! sequence with no gaps and no overlaps. Files with a non-matching
//! extension are an error (no mixed-format dirs).

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mirror_core::{Record, Sink, SinkError};
use mirror_envelope::{Format, ParquetCompression};
use tokio::io::AsyncWriteExt;

pub mod naming;

#[derive(Debug, Clone)]
pub struct FilesystemSinkConfig {
    /// Directory under `root` is `<root>/<destination_name>/<partition>/`.
    pub root: PathBuf,
    pub destination_name: String,
    pub partition: u32,
    pub format: Format,
    pub compression: ParquetCompression,
    pub flush: FlushTriggers,
}

#[derive(Debug, Clone, Copy)]
pub struct FlushTriggers {
    pub max_time: Duration,
    pub max_bytes: u64,
    pub max_offsets: u64,
}

pub struct FilesystemSink {
    dir: PathBuf,
    format: Format,
    compression: ParquetCompression,
    flush: FlushTriggers,
    /// Durable destination position: `max(to) + 1` of files on disk.
    durable_position: u64,
    buffer: Vec<Record>,
    buffer_bytes: u64,
    buffer_started: Option<Instant>,
    last_flush_at: Option<Instant>,
}

impl FilesystemSink {
    pub fn open(cfg: FilesystemSinkConfig) -> Result<Self, FsError> {
        let dir = naming::partition_dir(&cfg.root, &cfg.destination_name, cfg.partition);
        std::fs::create_dir_all(&dir).map_err(|e| FsError::Io {
            path: dir.clone(),
            source: e,
        })?;
        let durable_position = scan_validate(&dir, cfg.format)?;
        Ok(Self {
            dir,
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
        let by_count = self.buffer.len() as u64 >= self.flush.max_offsets;
        let by_bytes = self.buffer_bytes >= self.flush.max_bytes;
        let by_time = self
            .buffer_started
            .map(|t| t.elapsed() >= self.flush.max_time)
            .unwrap_or(false);
        by_count || by_bytes || by_time
    }

    async fn flush_locked(&mut self) -> Result<(), SinkError> {
        debug_assert!(!self.buffer.is_empty());
        let flush_started = Instant::now();
        let from = self.durable_position;
        let to = self.durable_position + self.buffer.len() as u64 - 1;
        let count = self.buffer.len();
        let ext = self.format.extension();
        let final_name = naming::batch_filename(from, to, ext);
        let final_path = self.dir.join(&final_name);
        let tmp_path = self
            .dir
            .join(format!("{}.tmp.{}", final_name, uuid::Uuid::new_v4()));

        // Encode the whole batch into bytes (NDJSON or Parquet) and
        // write+fsync the temp file.
        let bytes = mirror_envelope::encode_batch(self.format, self.compression, &self.buffer)
            .map_err(|e| SinkError::Transport(format!("encode: {e}")))?;
        let encoded_len = bytes.len() as u64;
        {
            let mut file = tokio::fs::File::create(&tmp_path)
                .await
                .map_err(|e| SinkError::Transport(format!("create tmp: {e}")))?;
            file.write_all(&bytes)
                .await
                .map_err(|e| SinkError::Transport(format!("write: {e}")))?;
            file.sync_all()
                .await
                .map_err(|e| SinkError::Transport(format!("fsync: {e}")))?;
        }

        match tokio::fs::rename(&tmp_path, &final_path).await {
            Ok(()) => {}
            Err(e) => {
                let _ = tokio::fs::remove_file(&tmp_path).await;
                return Err(SinkError::Transport(format!(
                    "rename {} -> {}: {e}",
                    tmp_path.display(),
                    final_path.display()
                )));
            }
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
        .set(unix_now_seconds() as f64);
        metrics::counter!(
            "mirror_v3_destination_bytes_total",
            "topic" => topic.clone(),
            "partition" => partition.clone(),
        )
        .increment(encoded_len);
        metrics::counter!(
            "mirror_v3_destination_flushes_total",
            "topic" => topic,
            "partition" => partition,
        )
        .increment(1);

        tracing::info!(
            path = %final_path.display(),
            from,
            to,
            count,
            bytes = encoded_len,
            elapsed_ms,
            interval_ms,
            "flushed batch"
        );
        Ok(())
    }
}

#[async_trait]
impl Sink for FilesystemSink {
    async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
        let on_disk = scan_validate(&self.dir, self.format)
            .map_err(|e| SinkError::Transport(e.to_string()))?;
        if on_disk != self.durable_position {
            return Err(SinkError::UnexpectedPosition {
                expected: self.durable_position,
                actual: on_disk,
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
        let bytes = record_byte_size(&record);
        self.buffer.push(record);
        self.buffer_bytes += bytes;
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

fn unix_now_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn record_byte_size(record: &Record) -> u64 {
    record.key.as_ref().map(|k| k.len()).unwrap_or(0) as u64
        + record.value.as_ref().map(|v| v.len()).unwrap_or(0) as u64
        + record
            .headers
            .iter()
            .map(|h| h.key.len() + h.value.as_ref().map(|v| v.len()).unwrap_or(0))
            .sum::<usize>() as u64
}

/// Scan a partition directory, validate the chain (from=0 contiguous,
/// no overlaps), and return the next-expected source offset
/// (`max(to) + 1` of the chain).
///
/// Files with a non-matching extension are reported as `CorruptChain`
/// so a misconfigured mirror (writing parquet into a directory that
/// already contains ndjson) is caught immediately.
fn scan_validate(dir: &Path, format: Format) -> Result<u64, FsError> {
    let expected_ext = format.extension();
    let mut entries: Vec<(u64, u64)> = Vec::new();
    let read = match std::fs::read_dir(dir) {
        Ok(r) => r,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(0),
        Err(e) => {
            return Err(FsError::Io {
                path: dir.to_path_buf(),
                source: e,
            })
        }
    };
    for entry in read {
        let entry = entry.map_err(|e| FsError::Io {
            path: dir.to_path_buf(),
            source: e,
        })?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.contains(".tmp.") {
            continue;
        }
        // Files of the wrong extension are an error — mixed-format
        // dirs are forbidden.
        if let Some(other_ext) = file_extension(&name) {
            if other_ext != expected_ext && naming::parse_filename(&name, other_ext).is_some() {
                return Err(FsError::CorruptChain {
                    msg: format!(
                        "{name}: extension '{other_ext}' does not match configured format \
                         '{expected_ext}'"
                    ),
                });
            }
        }
        if let Some((from, to)) = naming::parse_filename(&name, expected_ext) {
            if to < from {
                return Err(FsError::CorruptChain {
                    msg: format!("{name}: to < from"),
                });
            }
            entries.push((from, to));
        }
    }
    entries.sort_unstable();

    let mut expected_next: u64 = 0;
    for (from, to) in &entries {
        if *from != expected_next {
            return Err(FsError::CorruptChain {
                msg: format!(
                    "gap or overlap in chain: expected from={expected_next}, found {from}-{to}"
                ),
            });
        }
        expected_next = to + 1;
    }
    Ok(expected_next)
}

fn file_extension(name: &str) -> Option<&str> {
    let dot = name.rfind('.')?;
    Some(&name[dot + 1..])
}

/// Read every record from a partition directory in offset order.
/// Convenience for tests and operators verifying state.
pub fn read_all_records(dir: &Path, format: Format) -> Result<Vec<Record>, FsError> {
    let expected_ext = format.extension();
    let mut entries: Vec<(u64, u64, PathBuf)> = Vec::new();
    let read = match std::fs::read_dir(dir) {
        Ok(r) => r,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(FsError::Io {
                path: dir.to_path_buf(),
                source: e,
            })
        }
    };
    for entry in read {
        let entry = entry.map_err(|e| FsError::Io {
            path: dir.to_path_buf(),
            source: e,
        })?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.contains(".tmp.") {
            continue;
        }
        if let Some((from, to)) = naming::parse_filename(&name, expected_ext) {
            entries.push((from, to, entry.path()));
        }
    }
    entries.sort_unstable_by_key(|(from, _, _)| *from);

    let mut out = Vec::new();
    for (_, _, path) in entries {
        let bytes = std::fs::read(&path).map_err(|e| FsError::Io {
            path: path.clone(),
            source: e,
        })?;
        let decoded =
            mirror_envelope::decode_batch(format, &bytes).map_err(|e| FsError::CorruptChain {
                msg: format!("decode {}: {e}", path.display()),
            })?;
        out.extend(decoded);
    }
    Ok(out)
}

#[derive(Debug, thiserror::Error)]
pub enum FsError {
    #[error("filesystem io {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("destination chain is corrupt: {msg}")]
    CorruptChain { msg: String },
}
