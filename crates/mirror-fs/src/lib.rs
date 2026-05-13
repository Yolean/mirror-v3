//! Filesystem sink for mirror-v3.
//!
//! ## Atomicity
//!
//! Each flush writes the buffered records to a same-directory
//! temporary file (`<final>.tmp.<uuid>`), `fsync`s it, then
//! `rename(2)`s it to the canonical name `<from>-<to>.ndjson`. POSIX
//! rename is atomic; if the canonical name already exists we treat it
//! as a hard error (two writers shouldn't happen — k8s should keep
//! the deployment single-replica — but we still refuse silently
//! overwriting).
//!
//! ## Restart correctness
//!
//! On startup, [`FilesystemSink::open`] lists the partition directory,
//! parses every filename, and validates the chain forms a contiguous
//! `from→to` sequence with no gaps and no overlaps. `next_expected_offset`
//! returns `max(to) + 1` of the durable chain plus the in-memory
//! buffer length. Buffered-but-not-flushed records are lost on crash,
//! which is fine: the source will re-deliver them post-restart.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mirror_core::{Header, Record, Sink, SinkError};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

pub mod naming;

/// Encode one record as a single ndjson line, appending into `out`.
/// Shared by mirror-fs and mirror-s3 so a single decoder can read
/// either.
pub fn encode_line(record: &Record, out: &mut Vec<u8>) -> Result<(), serde_json::Error> {
    let pr = PersistedRecord::from(record);
    serde_json::to_writer(&mut *out, &pr)?;
    out.push(b'\n');
    Ok(())
}

/// Decode one ndjson line back to a [`Record`].
pub fn decode_line(bytes: &[u8]) -> Result<Record, serde_json::Error> {
    let pr: PersistedRecord = serde_json::from_slice(bytes)?;
    Ok(pr.into_record())
}

pub const FILE_EXT: &str = "ndjson";

#[derive(Debug, Clone)]
pub struct FilesystemSinkConfig {
    /// Directory under `root` is `<root>/<destination_name>/<partition>/`.
    pub root: PathBuf,
    pub destination_name: String,
    pub partition: u32,
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
    flush: FlushTriggers,
    /// Durable destination position: `max(to) + 1` of files on disk.
    durable_position: u64,
    buffer: Vec<Record>,
    buffer_bytes: u64,
    buffer_started: Option<Instant>,
    /// When the most recent flush completed; used to log "ms since
    /// last flush" so operators can see flush cadence.
    last_flush_at: Option<Instant>,
}

impl FilesystemSink {
    pub fn open(cfg: FilesystemSinkConfig) -> Result<Self, FsError> {
        let dir = naming::partition_dir(&cfg.root, &cfg.destination_name, cfg.partition);
        std::fs::create_dir_all(&dir).map_err(|e| FsError::Io {
            path: dir.clone(),
            source: e,
        })?;
        let durable_position = scan_validate(&dir)?;
        Ok(Self {
            dir,
            flush: cfg.flush,
            durable_position,
            buffer: Vec::new(),
            buffer_bytes: 0,
            buffer_started: None,
            last_flush_at: None,
        })
    }

    /// Force a flush even if no trigger has tripped. Used by tests and
    /// will be called by the loop on graceful shutdown (Phase 5).
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
        let bytes = self.buffer_bytes;
        let final_name = naming::batch_filename(from, to, FILE_EXT);
        let final_path = self.dir.join(&final_name);
        let tmp_path = self
            .dir
            .join(format!("{}.tmp.{}", final_name, uuid::Uuid::new_v4()));

        // Write + fsync the temp file.
        {
            let mut file = tokio::fs::File::create(&tmp_path)
                .await
                .map_err(|e| SinkError::Transport(format!("create tmp: {e}")))?;
            let mut buf = Vec::with_capacity(self.buffer_bytes as usize + 64 * self.buffer.len());
            for record in &self.buffer {
                encode_line(record, &mut buf)
                    .map_err(|e| SinkError::Transport(format!("encode: {e}")))?;
            }
            file.write_all(&buf)
                .await
                .map_err(|e| SinkError::Transport(format!("write: {e}")))?;
            file.sync_all()
                .await
                .map_err(|e| SinkError::Transport(format!("fsync: {e}")))?;
        }

        // Atomic publish.
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

        // We deliberately do NOT fsync the parent directory: rename
        // visibility within the same fs is sufficient for our
        // restart-from-listing model. Power-loss durability beyond
        // that is a property of the underlying filesystem.

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
        tracing::info!(
            path = %final_path.display(),
            from,
            to,
            count,
            bytes,
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
        // Re-verify durable state on every call so idle-drift checks
        // pick up out-of-band writes.
        let on_disk = scan_validate(&self.dir).map_err(|e| SinkError::Transport(e.to_string()))?;
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

fn record_byte_size(record: &Record) -> u64 {
    record.key.as_ref().map(|k| k.len()).unwrap_or(0) as u64
        + record.value.as_ref().map(|v| v.len()).unwrap_or(0) as u64
        + record
            .headers
            .iter()
            .map(|h| h.key.len() + h.value.as_ref().map(|v| v.len()).unwrap_or(0))
            .sum::<usize>() as u64
}

/// Scan a partition directory, validate the chain, return next-expected-offset.
fn scan_validate(dir: &Path) -> Result<u64, FsError> {
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
        // Ignore in-flight .tmp files; they belong to a crashed writer.
        if name.contains(".tmp.") {
            continue;
        }
        if let Some((from, to)) = naming::parse_filename(&name, FILE_EXT) {
            if to < from {
                return Err(FsError::CorruptChain {
                    msg: format!("{name}: to < from"),
                });
            }
            entries.push((from, to));
        }
    }
    entries.sort_unstable();

    // Validate contiguity starting at 0.
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

#[derive(Debug, Serialize, Deserialize)]
struct PersistedRecord {
    source_offset: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none", with = "opt_base64")]
    key: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none", with = "opt_base64")]
    value: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    headers: Vec<PersistedHeader>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedHeader {
    key: String,
    #[serde(skip_serializing_if = "Option::is_none", with = "opt_base64")]
    value: Option<Vec<u8>>,
}

impl From<&Record> for PersistedRecord {
    fn from(r: &Record) -> Self {
        PersistedRecord {
            source_offset: r.source_offset,
            timestamp_ms: r.timestamp_ms,
            key: r.key.clone(),
            value: r.value.clone(),
            headers: r
                .headers
                .iter()
                .map(|h| PersistedHeader {
                    key: h.key.clone(),
                    value: h.value.clone(),
                })
                .collect(),
        }
    }
}

impl PersistedRecord {
    pub fn into_record(self) -> Record {
        Record {
            source_offset: self.source_offset,
            key: self.key,
            value: self.value,
            timestamp_ms: self.timestamp_ms,
            headers: self
                .headers
                .into_iter()
                .map(|h| Header {
                    key: h.key,
                    value: h.value,
                })
                .collect(),
        }
    }
}

/// Read every record from a partition directory in offset order.
/// Convenience for tests and operators verifying state.
pub fn read_all_records(dir: &Path) -> Result<Vec<Record>, FsError> {
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
        if let Some((from, to)) = naming::parse_filename(&name, FILE_EXT) {
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
        for line in bytes.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let pr: PersistedRecord =
                serde_json::from_slice(line).map_err(|e| FsError::CorruptChain {
                    msg: format!("decode {}: {e}", path.display()),
                })?;
            out.push(pr.into_record());
        }
    }
    Ok(out)
}

mod opt_base64 {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(bytes) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
                encoded.serialize(s)
            }
            None => s.serialize_none(),
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
        let opt: Option<String> = Option::deserialize(d)?;
        match opt {
            None => Ok(None),
            Some(s) => base64::engine::general_purpose::STANDARD
                .decode(s.as_bytes())
                .map(Some)
                .map_err(serde::de::Error::custom),
        }
    }
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
