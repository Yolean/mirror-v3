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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use mirror_core::{FlushTrigger, Record, Sink, SinkError};
use mirror_envelope::{ColumnType, Format, ParquetCompression};
use mirror_fs::naming;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};

/// Same shape as `mirror_fs::UnixClock`. Tests inject; production
/// uses [`system_unix_clock`].
pub type UnixClock = Arc<dyn Fn() -> u64 + Send + Sync>;

fn system_unix_clock() -> UnixClock {
    Arc::new(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    })
}

#[derive(Debug, Clone, Copy)]
pub struct FlushTriggers {
    pub max_time: Duration,
    pub max_bytes: u64,
    pub max_offsets: u64,
    /// Seconds since UTC midnight (0..86400). `None` disables.
    pub daily_at_utc_seconds: Option<u32>,
}

pub struct S3SinkConfig {
    pub store: Arc<dyn ObjectStore>,
    /// Path prefix inside the store: `<prefix>/<destination_name>/<partition>/`.
    pub prefix: Option<Path>,
    pub destination_name: String,
    pub partition: u32,
    pub format: Format,
    pub compression: ParquetCompression,
    /// Storage representation for the record `key`. Caller is
    /// responsible for pairing `Bytes` with `compaction = None`.
    pub keys: ColumnType,
    /// Storage representation for the record `value`.
    pub values: ColumnType,
    /// Optional log-compaction mode. See `mirror_fs::CompactionMode`.
    /// Caller must combine `Some(Log)` with `Format::Parquet` and
    /// `keys` ∈ {`Utf8`, `Json`}.
    pub compaction: Option<CompactionMode>,
    /// Optional shared HTTP-cache state. See
    /// `mirror_fs::FilesystemSinkConfig::cache` for semantics; the
    /// S3 sink behaves identically.
    pub cache: Option<CacheBinding>,
    pub flush: FlushTriggers,
}

/// Log-compaction variant. Re-exported alias of the type used by
/// `mirror-fs`; today's only variant is `Log` (Kafka-style LWW).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionMode {
    Log,
}

/// Re-export of the canonical cache binding from `mirror-core`. See
/// `mirror_fs::CacheBinding` for the symmetric FS-side comment.
pub use mirror_core::CacheBinding;

pub struct S3Sink {
    store: Arc<dyn ObjectStore>,
    partition_prefix: Path,
    format: Format,
    compression: ParquetCompression,
    keys: ColumnType,
    values: ColumnType,
    compaction: Option<CompactionMode>,
    flush: FlushTriggers,
    durable_position: u64,
    buffer: Vec<Record>,
    buffer_bytes: u64,
    buffer_started: Option<Instant>,
    last_flush_at: Option<Instant>,
    view: Option<BTreeMap<String, Record>>,
    next_daily_unix: Option<u64>,
    clock: UnixClock,
    /// See [`mirror_fs::FilesystemSink::flush_observer`]; same
    /// contract: stored Arc, default `None`, fired after every
    /// successful PUT.
    flush_observer: Option<Arc<dyn mirror_core::FlushObserver>>,
}

impl S3Sink {
    pub async fn open(cfg: S3SinkConfig) -> Result<Self, S3Error> {
        Self::open_with_clock(cfg, system_unix_clock()).await
    }

    #[doc(hidden)]
    pub async fn open_with_clock(cfg: S3SinkConfig, clock: UnixClock) -> Result<Self, S3Error> {
        let partition_prefix =
            build_prefix(cfg.prefix.as_ref(), &cfg.destination_name, cfg.partition);
        let (durable_position, view) = match cfg.compaction {
            None => (
                scan_validate(cfg.store.as_ref(), &partition_prefix, cfg.format).await?,
                None,
            ),
            Some(CompactionMode::Log) => {
                let (pos, latest) =
                    scan_validate_compacted(cfg.store.as_ref(), &partition_prefix, cfg.format)
                        .await?;
                let view = match latest {
                    None => BTreeMap::new(),
                    Some(path) => load_view(cfg.store.as_ref(), &path, cfg.format).await?,
                };
                report_compaction_keys(view.len());
                (pos, Some(view))
            }
        };
        // Cache bootstrap: same shape as mirror-fs; replay durable
        // state into the shared CacheState. Compaction = read latest
        // snapshot; append + cache = scan + replay every object.
        if let Some(binding) = cfg.cache.as_ref() {
            match &view {
                Some(v) => {
                    for r in v.values() {
                        binding.state.apply_record(&binding.mirror_name, r);
                    }
                }
                None => {
                    let records =
                        read_all_records_via(cfg.store.as_ref(), &partition_prefix, cfg.format)
                            .await?;
                    for r in records.iter() {
                        binding.state.apply_record(&binding.mirror_name, r);
                    }
                }
            }
        }
        // See mirror-fs::FilesystemSink::open_with_clock for the
        // naive-vs-smart story.
        let next_daily_unix = cfg
            .flush
            .daily_at_utc_seconds
            .map(|target| mirror_fs::schedule_next_daily_public(target, (clock)()));
        Ok(Self {
            store: cfg.store,
            partition_prefix,
            format: cfg.format,
            compression: cfg.compression,
            keys: cfg.keys,
            values: cfg.values,
            compaction: cfg.compaction,
            flush: cfg.flush,
            durable_position,
            buffer: Vec::new(),
            buffer_bytes: 0,
            buffer_started: None,
            last_flush_at: None,
            view,
            next_daily_unix,
            clock,
            flush_observer: None,
        })
    }

    async fn tick_daily(&mut self) -> Result<(), SinkError> {
        let Some(next) = self.next_daily_unix else {
            return Ok(());
        };
        let now = (self.clock)();
        if now < next {
            return Ok(());
        }
        if !self.buffer.is_empty() {
            self.flush_locked(FlushTrigger::Daily).await?;
        }
        let mut t = next;
        let now = (self.clock)();
        while now >= t {
            t += 86_400;
        }
        self.next_daily_unix = Some(t);
        Ok(())
    }

    pub async fn flush_now(&mut self) -> Result<(), SinkError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.flush_locked(FlushTrigger::Explicit).await
    }

    /// Lowest source-offset the sink will accept on the next `write`.
    /// Append mode: `durable_position + buffer.len()` (contiguous chain).
    /// Compaction:log: `last_buffered.source_offset + 1` (or
    /// `durable_position` when the buffer is empty), so the buffer may
    /// carry gaps in its source-offset sequence; see mirror-fs.
    fn buffered_head(&self) -> u64 {
        match self.compaction {
            Some(CompactionMode::Log) => self
                .buffer
                .last()
                .map(|r| r.source_offset + 1)
                .unwrap_or(self.durable_position),
            _ => self.durable_position + self.buffer.len() as u64,
        }
    }

    fn should_flush(&self) -> Option<FlushTrigger> {
        if self.buffer.is_empty() {
            return None;
        }
        if self.buffer.len() as u64 >= self.flush.max_offsets {
            return Some(FlushTrigger::MaxOffsets);
        }
        if self.buffer_bytes >= self.flush.max_bytes {
            return Some(FlushTrigger::MaxBytes);
        }
        if self
            .buffer_started
            .map(|t| t.elapsed() >= self.flush.max_time)
            .unwrap_or(false)
        {
            return Some(FlushTrigger::MaxTime);
        }
        None
    }

    async fn flush_locked(&mut self, trigger: FlushTrigger) -> Result<(), SinkError> {
        debug_assert!(!self.buffer.is_empty());
        let flush_started = Instant::now();
        let from = self.durable_position;
        let to = match self.compaction {
            // Under compaction:log the buffer may contain gaps in its
            // source-offset sequence; the snapshot covers everything
            // from `durable_position` through the highest-seen offset.
            // See mirror-fs flush_locked for the same reasoning.
            Some(CompactionMode::Log) => self
                .buffer
                .last()
                .map(|r| r.source_offset)
                .expect("buffer non-empty by debug_assert above"),
            _ => self.durable_position + self.buffer.len() as u64 - 1,
        };
        let count = self.buffer.len();
        let buffered_bytes = self.buffer_bytes;
        let name = naming::batch_filename(from, to, self.format.extension());
        let path = child_of(&self.partition_prefix, &name);

        // Per mirror-fs: in compaction mode the local view is already
        // current (write() applies per-record). Append mode encodes
        // the buffer.
        let to_encode: Vec<Record> = match (self.compaction, self.view.as_ref()) {
            (Some(CompactionMode::Log), Some(view)) => view.values().cloned().collect(),
            _ => std::mem::take(&mut self.buffer),
        };
        let bytes = mirror_envelope::encode_batch(
            self.format,
            self.compression,
            self.keys,
            self.values,
            &to_encode,
        )
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
        .set((self.clock)() as f64);
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
            trigger = trigger.as_str(),
            "flushed batch"
        );
        // See mirror-fs for the destination-flush observer contract.
        if let Some(observer) = self.flush_observer.as_ref() {
            observer.on_flushed(from, to);
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for S3Sink {
    async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
        self.tick_daily().await?;
        let on_remote = match self.compaction {
            None => scan_validate(self.store.as_ref(), &self.partition_prefix, self.format)
                .await
                .map_err(|e| SinkError::Transport(e.to_string()))?,
            Some(CompactionMode::Log) => {
                let (pos, _) = scan_validate_compacted(
                    self.store.as_ref(),
                    &self.partition_prefix,
                    self.format,
                )
                .await
                .map_err(|e| SinkError::Transport(e.to_string()))?;
                pos
            }
        };
        // Drift = remote advanced PAST our in-memory view (out-of-band
        // write). The reverse is normal immediately after
        // `align_to_source_low_watermark` (durable advanced but the
        // first snapshot blob hasn't been PUT yet); see mirror-fs.
        if on_remote > self.durable_position {
            return Err(SinkError::UnexpectedPosition {
                expected: self.durable_position,
                actual: on_remote,
            });
        }
        Ok(self.buffered_head())
    }

    async fn write(&mut self, record: Record) -> Result<(), SinkError> {
        self.tick_daily().await?;
        let expected = self.buffered_head();
        // Backwards is always a hard error.
        if record.source_offset < expected {
            return Err(SinkError::UnexpectedPosition {
                expected,
                actual: record.source_offset,
            });
        }
        // Append mode rejects forward gaps too; compaction:log accepts
        // them (the snapshot only stores latest-per-key, so the
        // upstream-compacted intermediate offsets are legitimately
        // absent). See mirror-fs::write for the same logic.
        if !matches!(self.compaction, Some(CompactionMode::Log)) && record.source_offset != expected
        {
            return Err(SinkError::UnexpectedPosition {
                expected,
                actual: record.source_offset,
            });
        }
        // Compaction needs a non-null UTF-8 key to dedup by. The
        // http-access cache check now lives at the tee level (see
        // `mirror_core::TeeSink::write`).
        if matches!(self.compaction, Some(CompactionMode::Log)) {
            match &record.key {
                None => {
                    return Err(SinkError::Transport(format!(
                        "this mirror requires a non-null key; \
                         record at source offset {} has key=null",
                        record.source_offset
                    )));
                }
                Some(k) => {
                    if std::str::from_utf8(k).is_err() {
                        return Err(SinkError::Transport(format!(
                            "this mirror requires a UTF-8 key; \
                             record at source offset {} has non-UTF-8 key",
                            record.source_offset
                        )));
                    }
                }
            }
        }
        if let Some(view) = self.view.as_mut() {
            let key_bytes = record.key.as_ref().expect("checked non-null above");
            let key_str = std::str::from_utf8(key_bytes)
                .expect("checked UTF-8 above")
                .to_string();
            if record.value.is_none() {
                view.remove(&key_str);
            } else {
                view.insert(key_str, record.clone());
            }
            report_compaction_keys(view.len());
        }
        self.buffer_bytes += record_byte_size(&record);
        self.buffer.push(record);
        if self.buffer_started.is_none() {
            self.buffer_started = Some(Instant::now());
        }
        if let Some(trigger) = self.should_flush() {
            self.flush_locked(trigger).await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        self.flush_now().await
    }

    fn allows_compacted_source(&self) -> bool {
        matches!(self.compaction, Some(CompactionMode::Log))
    }

    async fn align_to_source_low_watermark(&mut self, low_watermark: u64) -> Result<(), SinkError> {
        // Mirrors `mirror_fs::FilesystemSink::align_to_source_low_watermark`.
        // The S3 compaction-mode chain (scan_validate_compacted) allows
        // gaps, so advancing `durable_position` past the prior value
        // is correct for blob naming.
        if !matches!(self.compaction, Some(CompactionMode::Log)) {
            return Err(SinkError::Transport(
                "align_to_source_low_watermark called on non-compaction sink".into(),
            ));
        }
        if !self.buffer.is_empty() || low_watermark < self.durable_position {
            return Err(SinkError::Transport(format!(
                "align_to_source_low_watermark called in inconsistent state: buffer={} durable={} low_watermark={}",
                self.buffer.len(),
                self.durable_position,
                low_watermark,
            )));
        }
        self.durable_position = low_watermark;
        Ok(())
    }

    fn set_flush_observer(&mut self, observer: Arc<dyn mirror_core::FlushObserver>) {
        self.flush_observer = Some(observer);
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

/// Compaction-mode scan-validate: gaps allowed (out-of-band GC),
/// overlaps and duplicate `to` rejected. Returns durable_position
/// (`max(to) + 1`) and the path to the latest snapshot if any exists.
async fn scan_validate_compacted(
    store: &dyn ObjectStore,
    prefix: &Path,
    format: Format,
) -> Result<(u64, Option<Path>), S3Error> {
    let expected_ext = format.extension();
    let mut entries: Vec<(u64, u64, Path)> = Vec::new();
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
            entries.push((from, to, meta.location.clone()));
        }
    }
    entries.sort_by_key(|(_, to, _)| *to);
    let mut prev_to: Option<u64> = None;
    for (from, to, _) in &entries {
        if let Some(p) = prev_to {
            if *from <= p {
                return Err(S3Error::CorruptChain(format!(
                    "overlap in compaction chain: {from}-{to} overlaps prior to={p}"
                )));
            }
        }
        prev_to = Some(*to);
    }
    let durable = prev_to.map(|t| t + 1).unwrap_or(0);
    let latest = entries.into_iter().next_back().map(|(_, _, p)| p);
    Ok((durable, latest))
}

async fn load_view(
    store: &dyn ObjectStore,
    path: &Path,
    format: Format,
) -> Result<BTreeMap<String, Record>, S3Error> {
    let got = store
        .get(path)
        .await
        .map_err(|e| S3Error::Store(format!("get {path}: {e}")))?;
    let bytes = got
        .bytes()
        .await
        .map_err(|e| S3Error::Store(format!("read {path}: {e}")))?;
    let records = mirror_envelope::decode_batch(format, &bytes)
        .map_err(|e| S3Error::CorruptChain(format!("decode {path}: {e}")))?;
    let mut view = BTreeMap::new();
    for r in records {
        let key_bytes = r.key.as_ref().ok_or_else(|| {
            S3Error::CorruptChain(format!("{path}: null key in compacted snapshot"))
        })?;
        let key_str = std::str::from_utf8(key_bytes)
            .map_err(|_| {
                S3Error::CorruptChain(format!("{path}: non-UTF-8 key in compacted snapshot"))
            })?
            .to_string();
        view.insert(key_str, r);
    }
    Ok(view)
}

/// Read every object under the partition prefix as records, in
/// offset order. Used to bootstrap the shared cache view on
/// append + cache mode (where the chain is the full event history,
/// not a single snapshot).
async fn read_all_records_via(
    store: &dyn ObjectStore,
    prefix: &Path,
    format: Format,
) -> Result<Vec<Record>, S3Error> {
    let expected_ext = format.extension();
    let mut entries: Vec<(u64, u64, Path)> = Vec::new();
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
        if let Some((from, to)) = naming::parse_filename(&name, expected_ext) {
            entries.push((from, to, meta.location.clone()));
        }
    }
    entries.sort_by_key(|(from, _, _)| *from);
    let mut out = Vec::new();
    for (_, _, path) in entries {
        let got = store
            .get(&path)
            .await
            .map_err(|e| S3Error::Store(format!("get {path}: {e}")))?;
        let bytes = got
            .bytes()
            .await
            .map_err(|e| S3Error::Store(format!("read {path}: {e}")))?;
        let records = mirror_envelope::decode_batch(format, &bytes)
            .map_err(|e| S3Error::CorruptChain(format!("decode {path}: {e}")))?;
        out.extend(records);
    }
    Ok(out)
}

fn report_compaction_keys(n: usize) {
    let (topic, partition) = mirror_core::current_labels();
    metrics::gauge!(
        "mirror_v3_destination_compaction_keys",
        "topic" => topic,
        "partition" => partition,
    )
    .set(n as f64);
}

#[derive(Debug, thiserror::Error)]
pub enum S3Error {
    #[error("object store: {0}")]
    Store(String),
    #[error("destination chain is corrupt: {0}")]
    CorruptChain(String),
}
