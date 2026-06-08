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

use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mirror_core::{FlushTrigger, Record, Sink, SinkError};
use mirror_envelope::{ColumnType, Format, ParquetCompression};
use tokio::io::AsyncWriteExt;

pub mod naming;

/// Unix-seconds clock the sink consults for the daily flush trigger
/// and for the `last_flush_timestamp_seconds` metric. The default
/// reads `SystemTime::now()`; tests inject a closure over an
/// `AtomicU64` via [`FilesystemSink::open_with_clock`].
pub type UnixClock = Arc<dyn Fn() -> u64 + Send + Sync>;

fn system_unix_clock() -> UnixClock {
    Arc::new(unix_now_seconds)
}

#[derive(Debug, Clone)]
pub struct FilesystemSinkConfig {
    /// Directory under `root` is `<root>/<destination_name>/<partition>/`.
    pub root: PathBuf,
    pub destination_name: String,
    pub partition: u32,
    pub format: Format,
    pub compression: ParquetCompression,
    /// Storage representation for the record `key`. Caller is
    /// responsible for pairing `Bytes` with `compaction = None`.
    pub keys: ColumnType,
    /// Storage representation for the record `value`.
    pub values: ColumnType,
    /// Optional log-compaction mode. When `Some(CompactionMode::Log)`,
    /// each emitted file is a full materialized snapshot of the
    /// latest value per key. Caller must combine this with
    /// `Format::Parquet` and `keys` ∈ {`Utf8`, `Json`}.
    pub compaction: Option<CompactionMode>,
    /// Optional shared HTTP-cache state. When `Some`, every record
    /// the sink receives is applied to the cache view from the
    /// consume loop (per-record, decoupled from flush cadence). The
    /// mirror is also bootstrapped against this state at `open()` -
    /// in compaction:log mode, the latest snapshot's keys are
    /// pre-loaded; in append mode, the entire on-disk chain is
    /// replayed (linear in total record count).
    pub cache: Option<CacheBinding>,
    pub flush: FlushTriggers,
}

/// Re-export of the canonical cache binding from `mirror-core`. When
/// set on a `FilesystemSinkConfig`, the sink uses it on `open` to
/// replay the durable destination state into the shared cache so
/// HTTP readers see what's already on disk. The per-record `write()`
/// path no longer touches the cache; that's the tee level's job
/// ([`mirror_core::TeeSink`]) so a single record never gets applied
/// twice when the same mirror feeds multiple destinations.
pub use mirror_core::CacheBinding;

/// Log-compaction variant. Reserved for future strategies; currently
/// only `Log` (Kafka-style, key-based, last-writer-wins) is defined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionMode {
    Log,
}

#[derive(Debug, Clone, Copy)]
pub struct FlushTriggers {
    pub max_time: Duration,
    pub max_bytes: u64,
    pub max_offsets: u64,
    /// Seconds since UTC midnight (0..86400). `None` disables the
    /// daily wall-clock flush boundary. See `mirror_config::DailyFlush`
    /// for the YAML surface.
    pub daily_at_utc_seconds: Option<u32>,
}

pub struct FilesystemSink {
    dir: PathBuf,
    format: Format,
    compression: ParquetCompression,
    keys: ColumnType,
    values: ColumnType,
    compaction: Option<CompactionMode>,
    flush: FlushTriggers,
    /// Optional callback fired after every successful flush. Wired
    /// up by [`Sink::set_flush_observer`]; default is `None` (no
    /// observer). Stored as `Arc` so the same observer can be
    /// shared across multiple sinks under a tee.
    flush_observer: Option<std::sync::Arc<dyn mirror_core::FlushObserver>>,
    /// Durable destination position: `max(to) + 1` of files on disk.
    durable_position: u64,
    /// Buffered records arrived since the last flush. In append mode
    /// this is the next file's contents; in compaction mode it's
    /// only used to drive flush triggers (the local `view` is the
    /// authoritative content, and is updated per-record).
    buffer: Vec<Record>,
    buffer_bytes: u64,
    buffer_started: Option<Instant>,
    last_flush_at: Option<Instant>,
    /// Compaction-mode in-memory materialized view, sorted by key.
    /// `None` when `compaction` is `None`; even with cache enabled,
    /// the cache state is held in `CacheBinding`, not here.
    view: Option<BTreeMap<String, Record>>,
    /// Absolute unix-seconds for the next daily-flush boundary, or
    /// `None` when the trigger is disabled.
    next_daily_unix: Option<u64>,
    clock: UnixClock,
}

impl FilesystemSink {
    pub fn open(cfg: FilesystemSinkConfig) -> Result<Self, FsError> {
        Self::open_with_clock(cfg, system_unix_clock())
    }

    /// Same as [`open`] but with a caller-supplied clock for tests.
    /// The clock returns "now" in unix seconds. Not for production.
    #[doc(hidden)]
    pub fn open_with_clock(cfg: FilesystemSinkConfig, clock: UnixClock) -> Result<Self, FsError> {
        let dir = naming::partition_dir(&cfg.root, &cfg.destination_name, cfg.partition);
        std::fs::create_dir_all(&dir).map_err(|e| FsError::Io {
            path: dir.clone(),
            source: e,
        })?;
        // Two scan-validate flavours: append mode requires a strictly
        // contiguous chain; compaction mode allows gaps (snapshots
        // can be GC'd out-of-band) but still forbids overlaps.
        let (durable_position, view) = match cfg.compaction {
            None => (scan_validate(&dir, cfg.format)?, None),
            Some(CompactionMode::Log) => {
                let (pos, latest) = scan_validate_compacted(&dir, cfg.format)?;
                let view = match latest {
                    None => BTreeMap::new(),
                    Some(path) => load_view(&path, cfg.format)?,
                };
                report_compaction_keys(view.len());
                (pos, Some(view))
            }
        };
        // Cache bootstrap: replay durable state into the shared
        // CacheState so HTTP readers see what's already on disk.
        // - compaction:log → fold the loaded snapshot view.
        // - append → replay the whole chain (cost is linear in the
        //   total record count; documented for large topics).
        //
        // `apply_record` enforces per-(topic, partition) offset
        // monotonicity and silently skips records with
        // `source_offset <= last_seen`. The compacted snapshot is a
        // `BTreeMap<key, Record>` whose `.values()` iterate in
        // lexicographic key order, NOT offset order — without
        // sorting, the first replayed record's offset would clamp
        // every subsequent record whose key happens to land
        // alphabetically later but at a lower offset, leaving the
        // in-memory view with a key-order-dependent subset of the
        // snapshot. Sort by `source_offset` ascending before
        // dispatch so every snapshot record passes the guard.
        if let Some(binding) = cfg.cache.as_ref() {
            match &view {
                Some(v) => {
                    let mut snapshot_records: Vec<&Record> = v.values().collect();
                    snapshot_records.sort_by_key(|r| r.source_offset);
                    for r in snapshot_records {
                        binding.state.apply_record(&binding.mirror_name, r);
                    }
                }
                None => {
                    let records = read_all_records(&dir, cfg.format)?;
                    for r in records.iter() {
                        binding.state.apply_record(&binding.mirror_name, r);
                    }
                }
            }
        }
        // NOTE: naive; computes the next future occurrence and
        // accepts that a mirror down at the boundary silently misses
        // it for that day. The richer version (planned alongside
        // debounce) inspects the destination chain (mtime / last
        // record timestamp) and uses last_flush_at to decide whether
        // the boundary was already honored. The shape of this
        // computation; `(target_secs, now) -> next_unix`; does not
        // change.
        let next_daily_unix = cfg
            .flush
            .daily_at_utc_seconds
            .map(|target| schedule_next_daily(target, (clock)()));
        Ok(Self {
            dir,
            format: cfg.format,
            compression: cfg.compression,
            keys: cfg.keys,
            values: cfg.values,
            compaction: cfg.compaction,
            flush: cfg.flush,
            flush_observer: None,
            durable_position,
            buffer: Vec::new(),
            buffer_bytes: 0,
            buffer_started: None,
            last_flush_at: None,
            view,
            next_daily_unix,
            clock,
        })
    }

    /// If the daily-flush boundary has been crossed, flush any
    /// buffered records and advance to the next boundary. Called
    /// from the top of `write` and `next_expected_offset` so both
    /// the record-arrival and idle-poll paths honor the schedule.
    async fn tick_daily(&mut self) -> Result<(), SinkError> {
        let Some(next) = self.next_daily_unix else {
            return Ok(());
        };
        let now = (self.clock)();
        if now < next {
            return Ok(());
        }
        // Boundary crossed. Flush only if there's data; an empty-
        // buffer slot is silently skipped (no zero-record file). The
        // boundary is *always* advanced so we don't fire repeatedly
        // until tomorrow.
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
    /// Under append mode this is `durable_position + buffer.len()` (the
    /// chain is contiguous). Under `compaction:log` the buffer may
    /// have gaps so the head is `last_buffered.source_offset + 1`
    /// (or `durable_position` when the buffer is empty).
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
            // source-offset sequence (records dropped by upstream
            // compaction are skipped on delivery, not buffered as
            // tombstones), so `durable + len - 1` would underflow the
            // actual high-water. Use the last buffered record's
            // source_offset directly.
            Some(CompactionMode::Log) => self
                .buffer
                .last()
                .map(|r| r.source_offset)
                .expect("buffer non-empty by debug_assert above"),
            _ => self.durable_position + self.buffer.len() as u64 - 1,
        };
        let count = self.buffer.len();
        let ext = self.format.extension();
        let final_name = naming::batch_filename(from, to, ext);
        let final_path = self.dir.join(&final_name);
        let tmp_path = self
            .dir
            .join(format!("{}.tmp.{}", final_name, uuid::Uuid::new_v4()));

        // In compaction mode the local `view` is already current
        // (write() applied each record per-record), so we just snapshot
        // it. BTreeMap iteration is in key-sorted order, which is what
        // we want for deterministic file content and good Parquet
        // dictionary-encoding. In append mode we encode the buffer.
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
        .set((self.clock)() as f64);
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
            trigger = trigger.as_str(),
            "flushed batch"
        );
        // Notify the destination-flush observer if one is wired.
        // The observer is expected to do something cheap (queue the
        // event for an async drainer); inlining HTTP here would
        // serialise per-flush write latency behind webhook RTT.
        if let Some(observer) = self.flush_observer.as_ref() {
            observer.on_flushed(from, to);
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for FilesystemSink {
    async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
        self.tick_daily().await?;
        let on_disk = match self.compaction {
            None => scan_validate(&self.dir, self.format)
                .map_err(|e| SinkError::Transport(e.to_string()))?,
            Some(CompactionMode::Log) => {
                let (pos, _) = scan_validate_compacted(&self.dir, self.format)
                    .map_err(|e| SinkError::Transport(e.to_string()))?;
                pos
            }
        };
        // Drift = disk advanced PAST our in-memory view (out-of-band
        // write). The reverse (in-memory ahead of disk) is normal
        // immediately after `align_to_source_low_watermark` and stays
        // until the first flush, so we tolerate it.
        if on_disk > self.durable_position {
            return Err(SinkError::UnexpectedPosition {
                expected: self.durable_position,
                actual: on_disk,
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
        // Append mode also rejects forward gaps (the destination
        // chain forbids holes). Under compaction:log forward gaps
        // are legitimate; the upstream may have compacted the
        // intermediate offsets out and the snapshot only stores
        // latest-per-key.
        if !matches!(self.compaction, Some(CompactionMode::Log)) && record.source_offset != expected
        {
            return Err(SinkError::UnexpectedPosition {
                expected,
                actual: record.source_offset,
            });
        }
        // Compaction needs a non-null UTF-8 key to dedup by. The
        // http-access cache also wants string keys, but its key check
        // now lives at the tee level alongside its `apply_record` call
        // (see `mirror_core::TeeSink::write`) so the sink only has to
        // reason about its own requirements.
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
        // Apply to the local compaction view per-record (was per-flush
        // before; moved here so view content tracks the consume loop
        // exactly, independent of the flush cadence).
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
        let bytes = record_byte_size(&record);
        self.buffer.push(record);
        self.buffer_bytes += bytes;
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
        // The loop only calls this when `allows_compacted_source`
        // returned true (i.e. we're in compaction:log mode) and the
        // sink's prior `next_expected_offset` was strictly less than
        // `low_watermark`. Refuse silently surprising calls.
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
        // Advance the sink's view of "where the next snapshot starts"
        // to the broker's low watermark. The compaction-mode chain
        // explicitly allows gaps (see `scan_validate_compacted`), so
        // the next flushed snapshot's filename will be `low_watermark-to.ext`
        // and the on-disk gap from the prior `durable_position` is fine.
        self.durable_position = low_watermark;
        Ok(())
    }

    fn set_flush_observer(&mut self, observer: std::sync::Arc<dyn mirror_core::FlushObserver>) {
        self.flush_observer = Some(observer);
    }
}

fn unix_now_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Public re-export so `mirror_s3` shares the same scheduling math
/// without re-implementing it (and inheriting any future changes).
#[doc(hidden)]
pub fn schedule_next_daily_public(target_secs: u32, now_unix: u64) -> u64 {
    schedule_next_daily(target_secs, now_unix)
}

/// First future unix-seconds at which the daily wall-clock-UTC
/// boundary should fire, given a target seconds-since-midnight and
/// the current unix-seconds. Pure math, no I/O; kept as a free
/// function so the smart-startup variant (which inspects the
/// destination chain) can replace just this body.
pub(crate) fn schedule_next_daily(target_secs: u32, now_unix: u64) -> u64 {
    let midnight = (now_unix / 86_400) * 86_400;
    let today_target = midnight + target_secs as u64;
    if now_unix < today_target {
        today_target
    } else {
        today_target + 86_400
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
        // Files of the wrong extension are an error; mixed-format
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

/// Scan a partition directory under compaction-mode rules: gaps in
/// the chain are allowed (snapshots can be GC'd) but overlaps and
/// duplicate `to` values are still rejected. Returns the durable
/// position (`max(to) + 1`) and the path to the latest snapshot, if
/// any exists.
fn scan_validate_compacted(dir: &Path, format: Format) -> Result<(u64, Option<PathBuf>), FsError> {
    let expected_ext = format.extension();
    let mut entries: Vec<(u64, u64, PathBuf)> = Vec::new();
    let read = match std::fs::read_dir(dir) {
        Ok(r) => r,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok((0, None)),
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
            entries.push((from, to, entry.path()));
        }
    }
    entries.sort_by_key(|(_, to, _)| *to);
    let mut prev_to: Option<u64> = None;
    for (from, to, _) in &entries {
        if let Some(p) = prev_to {
            if *from <= p {
                return Err(FsError::CorruptChain {
                    msg: format!("overlap in compaction chain: {from}-{to} overlaps prior to={p}"),
                });
            }
        }
        prev_to = Some(*to);
    }
    let durable = prev_to.map(|t| t + 1).unwrap_or(0);
    let latest_path = entries.into_iter().next_back().map(|(_, _, p)| p);
    Ok((durable, latest_path))
}

/// Decode a snapshot file into the in-memory materialized view. Keys
/// are required to be valid UTF-8 (we wrote them as Utf8; a non-UTF-8
/// key here would be data corruption).
fn load_view(path: &Path, format: Format) -> Result<BTreeMap<String, Record>, FsError> {
    let bytes = std::fs::read(path).map_err(|e| FsError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    let records =
        mirror_envelope::decode_batch(format, &bytes).map_err(|e| FsError::CorruptChain {
            msg: format!("decode {}: {e}", path.display()),
        })?;
    let mut view = BTreeMap::new();
    for r in records {
        let key_bytes = r.key.as_ref().ok_or_else(|| FsError::CorruptChain {
            msg: format!("{}: null key in compacted snapshot", path.display()),
        })?;
        let key_str = std::str::from_utf8(key_bytes)
            .map_err(|_| FsError::CorruptChain {
                msg: format!("{}: non-UTF-8 key in compacted snapshot", path.display()),
            })?
            .to_string();
        view.insert(key_str, r);
    }
    Ok(view)
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

/// Read the latest compacted snapshot's contents, in key-sorted order.
/// Convenience for tests and operators verifying state in compaction mode.
pub fn read_latest_snapshot(dir: &Path, format: Format) -> Result<Vec<Record>, FsError> {
    let (_, latest) = scan_validate_compacted(dir, format)?;
    match latest {
        None => Ok(Vec::new()),
        Some(path) => {
            let view = load_view(&path, format)?;
            Ok(view.into_values().collect())
        }
    }
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
