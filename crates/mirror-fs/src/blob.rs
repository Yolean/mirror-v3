//! The blob destination shared by the filesystem and S3 sinks.
//!
//! A blob destination is a directory (a local one, or an S3 prefix) of
//! objects named `<from>-<to>.<ext>` (see [`crate::naming`]). The names
//! are the destination's whole resume state: the chain they form gives
//! the next source offset the destination accepts. [`BlobSink`] holds
//! everything that does not depend on where the objects live: the
//! buffer and its flush triggers, the compaction view, the offset gate,
//! chain validation and the flush bookkeeping. A [`BlobStore`] lists,
//! writes and reads objects in one partition directory.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mirror_core::{FlushObserver, FlushTrigger, Record, Sink, SinkError};
use mirror_envelope::{ColumnType, Format, ParquetCompression};

use crate::naming;

/// Unix-seconds clock the sink consults for the daily flush trigger and
/// for the `last_flush_timestamp_seconds` metric. Tests inject one.
pub type UnixClock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// The clock production uses: `SystemTime::now()`.
pub fn system_unix_clock() -> UnixClock {
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
    /// Seconds since UTC midnight (0..86400) at which to flush daily,
    /// in addition to the other triggers. `None` disables.
    pub daily_at_utc_seconds: Option<u32>,
}

/// Log-compaction variant. Today's only variant is `Log` (Kafka-style
/// last-writer-wins): each object is a full snapshot of the latest
/// value per key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionMode {
    Log,
}

/// How a blob destination encodes and when it flushes.
#[derive(Debug, Clone, Copy)]
pub struct BlobSpec {
    pub format: Format,
    pub compression: ParquetCompression,
    pub keys: ColumnType,
    pub values: ColumnType,
    pub compaction: Option<CompactionMode>,
    pub flush: FlushTriggers,
}

#[derive(Debug, thiserror::Error)]
pub enum BlobError {
    #[error("{0}")]
    Store(String),
    #[error("destination chain is corrupt: {0}")]
    CorruptChain(String),
    #[error("object {0} already exists (a second writer, or a write that landed after it was reported failed)")]
    AlreadyExists(String),
}

/// One partition directory of a blob destination.
#[allow(clippy::double_must_use)] // async_trait expansion, see mirror-core Source
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// The names (no directory part) of every object in the directory.
    async fn list(&self) -> Result<Vec<String>, BlobError>;
    /// Write `bytes` as `name`, atomically, and fail with
    /// [`BlobError::AlreadyExists`] if the store can tell that `name`
    /// exists.
    async fn put_new(&self, name: &str, bytes: Vec<u8>) -> Result<(), BlobError>;
    /// Read the object `name`.
    async fn get(&self, name: &str) -> Result<Vec<u8>, BlobError>;
    /// Names sorting after `after` (all names when `None`). S3 serves
    /// this as one ListObjectsV2 with start-after.
    async fn list_after(&self, after: Option<&str>) -> Result<Vec<String>, BlobError>;
    /// Whether the object `name` exists.
    async fn exists(&self, name: &str) -> Result<bool, BlobError>;
    /// Where `name` lives, for logs and errors.
    fn location(&self, name: &str) -> String;
}

/// What a listing says about a destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chain {
    /// The next source offset the destination accepts.
    pub durable: u64,
    /// Compaction mode: the newest snapshot, which holds the view.
    pub latest: Option<String>,
    /// The object with the highest `to`: the end of the chain.
    pub last: Option<String>,
}

/// How often an idle blob mirror checks its destination for objects it
/// did not write. This process is the destination's only writer, so its
/// position is kept in memory; the check is the guard against a second
/// writer or a manual change, and costs one HEAD and one LIST that
/// starts after the last object (the previous check
/// listed the whole prefix every 2 s, 23M LISTs a day for a year-old
/// archive, and a sustained readdir on versitygw).
pub const DRIFT_CHECK_INTERVAL: Duration = Duration::from_secs(60);

/// Validate a listing. Append mode requires a contiguous chain of
/// `<from>-<to>` names from 0; compaction mode allows gaps (snapshots
/// may be removed out of band) and forbids overlaps. An object with
/// another format's extension is an error: no mixed-format directories.
pub fn validate_chain(
    names: &[String],
    format: Format,
    compaction: Option<CompactionMode>,
) -> Result<Chain, BlobError> {
    let expected_ext = format.extension();
    let mut entries: Vec<(u64, u64, &str)> = Vec::new();
    for name in names {
        if let Some(other_ext) = file_extension(name) {
            if other_ext != expected_ext && naming::parse_filename(name, other_ext).is_some() {
                return Err(BlobError::CorruptChain(format!(
                    "{name}: extension '{other_ext}' does not match configured format \
                     '{expected_ext}'"
                )));
            }
        }
        if let Some((from, to)) = naming::parse_filename(name, expected_ext) {
            if to < from {
                return Err(BlobError::CorruptChain(format!("{name}: to < from")));
            }
            entries.push((from, to, name.as_str()));
        }
    }
    match compaction {
        None => {
            entries.sort_unstable();
            let mut expected_next = 0u64;
            for (from, to, _) in &entries {
                if *from != expected_next {
                    return Err(BlobError::CorruptChain(format!(
                        "gap or overlap in chain: expected from={expected_next}, found {from}-{to}"
                    )));
                }
                expected_next = to + 1;
            }
            Ok(Chain {
                durable: expected_next,
                latest: None,
                last: entries.last().map(|(_, _, n)| n.to_string()),
            })
        }
        Some(CompactionMode::Log) => {
            entries.sort_by_key(|(_, to, _)| *to);
            let mut prev_to: Option<u64> = None;
            for (from, to, _) in &entries {
                if let Some(p) = prev_to {
                    if *from <= p {
                        return Err(BlobError::CorruptChain(format!(
                            "overlap in compaction chain: {from}-{to} overlaps prior to={p}"
                        )));
                    }
                }
                prev_to = Some(*to);
            }
            let last = entries.last().map(|(_, _, n)| n.to_string());
            Ok(Chain {
                durable: prev_to.map(|t| t + 1).unwrap_or(0),
                latest: last.clone(),
                last,
            })
        }
    }
}

fn file_extension(name: &str) -> Option<&str> {
    let dot = name.rfind('.')?;
    Some(&name[dot + 1..])
}

/// Decode a compaction snapshot into the view. Keys must be UTF-8: we
/// wrote them as such, so anything else is corruption.
pub fn decode_view(
    location: &str,
    bytes: &[u8],
    format: Format,
) -> Result<BTreeMap<String, Record>, BlobError> {
    let records = mirror_envelope::decode_batch(format, bytes)
        .map_err(|e| BlobError::CorruptChain(format!("decode {location}: {e}")))?;
    let mut view = BTreeMap::new();
    for r in records {
        let key_bytes = r.key.as_ref().ok_or_else(|| {
            BlobError::CorruptChain(format!("{location}: null key in compacted snapshot"))
        })?;
        let key_str = std::str::from_utf8(key_bytes)
            .map_err(|_| {
                BlobError::CorruptChain(format!("{location}: non-UTF-8 key in compacted snapshot"))
            })?
            .to_string();
        view.insert(key_str, r);
    }
    Ok(view)
}

/// First future unix-seconds at which the daily UTC boundary fires.
pub fn schedule_next_daily(target_secs: u32, now_unix: u64) -> u64 {
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

fn report_compaction_keys(n: usize) {
    let (topic, partition) = mirror_core::current_labels();
    metrics::gauge!(
        "mirror_v3_destination_compaction_keys",
        "topic" => topic,
        "partition" => partition,
    )
    .set(n as f64);
}

pub struct BlobSink<S> {
    store: S,
    spec: BlobSpec,
    /// `max(to) + 1` of the objects written.
    durable_position: u64,
    /// Records since the last flush. In append mode the next object's
    /// contents; in compaction mode only the flush-trigger input (the
    /// view is the content).
    buffer: Vec<Record>,
    buffer_bytes: u64,
    buffer_started: Option<Instant>,
    last_flush_at: Option<Instant>,
    /// Compaction mode's latest value per key, sorted by key.
    view: Option<BTreeMap<String, Record>>,
    next_daily_unix: Option<u64>,
    clock: UnixClock,
    flush_observer: Option<Arc<dyn FlushObserver>>,
    /// The last object this process wrote or found at open.
    last_name: Option<String>,
    last_drift_check: Instant,
    drift_check_interval: Duration,
}

impl<S: BlobStore> BlobSink<S> {
    /// Build the sink from a validated listing; `snapshot` is the
    /// content of `chain.latest` in compaction mode.
    pub fn new(
        store: S,
        spec: BlobSpec,
        chain: Chain,
        snapshot: Option<Vec<u8>>,
        clock: UnixClock,
    ) -> Result<Self, BlobError> {
        let view = match spec.compaction {
            None => None,
            Some(CompactionMode::Log) => {
                let view = match (&chain.latest, snapshot) {
                    (Some(name), Some(bytes)) => {
                        decode_view(&store.location(name), &bytes, spec.format)?
                    }
                    (None, _) => BTreeMap::new(),
                    (Some(name), None) => {
                        return Err(BlobError::Store(format!(
                            "snapshot {} was not read",
                            store.location(name)
                        )))
                    }
                };
                report_compaction_keys(view.len());
                Some(view)
            }
        };
        // Naive: the next future occurrence; a mirror down at the
        // boundary misses that day's flush.
        let next_daily_unix = spec
            .flush
            .daily_at_utc_seconds
            .map(|target| schedule_next_daily(target, (clock)()));
        Ok(Self {
            store,
            spec,
            durable_position: chain.durable,
            buffer: Vec::new(),
            buffer_bytes: 0,
            buffer_started: None,
            last_flush_at: None,
            view,
            next_daily_unix,
            clock,
            flush_observer: None,
            last_name: chain.last,
            last_drift_check: Instant::now(),
            drift_check_interval: DRIFT_CHECK_INTERVAL,
        })
    }

    /// Check for foreign objects this often instead of every
    /// [`DRIFT_CHECK_INTERVAL`] (tests).
    #[doc(hidden)]
    pub fn with_drift_check_interval(mut self, interval: Duration) -> Self {
        self.drift_check_interval = interval;
        self
    }

    /// List, validate and (compaction mode) read the latest snapshot.
    pub async fn open_store(store: S, spec: BlobSpec, clock: UnixClock) -> Result<Self, BlobError> {
        let names = store.list().await?;
        let chain = validate_chain(&names, spec.format, spec.compaction)?;
        let snapshot = match &chain.latest {
            Some(name) => Some(store.get(name).await?),
            None => None,
        };
        Self::new(store, spec, chain, snapshot, clock)
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    /// If the daily boundary has passed, flush what is buffered and
    /// move to the next boundary.
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

    /// The lowest source offset the next `write` accepts: the last
    /// buffered offset + 1, or the durable position. The buffer may have
    /// holes (see [`Sink::allows_offset_holes`]).
    fn buffered_head(&self) -> u64 {
        self.buffer
            .last()
            .map(|r| r.source_offset + 1)
            .unwrap_or(self.durable_position)
    }

    fn should_flush(&self) -> Option<FlushTrigger> {
        if self.buffer.is_empty() {
            return None;
        }
        if self.buffer.len() as u64 >= self.spec.flush.max_offsets {
            return Some(FlushTrigger::MaxOffsets);
        }
        if self.buffer_bytes >= self.spec.flush.max_bytes {
            return Some(FlushTrigger::MaxBytes);
        }
        if self
            .buffer_started
            .map(|t| t.elapsed() >= self.spec.flush.max_time)
            .unwrap_or(false)
        {
            return Some(FlushTrigger::MaxTime);
        }
        None
    }

    async fn flush_locked(&mut self, trigger: FlushTrigger) -> Result<(), SinkError> {
        debug_assert!(!self.buffer.is_empty());
        let flush_started = Instant::now();
        // An object covers consumer positions `from..=to`: from the
        // previous object's `to` + 1 through the last record buffered,
        // holes included, so the chain of names stays contiguous.
        let from = self.durable_position;
        let to = self
            .buffer
            .last()
            .map(|r| r.source_offset)
            .expect("buffer non-empty by debug_assert above");
        let count = self.buffer.len();
        let buffered_bytes = self.buffer_bytes;
        let name = naming::batch_filename(from, to, self.spec.format.extension());
        let location = self.store.location(&name);

        // Compaction mode snapshots the view (current per record);
        // append mode encodes the buffer.
        let to_encode: Vec<Record> = match (self.spec.compaction, self.view.as_ref()) {
            (Some(CompactionMode::Log), Some(view)) => view.values().cloned().collect(),
            _ => std::mem::take(&mut self.buffer),
        };
        let bytes = mirror_envelope::encode_batch(
            self.spec.format,
            self.spec.compression,
            self.spec.keys,
            self.spec.values,
            &to_encode,
        )
        .map_err(|e| SinkError::Transport(format!("encode: {e}")))?;
        let encoded_bytes = bytes.len() as u64;

        match self.store.put_new(&name, bytes).await {
            Ok(()) => {}
            Err(BlobError::AlreadyExists(_)) => {
                return Err(SinkError::UnexpectedPosition {
                    expected: from,
                    actual: from,
                });
            }
            Err(e) => return Err(SinkError::Transport(format!("write {location}: {e}"))),
        }

        self.durable_position = to + 1;
        self.last_name = Some(name);
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
            path = %location,
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
        // The observer does something cheap (queue the event).
        if let Some(observer) = self.flush_observer.as_ref() {
            observer.on_flushed(from, to);
        }
        Ok(())
    }

    /// The destination must hold exactly what this process wrote: the
    /// last object is still there, and nothing sorts after it.
    async fn check_drift(&mut self) -> Result<(), SinkError> {
        let transport = |e: BlobError| SinkError::Transport(e.to_string());
        if let Some(last) = self.last_name.as_deref() {
            if !self.store.exists(last).await.map_err(transport)? {
                return Err(SinkError::Transport(format!(
                    "destination drift: {} is gone; it is the end of this mirror's chain",
                    self.store.location(last)
                )));
            }
        }
        let after = self
            .store
            .list_after(self.last_name.as_deref())
            .await
            .map_err(transport)?;
        let ext = self.spec.format.extension();
        if let Some(foreign) = after
            .iter()
            .find(|n| naming::parse_filename(n, ext).is_some())
        {
            return Err(SinkError::Transport(format!(
                "destination drift: {} was not written by this process (a second writer, \
                 or a manual change); this mirror's chain ends before it, at next offset {}",
                self.store.location(foreign),
                self.durable_position
            )));
        }
        self.last_drift_check = Instant::now();
        Ok(())
    }
}

#[async_trait]
impl<S: BlobStore> Sink for BlobSink<S> {
    async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
        self.tick_daily().await?;
        // The loop calls this on every empty poll, so `max-time` holds
        // while the source is idle too, within one poll timeout: a burst
        // at 17:00 on a Friday is not left in memory until Monday.
        if self.should_flush() == Some(FlushTrigger::MaxTime) {
            self.flush_locked(FlushTrigger::MaxTime).await?;
        }
        if self.last_drift_check.elapsed() >= self.drift_check_interval {
            self.check_drift().await?;
        }
        Ok(self.buffered_head())
    }

    async fn write(&mut self, record: Record) -> Result<(), SinkError> {
        self.tick_daily().await?;
        let expected = self.buffered_head();
        if record.source_offset < expected {
            return Err(SinkError::UnexpectedPosition {
                expected,
                actual: record.source_offset,
            });
        }
        // Compaction dedups by key: a non-null UTF-8 key is required.
        if matches!(self.spec.compaction, Some(CompactionMode::Log)) {
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
        matches!(self.spec.compaction, Some(CompactionMode::Log))
    }

    fn allows_offset_holes(&self) -> bool {
        true
    }

    async fn align_to_source_low_watermark(&mut self, low_watermark: u64) -> Result<(), SinkError> {
        // Only called when `allows_compacted_source` (compaction mode),
        // whose chain allows the gap from the previous position.
        if !matches!(self.spec.compaction, Some(CompactionMode::Log)) {
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

    fn set_flush_observer(&mut self, observer: Arc<dyn FlushObserver>) {
        self.flush_observer = Some(observer);
    }

    fn supports_flush_observer(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn append_chain_must_start_at_zero_and_be_contiguous() {
        let n = |a, b| naming::batch_filename(a, b, "ndjson");
        let ok = validate_chain(&[n(0, 4), n(5, 9)], Format::Ndjson, None).unwrap();
        assert_eq!(ok.durable, 10);
        let gap = validate_chain(&[n(0, 4), n(6, 9)], Format::Ndjson, None).unwrap_err();
        assert!(gap.to_string().contains("gap or overlap"), "{gap}");
        let overlap = validate_chain(&[n(0, 4), n(4, 9)], Format::Ndjson, None).unwrap_err();
        assert!(overlap.to_string().contains("gap or overlap"), "{overlap}");
    }

    #[test]
    fn compaction_chain_allows_gaps_and_names_the_latest() {
        let n = |a, b| naming::batch_filename(a, b, "parquet");
        let c = validate_chain(
            &[n(0, 4), n(10, 19)],
            Format::Parquet,
            Some(CompactionMode::Log),
        )
        .unwrap();
        assert_eq!(c.durable, 20);
        assert_eq!(c.latest, Some(n(10, 19)));
    }

    #[test]
    fn other_format_in_the_directory_is_an_error() {
        let err = validate_chain(
            &names(&["00000000000000000000-00000000000000000004.ndjson"]),
            Format::Parquet,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not match"), "{err}");
    }
}
