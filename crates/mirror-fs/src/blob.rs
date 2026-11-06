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

use crate::floor::{self, Floor, FloorMarker};
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
#[derive(Debug, Clone)]
pub struct BlobSpec {
    pub format: Format,
    pub compression: ParquetCompression,
    pub keys: ColumnType,
    pub values: ColumnType,
    pub compaction: Option<CompactionMode>,
    pub flush: FlushTriggers,
    /// `None`: blobs are written in clear. `Some`: Parquet blobs
    /// encrypted with the key `key_id`, named `<from>-<to>.k-<id>.parquet`.
    pub encryption: Option<BlobEncryption>,
}

/// The key new blobs are encrypted with, and every key the directory
/// holds (a blob is read with the key its name carries).
#[derive(Debug, Clone)]
pub struct BlobEncryption {
    pub key_id: String,
    pub keyring: Arc<mirror_envelope::Keyring>,
}

#[derive(Debug, thiserror::Error)]
pub enum BlobError {
    #[error("{0}")]
    Store(String),
    #[error("destination chain is corrupt: {0}")]
    CorruptChain(String),
    #[error("object {0} already exists (a second writer, or a write that landed after it was reported failed)")]
    AlreadyExists(String),
    /// A retention lock that the store does not support, or did not
    /// apply to an object written with it. Trying again cannot fix it.
    #[error("{0}")]
    Lock(String),
}

/// How far ahead of the clock a record timestamp may be before it is
/// taken for a wrong clock (a producer's) rather than skew: trim blocks
/// at such an object, and a retention lock counts from the write time
/// instead.
pub const FUTURE_SKEW_MS: u64 = 3_600_000;

/// What a new object is, for the metadata and retention lock a store
/// gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NewObject {
    /// An append-mode object, written at `written_at` (unix seconds). Its
    /// record times go in its metadata, and a store with a retention
    /// lock locks it from its record time.
    Records { times: RecordTimes, written_at: u64 },
    /// A floor marker, written at `written_at`: no records, and a store
    /// with a retention lock locks it from then.
    Marker { written_at: u64 },
    /// A compaction snapshot: no metadata, no lock.
    Snapshot,
}

/// One version of an object, on a store that keeps versions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectVersion {
    /// The object's name in the directory.
    pub name: String,
    pub version_id: String,
    /// A delete marker rather than data: what a DELETE without a version
    /// id leaves on a versioned bucket.
    pub delete_marker: bool,
}

/// One partition directory of a blob destination.
#[allow(clippy::double_must_use)] // async_trait expansion, see mirror-core Source
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// The names (no directory part) of every object in the directory.
    async fn list(&self) -> Result<Vec<String>, BlobError>;
    /// Write `bytes` as `name`, atomically, and fail with
    /// [`BlobError::AlreadyExists`] if the store can tell that `name`
    /// exists. Where the store keeps metadata (S3: `x-amz-meta-<key>`),
    /// the record times of [`NewObject::Records`] go in it; a filesystem
    /// has none. Returns the retain-until time (unix seconds) of the
    /// retention lock the object was written with, if any: only a store
    /// configured with one locks, and then in the same request.
    async fn put_new(
        &self,
        name: &str,
        bytes: Vec<u8>,
        object: &NewObject,
    ) -> Result<Option<u64>, BlobError>;
    /// Read the object `name`.
    async fn get(&self, name: &str) -> Result<Vec<u8>, BlobError>;
    /// Names sorting after `after` (all names when `None`). S3 serves
    /// this as one ListObjectsV2 with start-after.
    async fn list_after(&self, after: Option<&str>) -> Result<Vec<String>, BlobError>;
    /// Where `name` lives, for logs and errors.
    fn location(&self, name: &str) -> String;
    /// The timestamps of the records in `name`: from its metadata where
    /// the store keeps metadata (a HEAD, so that `trim` needs neither
    /// the objects' content nor their keys), from its content otherwise.
    /// `Ok(None)` for an object written without them.
    async fn record_times(&self, name: &str) -> Result<Option<RecordTimes>, BlobError> {
        Err(BlobError::Store(format!(
            "{}: this store does not tell record times",
            self.location(name)
        )))
    }
    /// Delete `name`; one that does not exist is not an error. On a
    /// store that keeps versions this leaves the data (see
    /// [`BlobStore::versions`]).
    async fn delete(&self, name: &str) -> Result<(), BlobError> {
        Err(BlobError::Store(format!(
            "delete {}: this store does not delete",
            self.location(name)
        )))
    }
    /// Every version and delete marker in the directory, where the store
    /// keeps versions (S3 on a bucket with versioning enabled or
    /// suspended); `Ok(None)` where it does not, and a DELETE removes the
    /// data.
    async fn versions(&self) -> Result<Option<Vec<ObjectVersion>>, BlobError> {
        Ok(None)
    }
    /// Delete one version (or delete marker) of `name` for good.
    async fn delete_version(&self, name: &str, version_id: &str) -> Result<(), BlobError> {
        Err(BlobError::Store(format!(
            "delete {} version {version_id}: this store keeps no versions",
            self.location(name)
        )))
    }
}

/// The record timestamps an object holds. Each append-mode object
/// carries them as metadata ([`RecordTimes::to_metadata`]), written
/// with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RecordTimes {
    /// Lowest record timestamp, unix milliseconds; `None` when no record
    /// has one.
    pub min_ms: Option<u64>,
    pub max_ms: Option<u64>,
    /// Records without a timestamp (none, or a negative one).
    pub without: u64,
}

/// Metadata keys of [`RecordTimes`].
pub const META_MIN_TIMESTAMP_MS: &str = "mirror-v3-min-timestamp-ms";
pub const META_MAX_TIMESTAMP_MS: &str = "mirror-v3-max-timestamp-ms";
pub const META_RECORDS_WITHOUT_TIMESTAMP: &str = "mirror-v3-records-without-timestamp";

impl RecordTimes {
    pub fn of(records: &[Record]) -> Self {
        let mut times = Self::default();
        for r in records {
            match r.timestamp_ms.and_then(|t| u64::try_from(t).ok()) {
                Some(t) => {
                    times.min_ms = Some(times.min_ms.map_or(t, |m| m.min(t)));
                    times.max_ms = Some(times.max_ms.map_or(t, |m| m.max(t)));
                }
                None => times.without += 1,
            }
        }
        times
    }

    pub fn to_metadata(&self) -> Vec<(&'static str, String)> {
        let mut meta = Vec::with_capacity(3);
        if let Some(ms) = self.min_ms {
            meta.push((META_MIN_TIMESTAMP_MS, ms.to_string()));
        }
        if let Some(ms) = self.max_ms {
            meta.push((META_MAX_TIMESTAMP_MS, ms.to_string()));
        }
        meta.push((META_RECORDS_WITHOUT_TIMESTAMP, self.without.to_string()));
        meta
    }

    /// `Ok(None)` when the metadata has none of the keys (an object
    /// written before they were recorded); an error when it has some and
    /// they do not parse, or the count of records without a timestamp
    /// is missing.
    pub fn from_metadata(get: impl Fn(&str) -> Option<String>) -> Result<Option<Self>, String> {
        let field = |key: &str| -> Result<Option<u64>, String> {
            get(key)
                .map(|v| {
                    v.trim()
                        .parse::<u64>()
                        .map_err(|_| format!("metadata {key}={v:?} is not a number"))
                })
                .transpose()
        };
        let min_ms = field(META_MIN_TIMESTAMP_MS)?;
        let max_ms = field(META_MAX_TIMESTAMP_MS)?;
        let without = field(META_RECORDS_WITHOUT_TIMESTAMP)?;
        match (min_ms, max_ms, without) {
            (None, None, None) => Ok(None),
            (min_ms, max_ms, Some(without)) if min_ms.is_some() == max_ms.is_some() => {
                Ok(Some(Self {
                    min_ms,
                    max_ms,
                    without,
                }))
            }
            _ => Err(format!(
                "incomplete record-time metadata: {META_MIN_TIMESTAMP_MS}={min_ms:?} \
                 {META_MAX_TIMESTAMP_MS}={max_ms:?} {META_RECORDS_WITHOUT_TIMESTAMP}={without:?}"
            )),
        }
    }
}

/// What a listing says about a destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chain {
    /// The next source offset the destination accepts.
    pub durable: u64,
    /// Compaction mode: the newest snapshot, which holds the view.
    pub latest: Option<String>,
    /// The last two objects of the chain (highest `to` last). Append
    /// mode: objects at or above the floor only.
    pub tail: Vec<String>,
    /// Append mode: the floor its markers set (see [`crate::floor`]).
    pub floor: Option<Floor>,
    /// Append mode: objects that end below the floor and are still in
    /// the directory. The chain ignores them; `trim` deletes them.
    pub below_floor: Vec<String>,
    /// Append mode: every floor marker in the directory.
    pub markers: Vec<FloorMarker>,
}

/// How often an idle blob mirror checks its destination for objects it
/// did not write. This process is the destination's only writer, so its
/// position is kept in memory; the check is the guard against a second
/// writer or a manual change, and costs one LIST that starts after the
/// second-to-last object (the previous check listed
/// the whole prefix every 2 s, 23M LISTs a day for a year-old archive,
/// and a sustained readdir on versitygw). LIST only: a least-privilege
/// reading identity may list keys and read no object, so a HEAD is
/// not available.
pub const DRIFT_CHECK_INTERVAL: Duration = Duration::from_secs(60);

/// Validate a listing. Append mode requires a contiguous chain of
/// `<from>-<to>` names from the floor (0 without floor markers): objects
/// that end below the floor are ignored, present or not, and the first
/// object at or above it must cover it. A floor above the end of the
/// chain is an error, except in a directory that holds no objects at all
/// (every one expired), which resumes at the floor. Compaction mode
/// allows gaps (snapshots may be removed out of band), forbids overlaps,
/// and has no floor. Every other object must be one of this mirror's
/// blobs: an object with another format's extension, or a name that does
/// not parse, is an error (skipping it would hide a misconfiguration, or
/// make the resume position too low if it was a blob under a name this
/// version does not read).
pub fn validate_chain(
    names: &[String],
    format: Format,
    compaction: Option<CompactionMode>,
) -> Result<Chain, BlobError> {
    let expected_ext = format.extension();
    let (floor, markers) = floor::floor_of(names).map_err(BlobError::CorruptChain)?;
    if compaction.is_some() {
        if let Some(m) = markers.first() {
            return Err(BlobError::CorruptChain(format!(
                "{} is a floor marker, and a compaction-mode destination has no floor \
                 (its chain allows gaps)",
                m.filename()
            )));
        }
    }
    let mut entries: Vec<(u64, u64, &str)> = Vec::new();
    for name in names {
        if floor::is_marker(name) {
            continue;
        }
        if let Some(other_ext) = file_extension(name) {
            if other_ext != expected_ext && naming::parse_blob_name(name, other_ext).is_some() {
                return Err(BlobError::CorruptChain(format!(
                    "{name}: extension '{other_ext}' does not match configured format \
                     '{expected_ext}'"
                )));
            }
        }
        let Some(naming::BlobName { from, to, .. }) = naming::parse_blob_name(name, expected_ext)
        else {
            return Err(BlobError::CorruptChain(format!(
                "{name} is not a blob name this mirror writes \
                 (<from>-<to>.{expected_ext}, or <from>-<to>.k-<key id>.{expected_ext})"
            )));
        };
        if to < from {
            return Err(BlobError::CorruptChain(format!("{name}: to < from")));
        }
        entries.push((from, to, name.as_str()));
    }
    match compaction {
        None => {
            let floor_offset = floor.map_or(0, |f| f.offset);
            entries.sort_unstable();
            let (below, above): (Vec<_>, Vec<_>) = entries
                .into_iter()
                .partition(|(_, to, _)| *to < floor_offset);
            let mut expected_next = floor_offset;
            for (i, (from, to, _)) in above.iter().enumerate() {
                // The first object may start below the floor (a floor set
                // inside it); the rest continue the chain exactly.
                let fits = if i == 0 {
                    *from <= expected_next
                } else {
                    *from == expected_next
                };
                if !fits {
                    let hint = match (i, floor) {
                        (0, None) => format!(
                            "; if the objects before it were removed on purpose, set the \
                             chain's floor: mirror-v3 trim --floor {from}"
                        ),
                        (0, Some(_)) => format!(
                            " (the chain's floor is {floor_offset}: offsets from there on must be \
                             in the chain)"
                        ),
                        _ => String::new(),
                    };
                    return Err(BlobError::CorruptChain(format!(
                        "gap or overlap in chain: expected from={expected_next}, found \
                         {from}-{to}{hint}"
                    )));
                }
                expected_next = to + 1;
            }
            if above.is_empty() {
                if let Some(end) = below.iter().map(|(_, to, _)| to + 1).max() {
                    if end < floor_offset {
                        return Err(BlobError::CorruptChain(format!(
                            "the chain's floor {floor_offset} is above the end of the chain, \
                             {end}: offsets {end}-{} are in no object",
                            floor_offset - 1
                        )));
                    }
                }
            }
            Ok(Chain {
                durable: expected_next,
                latest: None,
                tail: tail_of(&above),
                floor,
                below_floor: below.iter().map(|(_, _, n)| n.to_string()).collect(),
                markers,
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
            Ok(Chain {
                durable: prev_to.map(|t| t + 1).unwrap_or(0),
                latest: entries.last().map(|(_, _, n)| n.to_string()),
                tail: tail_of(&entries),
                floor: None,
                below_floor: Vec::new(),
                markers: Vec::new(),
            })
        }
    }
}

fn tail_of(entries: &[(u64, u64, &str)]) -> Vec<String> {
    let skip = entries.len().saturating_sub(2);
    entries[skip..]
        .iter()
        .map(|(_, _, n)| n.to_string())
        .collect()
}

fn file_extension(name: &str) -> Option<&str> {
    let dot = name.rfind('.')?;
    Some(&name[dot + 1..])
}

/// Decode a blob, with the key its name carries if it has one;
/// `keyring` is `None` for a destination with `encryption: none`.
pub fn decode_blob(
    name: &str,
    location: &str,
    bytes: &[u8],
    format: Format,
    keyring: Option<&mirror_envelope::Keyring>,
) -> Result<Vec<Record>, BlobError> {
    let key_id = naming::parse_blob_name(name, format.extension()).and_then(|b| b.key_id);
    let decoded = match key_id {
        None => mirror_envelope::decode_batch(format, bytes),
        Some(id) => {
            let ring = keyring.ok_or_else(|| {
                BlobError::Store(format!(
                    "{location} is encrypted with Parquet key {id}, and this destination has \
                     `encryption: none`"
                ))
            })?;
            let key = ring
                .get(&id)
                .map_err(|e| BlobError::Store(format!("{location}: {e}")))?;
            mirror_envelope::parquet::decode_batch_encrypted(bytes, key)
        }
    };
    decoded.map_err(|e| BlobError::CorruptChain(format!("decode {location}: {e}")))
}

/// Decode a compaction snapshot into the view. Keys must be UTF-8: we
/// wrote them as such, so anything else is corruption.
pub fn decode_view(
    name: &str,
    location: &str,
    bytes: &[u8],
    format: Format,
    encryption: Option<&BlobEncryption>,
) -> Result<BTreeMap<String, Record>, BlobError> {
    let records = decode_blob(
        name,
        location,
        bytes,
        format,
        encryption.map(|e| e.keyring.as_ref()),
    )?;
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

/// The floor gauges. A destination that never had a floor has none; one
/// whose floor marker went away reports floor 0.
fn report_floor(floor: Option<&Floor>) {
    let (topic, partition, mirror) = mirror_core::current_labels();
    metrics::gauge!(
        "mirror_v3_destination_floor_offset",
        "topic" => topic.clone(),
        "partition" => partition.clone(),
        "mirror" => mirror.clone(),
    )
    .set(floor.map_or(0, |f| f.offset) as f64);
    metrics::gauge!(
        "mirror_v3_destination_floor_moved_timestamp_seconds",
        "topic" => topic.clone(),
        "partition" => partition.clone(),
        "mirror" => mirror.clone(),
    )
    .set(floor.map_or(0, |f| f.moved_at) as f64);
    // NaN when unknown: no objects above the floor when its marker was
    // written, or a marker written by hand.
    metrics::gauge!(
        "mirror_v3_destination_floor_oldest_record_timestamp_seconds",
        "topic" => topic,
        "partition" => partition,
        "mirror" => mirror,
    )
    .set(
        floor
            .and_then(|f| f.oldest_ms)
            .map_or(f64::NAN, |ms| ms as f64 / 1000.0),
    );
}

fn describe_floor(floor: Option<&Floor>) -> String {
    match floor {
        None => "none".to_string(),
        Some(f) => format!(
            "{} (since {}{})",
            f.offset,
            floor::utc_rfc3339(f.moved_at),
            f.oldest_ms
                .map(|ms| format!(
                    ", oldest record above it from {}",
                    floor::utc_rfc3339(ms / 1000)
                ))
                .unwrap_or_default()
        ),
    }
}

fn report_compaction_keys(n: usize) {
    let (topic, partition, mirror) = mirror_core::current_labels();
    metrics::gauge!(
        "mirror_v3_destination_compaction_keys",
        "topic" => topic,
        "partition" => partition,
        "mirror" => mirror,
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
    /// The last two objects of the chain, as written or found at open.
    tail: Vec<String>,
    last_drift_check: Instant,
    drift_check_interval: Duration,
    /// Append mode: the chain's floor, as found at open and by each
    /// drift check (the markers sort after every blob, so the drift
    /// check's listing has them all).
    floor: Option<Floor>,
    /// Objects below the floor that were still in the directory at open.
    below_floor_at_open: usize,
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
        if let Some(enc) = &spec.encryption {
            if spec.format != Format::Parquet {
                return Err(BlobError::Store(
                    "encryption needs `format: parquet`".to_string(),
                ));
            }
            enc.keyring
                .get(&enc.key_id)
                .map_err(|e| BlobError::Store(e.to_string()))?;
        }
        let view = match spec.compaction {
            None => None,
            Some(CompactionMode::Log) => {
                let view = match (&chain.latest, snapshot) {
                    (Some(name), Some(bytes)) => decode_view(
                        name,
                        &store.location(name),
                        &bytes,
                        spec.format,
                        spec.encryption.as_ref(),
                    )?,
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
        if chain.floor.is_some() {
            report_floor(chain.floor.as_ref());
            tracing::info!(
                floor = %describe_floor(chain.floor.as_ref()),
                below_floor_objects = chain.below_floor.len(),
                next = chain.durable,
                "the chain starts at its floor: objects that end below it may be missing"
            );
            if chain.tail.is_empty() {
                tracing::warn!(
                    floor = %describe_floor(chain.floor.as_ref()),
                    "the destination holds no object at or above its floor (every object \
                     expired); resuming at the floor"
                );
            }
        }
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
            tail: chain.tail,
            last_drift_check: Instant::now(),
            drift_check_interval: DRIFT_CHECK_INTERVAL,
            floor: chain.floor,
            below_floor_at_open: chain.below_floor.len(),
        })
    }

    /// Append mode: the chain's floor, as last seen.
    pub fn floor(&self) -> Option<&Floor> {
        self.floor.as_ref()
    }

    /// Objects below the floor that were still in the directory at open.
    pub fn below_floor_at_open(&self) -> usize {
        self.below_floor_at_open
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
        let name = naming::blob_filename(
            from,
            to,
            self.spec.encryption.as_ref().map(|e| e.key_id.as_str()),
            self.spec.format.extension(),
        );
        let location = self.store.location(&name);

        // Append mode: the record times go with the object, so that
        // `trim` can tell its age with a HEAD, and a retention lock
        // counts from them.
        let object = match self.spec.compaction {
            None => NewObject::Records {
                times: RecordTimes::of(&self.buffer),
                written_at: (self.clock)(),
            },
            Some(_) => NewObject::Snapshot,
        };
        // Compaction mode snapshots the view (current per record);
        // append mode encodes the buffer.
        let to_encode: Vec<Record> = match (self.spec.compaction, self.view.as_ref()) {
            (Some(CompactionMode::Log), Some(view)) => view.values().cloned().collect(),
            _ => std::mem::take(&mut self.buffer),
        };
        let bytes = match &self.spec.encryption {
            None => mirror_envelope::encode_batch(
                self.spec.format,
                self.spec.compression,
                self.spec.keys,
                self.spec.values,
                &to_encode,
            ),
            Some(enc) => mirror_envelope::parquet::encode_batch_encrypted(
                &to_encode,
                self.spec.compression,
                self.spec.keys,
                self.spec.values,
                enc.keyring
                    .get(&enc.key_id)
                    .expect("the active key was checked at open"),
            ),
        }
        .map_err(|e| SinkError::Rejected(format!("encode: {e}")))?;
        let encoded_bytes = bytes.len() as u64;

        let retain_until = match self.store.put_new(&name, bytes, &object).await {
            Ok(until) => until,
            Err(BlobError::AlreadyExists(_)) => {
                return Err(SinkError::Inconsistent(format!(
                    "{location} already exists (a second writer, or a write that landed after \
                     it was reported failed)"
                )));
            }
            Err(e @ BlobError::Lock(_)) => return Err(SinkError::Inconsistent(e.to_string())),
            Err(e) => return Err(SinkError::Transport(format!("write {location}: {e}"))),
        };

        self.durable_position = to + 1;
        self.tail.push(name);
        if self.tail.len() > 2 {
            self.tail.remove(0);
        }
        self.buffer.clear();
        self.buffer_bytes = 0;
        self.buffer_started = None;
        let elapsed_ms = flush_started.elapsed().as_millis() as u64;
        let interval_ms = self
            .last_flush_at
            .map(|t| t.elapsed().as_millis() as u64)
            .unwrap_or(0);
        self.last_flush_at = Some(Instant::now());

        let (topic, partition, mirror) = mirror_core::current_labels();
        metrics::gauge!(
            "mirror_v3_destination_offset_verified",
            "topic" => topic.clone(),
            "partition" => partition.clone(),
            "mirror" => mirror.clone(),
        )
        .set(self.durable_position as f64);
        metrics::gauge!(
            "mirror_v3_destination_last_flush_timestamp_seconds",
            "topic" => topic.clone(),
            "partition" => partition.clone(),
            "mirror" => mirror.clone(),
        )
        .set((self.clock)() as f64);
        metrics::counter!(
            "mirror_v3_destination_bytes_total",
            "topic" => topic.clone(),
            "partition" => partition.clone(),
            "mirror" => mirror.clone(),
        )
        .increment(encoded_bytes);
        metrics::counter!(
            "mirror_v3_destination_flushes_total",
            "topic" => topic,
            "partition" => partition,
            "mirror" => mirror,
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
            retain_until = retain_until.map(floor::utc_rfc3339).as_deref(),
            "flushed batch"
        );
        // The observer does something cheap (queue the event).
        if let Some(observer) = self.flush_observer.as_ref() {
            observer.on_flushed(from, to);
        }
        Ok(())
    }

    /// The destination must hold exactly what this process wrote: what
    /// sorts after the second-to-last object of the chain is the last
    /// object and nothing else, apart from floor markers (which sort
    /// after every blob) and, in append mode, objects that end below the
    /// floor.
    async fn check_drift(&mut self) -> Result<(), SinkError> {
        let (after, expected) = match self.tail.as_slice() {
            [] => (None, None),
            [last] => (None, Some(last.clone())),
            [before, last] => (Some(before.as_str()), Some(last.clone())),
            _ => unreachable!("the tail holds at most two names"),
        };
        let listed = self
            .store
            .list_after(after)
            .await
            .map_err(|e| SinkError::Transport(e.to_string()))?;
        let ext = self.spec.format.extension();
        let floor_offset = match self.spec.compaction {
            None => self.observe_floor(&listed)?,
            Some(_) => 0,
        };
        let below_floor =
            |name: &str| naming::parse_blob_name(name, ext).is_some_and(|b| b.to < floor_offset);
        // Trim removes objects below the floor. When the floor reaches
        // the end of the chain (every object expired), that is all of
        // them, the tail included.
        let expected = expected.filter(|last| !below_floor(last));
        if expected.is_none() {
            self.tail.clear();
        }
        let expected = expected.as_deref();
        // A compaction chain has no floor: a marker there is foreign.
        let append = self.spec.compaction.is_none();
        let listed: Vec<&String> = listed
            .iter()
            .filter(|n| !(append && floor::is_marker(n)) && !below_floor(n))
            .collect();
        if let Some(last) = expected {
            if !listed.iter().any(|n| *n == last) {
                return Err(SinkError::Inconsistent(format!(
                    "destination drift: {} is gone; it is the end of this mirror's chain",
                    self.store.location(last)
                )));
            }
        }
        if let Some(foreign) = listed.iter().find(|n| Some(n.as_str()) != expected) {
            return Err(SinkError::Inconsistent(format!(
                "destination drift: {} was not written by this process (a second writer, \
                 or a manual change); this mirror's chain ends before it, at next offset {}",
                self.store.location(foreign),
                self.durable_position
            )));
        }
        self.last_drift_check = Instant::now();
        Ok(())
    }

    /// Take the floor from a listing that holds every marker, log and
    /// report a change, and return its offset. A floor above the chain's
    /// end would make this mirror skip offsets: an error.
    fn observe_floor(&mut self, listed: &[String]) -> Result<u64, SinkError> {
        let (floor, _) = floor::floor_of(listed)
            .map_err(|e| SinkError::Inconsistent(format!("destination drift: {e}")))?;
        let offset = floor.map_or(0, |f| f.offset);
        if offset > self.durable_position {
            return Err(SinkError::Inconsistent(format!(
                "destination drift: a floor marker puts the chain's floor at {offset}, above \
                 the end of this mirror's chain, {}",
                self.durable_position
            )));
        }
        if floor != self.floor {
            let previous = self.floor.map_or(0, |f| f.offset);
            if offset < previous {
                tracing::warn!(
                    previous = %describe_floor(self.floor.as_ref()),
                    floor = %describe_floor(floor.as_ref()),
                    "the destination's floor moved back: its highest floor marker was removed"
                );
            } else {
                tracing::info!(
                    previous = %describe_floor(self.floor.as_ref()),
                    floor = %describe_floor(floor.as_ref()),
                    "the destination's floor moved"
                );
            }
            self.floor = floor;
            report_floor(self.floor.as_ref());
        }
        Ok(offset)
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
                    return Err(SinkError::Rejected(format!(
                        "this mirror requires a non-null key; \
                         record at source offset {} has key=null",
                        record.source_offset
                    )));
                }
                Some(k) => {
                    if std::str::from_utf8(k).is_err() {
                        return Err(SinkError::Rejected(format!(
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
            return Err(SinkError::Inconsistent(
                "align_to_source_low_watermark called on non-compaction sink".into(),
            ));
        }
        if !self.buffer.is_empty() || low_watermark < self.durable_position {
            return Err(SinkError::Inconsistent(format!(
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

    fn marker(offset: u64) -> String {
        FloorMarker {
            offset,
            at: 1_000 + offset,
            oldest_ms: None,
        }
        .filename()
    }

    fn append(names: &[String]) -> Result<Chain, String> {
        validate_chain(names, Format::Ndjson, None).map_err(|e| e.to_string())
    }

    fn n(a: u64, b: u64) -> String {
        naming::batch_filename(a, b, "ndjson")
    }

    #[test]
    fn a_chain_starts_at_its_floor_and_ignores_objects_below_it() {
        // The head expired entirely.
        let c = append(&[marker(10), n(10, 19), n(20, 29)]).unwrap();
        assert_eq!(c.durable, 30);
        assert_eq!(c.floor.map(|f| f.offset), Some(10));
        assert_eq!(c.tail, vec![n(10, 19), n(20, 29)]);
        assert!(c.below_floor.is_empty());
        // Objects below the floor are still there (a retention lock, a
        // deletion that has not run), some or all of them.
        let c = append(&[n(0, 4), n(5, 9), marker(10), n(10, 19)]).unwrap();
        assert_eq!(c.durable, 20);
        assert_eq!(c.below_floor, vec![n(0, 4), n(5, 9)]);
        let c = append(&[n(5, 9), marker(20), n(20, 29)]).unwrap();
        assert_eq!(c.durable, 30);
        assert_eq!(c.below_floor, vec![n(5, 9)]);
        // Below the floor nothing needs to be a chain.
        let c = append(&[n(0, 9), n(3, 12), marker(20), n(20, 29)]).unwrap();
        assert_eq!(c.durable, 30);
    }

    #[test]
    fn the_first_object_may_start_below_the_floor() {
        let c = append(&[marker(15), n(10, 19), n(20, 29)]).unwrap();
        assert_eq!(c.durable, 30);
        assert_eq!(c.tail, vec![n(10, 19), n(20, 29)]);
        assert!(c.below_floor.is_empty());
    }

    #[test]
    fn above_the_floor_the_chain_must_be_contiguous() {
        let gap = append(&[marker(10), n(10, 19), n(25, 29)]).unwrap_err();
        assert!(
            gap.contains("gap or overlap in chain: expected from=20"),
            "{gap}"
        );
        let overlap = append(&[marker(10), n(10, 19), n(15, 29)]).unwrap_err();
        assert!(overlap.contains("gap or overlap"), "{overlap}");
        let late = append(&[n(0, 9), marker(10), n(12, 19)]).unwrap_err();
        assert!(late.contains("expected from=10, found 12-19"), "{late}");
        assert!(late.contains("the chain's floor is 10"), "{late}");
    }

    #[test]
    fn without_a_floor_a_missing_head_is_corrupt_and_says_how_to_set_one() {
        let e = append(&[n(5, 9), n(10, 19)]).unwrap_err();
        assert!(e.contains("expected from=0, found 5-9"), "{e}");
        assert!(e.contains("mirror-v3 trim --floor 5"), "{e}");
    }

    #[test]
    fn a_floor_above_the_end_of_the_chain_is_corrupt_unless_nothing_is_left() {
        let e = append(&[n(0, 9), n(10, 19), marker(50)]).unwrap_err();
        assert!(
            e.contains("floor 50 is above the end of the chain, 20"),
            "{e}"
        );
        // At the end of the chain: every object expired.
        let c = append(&[n(0, 9), n(10, 19), marker(20)]).unwrap();
        assert_eq!(c.durable, 20);
        assert!(c.tail.is_empty());
        assert_eq!(c.below_floor.len(), 2);
        // Nothing left at all: the floor is where the chain ended.
        let c = append(&[marker(10), marker(20)]).unwrap();
        assert_eq!(c.durable, 20);
        assert_eq!(c.markers.len(), 2);
    }

    #[test]
    fn the_highest_marker_wins() {
        let c = append(&[marker(10), marker(5), n(10, 19)]).unwrap();
        assert_eq!(c.floor.map(|f| f.offset), Some(10));
        // Without the highest marker the floor is lower, and the chain
        // must reach down to it.
        let e = append(&[marker(5), n(10, 19)]).unwrap_err();
        assert!(e.contains("expected from=5, found 10-19"), "{e}");
    }

    #[test]
    fn a_broken_marker_name_is_an_error() {
        let e = append(&["floor-10.json".to_string(), n(0, 9)]).unwrap_err();
        assert!(e.contains("is not a floor marker name"), "{e}");
    }

    #[test]
    fn a_compaction_chain_has_no_floor() {
        let e = validate_chain(
            &[marker(10), naming::batch_filename(10, 19, "parquet")],
            Format::Parquet,
            Some(CompactionMode::Log),
        )
        .unwrap_err();
        assert!(e.to_string().contains("has no floor"), "{e}");
    }

    #[test]
    fn record_times_round_trip_through_metadata() {
        let rec = |ts: Option<i64>| Record {
            topic: "t".into(),
            partition: 0,
            source_offset: 0,
            timestamp_ms: ts,
            timestamp_type: mirror_core::TimestampType::CreateTime,
            key: None,
            value: None,
            headers: vec![],
        };
        let times = RecordTimes::of(&[rec(Some(20)), rec(None), rec(Some(10)), rec(Some(-1))]);
        assert_eq!(
            times,
            RecordTimes {
                min_ms: Some(10),
                max_ms: Some(20),
                without: 2
            }
        );
        let meta: std::collections::HashMap<&str, String> =
            times.to_metadata().into_iter().collect();
        let back = RecordTimes::from_metadata(|k| meta.get(k).cloned()).unwrap();
        assert_eq!(back, Some(times));
        let none = RecordTimes::of(&[rec(None)]);
        let meta: std::collections::HashMap<&str, String> =
            none.to_metadata().into_iter().collect();
        assert_eq!(
            RecordTimes::from_metadata(|k| meta.get(k).cloned()).unwrap(),
            Some(none)
        );
        assert_eq!(RecordTimes::from_metadata(|_| None).unwrap(), None);
        let e =
            RecordTimes::from_metadata(|k| (k == META_MAX_TIMESTAMP_MS).then(|| "5".to_string()))
                .unwrap_err();
        assert!(e.contains("incomplete"), "{e}");
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
    fn a_name_that_is_not_a_blob_is_an_error() {
        let err = validate_chain(
            &names(&["00000000000000000000-00000000000000000004.ndjson", "README"]),
            Format::Ndjson,
            None,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("README is not a blob name"),
            "{err}"
        );
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
