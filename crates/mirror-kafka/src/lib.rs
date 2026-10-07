//! Kafka source and sink for mirror-v3.
//!
//! Construction is parameterised by [`KafkaSourceConfig`] /
//! [`KafkaSinkConfig`]; transport is `rdkafka` (librdkafka under the
//! hood). The end-offset gate lives in [`KafkaSink::write`]: it queries
//! the destination high watermark, refuses to write if it has moved,
//! then asserts that the produced offset matches the source offset.
//! [`RestoreProducer`] is restore's target: in order, many records in
//! flight, every delivery checked against its offset.

#![allow(clippy::result_large_err)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use mirror_core::{
    ColumnType, Header, Record, Sink, SinkError, Source, SourceError, TimestampType, WriteObserver,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, StreamConsumer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::{Header as RdHeader, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::Offset;
use rdkafka::util::Timeout;
use rdkafka::TopicPartitionList;

mod restore_producer;
pub use restore_producer::RestoreProducer;

const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_WATERMARK_TIMEOUT: Duration = Duration::from_secs(10);

/// Fetch the high watermark for `(topic, partition)` against
/// `bootstrap`. One-shot; intended for the `status` subcommand and
/// other introspection callers. Sync call — wrap in spawn_blocking
/// for async contexts.
pub fn fetch_high_watermark(
    bootstrap: &str,
    topic: &str,
    partition: i32,
    timeout: Duration,
) -> Result<u64, KafkaError> {
    let (_low, high) = fetch_watermarks(bootstrap, topic, partition, timeout)?;
    Ok(high)
}

/// Fetch the low watermark for `(topic, partition)` against
/// `bootstrap` — the earliest offset still retained by the broker.
/// Greater than zero on compacted or `delete-records`-trimmed
/// partitions. Sync call — wrap in spawn_blocking for async contexts.
pub fn fetch_low_watermark(
    bootstrap: &str,
    topic: &str,
    partition: i32,
    timeout: Duration,
) -> Result<u64, KafkaError> {
    let (low, _high) = fetch_watermarks(bootstrap, topic, partition, timeout)?;
    Ok(low)
}

/// Read the broker's `__consumer_offsets` entry for the
/// `(group_id, topic, partition)` tuple. `Ok(None)` is the
/// "no committed value yet" sentinel (a fresh group, or a group
/// that hasn't committed for this partition); a committed value that
/// is not a position is an error. Sync; wrap in
/// `spawn_blocking` for async contexts. Mirrors the `fetch_*_watermark`
/// pattern so the supervisor can read the per-mirror committed
/// offset at startup without instantiating a full `KafkaSource`.
pub fn fetch_committed_offset(
    bootstrap: &str,
    group_id: &str,
    topic: &str,
    partition: i32,
    timeout: Duration,
) -> Result<Option<u64>, KafkaError> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .create()
        .map_err(|e| KafkaError::Init(e.to_string()))?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, partition);
    let filled = consumer
        .committed_offsets(tpl, Timeout::After(timeout))
        .map_err(|e| KafkaError::Init(format!("committed_offsets: {e}")))?;
    let elem = filled.find_partition(topic, partition).ok_or_else(|| {
        KafkaError::Init(format!(
            "committed_offsets returned no entry for {topic}/{partition}"
        ))
    })?;
    committed_position(elem.offset(), group_id, topic, partition)
}

/// A committed offset as a position: `Invalid` is librdkafka's "no
/// committed offset for this group" (`None`). Anything else that is not
/// a position (a logical offset, a negative number) is an error: reading
/// it as "no commit" would turn a returning deploy into a fresh one
/// (notify suppression from the high watermark).
fn committed_position(
    offset: Offset,
    group_id: &str,
    topic: &str,
    partition: i32,
) -> Result<Option<u64>, KafkaError> {
    match offset {
        Offset::Offset(n) if n >= 0 => Ok(Some(n as u64)),
        Offset::Invalid => Ok(None),
        other => Err(KafkaError::Init(format!(
            "group {group_id} has committed offset {other:?} for {topic}/{partition}, \
             which is not a position"
        ))),
    }
}

fn fetch_watermarks(
    bootstrap: &str,
    topic: &str,
    partition: i32,
    timeout: Duration,
) -> Result<(u64, u64), KafkaError> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "mirror-v3-status-noop")
        .set("enable.auto.commit", "false")
        .create()
        .map_err(|e| KafkaError::Init(e.to_string()))?;
    let (low, high) = consumer
        .fetch_watermarks(topic, partition, Timeout::After(timeout))
        .map_err(|e| KafkaError::Init(format!("fetch_watermarks: {e}")))?;
    Ok((
        watermark(low, topic, partition)?,
        watermark(high, topic, partition)?,
    ))
}

/// librdkafka reports an unknown watermark as -1; that is an error, not
/// offset 0 (which `.max(0)` used to make of it).
fn watermark(w: i64, topic: &str, partition: i32) -> Result<u64, KafkaError> {
    u64::try_from(w).map_err(|_| {
        KafkaError::Init(format!(
            "the broker reported watermark {w} for {topic}/{partition} (unknown)"
        ))
    })
}

#[derive(Debug, Clone)]
pub struct KafkaSourceConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
    pub poll_timeout: Duration,
}

impl KafkaSourceConfig {
    pub fn new(
        bootstrap_servers: impl Into<String>,
        group_id: impl Into<String>,
        topic: impl Into<String>,
        partition: i32,
    ) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            group_id: group_id.into(),
            topic: topic.into(),
            partition,
            poll_timeout: DEFAULT_POLL_TIMEOUT,
        }
    }
}

pub struct KafkaSource {
    consumer: Arc<StreamConsumer>,
    bootstrap_servers: String,
    group_id: String,
    topic: String,
    partition: i32,
    poll_timeout: Duration,
    /// Monotonic guard on `commit_through`. Shared with any
    /// [`KafkaCommitHandle`] handed out via [`Self::commit_handle`]
    /// so the supervisor's periodic task and any direct trait-method
    /// caller observe the same "highest staged" value.
    last_stored_offset: Arc<AtomicU64>,
}

/// Consumer settings for the source. Offsets are committed through
/// [`KafkaCommitHandle`] only (`store_offsets` needs the auto-store
/// path off). `auto.offset.reset=error`: the loop always seeks to an
/// explicit position (the destination's next offset, or the low
/// watermark), and if that position is gone (retention deleted it
/// while the mirror was down or lagging) the source fails instead of
/// jumping to the earliest offset. Blob destinations accept offset
/// holes (compaction, transaction markers), so a silent jump would
/// otherwise look like a hole and lose the records in between
/// unnoticed. The legacy Java worker used `earliest`; it also failed
/// whenever the destination did not match, which a hole-tolerant
/// destination no longer does.
fn consumer_config(bootstrap_servers: &str, group_id: &str) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "error");
    cfg
}

impl KafkaSource {
    pub fn open(cfg: KafkaSourceConfig) -> Result<Self, KafkaError> {
        let consumer: StreamConsumer = consumer_config(&cfg.bootstrap_servers, &cfg.group_id)
            .create()
            .map_err(|e| KafkaError::Init(e.to_string()))?;
        Ok(Self {
            consumer: Arc::new(consumer),
            bootstrap_servers: cfg.bootstrap_servers,
            group_id: cfg.group_id,
            topic: cfg.topic,
            partition: cfg.partition,
            poll_timeout: cfg.poll_timeout,
            last_stored_offset: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Hand the supervisor's periodic commit task a shared handle
    /// that can stage offsets and flush them to the broker without
    /// owning the source. The handle shares the same in-memory
    /// `last_stored_offset` so the monotonicity guard on
    /// [`Source::commit_through`] applies regardless of which path
    /// stages the value.
    pub fn commit_handle(&self) -> KafkaCommitHandle {
        KafkaCommitHandle {
            consumer: Arc::clone(&self.consumer),
            topic: self.topic.clone(),
            partition: self.partition,
            last_stored_offset: Arc::clone(&self.last_stored_offset),
        }
    }
}

/// Shared commit-side handle on a [`KafkaSource`]. Holds an `Arc` of
/// the underlying `StreamConsumer` so the supervisor's periodic
/// commit task can stage and flush offsets while the run loop holds
/// the source's `&mut Source` and is busy in `recv()`.
///
/// Cloning this is cheap (one `Arc::clone` per shared field) and
/// safe; every clone observes the same monotonic guard.
#[derive(Clone)]
pub struct KafkaCommitHandle {
    consumer: Arc<StreamConsumer>,
    topic: String,
    partition: i32,
    last_stored_offset: Arc<AtomicU64>,
}

impl KafkaCommitHandle {
    /// `true` iff the underlying consumer's `assignment()` currently
    /// includes the handle's `(topic, partition)`. The supervisor's
    /// readiness poller uses this to detect assignment loss without
    /// owning the source.
    ///
    /// Synchronous; rdkafka's `assignment()` reads in-memory state
    /// rather than contacting the broker. Returns `Err` if rdkafka
    /// reports an error reading the assignment.
    pub fn current_assignment_includes(&self) -> Result<bool, SourceError> {
        let tpl = self
            .consumer
            .assignment()
            .map_err(|e| SourceError::Transport(format!("assignment: {e}")))?;
        // `find_partition` is `None` when the partition isn't in the
        // current assignment.
        Ok(tpl.find_partition(&self.topic, self.partition).is_some())
    }

    /// Stage `through` as the next offset to commit. Idempotent and
    /// monotonic: identical to [`Source::commit_through`] but takes
    /// `&self`, so the supervisor's periodic task can call it
    /// without owning the source.
    pub fn commit_through(&self, through: u64) -> Result<(), SourceError> {
        stage_offset(
            &self.consumer,
            &self.topic,
            self.partition,
            &self.last_stored_offset,
            through,
        )
    }

    /// Flush every staged offset to the broker. Uses
    /// `CommitMode::Async` so the call returns immediately; the
    /// actual write happens inside librdkafka's poll thread. The
    /// supervisor's periodic task calls this after `commit_through`
    /// and treats the return as best-effort.
    pub fn commit_pending(&self) -> Result<(), SourceError> {
        self.consumer
            .commit_consumer_state(CommitMode::Async)
            .map_err(|e| SourceError::Transport(format!("commit_consumer_state: {e}")))
    }

    /// Like [`Self::commit_pending`] but with `CommitMode::Sync`:
    /// returns only after the broker has acknowledged the commit.
    /// Used for the one final commit on graceful shutdown, where an
    /// async commit would race process exit and usually lose. Blocks
    /// the calling thread; call under `spawn_blocking` from async
    /// contexts.
    pub fn commit_pending_sync(&self) -> Result<(), SourceError> {
        self.consumer
            .commit_consumer_state(CommitMode::Sync)
            .map_err(|e| SourceError::Transport(format!("commit_consumer_state sync: {e}")))
    }
}

/// Stage `through` as the offset to commit for `(topic, partition)`,
/// guarded by `last_stored_offset` against rewinds. Shared between
/// [`Source::commit_through`] (called via `&mut KafkaSource`) and
/// [`KafkaCommitHandle::commit_through`] (called via `&self`).
fn stage_offset(
    consumer: &StreamConsumer,
    topic: &str,
    partition: i32,
    last_stored_offset: &AtomicU64,
    through: u64,
) -> Result<(), SourceError> {
    // CAS-loop monotonicity guard. `fetch_max` reads the current
    // value, computes the new value (max of current and `through`),
    // and stores it atomically. If `through` is not higher we no-op.
    let prev = last_stored_offset.fetch_max(through, Ordering::AcqRel);
    if through <= prev {
        return Ok(());
    }
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Offset(through as i64))
        .map_err(|e| SourceError::Transport(format!("tpl add: {e}")))?;
    consumer
        .store_offsets(&tpl)
        .map_err(|e| SourceError::Transport(format!("store_offsets: {e}")))?;
    Ok(())
}

#[async_trait]
impl Source for KafkaSource {
    async fn seek(&mut self, next_offset: u64) -> Result<(), SourceError> {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(
            &self.topic,
            self.partition,
            Offset::Offset(next_offset as i64),
        )
        .map_err(|e| SourceError::Transport(format!("tpl add: {e}")))?;
        self.consumer
            .assign(&tpl)
            .map_err(|e| SourceError::Transport(format!("assign: {e}")))?;
        // Explicit seek in case the broker had a different committed
        // offset cached for this group.
        self.consumer
            .seek(
                &self.topic,
                self.partition,
                Offset::Offset(next_offset as i64),
                Timeout::After(Duration::from_secs(5)),
            )
            .map_err(|e| SourceError::Transport(format!("seek: {e}")))?;
        Ok(())
    }

    async fn poll_one(&mut self) -> Result<Option<Record>, SourceError> {
        match tokio::time::timeout(self.poll_timeout, self.consumer.recv()).await {
            Ok(Ok(borrowed)) => Ok(Some(borrowed_to_record(&borrowed))),
            // `auto.offset.reset=error` reports a position the broker
            // no longer has as AutoOffsetReset.
            Ok(Err(e)) => Err(match e.rdkafka_error_code() {
                Some(RDKafkaErrorCode::AutoOffsetReset | RDKafkaErrorCode::OffsetOutOfRange) => {
                    SourceError::PositionLost(e.to_string())
                }
                _ => SourceError::Transport(e.to_string()),
            }),
            Err(_elapsed) => Ok(None),
        }
    }

    async fn low_watermark(&mut self) -> Result<u64, SourceError> {
        // Issue a fresh `BaseConsumer`-backed watermark query via
        // `spawn_blocking`. Calling `fetch_watermarks` directly on
        // the `StreamConsumer` was unreliable in production: the
        // StreamConsumer's internal poll thread may not yet have
        // connected to the broker or fetched topic metadata by the
        // time bootstrap calls this, in which case librdkafka
        // returns Ok((0, 0)) (the "unknown" sentinel mapped to 0)
        // immediately. The mirror would then seek(0), the broker
        // would deliver its actual earliest offset (e.g. 461 on a
        // trimmed/compacted topic), and the bootstrap branch in
        // `mirror_core::run_mirror_with_heartbeat` would never
        // trigger because `0 < 0` is false.
        //
        // `fetch_low_watermark` matches the proven pattern used by
        // `KafkaSink::fetch_high_watermark` and the cache-readiness
        // gate in mirror-bin: a fresh `BaseConsumer` whose own
        // poll loop drives the metadata fetch synchronously inside
        // the call.
        let bootstrap = self.bootstrap_servers.clone();
        let topic = self.topic.clone();
        let partition = self.partition;
        let low = tokio::task::spawn_blocking(move || {
            fetch_low_watermark(&bootstrap, &topic, partition, DEFAULT_WATERMARK_TIMEOUT)
        })
        .await
        .map_err(|e| SourceError::Transport(format!("low_watermark join: {e}")))?
        .map_err(|e| SourceError::Transport(format!("fetch_low_watermark: {e}")))?;
        Ok(low)
    }

    async fn high_watermark(&mut self) -> Result<u64, SourceError> {
        let bootstrap = self.bootstrap_servers.clone();
        let topic = self.topic.clone();
        let partition = self.partition;
        let high = tokio::task::spawn_blocking(move || {
            fetch_high_watermark(&bootstrap, &topic, partition, DEFAULT_WATERMARK_TIMEOUT)
        })
        .await
        .map_err(|e| SourceError::Transport(format!("high_watermark join: {e}")))?
        .map_err(|e| SourceError::Transport(format!("fetch_high_watermark: {e}")))?;
        Ok(high)
    }

    async fn commit_through(&mut self, through: u64) -> Result<(), SourceError> {
        // Forwards into the shared helper so the trait path and the
        // `KafkaCommitHandle` path observe the same monotonic guard.
        // `store_offsets` is non-blocking in librdkafka (in-memory
        // stage), so no `spawn_blocking` here.
        stage_offset(
            &self.consumer,
            &self.topic,
            self.partition,
            &self.last_stored_offset,
            through,
        )
    }

    async fn fetch_committed_offset(&mut self) -> Result<Option<u64>, SourceError> {
        // Mirrors the `low_watermark` pattern: a fresh `BaseConsumer`
        // with the same `group.id` drives the metadata + offset
        // lookup inside a `spawn_blocking`. Delegates to the free
        // `fetch_committed_offset` helper so the supervisor's
        // startup path can read the value without instantiating a
        // full `KafkaSource`.
        let bootstrap = self.bootstrap_servers.clone();
        let group_id = self.group_id.clone();
        let topic = self.topic.clone();
        let partition = self.partition;
        tokio::task::spawn_blocking(move || {
            fetch_committed_offset(
                &bootstrap,
                &group_id,
                &topic,
                partition,
                DEFAULT_WATERMARK_TIMEOUT,
            )
            .map_err(|e| SourceError::Transport(format!("fetch_committed_offset: {e}")))
        })
        .await
        .map_err(|e| SourceError::Transport(format!("committed join: {e}")))?
    }
}

fn borrowed_to_record(msg: &rdkafka::message::BorrowedMessage<'_>) -> Record {
    let headers = msg
        .headers()
        .map(|hs| {
            (0..hs.count())
                .map(|i| {
                    let h = hs.get(i);
                    Header {
                        key: h.key.to_string(),
                        value: h.value.map(|v| v.to_vec()),
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let ts = msg.timestamp();
    let (timestamp_ms, timestamp_type) = match ts {
        rdkafka::Timestamp::CreateTime(ms) => (Some(ms), TimestampType::CreateTime),
        rdkafka::Timestamp::LogAppendTime(ms) => (Some(ms), TimestampType::LogAppendTime),
        rdkafka::Timestamp::NotAvailable => (None, TimestampType::NotAvailable),
    };
    Record {
        topic: msg.topic().to_string(),
        partition: msg.partition(),
        source_offset: msg.offset() as u64,
        timestamp_ms,
        timestamp_type,
        key: msg.key().map(|k| k.to_vec()),
        value: msg.payload().map(|v| v.to_vec()),
        headers,
    }
}

#[derive(Debug, Clone)]
pub struct KafkaSinkConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    pub partition: i32,
    pub watermark_timeout: Duration,
    pub timestamp_mode: TimestampMode,
    /// Topic-schema gate for the record `key`. Defaults to
    /// [`ColumnType::Utf8`]. The sink validates each non-null payload
    /// before producing; non-UTF-8 input (under `Utf8`/`Json`/
    /// `JsonParseable`) or unparseable JSON (under `JsonParseable`)
    /// is a hard `Transport` error pointing at the offending source
    /// offset. `Bytes` skips the check.
    pub keys: ColumnType,
    /// Topic-schema gate for the record `value`. Same semantics as
    /// `keys`.
    pub values: ColumnType,
}

/// Which timestamp the destination record ends up with.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TimestampMode {
    /// Pass `record.timestamp_ms` to `FutureRecord::timestamp(...)`.
    /// Destination stores it as CreateTime.
    #[default]
    Source,
    /// Don't call `.timestamp(...)`; destination broker stamps on
    /// receipt (CreateTime = send-time, or LogAppendTime if the
    /// destination topic is configured that way).
    Destination,
}

impl KafkaSinkConfig {
    pub fn new(
        bootstrap_servers: impl Into<String>,
        topic: impl Into<String>,
        partition: i32,
    ) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            topic: topic.into(),
            partition,
            watermark_timeout: DEFAULT_WATERMARK_TIMEOUT,
            timestamp_mode: TimestampMode::Source,
            keys: ColumnType::default(),
            values: ColumnType::default(),
        }
    }
}

/// Producer settings for the Kafka destination. The high-watermark gate
/// before each produce is the only retry: a produce that fails, or whose
/// ack is lost, ends the mirror, and the restart re-reads the
/// destination's high watermark, so a record that did land is not
/// produced again and one that did not is produced once. librdkafka's own
/// retries sit beneath the gate and cannot see it: a lost ack plus a
/// retry writes the record twice, and the restart then skips the next
/// source record. So `message.send.max.retries=0`.
/// `acks=all`, idempotence off and one request in flight are the legacy
/// Java worker's settings (MirrorProducer.java): the deployment that this
/// replaces chose them, and the gate makes ordering explicit.
fn producer_config(bootstrap_servers: &str) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", bootstrap_servers)
        .set("acks", "all")
        .set("enable.idempotence", "false")
        .set("max.in.flight.requests.per.connection", "1")
        .set("message.send.max.retries", "0");
    cfg
}

pub struct KafkaSink {
    producer: FutureProducer,
    watermark_consumer: Arc<BaseConsumer>,
    topic: String,
    partition: i32,
    watermark_timeout: Duration,
    timestamp_mode: TimestampMode,
    keys: ColumnType,
    values: ColumnType,
    /// Optional observer fired after every successful produce. Wired
    /// in by the supervisor via [`Sink::set_write_observer`]; default
    /// `None` so production code unaware of ack tracking keeps the
    /// existing single-write behaviour.
    write_observer: Option<Arc<dyn WriteObserver>>,
}

impl KafkaSink {
    pub fn open(cfg: KafkaSinkConfig) -> Result<Self, KafkaError> {
        let producer: FutureProducer = producer_config(&cfg.bootstrap_servers)
            .create()
            .map_err(|e| KafkaError::Init(e.to_string()))?;
        let watermark_consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &cfg.bootstrap_servers)
            .set("group.id", "mirror-v3-watermark-noop")
            .set("enable.auto.commit", "false")
            .create()
            .map_err(|e| KafkaError::Init(e.to_string()))?;
        Ok(Self {
            producer,
            watermark_consumer: Arc::new(watermark_consumer),
            topic: cfg.topic,
            partition: cfg.partition,
            watermark_timeout: cfg.watermark_timeout,
            timestamp_mode: cfg.timestamp_mode,
            keys: cfg.keys,
            values: cfg.values,
            write_observer: None,
        })
    }

    async fn fetch_high_watermark(&self) -> Result<u64, SinkError> {
        let consumer = Arc::clone(&self.watermark_consumer);
        let topic = self.topic.clone();
        let partition = self.partition;
        let timeout = self.watermark_timeout;
        let (_low, high) = tokio::task::spawn_blocking(move || {
            consumer.fetch_watermarks(&topic, partition, Timeout::After(timeout))
        })
        .await
        .map_err(|e| SinkError::Transport(format!("join: {e}")))?
        .map_err(|e| SinkError::Transport(e.to_string()))?;
        u64::try_from(high).map_err(|_| {
            SinkError::Transport(format!(
                "the destination broker reported high watermark {high} for {}/{} (unknown)",
                self.topic, self.partition
            ))
        })
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
        self.fetch_high_watermark().await
    }

    async fn write(&mut self, record: Record) -> Result<(), SinkError> {
        // Schema gate: enforce the declared column-type contract
        // before producing. Bytes skips, Utf8/Json check UTF-8,
        // JsonParseable also checks `serde_json::from_slice`.
        self.keys
            .validate("key", record.source_offset, record.key.as_deref())
            .map_err(SinkError::Transport)?;
        self.values
            .validate("value", record.source_offset, record.value.as_deref())
            .map_err(SinkError::Transport)?;

        // Gate: destination must still be at exactly source_offset.
        let current = self.fetch_high_watermark().await?;
        if current != record.source_offset {
            return Err(SinkError::UnexpectedPosition {
                expected: record.source_offset,
                actual: current,
            });
        }

        let key = record.key.as_deref();
        let value = record.value.as_deref();
        let mut fr: FutureRecord<'_, [u8], [u8]> =
            FutureRecord::to(&self.topic).partition(self.partition);
        if let Some(k) = key {
            fr = fr.key(k);
        }
        if let Some(v) = value {
            fr = fr.payload(v);
        }
        // Timestamp policy: Source preserves the source's timestamp_ms
        // by passing it to FutureRecord (destination stores it as
        // CreateTime). Destination omits the call so the broker stamps
        // it on receipt.
        if self.timestamp_mode == TimestampMode::Source {
            if let Some(ts) = record.timestamp_ms {
                fr = fr.timestamp(ts);
            }
        }
        let owned_headers = build_headers(&record.headers);
        if !record.headers.is_empty() {
            fr = fr.headers(owned_headers);
        }

        let delivery = self
            .producer
            .send(fr, Timeout::Never)
            .await
            .map_err(|(e, _msg)| SinkError::Transport(e.to_string()))?;

        if (delivery.offset as u64) != record.source_offset {
            return Err(SinkError::Transport(format!(
                "produced offset {} != source offset {}",
                delivery.offset, record.source_offset
            )));
        }
        // acks=all means the message is replicated and committed on
        // the destination by the time `send()` returns. The next
        // source offset the destination will accept is
        // `delivery.offset + 1`, which equals the destination's high
        // watermark — i.e. the verified-durable boundary.
        let (topic, partition, mirror) = mirror_core::current_labels();
        metrics::gauge!(
            "mirror_v3_destination_offset_verified",
            "topic" => topic,
            "partition" => partition,
            "mirror" => mirror,
        )
        .set((delivery.offset as u64 + 1) as f64);
        // Per-write ack signal. The supervisor's installed observer
        // bumps the per-destination ack tracker; the source-side
        // commit task then advances the broker-committed offset up
        // to the AND of every destination's ack and any notify ack.
        if let Some(obs) = self.write_observer.as_ref() {
            obs.on_written(record.source_offset);
        }
        Ok(())
    }

    fn set_write_observer(&mut self, observer: Arc<dyn WriteObserver>) {
        self.write_observer = Some(observer);
    }
}

fn build_headers(headers: &[Header]) -> OwnedHeaders {
    let mut out = OwnedHeaders::new_with_capacity(headers.len());
    for h in headers {
        out = out.insert(RdHeader {
            key: &h.key,
            value: h.value.as_deref(),
        });
    }
    out
}

#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    #[error("kafka client init: {0}")]
    Init(String),
}

#[cfg(test)]
mod tests {
    use rdkafka::config::ClientConfig;

    #[test]
    fn a_committed_offset_is_a_position_or_no_commit() {
        use rdkafka::topic_partition_list::Offset;
        let pos = |o| super::committed_position(o, "g", "ops", 0);
        assert_eq!(pos(Offset::Offset(42)).unwrap(), Some(42));
        assert_eq!(pos(Offset::Offset(0)).unwrap(), Some(0));
        assert_eq!(pos(Offset::Invalid).unwrap(), None);
        for odd in [
            Offset::Offset(-5),
            Offset::End,
            Offset::Beginning,
            Offset::Stored,
        ] {
            let err = pos(odd).expect_err("not a position");
            assert!(
                err.to_string().contains("group g has committed offset"),
                "{err}"
            );
        }
    }

    #[test]
    fn source_fails_on_a_lost_position_instead_of_jumping() {
        let native = super::consumer_config("localhost:9092", "g")
            .create_native_config()
            .expect("native config");
        assert_eq!(native.get("auto.offset.reset").unwrap(), "error");
        assert_eq!(native.get("enable.auto.commit").unwrap(), "false");
    }

    #[test]
    fn producer_never_retries_beneath_the_gate() {
        let native = super::producer_config("localhost:9092")
            .create_native_config()
            .expect("native config");
        let get = |k: &str| native.get(k).expect(k);
        assert_eq!(get("message.send.max.retries"), "0");
        assert_eq!(get("enable.idempotence"), "false");
        assert_eq!(get("acks"), "-1", "acks=all");
        assert_eq!(get("max.in.flight.requests.per.connection"), "1");
    }

    /// Producers on the topics we mirror compress batches with gzip or
    /// zstd (Java and Quarkus defaults). A librdkafka built without
    /// them fails every fetch of such a batch with "Not implemented",
    /// and the mirror crash-loops on the same batch.
    #[test]
    fn librdkafka_decompresses_gzip_and_zstd() {
        let features = ClientConfig::new()
            .create_native_config()
            .expect("native config")
            .get("builtin.features")
            .expect("builtin.features");
        let features: Vec<&str> = features.split(',').collect();
        for codec in ["gzip", "zstd", "snappy", "lz4"] {
            assert!(
                features.contains(&codec),
                "librdkafka lacks {codec}: builtin.features = {features:?}"
            );
        }
    }
}
