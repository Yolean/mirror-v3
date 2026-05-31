//! Kafka source and sink for mirror-v3.
//!
//! Construction is parameterised by [`KafkaSourceConfig`] /
//! [`KafkaSinkConfig`]; transport is `rdkafka` (librdkafka under the
//! hood). The end-offset gate lives in [`KafkaSink::write`]: it queries
//! the destination high watermark, refuses to write if it has moved,
//! then asserts that the produced offset matches the source offset.

#![allow(clippy::result_large_err)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use mirror_core::{
    ColumnType, Header, Record, Sink, SinkError, Source, SourceError, TimestampType,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::message::{Header as RdHeader, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::Offset;
use rdkafka::util::Timeout;
use rdkafka::TopicPartitionList;

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
) -> Result<i64, KafkaError> {
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
) -> Result<i64, KafkaError> {
    let (low, _high) = fetch_watermarks(bootstrap, topic, partition, timeout)?;
    Ok(low)
}

fn fetch_watermarks(
    bootstrap: &str,
    topic: &str,
    partition: i32,
    timeout: Duration,
) -> Result<(i64, i64), KafkaError> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "mirror-v3-status-noop")
        .set("enable.auto.commit", "false")
        .create()
        .map_err(|e| KafkaError::Init(e.to_string()))?;
    consumer
        .fetch_watermarks(topic, partition, Timeout::After(timeout))
        .map_err(|e| KafkaError::Init(format!("fetch_watermarks: {e}")))
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
    consumer: StreamConsumer,
    bootstrap_servers: String,
    topic: String,
    partition: i32,
    poll_timeout: Duration,
}

impl KafkaSource {
    pub fn open(cfg: KafkaSourceConfig) -> Result<Self, KafkaError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &cfg.bootstrap_servers)
            .set("group.id", &cfg.group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            // Note: the Java worker used `max.poll.records=1` for
            // single-record progression; that property is Java-client
            // only, not librdkafka. The loop in mirror-core already
            // takes one record at a time via `recv()` so we don't
            // need a fetcher-side cap to preserve the invariant.
            .create()
            .map_err(|e| KafkaError::Init(e.to_string()))?;
        Ok(Self {
            consumer,
            bootstrap_servers: cfg.bootstrap_servers,
            topic: cfg.topic,
            partition: cfg.partition,
            poll_timeout: cfg.poll_timeout,
        })
    }
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
            Ok(Err(e)) => Err(SourceError::Transport(e.to_string())),
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
        Ok(low.max(0) as u64)
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

pub struct KafkaSink {
    producer: FutureProducer,
    watermark_consumer: Arc<BaseConsumer>,
    topic: String,
    partition: i32,
    watermark_timeout: Duration,
    timestamp_mode: TimestampMode,
    keys: ColumnType,
    values: ColumnType,
}

impl KafkaSink {
    pub fn open(cfg: KafkaSinkConfig) -> Result<Self, KafkaError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &cfg.bootstrap_servers)
            .set("acks", "all")
            // The gate is what enforces ordering; idempotence not needed
            // and incompatible with the offset-equality assertion.
            .set("enable.idempotence", "false")
            .set("max.in.flight.requests.per.connection", "1")
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
        Ok(high.max(0) as u64)
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
        let (topic, partition) = mirror_core::current_labels();
        metrics::gauge!(
            "mirror_v3_destination_offset_verified",
            "topic" => topic,
            "partition" => partition,
        )
        .set((delivery.offset as u64 + 1) as f64);
        Ok(())
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
