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
use mirror_core::{Header, Record, Sink, SinkError, Source, SourceError};
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
    use rdkafka::consumer::Consumer;
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "mirror-v3-status-noop")
        .set("enable.auto.commit", "false")
        .create()
        .map_err(|e| KafkaError::Init(e.to_string()))?;
    let (_low, high) = consumer
        .fetch_watermarks(topic, partition, Timeout::After(timeout))
        .map_err(|e| KafkaError::Init(format!("fetch_watermarks: {e}")))?;
    Ok(high)
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
    Record {
        source_offset: msg.offset() as u64,
        key: msg.key().map(|k| k.to_vec()),
        value: msg.payload().map(|v| v.to_vec()),
        timestamp_ms: msg.timestamp().to_millis(),
        headers,
    }
}

#[derive(Debug, Clone)]
pub struct KafkaSinkConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    pub partition: i32,
    pub watermark_timeout: Duration,
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
        }
    }
}

pub struct KafkaSink {
    producer: FutureProducer,
    watermark_consumer: Arc<BaseConsumer>,
    topic: String,
    partition: i32,
    watermark_timeout: Duration,
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
        if let Some(ts) = record.timestamp_ms {
            fr = fr.timestamp(ts);
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
