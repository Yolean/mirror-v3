//! A producer that writes records to one partition in order, many in
//! flight, and requires every one to land at the offset it is sent for.
//!
//! This is restore's target, not the mirror's: the mirror's
//! [`crate::KafkaSink`] reads the high watermark before every produce
//! and never retries, so that a restart can always resume. A restore
//! does not resume (it requires an empty topic, and a failed one is
//! deleted and run again), so it can trade that per-record gate for
//! throughput: the idempotent producer retries without duplicating or
//! reordering, and every delivery report is checked against the offset
//! the record was sent for, so another writer on the partition, or a
//! record lost or written twice, still ends the restore. Keys and values
//! are checked against the mirror's column types before they are sent,
//! as the mirror's destination does.
//!
//! The broker refuses a record batch larger than the topic's
//! `max.message.bytes`, which limits the batch, not the record. Topics
//! are often created with a lower limit than the producer's default
//! batch size (1000000 bytes), so the producer is opened with the
//! topic's limit, read from the broker, as its batch size: batches are
//! as large as the topic accepts, and a record that does not fit in
//! the current batch starts the next one. A record that alone exceeds
//! the limit fails the restore before it is sent.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use mirror_core::{ColumnType, Record, RestoreTarget};
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError as RdKafkaError, RDKafkaErrorCode};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use rdkafka::statistics::Statistics;
use rdkafka::util::Timeout;

use crate::{build_headers, KafkaError};

/// Records sent and not yet acknowledged, at most. librdkafka's own
/// queue holds 100000 by default; this keeps memory bounded well below
/// it and leaves the broker full batches to work on.
const DEFAULT_WINDOW: usize = 10_000;

/// How often librdkafka reports the batches it sent, which the restore
/// logs when it finishes.
const STATISTICS_INTERVAL_MS: u64 = 1000;

/// The record batch header of Kafka's message format v2, from base
/// offset to record count: what a batch costs beyond its records.
const RECORD_BATCH_OVERHEAD: usize = 61;

/// Room for the produce request around one batch (request header,
/// client id, topic name), which librdkafka's `message.max.bytes`
/// counts and the topic's limit does not.
const REQUEST_OVERHEAD: u64 = 64 * 1024;

/// Producer settings for restore. `enable.idempotence` implies
/// `acks=all` and retries that neither duplicate nor reorder, with up
/// to five requests in flight. `enable.gapless.guarantee` makes a batch
/// that fails while later ones succeed a fatal producer error at once,
/// rather than a delivery report at an unexpected offset later.
/// `linger.ms` lets records batch.
///
/// `batch.size` is the topic's `max.message.bytes`. librdkafka closes
/// a batch before the record that would take it over `batch.size`,
/// counting each record at its largest encoding and the produce
/// request's header too, so a batch on the wire stays below the limit.
/// Its first record is always added, so a batch of one can exceed it:
/// [`RestoreProducer::send`] refuses such a record first. Restore does
/// not compress (librdkafka's default), so the size the broker checks
/// is the size librdkafka counted. `message.max.bytes`, librdkafka's
/// limit on a request and on a single record, must leave room for that
/// batch.
fn restore_producer_config(bootstrap_servers: &str, max_message_bytes: u64) -> ClientConfig {
    let batch_size = max_message_bytes.min(i32::MAX as u64);
    let request_size = (batch_size + REQUEST_OVERHEAD).clamp(1_000_000, 1_000_000_000);
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", bootstrap_servers)
        .set("enable.idempotence", "true")
        .set("enable.gapless.guarantee", "true")
        .set("acks", "all")
        .set("max.in.flight.requests.per.connection", "5")
        .set("linger.ms", "20")
        .set("compression.codec", "none")
        .set("batch.size", batch_size.to_string())
        .set("message.max.bytes", request_size.to_string())
        .set("statistics.interval.ms", STATISTICS_INTERVAL_MS.to_string());
    cfg
}

/// The largest record batch `topic` accepts: its `max.message.bytes`.
/// Brokers report it for every topic, from the topic's own config or
/// from the broker default; if a broker leaves it out, the broker's
/// `message.max.bytes` is that default.
pub async fn fetch_max_message_bytes(
    bootstrap: &str,
    topic: &str,
    timeout: Duration,
) -> Result<u64, KafkaError> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .map_err(|e| KafkaError::Init(e.to_string()))?;
    let opts = AdminOptions::new().request_timeout(Some(timeout));
    let described = describe_one(&admin, ResourceSpecifier::Topic(topic), &opts)
        .await
        .map_err(|e| KafkaError::Read(format!("topic {topic}'s config: {e}")))?;
    if let Some(value) = described.get("max.message.bytes") {
        return parse_bytes(topic, "max.message.bytes", value.value.as_deref());
    }
    let (bootstrap, name) = (bootstrap.to_string(), topic.to_string());
    let broker = tokio::task::spawn_blocking(move || -> Result<i32, KafkaError> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .create()
            .map_err(|e| KafkaError::Init(e.to_string()))?;
        let md = consumer
            .fetch_metadata(Some(&name), Timeout::After(timeout))
            .map_err(|e| KafkaError::Read(format!("metadata of {name}: {e}")))?;
        md.brokers()
            .first()
            .map(|b| b.id())
            .ok_or_else(|| KafkaError::Read(format!("metadata of {name}: no brokers")))
    })
    .await
    .map_err(|e| KafkaError::Read(format!("join: {e}")))??;
    let described = describe_one(&admin, ResourceSpecifier::Broker(broker), &opts)
        .await
        .map_err(|e| KafkaError::Read(format!("broker {broker}'s config: {e}")))?;
    parse_bytes(
        topic,
        "message.max.bytes (the broker default, as the topic has no max.message.bytes)",
        described
            .get("message.max.bytes")
            .and_then(|e| e.value.as_deref()),
    )
}

async fn describe_one(
    admin: &AdminClient<DefaultClientContext>,
    resource: ResourceSpecifier<'_>,
    opts: &AdminOptions,
) -> Result<rdkafka::admin::ConfigResource, String> {
    admin
        .describe_configs(&[resource], opts)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .next()
        .ok_or_else(|| "the broker described nothing".to_string())?
        .map_err(|e| e.to_string())
}

fn parse_bytes(topic: &str, name: &str, value: Option<&str>) -> Result<u64, KafkaError> {
    value
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&v| v > 0)
        .ok_or_else(|| KafkaError::Read(format!("topic {topic}: {name} is {value:?}, not a size")))
}

/// Bytes of a zigzag varint, as the record format encodes lengths and
/// deltas.
fn varint_len(v: i64) -> usize {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    let mut n = 1;
    while z >= 0x80 {
        z >>= 7;
        n += 1;
    }
    n
}

/// A length-prefixed field: a null is the length -1 alone.
fn field_len(bytes: Option<&[u8]>) -> usize {
    match bytes {
        None => varint_len(-1),
        Some(b) => varint_len(b.len() as i64) + b.len(),
    }
}

/// The size of `record` as a batch of its own, uncompressed, in Kafka's
/// message format v2: the smallest batch it can be sent in, which is
/// what a topic's `max.message.bytes` must allow. Its offset and
/// timestamp deltas are 0, one byte each.
pub(crate) fn single_record_batch_bytes(record: &Record) -> usize {
    let mut body = 1 // attributes
        + varint_len(0) // timestamp delta
        + varint_len(0) // offset delta
        + field_len(record.key.as_deref())
        + field_len(record.value.as_deref())
        + varint_len(record.headers.len() as i64);
    for h in &record.headers {
        body += field_len(Some(h.key.as_bytes())) + field_len(h.value.as_deref());
    }
    RECORD_BATCH_OVERHEAD + varint_len(body as i64) + body
}

/// The batches librdkafka reports in its statistics for one topic:
/// how many, and the largest in bytes and in records.
#[derive(Debug, Default)]
struct BatchStats {
    topic: String,
    reports: AtomicU64,
    batches: AtomicU64,
    largest_bytes: AtomicU64,
    most_records: AtomicU64,
}

/// Each report covers the batches since the one before.
struct StatsContext(Arc<BatchStats>);

impl ClientContext for StatsContext {
    fn stats(&self, statistics: Statistics) {
        let s = &self.0;
        if let Some(t) = statistics.topics.get(&s.topic) {
            s.batches
                .fetch_add(t.batchsize.cnt.max(0) as u64, Ordering::Relaxed);
            s.largest_bytes
                .fetch_max(t.batchsize.max.max(0) as u64, Ordering::Relaxed);
            s.most_records
                .fetch_max(t.batchcnt.max.max(0) as u64, Ordering::Relaxed);
        }
        s.reports.fetch_add(1, Ordering::Release);
    }
}

pub struct RestoreProducer {
    producer: FutureProducer<StatsContext>,
    bootstrap_servers: String,
    topic: String,
    partition: i32,
    keys: ColumnType,
    values: ColumnType,
    max_message_bytes: u64,
    stats: Arc<BatchStats>,
    watermark_timeout: Duration,
    window: usize,
    in_flight: VecDeque<(u64, DeliveryFuture)>,
}

impl RestoreProducer {
    /// Open a producer for `topic`'s `partition`, with batches as large
    /// as the topic's `max.message.bytes`, which is read from the broker
    /// (and logged). `keys` and `values` are the mirror's column types,
    /// which every key and value must satisfy before it is sent.
    pub async fn open(
        bootstrap_servers: impl Into<String>,
        topic: impl Into<String>,
        partition: i32,
        keys: ColumnType,
        values: ColumnType,
    ) -> Result<Self, KafkaError> {
        let bootstrap_servers = bootstrap_servers.into();
        let topic = topic.into();
        let max_message_bytes =
            fetch_max_message_bytes(&bootstrap_servers, &topic, crate::DEFAULT_WATERMARK_TIMEOUT)
                .await?;
        tracing::info!(
            topic = %topic,
            max_message_bytes,
            "batches are limited to the topic's max.message.bytes"
        );
        Self::with_max_message_bytes(
            bootstrap_servers,
            topic,
            partition,
            keys,
            values,
            max_message_bytes,
        )
    }

    /// [`Self::open`] with the topic's `max.message.bytes` given.
    pub fn with_max_message_bytes(
        bootstrap_servers: impl Into<String>,
        topic: impl Into<String>,
        partition: i32,
        keys: ColumnType,
        values: ColumnType,
        max_message_bytes: u64,
    ) -> Result<Self, KafkaError> {
        let bootstrap_servers = bootstrap_servers.into();
        let topic = topic.into();
        let stats = Arc::new(BatchStats {
            topic: topic.clone(),
            ..BatchStats::default()
        });
        let producer: FutureProducer<StatsContext> =
            restore_producer_config(&bootstrap_servers, max_message_bytes)
                .create_with_context(StatsContext(Arc::clone(&stats)))
                .map_err(|e| KafkaError::Init(e.to_string()))?;
        Ok(Self {
            producer,
            bootstrap_servers,
            topic,
            partition,
            keys,
            values,
            max_message_bytes,
            stats,
            watermark_timeout: crate::DEFAULT_WATERMARK_TIMEOUT,
            window: DEFAULT_WINDOW,
            in_flight: VecDeque::new(),
        })
    }

    async fn ack_oldest(&mut self) -> Result<(), String> {
        let Some((offset, delivery)) = self.in_flight.pop_front() else {
            return Ok(());
        };
        let delivered = delivery
            .await
            .map_err(|_| format!("offset {offset}: the producer dropped the delivery report"))?
            .map_err(|(e, _)| format!("offset {offset} was not delivered: {e}"))?;
        if delivered.partition != self.partition || delivered.offset != offset as i64 {
            return Err(format!(
                "sent for offset {offset}, the broker stored it at {}/{} offset {}: another \
                 writer on the partition, or a record lost or written twice",
                self.topic, delivered.partition, delivered.offset
            ));
        }
        Ok(())
    }

    /// Log the batches librdkafka reported, after one more report so
    /// that the last batches are in it.
    async fn log_batches(&self) {
        let seen = self.stats.reports.load(Ordering::Acquire);
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(STATISTICS_INTERVAL_MS * 2 + 500);
        while self.stats.reports.load(Ordering::Acquire) == seen
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        tracing::info!(
            topic = %self.topic,
            batches = self.stats.batches.load(Ordering::Relaxed),
            largest_batch_bytes = self.stats.largest_bytes.load(Ordering::Relaxed),
            most_records_in_a_batch = self.stats.most_records.load(Ordering::Relaxed),
            max_message_bytes = self.max_message_bytes,
            "produced in batches"
        );
    }
}

#[async_trait]
impl RestoreTarget for RestoreProducer {
    /// The partition's high watermark, from the broker.
    async fn high_watermark(&mut self) -> Result<u64, String> {
        let bootstrap = self.bootstrap_servers.clone();
        let topic = self.topic.clone();
        let partition = self.partition;
        let timeout = self.watermark_timeout;
        tokio::task::spawn_blocking(move || {
            crate::fetch_high_watermark(&bootstrap, &topic, partition, timeout)
        })
        .await
        .map_err(|e| format!("join: {e}"))?
        .map_err(|e| e.to_string())
    }

    /// Send `record` to be stored at `offset`, with its key, value,
    /// headers and timestamp (as CreateTime). Returns once it is
    /// queued; waits first for the oldest record in flight when the
    /// window is full, and fails if that one did not land at its
    /// offset. A record that alone is a batch larger than the topic's
    /// `max.message.bytes` is not sent.
    async fn send(&mut self, record: &Record, offset: u64) -> Result<(), String> {
        self.keys.validate("key", offset, record.key.as_deref())?;
        self.values
            .validate("value", offset, record.value.as_deref())?;
        let alone = single_record_batch_bytes(record) as u64;
        if alone > self.max_message_bytes {
            return Err(format!(
                "the record for offset {offset} is a {alone}-byte batch on its own (key {} \
                 bytes, value {} bytes, {} headers), and topic {}'s max.message.bytes is {}: \
                 the broker would refuse it",
                record.key.as_ref().map_or(0, Vec::len),
                record.value.as_ref().map_or(0, Vec::len),
                record.headers.len(),
                self.topic,
                self.max_message_bytes
            ));
        }
        if self.in_flight.len() >= self.window {
            self.ack_oldest().await?;
        }
        loop {
            let mut fr: FutureRecord<'_, [u8], [u8]> =
                FutureRecord::to(&self.topic).partition(self.partition);
            if let Some(k) = record.key.as_deref() {
                fr = fr.key(k);
            }
            if let Some(v) = record.value.as_deref() {
                fr = fr.payload(v);
            }
            if let Some(ts) = record.timestamp_ms {
                fr = fr.timestamp(ts);
            }
            if !record.headers.is_empty() {
                fr = fr.headers(build_headers(&record.headers));
            }
            match self.producer.send_result(fr) {
                Ok(delivery) => {
                    self.in_flight.push_back((offset, delivery));
                    return Ok(());
                }
                Err((RdKafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _))
                    if !self.in_flight.is_empty() =>
                {
                    self.ack_oldest().await?;
                }
                Err((e, _)) => return Err(format!("producing offset {offset}: {e}")),
            }
        }
    }

    /// Wait until every record sent has landed at its offset.
    async fn finish(&mut self) -> Result<(), String> {
        while !self.in_flight.is_empty() {
            self.ack_oldest().await?;
        }
        self.producer
            .flush(Timeout::After(self.watermark_timeout))
            .map_err(|e| format!("flush: {e}"))?;
        self.log_batches().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use mirror_core::{ColumnType, Header, Record, RestoreTarget, TimestampType};
    use rdkafka::producer::BaseProducer;

    use super::{single_record_batch_bytes, RestoreProducer};

    fn record(key: &[u8], value: &[u8]) -> Record {
        Record {
            topic: "t".into(),
            partition: 0,
            source_offset: 0,
            timestamp_ms: None,
            timestamp_type: TimestampType::CreateTime,
            key: Some(key.to_vec()),
            value: Some(value.to_vec()),
            headers: Vec::new(),
        }
    }

    fn open(keys: ColumnType, values: ColumnType, max_message_bytes: u64) -> RestoreProducer {
        RestoreProducer::with_max_message_bytes(
            "localhost:1",
            "t",
            0,
            keys,
            values,
            max_message_bytes,
        )
        .unwrap()
    }

    /// Checked before anything is sent: no broker is needed.
    #[tokio::test]
    async fn keys_and_values_are_checked_against_the_column_types() {
        let mut p = open(ColumnType::Utf8, ColumnType::JsonParseable, 1_000_000);
        let err = p.send(&record(&[0xff], b"{}"), 7).await.unwrap_err();
        assert!(
            err.contains("key at source offset 7 is not valid UTF-8"),
            "{err}"
        );
        let err = p.send(&record(b"k", b"{not json"), 8).await.unwrap_err();
        assert!(
            err.contains("value at source offset 8 is not parseable JSON"),
            "{err}"
        );
        let mut p = open(ColumnType::Bytes, ColumnType::Bytes, 1_000_000);
        // Bytes are not checked; nothing is awaited before the send is queued.
        p.send(&record(&[0xff], &[0xfe]), 0).await.unwrap();
    }

    /// Sizes as Kafka's DefaultRecordBatch writes them: 61 bytes of
    /// batch header, then the record with its zigzag varint lengths.
    #[test]
    fn a_record_alone_is_sized_as_kafka_writes_it() {
        // attributes, two deltas, key 1+1, value 1+1, header count: 8,
        // and its length: 1.
        assert_eq!(single_record_batch_bytes(&record(b"k", b"v")), 61 + 9);
        // A null key and value are a length of -1 each.
        let mut tombstone = record(b"", b"");
        tombstone.key = None;
        tombstone.value = None;
        assert_eq!(single_record_batch_bytes(&tombstone), 61 + 7);
        // Lengths from 64 take two bytes (zigzag), as does a body from 64.
        let r = record(b"", &[b'x'; 64]);
        assert_eq!(
            single_record_batch_bytes(&r),
            61 + 2 + (1 + 1 + 1 + 1 + 2 + 64 + 1)
        );
        // A header is its key and value, a null value being -1.
        let mut h = record(b"k", b"v");
        h.headers = vec![
            Header {
                key: "trace".into(),
                value: Some(b"t1".to_vec()),
            },
            Header {
                key: "empty".into(),
                value: None,
            },
        ];
        assert_eq!(
            single_record_batch_bytes(&h),
            61 + 1 + (8 + (1 + 5 + 1 + 2) + (1 + 5 + 1))
        );
        // A record near a 500006-byte topic's limit: three-byte lengths.
        let big = record(b"k", &vec![b'x'; 499_930]);
        assert_eq!(
            single_record_batch_bytes(&big),
            61 + 3 + (1 + 1 + 1 + 2 + 3 + 499_930 + 1)
        );
    }

    /// The limit is the batch's: a record that fills it is sent, one
    /// byte more is refused with the topic, offset and sizes, before
    /// anything reaches a broker.
    #[tokio::test]
    async fn a_record_larger_than_the_topics_limit_alone_is_refused() {
        let fits = record(b"k", &[b'x'; 1000]);
        let limit = single_record_batch_bytes(&fits) as u64;
        let mut p = open(ColumnType::Bytes, ColumnType::Bytes, limit);
        p.send(&fits, 0).await.unwrap();
        let mut too_large = fits.clone();
        too_large.value = Some(vec![b'x'; 1001]);
        let err = p.send(&too_large, 1).await.unwrap_err();
        assert_eq!(
            err,
            format!(
                "the record for offset 1 is a {}-byte batch on its own (key 1 bytes, value \
                 1001 bytes, 0 headers), and topic t's max.message.bytes is {limit}: the \
                 broker would refuse it",
                limit + 1
            )
        );
    }

    #[test]
    fn restore_produces_idempotently_with_retries_and_batching() {
        let native = super::restore_producer_config("localhost:9092", 1_000_012)
            .create_native_config()
            .expect("native config");
        let get = |k: &str| native.get(k).unwrap();
        assert_eq!(get("enable.idempotence"), "true");
        assert_eq!(get("acks"), "-1", "acks=all");
        assert_eq!(get("max.in.flight.requests.per.connection"), "5");
        assert_ne!(get("message.send.max.retries"), "0");
        assert_eq!(get("enable.gapless.guarantee"), "true");
        assert_eq!(get("linger.ms"), "20");
        assert_eq!(get("compression.codec"), "none");
    }

    /// Batches are as large as the topic allows, below the producer's
    /// default too; librdkafka's request limit leaves room for one.
    #[test]
    fn batches_are_as_large_as_the_topics_max_message_bytes() {
        for (limit, batch, request) in [
            (500_006, "500006", "1000000"),
            (1_000_012, "1000012", "1065548"),
            (1_048_576, "1048576", "1114112"),
            (10_485_760, "10485760", "10551296"),
        ] {
            let cfg = super::restore_producer_config("localhost:9092", limit);
            let native = cfg.create_native_config().expect("native config");
            assert_eq!(native.get("batch.size").unwrap(), batch, "{limit}");
            assert_eq!(native.get("message.max.bytes").unwrap(), request, "{limit}");
            // librdkafka accepts the combination (no broker needed).
            cfg.create::<BaseProducer>().expect("producer");
        }
    }
}
