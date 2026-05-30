//! Small helpers around `rdkafka` for tests: topic management,
//! producing fixtures, draining a partition.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::TopicPartitionList;

pub async fn create_topic(bootstrap: &str, topic: &str, partitions: i32) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .context("admin client")?;
    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    let results = admin
        .create_topics(&[new_topic], &AdminOptions::new())
        .await
        .context("create_topics call")?;
    for result in results {
        result.map_err(|(t, e)| anyhow!("create topic {t}: {e:?}"))?;
    }
    Ok(())
}

/// Create a topic with the combined `cleanup.policy=compact,delete`.
///
/// This is the operator-intent stand-in for a production compacted
/// topic *and* the only policy on which Redpanda (and Kafka) accept
/// manual `DeleteRecords` calls: `cleanup.policy=compact` alone
/// raises `PolicyViolation` on delete-records, since the broker's
/// segment-reclaim flow is meant to be the only thing that advances
/// the low watermark. The combined policy preserves the
/// compaction semantics we want to model (latest value per key,
/// tombstone-removable) while letting the test deterministically
/// advance the low watermark via [`trim_records_before`] without
/// waiting on the broker's own compaction loop.
pub async fn create_compacted_topic(bootstrap: &str, topic: &str, partitions: i32) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .context("admin client")?;
    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1))
        .set("cleanup.policy", "compact,delete");
    let results = admin
        .create_topics(&[new_topic], &AdminOptions::new())
        .await
        .context("create_topics call")?;
    for result in results {
        result.map_err(|(t, e)| anyhow!("create topic {t}: {e:?}"))?;
    }
    Ok(())
}

/// Advance the partition's low watermark by deleting all records
/// with `source_offset < before_offset`. Returns the new low
/// watermark as reported by the broker after deletion. This is the
/// deterministic stand-in for "broker-side compaction has removed
/// earlier records": from the mirror consumer's point of view the
/// effect is identical (seek to 0 → broker delivers the new earliest
/// available offset).
pub async fn trim_records_before(
    bootstrap: &str,
    topic: &str,
    partition: i32,
    before_offset: i64,
) -> Result<i64> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .context("admin client")?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(
        topic,
        partition,
        rdkafka::topic_partition_list::Offset::Offset(before_offset),
    )
    .context("tpl add")?;
    let result = admin
        .delete_records(&tpl, &AdminOptions::new())
        .await
        .context("delete_records call")?;
    // result is a TPL with each partition's post-deletion low watermark
    // (or an error). We expect exactly one partition in the response.
    let elem = result
        .find_partition(topic, partition)
        .ok_or_else(|| anyhow!("delete_records returned no entry for {topic}-{partition}"))?;
    elem.error()
        .map_err(|e| anyhow!("delete_records partition error: {e}"))?;
    let new_low = match elem.offset() {
        rdkafka::topic_partition_list::Offset::Offset(o) => o,
        other => return Err(anyhow!("unexpected post-delete offset: {other:?}")),
    };
    Ok(new_low)
}

pub async fn produce_records(
    bootstrap: &str,
    topic: &str,
    partition: i32,
    pairs: &[(String, String)],
) -> Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("acks", "all")
        .create()
        .context("producer")?;
    for (k, v) in pairs {
        let record: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(topic)
            .partition(partition)
            .key(k.as_bytes())
            .payload(v.as_bytes());
        producer
            .send(record, Timeout::After(Duration::from_secs(10)))
            .await
            .map_err(|(e, _)| anyhow!("produce: {e}"))?;
    }
    Ok(())
}

/// Like [`produce_records`] but accepts `Option<String>` values so
/// callers can emit Kafka tombstones (null value). The key is always
/// non-null.
pub async fn produce_records_with_nullable_values(
    bootstrap: &str,
    topic: &str,
    partition: i32,
    pairs: &[(String, Option<String>)],
) -> Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("acks", "all")
        .create()
        .context("producer")?;
    for (k, v) in pairs {
        let mut record: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(topic)
            .partition(partition)
            .key(k.as_bytes());
        if let Some(v) = v {
            record = record.payload(v.as_bytes());
        }
        producer
            .send(record, Timeout::After(Duration::from_secs(10)))
            .await
            .map_err(|(e, _)| anyhow!("produce: {e}"))?;
    }
    Ok(())
}

/// Produce records, each with an explicit CreateTime timestamp in
/// milliseconds. The destination broker — depending on its topic
/// config and the mirror's `timestamp-mode` — may either keep this
/// timestamp or overwrite it.
pub async fn produce_records_with_timestamps(
    bootstrap: &str,
    topic: &str,
    partition: i32,
    triples: &[(String, String, i64)],
) -> Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("acks", "all")
        .create()
        .context("producer")?;
    for (k, v, ts) in triples {
        let record: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(topic)
            .partition(partition)
            .key(k.as_bytes())
            .payload(v.as_bytes())
            .timestamp(*ts);
        producer
            .send(record, Timeout::After(Duration::from_secs(10)))
            .await
            .map_err(|(e, _)| anyhow!("produce: {e}"))?;
    }
    Ok(())
}

/// Drain a partition and return each record's timestamp alongside
/// the offset/key/value. Used by timestamp-mode tests.
pub fn drain_partition_with_timestamps(
    bootstrap: &str,
    topic: &str,
    partition: i32,
    timeout: Duration,
) -> Result<Vec<TimestampedRecord>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "mirror-e2e-drain-ts")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("consumer")?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(
        topic,
        partition,
        rdkafka::topic_partition_list::Offset::Beginning,
    )?;
    consumer.assign(&tpl)?;
    let (_low, high) =
        consumer.fetch_watermarks(topic, partition, Timeout::After(Duration::from_secs(10)))?;
    let mut out = Vec::with_capacity(high.max(0) as usize);
    let deadline = std::time::Instant::now() + timeout;
    while (out.len() as i64) < high {
        if std::time::Instant::now() > deadline {
            return Err(anyhow!(
                "timed out draining partition with timestamps: got {}/{}",
                out.len(),
                high
            ));
        }
        if let Some(msg) = consumer.poll(Timeout::After(Duration::from_millis(500))) {
            let msg = msg.map_err(|e| anyhow!("poll: {e}"))?;
            out.push(TimestampedRecord {
                offset: msg.offset(),
                key: msg.key().map(|k| k.to_vec()),
                value: msg.payload().map(|p| p.to_vec()),
                timestamp_ms: msg.timestamp().to_millis(),
            });
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampedRecord {
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub timestamp_ms: Option<i64>,
}

/// Drain a partition by polling a BaseConsumer with an assigned
/// TopicPartitionList. Returns once the high watermark is reached or
/// `timeout` elapses.
pub fn drain_partition(
    bootstrap: &str,
    topic: &str,
    partition: i32,
    timeout: Duration,
) -> Result<Vec<DrainedRecord>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "mirror-e2e-drain")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("consumer")?;

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(
        topic,
        partition,
        rdkafka::topic_partition_list::Offset::Beginning,
    )?;
    consumer.assign(&tpl)?;

    let (_low, high) =
        consumer.fetch_watermarks(topic, partition, Timeout::After(Duration::from_secs(10)))?;
    let mut out = Vec::with_capacity(high.max(0) as usize);
    let deadline = std::time::Instant::now() + timeout;
    while (out.len() as i64) < high {
        if std::time::Instant::now() > deadline {
            return Err(anyhow!(
                "timed out draining partition: got {}/{}",
                out.len(),
                high
            ));
        }
        if let Some(msg) = consumer.poll(Timeout::After(Duration::from_millis(500))) {
            let msg = msg.map_err(|e| anyhow!("poll: {e}"))?;
            out.push(DrainedRecord {
                offset: msg.offset(),
                key: msg.key().map(|k| k.to_vec()),
                value: msg.payload().map(|p| p.to_vec()),
            });
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainedRecord {
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
}

/// Block until the destination partition's high watermark reaches
/// `expected`, or `timeout` elapses.
pub async fn wait_for_high_watermark(
    bootstrap: &str,
    topic: &str,
    partition: i32,
    expected: i64,
    timeout: Duration,
) -> Result<()> {
    let bootstrap = bootstrap.to_string();
    let topic = topic.to_string();
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let bs = bootstrap.clone();
        let t = topic.clone();
        let (_low, high) = tokio::task::spawn_blocking(move || {
            let consumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &bs)
                .set("group.id", "mirror-e2e-watermark")
                .create()
                .expect("consumer");
            consumer
                .fetch_watermarks(&t, partition, Timeout::After(Duration::from_secs(2)))
                .map_err(|e| anyhow!("fetch_watermarks: {e}"))
        })
        .await
        .map_err(|e| anyhow!("join: {e}"))??;
        if high >= expected {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            return Err(anyhow!(
                "timed out waiting for watermark: got {high}, expected {expected}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
