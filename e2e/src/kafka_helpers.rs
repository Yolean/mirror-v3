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
