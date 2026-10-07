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

use std::collections::VecDeque;
use std::time::Duration;

use mirror_core::{ColumnType, Record};
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError as RdKafkaError, RDKafkaErrorCode};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;

use crate::{build_headers, KafkaError};

/// Records sent and not yet acknowledged, at most. librdkafka's own
/// queue holds 100000 by default; this keeps memory bounded well below
/// it and leaves the broker full batches to work on.
const DEFAULT_WINDOW: usize = 10_000;

/// Producer settings for restore. `enable.idempotence` implies
/// `acks=all` and retries that neither duplicate nor reorder, with up
/// to five requests in flight. `enable.gapless.guarantee` makes a batch
/// that fails while later ones succeed a fatal producer error at once,
/// rather than a delivery report at an unexpected offset later.
/// `linger.ms` lets records batch.
fn restore_producer_config(bootstrap_servers: &str) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", bootstrap_servers)
        .set("enable.idempotence", "true")
        .set("enable.gapless.guarantee", "true")
        .set("acks", "all")
        .set("max.in.flight.requests.per.connection", "5")
        .set("linger.ms", "20");
    cfg
}

pub struct RestoreProducer {
    producer: FutureProducer,
    bootstrap_servers: String,
    topic: String,
    partition: i32,
    keys: ColumnType,
    values: ColumnType,
    watermark_timeout: Duration,
    window: usize,
    in_flight: VecDeque<(u64, DeliveryFuture)>,
}

impl RestoreProducer {
    /// `keys` and `values` are the mirror's column types, which every
    /// key and value must satisfy before it is sent.
    pub fn open(
        bootstrap_servers: impl Into<String>,
        topic: impl Into<String>,
        partition: i32,
        keys: ColumnType,
        values: ColumnType,
    ) -> Result<Self, KafkaError> {
        let bootstrap_servers = bootstrap_servers.into();
        let producer: FutureProducer = restore_producer_config(&bootstrap_servers)
            .create()
            .map_err(|e| KafkaError::Init(e.to_string()))?;
        Ok(Self {
            producer,
            bootstrap_servers,
            topic: topic.into(),
            partition,
            keys,
            values,
            watermark_timeout: crate::DEFAULT_WATERMARK_TIMEOUT,
            window: DEFAULT_WINDOW,
            in_flight: VecDeque::new(),
        })
    }

    /// The partition's high watermark, from the broker.
    pub async fn high_watermark(&self) -> Result<u64, String> {
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
    /// offset.
    pub async fn send(&mut self, record: &Record, offset: u64) -> Result<(), String> {
        self.keys.validate("key", offset, record.key.as_deref())?;
        self.values
            .validate("value", offset, record.value.as_deref())?;
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
    pub async fn finish(&mut self) -> Result<(), String> {
        while !self.in_flight.is_empty() {
            self.ack_oldest().await?;
        }
        self.producer
            .flush(Timeout::After(self.watermark_timeout))
            .map_err(|e| format!("flush: {e}"))
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
}

#[cfg(test)]
mod tests {
    use mirror_core::{ColumnType, Record, TimestampType};

    use super::RestoreProducer;

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

    /// Checked before anything is sent: no broker is needed.
    #[tokio::test]
    async fn keys_and_values_are_checked_against_the_column_types() {
        let mut p = RestoreProducer::open(
            "localhost:1",
            "t",
            0,
            ColumnType::Utf8,
            ColumnType::JsonParseable,
        )
        .unwrap();
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
        let mut p =
            RestoreProducer::open("localhost:1", "t", 0, ColumnType::Bytes, ColumnType::Bytes)
                .unwrap();
        // Bytes are not checked; nothing is awaited before the send is queued.
        p.send(&record(&[0xff], &[0xfe]), 0).await.unwrap();
    }

    #[test]
    fn restore_produces_idempotently_with_retries_and_batching() {
        let native = super::restore_producer_config("localhost:9092")
            .create_native_config()
            .expect("native config");
        let get = |k: &str| native.get(k).unwrap();
        assert_eq!(get("enable.idempotence"), "true");
        assert_eq!(get("acks"), "-1", "acks=all");
        assert_eq!(get("max.in.flight.requests.per.connection"), "5");
        assert_ne!(get("message.send.max.retries"), "0");
        assert_eq!(get("enable.gapless.guarantee"), "true");
        assert_eq!(get("linger.ms"), "20");
    }
}
