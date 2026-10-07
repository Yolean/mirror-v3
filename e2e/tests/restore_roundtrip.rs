//! E2e: a topic backed up by the mirror to a filesystem destination and
//! restored from it into another topic, against real brokers.
//!
//! - `preserve`: every record lands at its original offset with its
//!   key, value (tombstones too), headers and timestamp; a second
//!   restore into the now non-empty topic is refused; a target topic
//!   that does not exist is an error and is not created.
//! - holes: a source written in transactions has an offset hole at each
//!   commit marker, which the backup keeps. `preserve` refuses it
//!   before producing anything; `renumber` restores every record in
//!   order at 0, 1, 2, ...
//!
//! Each scenario runs against the Docker stack (kafka-native source,
//! Redpanda target), and, `#[ignore]`d, against any broker named by
//! `MIRROR_E2E_EXTERNAL_KAFKA=<bootstrap>` (one cluster for source and
//! target, as on a host without Docker):
//!
//!     MIRROR_E2E_EXTERNAL_KAFKA=localhost:9092 \
//!       cargo test -p mirror-e2e --test restore_roundtrip -- --ignored

use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use mirror_e2e::docker::KafkaNativeToRedpandaStack;
use mirror_e2e::kafka_helpers::create_topic;
use mirror_e2e::mirror_runner::{spawn_kafka_to_filesystem, FsMirrorSpec};
use mirror_e2e::ProvisionedStack;
use mirror_envelope::{ColumnType, Format, ParquetCompression};
use mirror_fs::blob::BlobStore;
use mirror_fs::{read_all_records, FlushTriggers, FsStore};
use mirror_kafka::{KafkaSink, KafkaSinkConfig};
use mirror_restore::{plan_chain, produce, BackupSource, OffsetMode, Reader, RestoreError};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka::TopicPartitionList;

/// Where the scenarios run.
struct Brokers {
    /// Source of the backup mirror.
    source: String,
    /// Source written in transactions (needs a broker that serves
    /// them on one node).
    txn_source: String,
    /// Restore target.
    target: String,
    _stack: Option<KafkaNativeToRedpandaStack>,
}

async fn docker() -> Brokers {
    let stack = KafkaNativeToRedpandaStack::start()
        .await
        .expect("provision the Docker stack");
    let target = stack.target_kafka_bootstrap().expect("target bootstrap");
    Brokers {
        source: stack.source_bootstrap(),
        txn_source: target.clone(),
        target,
        _stack: Some(stack),
    }
}

fn external() -> Brokers {
    let bootstrap = std::env::var("MIRROR_E2E_EXTERNAL_KAFKA")
        .expect("MIRROR_E2E_EXTERNAL_KAFKA=<bootstrap> names the broker these tests use");
    Brokers {
        source: bootstrap.clone(),
        txn_source: bootstrap.clone(),
        target: bootstrap,
        _stack: None,
    }
}

fn install_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

fn unique(name: &str) -> String {
    format!("{name}-{}", uuid::Uuid::new_v4().simple())
}

/// A record as a consumer sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Consumed {
    offset: i64,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<(String, Option<Vec<u8>>)>,
    timestamp_ms: Option<i64>,
}

/// Every record of partition 0 up to the high watermark (holes and
/// transaction markers are not records).
fn consume_all(bootstrap: &str, topic: &str) -> Result<Vec<Consumed>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "mirror-e2e-restore-read")
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .create()
        .context("consumer")?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, rdkafka::Offset::Beginning)?;
    consumer.assign(&tpl)?;
    let (_low, high) =
        consumer.fetch_watermarks(topic, 0, Timeout::After(Duration::from_secs(10)))?;
    let mut out = Vec::new();
    if high == 0 {
        return Ok(out);
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if consumer
            .position()?
            .find_partition(topic, 0)
            .map(|p| p.offset())
            == Some(rdkafka::Offset::Offset(high))
        {
            return Ok(out);
        }
        if std::time::Instant::now() > deadline {
            return Err(anyhow!("timed out reading {topic} to {high}"));
        }
        if let Some(msg) = consumer.poll(Timeout::After(Duration::from_millis(500))) {
            let msg = msg.map_err(|e| anyhow!("poll: {e}"))?;
            out.push(Consumed {
                offset: msg.offset(),
                key: msg.key().map(<[u8]>::to_vec),
                value: msg.payload().map(<[u8]>::to_vec),
                headers: msg
                    .headers()
                    .map(|h| {
                        h.iter()
                            .map(|h| (h.key.to_string(), h.value.map(<[u8]>::to_vec)))
                            .collect()
                    })
                    .unwrap_or_default(),
                timestamp_ms: msg.timestamp().to_millis(),
            });
        }
    }
}

fn high_watermark(bootstrap: &str, topic: &str) -> Result<i64> {
    Ok(mirror_kafka::fetch_high_watermark(bootstrap, topic, 0, Duration::from_secs(10))? as i64)
}

fn topic_exists(bootstrap: &str, topic: &str) -> Result<bool> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .context("metadata consumer")?;
    let md = consumer.fetch_metadata(None, Timeout::After(Duration::from_secs(10)))?;
    Ok(md.topics().iter().any(|t| t.name() == topic))
}

fn record<'a>(
    topic: &'a str,
    i: usize,
    key: &'a str,
    value: Option<&'a str>,
    headers: OwnedHeaders,
) -> FutureRecord<'a, str, str> {
    let mut r = FutureRecord::to(topic)
        .partition(0)
        .key(key)
        .headers(headers)
        .timestamp(1_700_000_000_000 + i as i64);
    if let Some(v) = value {
        r = r.payload(v);
    }
    r
}

/// Back `topic` up to `root/ops/0` with the mirror and wait until the
/// backup holds `records` records.
async fn back_up(source: &str, topic: &str, root: &Path, records: usize) -> Result<()> {
    let mirror = spawn_kafka_to_filesystem(FsMirrorSpec {
        source_bootstrap: source.to_string(),
        source_topic: topic.to_string(),
        partition: 0,
        group_id: unique("mirror-e2e-restore"),
        root: root.to_path_buf(),
        destination_name: "ops".into(),
        format: Format::Parquet,
        compression: ParquetCompression::Zstd1,
        keys: ColumnType::Utf8,
        values: ColumnType::Utf8,
        compaction: None,
        cache: None,
        flush: FlushTriggers {
            max_time: Duration::from_millis(500),
            max_bytes: u64::MAX,
            max_offsets: 10,
            daily_at_utc_seconds: None,
        },
    })?;
    let dir = root.join("ops").join("0");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let held = if dir.is_dir() {
            read_all_records(&dir, Format::Parquet)?.len()
        } else {
            0
        };
        if held == records {
            break;
        }
        if std::time::Instant::now() > deadline {
            return Err(anyhow!("the backup holds {held} of {records} records"));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    mirror.shutdown().await
}

/// Verify the backup in `root/ops/0` and restore it to `target_topic`.
async fn restore(
    root: &Path,
    source_topic: &str,
    target: &str,
    target_topic: &str,
    mode: OffsetMode,
) -> Result<mirror_restore::RestoreReport, RestoreError> {
    let store = FsStore::new(root.join("ops").join("0"));
    let names = store.list().await.expect("list");
    let chain = plan_chain(&names, Format::Parquet, 0, None)?;
    let source = BackupSource {
        topic: source_topic.to_string(),
        partition: 0,
    };
    let reader = Reader {
        store: &store,
        format: Format::Parquet,
        keyring: None,
        source: &source,
    };
    let summary = reader.verify(&chain).await?;
    let mut sink = KafkaSink::open(KafkaSinkConfig::new(target, target_topic, 0)).expect("sink");
    produce(&reader, &chain, &summary, mode, &mut sink).await
}

async fn preserve_round_trip(brokers: &Brokers) {
    install_tracing();
    let source_topic = unique("restore-src");
    create_topic(&brokers.source, &source_topic, 1)
        .await
        .unwrap();
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers.source)
        .set("acks", "all")
        .create()
        .unwrap();
    const N: usize = 40;
    for i in 0..N {
        let key = format!("k{}", i % 7);
        let value = (i % 5 != 0).then(|| format!("{{\"n\":{i}}}"));
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "trace",
                value: Some(format!("t{i}").as_bytes()),
            })
            .insert(Header {
                key: "empty",
                value: None::<&[u8]>,
            });
        producer
            .send(
                record(&source_topic, i, &key, value.as_deref(), headers),
                Timeout::After(Duration::from_secs(10)),
            )
            .await
            .map_err(|(e, _)| e)
            .unwrap();
    }
    let root = tempfile::tempdir().unwrap();
    back_up(&brokers.source, &source_topic, root.path(), N)
        .await
        .unwrap();

    let target_topic = unique("restore-dst");
    create_topic(&brokers.target, &target_topic, 1)
        .await
        .unwrap();
    let report = restore(
        root.path(),
        &source_topic,
        &brokers.target,
        &target_topic,
        OffsetMode::Preserve,
    )
    .await
    .unwrap();
    assert_eq!(
        (report.records, report.high_watermark),
        (N as u64, N as u64)
    );
    let original = consume_all(&brokers.source, &source_topic).unwrap();
    let restored = consume_all(&brokers.target, &target_topic).unwrap();
    assert_eq!(original.len(), N);
    assert_eq!(restored, original);

    let again = restore(
        root.path(),
        &source_topic,
        &brokers.target,
        &target_topic,
        OffsetMode::Preserve,
    )
    .await
    .unwrap_err();
    assert!(matches!(again, RestoreError::TargetNotEmpty(40)), "{again}");

    let missing = unique("restore-missing");
    let err = restore(
        root.path(),
        &source_topic,
        &brokers.target,
        &missing,
        OffsetMode::Preserve,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("reading its high watermark"),
        "{err}"
    );
    assert!(!topic_exists(&brokers.target, &missing).unwrap());
}

async fn holes(brokers: &Brokers) {
    install_tracing();
    let source_topic = unique("restore-txn");
    create_topic(&brokers.txn_source, &source_topic, 1)
        .await
        .unwrap();
    // Four transactions of three records. Every transaction marker
    // takes an offset, a hole for every consumer; where they land is the
    // broker's: Apache Kafka writes a commit marker after each
    // transaction (3, 7, 11, 15), Redpanda also a control batch before
    // each (0, 4, 5, 9, ...).
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers.txn_source)
        .set("transactional.id", unique("restore-e2e"))
        .create()
        .unwrap();
    tokio::task::block_in_place(|| {
        producer.init_transactions(Timeout::After(Duration::from_secs(30)))
    })
    .unwrap();
    let mut i = 0;
    for _ in 0..4 {
        producer.begin_transaction().unwrap();
        for _ in 0..3 {
            let key = format!("user-{}", i % 4);
            let value = format!("state-{i}");
            producer
                .send(
                    record(&source_topic, i, &key, Some(&value), OwnedHeaders::new()),
                    Timeout::After(Duration::from_secs(10)),
                )
                .await
                .map_err(|(e, _)| e)
                .unwrap();
            i += 1;
        }
        tokio::task::block_in_place(|| {
            producer.commit_transaction(Timeout::After(Duration::from_secs(30)))
        })
        .unwrap();
    }
    let original = consume_all(&brokers.txn_source, &source_topic).unwrap();
    let offsets: Vec<i64> = original.iter().map(|r| r.offset).collect();
    assert_eq!(offsets.len(), 12);
    // The backup's chain covers 0 to the last record's offset.
    let last = *offsets.last().unwrap();
    let holes = last + 1 - 12;
    let first_hole = (0..).find(|o| !offsets.contains(o)).unwrap();
    assert!(holes >= 3, "a marker between each transaction: {offsets:?}");

    let root = tempfile::tempdir().unwrap();
    back_up(&brokers.txn_source, &source_topic, root.path(), 12)
        .await
        .unwrap();

    let target_topic = unique("restore-dst");
    create_topic(&brokers.target, &target_topic, 1)
        .await
        .unwrap();
    let err = restore(
        root.path(),
        &source_topic,
        &brokers.target,
        &target_topic,
        OffsetMode::Preserve,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains(&format!(
            "{holes} offset hole(s), the first at offset {first_hole}"
        )),
        "{err}"
    );
    assert_eq!(high_watermark(&brokers.target, &target_topic).unwrap(), 0);

    let report = restore(
        root.path(),
        &source_topic,
        &brokers.target,
        &target_topic,
        OffsetMode::Renumber,
    )
    .await
    .unwrap();
    assert_eq!((report.records, report.high_watermark), (12, 12));
    let restored = consume_all(&brokers.target, &target_topic).unwrap();
    let renumbered: Vec<Consumed> = original
        .into_iter()
        .enumerate()
        .map(|(i, r)| Consumed {
            offset: i as i64,
            ..r
        })
        .collect();
    assert_eq!(restored, renumbered);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preserve_restores_every_record_at_its_offset() {
    preserve_round_trip(&docker().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn holes_refuse_preserve_and_renumber_restores_in_order() {
    holes(&docker().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs MIRROR_E2E_EXTERNAL_KAFKA=<bootstrap>"]
async fn external_preserve_restores_every_record_at_its_offset() {
    preserve_round_trip(&external()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs MIRROR_E2E_EXTERNAL_KAFKA=<bootstrap>"]
async fn external_holes_refuse_preserve_and_renumber_restores_in_order() {
    holes(&external()).await;
}
