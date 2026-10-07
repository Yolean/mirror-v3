//! E2e: a topic backed up by the mirror to a filesystem or S3
//! destination and restored from it into another topic, against real
//! brokers.
//!
//! - `preserve`: every record lands at its original offset with its
//!   key, value (tombstones too), headers and timestamp; a second
//!   restore into the now non-empty topic is refused; a target topic
//!   that does not exist is an error and is not created.
//! - holes: a source written in transactions has an offset hole at each
//!   commit marker, which the backup keeps. `preserve` refuses it
//!   before producing anything; `renumber` restores every record in
//!   order at 0, 1, 2, ...
//! - S3: a backup encrypted across a key rotation, restored from the
//!   bucket through a read-only store, on kafka-native and VersityGW,
//!   and, `#[ignore]`d, on the broker and S3 endpoint the environment
//!   names (see its test).
//! - follow: a backup read as a mirror's source while its own mirror
//!   writes it, into a topic that keeps up; stopped, and resumed at the
//!   topic's high watermark.
//!
//! The others run against the Docker stack (kafka-native source,
//! Redpanda target), and, `#[ignore]`d, against any broker named by
//! `MIRROR_E2E_EXTERNAL_KAFKA=<bootstrap>` (one cluster for source and
//! target, as on a host without Docker):
//!
//!     MIRROR_E2E_EXTERNAL_KAFKA=localhost:9092 \
//!       cargo test -p mirror-e2e --test restore_roundtrip -- --ignored

use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use std::sync::Arc;

use mirror_e2e::docker::{
    KafkaNativeToRedpandaStack, KafkaNativeToVersityGWStack, VERSITYGW_ACCESS_KEY,
    VERSITYGW_SECRET_KEY,
};
use mirror_e2e::kafka_helpers::create_topic;
use mirror_e2e::mirror_runner::{
    spawn_kafka_to_filesystem, spawn_kafka_to_s3, FsMirrorSpec, MirrorHandle, S3MirrorSpec,
};
use mirror_e2e::ProvisionedStack;
use mirror_envelope::{ColumnType, Format, Keyring, ParquetCompression};
use mirror_fs::blob::BlobStore;
use mirror_fs::{read_all_records, BlobEncryption, FlushTriggers, FsStore};
use mirror_kafka::{KafkaSink, KafkaSinkConfig, RestoreProducer};
use mirror_restore::{
    plan_chain, produce, BackupSource, ChainSource, ChainSourceConfig, OffsetMode, Reader,
    RestoreError,
};
use mirror_s3::S3Store;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
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

/// Start the mirror that backs `topic` up to `root/ops/0`.
fn spawn_backup(source: &str, topic: &str, root: &Path) -> Result<MirrorHandle> {
    spawn_kafka_to_filesystem(FsMirrorSpec {
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
    })
}

/// Back `topic` up to `root/ops/0` with the mirror and wait until the
/// backup holds `records` records.
async fn back_up(source: &str, topic: &str, root: &Path, records: usize) -> Result<()> {
    let mirror = spawn_backup(source, topic, root)?;
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

async fn wait_for_high_watermark(bootstrap: &str, topic: &str, records: i64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let held = high_watermark(bootstrap, topic).unwrap();
        if held == records {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{topic} holds {held} of {records} records"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The backup in `root/ops/0` as a mirror's source.
async fn chain_source(root: &Path, source_topic: &str) -> ChainSource<FsStore> {
    let dir = root.join("ops").join("0");
    ChainSource::open(ChainSourceConfig {
        location: dir.display().to_string(),
        store: Arc::new(FsStore::new(dir)),
        format: Format::Parquet,
        keyring: None,
        source: BackupSource {
            topic: source_topic.to_string(),
            partition: 0,
        },
        mode: OffsetMode::Preserve,
        chain_start: 0,
        poll_interval: Duration::from_millis(100),
    })
    .await
    .unwrap()
}

/// `run_mirror` from `source` into `target_topic`, through the Kafka
/// destination, until the returned sender fires.
fn start_follow(
    source: ChainSource<FsStore>,
    target: &str,
    target_topic: &str,
) -> (
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), mirror_core::MirrorError>>,
) {
    let sink = KafkaSink::open(KafkaSinkConfig::new(target, target_topic, 0)).unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let run = tokio::spawn(mirror_core::run_mirror(source, sink, async move {
        let _ = stopped.await;
    }));
    (stop, run)
}

/// Follow a backup while its mirror writes it, stop, and resume.
async fn follow(brokers: &Brokers) {
    install_tracing();
    let source_topic = unique("restore-follow-src");
    create_topic(&brokers.source, &source_topic, 1)
        .await
        .unwrap();
    let target_topic = unique("restore-follow-dst");
    create_topic(&brokers.target, &target_topic, 1)
        .await
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    // Nothing backed up yet: the follower waits for the first object.
    let backup = spawn_backup(&brokers.source, &source_topic, root.path()).unwrap();
    let (stop, run) = start_follow(
        chain_source(root.path(), &source_topic).await,
        &brokers.target,
        &target_topic,
    );
    produce_numbered(&brokers.source, &source_topic, 0, 30).await;
    wait_for_high_watermark(&brokers.target, &target_topic, 30).await;
    produce_numbered(&brokers.source, &source_topic, 30, 20).await;
    wait_for_high_watermark(&brokers.target, &target_topic, 50).await;
    stop.send(()).unwrap();
    run.await.unwrap().unwrap();

    // Records backed up while no follower runs; then one resumes, at
    // the target's high watermark, after the check the CLI makes.
    produce_numbered(&brokers.source, &source_topic, 50, 15).await;
    let mut source = chain_source(root.path(), &source_topic).await;
    let last = mirror_kafka::read_record_at(
        &brokers.target,
        &target_topic,
        0,
        49,
        Duration::from_secs(10),
    )
    .unwrap();
    source.check_resume(50, &last).await.unwrap();
    let (stop, run) = start_follow(source, &brokers.target, &target_topic);
    wait_for_high_watermark(&brokers.target, &target_topic, 65).await;
    stop.send(()).unwrap();
    run.await.unwrap().unwrap();
    backup.shutdown().await.unwrap();

    let original = consume_all(&brokers.source, &source_topic).unwrap();
    assert_eq!(original.len(), 65);
    assert_eq!(
        consume_all(&brokers.target, &target_topic).unwrap(),
        original
    );
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
    restore_from(&store, None, source_topic, target, target_topic, mode).await
}

/// Verify the backup in `store` and restore it to `target_topic`.
async fn restore_from<S: BlobStore>(
    store: &S,
    keyring: Option<&Keyring>,
    source_topic: &str,
    target: &str,
    target_topic: &str,
    mode: OffsetMode,
) -> Result<mirror_restore::RestoreReport, RestoreError> {
    let names = store.list().await.expect("list");
    let chain = plan_chain(&names, Format::Parquet, 0, keyring)?;
    let source = BackupSource {
        topic: source_topic.to_string(),
        partition: 0,
    };
    let reader = Reader {
        store,
        format: Format::Parquet,
        keyring,
        source: &source,
    };
    let summary = reader.verify(&chain).await?;
    let mut producer = RestoreProducer::open(
        target,
        target_topic,
        0,
        mirror_core::ColumnType::Utf8,
        mirror_core::ColumnType::Utf8,
    )
    .expect("producer");
    produce(&reader, &chain, &summary, mode, &mut producer).await
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

/// A topic backed up to S3 by the mirror, encrypted with k1 and, after a
/// restart with another active key, k2, restored from the bucket through
/// a store that may only read: the disaster-recovery path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_encrypted_s3_backup_restores_across_a_key_rotation() {
    const BUCKET: &str = "mirror-v3";
    let stack = KafkaNativeToVersityGWStack::start(BUCKET)
        .await
        .expect("provision the Docker stack");
    let s3 = s3_store(
        &stack.s3_endpoint(),
        BUCKET,
        VERSITYGW_ACCESS_KEY,
        VERSITYGW_SECRET_KEY,
    );
    s3_key_rotation(&stack.source_bootstrap(), s3).await;
}

/// The same against a broker and an S3 endpoint the environment names,
/// for a host without Docker (VersityGW runs as a single binary):
///
///     MIRROR_E2E_EXTERNAL_KAFKA=localhost:9092 \
///     MIRROR_E2E_EXTERNAL_S3=http://localhost:7070 \
///       cargo test -p mirror-e2e --test restore_roundtrip external_an_encrypted -- --ignored
///
/// The bucket (`MIRROR_E2E_EXTERNAL_S3_BUCKET`, default `mirror-v3`)
/// must exist; the credentials default to the Docker stack's
/// (`MIRROR_E2E_EXTERNAL_S3_ACCESS_KEY_ID`,
/// `MIRROR_E2E_EXTERNAL_S3_SECRET_ACCESS_KEY`). Every run writes under
/// a prefix of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs MIRROR_E2E_EXTERNAL_KAFKA=<bootstrap> and MIRROR_E2E_EXTERNAL_S3=<endpoint>"]
async fn external_an_encrypted_s3_backup_restores_across_a_key_rotation() {
    let env = |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.into());
    let endpoint = std::env::var("MIRROR_E2E_EXTERNAL_S3")
        .expect("MIRROR_E2E_EXTERNAL_S3=<endpoint> names the S3 endpoint");
    let s3 = s3_store(
        &endpoint,
        &env("MIRROR_E2E_EXTERNAL_S3_BUCKET", "mirror-v3"),
        &env("MIRROR_E2E_EXTERNAL_S3_ACCESS_KEY_ID", VERSITYGW_ACCESS_KEY),
        &env(
            "MIRROR_E2E_EXTERNAL_S3_SECRET_ACCESS_KEY",
            VERSITYGW_SECRET_KEY,
        ),
    );
    s3_key_rotation(&external().source, s3).await;
}

fn s3_store(endpoint: &str, bucket: &str, key_id: &str, secret: &str) -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_region("us-east-1")
            .with_bucket_name(bucket)
            .with_access_key_id(key_id)
            .with_secret_access_key(secret)
            .build()
            .expect("S3 client"),
    )
}

async fn s3_key_rotation(broker: &str, s3: Arc<dyn ObjectStore>) {
    install_tracing();
    let broker = broker.to_string();
    let keys_dir = tempfile::tempdir().unwrap();
    std::fs::write(
        keys_dir.path().join("k1"),
        "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
    )
    .unwrap();
    std::fs::write(
        keys_dir.path().join("k2"),
        "AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=",
    )
    .unwrap();
    let keyring = Arc::new(Keyring::load(keys_dir.path()).unwrap());
    let prefix = object_store::path::Path::from(unique("e2e/restore"));
    let dir = mirror_s3::partition_prefix(Some(&prefix), "ops", 0);
    let backup = S3Store::read_only(Arc::clone(&s3), dir);

    let source_topic = unique("restore-s3-src");
    create_topic(&broker, &source_topic, 1).await.unwrap();
    let mut written = 0;
    for (key_id, count) in [("k1", 25), ("k2", 15)] {
        produce_numbered(&broker, &source_topic, written, count).await;
        written += count;
        let mirror = spawn_kafka_to_s3(S3MirrorSpec {
            source_bootstrap: broker.clone(),
            source_topic: source_topic.clone(),
            partition: 0,
            group_id: unique("mirror-e2e-restore-s3"),
            store: Arc::clone(&s3),
            prefix: Some(prefix.clone()),
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
            encryption: Some(BlobEncryption {
                key_id: key_id.into(),
                keyring: Arc::clone(&keyring),
            }),
        })
        .await
        .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let names = backup.list().await.unwrap();
            let held = if names.is_empty() {
                0
            } else {
                let chain = plan_chain(&names, Format::Parquet, 0, Some(&keyring)).unwrap();
                chain.last().map_or(0, |o| o.to + 1)
            };
            if held == written {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the backup holds {held} of {written} records"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        mirror.shutdown().await.unwrap();
    }
    let names = backup.list().await.unwrap();
    for id in ["k1", "k2"] {
        assert!(
            names.iter().any(|n| n.contains(&format!(".k-{id}."))),
            "{names:?}"
        );
    }

    // Without k2 the chain is refused from the names alone.
    let only_k1 = tempfile::tempdir().unwrap();
    std::fs::copy(keys_dir.path().join("k1"), only_k1.path().join("k1")).unwrap();
    let only_k1 = Keyring::load(only_k1.path()).unwrap();
    let err = plan_chain(&names, Format::Parquet, 0, Some(&only_k1)).unwrap_err();
    assert!(err.to_string().contains("k2"), "{err}");

    let target_topic = unique("restore-s3-dst");
    create_topic(&broker, &target_topic, 1).await.unwrap();
    let report = restore_from(
        &backup,
        Some(&keyring),
        &source_topic,
        &broker,
        &target_topic,
        OffsetMode::Preserve,
    )
    .await
    .unwrap();
    assert_eq!((report.records, report.high_watermark), (40, 40));
    let original = consume_all(&broker, &source_topic).unwrap();
    assert_eq!(original.len(), 40);
    assert_eq!(consume_all(&broker, &target_topic).unwrap(), original);
}

/// Produce records `first..first + count` to partition 0, a tombstone
/// on every fourth.
async fn produce_numbered(bootstrap: &str, topic: &str, first: u64, count: u64) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("acks", "all")
        .create()
        .unwrap();
    for i in first..first + count {
        let key = format!("k{}", i % 7);
        let value = (i % 4 != 0).then(|| format!("{{\"n\":{i}}}"));
        let headers = OwnedHeaders::new().insert(Header {
            key: "trace",
            value: Some(format!("t{i}").as_bytes()),
        });
        producer
            .send(
                record(topic, i as usize, &key, value.as_deref(), headers),
                Timeout::After(Duration::from_secs(10)),
            )
            .await
            .map_err(|(e, _)| e)
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follow_keeps_a_topic_restored_while_the_backup_grows_and_resumes() {
    follow(&docker().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs MIRROR_E2E_EXTERNAL_KAFKA=<bootstrap>"]
async fn external_follow_keeps_a_topic_restored_while_the_backup_grows_and_resumes() {
    follow(&external()).await;
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
