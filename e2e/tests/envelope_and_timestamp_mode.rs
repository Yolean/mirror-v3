//! E2e for the Phase-2 envelope work:
//!
//! 1. Parquet round-trip via mirror-v3 against a real kafka-native
//!    source. Produces records, mirrors them through the parquet
//!    envelope, then `mirror_envelope::decode_batch`s the resulting
//!    files and asserts byte-identical content.
//!
//! 2. `timestamp-mode: source` on the Kafka destination preserves
//!    `record.timestamp_ms` exactly.
//!
//! 3. `timestamp-mode: destination` lets the destination broker
//!    stamp the record on receipt; the timestamp must differ from
//!    the producer's explicit timestamp.

use std::time::Duration;

use mirror_e2e::docker::DockerProvisioner;
use mirror_e2e::kafka_helpers::{
    create_topic, drain_partition_with_timestamps, produce_records, produce_records_with_timestamps,
};
use mirror_e2e::mirror_runner::{
    spawn_kafka_to_filesystem, spawn_kafka_to_kafka, FsMirrorSpec, MirrorSpec,
};
use mirror_e2e::{ProvisionedStack, Provisioner};
use mirror_envelope::{Format, ParquetCompression};
use mirror_fs::FlushTriggers as FsFlushTriggers;
use mirror_kafka::{KafkaSink, KafkaSinkConfig, TimestampMode};

fn install_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parquet_roundtrip_via_kafka_native_to_filesystem() {
    install_tracing();

    let stack = DockerProvisioner
        .provision()
        .await
        .expect("provision stack");
    let source = stack.source_bootstrap();
    let root = tempfile::tempdir().expect("tempdir");
    let topic = "mirror-e2e-parquet";

    create_topic(&source, topic, 1).await.expect("topic");

    // 30 records, 3 files of 10 each.
    let fixtures: Vec<(String, String)> = (0..30)
        .map(|i| {
            (
                format!("k{i:03}"),
                format!("v{i:03}-{:0pad$}", i, pad = 200),
            )
        })
        .collect();
    produce_records(&source, topic, 0, &fixtures)
        .await
        .expect("produce");

    let flush = FsFlushTriggers {
        max_time: Duration::from_secs(3600),
        max_bytes: u64::MAX,
        max_offsets: 10,
    };
    let mirror = spawn_kafka_to_filesystem(FsMirrorSpec {
        source_bootstrap: source.clone(),
        source_topic: topic.into(),
        partition: 0,
        group_id: "mirror-e2e-parquet".into(),
        root: root.path().to_path_buf(),
        destination_name: "ops".into(),
        format: Format::Parquet,
        compression: ParquetCompression::Zstd1,
        flush,
    })
    .expect("spawn mirror");

    let dir = root.path().join("ops").join("0");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let count = std::fs::read_dir(&dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| {
                        let name = e.file_name().to_string_lossy().into_owned();
                        name.ends_with(".parquet") && !name.contains(".tmp.")
                    })
                    .count()
            })
            .unwrap_or(0);
        if count >= 3 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for 3 parquet files; got {count}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    mirror.abort();

    // Filenames must use the .parquet extension.
    let mut filenames: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| !n.contains(".tmp."))
        .collect();
    filenames.sort();
    let expected: Vec<String> = (0..3)
        .map(|i| {
            let from = i * 10;
            let to = from + 9;
            format!("{from:020}-{to:020}.parquet")
        })
        .collect();
    assert_eq!(filenames, expected);

    // Decode every parquet file and check the records match the
    // fixtures byte-for-byte (and that topic / partition /
    // timestamp_type propagated from the source).
    let records = mirror_fs::read_all_records(&dir, Format::Parquet).expect("read parquet");
    assert_eq!(records.len(), 30);
    for (i, rec) in records.iter().enumerate() {
        assert_eq!(rec.source_offset, i as u64);
        assert_eq!(rec.topic, topic);
        assert_eq!(rec.partition, 0);
        assert_eq!(rec.key.as_deref(), Some(format!("k{i:03}").as_bytes()));
        assert_eq!(
            rec.value.as_deref(),
            Some(format!("v{i:03}-{:0pad$}", i, pad = 200).as_bytes())
        );
    }
}

const TS_TOPIC_SRC: &str = "mirror-e2e-ts-src";
const TS_TOPIC_DST: &str = "mirror-e2e-ts-dst";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timestamp_mode_source_preserves_record_timestamp() {
    install_tracing();

    let stack = DockerProvisioner
        .provision()
        .await
        .expect("provision stack");
    let source = stack.source_bootstrap();
    let target = stack.target_kafka_bootstrap().unwrap();

    create_topic(&source, TS_TOPIC_SRC, 1).await.expect("src");
    create_topic(&target, TS_TOPIC_DST, 1).await.expect("dst");

    // Distinct timestamps clearly in the past, so any broker-assigned
    // CreateTime (~ now) would be plainly different.
    let base = 1_500_000_000_000_i64;
    let fixtures: Vec<(String, String, i64)> = (0..5)
        .map(|i| (format!("k{i}"), format!("v{i}"), base + i as i64 * 1_000))
        .collect();
    produce_records_with_timestamps(&source, TS_TOPIC_SRC, 0, &fixtures)
        .await
        .expect("produce ts");

    let mirror = spawn_kafka_to_kafka(MirrorSpec {
        source_bootstrap: source.clone(),
        target_bootstrap: target.clone(),
        source_topic: TS_TOPIC_SRC.into(),
        target_topic: TS_TOPIC_DST.into(),
        partition: 0,
        group_id: "mirror-e2e-ts-source".into(),
    })
    .expect("spawn");

    // Wait for the destination's high watermark to reach 5.
    use mirror_e2e::kafka_helpers::wait_for_high_watermark;
    wait_for_high_watermark(&target, TS_TOPIC_DST, 0, 5, Duration::from_secs(30))
        .await
        .expect("hwm");
    mirror.abort();

    let drained =
        drain_partition_with_timestamps(&target, TS_TOPIC_DST, 0, Duration::from_secs(15))
            .expect("drain");
    assert_eq!(drained.len(), 5);
    for (i, rec) in drained.iter().enumerate() {
        let expected_ts = base + i as i64 * 1_000;
        assert_eq!(
            rec.timestamp_ms,
            Some(expected_ts),
            "record {i} timestamp was rewritten under timestamp-mode=source",
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timestamp_mode_destination_replaces_record_timestamp() {
    install_tracing();

    let stack = DockerProvisioner
        .provision()
        .await
        .expect("provision stack");
    let source = stack.source_bootstrap();
    let target = stack.target_kafka_bootstrap().unwrap();

    create_topic(&source, TS_TOPIC_SRC, 1).await.expect("src");
    create_topic(&target, TS_TOPIC_DST, 1).await.expect("dst");

    // Old timestamps from 2017. If the destination broker stamps the
    // records itself, the result will be ~ now and well in the
    // future relative to these.
    let ancient = 1_500_000_000_000_i64;
    let fixtures: Vec<(String, String, i64)> = (0..5)
        .map(|i| (format!("k{i}"), format!("v{i}"), ancient + i as i64))
        .collect();
    produce_records_with_timestamps(&source, TS_TOPIC_SRC, 0, &fixtures)
        .await
        .expect("produce ts");

    // Build the KafkaSink config directly so we can pass
    // timestamp_mode = Destination. (spawn_kafka_to_kafka uses
    // defaults; we bypass it here for this single test.)
    use mirror_core::run_mirror;
    use mirror_kafka::{KafkaSource, KafkaSourceConfig};

    let src_cfg = {
        let mut c = KafkaSourceConfig::new(
            source.clone(),
            "mirror-e2e-ts-dest".to_string(),
            TS_TOPIC_SRC.to_string(),
            0,
        );
        c.poll_timeout = Duration::from_millis(500);
        c
    };
    let mut snk_cfg = KafkaSinkConfig::new(target.clone(), TS_TOPIC_DST, 0);
    snk_cfg.timestamp_mode = TimestampMode::Destination;

    let source_h = KafkaSource::open(src_cfg).expect("open source");
    let sink = KafkaSink::open(snk_cfg).expect("open sink");
    let handle =
        tokio::spawn(async move { run_mirror(source_h, sink, std::future::pending::<()>()).await });

    use mirror_e2e::kafka_helpers::wait_for_high_watermark;
    wait_for_high_watermark(&target, TS_TOPIC_DST, 0, 5, Duration::from_secs(30))
        .await
        .expect("hwm");
    handle.abort();

    let drained =
        drain_partition_with_timestamps(&target, TS_TOPIC_DST, 0, Duration::from_secs(15))
            .expect("drain");
    assert_eq!(drained.len(), 5);
    let cutoff = ancient + 10_000_000_000; // well past 2017
    for (i, rec) in drained.iter().enumerate() {
        let ts = rec.timestamp_ms.expect("dest should carry a timestamp");
        assert!(
            ts > cutoff,
            "record {i} kept the source's ancient timestamp ({ts}); \
             timestamp-mode=destination should have re-stamped it",
        );
    }
}
