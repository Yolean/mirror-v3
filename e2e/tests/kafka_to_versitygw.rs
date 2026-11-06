//! E2e: kafka-native → mirror-v3 (S3 sink) → VersityGW POSIX backend.
//!
//! Doubles as the Phase 4 VersityGW compatibility spike: it exercises
//! `PutMode::Create` against a real VersityGW. If VersityGW silently
//! treats it as a regular PUT, the *first* mirror still produces a
//! valid, non-overlapping chain (because it's single-writer), and the
//! `corrupt_chain_is_rejected_on_open` invariant in `mirror-s3`'s
//! unit tests covers the detection path on restart.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use mirror_core::Sink;
use mirror_e2e::docker::{KafkaNativeToVersityGWStack, VERSITYGW_ACCESS_KEY, VERSITYGW_SECRET_KEY};
use mirror_e2e::kafka_helpers::{create_topic, produce_records};
use mirror_e2e::mirror_runner::{spawn_kafka_to_s3, S3MirrorSpec};
use mirror_e2e::ProvisionedStack;
use mirror_envelope::Format;
use mirror_s3::{FlushTriggers, S3Sink, S3SinkConfig};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;

const BUCKET: &str = "mirror-v3";
const TOPIC: &str = "mirror-e2e-s3";
const N: usize = 50;

fn store(endpoint: &str) -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_region("us-east-1")
            .with_bucket_name(BUCKET)
            .with_access_key_id(VERSITYGW_ACCESS_KEY)
            .with_secret_access_key(VERSITYGW_SECRET_KEY)
            .build()
            .expect("build S3 client"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mirrors_to_versitygw_with_offset_named_objects() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    let stack = KafkaNativeToVersityGWStack::start(BUCKET)
        .await
        .expect("provision stack");
    let source = stack.source_bootstrap();
    let endpoint = stack.s3_endpoint();
    tracing::info!(source = %source, endpoint = %endpoint, "stack ready");

    create_topic(&source, TOPIC, 1).await.expect("topic");

    let fixtures: Vec<(String, String)> = (0..N)
        .map(|i| (format!("k{i:04}"), format!("v{i:04}")))
        .collect();
    produce_records(&source, TOPIC, 0, &fixtures)
        .await
        .expect("produce");

    let flush = FlushTriggers {
        max_time: Duration::from_secs(3600),
        max_bytes: u64::MAX,
        max_offsets: 10,
        daily_at_utc_seconds: None,
    };
    let s3 = store(&endpoint);
    let mirror = spawn_kafka_to_s3(S3MirrorSpec {
        source_bootstrap: source.clone(),
        source_topic: TOPIC.into(),
        partition: 0,
        group_id: "mirror-e2e-s3".into(),
        store: Arc::clone(&s3),
        prefix: Some(Path::from("archive")),
        destination_name: "ops".into(),
        format: mirror_envelope::Format::Ndjson,
        compression: mirror_envelope::ParquetCompression::Zstd1,
        keys: mirror_envelope::ColumnType::Utf8,
        values: mirror_envelope::ColumnType::Utf8,
        compaction: None,
        cache: None,
        flush,
        encryption: None,
    })
    .await
    .expect("spawn mirror");

    // Poll the bucket for 5 batches of 10 records.
    let prefix = Path::from("archive/ops/0");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let names = list_names(s3.as_ref(), &prefix).await;
        if names.len() >= 5 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for 5 objects; got {}: {:?}",
            names.len(),
            names
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    mirror.abort();

    let names = list_names(s3.as_ref(), &prefix).await;
    let expected: Vec<String> = (0..5)
        .map(|i| {
            let from = i * 10;
            let to = from + 9;
            format!("{from:020}-{to:020}.ndjson")
        })
        .collect();
    assert_eq!(names, expected);

    // Read every record back, verify byte-identical content + offsets.
    let mut records = Vec::new();
    for name in &names {
        let path = Path::from(format!("archive/ops/0/{name}"));
        let result = s3.get(&path).await.expect("get");
        let bytes = result.bytes().await.expect("bytes");
        records.extend(mirror_envelope::decode_batch(Format::Ndjson, &bytes).expect("decode"));
    }
    assert_eq!(records.len(), N);
    for (i, rec) in records.iter().enumerate() {
        assert_eq!(rec.source_offset, i as u64);
        assert_eq!(rec.key.as_deref(), Some(format!("k{i:04}").as_bytes()));
        assert_eq!(rec.value.as_deref(), Some(format!("v{i:04}").as_bytes()));
    }

    // Restart correctness: a fresh S3Sink reports next-expected = N.
    let mut restarted = S3Sink::open(S3SinkConfig {
        read_store: Arc::clone(&s3),
        write_store: Arc::clone(&s3),
        prefix: Some(Path::from("archive")),
        destination_name: "ops".into(),
        partition: 0,
        format: mirror_envelope::Format::Ndjson,
        compression: mirror_envelope::ParquetCompression::Zstd1,
        keys: mirror_envelope::ColumnType::Utf8,
        values: mirror_envelope::ColumnType::Utf8,
        compaction: None,
        encryption: None,
        flush,
        lock: None,
    })
    .await
    .expect("reopen");
    assert_eq!(restarted.next_expected_offset().await.unwrap(), N as u64);
}

async fn list_names(store: &dyn ObjectStore, prefix: &Path) -> Vec<String> {
    let mut stream = store.list(Some(prefix));
    let mut names = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta.unwrap();
        if let Some(name) = meta.location.filename() {
            names.push(name.to_string());
        }
    }
    names.sort();
    names
}

/// Direct spike: does VersityGW honor `If-None-Match: *` (PutMode::Create)
/// on its POSIX backend? Writes the same object twice and checks the
/// second write's outcome. We don't fail the suite if it overwrites —
/// the scan-validate layer in mirror-s3 still keeps us correct — we
/// just want a black-box answer recorded in CI output.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn versitygw_conditional_put_spike() {
    use bytes::Bytes;
    use object_store::{PutMode, PutOptions, PutPayload};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    let stack = KafkaNativeToVersityGWStack::start(BUCKET)
        .await
        .expect("provision stack");
    let s3 = store(&stack.s3_endpoint());
    let key = Path::from("conditional-spike/object");

    s3.put(&key, PutPayload::from(Bytes::from_static(b"first")))
        .await
        .expect("first put");

    let second = s3
        .put_opts(
            &key,
            PutPayload::from(Bytes::from_static(b"second")),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await;

    let behavior = match second {
        Ok(_) => "OVERWROTE-SILENTLY",
        Err(object_store::Error::AlreadyExists { .. })
        | Err(object_store::Error::Precondition { .. }) => "REJECTED",
        Err(other) => {
            tracing::error!("unexpected error: {other:?}");
            "UNKNOWN"
        }
    };
    tracing::info!(versitygw_conditional_put = behavior, "spike result");
    println!("VersityGW PutMode::Create behaviour: {behavior}");
    // This test is informational; it does not fail. mirror-s3's
    // scan-validate layer is the universal correctness backstop.
}

/// Trim tells an object's age from the record times the mirror writes as
/// user metadata (`x-amz-meta-*`), with a HEAD: VersityGW must keep
/// them, and the floor marker and deletes must work through its API.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn versitygw_keeps_record_times_and_trim_moves_the_floor() {
    use mirror_core::{Record, TimestampType};
    use mirror_fs::blob::BlobStore;
    use mirror_restore::{trim, FloorTarget, TrimConfig};

    let stack = KafkaNativeToVersityGWStack::start(BUCKET)
        .await
        .expect("provision stack");
    let s3 = store(&stack.s3_endpoint());
    let cfg = || S3SinkConfig {
        read_store: Arc::clone(&s3),
        write_store: Arc::clone(&s3),
        prefix: Some(Path::from("trim")),
        destination_name: "audit".into(),
        partition: 0,
        format: Format::Parquet,
        compression: mirror_envelope::ParquetCompression::Zstd1,
        keys: mirror_envelope::ColumnType::Utf8,
        values: mirror_envelope::ColumnType::Utf8,
        compaction: None,
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets: 2,
            daily_at_utc_seconds: None,
        },
        encryption: None,
        lock: None,
    };
    let t0: i64 = 1_700_000_000_000;
    let mut sink = S3Sink::open(cfg()).await.expect("open");
    for o in 0..6u64 {
        sink.write(Record {
            topic: "audit".into(),
            partition: 0,
            source_offset: o,
            timestamp_ms: Some(t0 + o as i64 * 1000),
            timestamp_type: TimestampType::CreateTime,
            key: Some(format!("k{o}").into_bytes()),
            value: Some(b"v".to_vec()),
            headers: vec![],
        })
        .await
        .expect("write");
    }
    drop(sink);

    let dir = mirror_s3::partition_prefix(Some(&Path::from("trim")), "audit", 0);
    let store = mirror_s3::S3Store::new(Arc::clone(&s3), Arc::clone(&s3), dir);
    let times = store
        .record_times(&mirror_fs::naming::batch_filename(2, 3, "parquet"))
        .await
        .expect("head");
    assert_eq!(
        times.and_then(|t| t.max_ms),
        Some((t0 + 3000) as u64),
        "VersityGW dropped the record-time metadata: {times:?}"
    );

    let report = trim(TrimConfig {
        store: &store,
        format: Format::Parquet,
        target: FloorTarget::Cutoff {
            cutoff_ms: (t0 + 3500) as u64,
        },
        now: 1_800_000_000,
        dry_run: false,
    })
    .await
    .expect("trim");
    assert_eq!(report.floor, 4);
    assert_eq!(report.deleted.len(), 2);
    assert!(report.failed.is_empty(), "{:?}", report.failed);

    let mut reopened = S3Sink::open(cfg()).await.expect("reopen after trim");
    assert_eq!(reopened.next_expected_offset().await.unwrap(), 6);
    assert_eq!(reopened.floor().map(|f| f.offset), Some(4));
}
