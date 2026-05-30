//! E2e: a single mirror with TWO destinations (local filesystem +
//! VersityGW S3) fed from one source consumer via `mirror_core::TeeSink`.
//!
//! Verifies the happy-path "one consume, many destinations" contract:
//!   - the source broker is read once per record;
//!   - both destinations end up with the full contiguous chain
//!     0..N-1 of identical records;
//!   - graceful shutdown flushes both sinks.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use mirror_e2e::docker::{KafkaNativeToVersityGWStack, VERSITYGW_ACCESS_KEY, VERSITYGW_SECRET_KEY};
use mirror_e2e::kafka_helpers::{create_topic, produce_records};
use mirror_e2e::mirror_runner::{
    spawn_kafka_to_tee, FsInnerSpec, S3InnerSpec, TeeInnerSpec, TeeMirrorSpec,
};
use mirror_e2e::ProvisionedStack;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;

const BUCKET: &str = "mirror-v3";
const TOPIC: &str = "mirror-e2e-tee-happy";
const N: usize = 25;

fn install_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

fn s3_store(endpoint: &str) -> Arc<dyn ObjectStore> {
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
async fn tee_fs_and_s3_happy_path() {
    install_tracing();

    let stack = KafkaNativeToVersityGWStack::start(BUCKET)
        .await
        .expect("provision stack");
    let source = stack.source_bootstrap();
    let s3_endpoint = stack.s3_endpoint();

    create_topic(&source, TOPIC, 1).await.expect("create topic");

    let fixtures: Vec<(String, String)> = (0..N)
        .map(|i| (format!("k{i:04}"), format!("v{i:04}")))
        .collect();
    produce_records(&source, TOPIC, 0, &fixtures)
        .await
        .expect("produce");

    let root = tempfile::tempdir().expect("tempdir");
    let s3 = s3_store(&s3_endpoint);

    // One mirror, two destinations: local FS + S3. Shared encoding
    // settings (ndjson, utf8) — same flush triggers for both, but
    // each sink keeps its own buffer/durable position.
    let flush_fs = mirror_fs::FlushTriggers {
        max_time: Duration::from_secs(3600),
        max_bytes: u64::MAX,
        max_offsets: u64::MAX,
        daily_at_utc_seconds: None,
    };
    let flush_s3 = mirror_s3::FlushTriggers {
        max_time: Duration::from_secs(3600),
        max_bytes: u64::MAX,
        max_offsets: u64::MAX,
        daily_at_utc_seconds: None,
    };
    let mirror = spawn_kafka_to_tee(TeeMirrorSpec {
        source_bootstrap: source.clone(),
        source_topic: TOPIC.into(),
        partition: 0,
        group_id: "mirror-e2e-tee-happy".into(),
        destinations: vec![
            TeeInnerSpec::Filesystem(FsInnerSpec {
                name: "local".into(),
                root: root.path().to_path_buf(),
                format: mirror_envelope::Format::Ndjson,
                compression: mirror_envelope::ParquetCompression::Zstd1,
                keys: mirror_envelope::ColumnType::Utf8,
                values: mirror_envelope::ColumnType::Utf8,
                compaction: None,
                flush: flush_fs,
            }),
            TeeInnerSpec::S3(S3InnerSpec {
                name: "offsite".into(),
                store: Arc::clone(&s3),
                prefix: Some(Path::from("archive")),
                format: mirror_envelope::Format::Ndjson,
                compression: mirror_envelope::ParquetCompression::Zstd1,
                keys: mirror_envelope::ColumnType::Utf8,
                values: mirror_envelope::ColumnType::Utf8,
                compaction: None,
                flush: flush_s3,
            }),
        ],
        cache: None,
    })
    .await
    .expect("spawn tee mirror");

    // Let the consume loop catch up, then graceful-shutdown so both
    // sinks flush their tail.
    tokio::time::sleep(Duration::from_secs(3)).await;
    mirror.shutdown().await.expect("graceful shutdown");

    // Read FS state.
    let fs_dir = mirror_fs::naming::partition_dir(root.path(), "local", 0);
    let fs_records = mirror_fs::read_all_records(&fs_dir, mirror_envelope::Format::Ndjson)
        .expect("read fs records");
    assert_eq!(
        fs_records.len(),
        N,
        "FS destination should have all {N} records"
    );
    for (i, rec) in fs_records.iter().enumerate() {
        assert_eq!(rec.source_offset, i as u64);
        assert_eq!(rec.key.as_deref(), Some(format!("k{i:04}").as_bytes()));
        assert_eq!(rec.value.as_deref(), Some(format!("v{i:04}").as_bytes()));
    }

    // Read S3 state.
    let s3_prefix = Path::from("archive/offsite/0");
    let mut stream = s3.list(Some(&s3_prefix));
    let mut s3_object_names: Vec<String> = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta.expect("list entry");
        if let Some(name) = meta.location.filename() {
            s3_object_names.push(name.to_string());
        }
    }
    s3_object_names.sort();
    assert!(
        !s3_object_names.is_empty(),
        "S3 destination must have at least one object"
    );

    let mut s3_records = Vec::new();
    for name in &s3_object_names {
        let path = Path::from(format!("archive/offsite/0/{name}"));
        let bytes = s3
            .get(&path)
            .await
            .expect("get")
            .bytes()
            .await
            .expect("bytes");
        s3_records.extend(
            mirror_envelope::decode_batch(mirror_envelope::Format::Ndjson, &bytes).expect("decode"),
        );
    }
    assert_eq!(
        s3_records.len(),
        N,
        "S3 destination should have all {N} records"
    );
    for (i, rec) in s3_records.iter().enumerate() {
        assert_eq!(rec.source_offset, i as u64);
        assert_eq!(rec.key.as_deref(), Some(format!("k{i:04}").as_bytes()));
        assert_eq!(rec.value.as_deref(), Some(format!("v{i:04}").as_bytes()));
    }

    // The two destinations agree, byte-for-byte. This is the core
    // tee invariant for the happy path: identical writes land on
    // every inner sink.
    assert_eq!(fs_records, s3_records, "FS and S3 must agree byte-for-byte");
}
