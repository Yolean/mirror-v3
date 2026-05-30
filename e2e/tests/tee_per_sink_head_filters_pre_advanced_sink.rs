//! E2e: load-bearing test for `TeeSink`'s per-sink head filter.
//!
//! Setup: pre-seed the filesystem destination with a snapshot file
//! containing records 0..=9 written verbatim (out of band, before
//! the mirror runs). Then start a tee mirror with two destinations
//! (FS + S3) against a broker that holds records 0..=19.
//!
//! Expected behaviour (the per-sink head filter is what makes this
//! work):
//!   - At `TeeSink::open`, inner heads are `{ FS: 10, S3: 0 }`.
//!   - `tee.next_expected_offset() = min(10, 0) = 0`. The consumer
//!     seeks to 0.
//!   - Records 0..=9 are presented to S3 only (FS head = 10 > 9,
//!     so the FS sink is silently skipped — this is the per-sink
//!     filter that lets a Kafka inner sink coexist with FS/S3 in
//!     the tee, and it's what we're checking here).
//!   - Records 10..=19 are presented to both.
//!   - Final state: both destinations hold records 0..=19.
//!
//! If the tee just delegated naively to inner sinks (no per-sink
//! head), the FS sink's `write(rec@0)` would crash on
//! `UnexpectedPosition { expected: 10, actual: 0 }` and the mirror
//! would abort. The fact that this test passes is the proof.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use mirror_core::{Record, TimestampType};
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
const TOPIC: &str = "mirror-e2e-tee-per-sink-head";
const PRE_SEED: usize = 10;
const TOTAL: usize = 20;

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
async fn tee_per_sink_head_filters_records_already_durable_on_one_sink() {
    install_tracing();

    let stack = KafkaNativeToVersityGWStack::start(BUCKET)
        .await
        .expect("provision stack");
    let source = stack.source_bootstrap();
    let s3_endpoint = stack.s3_endpoint();

    create_topic(&source, TOPIC, 1).await.expect("create topic");

    let fixtures: Vec<(String, String)> = (0..TOTAL)
        .map(|i| (format!("k{i:04}"), format!("v{i:04}")))
        .collect();
    produce_records(&source, TOPIC, 0, &fixtures)
        .await
        .expect("produce");

    // Pre-seed FS: write `0-9.ndjson` directly to the destination
    // directory so the FS sink starts at next-expected-offset = 10.
    let root = tempfile::tempdir().expect("tempdir");
    let dest_name = "local";
    let fs_dir = mirror_fs::naming::partition_dir(root.path(), dest_name, 0);
    std::fs::create_dir_all(&fs_dir).expect("create fs dir");
    let pre_records: Vec<Record> = (0..PRE_SEED)
        .map(|i| Record {
            topic: TOPIC.into(),
            partition: 0,
            source_offset: i as u64,
            timestamp_ms: Some(1_700_000_000_000 + i as i64),
            timestamp_type: TimestampType::CreateTime,
            key: Some(format!("k{i:04}").into_bytes()),
            value: Some(format!("v{i:04}").into_bytes()),
            headers: vec![],
        })
        .collect();
    let encoded = mirror_envelope::encode_batch(
        mirror_envelope::Format::Ndjson,
        mirror_envelope::ParquetCompression::Zstd1,
        mirror_envelope::ColumnType::Utf8,
        mirror_envelope::ColumnType::Utf8,
        &pre_records,
    )
    .expect("encode pre-seed batch");
    let pre_seed_filename = mirror_fs::naming::batch_filename(0, (PRE_SEED - 1) as u64, "ndjson");
    std::fs::write(fs_dir.join(&pre_seed_filename), &encoded).expect("write pre-seed file");

    // Sanity-check: the FS sink, opened standalone, reports
    // next-expected-offset = PRE_SEED. (Otherwise the test is
    // checking nothing.)
    let fs_check_cfg = mirror_fs::FilesystemSinkConfig {
        root: root.path().to_path_buf(),
        destination_name: dest_name.into(),
        partition: 0,
        format: mirror_envelope::Format::Ndjson,
        compression: mirror_envelope::ParquetCompression::Zstd1,
        keys: mirror_envelope::ColumnType::Utf8,
        values: mirror_envelope::ColumnType::Utf8,
        compaction: None,
        cache: None,
        flush: mirror_fs::FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets: u64::MAX,
            daily_at_utc_seconds: None,
        },
    };
    {
        use mirror_core::Sink;
        let mut fs_check =
            mirror_fs::FilesystemSink::open(fs_check_cfg).expect("open fs check sink");
        let next = fs_check
            .next_expected_offset()
            .await
            .expect("next_expected");
        assert_eq!(
            next, PRE_SEED as u64,
            "pre-seed file must put FS at offset {PRE_SEED}"
        );
    }

    let s3 = s3_store(&s3_endpoint);
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
        group_id: "mirror-e2e-tee-per-sink-head".into(),
        destinations: vec![
            TeeInnerSpec::Filesystem(FsInnerSpec {
                name: dest_name.into(),
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

    tokio::time::sleep(Duration::from_secs(3)).await;
    mirror.shutdown().await.expect("graceful shutdown");

    // FS final state: pre-seed (0..=9) + new (10..=19) = 0..=19.
    let fs_records = mirror_fs::read_all_records(&fs_dir, mirror_envelope::Format::Ndjson)
        .expect("read fs records");
    assert_eq!(
        fs_records.len(),
        TOTAL,
        "FS should have pre-seed + new = {TOTAL} records"
    );
    let fs_offsets: Vec<u64> = fs_records.iter().map(|r| r.source_offset).collect();
    let expected_offsets: Vec<u64> = (0..TOTAL as u64).collect();
    assert_eq!(
        fs_offsets, expected_offsets,
        "FS offsets must be 0..{TOTAL}"
    );

    // FS should have exactly TWO files: the pre-seed `0-9.ndjson`
    // (untouched) and a new file for the records the mirror added.
    // The mirror's per-sink filter must not have written records
    // 0..=9 again — that would create an overlap and `scan_validate`
    // would error.
    let mut fs_files: Vec<String> = std::fs::read_dir(&fs_dir)
        .expect("read fs dir")
        .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().to_string()))
        .filter(|n| n.ends_with(".ndjson") && !n.contains(".tmp."))
        .collect();
    fs_files.sort();
    assert_eq!(
        fs_files.len(),
        2,
        "FS must have exactly 2 files (pre-seed + new); got {fs_files:?}"
    );
    assert_eq!(
        fs_files[0], pre_seed_filename,
        "pre-seed file must be present and untouched"
    );
    // The new file's range starts at 10 (the next expected after pre-seed).
    let new_filename =
        mirror_fs::naming::batch_filename(PRE_SEED as u64, (TOTAL - 1) as u64, "ndjson");
    assert_eq!(
        fs_files[1], new_filename,
        "the mirror-written file must cover offsets {PRE_SEED}..{TOTAL}"
    );

    // S3 final state: the mirror saw 0..=19 (because S3's head
    // started at 0), so the S3 destination has the full range. We
    // don't enforce a specific file layout — flush triggers were
    // open, so it could be one big file.
    let s3_prefix = Path::from("archive/offsite/0");
    let mut stream = s3.list(Some(&s3_prefix));
    let mut s3_names: Vec<String> = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta.expect("list entry");
        if let Some(name) = meta.location.filename() {
            s3_names.push(name.to_string());
        }
    }
    s3_names.sort();
    let mut s3_records = Vec::new();
    for name in &s3_names {
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
        TOTAL,
        "S3 destination should have the full 0..{TOTAL}"
    );
    let s3_offsets: Vec<u64> = s3_records.iter().map(|r| r.source_offset).collect();
    assert_eq!(
        s3_offsets, expected_offsets,
        "S3 offsets must be 0..{TOTAL}"
    );
}
