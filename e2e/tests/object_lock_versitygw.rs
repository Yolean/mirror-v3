//! The retention lock against VersityGW with object lock and versioning:
//! each object is locked COMPLIANCE from its record time in the PUT that
//! creates it, a backfill older than `lock-days` is not locked, a record
//! without a timestamp locks from the write, a bucket without object lock
//! is refused at open, and trim deletes expired objects by version id
//! (a DELETE without one only adds a delete marker) and locks its floor
//! marker.

use std::sync::Arc;
use std::time::Duration;

use mirror_core::{Record, Sink, TimestampType};
use mirror_e2e::docker::{VersityGWObjectLockStack, VERSITYGW_ACCESS_KEY, VERSITYGW_SECRET_KEY};
use mirror_envelope::{ColumnType, Format, ParquetCompression};
use mirror_fs::blob::{BlobError, BlobStore};
use mirror_fs::floor::utc_rfc3339;
use mirror_fs::naming;
use mirror_restore::{trim, FloorTarget, TrimConfig};
use mirror_s3::{FlushTriggers, S3Api, S3Sink, S3SinkConfig, S3Store, SinkLock};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;

const LOCK_BUCKET: &str = "locked-backup";
const PLAIN_BUCKET: &str = "plain-backup";
const LOCK_DAYS: u32 = 2;
const MAX_AGE_DAYS: u64 = 3;
const DAY: u64 = 86_400;

fn store(endpoint: &str, bucket: &str) -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_region("us-east-1")
            .with_bucket_name(bucket)
            .with_access_key_id(VERSITYGW_ACCESS_KEY)
            .with_secret_access_key(VERSITYGW_SECRET_KEY)
            .build()
            .expect("build S3 client"),
    )
}

fn api(endpoint: &str, bucket: &str) -> Arc<S3Api> {
    Arc::new(
        S3Api::new(
            Some(endpoint),
            "us-east-1",
            bucket,
            VERSITYGW_ACCESS_KEY.into(),
            VERSITYGW_SECRET_KEY.into(),
        )
        .expect("S3 API"),
    )
}

fn cfg(endpoint: &str, bucket: &str) -> S3SinkConfig {
    let s3 = store(endpoint, bucket);
    S3SinkConfig {
        read_store: Arc::clone(&s3),
        write_store: s3,
        prefix: Some(Path::from("lock")),
        destination_name: "audit".into(),
        partition: 0,
        format: Format::Parquet,
        compression: ParquetCompression::Zstd1,
        keys: ColumnType::Utf8,
        values: ColumnType::Utf8,
        compaction: None,
        encryption: None,
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets: 2,
            daily_at_utc_seconds: None,
        },
        lock: Some(SinkLock {
            days: LOCK_DAYS,
            api: api(endpoint, bucket),
        }),
    }
}

fn rec(offset: u64, timestamp_ms: Option<u64>) -> Record {
    Record {
        topic: "audit".into(),
        partition: 0,
        source_offset: offset,
        timestamp_ms: timestamp_ms.map(|t| t as i64),
        timestamp_type: TimestampType::CreateTime,
        key: Some(format!("k{offset}").into_bytes()),
        value: Some(b"v".to_vec()),
        headers: vec![],
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn key(name: &str) -> String {
    format!("lock/audit/0/{name}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn versitygw_locks_from_record_time_and_trim_deletes_versions_by_id() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
    let stack = VersityGWObjectLockStack::start(LOCK_BUCKET, PLAIN_BUCKET)
        .await
        .expect("provision versitygw");
    let endpoint = stack.s3_endpoint();
    let lock = api(&endpoint, LOCK_BUCKET);

    // 4. A bucket without object lock is refused at open, with the reason.
    let err = S3Sink::open(cfg(&endpoint, PLAIN_BUCKET))
        .await
        .err()
        .expect("a bucket without object lock is refused");
    assert!(matches!(err, BlobError::Lock(_)), "{err:?}");
    assert!(err.to_string().contains("has no object lock"), "{err}");

    let start = now();
    let backfill_ms = (start - (LOCK_DAYS as u64 + 2) * DAY) * 1000;
    let recent_ms = (start - DAY) * 1000;
    let mut sink = S3Sink::open(cfg(&endpoint, LOCK_BUCKET))
        .await
        .expect("open on a bucket with object lock");
    // 0-1: a backfill older than lock-days; 2-3: recent; 4-5: a record
    // without a timestamp.
    for (offset, ts) in [
        (0, Some(backfill_ms)),
        (1, Some(backfill_ms + 1000)),
        (2, Some(recent_ms - 1000)),
        (3, Some(recent_ms)),
        (4, None),
        (5, Some(recent_ms)),
    ] {
        sink.write(rec(offset, ts)).await.expect("write");
    }
    let written = now();
    drop(sink);
    let (a, b, c) = (
        naming::batch_filename(0, 1, "parquet"),
        naming::batch_filename(2, 3, "parquet"),
        naming::batch_filename(4, 5, "parquet"),
    );

    // 2. The backfill has no lock.
    assert_eq!(lock.object_retention(&key(&a)).await.unwrap(), None);
    // 1. A new object is COMPLIANCE until its newest record + lock-days.
    let rb = lock
        .object_retention(&key(&b))
        .await
        .unwrap()
        .expect("locked");
    assert_eq!(rb.mode, "COMPLIANCE");
    assert_eq!(
        utc_rfc3339(rb.retain_until),
        utc_rfc3339(recent_ms / 1000 + LOCK_DAYS as u64 * DAY)
    );
    // 3. A record without a timestamp: locked from the write.
    let rc = lock
        .object_retention(&key(&c))
        .await
        .unwrap()
        .expect("locked");
    assert!(
        (start + LOCK_DAYS as u64 * DAY..=written + LOCK_DAYS as u64 * DAY + 1)
            .contains(&rc.retain_until),
        "{rc}"
    );
    // The lock holds: the locked version does not delete.
    let versions = lock.list_object_versions("lock/audit/0/").await.unwrap();
    let version_of = |name: &str| {
        versions
            .iter()
            .find(|v| v.key == key(name) && !v.delete_marker)
            .expect("a version")
            .version_id
            .clone()
    };
    let err = lock
        .delete_object_version(&key(&b), &version_of(&b))
        .await
        .unwrap_err();
    assert_eq!(err.status, Some(403), "{err}");

    // Trim at max-age-days: the backfill expires, and on a versioned
    // bucket it is deleted by version id.
    let s3 = store(&endpoint, LOCK_BUCKET);
    let dir = mirror_s3::partition_prefix(Some(&Path::from("lock")), "audit", 0);
    let trim_store = S3Store::new(Arc::clone(&s3), s3, dir)
        .with_api(Arc::clone(&lock))
        .with_lock(LOCK_DAYS);
    let trim_now = now();
    let report = trim(TrimConfig {
        store: &trim_store,
        format: Format::Parquet,
        target: FloorTarget::Cutoff {
            cutoff_ms: (trim_now - MAX_AGE_DAYS * DAY) * 1000,
        },
        now: trim_now,
        dry_run: false,
    })
    .await
    .expect("trim");
    assert_eq!(report.floor, 2);
    assert!(report.versioned);
    assert_eq!(
        report.deleted,
        vec![format!("{a} (version {})", version_of(&a))]
    );
    assert!(report.failed.is_empty(), "{:?}", report.failed);
    let after = lock.list_object_versions("lock/audit/0/").await.unwrap();
    assert!(
        !after.iter().any(|v| v.key == key(&a)),
        "no version and no delete marker of the expired object is left: {after:?}"
    );
    // The floor marker is locked from its write.
    let marker = report.marker.expect("a marker");
    let rm = lock
        .object_retention(&key(&marker))
        .await
        .unwrap()
        .expect("the marker is locked");
    assert_eq!(rm.mode, "COMPLIANCE");
    assert_eq!(rm.retain_until, trim_now + LOCK_DAYS as u64 * DAY);
    assert_eq!(report.marker_retain_until, Some(rm.retain_until));

    let mut reopened = S3Sink::open(cfg(&endpoint, LOCK_BUCKET))
        .await
        .expect("reopen after trim");
    assert_eq!(reopened.next_expected_offset().await.unwrap(), 6);
    assert_eq!(reopened.floor().map(|f| f.offset), Some(2));
    assert_eq!(
        trim_store.list().await.unwrap().len(),
        3,
        "2-3, 4-5 and the marker"
    );
}
