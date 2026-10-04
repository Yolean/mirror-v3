//! What an S3 destination costs in requests: its resume position comes
//! from object names, so open lists the prefix and reads no object
//! (every blob mirror used to download and decode
//! its whole archive at startup).

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::CountingStore;
use mirror_core::{Record, Sink, TimestampType};
use mirror_envelope::{Format, ParquetCompression};
use mirror_s3::{FlushTriggers, S3Sink, S3SinkConfig};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;

fn rec(offset: u64) -> Record {
    Record {
        topic: "t".into(),
        partition: 0,
        source_offset: offset,
        timestamp_ms: Some(1_700_000_000_000),
        timestamp_type: TimestampType::CreateTime,
        key: Some(format!("k{offset}").into_bytes()),
        value: Some(b"v".to_vec()),
        headers: vec![],
    }
}

fn cfg(store: Arc<dyn ObjectStore>, max_offsets: u64) -> S3SinkConfig {
    S3SinkConfig {
        store,
        prefix: Some(Path::from("archive")),
        destination_name: "ops".into(),
        partition: 0,
        format: Format::Parquet,
        compression: ParquetCompression::Zstd1,
        keys: mirror_envelope::ColumnType::Utf8,
        values: mirror_envelope::ColumnType::Utf8,
        compaction: None,
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets,
            daily_at_utc_seconds: None,
        },
    }
}

#[tokio::test]
async fn open_reads_names_not_objects() {
    let store = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let dyn_store: Arc<dyn ObjectStore> = store.clone();
    let mut sink = S3Sink::open(cfg(Arc::clone(&dyn_store), 1)).await.unwrap();
    for o in 0..100 {
        sink.write(rec(o)).await.unwrap();
    }
    drop(sink);
    store.reset();
    let mut sink = S3Sink::open(cfg(dyn_store, 1)).await.unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 100);
    assert_eq!(store.get(&store.counts.gets), 0, "open must not GET blobs");
}

async fn archive_of(n: u64) -> (Arc<CountingStore>, Arc<dyn ObjectStore>) {
    let store = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let dyn_store: Arc<dyn ObjectStore> = store.clone();
    let mut sink = S3Sink::open(cfg(Arc::clone(&dyn_store), 1)).await.unwrap();
    for o in 0..n {
        sink.write(rec(o)).await.unwrap();
    }
    (store, dyn_store)
}

/// An idle mirror listed its whole prefix on every
/// 2 s poll. Its position now stays in memory between drift checks.
#[tokio::test]
async fn idle_polls_list_nothing_between_drift_checks() {
    let (store, dyn_store) = archive_of(200).await;
    let mut sink = S3Sink::open(cfg(dyn_store, 1)).await.unwrap();
    store.reset();
    for _ in 0..10 {
        assert_eq!(sink.next_expected_offset().await.unwrap(), 200);
    }
    assert_eq!(store.get(&store.counts.lists), 0);
    assert_eq!(store.get(&store.counts.heads), 0);
}

#[tokio::test]
async fn drift_check_costs_one_head_and_one_empty_list() {
    let (store, dyn_store) = archive_of(200).await;
    let mut sink = S3Sink::open(cfg(dyn_store, 1))
        .await
        .unwrap()
        .with_drift_check_interval(Duration::ZERO);
    store.reset();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 200);
    assert_eq!(store.get(&store.counts.heads), 1);
    assert_eq!(store.get(&store.counts.lists), 1);
    assert_eq!(
        store.get(&store.counts.listed),
        0,
        "lists only after the last object"
    );
}

#[tokio::test]
async fn drift_check_catches_an_object_this_process_did_not_write() {
    let (_store, dyn_store) = archive_of(3).await;
    let mut sink = S3Sink::open(cfg(Arc::clone(&dyn_store), 1))
        .await
        .unwrap()
        .with_drift_check_interval(Duration::ZERO);
    assert_eq!(sink.next_expected_offset().await.unwrap(), 3);
    dyn_store
        .put(
            &Path::from(format!(
                "archive/ops/0/{}",
                mirror_fs::naming::batch_filename(3, 9, "parquet")
            )),
            vec![0u8].into(),
        )
        .await
        .unwrap();
    let err = sink.next_expected_offset().await.unwrap_err();
    assert!(
        format!("{err}").contains("was not written by this process"),
        "{err}"
    );
}

#[tokio::test]
async fn drift_check_catches_a_deleted_tail() {
    let (_store, dyn_store) = archive_of(3).await;
    let mut sink = S3Sink::open(cfg(Arc::clone(&dyn_store), 1))
        .await
        .unwrap()
        .with_drift_check_interval(Duration::ZERO);
    dyn_store
        .delete(&Path::from(format!(
            "archive/ops/0/{}",
            mirror_fs::naming::batch_filename(2, 2, "parquet")
        )))
        .await
        .unwrap();
    let err = sink.next_expected_offset().await.unwrap_err();
    assert!(format!("{err}").contains("is gone"), "{err}");
}
