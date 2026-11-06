//! A running S3 mirror and the chain's floor: the drift check takes in a
//! floor that `trim` moves, and tells it from a second writer.

use std::sync::Arc;
use std::time::Duration;

use mirror_core::{Record, Sink, TimestampType};
use mirror_envelope::{Format, ParquetCompression};
use mirror_fs::blob::{BlobStore, RecordTimes};
use mirror_fs::floor::FloorMarker;
use mirror_fs::naming;
use mirror_s3::{FlushTriggers, S3Sink, S3SinkConfig, S3Store};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;

fn rec(offset: u64) -> Record {
    Record {
        topic: "t".into(),
        partition: 0,
        source_offset: offset,
        timestamp_ms: Some(1_700_000_000_000 + offset as i64),
        timestamp_type: TimestampType::CreateTime,
        key: Some(format!("k{offset}").into_bytes()),
        value: Some(b"v".to_vec()),
        headers: vec![],
    }
}

fn cfg(store: Arc<dyn ObjectStore>) -> S3SinkConfig {
    S3SinkConfig {
        read_store: Arc::clone(&store),
        write_store: store,
        prefix: Some(Path::from("archive")),
        destination_name: "ops".into(),
        partition: 0,
        format: Format::Parquet,
        compression: ParquetCompression::Zstd1,
        keys: mirror_envelope::ColumnType::Utf8,
        values: mirror_envelope::ColumnType::Utf8,
        compaction: None,
        encryption: None,
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets: 2,
            daily_at_utc_seconds: None,
        },
        lock: None,
    }
}

fn path(name: &str) -> Path {
    Path::from(format!("archive/ops/0/{name}"))
}

fn n(from: u64, to: u64) -> String {
    naming::batch_filename(from, to, "parquet")
}

fn marker(offset: u64) -> String {
    FloorMarker {
        offset,
        at: 1_800_000_000,
        oldest_ms: None,
    }
    .filename()
}

/// A running sink that has written offsets 0..6 (three objects), and
/// checks for drift on every poll.
async fn running() -> (Arc<dyn ObjectStore>, S3Sink) {
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut sink = S3Sink::open(cfg(Arc::clone(&mem)))
        .await
        .unwrap()
        .with_drift_check_interval(Duration::ZERO);
    for o in 0..6 {
        sink.write(rec(o)).await.unwrap();
    }
    (mem, sink)
}

#[tokio::test]
async fn each_object_carries_its_record_times() {
    let (mem, _sink) = running().await;
    let store = S3Store::read_only(Arc::clone(&mem), Path::from("archive/ops/0"));
    assert_eq!(
        store.record_times(&n(2, 3)).await.unwrap(),
        Some(RecordTimes {
            min_ms: Some(1_700_000_000_002),
            max_ms: Some(1_700_000_000_003),
            without: 0,
        })
    );
    // An object written without them.
    mem.put(&path("x"), vec![0u8].into()).await.unwrap();
    assert_eq!(store.record_times("x").await.unwrap(), None);
}

#[tokio::test]
async fn the_drift_check_takes_in_a_floor_that_trim_moves() {
    let (mem, mut sink) = running().await;
    assert_eq!(sink.floor(), None);
    mem.put(&path(&marker(2)), vec![].into()).await.unwrap();
    mem.delete(&path(&n(0, 1))).await.unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 6);
    assert_eq!(sink.floor().map(|f| f.offset), Some(2));
    sink.write(rec(6)).await.unwrap();
    sink.write(rec(7)).await.unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 8);
}

#[tokio::test]
async fn trim_may_remove_the_whole_chain_when_its_floor_reaches_the_end() {
    let (mem, mut sink) = running().await;
    mem.put(&path(&marker(6)), vec![].into()).await.unwrap();
    for name in [n(2, 3), n(4, 5)] {
        mem.delete(&path(&name)).await.unwrap();
    }
    // 0-1 is still there: deletion lags (a retention lock).
    assert_eq!(sink.next_expected_offset().await.unwrap(), 6);
    assert_eq!(sink.next_expected_offset().await.unwrap(), 6);
    sink.write(rec(6)).await.unwrap();
    sink.write(rec(7)).await.unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 8);
    drop(sink);
    let mut reopened = S3Sink::open(cfg(mem)).await.unwrap();
    assert_eq!(reopened.next_expected_offset().await.unwrap(), 8);
    assert_eq!(reopened.below_floor_at_open(), 1);
}

#[tokio::test]
async fn a_tail_removed_below_no_floor_is_still_drift() {
    let (mem, mut sink) = running().await;
    // A floor short of the end does not cover the tail.
    mem.put(&path(&marker(4)), vec![].into()).await.unwrap();
    mem.delete(&path(&n(4, 5))).await.unwrap();
    let err = sink.next_expected_offset().await.unwrap_err();
    assert!(format!("{err}").contains("is gone"), "{err}");
}

#[tokio::test]
async fn a_floor_above_the_end_of_the_chain_is_drift() {
    let (mem, mut sink) = running().await;
    mem.put(&path(&marker(7)), vec![].into()).await.unwrap();
    let err = sink.next_expected_offset().await.unwrap_err();
    assert!(
        format!("{err}").contains("floor at 7, above the end of this mirror's chain, 6"),
        "{err}"
    );
}

#[tokio::test]
async fn a_second_writer_is_drift_with_a_floor_too() {
    let (mem, mut sink) = running().await;
    mem.put(&path(&marker(2)), vec![].into()).await.unwrap();
    mem.put(&path(&n(6, 9)), vec![0u8].into()).await.unwrap();
    let err = sink.next_expected_offset().await.unwrap_err();
    assert!(
        format!("{err}").contains("was not written by this process"),
        "{err}"
    );
}

#[tokio::test]
async fn a_broken_marker_name_is_drift() {
    let (mem, mut sink) = running().await;
    mem.put(&path("floor-2.json"), vec![].into()).await.unwrap();
    let err = sink.next_expected_offset().await.unwrap_err();
    assert!(
        format!("{err}").contains("not a floor marker name"),
        "{err}"
    );
}
