//! Pin the contract that [`FilesystemSink::set_flush_observer`]
//! fires the installed observer exactly once per durable batch flush,
//! with the source-offset range `(from, to)` matching the just-
//! flushed file's bounds.
//!
//! This is the load-bearing test for the `notify.trigger.on:
//! destination-flush` dispatch path — the webhook receiver gets one
//! POST per (from, to) the observer fires.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use mirror_core::{FlushObserver, Record, Sink, TimestampType};
use mirror_envelope::{Format, ParquetCompression};
use mirror_fs::{FilesystemSink, FilesystemSinkConfig, FlushTriggers};

fn rec(offset: u64) -> Record {
    Record {
        topic: "fs-observer".into(),
        partition: 0,
        source_offset: offset,
        timestamp_ms: Some(1_700_000_000_000 + offset as i64),
        timestamp_type: TimestampType::CreateTime,
        key: Some(format!("k{offset}").into_bytes()),
        value: Some(format!("v{offset}").into_bytes()),
        headers: vec![],
    }
}

fn cfg(root: &std::path::Path, max_offsets: u64) -> FilesystemSinkConfig {
    FilesystemSinkConfig {
        root: root.to_path_buf(),
        destination_name: "ops".into(),
        partition: 0,
        format: Format::Ndjson,
        compression: ParquetCompression::Zstd1,
        keys: mirror_envelope::ColumnType::Utf8,
        values: mirror_envelope::ColumnType::Utf8,
        compaction: None,
        cache: None,
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets,
            daily_at_utc_seconds: None,
        },
    }
}

#[derive(Debug, Default)]
struct Recording {
    fires: Mutex<Vec<(u64, u64)>>,
}

impl FlushObserver for Recording {
    fn on_flushed(&self, from: u64, to: u64) {
        self.fires.lock().unwrap().push((from, to));
    }
}

#[tokio::test]
async fn observer_fires_once_per_max_offsets_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let mut sink = FilesystemSink::open(cfg(tmp.path(), 3)).unwrap();
    let obs = Arc::new(Recording::default());
    sink.set_flush_observer(obs.clone() as Arc<dyn FlushObserver>);

    sink.write(rec(0)).await.unwrap();
    sink.write(rec(1)).await.unwrap();
    sink.write(rec(2)).await.unwrap(); // trips max-offsets=3 → first flush
    sink.write(rec(3)).await.unwrap();
    sink.write(rec(4)).await.unwrap();
    sink.write(rec(5)).await.unwrap(); // second flush

    let fires = obs.fires.lock().unwrap().clone();
    assert_eq!(
        fires,
        vec![(0, 2), (3, 5)],
        "each max-offsets trip must fire exactly once with the batch's (from, to)"
    );
}

#[tokio::test]
async fn observer_fires_on_explicit_flush_when_buffer_non_empty() {
    let tmp = tempfile::tempdir().unwrap();
    let mut sink = FilesystemSink::open(cfg(tmp.path(), 1_000)).unwrap();
    let obs = Arc::new(Recording::default());
    sink.set_flush_observer(obs.clone() as Arc<dyn FlushObserver>);

    sink.write(rec(0)).await.unwrap();
    sink.write(rec(1)).await.unwrap();
    sink.flush().await.unwrap(); // explicit (graceful shutdown path)

    let fires = obs.fires.lock().unwrap().clone();
    assert_eq!(fires, vec![(0, 1)]);
}

#[tokio::test]
async fn observer_does_not_fire_on_explicit_flush_when_buffer_empty() {
    let tmp = tempfile::tempdir().unwrap();
    let mut sink = FilesystemSink::open(cfg(tmp.path(), 1_000)).unwrap();
    let obs = Arc::new(Recording::default());
    sink.set_flush_observer(obs.clone() as Arc<dyn FlushObserver>);

    sink.flush().await.unwrap();
    assert!(
        obs.fires.lock().unwrap().is_empty(),
        "no records buffered → no flush event → observer must not fire"
    );
}

#[tokio::test]
async fn no_observer_does_not_panic() {
    // Sanity: leaving the default no-op observer in place must not
    // panic across the same record + flush path.
    let tmp = tempfile::tempdir().unwrap();
    let mut sink = FilesystemSink::open(cfg(tmp.path(), 2)).unwrap();
    sink.write(rec(0)).await.unwrap();
    sink.write(rec(1)).await.unwrap(); // flush
    sink.flush().await.unwrap();
}
