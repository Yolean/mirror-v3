//! Filesystem sink invariants.

use std::time::Duration;

use mirror_core::{Record, Sink, TimestampType};
use mirror_envelope::{Format, ParquetCompression};
use mirror_fs::{read_all_records, FilesystemSink, FilesystemSinkConfig, FlushTriggers};

// mirror_envelope must be reachable from the test as a path; brought
// in via mirror-fs's re-export below.

fn rec(offset: u64) -> Record {
    Record {
        topic: "fs-test".into(),
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
    // Existing sink tests target the ndjson envelope. The parquet
    // path is covered by mirror-envelope's round-trip tests and by
    // the upcoming e2e suite.
    FilesystemSinkConfig {
        root: root.to_path_buf(),
        destination_name: "ops".into(),
        partition: 0,
        format: Format::Ndjson,
        compression: ParquetCompression::Zstd1,
        value_as_json: false,
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets,
            daily_at_utc_seconds: None,
        },
    }
}

#[tokio::test]
async fn empty_directory_starts_at_zero() {
    let tmp = tempfile::tempdir().unwrap();
    let mut sink = FilesystemSink::open(cfg(tmp.path(), 100)).unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 0);
}

#[tokio::test]
async fn write_buffers_then_flushes_on_count_trigger() {
    let tmp = tempfile::tempdir().unwrap();
    let mut sink = FilesystemSink::open(cfg(tmp.path(), 3)).unwrap();

    sink.write(rec(0)).await.unwrap();
    sink.write(rec(1)).await.unwrap();
    // Not yet flushed; next-expected accounts for the in-memory buffer.
    assert_eq!(sink.next_expected_offset().await.unwrap(), 2);
    let listing_before: Vec<_> = std::fs::read_dir(tmp.path().join("ops").join("0"))
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert!(
        listing_before.is_empty(),
        "no file should exist yet: {listing_before:?}"
    );

    // Third record trips the count trigger.
    sink.write(rec(2)).await.unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 3);
    let records = read_all_records(
        &tmp.path().join("ops").join("0"),
        mirror_envelope::Format::Ndjson,
    )
    .unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].source_offset, 0);
    assert_eq!(records[2].source_offset, 2);
}

#[tokio::test]
async fn rejects_out_of_order_write() {
    let tmp = tempfile::tempdir().unwrap();
    let mut sink = FilesystemSink::open(cfg(tmp.path(), 100)).unwrap();
    sink.write(rec(0)).await.unwrap();
    let err = sink.write(rec(5)).await.expect_err("gap must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("destination advanced") || msg.contains("expected"),
        "got {msg}"
    );
}

#[tokio::test]
async fn restart_recomputes_position_from_listing() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut sink = FilesystemSink::open(cfg(tmp.path(), 2)).unwrap();
        sink.write(rec(0)).await.unwrap();
        sink.write(rec(1)).await.unwrap(); // flushes
                                           // sink dropped here; buffer (empty) is irrelevant
    }
    // Simulate restart: open a fresh sink against the same dir.
    let mut sink = FilesystemSink::open(cfg(tmp.path(), 2)).unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 2);
    // Must accept record 2 next, not 0.
    let err = sink
        .write(rec(0))
        .await
        .expect_err("offset 0 must be rejected after restart");
    let msg = format!("{err}");
    assert!(
        msg.contains("expected") || msg.contains("destination"),
        "got {msg}"
    );
    sink.write(rec(2)).await.unwrap();
    sink.write(rec(3)).await.unwrap(); // flushes 2-3
    assert_eq!(sink.next_expected_offset().await.unwrap(), 4);
}

#[tokio::test]
async fn crashed_tmp_file_is_ignored_on_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ops").join("0");
    std::fs::create_dir_all(&dir).unwrap();
    // Pretend a prior writer crashed mid-flush: leftover .tmp.<uuid>.
    std::fs::write(
        dir.join("00000000000000000000-00000000000000000099.ndjson.tmp.abc"),
        b"{\"source_offset\":0}\n",
    )
    .unwrap();
    // Real committed file at 0..=0.
    let line = serde_json::to_string(&serde_json::json!({
        "source_offset": 0,
        "key": null,
        "value": null,
    }))
    .unwrap();
    std::fs::write(
        dir.join("00000000000000000000-00000000000000000000.ndjson"),
        format!("{line}\n"),
    )
    .unwrap();

    let mut sink = FilesystemSink::open(cfg(tmp.path(), 100)).unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 1);
}

#[tokio::test]
async fn corrupt_chain_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ops").join("0");
    std::fs::create_dir_all(&dir).unwrap();
    // Two overlapping files at from=0.
    std::fs::write(
        dir.join("00000000000000000000-00000000000000000004.ndjson"),
        "",
    )
    .unwrap();
    std::fs::write(
        dir.join("00000000000000000000-00000000000000000009.ndjson"),
        "",
    )
    .unwrap();

    let err = FilesystemSink::open(cfg(tmp.path(), 100))
        .err()
        .expect("must reject overlap");
    let msg = format!("{err}");
    assert!(
        msg.contains("gap or overlap") || msg.contains("corrupt"),
        "got {msg}"
    );
}

// --- daily-flush tests (clock-injected) ---

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Returns (atomic clock, clock-fn handle). Tests mutate the atomic
/// to advance wall-clock time without `tokio::time::sleep`.
fn injected_clock(initial: u64) -> (Arc<AtomicU64>, mirror_fs::UnixClock) {
    let val = Arc::new(AtomicU64::new(initial));
    let val2 = Arc::clone(&val);
    let f: mirror_fs::UnixClock = Arc::new(move || val2.load(Ordering::SeqCst));
    (val, f)
}

fn cfg_with_daily(root: &std::path::Path, target_secs: u32) -> FilesystemSinkConfig {
    let mut c = cfg(root, 1000); // large max_offsets so only daily can trip
    c.flush.daily_at_utc_seconds = Some(target_secs);
    c
}

fn files_in(root: &std::path::Path) -> Vec<String> {
    let dir = root.join("ops").join("0");
    if !dir.exists() {
        return Vec::new();
    }
    let mut out: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| !n.contains(".tmp."))
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn daily_flush_trips_when_clock_crosses_boundary() {
    let tmp = tempfile::tempdir().unwrap();
    // 23:55:00 UTC today; daily target = 00:00:00 UTC (= 86400 absolute)
    let (clock_val, clock) = injected_clock(23 * 3600 + 55 * 60);
    let mut sink = FilesystemSink::open_with_clock(cfg_with_daily(tmp.path(), 0), clock).unwrap();

    sink.write(rec(0)).await.unwrap();
    sink.write(rec(1)).await.unwrap();
    assert!(files_in(tmp.path()).is_empty(), "no flush before boundary");

    // Jump past midnight UTC.
    clock_val.store(86_400 + 5, Ordering::SeqCst);
    // tick_daily runs at the top of write, flushes records 0+1, then
    // appends record 2 into an empty buffer for tomorrow's batch.
    sink.write(rec(2)).await.unwrap();

    let records = read_all_records(
        &tmp.path().join("ops").join("0"),
        mirror_envelope::Format::Ndjson,
    )
    .unwrap();
    assert_eq!(
        records.iter().map(|r| r.source_offset).collect::<Vec<_>>(),
        vec![0, 1],
        "boundary flush carries records buffered before midnight"
    );
    assert_eq!(
        files_in(tmp.path()),
        vec!["00000000000000000000-00000000000000000001.ndjson"]
    );
}

#[tokio::test]
async fn daily_flush_with_empty_buffer_skips_silently_and_advances() {
    let tmp = tempfile::tempdir().unwrap();
    let (clock_val, clock) = injected_clock(23 * 3600 + 55 * 60);
    let mut sink = FilesystemSink::open_with_clock(cfg_with_daily(tmp.path(), 0), clock).unwrap();

    // No writes. Cross midnight. tick_daily on the idle-poll path
    // (next_expected_offset) should advance the boundary but not
    // produce a zero-record file.
    clock_val.store(86_400 + 5, Ordering::SeqCst);
    let _ = sink.next_expected_offset().await.unwrap();
    assert!(files_in(tmp.path()).is_empty(), "empty buffer => no file");

    // A record arriving 5s after the boundary now sits in the
    // buffer waiting for the *next* trigger (tomorrow's midnight,
    // or max-*). Today's slot is gone.
    sink.write(rec(0)).await.unwrap();
    assert!(files_in(tmp.path()).is_empty(), "no premature flush");
    sink.flush_now().await.unwrap();
    let recs = read_all_records(
        &tmp.path().join("ops").join("0"),
        mirror_envelope::Format::Ndjson,
    )
    .unwrap();
    assert_eq!(recs.len(), 1);
}

#[tokio::test]
async fn daily_flush_skips_correctly_across_a_multi_day_jump() {
    let tmp = tempfile::tempdir().unwrap();
    let (clock_val, clock) = injected_clock(23 * 3600);
    let mut sink = FilesystemSink::open_with_clock(cfg_with_daily(tmp.path(), 0), clock).unwrap();

    sink.write(rec(0)).await.unwrap();
    // Jump three days forward.
    clock_val.store(3 * 86_400 + 10, Ordering::SeqCst);
    sink.write(rec(1)).await.unwrap();

    // tick_daily flushes the pre-jump buffer ([0]) exactly once and
    // advances next_daily past the new "now" — we don't fire again
    // per skipped day.
    let records = read_all_records(
        &tmp.path().join("ops").join("0"),
        mirror_envelope::Format::Ndjson,
    )
    .unwrap();
    assert_eq!(
        records.iter().map(|r| r.source_offset).collect::<Vec<_>>(),
        vec![0]
    );
    assert_eq!(files_in(tmp.path()).len(), 1);
}

#[tokio::test]
async fn no_daily_trigger_means_midnight_crossing_does_not_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let (clock_val, clock) = injected_clock(23 * 3600);
    let mut sink = FilesystemSink::open_with_clock(cfg(tmp.path(), 1000), clock).unwrap();

    sink.write(rec(0)).await.unwrap();
    clock_val.store(2 * 86_400, Ordering::SeqCst);
    sink.write(rec(1)).await.unwrap();

    assert!(
        files_in(tmp.path()).is_empty(),
        "with daily disabled, crossing midnight must not trigger a flush"
    );
}

#[tokio::test]
async fn flush_now_writes_partial_batch() {
    let tmp = tempfile::tempdir().unwrap();
    let mut sink = FilesystemSink::open(cfg(tmp.path(), 1000)).unwrap();
    sink.write(rec(0)).await.unwrap();
    sink.write(rec(1)).await.unwrap();
    sink.flush_now().await.unwrap();
    let records = read_all_records(
        &tmp.path().join("ops").join("0"),
        mirror_envelope::Format::Ndjson,
    )
    .unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].source_offset, 0);
    assert_eq!(records[1].source_offset, 1);
}
