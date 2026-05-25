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

// ---------- compaction-mode tests ----------

fn cfg_compacted(root: &std::path::Path, max_offsets: u64) -> FilesystemSinkConfig {
    FilesystemSinkConfig {
        root: root.to_path_buf(),
        destination_name: "ops".into(),
        partition: 0,
        format: Format::Parquet,
        compression: ParquetCompression::Zstd1,
        keys: mirror_envelope::ColumnType::Utf8,
        values: mirror_envelope::ColumnType::Utf8,
        compaction: Some(mirror_fs::CompactionMode::Log),
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets,
            daily_at_utc_seconds: None,
        },
    }
}

fn rec_kv(offset: u64, key: &str, value: Option<&str>) -> Record {
    Record {
        topic: "fs-test".into(),
        partition: 0,
        source_offset: offset,
        timestamp_ms: Some(1_700_000_000_000 + offset as i64),
        timestamp_type: TimestampType::CreateTime,
        key: Some(key.as_bytes().to_vec()),
        value: value.map(|s| s.as_bytes().to_vec()),
        headers: vec![],
    }
}

#[tokio::test]
async fn compaction_emits_one_row_per_key_with_latest_value() {
    let tmp = tempfile::tempdir().unwrap();
    let mut sink = FilesystemSink::open(cfg_compacted(tmp.path(), 4)).unwrap();
    sink.write(rec_kv(0, "k1", Some("v1a"))).await.unwrap();
    sink.write(rec_kv(1, "k2", Some("v2a"))).await.unwrap();
    sink.write(rec_kv(2, "k1", Some("v1b"))).await.unwrap();
    sink.write(rec_kv(3, "k3", Some("v3"))).await.unwrap(); // triggers flush

    let snapshot =
        mirror_fs::read_latest_snapshot(&tmp.path().join("ops").join("0"), Format::Parquet)
            .unwrap();
    // 3 distinct keys; k1 has the later value (v1b), and BTreeMap order
    // gives us k1, k2, k3.
    assert_eq!(snapshot.len(), 3);
    let by_key: std::collections::BTreeMap<_, _> = snapshot
        .iter()
        .map(|r| {
            (
                std::str::from_utf8(r.key.as_ref().unwrap())
                    .unwrap()
                    .to_string(),
                std::str::from_utf8(r.value.as_ref().unwrap())
                    .unwrap()
                    .to_string(),
            )
        })
        .collect();
    assert_eq!(by_key["k1"], "v1b");
    assert_eq!(by_key["k2"], "v2a");
    assert_eq!(by_key["k3"], "v3");
}

#[tokio::test]
async fn compaction_tombstone_removes_key_from_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let mut sink = FilesystemSink::open(cfg_compacted(tmp.path(), 3)).unwrap();
    sink.write(rec_kv(0, "k1", Some("v1"))).await.unwrap();
    sink.write(rec_kv(1, "k2", Some("v2"))).await.unwrap();
    sink.write(rec_kv(2, "k1", None)).await.unwrap(); // tombstone, triggers flush

    let snapshot =
        mirror_fs::read_latest_snapshot(&tmp.path().join("ops").join("0"), Format::Parquet)
            .unwrap();
    let keys: Vec<_> = snapshot
        .iter()
        .map(|r| {
            std::str::from_utf8(r.key.as_ref().unwrap())
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(keys, vec!["k2".to_string()]);
}

#[tokio::test]
async fn compaction_restart_bootstraps_view_from_latest_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut sink = FilesystemSink::open(cfg_compacted(tmp.path(), 2)).unwrap();
        sink.write(rec_kv(0, "k1", Some("v1"))).await.unwrap();
        sink.write(rec_kv(1, "k2", Some("v2"))).await.unwrap(); // flush 0-1
    }
    // Reopen — durable_position should be 2 (resumes from Kafka here),
    // and the in-memory view should contain k1=v1, k2=v2.
    let mut sink = FilesystemSink::open(cfg_compacted(tmp.path(), 2)).unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 2);
    // Write a tombstone for k1 plus a new k3 — the resulting snapshot
    // must reflect the merged view, not a fresh "from scratch" view.
    sink.write(rec_kv(2, "k1", None)).await.unwrap();
    sink.write(rec_kv(3, "k3", Some("v3"))).await.unwrap(); // flush 2-3

    let snapshot =
        mirror_fs::read_latest_snapshot(&tmp.path().join("ops").join("0"), Format::Parquet)
            .unwrap();
    let by_key: std::collections::BTreeMap<_, _> = snapshot
        .iter()
        .map(|r| {
            (
                std::str::from_utf8(r.key.as_ref().unwrap())
                    .unwrap()
                    .to_string(),
                std::str::from_utf8(r.value.as_ref().unwrap())
                    .unwrap()
                    .to_string(),
            )
        })
        .collect();
    assert_eq!(by_key.get("k1"), None, "k1 should be tombstoned");
    assert_eq!(by_key["k2"], "v2");
    assert_eq!(by_key["k3"], "v3");
}

#[tokio::test]
async fn compaction_scan_validate_accepts_gap() {
    // Simulates an operator that GC'd an old snapshot. The chain has
    // a gap but the latest snapshot is intact, so open() must succeed.
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut sink = FilesystemSink::open(cfg_compacted(tmp.path(), 2)).unwrap();
        sink.write(rec_kv(0, "k1", Some("v1"))).await.unwrap();
        sink.write(rec_kv(1, "k2", Some("v2"))).await.unwrap(); // 0-1.parquet
    }
    {
        let mut sink = FilesystemSink::open(cfg_compacted(tmp.path(), 2)).unwrap();
        sink.write(rec_kv(2, "k3", Some("v3"))).await.unwrap();
        sink.write(rec_kv(3, "k4", Some("v4"))).await.unwrap(); // 2-3.parquet
    }
    // GC the older snapshot. Filenames are zero-padded to 20 digits,
    // so resolve by searching the directory for the earlier `from`.
    let dir = tmp.path().join("ops").join("0");
    let older = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.starts_with("00000000000000000000-"))
                .unwrap_or(false)
        })
        .expect("must find 0..=1 snapshot on disk");
    std::fs::remove_file(&older).unwrap();
    // Reopen — should succeed and resume at offset 4 with the latest
    // snapshot's view (k3, k4 only, because the GC'd snapshot's keys
    // were not in the latest).
    let mut sink = FilesystemSink::open(cfg_compacted(tmp.path(), 100)).unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 4);
}

#[tokio::test]
async fn compaction_rejects_null_key_at_write() {
    let tmp = tempfile::tempdir().unwrap();
    let mut sink = FilesystemSink::open(cfg_compacted(tmp.path(), 100)).unwrap();
    let mut r = rec_kv(0, "x", Some("v"));
    r.key = None;
    let err = sink.write(r).await.expect_err("null key must be rejected");
    assert!(format!("{err}").contains("null"), "got: {err}");
}

#[tokio::test]
async fn compaction_rejects_non_utf8_key_at_write() {
    let tmp = tempfile::tempdir().unwrap();
    let mut sink = FilesystemSink::open(cfg_compacted(tmp.path(), 100)).unwrap();
    let mut r = rec_kv(0, "x", Some("v"));
    r.key = Some(vec![0xff, 0xfe]);
    let err = sink
        .write(r)
        .await
        .expect_err("non-UTF-8 key must be rejected");
    assert!(format!("{err}").contains("UTF-8"), "got: {err}");
}

#[tokio::test]
async fn compaction_idle_flush_now_does_not_emit_file() {
    // Option A: empty buffer + no view change = no file emitted.
    let tmp = tempfile::tempdir().unwrap();
    let mut sink = FilesystemSink::open(cfg_compacted(tmp.path(), 100)).unwrap();
    sink.flush_now().await.unwrap();
    let dir = tmp.path().join("ops").join("0");
    let files: Vec<_> = std::fs::read_dir(&dir)
        .map(|rd| rd.filter_map(|e| e.ok()).collect())
        .unwrap_or_default();
    assert!(
        files.is_empty(),
        "idle flush in compaction mode must not emit a file"
    );
}
