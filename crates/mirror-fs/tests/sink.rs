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
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets,
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
