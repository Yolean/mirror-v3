//! Loop-invariant tests that drive `run_mirror` against a *real*
//! `FilesystemSink` (tempfile-backed) instead of mocks.
//!
//! ## Why this exists
//!
//! `mirror-core`'s own `tests/loop_invariants.rs` runs against the
//! in-crate `MockSink`. The mock has been a useful fast lane for
//! invariant tests, but production bugs have repeatedly turned out
//! to live in the mock-vs-real gap: the mock had no buffer/durable
//! split, no empty-buffer precondition on `align_to_source_low_watermark`,
//! and (until the PR that bundles this file) no notion of forward
//! gaps under `compaction:log`. Each gap let a real-sink-only bug
//! pass `cargo test` and break in production.
//!
//! These tests close that gap by driving the same run loop through
//! the *actual* `FilesystemSink`. They live in mirror-fs (not
//! mirror-core) because the dep direction is `mirror-fs -> mirror-core`;
//! mirror-core can't reach for `FilesystemSink` even as a dev-dep
//! without creating a dev-dep cycle.
//!
//! The cases here are deliberately a curated subset of the mock-based
//! suite — the ones where sink behaviour is the load-bearing
//! invariant. Other cases (pure error-variant matching, MockSource's
//! `Hang`/`Error` scripts) stay in `mirror-core/tests/loop_invariants.rs`
//! where they're already cheap.

use std::path::Path;
use std::time::Duration;

use mirror_core::mock::{MockSource, MockSourceEvent};
use mirror_core::{run_mirror, MirrorError, Record, TimestampType};
use mirror_envelope::{ColumnType, Format, ParquetCompression};
use mirror_fs::{
    naming, read_all_records, CompactionMode, FilesystemSink, FilesystemSinkConfig, FlushTriggers,
};

fn rec(offset: u64) -> Record {
    Record {
        topic: "loop-real".into(),
        partition: 0,
        source_offset: offset,
        timestamp_ms: Some(1_700_000_000_000 + offset as i64),
        timestamp_type: TimestampType::CreateTime,
        key: Some(format!("k{}", offset % 4).into_bytes()),
        value: Some(format!("v{offset}").into_bytes()),
        headers: vec![],
    }
}

fn fs_cfg(root: &Path, compaction: Option<CompactionMode>) -> FilesystemSinkConfig {
    let format = match compaction {
        Some(CompactionMode::Log) => Format::Parquet,
        None => Format::Ndjson,
    };
    FilesystemSinkConfig {
        root: root.to_path_buf(),
        destination_name: "ops".into(),
        partition: 0,
        format,
        compression: ParquetCompression::Zstd1,
        keys: ColumnType::Utf8,
        values: ColumnType::Utf8,
        compaction,
        cache: None,
        // High thresholds — explicit flush_now is the only thing
        // that rotates a file during these tests so we can drive
        // buffer state precisely from the events list.
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets: u64::MAX,
            daily_at_utc_seconds: None,
        },
    }
}

/// Drive `run_mirror` against a real FS sink and a scripted source.
///
/// The shutdown future is a `tokio::time::sleep(grace)`, so the loop
/// has `grace` milliseconds to process events before graceful
/// shutdown fires. A short grace (~50ms) is enough to chew through
/// the scripted events; the source's terminal `Hang` event then
/// parks the poll future indefinitely until the sleep resolves and
/// triggers graceful shutdown.
fn drive_real_fs(
    compaction: Option<CompactionMode>,
    events: Vec<MockSourceEvent>,
    grace: Duration,
) -> (Result<(), MirrorError>, tempfile::TempDir) {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let sink = FilesystemSink::open(fs_cfg(tempdir.path(), compaction)).expect("open sink");
    let source = MockSource::new(events);
    let result = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async move { run_mirror(source, sink, tokio::time::sleep(grace)).await });
    (result, tempdir)
}

#[test]
fn append_mode_writes_records_in_order_to_real_disk() {
    // Three contiguous records, then graceful shutdown after a
    // 100ms grace window. The flush-on-shutdown should produce
    // a `0-2.ndjson` file containing all three records.
    let (result, tempdir) = drive_real_fs(
        None,
        vec![
            MockSourceEvent::Record(rec(0)),
            MockSourceEvent::Record(rec(1)),
            MockSourceEvent::Record(rec(2)),
            MockSourceEvent::Hang,
        ],
        Duration::from_millis(100),
    );
    assert!(
        matches!(result, Ok(())),
        "graceful shutdown expected, got: {result:?}"
    );
    let dir = naming::partition_dir(tempdir.path(), "ops", 0);
    let records = read_all_records(&dir, Format::Ndjson).expect("read disk");
    assert_eq!(
        records.iter().map(|r| r.source_offset).collect::<Vec<_>>(),
        vec![0, 1, 2],
        "all three records must land on disk after graceful shutdown's flush"
    );
}

#[test]
fn append_mode_real_sink_rejects_source_gap() {
    // Source skips from 0 to 5 — append mode must reject the gap
    // via SourceGapAboveExpected from the run loop. Disk should
    // contain only the first record (or none, depending on whether
    // the buffer flushed before the error fired; we don't assert).
    let (result, _td) = drive_real_fs(
        None,
        vec![
            MockSourceEvent::Record(rec(0)),
            MockSourceEvent::Record(rec(5)),
        ],
        Duration::from_secs(1),
    );
    match result {
        Err(MirrorError::SourceGapAboveExpected { expected, got }) => {
            assert_eq!((expected, got), (1, 5));
        }
        other => panic!("expected SourceGapAboveExpected, got {other:?}"),
    }
}

#[test]
fn real_sink_rejects_source_going_backwards() {
    // Source delivers 5 then 3 — always fatal, in any mode.
    let (result, _td) = drive_real_fs(
        Some(CompactionMode::Log),
        vec![
            MockSourceEvent::Record(rec(5)),
            MockSourceEvent::Record(rec(3)),
        ],
        Duration::from_secs(1),
    );
    match result {
        Err(MirrorError::SourceWentBackwards { expected, got }) => {
            assert_eq!((expected, got), (6, 3));
        }
        other => panic!("expected SourceWentBackwards, got {other:?}"),
    }
}

#[test]
fn compaction_log_real_sink_accepts_bootstrap_gap_from_compact_only_topic() {
    // The cleanup.policy=compact case: broker reports low_watermark=0
    // (default for MockSource), the loop seeks(0), then the source
    // delivers an offset much later because compaction skipped earlier
    // records. The run loop must align expected to the delivered
    // offset and the real FilesystemSink must accept the gap.
    let (result, tempdir) = drive_real_fs(
        Some(CompactionMode::Log),
        vec![MockSourceEvent::Record(rec(461)), MockSourceEvent::Hang],
        Duration::from_millis(100),
    );
    // Graceful shutdown after the loop processed the aligned write.
    // The PRE-FIX run loop would have errored here with
    // SourceOffsetMismatch / Sink::UnexpectedPosition (expected 0,
    // got 461) before the shutdown timer ever fired.
    assert!(
        matches!(result, Ok(())),
        "expected graceful shutdown after aligned write, got: {result:?}"
    );
    let dir = naming::partition_dir(tempdir.path(), "ops", 0);
    let records = read_all_records(&dir, Format::Parquet).expect("read disk");
    assert_eq!(
        records.iter().map(|r| r.source_offset).collect::<Vec<_>>(),
        vec![461],
        "the aligned record at offset 461 must land on disk"
    );
}

#[test]
fn compaction_log_real_sink_accepts_repeated_midstream_gaps() {
    // The production repro the PR fixes: after the first aligned
    // write at offset 461, the broker delivers 466 then 470. The
    // buffer is non-empty so the original mid-stream attempt to call
    // `align_to_source_low_watermark` would have tripped the
    // empty-buffer precondition. The new path lets the run loop bump
    // `expected` and the sink's write accept the gap.
    let (result, tempdir) = drive_real_fs(
        Some(CompactionMode::Log),
        vec![
            MockSourceEvent::Record(rec(461)),
            MockSourceEvent::Record(rec(466)),
            MockSourceEvent::Record(rec(470)),
            MockSourceEvent::Hang,
        ],
        Duration::from_millis(100),
    );
    // The PRE-FIX path crashed on the second record (mid-stream gap
    // tripped `align_to_source_low_watermark`'s empty-buffer
    // precondition). Graceful exit here means all three records were
    // accepted into the buffer and the flush rolled them into a
    // single snapshot file.
    assert!(
        matches!(result, Ok(())),
        "expected graceful shutdown after all three gapped writes, got: {result:?}"
    );
    // The snapshot is a compaction:log file `<from>-<max>.parquet`.
    // `from` = durable_position at flush time (0, since no prior
    // flush happened); `max` = last buffered source_offset (470).
    let dir = naming::partition_dir(tempdir.path(), "ops", 0);
    let mut files: Vec<String> = std::fs::read_dir(&dir)
        .expect("readdir")
        .filter_map(|e| {
            let p = e.ok()?.path();
            let n = p.file_name()?.to_str()?.to_string();
            (n.ends_with(".parquet") && !n.contains(".tmp.")).then_some(n)
        })
        .collect();
    files.sort();
    assert_eq!(
        files,
        vec!["00000000000000000000-00000000000000000470.parquet".to_string()],
        "the snapshot file's range must cover all three accepted records"
    );
    // The snapshot's compaction view is "latest per key". The
    // three accepted records have keys `k{offset % 4}` — so
    // offsets 461, 466, 470 map to keys k1, k2, k2. The k2 entry
    // is deduplicated to its latest value (v470), leaving two
    // distinct keys in the snapshot.
    let records = read_all_records(&dir, Format::Parquet).expect("read disk");
    let mut by_key: std::collections::BTreeMap<Vec<u8>, &Record> =
        std::collections::BTreeMap::new();
    for r in &records {
        by_key.insert(r.key.clone().expect("key"), r);
    }
    assert_eq!(
        by_key.len(),
        2,
        "two distinct keys after compaction; got: {records:?}"
    );
    assert_eq!(
        by_key.get(&b"k1"[..]).expect("k1 present").value.as_deref(),
        Some(b"v461".as_slice()),
        "k1's value is its only record (v461)"
    );
    assert_eq!(
        by_key.get(&b"k2"[..]).expect("k2 present").value.as_deref(),
        Some(b"v470".as_slice()),
        "k2's value is the latest record at offset 470, not the earlier v466"
    );
}
