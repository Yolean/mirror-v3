//! Sink-trait matrix against a real `FilesystemSink`.
//!
//! Walks the (compaction-mode × buffer-state × action) grid from
//! `REVIEW_TEST_STRATEGY.md §4` against a real sink backed by
//! `tempfile::TempDir`; no mocks, so an invariant change in the
//! real sink surfaces here instead of slipping past a mock that
//! quietly diverged from production. The full 16-cell table is in
//! the `MATRIX` const at the bottom of this file; each row names
//! what it covers (e.g. `log/non-empty/delivered>exp`) so a CI
//! failure points at the regressed cell directly.
//!
//! The matrix is constructed once per test (Rust integration tests
//! sit in their own binary and we want each row's failure to be
//! attributed), but the per-row setup is deterministic and cheap:
//! one tempdir + a handful of writes per cell.
//!
//! **Why this exists.** The mid-stream-gap bug
//! (`log/non-empty/delivered>exp`) was a new cell that the existing
//! one-test-per-scenario layout didn't naturally encode. A table
//! catches "we added gap acceptance and missed one of the buffer
//! states" by making *every* gated cell explicit. It also lets the
//! S3 sink's matrix (see `crates/mirror-s3/tests/sink_matrix.rs`)
//! assert symmetry: any FS row that's present must have an S3
//! counterpart with the same outcome, modulo backend specifics.

use std::time::Duration;

use mirror_core::{Record, Sink, SinkError, TimestampType};
use mirror_envelope::{ColumnType, Format, ParquetCompression};
use mirror_fs::{CompactionMode, FilesystemSink, FilesystemSinkConfig, FlushTriggers};

fn rec(offset: u64) -> Record {
    Record {
        topic: "sink-matrix".into(),
        partition: 0,
        source_offset: offset,
        timestamp_ms: Some(1_700_000_000_000 + offset as i64),
        timestamp_type: TimestampType::CreateTime,
        key: Some(format!("k{}", offset % 4).into_bytes()),
        value: Some(format!("v{offset}").into_bytes()),
        headers: vec![],
    }
}

fn cfg(root: &std::path::Path, compaction: Option<CompactionMode>) -> FilesystemSinkConfig {
    // Compaction:log requires Parquet (an explicit precondition in
    // mirror_config validation). Append mode runs against ndjson
    // because the existing `tests/sink.rs` shape uses ndjson and
    // mirroring that keeps the failure output operator-friendly.
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
        // Huge thresholds so explicit `flush()` is the only thing
        // that actually rotates a file; matrix rows that *don't*
        // call flush get to control buffer state precisely.
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets: u64::MAX,
            daily_at_utc_seconds: None,
        },
    }
}

/// Compaction mode the cell exercises.
#[derive(Debug, Clone, Copy)]
enum Mode {
    Append,
    Log,
}

impl Mode {
    fn to_compaction(self) -> Option<CompactionMode> {
        match self {
            Mode::Append => None,
            Mode::Log => Some(CompactionMode::Log),
        }
    }
}

/// Buffer state the cell exercises *at the moment of the action*.
/// Set up by the preload phase: `Empty` flushes after the preload,
/// `NonEmpty` leaves the preloaded records in the buffer.
#[derive(Debug, Clone, Copy)]
enum BufferState {
    Empty,
    NonEmpty,
}

/// The action under test.
#[derive(Debug)]
enum Action {
    /// `sink.write(rec(offset))`.
    Write(u64),
    /// `sink.flush_now()` and assert on the produced filename.
    /// Tuple is the expected `(from, to)` parsed back from disk.
    Flush {
        expected_from: u64,
        expected_to: u64,
    },
    /// `sink.align_to_source_low_watermark(low_watermark)`.
    Align { low_watermark: u64 },
    /// `sink.next_expected_offset()`.
    NextExpected,
}

#[derive(Debug)]
enum Outcome {
    /// The action returned `Ok(())` (write/flush/align).
    Ok,
    /// `next_expected_offset()` returned this value.
    NextExpectedIs(u64),
    /// `SinkError::UnexpectedPosition { expected, actual }`.
    UnexpectedPosition { expected: u64, actual: u64 },
    /// `SinkError::Transport(message)` where the message contains
    /// this substring. Used for the align preconditions, which fail
    /// with descriptive transport errors rather than the structured
    /// `UnexpectedPosition` variant.
    TransportContains(&'static str),
}

struct Case {
    name: &'static str,
    mode: Mode,
    /// Records to write before the action runs. Numeric offsets.
    /// For compaction:log cases the preload offsets may include
    /// gaps; for append mode they must be contiguous starting at 0
    /// (otherwise the preload itself fails).
    preload: &'static [u64],
    /// `Empty` → flush after the preload (so the buffer is empty at
    /// action time); `NonEmpty` → skip the flush.
    buffer_state: BufferState,
    action: Action,
    expected: Outcome,
}

async fn run_case(case: &Case) {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut sink = FilesystemSink::open(cfg(tempdir.path(), case.mode.to_compaction()))
        .expect("open FilesystemSink");

    // Preload phase.
    for &offset in case.preload {
        sink.write(rec(offset))
            .await
            .unwrap_or_else(|e| panic!("[{}] preload write({offset}) failed: {e}", case.name));
    }
    if matches!(case.buffer_state, BufferState::Empty) && !case.preload.is_empty() {
        sink.flush_now()
            .await
            .unwrap_or_else(|e| panic!("[{}] preload flush failed: {e}", case.name));
    }

    // Action phase.
    let observed = match &case.action {
        Action::Write(offset) => sink.write(rec(*offset)).await.map(|()| None),
        Action::Flush {
            expected_from,
            expected_to,
        } => {
            sink.flush_now().await.map(|()| {
                // Filename verification: the latest ndjson/parquet
                // file in the partition dir must be `<from>-<to>`.
                let dir = mirror_fs::naming::partition_dir(tempdir.path(), "ops", 0);
                let mut files: Vec<String> = std::fs::read_dir(&dir)
                    .expect("readdir")
                    .filter_map(|e| {
                        let p = e.ok()?.path();
                        let name = p.file_name()?.to_str()?.to_string();
                        let is_real = (name.ends_with(".ndjson") || name.ends_with(".parquet"))
                            && !name.contains(".tmp.");
                        is_real.then_some(name)
                    })
                    .collect();
                files.sort();
                let last = files
                    .last()
                    .unwrap_or_else(|| panic!("[{}] no flushed file found", case.name));
                // Filenames look like `00000000000000000000-00000000000000000004.ndjson`.
                let ext = if matches!(case.mode, Mode::Log) {
                    "parquet"
                } else {
                    "ndjson"
                };
                let expected_name = format!("{expected_from:020}-{expected_to:020}.{ext}");
                assert_eq!(
                    last, &expected_name,
                    "[{}] flushed filename should encode (from={expected_from}, to={expected_to})",
                    case.name
                );
                None
            })
        }
        Action::Align { low_watermark } => sink
            .align_to_source_low_watermark(*low_watermark)
            .await
            .map(|()| None),
        Action::NextExpected => sink.next_expected_offset().await.map(Some),
    };

    // Outcome assertion.
    match (&case.expected, observed) {
        (Outcome::Ok, Ok(_)) => {}
        (Outcome::NextExpectedIs(expected), Ok(Some(value))) => {
            assert_eq!(
                value, *expected,
                "[{}] next_expected_offset value",
                case.name
            );
        }
        (
            Outcome::UnexpectedPosition {
                expected: exp,
                actual: act,
            },
            Err(SinkError::UnexpectedPosition { expected, actual }),
        ) => {
            assert_eq!(
                (expected, actual),
                (*exp, *act),
                "[{}] UnexpectedPosition payload",
                case.name
            );
        }
        (Outcome::TransportContains(needle), Err(SinkError::Transport(msg))) => {
            assert!(
                msg.contains(needle),
                "[{}] Transport({msg:?}) should contain {needle:?}",
                case.name
            );
        }
        (expected, observed) => {
            panic!(
                "[{}] mismatch: expected={expected:?} observed={observed:?}",
                case.name
            );
        }
    }
}

#[tokio::test]
async fn matrix() {
    let cases = matrix_cases();
    for case in &cases {
        run_case(case).await;
    }
}

fn matrix_cases() -> Vec<Case> {
    vec![
        // ============================================================
        //  APPEND MODE; every gap is fatal, equality is the only OK
        // ============================================================

        // append × empty × write at expected → OK
        Case {
            name: "append/empty/write_at_expected/ok",
            mode: Mode::Append,
            preload: &[],
            buffer_state: BufferState::Empty,
            action: Action::Write(0),
            expected: Outcome::Ok,
        },
        // append × empty × write above expected → reject (gap forbidden)
        Case {
            name: "append/empty/write_above_expected/rejects",
            mode: Mode::Append,
            preload: &[],
            buffer_state: BufferState::Empty,
            action: Action::Write(5),
            expected: Outcome::UnexpectedPosition {
                expected: 0,
                actual: 5,
            },
        },
        // append × empty (post-flush, durable=5) × write below durable → reject (backwards)
        Case {
            name: "append/empty_after_flush/write_below_durable/rejects",
            mode: Mode::Append,
            preload: &[0, 1, 2, 3, 4],
            buffer_state: BufferState::Empty, // flush after preload
            action: Action::Write(3),
            expected: Outcome::UnexpectedPosition {
                expected: 5,
                actual: 3,
            },
        },
        // append × non-empty × write at expected → OK
        Case {
            name: "append/non_empty/write_at_expected/ok",
            mode: Mode::Append,
            preload: &[0, 1, 2],
            buffer_state: BufferState::NonEmpty,
            action: Action::Write(3),
            expected: Outcome::Ok,
        },
        // append × non-empty × write above expected → reject (gap forbidden)
        Case {
            name: "append/non_empty/write_above_expected/rejects",
            mode: Mode::Append,
            preload: &[0, 1, 2],
            buffer_state: BufferState::NonEmpty,
            action: Action::Write(7),
            expected: Outcome::UnexpectedPosition {
                expected: 3,
                actual: 7,
            },
        },
        // append × non-empty × write below buffered head → reject (backwards)
        Case {
            name: "append/non_empty/write_below_buffered_head/rejects",
            mode: Mode::Append,
            preload: &[0, 1, 2],
            buffer_state: BufferState::NonEmpty,
            action: Action::Write(1),
            expected: Outcome::UnexpectedPosition {
                expected: 3,
                actual: 1,
            },
        },
        // ============================================================
        //  COMPACTION:LOG; forward gaps OK, backwards still fatal
        // ============================================================

        // log × empty × write at expected (offset 0) → OK
        Case {
            name: "log/empty/write_at_expected/ok",
            mode: Mode::Log,
            preload: &[],
            buffer_state: BufferState::Empty,
            action: Action::Write(0),
            expected: Outcome::Ok,
        },
        // log × empty × write above expected (bootstrap-time gap from compact-only topic) → OK
        Case {
            name: "log/empty/write_above_expected/ok_bootstrap_gap",
            mode: Mode::Log,
            preload: &[],
            buffer_state: BufferState::Empty,
            action: Action::Write(461),
            expected: Outcome::Ok,
        },
        // log × empty (post-flush, durable=5) × write below durable → reject (backwards)
        Case {
            name: "log/empty_after_flush/write_below_durable/rejects",
            mode: Mode::Log,
            preload: &[0, 1, 2, 3, 4],
            buffer_state: BufferState::Empty,
            action: Action::Write(3),
            expected: Outcome::UnexpectedPosition {
                expected: 5,
                actual: 3,
            },
        },
        // log × non-empty × write at expected → OK
        Case {
            name: "log/non_empty/write_at_expected/ok",
            mode: Mode::Log,
            preload: &[0, 1, 2],
            buffer_state: BufferState::NonEmpty,
            action: Action::Write(3),
            expected: Outcome::Ok,
        },
        // log × non-empty × write above expected (mid-stream compaction gap) → OK
        // This is THE bug that motivated the matrix.
        Case {
            name: "log/non_empty/write_above_expected/ok_midstream_gap",
            mode: Mode::Log,
            preload: &[0, 1, 2],
            buffer_state: BufferState::NonEmpty,
            action: Action::Write(7),
            expected: Outcome::Ok,
        },
        // log × non-empty × write below buffered head → reject (backwards)
        Case {
            name: "log/non_empty/write_below_buffered_head/rejects",
            mode: Mode::Log,
            preload: &[0, 1, 2],
            buffer_state: BufferState::NonEmpty,
            action: Action::Write(1),
            expected: Outcome::UnexpectedPosition {
                expected: 3,
                actual: 1,
            },
        },
        // ============================================================
        //  ALIGN; bootstrap-only, empty-buffer precondition
        // ============================================================

        // log × empty × align(low_watermark=461) → OK
        Case {
            name: "log/empty/align/ok",
            mode: Mode::Log,
            preload: &[],
            buffer_state: BufferState::Empty,
            action: Action::Align { low_watermark: 461 },
            expected: Outcome::Ok,
        },
        // log × non-empty × align → reject (empty-buffer precondition)
        Case {
            name: "log/non_empty/align/rejects_with_empty_buffer_precondition",
            mode: Mode::Log,
            preload: &[0, 1, 2],
            buffer_state: BufferState::NonEmpty,
            action: Action::Align { low_watermark: 461 },
            expected: Outcome::TransportContains("inconsistent state"),
        },
        // append × empty × align → reject (compaction-mode precondition)
        Case {
            name: "append/empty/align/rejects_on_non_compaction_sink",
            mode: Mode::Append,
            preload: &[],
            buffer_state: BufferState::Empty,
            action: Action::Align { low_watermark: 461 },
            expected: Outcome::TransportContains("non-compaction sink"),
        },
        // ============================================================
        //  FLUSH; filename encodes the offset range correctly
        // ============================================================

        // append × non-empty × flush → file `<dur>-<dur+len-1>` (contiguous)
        Case {
            name: "append/non_empty/flush/contiguous_filename",
            mode: Mode::Append,
            preload: &[0, 1, 2, 3, 4],
            buffer_state: BufferState::NonEmpty,
            action: Action::Flush {
                expected_from: 0,
                expected_to: 4,
            },
            expected: Outcome::Ok,
        },
        // log × non-empty × flush after gap-spanning writes → file `<dur>-<max(offsets)>`
        // The buffer carries offsets 0, 461, 466; the snapshot file
        // must name `0-466.parquet` (not `0-2` from len-1).
        Case {
            name: "log/non_empty_with_gaps/flush/uses_max_offset_for_to",
            mode: Mode::Log,
            preload: &[0, 461, 466],
            buffer_state: BufferState::NonEmpty,
            action: Action::Flush {
                expected_from: 0,
                expected_to: 466,
            },
            expected: Outcome::Ok,
        },
        // ============================================================
        //  NEXT_EXPECTED_OFFSET; reflects buffered_head() correctly
        // ============================================================

        // append × non-empty × next_expected → durable + buffer.len()
        Case {
            name: "append/non_empty/next_expected/durable_plus_len",
            mode: Mode::Append,
            preload: &[0, 1, 2],
            buffer_state: BufferState::NonEmpty,
            action: Action::NextExpected,
            expected: Outcome::NextExpectedIs(3),
        },
        // log × non-empty with gaps × next_expected → last_buffered + 1
        Case {
            name: "log/non_empty_with_gaps/next_expected/last_buffered_plus_one",
            mode: Mode::Log,
            preload: &[0, 461, 466],
            buffer_state: BufferState::NonEmpty,
            action: Action::NextExpected,
            expected: Outcome::NextExpectedIs(467),
        },
    ]
}
