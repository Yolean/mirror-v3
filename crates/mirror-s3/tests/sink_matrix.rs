//! Sink-trait matrix against a real `S3Sink` on
//! `object_store::memory::InMemory`. Mirrors
//! `crates/mirror-fs/tests/sink_matrix.rs` cell-for-cell so the two
//! sinks' contracts stay symmetric.
//!
//! Diverges from the FS matrix only where the backend semantics
//! genuinely differ:
//!   - **No file path on disk** — the produced-object-name assertion
//!     reads the InMemory store's object list instead of `read_dir`.
//!   - **Async open** — `S3Sink::open` is async; the rest of the
//!     trait surface is identical.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use mirror_core::{Record, Sink, SinkError, TimestampType};
use mirror_envelope::{ColumnType, Format, ParquetCompression};
use mirror_s3::{CompactionMode, FlushTriggers, S3Sink, S3SinkConfig};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;

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

fn cfg(store: Arc<dyn ObjectStore>, compaction: Option<CompactionMode>) -> S3SinkConfig {
    let format = match compaction {
        Some(CompactionMode::Log) => Format::Parquet,
        None => Format::Ndjson,
    };
    S3SinkConfig {
        store,
        prefix: Some(Path::from("archive")),
        destination_name: "ops".into(),
        partition: 0,
        format,
        compression: ParquetCompression::Zstd1,
        keys: ColumnType::Utf8,
        values: ColumnType::Utf8,
        compaction,
        cache: None,
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets: u64::MAX,
            daily_at_utc_seconds: None,
        },
    }
}

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

#[derive(Debug, Clone, Copy)]
enum BufferState {
    Empty,
    NonEmpty,
}

#[derive(Debug)]
enum Action {
    Write(u64),
    Flush {
        expected_from: u64,
        expected_to: u64,
    },
    Align {
        low_watermark: u64,
    },
    NextExpected,
}

#[derive(Debug)]
enum Outcome {
    Ok,
    NextExpectedIs(u64),
    UnexpectedPosition { expected: u64, actual: u64 },
    TransportContains(&'static str),
}

struct Case {
    name: &'static str,
    mode: Mode,
    preload: &'static [u64],
    buffer_state: BufferState,
    action: Action,
    expected: Outcome,
}

async fn run_case(case: &Case) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut sink = S3Sink::open(cfg(Arc::clone(&store), case.mode.to_compaction()))
        .await
        .expect("open S3Sink");

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

    let observed = match &case.action {
        Action::Write(offset) => sink.write(rec(*offset)).await.map(|()| None),
        Action::Flush { .. } => sink.flush_now().await.map(|()| None),
        Action::Align { low_watermark } => sink
            .align_to_source_low_watermark(*low_watermark)
            .await
            .map(|()| None),
        Action::NextExpected => sink.next_expected_offset().await.map(Some),
    };

    // Filename check happens out-of-band: it needs an async listing
    // call on the store, which can't easily be threaded through the
    // synchronous `.map` chain above.
    if let Action::Flush {
        expected_from,
        expected_to,
    } = &case.action
    {
        if observed.is_ok() {
            let prefix = Path::from("archive/ops/0");
            let mut stream = store.list(Some(&prefix));
            let mut names: Vec<String> = Vec::new();
            while let Some(meta) = stream.next().await {
                if let Some(name) = meta.expect("list entry").location.filename() {
                    names.push(name.to_string());
                }
            }
            names.sort();
            let last = names
                .last()
                .unwrap_or_else(|| panic!("[{}] no flushed object found", case.name));
            let ext = if matches!(case.mode, Mode::Log) {
                "parquet"
            } else {
                "ndjson"
            };
            let expected_name = format!("{expected_from:020}-{expected_to:020}.{ext}");
            assert_eq!(
                last, &expected_name,
                "[{}] flushed object name should encode (from={expected_from}, to={expected_to})",
                case.name
            );
        }
    }

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
    for case in &matrix_cases() {
        run_case(case).await;
    }
}

fn matrix_cases() -> Vec<Case> {
    vec![
        // ============================================================
        //  APPEND MODE
        // ============================================================
        Case {
            name: "append/empty/write_at_expected/ok",
            mode: Mode::Append,
            preload: &[],
            buffer_state: BufferState::Empty,
            action: Action::Write(0),
            expected: Outcome::Ok,
        },
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
        Case {
            name: "append/empty_after_flush/write_below_durable/rejects",
            mode: Mode::Append,
            preload: &[0, 1, 2, 3, 4],
            buffer_state: BufferState::Empty,
            action: Action::Write(3),
            expected: Outcome::UnexpectedPosition {
                expected: 5,
                actual: 3,
            },
        },
        Case {
            name: "append/non_empty/write_at_expected/ok",
            mode: Mode::Append,
            preload: &[0, 1, 2],
            buffer_state: BufferState::NonEmpty,
            action: Action::Write(3),
            expected: Outcome::Ok,
        },
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
        //  COMPACTION:LOG
        // ============================================================
        Case {
            name: "log/empty/write_at_expected/ok",
            mode: Mode::Log,
            preload: &[],
            buffer_state: BufferState::Empty,
            action: Action::Write(0),
            expected: Outcome::Ok,
        },
        Case {
            name: "log/empty/write_above_expected/ok_bootstrap_gap",
            mode: Mode::Log,
            preload: &[],
            buffer_state: BufferState::Empty,
            action: Action::Write(461),
            expected: Outcome::Ok,
        },
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
        Case {
            name: "log/non_empty/write_at_expected/ok",
            mode: Mode::Log,
            preload: &[0, 1, 2],
            buffer_state: BufferState::NonEmpty,
            action: Action::Write(3),
            expected: Outcome::Ok,
        },
        Case {
            name: "log/non_empty/write_above_expected/ok_midstream_gap",
            mode: Mode::Log,
            preload: &[0, 1, 2],
            buffer_state: BufferState::NonEmpty,
            action: Action::Write(7),
            expected: Outcome::Ok,
        },
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
        //  ALIGN
        // ============================================================
        Case {
            name: "log/empty/align/ok",
            mode: Mode::Log,
            preload: &[],
            buffer_state: BufferState::Empty,
            action: Action::Align { low_watermark: 461 },
            expected: Outcome::Ok,
        },
        Case {
            name: "log/non_empty/align/rejects_with_empty_buffer_precondition",
            mode: Mode::Log,
            preload: &[0, 1, 2],
            buffer_state: BufferState::NonEmpty,
            action: Action::Align { low_watermark: 461 },
            expected: Outcome::TransportContains("inconsistent state"),
        },
        Case {
            name: "append/empty/align/rejects_on_non_compaction_sink",
            mode: Mode::Append,
            preload: &[],
            buffer_state: BufferState::Empty,
            action: Action::Align { low_watermark: 461 },
            expected: Outcome::TransportContains("non-compaction sink"),
        },
        // ============================================================
        //  FLUSH
        // ============================================================
        Case {
            name: "append/non_empty/flush/contiguous_object_name",
            mode: Mode::Append,
            preload: &[0, 1, 2, 3, 4],
            buffer_state: BufferState::NonEmpty,
            action: Action::Flush {
                expected_from: 0,
                expected_to: 4,
            },
            expected: Outcome::Ok,
        },
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
        //  NEXT_EXPECTED_OFFSET
        // ============================================================
        Case {
            name: "append/non_empty/next_expected/durable_plus_len",
            mode: Mode::Append,
            preload: &[0, 1, 2],
            buffer_state: BufferState::NonEmpty,
            action: Action::NextExpected,
            expected: Outcome::NextExpectedIs(3),
        },
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
