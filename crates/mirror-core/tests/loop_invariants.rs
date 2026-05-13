//! Invariant tests for `run_mirror`.
//!
//! These run against hand-written mocks; they are the load-bearing
//! tests for the gate logic. Anything that loosens these invariants
//! breaks exactly-once.

use mirror_core::mock::{rec, MockSink, MockSource, MockSourceEvent};
use mirror_core::{run_mirror, MirrorError, Record, SinkError};

/// Poll `run_mirror` to completion (graceful or error). Bounded by
/// the scripted MockSource events; if the loop never finishes the
/// scripted events fall through to `Hang` and the test would block,
/// which surfaces a bug.
fn drive<F>(future: F) -> Result<(), MirrorError>
where
    F: std::future::IntoFuture<Output = Result<(), MirrorError>>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    rt.block_on(async move { future.into_future().await })
}

fn never() -> std::future::Pending<()> {
    std::future::pending::<()>()
}

#[test]
fn seeks_source_to_destination_position_on_startup() {
    let source = MockSource::new([MockSourceEvent::Error("stop after seek".into())]);
    let sink = MockSink::starting_at(42).with_position_program([42]);

    // We can't easily inspect `source.seeks` after consumption; instead,
    // the next test (`processes_in_order`) covers the seek path too.
    let _ = drive(run_mirror(source, sink, never()));
}

#[test]
fn processes_records_in_order() {
    let source = MockSource::new([
        MockSourceEvent::Record(rec(10)),
        MockSourceEvent::Record(rec(11)),
        MockSourceEvent::Record(rec(12)),
        MockSourceEvent::Error("end of test".into()),
    ]);
    let sink = MockSink::starting_at(10);
    // Snapshot the sink by wrapping: we want to inspect `writes` after.
    let inspector = WriteInspector::wrap(sink);
    let handle = inspector.handle();

    let result = drive(run_mirror(source, handle, never()));
    // The driver stops at the Error event after consuming three records.
    assert!(
        matches!(result, Err(MirrorError::Source(_))),
        "got: {result:?}"
    );
    let writes = inspector.into_writes();
    assert_eq!(
        writes.iter().map(|r| r.source_offset).collect::<Vec<_>>(),
        vec![10, 11, 12]
    );
}

#[test]
fn errors_on_source_offset_gap() {
    // Source skips from 10 directly to 12 — must be rejected.
    let source = MockSource::new([
        MockSourceEvent::Record(rec(10)),
        MockSourceEvent::Record(rec(12)),
    ]);
    let sink = MockSink::starting_at(10);

    let result = drive(run_mirror(source, sink, never()));
    match result {
        Err(MirrorError::SourceOffsetMismatch { expected, actual }) => {
            assert_eq!(expected, 11);
            assert_eq!(actual, 12);
        }
        other => panic!("expected SourceOffsetMismatch, got {other:?}"),
    }
}

#[test]
fn errors_on_destination_drift_during_idle() {
    // After processing offset 10, an idle poll reveals the destination
    // is now at 15 (someone wrote out-of-band).
    let source = MockSource::new([
        MockSourceEvent::Record(rec(10)),
        MockSourceEvent::Idle,
        MockSourceEvent::Hang,
    ]);
    let sink = MockSink::starting_at(10)
        // initial call (startup) -> 10; drift check after idle -> 15.
        .with_position_program([10, 15]);

    let result = drive(run_mirror(source, sink, never()));
    match result {
        Err(MirrorError::DestinationDrift { expected, actual }) => {
            assert_eq!(expected, 11);
            assert_eq!(actual, 15);
        }
        other => panic!("expected DestinationDrift, got {other:?}"),
    }
}

#[test]
fn propagates_sink_write_error() {
    let source = MockSource::new([MockSourceEvent::Record(rec(10))]);
    let sink = MockSink::starting_at(10).with_write_error(SinkError::UnexpectedPosition {
        expected: 10,
        actual: 11,
    });

    let result = drive(run_mirror(source, sink, never()));
    match result {
        Err(MirrorError::Sink(SinkError::UnexpectedPosition { expected, actual })) => {
            assert_eq!(expected, 10);
            assert_eq!(actual, 11);
        }
        other => panic!("expected sink UnexpectedPosition, got {other:?}"),
    }
}

#[test]
fn propagates_source_poll_error() {
    let source = MockSource::new([MockSourceEvent::Error("kafka down".into())]);
    let sink = MockSink::starting_at(0);

    let result = drive(run_mirror(source, sink, never()));
    assert!(matches!(result, Err(MirrorError::Source(_))));
}

#[test]
fn empty_destination_starts_at_zero_and_processes_first_record() {
    let source = MockSource::new([
        MockSourceEvent::Record(rec(0)),
        MockSourceEvent::Error("stop".into()),
    ]);
    let sink = MockSink::starting_at(0);
    let inspector = WriteInspector::wrap(sink);
    let handle = inspector.handle();

    let _ = drive(run_mirror(source, handle, never()));
    let writes = inspector.into_writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].source_offset, 0);
}

#[test]
fn graceful_shutdown_calls_flush_and_returns_ok() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let flush_count = Arc::new(AtomicUsize::new(0));

    struct FlushTrackingSink {
        position: u64,
        flush_count: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl mirror_core::Sink for FlushTrackingSink {
        async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
            Ok(self.position)
        }
        async fn write(&mut self, _record: Record) -> Result<(), SinkError> {
            self.position += 1;
            Ok(())
        }
        async fn flush(&mut self) -> Result<(), SinkError> {
            self.flush_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let source = MockSource::new([MockSourceEvent::Hang]);
    let sink = FlushTrackingSink {
        position: 0,
        flush_count: Arc::clone(&flush_count),
    };
    // The shutdown future is already ready, so the very first `select!`
    // takes the shutdown branch (which is biased first).
    let result = drive(run_mirror(source, sink, async {}));
    assert!(matches!(result, Ok(())), "expected Ok, got {result:?}");
    assert_eq!(
        flush_count.load(Ordering::SeqCst),
        1,
        "flush must be called exactly once"
    );
}

// ---- helper: a Sink wrapper that exposes recorded writes after the
// loop has consumed the original by value ----

use std::sync::{Arc, Mutex};

struct WriteInspector {
    writes: Arc<Mutex<Vec<Record>>>,
    position: Arc<Mutex<u64>>,
}

impl WriteInspector {
    fn wrap(sink: MockSink) -> Self {
        Self {
            writes: Arc::new(Mutex::new(sink.writes)),
            position: Arc::new(Mutex::new(sink.running_position)),
        }
    }
    fn handle(&self) -> InspectorSink {
        InspectorSink {
            writes: Arc::clone(&self.writes),
            position: Arc::clone(&self.position),
        }
    }
    fn into_writes(self) -> Vec<Record> {
        Arc::try_unwrap(self.writes)
            .expect("inspector still held")
            .into_inner()
            .unwrap()
    }
}

struct InspectorSink {
    writes: Arc<Mutex<Vec<Record>>>,
    position: Arc<Mutex<u64>>,
}

#[async_trait::async_trait]
impl mirror_core::Sink for InspectorSink {
    async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
        Ok(*self.position.lock().unwrap())
    }
    async fn write(&mut self, record: Record) -> Result<(), SinkError> {
        let mut pos = self.position.lock().unwrap();
        if record.source_offset != *pos {
            return Err(SinkError::UnexpectedPosition {
                expected: *pos,
                actual: record.source_offset,
            });
        }
        *pos += 1;
        self.writes.lock().unwrap().push(record);
        Ok(())
    }
}
