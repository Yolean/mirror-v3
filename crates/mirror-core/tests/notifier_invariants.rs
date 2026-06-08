//! Invariant tests for the [`Notifier`] hook in `run_mirror`.
//!
//! These pin the contract every notifier implementation must honour:
//!   * `on_record` fires exactly once per successful `sink.write`,
//!     in source-offset order, *after* the destination has accepted
//!     the record.
//!   * `shutdown` fires once on graceful exit, *after* `sink.flush`.
//!   * `NotifyError` returned from either hook aborts the loop and
//!     surfaces as [`MirrorError::Notify`].
//!   * The hook never fires on the rejection paths
//!     (source-went-backwards, sink write error, etc.).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use mirror_core::mock::{rec, MockSink, MockSource, MockSourceEvent};
use mirror_core::{
    run_mirror_with_notifier, MirrorError, Notifier, NotifyError, Record, Sink, SinkError,
};

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

fn no_heartbeat() -> std::time::Duration {
    std::time::Duration::ZERO
}

/// Records every `on_record` and `shutdown` call. Configurable to
/// return a `NotifyError` on a specific record offset, or on shutdown.
#[derive(Default)]
struct RecordingNotifier {
    log: Arc<Mutex<Vec<NotifierEvent>>>,
    fail_on_offset: Option<u64>,
    fail_on_shutdown: bool,
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum NotifierEvent {
    OnRecord(u64),
    Shutdown,
}

impl RecordingNotifier {
    fn new() -> Self {
        Self::default()
    }

    fn fail_on(mut self, offset: u64) -> Self {
        self.fail_on_offset = Some(offset);
        self
    }

    fn fail_on_shutdown(mut self) -> Self {
        self.fail_on_shutdown = true;
        self
    }

    fn log_handle(&self) -> Arc<Mutex<Vec<NotifierEvent>>> {
        Arc::clone(&self.log)
    }
}

#[async_trait]
impl Notifier for RecordingNotifier {
    async fn on_record(&mut self, record: &Record) -> Result<(), NotifyError> {
        self.log
            .lock()
            .unwrap()
            .push(NotifierEvent::OnRecord(record.source_offset));
        if Some(record.source_offset) == self.fail_on_offset {
            return Err(NotifyError::Transport(format!(
                "boom at offset {}",
                record.source_offset
            )));
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), NotifyError> {
        self.log.lock().unwrap().push(NotifierEvent::Shutdown);
        if self.fail_on_shutdown {
            return Err(NotifyError::Exhausted {
                attempts: 5,
                last_error: "shutdown drain failed".into(),
            });
        }
        Ok(())
    }
}

#[test]
fn on_record_fires_once_per_successful_write_in_offset_order() {
    let source = MockSource::new([
        MockSourceEvent::Record(rec(10)),
        MockSourceEvent::Record(rec(11)),
        MockSourceEvent::Record(rec(12)),
        MockSourceEvent::Error("stop".into()),
    ]);
    let sink = MockSink::starting_at(10);
    let notifier = RecordingNotifier::new();
    let log = notifier.log_handle();

    let result = drive(run_mirror_with_notifier(
        source,
        sink,
        notifier,
        never(),
        no_heartbeat(),
    ));
    assert!(matches!(result, Err(MirrorError::Source(_))));

    let log = log.lock().unwrap().clone();
    assert_eq!(
        log,
        vec![
            NotifierEvent::OnRecord(10),
            NotifierEvent::OnRecord(11),
            NotifierEvent::OnRecord(12),
        ],
        "notifier must observe every accepted record in offset order, and only those"
    );
}

#[test]
fn shutdown_fires_after_flush_on_graceful_exit() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let flush_count = Arc::new(AtomicUsize::new(0));
    let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));

    struct OrderingSink {
        position: u64,
        flush_count: Arc<AtomicUsize>,
        order: Arc<Mutex<Vec<&'static str>>>,
    }
    #[async_trait]
    impl Sink for OrderingSink {
        async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
            Ok(self.position)
        }
        async fn write(&mut self, _record: Record) -> Result<(), SinkError> {
            self.position += 1;
            Ok(())
        }
        async fn flush(&mut self) -> Result<(), SinkError> {
            self.flush_count.fetch_add(1, Ordering::SeqCst);
            self.order.lock().unwrap().push("sink.flush");
            Ok(())
        }
    }

    struct OrderingNotifier {
        order: Arc<Mutex<Vec<&'static str>>>,
        log: Arc<Mutex<Vec<NotifierEvent>>>,
    }
    #[async_trait]
    impl Notifier for OrderingNotifier {
        async fn on_record(&mut self, record: &Record) -> Result<(), NotifyError> {
            self.log
                .lock()
                .unwrap()
                .push(NotifierEvent::OnRecord(record.source_offset));
            Ok(())
        }
        async fn shutdown(&mut self) -> Result<(), NotifyError> {
            self.order.lock().unwrap().push("notifier.shutdown");
            self.log.lock().unwrap().push(NotifierEvent::Shutdown);
            Ok(())
        }
    }

    let log = Arc::new(Mutex::new(Vec::<NotifierEvent>::new()));
    let source = MockSource::new([MockSourceEvent::Hang]);
    let sink = OrderingSink {
        position: 0,
        flush_count: Arc::clone(&flush_count),
        order: Arc::clone(&order),
    };
    let notifier = OrderingNotifier {
        order: Arc::clone(&order),
        log: Arc::clone(&log),
    };

    // Shutdown future already ready -> biased select takes shutdown
    // branch immediately on first iteration.
    let result = drive(run_mirror_with_notifier(
        source,
        sink,
        notifier,
        async {},
        no_heartbeat(),
    ));
    assert!(matches!(result, Ok(())), "expected Ok, got {result:?}");
    assert_eq!(flush_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        order.lock().unwrap().clone(),
        vec!["sink.flush", "notifier.shutdown"],
        "sink.flush must run before notifier.shutdown so the destination is durable before the webhook drain"
    );
}

#[test]
fn notify_error_from_on_record_propagates_as_mirror_error() {
    let source = MockSource::new([
        MockSourceEvent::Record(rec(0)),
        MockSourceEvent::Record(rec(1)), // never reached
    ]);
    let sink = MockSink::starting_at(0);
    let notifier = RecordingNotifier::new().fail_on(0);
    let log = notifier.log_handle();

    let result = drive(run_mirror_with_notifier(
        source,
        sink,
        notifier,
        never(),
        no_heartbeat(),
    ));
    match result {
        Err(MirrorError::Notify(NotifyError::Transport(msg))) => {
            assert!(msg.contains("offset 0"), "got: {msg}");
        }
        other => panic!("expected MirrorError::Notify(Transport), got {other:?}"),
    }
    let log = log.lock().unwrap().clone();
    assert_eq!(
        log,
        vec![NotifierEvent::OnRecord(0)],
        "loop must abort after the failing on_record, never observing offset 1"
    );
}

#[test]
fn notify_error_from_shutdown_propagates_as_mirror_error() {
    let source = MockSource::new([MockSourceEvent::Hang]);
    let sink = MockSink::starting_at(0);
    let notifier = RecordingNotifier::new().fail_on_shutdown();

    let result = drive(run_mirror_with_notifier(
        source,
        sink,
        notifier,
        async {},
        no_heartbeat(),
    ));
    match result {
        Err(MirrorError::Notify(NotifyError::Exhausted {
            attempts,
            last_error,
        })) => {
            assert_eq!(attempts, 5);
            assert_eq!(last_error, "shutdown drain failed");
        }
        other => panic!("expected MirrorError::Notify(Exhausted), got {other:?}"),
    }
}

#[test]
fn on_record_does_not_fire_when_sink_write_fails() {
    let source = MockSource::new([MockSourceEvent::Record(rec(0))]);
    let sink = MockSink::starting_at(0).with_write_error(SinkError::Transport("disk full".into()));
    let notifier = RecordingNotifier::new();
    let log = notifier.log_handle();

    let result = drive(run_mirror_with_notifier(
        source,
        sink,
        notifier,
        never(),
        no_heartbeat(),
    ));
    assert!(matches!(
        result,
        Err(MirrorError::Sink(SinkError::Transport(_)))
    ));
    assert!(
        log.lock().unwrap().is_empty(),
        "notifier must not observe a record the destination rejected"
    );
}

#[test]
fn on_record_does_not_fire_on_source_went_backwards() {
    // Source delivers 10 then 9. Loop must error before ever calling
    // sink.write; and therefore before on_record.
    let source = MockSource::new([
        MockSourceEvent::Record(rec(10)),
        MockSourceEvent::Record(rec(9)),
    ]);
    let sink = MockSink::starting_at(10).with_allows_compacted_source(true);
    let notifier = RecordingNotifier::new();
    let log = notifier.log_handle();

    let result = drive(run_mirror_with_notifier(
        source,
        sink,
        notifier,
        never(),
        no_heartbeat(),
    ));
    assert!(matches!(
        result,
        Err(MirrorError::SourceWentBackwards { .. })
    ));
    // The first record (offset 10) IS accepted and observed; the
    // backwards record (offset 9) must not be.
    let log = log.lock().unwrap().clone();
    assert_eq!(log, vec![NotifierEvent::OnRecord(10)]);
}

/// Compaction-tolerant sink: accepts forward gaps when
/// `allows_compacted_source = true`, mirroring the real FS/S3 sinks.
/// `MockSink` is too strict for the gap test below.
struct CompactionLogSink {
    position: u64,
}
#[async_trait]
impl Sink for CompactionLogSink {
    async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
        Ok(self.position)
    }
    async fn write(&mut self, record: Record) -> Result<(), SinkError> {
        if record.source_offset < self.position {
            return Err(SinkError::UnexpectedPosition {
                expected: self.position,
                actual: record.source_offset,
            });
        }
        // Forward gap accepted under compaction:log; tracker jumps
        // to the delivered offset + 1.
        self.position = record.source_offset + 1;
        Ok(())
    }
    fn allows_compacted_source(&self) -> bool {
        true
    }
    async fn align_to_source_low_watermark(&mut self, low_watermark: u64) -> Result<(), SinkError> {
        self.position = low_watermark;
        Ok(())
    }
}

#[test]
fn on_record_fires_for_gapped_offsets_under_compaction_log() {
    // Mirrors `compaction_log_accepts_repeated_gaps_mid_stream` in
    // loop_invariants.rs: under compaction:log the loop must accept
    // forward gaps, and the notifier must see each accepted offset
    // (KKV semantics: every committed record is a stale-key
    // invalidation event downstream).
    let source = MockSource::new([
        MockSourceEvent::Record(rec(461)),
        MockSourceEvent::Record(rec(466)),
        MockSourceEvent::Record(rec(470)),
        MockSourceEvent::Error("stop".into()),
    ])
    .with_low_watermark(0);
    let sink = CompactionLogSink { position: 0 };
    let notifier = RecordingNotifier::new();
    let log = notifier.log_handle();

    let result = drive(run_mirror_with_notifier(
        source,
        sink,
        notifier,
        never(),
        no_heartbeat(),
    ));
    assert!(
        matches!(result, Err(MirrorError::Source(_))),
        "got: {result:?}"
    );

    let log = log.lock().unwrap().clone();
    assert_eq!(
        log,
        vec![
            NotifierEvent::OnRecord(461),
            NotifierEvent::OnRecord(466),
            NotifierEvent::OnRecord(470),
        ]
    );
}
