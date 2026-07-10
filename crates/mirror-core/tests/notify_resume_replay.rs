//! Loop-level test of the committed-offset resume mechanism: when
//! the tee's resume floor sits below the destinations' durable
//! heads (records were made durable but their notify batch was
//! never acked/committed), the loop re-reads the gap from the
//! source, hands every record to the notifier, skips the
//! destination writes, and keeps the idle drift re-check satisfied
//! throughout the replay.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use mirror_core::mock::{rec, MockSink, MockSource, MockSourceEvent};
use mirror_core::{run_mirror_with_notifier, MirrorError, Notifier, NotifyError, Record, TeeSink};

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

struct RecordingNotifier {
    seen: Arc<Mutex<Vec<u64>>>,
}

#[async_trait]
impl Notifier for RecordingNotifier {
    async fn on_record(&mut self, record: &Record) -> Result<(), NotifyError> {
        self.seen.lock().unwrap().push(record.source_offset);
        Ok(())
    }
}

#[test]
fn resume_floor_replays_unacked_window_to_notifier_without_rewriting_destination() {
    // Destination durable through offset 4 (head 5); committed
    // (notify-acked) only through 2. The supervisor sets the floor
    // to the committed offset; the source re-delivers 2..=6, with
    // idle polls interleaved to exercise the drift re-check both
    // inside and after the replay window.
    let source = MockSource::new([
        MockSourceEvent::Record(rec(2)),
        MockSourceEvent::Idle,
        MockSourceEvent::Record(rec(3)),
        MockSourceEvent::Record(rec(4)),
        MockSourceEvent::Idle,
        MockSourceEvent::Record(rec(5)),
        MockSourceEvent::Record(rec(6)),
        MockSourceEvent::Idle,
        MockSourceEvent::Error("end of test".into()),
    ]);
    let sink = MockSink::starting_at(5);
    let mut tee = TeeSink::from_inners_for_test(vec![("dest".into(), Box::new(sink), 5)], None);
    tee.set_resume_floor(2);

    let seen = Arc::new(Mutex::new(Vec::new()));
    let notifier = RecordingNotifier {
        seen: Arc::clone(&seen),
    };

    let result = drive(run_mirror_with_notifier(
        source,
        tee,
        notifier,
        never(),
        std::time::Duration::ZERO,
    ));
    assert!(
        matches!(result, Err(MirrorError::Source(_))),
        "loop must run through the whole script (no drift error mid-replay); got: {result:?}"
    );

    assert_eq!(
        seen.lock().unwrap().clone(),
        vec![2, 3, 4, 5, 6],
        "the notifier must observe the full re-read window plus the new records"
    );
}
