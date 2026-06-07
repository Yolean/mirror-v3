//! Pin the contract that a sink's [`WriteObserver`] fires after
//! every successful write and never fires after a failed one. Also
//! pin the bridge `WriteObserver -> AckSink::note_through(offset + 1)`
//! shape the supervisor's per-destination ack collector will use.

use std::sync::Arc;
use std::sync::Mutex;

use mirror_core::mock::{rec, MockSink};
use mirror_core::{AckSink, Sink, SinkError, WriteObserver};

/// Tiny observer that just appends each `on_written` offset.
#[derive(Debug, Default)]
struct RecordingObserver {
    offsets: Mutex<Vec<u64>>,
}

impl WriteObserver for RecordingObserver {
    fn on_written(&self, source_offset: u64) {
        self.offsets.lock().unwrap().push(source_offset);
    }
}

/// AckSink that records every `note_through` value. The supervisor's
/// real ack tracker takes the running max; this stub keeps the raw
/// sequence so a test can assert on what its bridge fed in.
#[derive(Debug, Default)]
struct RecordingAck {
    values: Mutex<Vec<u64>>,
}

impl AckSink for RecordingAck {
    fn note_through(&self, through: u64) {
        self.values.lock().unwrap().push(through);
    }
}

/// A `WriteObserver` that bridges every `on_written(offset)` into
/// `AckSink::note_through(offset + 1)`. This is the exact shape the
/// supervisor's per-destination wiring takes for Kafka sinks. The
/// trait lives in mirror-core; the wiring lives in mirror-bin
/// (committed separately) and isn't part of the public crate.
struct BridgeToAck {
    ack: Arc<dyn AckSink>,
}

impl WriteObserver for BridgeToAck {
    fn on_written(&self, source_offset: u64) {
        self.ack.note_through(source_offset + 1);
    }
}

#[tokio::test]
async fn observer_fires_once_per_successful_write_in_order() {
    let mut sink = MockSink::starting_at(0);
    let obs = Arc::new(RecordingObserver::default());
    sink.set_write_observer(obs.clone() as Arc<dyn WriteObserver>);

    for off in 0..5 {
        sink.write(rec(off)).await.unwrap();
    }

    assert_eq!(
        obs.offsets.lock().unwrap().clone(),
        vec![0, 1, 2, 3, 4],
        "every successful write must fire on_written exactly once, in order"
    );
}

#[tokio::test]
async fn observer_does_not_fire_when_write_rejects_the_gate() {
    // MockSink rejects a record whose offset doesn't match its
    // running position. The observer must not fire for the rejected
    // call.
    let mut sink = MockSink::starting_at(0);
    let obs = Arc::new(RecordingObserver::default());
    sink.set_write_observer(obs.clone() as Arc<dyn WriteObserver>);

    sink.write(rec(0)).await.unwrap();
    // Skip ahead — MockSink expects 1, we send 5.
    let err = sink.write(rec(5)).await.unwrap_err();
    assert!(
        matches!(err, SinkError::UnexpectedPosition { .. }),
        "got {err:?}"
    );

    assert_eq!(
        obs.offsets.lock().unwrap().clone(),
        vec![0],
        "observer must see only the accepted write"
    );
}

#[tokio::test]
async fn observer_does_not_fire_on_a_scripted_write_error() {
    // `with_write_error` makes the next write fail without touching
    // running_position. The observer must not fire.
    let mut sink = MockSink::starting_at(0).with_write_error(SinkError::Transport("boom".into()));
    let obs = Arc::new(RecordingObserver::default());
    sink.set_write_observer(obs.clone() as Arc<dyn WriteObserver>);

    let err = sink.write(rec(0)).await.unwrap_err();
    assert!(matches!(err, SinkError::Transport(_)), "got {err:?}");
    assert!(
        obs.offsets.lock().unwrap().is_empty(),
        "observer must not fire on the failed write"
    );

    // Subsequent successful write fires normally.
    sink.write(rec(0)).await.unwrap();
    assert_eq!(obs.offsets.lock().unwrap().clone(), vec![0]);
}

#[tokio::test]
async fn write_observer_bridge_to_ack_sink_increments_through_offsets() {
    // The supervisor's per-destination shim is exactly this shape:
    // wrap an AckSink in a WriteObserver that translates
    // `on_written(offset)` into `note_through(offset + 1)` (i.e. "the
    // destination is durable through offset + 1").
    let mut sink = MockSink::starting_at(0);
    let ack = Arc::new(RecordingAck::default());
    let bridge = Arc::new(BridgeToAck {
        ack: ack.clone() as Arc<dyn AckSink>,
    });
    sink.set_write_observer(bridge as Arc<dyn WriteObserver>);

    for off in 0..3 {
        sink.write(rec(off)).await.unwrap();
    }

    assert_eq!(
        ack.values.lock().unwrap().clone(),
        vec![1, 2, 3],
        "bridge must hand the ack `offset + 1` per successful write"
    );
}

#[tokio::test]
async fn unsupervised_sink_default_set_write_observer_is_noop() {
    // The default `Sink::set_write_observer` is a no-op. Sinks that
    // don't override it should silently accept the call.
    struct NoOverrideSink {
        position: u64,
    }
    #[async_trait::async_trait]
    impl Sink for NoOverrideSink {
        async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
            Ok(self.position)
        }
        async fn write(&mut self, _record: mirror_core::Record) -> Result<(), SinkError> {
            self.position += 1;
            Ok(())
        }
    }
    let mut sink = NoOverrideSink { position: 0 };
    let obs = Arc::new(RecordingObserver::default());
    sink.set_write_observer(obs.clone() as Arc<dyn WriteObserver>);

    sink.write(rec(0)).await.unwrap();
    assert!(
        obs.offsets.lock().unwrap().is_empty(),
        "default impl must not fire the observer"
    );
}
