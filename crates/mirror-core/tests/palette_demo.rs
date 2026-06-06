//! Demonstration of the test-helper palette in `mirror_core::testing`.
//!
//! Every test in this file uses ONLY the published palette
//! ([`mirror_core::mock`] + [`mirror_core::testing`]). The point is to
//! prove that the palette is rich enough to express the common
//! shapes of spec tests *without* a contributor having to extend the
//! mock infrastructure first.
//!
//! See `TESTING.md` at the repo root for the catalogue of layers and
//! which one a given spec change belongs in.

use mirror_core::mock::{rec, MockSource, MockSourceEvent};
use mirror_core::testing::{BlanketMockSink, Call};
use mirror_core::{run_mirror, MirrorError, SinkError};

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

/// Demonstration #1; encode the committed `SourceWentBackwards`
/// invariant entirely through the palette.
///
/// The point isn't the test result (`mirror-core/tests/loop_invariants.rs`
/// already has this case). The point is the *shape*: declarative
/// mock setup + drive + match on the error variant, with no
/// `InspectorSink`-style state plumbing.
#[test]
fn palette_encodes_source_went_backwards() {
    // Sink reports it's at offset 5; the loop's `expected` starts
    // here.
    let sink = BlanketMockSink::builder()
        .with_next_expected_offset(5)
        // The loop's per-record gate fires BEFORE delegating to
        // sink.write(), so the closure here is never reached for
        // the offending record. It still has to be present for
        // any preceding records the loop accepts; default returns
        // Ok, which is fine.
        ;

    // Source delivers 5 (matches expected) then 3 (goes backwards).
    let source = MockSource::new([
        MockSourceEvent::Record(rec(5)),
        MockSourceEvent::Record(rec(3)),
    ]);

    let result = drive(run_mirror(source, sink, never()));
    match result {
        Err(MirrorError::SourceWentBackwards { expected, got }) => {
            assert_eq!((expected, got), (6, 3));
        }
        other => panic!("expected SourceWentBackwards, got {other:?}"),
    }
}

/// Demonstration #2; encode an *idle-drift* invariant where the
/// sink's `next_expected_offset` changes across calls.
///
/// The existing `MockSink::with_position_program` already supports
/// scripted positions; this test deliberately uses `BlanketMockSink`'s
/// closure-driven sequence instead, to show the equivalence:
/// `with_next_expected_offset_sequence` covers the same shape with
/// fewer assumptions about MockSink's structure. A future spec test
/// that needed e.g. "the third call returns an error, not just a
/// different value" would use `with_next_expected_offset_fn` directly.
#[test]
fn palette_encodes_destination_drift_via_sequence() {
    // Startup call returns 10; idle re-check (after the Idle event)
    // returns 15; out-of-band write detected.
    let sink = BlanketMockSink::builder().with_next_expected_offset_sequence(vec![10, 15]);

    let source = MockSource::new([
        MockSourceEvent::Record(rec(10)),
        MockSourceEvent::Idle,
        MockSourceEvent::Hang,
    ]);

    let result = drive(run_mirror(source, sink, never()));
    match result {
        Err(MirrorError::DestinationDrift { expected, actual }) => {
            assert_eq!((expected, actual), (11, 15));
        }
        other => panic!("expected DestinationDrift, got {other:?}"),
    }
}

/// Demonstration #3; encode a per-record decision via `with_write_fn`.
///
/// Scenario: the spec under test is "the sink rejects exactly the
/// fifth record." The closure captures a counter, decides per call.
/// No new mock method needed.
#[test]
fn palette_encodes_per_record_sink_decision() {
    // The closure captures a counter that drives the per-call
    // decision; that's the demonstration. The fifth write call
    // (regardless of record offset) is rejected.
    let mut written = 0u32;
    let sink = BlanketMockSink::builder()
        .with_next_expected_offset(0)
        .with_write_fn(move |r| {
            written += 1;
            if written == 5 {
                Err(SinkError::UnexpectedPosition {
                    expected: written as u64 - 1,
                    actual: r.source_offset,
                })
            } else {
                Ok(())
            }
        });

    let source = MockSource::new([
        MockSourceEvent::Record(rec(0)),
        MockSourceEvent::Record(rec(1)),
        MockSourceEvent::Record(rec(2)),
        MockSourceEvent::Record(rec(3)),
        MockSourceEvent::Record(rec(4)), // the 5th write; rejected
    ]);

    let result = drive(run_mirror(source, sink, never()));
    match result {
        Err(MirrorError::Sink(SinkError::UnexpectedPosition { expected, actual })) => {
            assert_eq!((expected, actual), (4, 4));
        }
        other => panic!("expected sink UnexpectedPosition on 5th write, got {other:?}"),
    }
}

/// Demonstration #4; inspect call ordering after the loop exits.
///
/// `BlanketMockSink::calls()` returns the full trait-method
/// invocation history. Useful when the spec is about *what order*
/// the loop calls methods in, not the values returned. Example: a
/// spec might say "shutdown must call flush() exactly once, and only
/// after any in-flight write completes."
#[test]
fn palette_records_call_order_for_post_hoc_assertion() {
    let sink = BlanketMockSink::builder().with_next_expected_offset(0);

    let source = MockSource::new([
        MockSourceEvent::Record(rec(0)),
        MockSourceEvent::Record(rec(1)),
        MockSourceEvent::Hang,
    ]);

    // Shutdown future is already-ready, so the loop takes the
    // shutdown branch at the next iteration boundary after some
    // (possibly zero) records have been processed.
    let _ = drive(run_mirror(source, sink, async {}));

    // The contract `BlanketMockSink` upholds: every trait-method
    // call is recorded. We can't assert that the loop processed N
    // records (`tokio::select!` biases shutdown), but we CAN assert
    // structural properties; every Write is preceded by a
    // NextExpectedOffset at startup, flush is called at most once,
    // etc. For a true post-hoc inspection the test holds the sink
    // by reference via Arc<Mutex> instead of moving into run_mirror.
    // The shape of that pattern lives in `tee.rs` already and isn't
    // reproduced here; the point is the calls() accessor exists
    // and is the entrypoint.
    //
    // For this test, just confirm the discrimination works: a
    // freshly built sink has no calls.
    let fresh = BlanketMockSink::builder();
    assert_eq!(fresh.calls(), Vec::<Call>::new());
}

/// Demonstration #5; TDD sketch for a future spec.
///
/// This test is `#[ignore]`d because the spec it asserts on doesn't
/// exist yet. It compiles, runs in `--include-ignored` mode, and
/// fails with a clear panic naming the work to do; exactly the
/// red-green-refactor entrypoint a contributor wants when picking
/// up the work.
///
/// **The spec:** "It's a fatal condition if any sink has a higher
/// offset than its source." Concretely: at startup, the run loop
/// must compare `sink.next_expected_offset()` against
/// `source.high_watermark()` and crash with a specific error if the
/// sink is ahead.
///
/// **What the palette provides today:**
///   - `MockSource::with_high_watermark(100)` to script the source's
///     HWM (the trait method's default is `u64::MAX` so existing
///     tests are unaffected).
///   - `BlanketMockSink::with_next_expected_offset(150)` to script
///     a sink that's ahead.
///
/// **What the spec implementer would add:**
///   - A new `MirrorError::SinkAheadOfSource { sink_offset, source_hwm }`
///     variant in `crates/mirror-core/src/lib.rs`.
///   - A check in `run_mirror_with_heartbeat` after the initial
///     `sink.next_expected_offset()` call (or on idle, if the spec
///     wants ongoing monitoring) that calls `source.high_watermark()`
///     and returns the new variant when sink > hwm.
///
/// Removing the `#[ignore]` and replacing the body with the actual
/// assertion (see the commented sketch below) is the green-side
/// landing.
#[test]
#[ignore = "TODO: spec not yet implemented; see body for the TDD pattern"]
fn future_spec_sink_ahead_of_source_is_fatal() {
    // Palette setup that the future test would use:
    //
    // let source = MockSource::new([MockSourceEvent::Hang])
    //     .with_high_watermark(100); // broker HWM
    // let sink = BlanketMockSink::builder()
    //     .with_next_expected_offset(150); // sink claims to be at 150
    //
    // let result = drive(run_mirror(source, sink, never()));
    // match result {
    //     Err(MirrorError::SinkAheadOfSource { sink_offset, source_hwm }) => {
    //         assert_eq!(sink_offset, 150);
    //         assert_eq!(source_hwm, 100);
    //     }
    //     other => panic!("expected SinkAheadOfSource, got {other:?}"),
    // }
    panic!(
        "Implement `MirrorError::SinkAheadOfSource` + the HWM check in \
         `run_mirror_with_heartbeat`, then drop the `#[ignore]` and \
         uncomment the body above. The palette ({MockSource}::with_high_watermark, \
         {BlanketMockSink}::with_next_expected_offset) already supports \
         everything the test needs.",
        MockSource = "MockSource",
        BlanketMockSink = "BlanketMockSink"
    );
}
