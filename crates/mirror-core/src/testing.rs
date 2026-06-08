//! Test-only helpers for TDD-style spec authoring.
//!
//! The existing [`crate::mock`] types ([`crate::mock::MockSink`],
//! [`crate::mock::MockSource`]) cover the common case where a spec
//! test just needs to script events and scripted positions.
//!
//! This module adds primitives for the *uncommon* case: a spec test
//! that needs a `Sink` or `Source` with behaviour the existing
//! mocks don't model directly; typically because the spec is being
//! TDD'd before the implementation exists, and the test wants to
//! express "next_expected_offset returns 150 and write fails with
//! UnexpectedPosition" without anyone adding a new builder method
//! to MockSink first.
//!
//! ## When to reach for `BlanketMockSink`
//!
//! - You're writing a test for a spec change that hasn't been
//!   implemented yet, and you want the test to compile and fail
//!   loudly (the "red" of red-green-refactor) without changing
//!   shared mock APIs.
//! - You need a Sink whose behaviour changes across calls (each
//!   `next_expected_offset` returns a different value, `write`
//!   succeeds the first time but errors the second, …).
//! - The existing `MockSink` builder doesn't expose the override
//!   you need *and* the override is genuinely test-only (i.e. it
//!   would be wrong to add it to the production-facing mock API).
//!
//! ## When NOT to
//!
//! For straightforward "sink starts at offset N, accepts contiguous
//! writes" the plain [`crate::mock::MockSink`] is cheaper to read.
//! Reach for `BlanketMockSink` only when the closures' flexibility
//! is actually paying for itself.

use std::sync::Mutex;

use async_trait::async_trait;

use crate::{Record, Sink, SinkError};

/// A `Sink` whose every trait method is a closure the test owns.
///
/// Built via the [`BlanketMockSink::builder`] entrypoint and the
/// `with_*` methods. Each closure is `FnMut`, so it can capture
/// mutable state (call counters, scripted return sequences, etc.)
/// from the test's stack frame.
///
/// All recorded calls are accessible via the [`BlanketMockSink::calls`]
/// accessor for post-hoc assertions.
pub struct BlanketMockSink {
    on_next_expected_offset: Box<dyn FnMut() -> Result<u64, SinkError> + Send>,
    on_write: Box<dyn FnMut(Record) -> Result<(), SinkError> + Send>,
    on_flush: Box<dyn FnMut() -> Result<(), SinkError> + Send>,
    on_allows_compacted_source: bool,
    on_align_to_source_low_watermark: Box<dyn FnMut(u64) -> Result<(), SinkError> + Send>,
    /// Recorded calls, in order, for the test to assert on.
    calls: Mutex<Vec<Call>>,
}

/// Trace of one trait-method invocation, for post-hoc assertion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Call {
    NextExpectedOffset,
    Write { source_offset: u64 },
    Flush,
    AllowsCompactedSource,
    AlignToSourceLowWatermark { low_watermark: u64 },
}

impl Default for BlanketMockSink {
    fn default() -> Self {
        Self {
            on_next_expected_offset: Box::new(|| Ok(0)),
            on_write: Box::new(|_| Ok(())),
            on_flush: Box::new(|| Ok(())),
            on_allows_compacted_source: false,
            on_align_to_source_low_watermark: Box::new(|_| Ok(())),
            calls: Mutex::new(Vec::new()),
        }
    }
}

impl BlanketMockSink {
    /// Start a builder from defaults: every method returns `Ok` and
    /// `allows_compacted_source` is `false`. Override individually
    /// with `with_*`.
    pub fn builder() -> Self {
        Self::default()
    }

    /// `next_expected_offset` returns this fixed value on every call.
    /// For varying values across calls use [`Self::with_next_expected_offset_fn`]
    /// or [`Self::with_next_expected_offset_sequence`].
    pub fn with_next_expected_offset(mut self, value: u64) -> Self {
        self.on_next_expected_offset = Box::new(move || Ok(value));
        self
    }

    /// `next_expected_offset` returns each value in `values` in turn,
    /// then errors with a transport error once exhausted. Useful for
    /// "first call returns X, second call returns Y" idle-drift tests.
    pub fn with_next_expected_offset_sequence(mut self, values: Vec<u64>) -> Self {
        let mut iter = values.into_iter();
        self.on_next_expected_offset = Box::new(move || match iter.next() {
            Some(v) => Ok(v),
            None => Err(SinkError::Transport(
                "BlanketMockSink: next_expected_offset sequence exhausted".into(),
            )),
        });
        self
    }

    /// Full closure override for `next_expected_offset`. The closure
    /// is invoked on every call; capture state via the closure to
    /// implement test-specific behaviour.
    pub fn with_next_expected_offset_fn<F>(mut self, f: F) -> Self
    where
        F: FnMut() -> Result<u64, SinkError> + Send + 'static,
    {
        self.on_next_expected_offset = Box::new(f);
        self
    }

    /// `write` returns this error on every call. Useful for "the sink
    /// rejects everything" tests; for selective rejection use
    /// [`Self::with_write_fn`].
    pub fn with_write_always_errors(mut self, err: SinkError) -> Self {
        // SinkError isn't Clone, so we wrap in Mutex<Option<_>> and
        // re-emit by reconstructing the variant from a recorded copy.
        let stored = std::sync::Arc::new(Mutex::new(Some(err)));
        self.on_write = Box::new(move |_| {
            let mut slot = stored.lock().unwrap();
            // Reconstruct an equivalent error each call; match on
            // the originally-stored variant if it's still there;
            // synthesise a Transport variant after the first call so
            // SinkError doesn't need to be Clone.
            match slot.take() {
                Some(e) => Err(e),
                None => Err(SinkError::Transport(
                    "BlanketMockSink::with_write_always_errors (subsequent call)".into(),
                )),
            }
        });
        self
    }

    /// Full closure override for `write`. The closure receives the
    /// `Record` and returns `Result<(), SinkError>`. Capture mutable
    /// state in the closure for per-call decisions.
    pub fn with_write_fn<F>(mut self, f: F) -> Self
    where
        F: FnMut(Record) -> Result<(), SinkError> + Send + 'static,
    {
        self.on_write = Box::new(f);
        self
    }

    /// Full closure override for `flush`.
    pub fn with_flush_fn<F>(mut self, f: F) -> Self
    where
        F: FnMut() -> Result<(), SinkError> + Send + 'static,
    {
        self.on_flush = Box::new(f);
        self
    }

    /// Set the value returned by `allows_compacted_source`. Plain
    /// boolean because the trait method isn't async.
    pub fn with_allows_compacted_source(mut self, value: bool) -> Self {
        self.on_allows_compacted_source = value;
        self
    }

    /// Full closure override for `align_to_source_low_watermark`.
    pub fn with_align_to_source_low_watermark_fn<F>(mut self, f: F) -> Self
    where
        F: FnMut(u64) -> Result<(), SinkError> + Send + 'static,
    {
        self.on_align_to_source_low_watermark = Box::new(f);
        self
    }

    /// Snapshot of trait-method calls in invocation order.
    pub fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl Sink for BlanketMockSink {
    async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
        self.calls.lock().unwrap().push(Call::NextExpectedOffset);
        (self.on_next_expected_offset)()
    }

    async fn write(&mut self, record: Record) -> Result<(), SinkError> {
        self.calls.lock().unwrap().push(Call::Write {
            source_offset: record.source_offset,
        });
        (self.on_write)(record)
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        self.calls.lock().unwrap().push(Call::Flush);
        (self.on_flush)()
    }

    fn allows_compacted_source(&self) -> bool {
        self.calls.lock().unwrap().push(Call::AllowsCompactedSource);
        self.on_allows_compacted_source
    }

    async fn align_to_source_low_watermark(&mut self, low_watermark: u64) -> Result<(), SinkError> {
        self.calls
            .lock()
            .unwrap()
            .push(Call::AlignToSourceLowWatermark { low_watermark });
        (self.on_align_to_source_low_watermark)(low_watermark)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TimestampType;

    fn rec(offset: u64) -> Record {
        Record {
            topic: "t".into(),
            partition: 0,
            source_offset: offset,
            timestamp_ms: Some(1),
            timestamp_type: TimestampType::CreateTime,
            key: Some(b"k".to_vec()),
            value: Some(b"v".to_vec()),
            headers: vec![],
        }
    }

    #[tokio::test]
    async fn defaults_return_ok_zero_and_record_calls() {
        let mut s = BlanketMockSink::builder();
        assert_eq!(s.next_expected_offset().await.unwrap(), 0);
        s.write(rec(0)).await.unwrap();
        s.flush().await.unwrap();
        assert!(!s.allows_compacted_source());
        s.align_to_source_low_watermark(42).await.unwrap();
        assert_eq!(
            s.calls(),
            vec![
                Call::NextExpectedOffset,
                Call::Write { source_offset: 0 },
                Call::Flush,
                Call::AllowsCompactedSource,
                Call::AlignToSourceLowWatermark { low_watermark: 42 },
            ]
        );
    }

    #[tokio::test]
    async fn next_expected_sequence_advances_per_call() {
        let mut s = BlanketMockSink::builder().with_next_expected_offset_sequence(vec![10, 20, 30]);
        assert_eq!(s.next_expected_offset().await.unwrap(), 10);
        assert_eq!(s.next_expected_offset().await.unwrap(), 20);
        assert_eq!(s.next_expected_offset().await.unwrap(), 30);
        // Fourth call: sequence exhausted -> transport error.
        match s.next_expected_offset().await {
            Err(SinkError::Transport(msg)) => assert!(msg.contains("exhausted")),
            other => panic!("expected exhaustion error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn closure_can_capture_mutable_state() {
        // The decision depends on captured state (the call counter),
        // not just the record's intrinsics; this is the test's
        // whole point. Reject the 3rd write call regardless of which
        // offset it carries.
        let mut written = 0u32;
        let mut s = BlanketMockSink::builder().with_write_fn(move |r| {
            written += 1;
            if written == 3 {
                Err(SinkError::UnexpectedPosition {
                    expected: 99,
                    actual: r.source_offset,
                })
            } else {
                Ok(())
            }
        });
        s.write(rec(10)).await.unwrap();
        s.write(rec(11)).await.unwrap();
        match s.write(rec(12)).await {
            Err(SinkError::UnexpectedPosition { expected, actual }) => {
                assert_eq!((expected, actual), (99, 12));
            }
            other => panic!("got {other:?}"),
        }
    }
}
