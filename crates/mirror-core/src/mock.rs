//! Hand-written mocks for testing the mirror loop.
//!
//! These are public so downstream crates (notably the e2e harness in
//! Phase 2) can reuse them, but the API is `#[doc(hidden)]`-ish: it
//! exists to be shaped by the tests next to it.

use async_trait::async_trait;
use std::collections::VecDeque;

use crate::{Record, Sink, SinkError, Source, SourceError};

/// Scriptable [`Source`] that returns canned events. Records seek
/// calls and poll results so tests can assert on them.
pub struct MockSource {
    events: VecDeque<MockSourceEvent>,
    pub seeks: Vec<u64>,
}

pub enum MockSourceEvent {
    /// Return `Ok(Some(record))` on next poll.
    Record(Record),
    /// Return `Ok(None)` on next poll (idle window).
    Idle,
    /// Return `Err(...)` on next poll.
    Error(String),
    /// Block forever once reached (no further events scripted).
    Hang,
}

impl MockSource {
    pub fn new(events: impl IntoIterator<Item = MockSourceEvent>) -> Self {
        Self {
            events: events.into_iter().collect(),
            seeks: Vec::new(),
        }
    }
}

#[async_trait]
impl Source for MockSource {
    async fn seek(&mut self, next_offset: u64) -> Result<(), SourceError> {
        self.seeks.push(next_offset);
        Ok(())
    }

    async fn poll_one(&mut self) -> Result<Option<Record>, SourceError> {
        match self.events.pop_front() {
            Some(MockSourceEvent::Record(r)) => Ok(Some(r)),
            Some(MockSourceEvent::Idle) => Ok(None),
            Some(MockSourceEvent::Error(e)) => Err(SourceError::Transport(e)),
            Some(MockSourceEvent::Hang) | None => {
                // Park forever; tests with timeouts will cancel.
                futures_pending().await;
                unreachable!()
            }
        }
    }
}

async fn futures_pending() {
    // Hand-rolled tiny pending future to avoid pulling in `futures`.
    struct Pending;
    impl std::future::Future for Pending {
        type Output = ();
        fn poll(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            std::task::Poll::Pending
        }
    }
    Pending.await
}

/// Scriptable [`Sink`]. `position_program` queues the values returned
/// by successive `next_expected_offset()` calls; once exhausted, the
/// recorded position (i.e. `start + writes.len()`) is returned, which
/// is the realistic behaviour of a real destination.
pub struct MockSink {
    pub position_program: VecDeque<u64>,
    pub writes: Vec<Record>,
    /// If set, `write()` returns this error and does not record.
    pub write_error: Option<SinkError>,
    /// Starting position used when `position_program` is empty.
    pub running_position: u64,
}

impl MockSink {
    pub fn starting_at(offset: u64) -> Self {
        Self {
            position_program: VecDeque::new(),
            writes: Vec::new(),
            write_error: None,
            running_position: offset,
        }
    }

    pub fn with_position_program(mut self, positions: impl IntoIterator<Item = u64>) -> Self {
        self.position_program = positions.into_iter().collect();
        self
    }

    pub fn with_write_error(mut self, err: SinkError) -> Self {
        self.write_error = Some(err);
        self
    }
}

#[async_trait]
impl Sink for MockSink {
    async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
        if let Some(p) = self.position_program.pop_front() {
            Ok(p)
        } else {
            Ok(self.running_position)
        }
    }

    async fn write(&mut self, record: Record) -> Result<(), SinkError> {
        if let Some(err) = self.write_error.take() {
            return Err(err);
        }
        if record.source_offset != self.running_position {
            return Err(SinkError::UnexpectedPosition {
                expected: self.running_position,
                actual: record.source_offset,
            });
        }
        self.running_position += 1;
        self.writes.push(record);
        Ok(())
    }
}

/// Convenience constructor.
pub fn rec(offset: u64) -> Record {
    Record {
        source_offset: offset,
        key: Some(format!("k{offset}").into_bytes()),
        value: Some(format!("v{offset}").into_bytes()),
        timestamp_ms: Some(1_700_000_000_000 + offset as i64),
        headers: vec![],
    }
}
