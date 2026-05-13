//! Core mirror loop and trait surface.
//!
//! The loop is generic over [`Source`] and [`Sink`]. The single
//! correctness invariant is enforced here, in [`run_mirror`]:
//!
//! 1. On startup, ask the sink for `next_expected_offset()`. That is
//!    the source offset we will seek to and the source offset we will
//!    refuse to write anything else at.
//! 2. Every record produced by the source must carry that same offset
//!    (`Record::source_offset`). A gap is a hard error.
//! 3. The sink is contracted to write the record only if the
//!    destination is still at exactly `record.source_offset`, and to
//!    error otherwise.
//! 4. On an idle poll (no record available), we re-read the sink's
//!    `next_expected_offset()` and require it to still equal what we
//!    expect. This catches external topic resets / out-of-band writes.

use async_trait::async_trait;
use thiserror::Error;

pub mod mock;

/// A record in transit. `source_offset` is the partition offset on the
/// *source* topic; the loop and the sink both gate on this value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub source_offset: u64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub timestamp_ms: Option<i64>,
    pub headers: Vec<Header>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

/// A Kafka-shaped record stream pinned to one (topic, partition).
#[async_trait]
pub trait Source: Send {
    /// Position the source so the next `poll_one` returns the record
    /// at `next_offset` (or `None` until one is available).
    async fn seek(&mut self, next_offset: u64) -> Result<(), SourceError>;

    /// Wait up to an implementation-defined poll timeout for the next
    /// record. `Ok(None)` means the window elapsed without one — the
    /// loop will use that as a heartbeat to revalidate the sink.
    async fn poll_one(&mut self) -> Result<Option<Record>, SourceError>;
}

/// A destination for exactly-once mirroring. The sink owns the truth
/// about "where we are" — the loop trusts `next_expected_offset`.
#[async_trait]
pub trait Sink: Send {
    /// The source offset the destination will accept next. Must be
    /// re-derived from durable destination state, not cached in memory
    /// (otherwise the idle-drift check is meaningless).
    async fn next_expected_offset(&mut self) -> Result<u64, SinkError>;

    /// Atomically commit `record` at exactly `record.source_offset`.
    /// MUST fail if the destination is not at that offset at the
    /// moment of write.
    async fn write(&mut self, record: Record) -> Result<(), SinkError>;
}

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("source transport: {0}")]
    Transport(String),
}

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("destination advanced: expected next-offset {expected}, found {actual}")]
    UnexpectedPosition { expected: u64, actual: u64 },
    #[error("sink transport: {0}")]
    Transport(String),
}

#[derive(Debug, Error)]
pub enum MirrorError {
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error(transparent)]
    Sink(#[from] SinkError),
    /// Source delivered a record whose offset does not match what we
    /// asked for. Indicates a Kafka client bug, a producer that skipped
    /// offsets (impossible in normal Kafka), or a logic error.
    #[error("source delivered offset {actual}, expected {expected}")]
    SourceOffsetMismatch { expected: u64, actual: u64 },
    /// Sink's view of next-expected-offset diverged from what we
    /// believed while we were idle. Indicates an out-of-band write or
    /// a topic reset.
    #[error("destination drift while idle: expected next-offset {expected}, found {actual}")]
    DestinationDrift { expected: u64, actual: u64 },
}

/// Drive the mirror loop until something fails.
///
/// This function never returns `Ok`; it loops forever or errors. The
/// caller is responsible for crashing the process on error.
pub async fn run_mirror<S: Source, K: Sink>(
    mut source: S,
    mut sink: K,
) -> Result<std::convert::Infallible, MirrorError> {
    let start = sink.next_expected_offset().await?;
    tracing::info!(start_offset = start, "starting mirror");
    source.seek(start).await?;
    let mut expected = start;

    loop {
        match source.poll_one().await? {
            Some(record) => {
                if record.source_offset != expected {
                    return Err(MirrorError::SourceOffsetMismatch {
                        expected,
                        actual: record.source_offset,
                    });
                }
                sink.write(record).await?;
                expected = expected
                    .checked_add(1)
                    .expect("source offset overflowed u64");
            }
            None => {
                let current = sink.next_expected_offset().await?;
                if current != expected {
                    return Err(MirrorError::DestinationDrift {
                        expected,
                        actual: current,
                    });
                }
            }
        }
    }
}
