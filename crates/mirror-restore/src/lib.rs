//! Restore a mirror-v3 blob backup into a Kafka topic.
//!
//! A blob destination (filesystem or S3) in append mode holds every
//! record of its source partition, with its offset, key, value, headers
//! and timestamp, in objects whose names form a contiguous chain of
//! consumer positions. Restore reads that chain back: [`chain`]
//! validates the names, [`read`] decodes and checks every object (the
//! verify pass, which is also the "is the backup complete" check), and
//! [`produce`] writes the records to a [`RestoreTarget`] (in production
//! a Kafka producer with many records in flight, each checked to land
//! at its offset).

pub mod chain;
pub mod produce;
pub mod read;

pub use chain::{plan_chain, ChainObject};
pub use produce::{produce, OffsetMode, RestoreReport, RestoreTarget};
pub use read::{BackupSource, BackupSummary, ObjectSummary, Reader};

#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    /// The object names do not form a chain this restore can read.
    #[error("backup chain: {0}")]
    Chain(String),
    /// The backup's store could not be listed or read.
    #[error("backup store: {0}")]
    Store(String),
    /// An object does not decode, or contradicts its name or the
    /// backup's source.
    #[error("backup object: {0}")]
    Object(String),
    /// The backup cannot be restored with the chosen offset mode.
    #[error("{0}")]
    Mode(String),
    /// Restore writes only to an empty topic.
    #[error(
        "the target topic is not empty: its high watermark is {0}; restore writes to an empty \
         topic only (delete and create it again, with the configuration it needs)"
    )]
    TargetNotEmpty(u64),
    /// The target refused a record, could not be reached, or does not
    /// hold what was produced.
    #[error("target: {0}")]
    Target(String),
}
