//! Restore a mirror-v3 blob backup into a Kafka topic.
//!
//! A blob destination (filesystem or S3) in append mode holds every
//! record of its source partition, with its offset, key, value, headers
//! and timestamp, in objects whose names form a contiguous chain of
//! consumer positions. Restore reads that chain back: [`chain`]
//! validates the names, [`read`] decodes and checks every object (the
//! verify pass, which is also the "is the backup complete" check), and
//! [`produce`] writes the records to a [`mirror_core::Sink`] (the Kafka
//! destination, with its high-watermark gate before every produce).

pub mod chain;

pub use chain::{plan_chain, ChainObject};

#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    /// The object names do not form a chain this restore can read.
    #[error("backup chain: {0}")]
    Chain(String),
}
