//! Record envelope encoding for mirror-v3 destinations.
//!
//! Two formats:
//! - **Parquet** (default): columnar, schema embedded in the footer,
//!   compressed with zstd-1 by default. Standard data-lake format —
//!   readable by DuckDB / Athena / Spark out of the box.
//! - **NDJSON**: one JSON object per record line, base64-encoded
//!   binary fields. Operator-friendly for `jq` debugging.
//!
//! The on-disk wire shape is identical for both: each record carries
//! `topic`, `partition`, `offset`, `timestamp_ms` (nullable),
//! `timestamp_type`, `key` (nullable bytes), `value` (nullable bytes),
//! and `headers` (list of `{key, value (nullable bytes)}`).

use mirror_core::Record;

pub mod ndjson;
pub mod parquet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Format {
    Parquet,
    Ndjson,
}

impl Format {
    /// File extension to use for blob naming.
    pub fn extension(self) -> &'static str {
        match self {
            Format::Parquet => "parquet",
            Format::Ndjson => "ndjson",
        }
    }
}

/// Parquet compression codec. Only meaningful when [`Format::Parquet`]
/// is selected; ignored for NDJSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParquetCompression {
    Zstd1,
    Zstd3,
    Snappy,
    Lz4,
    Uncompressed,
}

#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    #[error("encode: {0}")]
    Encode(String),
    #[error("decode: {0}")]
    Decode(String),
}

/// Encode a batch of records into the configured format. Returns the
/// fully-formed on-disk bytes including the schema footer (Parquet)
/// or all NDJSON lines.
pub fn encode_batch(
    format: Format,
    compression: ParquetCompression,
    records: &[Record],
) -> Result<Vec<u8>, EnvelopeError> {
    match format {
        Format::Ndjson => ndjson::encode_batch(records),
        Format::Parquet => parquet::encode_batch(records, compression),
    }
}

/// Decode a single on-disk file's bytes back into records. Used by
/// tests and operator tooling.
pub fn decode_batch(format: Format, bytes: &[u8]) -> Result<Vec<Record>, EnvelopeError> {
    match format {
        Format::Ndjson => ndjson::decode_batch(bytes),
        Format::Parquet => parquet::decode_batch(bytes),
    }
}
