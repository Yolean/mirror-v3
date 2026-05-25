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
//! `timestamp_type`, `key` (nullable), `value` (nullable), and
//! `headers` (list of `{key, value (nullable bytes)}`). The Parquet
//! physical type of `key` and `value` is selected by [`ColumnType`].

// Re-export the shared topic-schema enum so sinks and the envelope
// crate agree on one type. Variants and validation contract live in
// `mirror_core::ColumnType`.
pub use mirror_core::ColumnType;
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
///
/// `keys` and `values` are only meaningful for Parquet. They control
/// the physical column type and (for `Utf8` / `Json`) UTF-8 validation.
/// NDJSON ignores them — it always emits base64-encoded byte fields.
pub fn encode_batch(
    format: Format,
    compression: ParquetCompression,
    keys: ColumnType,
    values: ColumnType,
    records: &[Record],
) -> Result<Vec<u8>, EnvelopeError> {
    match format {
        Format::Ndjson => ndjson::encode_batch(records),
        Format::Parquet => parquet::encode_batch(records, compression, keys, values),
    }
}

/// Decode a single on-disk file's bytes back into records. The parquet
/// path auto-detects the physical type of the `key` and `value`
/// columns (`LargeBinary` or `Utf8`) and reconstructs `Record.key` /
/// `Record.value` as byte vectors either way.
pub fn decode_batch(format: Format, bytes: &[u8]) -> Result<Vec<Record>, EnvelopeError> {
    match format {
        Format::Ndjson => ndjson::decode_batch(bytes),
        Format::Parquet => parquet::decode_batch(bytes),
    }
}
