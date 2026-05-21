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
///
/// `value_as_json` is only meaningful for Parquet: when `true`, the
/// output schema has a `json: Utf8` column instead of the default
/// `value: LargeBinary`, and each record's value bytes must be valid
/// UTF-8 (else a hard `Encode` error). Caller is responsible for
/// rejecting `value_as_json = true` with `Format::Ndjson` before
/// reaching here — `encode_batch` silently ignores the flag for
/// NDJSON.
pub fn encode_batch(
    format: Format,
    compression: ParquetCompression,
    value_as_json: bool,
    records: &[Record],
) -> Result<Vec<u8>, EnvelopeError> {
    match format {
        Format::Ndjson => ndjson::encode_batch(records),
        Format::Parquet => parquet::encode_batch(records, compression, value_as_json),
    }
}

/// Decode a single on-disk file's bytes back into records. The
/// parquet path auto-detects whether the file has a `value` column
/// (binary) or a `json` column (Utf8); the resulting `Record.value`
/// always carries bytes, with UTF-8 strings encoded as their bytes.
pub fn decode_batch(format: Format, bytes: &[u8]) -> Result<Vec<Record>, EnvelopeError> {
    match format {
        Format::Ndjson => ndjson::decode_batch(bytes),
        Format::Parquet => parquet::decode_batch(bytes),
    }
}
