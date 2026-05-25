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

/// Storage representation for a record column (`key` or `value`).
///
/// All three variants land as `Utf8` Parquet columns; the difference
/// is what's inside the string and which extension metadata is set on
/// the field. NDJSON ignores this setting entirely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ColumnType {
    /// Arbitrary bytes, base64-encoded into a `Utf8` Parquet column,
    /// tagged with `ARROW:extension:name = mirror_v3.bytes_base64`.
    /// The decoder base64-decodes back to the original bytes.
    BytesBase64,
    /// UTF-8 string, stored verbatim in a `Utf8` Parquet column with
    /// no extension metadata. Non-UTF-8 input is a hard `Encode` error
    /// pointing at the offending source offset.
    #[default]
    Utf8,
    /// UTF-8 JSON document, stored verbatim in a `Utf8` Parquet column
    /// tagged with the `arrow.json` canonical extension metadata.
    /// mirror-v3 does not parse or validate JSON beyond UTF-8.
    Json,
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
