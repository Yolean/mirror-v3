//! Parquet envelope.
//!
//! Each `encode_batch` call produces one Parquet file with one row
//! group: write the records as a single `RecordBatch`, close. The
//! footer carries the schema (so the file is self-describing) and
//! per-column statistics (so future readers can do predicate
//! pushdown).
//!
//! Compression: zstd-1 by default. Dictionary encoding is on for
//! string columns, which compresses `topic` / `partition` /
//! `timestamp_type` (low-cardinality, repeated per record) down to
//! near-nothing.
//!
//! The `key` and `value` columns are always named `key` and `value`,
//! and their physical type is always `Utf8`. The variant of
//! [`ColumnType`] picks what's *inside* the string and which
//! `ARROW:extension:name` tag the field carries:
//!
//! - `BytesBase64` → base64-encoded bytes, tagged `mirror_v3.bytes_base64`.
//! - `Utf8`        → verbatim UTF-8, no extension tag.
//! - `Json`        → verbatim UTF-8 JSON, tagged `arrow.json`.
//!
//! On decode, the field metadata is what disambiguates `BytesBase64`
//! (which the decoder base64-decodes) from `Utf8` / `Json` (passed
//! through as bytes). UTF-8 enforcement happens at encode for `Utf8`
//! and `Json`; a bad byte is a hard `Encode` error pointing at the
//! offending source offset.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int32Builder, Int64Builder, LargeBinaryBuilder, ListBuilder, RecordBatch,
    StringBuilder, StructBuilder, UInt64Builder,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use mirror_core::{Header, Record, TimestampType};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use crate::{ColumnType, EnvelopeError, ParquetCompression};

fn header_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::LargeBinary, true),
    ])
}

/// `ARROW:extension:name` value for the bytes-base64 column variant.
/// Custom (non-canonical) extension; tools that don't recognise it
/// see a plain Utf8 column.
const BYTES_BASE64_EXT: &str = "mirror_v3.bytes_base64";
const JSON_EXT: &str = "arrow.json";

fn column_field(name: &str, kind: ColumnType) -> Field {
    let mut md = std::collections::HashMap::new();
    let ext = match kind {
        ColumnType::BytesBase64 => Some(BYTES_BASE64_EXT),
        ColumnType::Utf8 => None,
        ColumnType::Json => Some(JSON_EXT),
    };
    if let Some(name) = ext {
        md.insert("ARROW:extension:name".to_string(), name.to_string());
        md.insert("ARROW:extension:metadata".to_string(), String::new());
    }
    Field::new(name, DataType::Utf8, true).with_metadata(md)
}

fn build_schema(keys: ColumnType, values: ColumnType) -> SchemaRef {
    let header_struct = DataType::Struct(header_struct_fields());
    // `nullable: true` matches arrow's ListBuilder<StructBuilder>
    // default; the records themselves never contain a null header
    // struct, but Arrow requires the schema to permit it.
    let header_item = Field::new("item", header_struct, true);
    Arc::new(Schema::new(vec![
        Field::new("topic", DataType::Utf8, false),
        Field::new("partition", DataType::Int32, false),
        Field::new("offset", DataType::UInt64, false),
        Field::new("timestamp_ms", DataType::Int64, true),
        Field::new("timestamp_type", DataType::Utf8, false),
        column_field("key", keys),
        column_field("value", values),
        Field::new("headers", DataType::List(Arc::new(header_item)), false),
    ]))
}

fn to_compression(c: ParquetCompression) -> Compression {
    match c {
        ParquetCompression::Zstd1 => {
            Compression::ZSTD(ZstdLevel::try_new(1).expect("zstd level 1 valid"))
        }
        ParquetCompression::Zstd3 => {
            Compression::ZSTD(ZstdLevel::try_new(3).expect("zstd level 3 valid"))
        }
        ParquetCompression::Snappy => Compression::SNAPPY,
        ParquetCompression::Lz4 => Compression::LZ4,
        ParquetCompression::Uncompressed => Compression::UNCOMPRESSED,
    }
}

pub fn encode_batch(
    records: &[Record],
    compression: ParquetCompression,
    keys: ColumnType,
    values: ColumnType,
) -> Result<Vec<u8>, EnvelopeError> {
    let schema = build_schema(keys, values);
    let batch = build_record_batch(records, &schema, keys, values)?;

    let props = WriterProperties::builder()
        .set_compression(to_compression(compression))
        .set_dictionary_enabled(true)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let mut buf: Vec<u8> = Vec::with_capacity(records.len() * 64);
    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))
            .map_err(|e| EnvelopeError::Encode(format!("writer init: {e}")))?;
        writer
            .write(&batch)
            .map_err(|e| EnvelopeError::Encode(format!("write batch: {e}")))?;
        writer
            .close()
            .map_err(|e| EnvelopeError::Encode(format!("close: {e}")))?;
    }
    Ok(buf)
}

fn build_record_batch(
    records: &[Record],
    schema: &SchemaRef,
    keys: ColumnType,
    values: ColumnType,
) -> Result<RecordBatch, EnvelopeError> {
    let mut topics = StringBuilder::new();
    let mut partitions = Int32Builder::new();
    let mut offsets = UInt64Builder::new();
    let mut timestamps = Int64Builder::new();
    let mut timestamp_types = StringBuilder::new();
    let mut keys_string = StringBuilder::new();
    let mut values_string = StringBuilder::new();

    // Headers: List<Struct{key: Utf8, value: LargeBinary}>
    let struct_builders: Vec<Box<dyn arrow::array::ArrayBuilder>> = vec![
        Box::new(StringBuilder::new()),
        Box::new(LargeBinaryBuilder::new()),
    ];
    let inner_struct = StructBuilder::new(header_struct_fields(), struct_builders);
    let mut headers_builder = ListBuilder::new(inner_struct);

    for r in records {
        topics.append_value(&r.topic);
        partitions.append_value(r.partition);
        offsets.append_value(r.source_offset);
        match r.timestamp_ms {
            Some(ts) => timestamps.append_value(ts),
            None => timestamps.append_null(),
        }
        timestamp_types.append_value(r.timestamp_type.as_str());
        append_payload(
            "key",
            keys,
            r.source_offset,
            r.key.as_deref(),
            &mut keys_string,
        )?;
        append_payload(
            "value",
            values,
            r.source_offset,
            r.value.as_deref(),
            &mut values_string,
        )?;
        append_headers(&mut headers_builder, &r.headers);
    }

    let topics: ArrayRef = Arc::new(topics.finish());
    let partitions: ArrayRef = Arc::new(partitions.finish());
    let offsets: ArrayRef = Arc::new(offsets.finish());
    let timestamps: ArrayRef = Arc::new(timestamps.finish());
    let timestamp_types: ArrayRef = Arc::new(timestamp_types.finish());
    let keys_arr: ArrayRef = Arc::new(keys_string.finish());
    let values_arr: ArrayRef = Arc::new(values_string.finish());
    let headers: ArrayRef = Arc::new(headers_builder.finish());

    RecordBatch::try_new(
        schema.clone(),
        vec![
            topics,
            partitions,
            offsets,
            timestamps,
            timestamp_types,
            keys_arr,
            values_arr,
            headers,
        ],
    )
    .map_err(|e| EnvelopeError::Encode(format!("record batch: {e}")))
}

fn append_payload(
    column: &str,
    kind: ColumnType,
    source_offset: u64,
    payload: Option<&[u8]>,
    string: &mut StringBuilder,
) -> Result<(), EnvelopeError> {
    use base64::engine::general_purpose::STANDARD as B64;
    use base64::Engine;
    match payload {
        None => string.append_null(),
        Some(bytes) => match kind {
            ColumnType::BytesBase64 => {
                string.append_value(B64.encode(bytes));
            }
            ColumnType::Utf8 | ColumnType::Json => {
                let s = std::str::from_utf8(bytes).map_err(|e| {
                    EnvelopeError::Encode(format!(
                        "{column} at source offset {source_offset} is not valid UTF-8: {e}",
                    ))
                })?;
                string.append_value(s);
            }
        },
    }
    Ok(())
}

fn append_headers(builder: &mut ListBuilder<StructBuilder>, headers: &[Header]) {
    let inner = builder.values();
    for h in headers {
        // field 0 = key (Utf8), field 1 = value (LargeBinary nullable)
        inner
            .field_builder::<StringBuilder>(0)
            .expect("key builder")
            .append_value(&h.key);
        let value_b = inner
            .field_builder::<LargeBinaryBuilder>(1)
            .expect("value builder");
        match &h.value {
            Some(v) => value_b.append_value(v),
            None => value_b.append_null(),
        }
        inner.append(true);
    }
    builder.append(true);
}

pub fn decode_batch(bytes: &[u8]) -> Result<Vec<Record>, EnvelopeError> {
    let cursor = bytes::Bytes::copy_from_slice(bytes);
    let reader = ParquetRecordBatchReaderBuilder::try_new(cursor)
        .map_err(|e| EnvelopeError::Decode(format!("reader init: {e}")))?
        .build()
        .map_err(|e| EnvelopeError::Decode(format!("reader build: {e}")))?;

    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| EnvelopeError::Decode(format!("read batch: {e}")))?;
        out.extend(record_batch_into_records(&batch)?);
    }
    Ok(out)
}

fn record_batch_into_records(batch: &RecordBatch) -> Result<Vec<Record>, EnvelopeError> {
    use arrow::array::{
        Int32Array, Int64Array, LargeBinaryArray, ListArray, StringArray, StructArray, UInt64Array,
    };

    let n = batch.num_rows();
    let col = |name: &str| -> Result<ArrayRef, EnvelopeError> {
        batch
            .column_by_name(name)
            .cloned()
            .ok_or_else(|| EnvelopeError::Decode(format!("missing column {name}")))
    };
    let topics = col("topic")?;
    let partitions = col("partition")?;
    let offsets = col("offset")?;
    let timestamps = col("timestamp_ms")?;
    let timestamp_types = col("timestamp_type")?;
    let keys_col = col("key")?;
    let values_col = col("value")?;
    let headers = col("headers")?;

    let schema = batch.schema();
    let read_key = payload_reader("key", &keys_col, schema.field_with_name("key").ok())?;
    let read_value = payload_reader("value", &values_col, schema.field_with_name("value").ok())?;

    let topics = topics
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| EnvelopeError::Decode("topic not Utf8".into()))?;
    let partitions = partitions
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| EnvelopeError::Decode("partition not Int32".into()))?;
    let offsets = offsets
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| EnvelopeError::Decode("offset not UInt64".into()))?;
    let timestamps = timestamps
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| EnvelopeError::Decode("timestamp_ms not Int64".into()))?;
    let timestamp_types = timestamp_types
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| EnvelopeError::Decode("timestamp_type not Utf8".into()))?;
    let headers_list = headers
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| EnvelopeError::Decode("headers not List".into()))?;

    let header_struct = headers_list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| EnvelopeError::Decode("headers items not Struct".into()))?
        .clone();
    let header_keys = header_struct
        .column_by_name("key")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| EnvelopeError::Decode("header.key not Utf8".into()))?
        .clone();
    let header_values = header_struct
        .column_by_name("value")
        .and_then(|c| c.as_any().downcast_ref::<LargeBinaryArray>())
        .ok_or_else(|| EnvelopeError::Decode("header.value not LargeBinary".into()))?
        .clone();

    let _: Option<&NullBuffer> = headers_list.nulls();
    let header_offsets = headers_list.offsets();

    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let timestamp_ms = if timestamps.is_null(i) {
            None
        } else {
            Some(timestamps.value(i))
        };
        let key = read_key(i)?;
        let value = read_value(i)?;
        let h_start = header_offsets[i] as usize;
        let h_end = header_offsets[i + 1] as usize;
        let mut headers = Vec::with_capacity(h_end - h_start);
        for j in h_start..h_end {
            let hk = header_keys.value(j).to_string();
            let hv = if header_values.is_null(j) {
                None
            } else {
                Some(header_values.value(j).to_vec())
            };
            headers.push(Header { key: hk, value: hv });
        }
        out.push(Record {
            topic: topics.value(i).to_string(),
            partition: partitions.value(i),
            source_offset: offsets.value(i),
            timestamp_ms,
            timestamp_type: TimestampType::from_wire(timestamp_types.value(i))
                .unwrap_or(TimestampType::NotAvailable),
            key,
            value,
            headers,
        });
    }
    Ok(out)
}

type PayloadReader = Box<dyn Fn(usize) -> Result<Option<Vec<u8>>, EnvelopeError>>;

/// Returns a reader that converts the given column's value at index
/// `i` into `Option<Vec<u8>>`. Branches once on the column's
/// `ARROW:extension:name` to decide whether to base64-decode and
/// produces a small closure for the hot loop.
fn payload_reader(
    name: &str,
    col: &ArrayRef,
    field: Option<&Field>,
) -> Result<PayloadReader, EnvelopeError> {
    use arrow::array::StringArray;
    if !matches!(col.data_type(), DataType::Utf8) {
        return Err(EnvelopeError::Decode(format!(
            "{name} column must be Utf8, got {:?}",
            col.data_type()
        )));
    }
    let arr = col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| EnvelopeError::Decode(format!("{name} not Utf8")))?
        .clone();
    let ext = field
        .and_then(|f| f.metadata().get("ARROW:extension:name").cloned())
        .unwrap_or_default();
    let column = name.to_string();
    if ext == BYTES_BASE64_EXT {
        use base64::engine::general_purpose::STANDARD as B64;
        use base64::Engine;
        Ok(Box::new(move |i| {
            if arr.is_null(i) {
                Ok(None)
            } else {
                B64.decode(arr.value(i)).map(Some).map_err(|e| {
                    EnvelopeError::Decode(format!("{column} row {i}: base64-decode: {e}"))
                })
            }
        }))
    } else {
        Ok(Box::new(move |i| {
            if arr.is_null(i) {
                Ok(None)
            } else {
                Ok(Some(arr.value(i).as_bytes().to_vec()))
            }
        }))
    }
}
