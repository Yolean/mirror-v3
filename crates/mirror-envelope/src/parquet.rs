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

use crate::{EnvelopeError, KeyType, ParquetCompression};

fn header_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::LargeBinary, true),
    ])
}

fn build_schema(value_as_json: bool, key_type: KeyType) -> SchemaRef {
    let header_struct = DataType::Struct(header_struct_fields());
    // `nullable: true` matches arrow's ListBuilder<StructBuilder>
    // default; the records themselves never contain a null header
    // struct, but Arrow requires the schema to permit it.
    let header_item = Field::new("item", header_struct, true);
    let value_field = if value_as_json {
        // Carry the `arrow.json` canonical extension type so
        // readers that respect it (e.g. arrow-aware tooling) get
        // the JSON logical-type hint. DuckDB doesn't currently
        // round-trip the extension but reads the column as
        // VARCHAR, which is what we need.
        let mut md = std::collections::HashMap::new();
        md.insert("ARROW:extension:name".to_string(), "arrow.json".to_string());
        md.insert("ARROW:extension:metadata".to_string(), String::new());
        Field::new("json", DataType::Utf8, true).with_metadata(md)
    } else {
        Field::new("value", DataType::LargeBinary, true)
    };
    let key_field = match key_type {
        KeyType::Utf8 => Field::new("key", DataType::Utf8, true),
        KeyType::Binary => Field::new("key", DataType::LargeBinary, true),
    };
    Arc::new(Schema::new(vec![
        Field::new("topic", DataType::Utf8, false),
        Field::new("partition", DataType::Int32, false),
        Field::new("offset", DataType::UInt64, false),
        Field::new("timestamp_ms", DataType::Int64, true),
        Field::new("timestamp_type", DataType::Utf8, false),
        key_field,
        value_field,
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
    value_as_json: bool,
    key_type: KeyType,
) -> Result<Vec<u8>, EnvelopeError> {
    let schema = build_schema(value_as_json, key_type);
    let batch = build_record_batch(records, &schema, value_as_json, key_type)?;

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
    value_as_json: bool,
    key_type: KeyType,
) -> Result<RecordBatch, EnvelopeError> {
    let mut topics = StringBuilder::new();
    let mut partitions = Int32Builder::new();
    let mut offsets = UInt64Builder::new();
    let mut timestamps = Int64Builder::new();
    let mut timestamp_types = StringBuilder::new();
    let mut keys_binary = LargeBinaryBuilder::new();
    let mut keys_string = StringBuilder::new();
    let mut values_binary = LargeBinaryBuilder::new();
    let mut values_json = StringBuilder::new();

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
        match key_type {
            KeyType::Binary => match &r.key {
                Some(k) => keys_binary.append_value(k),
                None => keys_binary.append_null(),
            },
            KeyType::Utf8 => match &r.key {
                None => keys_string.append_null(),
                Some(bytes) => {
                    let s = std::str::from_utf8(bytes).map_err(|e| {
                        EnvelopeError::Encode(format!(
                            "key at source offset {} is not valid UTF-8 \
                             (key-type=utf8 requires UTF-8): {}",
                            r.source_offset, e
                        ))
                    })?;
                    keys_string.append_value(s);
                }
            },
        }
        // Value column: binary by default; UTF-8 string when
        // value_as_json is on. We fail hard on non-UTF-8 in json
        // mode (no per-record skipping, no parse-error column).
        if value_as_json {
            match &r.value {
                None => values_json.append_null(),
                Some(bytes) => {
                    let s = std::str::from_utf8(bytes).map_err(|e| {
                        EnvelopeError::Encode(format!(
                            "value at source offset {} is not valid UTF-8 \
                             (json mode requires UTF-8): {}",
                            r.source_offset, e
                        ))
                    })?;
                    values_json.append_value(s);
                }
            }
        } else {
            match &r.value {
                Some(v) => values_binary.append_value(v),
                None => values_binary.append_null(),
            }
        }
        append_headers(&mut headers_builder, &r.headers);
    }

    let topics: ArrayRef = Arc::new(topics.finish());
    let partitions: ArrayRef = Arc::new(partitions.finish());
    let offsets: ArrayRef = Arc::new(offsets.finish());
    let timestamps: ArrayRef = Arc::new(timestamps.finish());
    let timestamp_types: ArrayRef = Arc::new(timestamp_types.finish());
    let keys: ArrayRef = match key_type {
        KeyType::Binary => Arc::new(keys_binary.finish()),
        KeyType::Utf8 => Arc::new(keys_string.finish()),
    };
    let values: ArrayRef = if value_as_json {
        Arc::new(values_json.finish())
    } else {
        Arc::new(values_binary.finish())
    };
    let headers: ArrayRef = Arc::new(headers_builder.finish());

    RecordBatch::try_new(
        schema.clone(),
        vec![
            topics,
            partitions,
            offsets,
            timestamps,
            timestamp_types,
            keys,
            values,
            headers,
        ],
    )
    .map_err(|e| EnvelopeError::Encode(format!("record batch: {e}")))
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

enum ValueColumn {
    Binary(ArrayRef),
    Json(ArrayRef),
}

enum KeyColumn {
    Binary(ArrayRef),
    Utf8(ArrayRef),
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
    let keys = col("key")?;
    // Key column may be LargeBinary (legacy / key-type=binary) or
    // Utf8 (key-type=utf8). Auto-detect from the schema.
    let key_column = match keys.data_type() {
        DataType::LargeBinary => KeyColumn::Binary(keys.clone()),
        DataType::Utf8 => KeyColumn::Utf8(keys.clone()),
        other => {
            return Err(EnvelopeError::Decode(format!(
                "key column must be LargeBinary or Utf8, got {other:?}"
            )))
        }
    };
    // Files written before the `json: true` feature have a `value`
    // column (LargeBinary); files written under json-mode have a
    // `json` column (Utf8). Auto-detect from the schema so the same
    // decoder works for both.
    let value_column = batch
        .column_by_name("value")
        .map(|c| ValueColumn::Binary(c.clone()))
        .or_else(|| {
            batch
                .column_by_name("json")
                .map(|c| ValueColumn::Json(c.clone()))
        })
        .ok_or_else(|| {
            EnvelopeError::Decode("missing column: expected `value` or `json`".into())
        })?;
    let headers = col("headers")?;

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
    let keys_binary;
    let keys_string;
    let read_key: Box<dyn Fn(usize) -> Option<Vec<u8>>>;
    match &key_column {
        KeyColumn::Binary(arr) => {
            keys_binary = arr
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| EnvelopeError::Decode("key column not LargeBinary".into()))?
                .clone();
            let k = keys_binary.clone();
            read_key = Box::new(move |i| {
                if k.is_null(i) {
                    None
                } else {
                    Some(k.value(i).to_vec())
                }
            });
        }
        KeyColumn::Utf8(arr) => {
            keys_string = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| EnvelopeError::Decode("key column not Utf8".into()))?
                .clone();
            let k = keys_string.clone();
            read_key = Box::new(move |i| {
                if k.is_null(i) {
                    None
                } else {
                    Some(k.value(i).as_bytes().to_vec())
                }
            });
        }
    }
    let values_binary;
    let values_string;
    let read_value: Box<dyn Fn(usize) -> Option<Vec<u8>>>;
    match &value_column {
        ValueColumn::Binary(arr) => {
            values_binary = arr
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| EnvelopeError::Decode("value column not LargeBinary".into()))?
                .clone();
            let v = values_binary.clone();
            read_value = Box::new(move |i| {
                if v.is_null(i) {
                    None
                } else {
                    Some(v.value(i).to_vec())
                }
            });
        }
        ValueColumn::Json(arr) => {
            values_string = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| EnvelopeError::Decode("json column not Utf8".into()))?
                .clone();
            let v = values_string.clone();
            read_value = Box::new(move |i| {
                if v.is_null(i) {
                    None
                } else {
                    Some(v.value(i).as_bytes().to_vec())
                }
            });
        }
    }
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
        let key = read_key(i);
        let value = read_value(i);
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
