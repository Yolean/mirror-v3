//! Round-trip every record field through both envelope formats and
//! verify byte-identical output. Especially for Parquet — it's easy
//! to silently drop a field when building the schema.

use mirror_core::{Header, Record, TimestampType};
use mirror_envelope::{decode_batch, encode_batch, ColumnType, Format, ParquetCompression};

fn fixture(n: usize) -> Vec<Record> {
    (0..n as u64)
        .map(|i| Record {
            topic: format!("topic-{}", i % 3),
            partition: (i % 4) as i32,
            source_offset: i,
            timestamp_ms: if i == 7 {
                None
            } else {
                Some(1_700_000_000_000 + i as i64)
            },
            timestamp_type: match i % 3 {
                0 => TimestampType::CreateTime,
                1 => TimestampType::LogAppendTime,
                _ => TimestampType::NotAvailable,
            },
            key: if i == 5 {
                None
            } else {
                Some(format!("k{i:04}").into_bytes())
            },
            value: if i == 9 {
                None
            } else {
                Some(format!("v{i:04}-{:0pad$}", i, pad = 200).into_bytes())
            },
            headers: if i % 4 == 0 {
                vec![]
            } else {
                vec![
                    Header {
                        key: "trace-id".into(),
                        value: Some(format!("t{i}").into_bytes()),
                    },
                    Header {
                        key: "null-value".into(),
                        value: None,
                    },
                ]
            },
        })
        .collect()
}

#[test]
fn ndjson_roundtrip_preserves_every_field() {
    let records = fixture(20);
    let bytes = encode_batch(
        Format::Ndjson,
        ParquetCompression::Zstd1,
        ColumnType::Utf8,
        ColumnType::Utf8,
        &records,
    )
    .unwrap();
    let decoded = decode_batch(Format::Ndjson, &bytes).unwrap();
    assert_eq!(records, decoded);
}

#[test]
fn parquet_roundtrip_preserves_every_field_with_utf8_columns() {
    let records = fixture(20);
    let bytes = encode_batch(
        Format::Parquet,
        ParquetCompression::Zstd1,
        ColumnType::Utf8,
        ColumnType::Utf8,
        &records,
    )
    .unwrap();
    let decoded = decode_batch(Format::Parquet, &bytes).unwrap();
    assert_eq!(records, decoded);
}

#[test]
fn parquet_with_each_compression() {
    let records = fixture(5);
    for c in [
        ParquetCompression::Zstd1,
        ParquetCompression::Zstd3,
        ParquetCompression::Snappy,
        ParquetCompression::Lz4,
        ParquetCompression::Uncompressed,
    ] {
        let bytes = encode_batch(
            Format::Parquet,
            c,
            ColumnType::Utf8,
            ColumnType::Utf8,
            &records,
        )
        .unwrap();
        let decoded = decode_batch(Format::Parquet, &bytes).unwrap();
        assert_eq!(records, decoded, "compression={c:?}");
    }
}

/// JSON fixture: every value is a small UTF-8 JSON object.
fn json_fixture(n: usize) -> Vec<Record> {
    (0..n as u64)
        .map(|i| Record {
            topic: "orders".into(),
            partition: 0,
            source_offset: i,
            timestamp_ms: Some(1_700_000_000_000 + i as i64),
            timestamp_type: TimestampType::CreateTime,
            key: Some(format!("k{i}").into_bytes()),
            value: Some(
                format!(
                    r#"{{"sku":"abc{i}","qty":{i},"price":{:.2}}}"#,
                    i as f64 * 1.5
                )
                .into_bytes(),
            ),
            headers: vec![],
        })
        .collect()
}

#[test]
fn parquet_values_json_roundtrips_through_utf8_column() {
    let records = json_fixture(10);
    let bytes = encode_batch(
        Format::Parquet,
        ParquetCompression::Zstd1,
        ColumnType::Utf8,
        ColumnType::Json,
        &records,
    )
    .unwrap();
    let decoded = decode_batch(Format::Parquet, &bytes).unwrap();
    assert_eq!(records, decoded);
}

#[test]
fn parquet_values_json_supports_null_value() {
    let mut records = json_fixture(3);
    records[1].value = None;
    let bytes = encode_batch(
        Format::Parquet,
        ParquetCompression::Zstd1,
        ColumnType::Utf8,
        ColumnType::Json,
        &records,
    )
    .unwrap();
    let decoded = decode_batch(Format::Parquet, &bytes).unwrap();
    assert_eq!(records, decoded);
    assert!(decoded[1].value.is_none());
}

#[test]
fn parquet_values_json_rejects_non_utf8_value() {
    let mut records = json_fixture(3);
    // 0xFF is never a valid UTF-8 leading byte.
    records[1].value = Some(vec![0xff, 0xfe, 0xfd]);
    let err = encode_batch(
        Format::Parquet,
        ParquetCompression::Zstd1,
        ColumnType::Utf8,
        ColumnType::Json,
        &records,
    )
    .expect_err("non-UTF-8 value under Utf8/Json must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("value") && msg.contains("UTF-8") && msg.contains("offset 1"),
        "error must point to the offending record: {msg}"
    );
}

#[test]
fn parquet_value_column_is_always_named_value() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    for vt in [ColumnType::Bytes, ColumnType::Utf8, ColumnType::Json] {
        let records = json_fixture(2);
        let bytes = encode_batch(
            Format::Parquet,
            ParquetCompression::Zstd1,
            ColumnType::Utf8,
            vt,
            &records,
        )
        .unwrap();
        let cursor = bytes::Bytes::from(bytes);
        let reader = ParquetRecordBatchReaderBuilder::try_new(cursor)
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.into_iter().next().unwrap().unwrap();
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            names.contains(&"value"),
            "value column must always be named `value` (values={vt:?}): {names:?}"
        );
        assert!(
            !names.contains(&"json"),
            "must never emit a `json` column (values={vt:?}): {names:?}"
        );
    }
}

#[test]
fn parquet_values_json_emits_arrow_json_extension_metadata() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let records = json_fixture(3);
    let bytes = encode_batch(
        Format::Parquet,
        ParquetCompression::Zstd1,
        ColumnType::Utf8,
        ColumnType::Json,
        &records,
    )
    .unwrap();
    let cursor = bytes::Bytes::from(bytes);
    let reader = ParquetRecordBatchReaderBuilder::try_new(cursor)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();
    let value_field = batch.schema().field_with_name("value").unwrap().clone();
    assert_eq!(value_field.data_type(), &arrow::datatypes::DataType::Utf8);
    assert_eq!(
        value_field.metadata().get("ARROW:extension:name"),
        Some(&"arrow.json".to_string()),
        "values=Json must tag the value column with arrow.json extension"
    );
}

#[test]
fn parquet_values_utf8_does_not_emit_arrow_json_extension() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let records = json_fixture(3);
    let bytes = encode_batch(
        Format::Parquet,
        ParquetCompression::Zstd1,
        ColumnType::Utf8,
        ColumnType::Utf8,
        &records,
    )
    .unwrap();
    let cursor = bytes::Bytes::from(bytes);
    let reader = ParquetRecordBatchReaderBuilder::try_new(cursor)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();
    let value_field = batch.schema().field_with_name("value").unwrap().clone();
    assert_eq!(value_field.metadata().get("ARROW:extension:name"), None);
}

#[test]
fn parquet_keys_bytes_preserves_non_utf8_keys() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let mut records = fixture(3);
    records[1].key = Some(vec![0xff, 0xfe, 0xfd]);
    let bytes = encode_batch(
        Format::Parquet,
        ParquetCompression::Zstd1,
        ColumnType::Bytes,
        ColumnType::Utf8,
        &records,
    )
    .unwrap();
    let decoded = decode_batch(Format::Parquet, &bytes).unwrap();
    assert_eq!(records, decoded);
    let cursor = bytes::Bytes::from(bytes);
    let reader = ParquetRecordBatchReaderBuilder::try_new(cursor)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();
    let key_field = batch.schema().field_with_name("key").unwrap().clone();
    assert_eq!(
        key_field.data_type(),
        &arrow::datatypes::DataType::LargeBinary
    );
}

#[test]
fn parquet_keys_utf8_rejects_non_utf8_key() {
    let mut records = fixture(3);
    records[1].key = Some(vec![0xff, 0xfe, 0xfd]);
    let err = encode_batch(
        Format::Parquet,
        ParquetCompression::Zstd1,
        ColumnType::Utf8,
        ColumnType::Utf8,
        &records,
    )
    .expect_err("non-UTF-8 key under keys=Utf8 must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("key") && msg.contains("UTF-8") && msg.contains("offset 1"),
        "error must point to the offending record: {msg}"
    );
}

#[test]
fn parquet_is_smaller_than_ndjson_for_repetitive_columns() {
    // 100 records all with the same topic+partition+timestamp_type
    // — dictionary encoding plus zstd should annihilate those
    // columns. We don't assert a specific ratio, just that parquet
    // is meaningfully smaller (more than the per-file footer
    // overhead).
    let records = fixture(100);
    let ndjson = encode_batch(
        Format::Ndjson,
        ParquetCompression::Zstd1,
        ColumnType::Utf8,
        ColumnType::Utf8,
        &records,
    )
    .unwrap();
    let parquet = encode_batch(
        Format::Parquet,
        ParquetCompression::Zstd1,
        ColumnType::Utf8,
        ColumnType::Utf8,
        &records,
    )
    .unwrap();
    assert!(
        parquet.len() < ndjson.len(),
        "parquet {} bytes >= ndjson {} bytes",
        parquet.len(),
        ndjson.len()
    );
}
