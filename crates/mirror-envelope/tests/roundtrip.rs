//! Round-trip every record field through both envelope formats and
//! verify byte-identical output. Especially for Parquet — it's easy
//! to silently drop a field when building the schema.

use mirror_core::{Header, Record, TimestampType};
use mirror_envelope::{decode_batch, encode_batch, Format, ParquetCompression};

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
    let bytes = encode_batch(Format::Ndjson, ParquetCompression::Zstd1, false, &records).unwrap();
    let decoded = decode_batch(Format::Ndjson, &bytes).unwrap();
    assert_eq!(records, decoded);
}

#[test]
fn parquet_roundtrip_preserves_every_field() {
    let records = fixture(20);
    let bytes = encode_batch(Format::Parquet, ParquetCompression::Zstd1, false, &records).unwrap();
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
        let bytes = encode_batch(Format::Parquet, c, false, &records).unwrap();
        let decoded = decode_batch(Format::Parquet, &bytes).unwrap();
        assert_eq!(records, decoded, "compression={c:?}");
    }
}

/// json-mode fixture: every value is a small UTF-8 JSON object.
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
fn parquet_json_mode_roundtrip_preserves_bytes_through_utf8_column() {
    let records = json_fixture(10);
    let bytes = encode_batch(Format::Parquet, ParquetCompression::Zstd1, true, &records).unwrap();
    let decoded = decode_batch(Format::Parquet, &bytes).unwrap();
    assert_eq!(records, decoded);
}

#[test]
fn parquet_json_mode_supports_null_value() {
    let mut records = json_fixture(3);
    records[1].value = None;
    let bytes = encode_batch(Format::Parquet, ParquetCompression::Zstd1, true, &records).unwrap();
    let decoded = decode_batch(Format::Parquet, &bytes).unwrap();
    assert_eq!(records, decoded);
    assert!(decoded[1].value.is_none());
}

#[test]
fn parquet_json_mode_rejects_non_utf8_value() {
    let mut records = json_fixture(3);
    // 0xFF is never a valid UTF-8 leading byte.
    records[1].value = Some(vec![0xff, 0xfe, 0xfd]);
    let err = encode_batch(Format::Parquet, ParquetCompression::Zstd1, true, &records)
        .expect_err("non-UTF-8 in json mode must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("UTF-8") && msg.contains("offset 1"),
        "error must point to the offending record: {msg}"
    );
}

#[test]
fn parquet_json_mode_emits_utf8_column_named_json_and_omits_value() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let records = json_fixture(3);
    let bytes = encode_batch(Format::Parquet, ParquetCompression::Zstd1, true, &records).unwrap();
    let cursor = bytes::Bytes::from(bytes);
    let reader = ParquetRecordBatchReaderBuilder::try_new(cursor)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();
    let schema = batch.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(names.contains(&"json"), "expected `json` column: {names:?}");
    assert!(
        !names.contains(&"value"),
        "must not have a `value` column in json mode: {names:?}"
    );
    let json_field = schema.field_with_name("json").unwrap();
    assert_eq!(
        json_field.data_type(),
        &arrow::datatypes::DataType::Utf8,
        "json column must be Utf8 so DuckDB reads it as VARCHAR"
    );
    let md = json_field.metadata();
    assert_eq!(
        md.get("ARROW:extension:name"),
        Some(&"arrow.json".to_string())
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
    let ndjson = encode_batch(Format::Ndjson, ParquetCompression::Zstd1, false, &records).unwrap();
    let parquet =
        encode_batch(Format::Parquet, ParquetCompression::Zstd1, false, &records).unwrap();
    assert!(
        parquet.len() < ndjson.len(),
        "parquet {} bytes >= ndjson {} bytes",
        parquet.len(),
        ndjson.len()
    );
}
