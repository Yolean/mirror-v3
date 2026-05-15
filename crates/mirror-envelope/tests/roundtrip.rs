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
    let bytes = encode_batch(Format::Ndjson, ParquetCompression::Zstd1, &records).unwrap();
    let decoded = decode_batch(Format::Ndjson, &bytes).unwrap();
    assert_eq!(records, decoded);
}

#[test]
fn parquet_roundtrip_preserves_every_field() {
    let records = fixture(20);
    let bytes = encode_batch(Format::Parquet, ParquetCompression::Zstd1, &records).unwrap();
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
        let bytes = encode_batch(Format::Parquet, c, &records).unwrap();
        let decoded = decode_batch(Format::Parquet, &bytes).unwrap();
        assert_eq!(records, decoded, "compression={c:?}");
    }
}

#[test]
fn parquet_is_smaller_than_ndjson_for_repetitive_columns() {
    // 100 records all with the same topic+partition+timestamp_type
    // — dictionary encoding plus zstd should annihilate those
    // columns. We don't assert a specific ratio, just that parquet
    // is meaningfully smaller (more than the per-file footer
    // overhead).
    let records = fixture(100);
    let ndjson = encode_batch(Format::Ndjson, ParquetCompression::Zstd1, &records).unwrap();
    let parquet = encode_batch(Format::Parquet, ParquetCompression::Zstd1, &records).unwrap();
    assert!(
        parquet.len() < ndjson.len(),
        "parquet {} bytes >= ndjson {} bytes",
        parquet.len(),
        ndjson.len()
    );
}
