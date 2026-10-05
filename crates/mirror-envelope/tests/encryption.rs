//! Parquet modular encryption of blob contents.

use mirror_core::{Record, TimestampType};
use mirror_envelope::parquet::{decode_batch, decode_batch_encrypted, encode_batch_encrypted};
use mirror_envelope::{ColumnType, ParquetCompression, ParquetKey};

fn records() -> Vec<Record> {
    (0..3)
        .map(|o| Record {
            topic: "userstate".into(),
            partition: 0,
            source_offset: o,
            timestamp_ms: Some(1_700_000_000_000 + o as i64),
            timestamp_type: TimestampType::CreateTime,
            key: Some(format!("user-secret-key-{o}").into_bytes()),
            value: Some(format!(r#"{{"email":"person{o}@example.com"}}"#).into_bytes()),
            headers: vec![],
        })
        .collect()
}

fn key(b: u8) -> ParquetKey {
    ParquetKey([b; 32])
}

fn encrypted() -> Vec<u8> {
    encode_batch_encrypted(
        &records(),
        ParquetCompression::Zstd1,
        ColumnType::Utf8,
        ColumnType::Utf8,
        &key(7),
    )
    .unwrap()
}

#[test]
fn round_trips_with_the_key() {
    assert_eq!(
        decode_batch_encrypted(&encrypted(), &key(7)).unwrap(),
        records()
    );
}

#[test]
fn unreadable_without_the_key_or_with_another() {
    let bytes = encrypted();
    assert!(decode_batch(&bytes).is_err(), "a plain reader must fail");
    assert!(
        decode_batch_encrypted(&bytes, &key(8)).is_err(),
        "another key must fail"
    );
}

/// The footer is encrypted too: no key, value or statistic is in clear
/// text anywhere in the file (a plaintext footer carries min/max of
/// `key` and `value`).
#[test]
fn no_plaintext_in_the_file() {
    let bytes = encrypted();
    let text = String::from_utf8_lossy(&bytes);
    for needle in ["user-secret-key", "example.com", "userstate", "email"] {
        assert!(!text.contains(needle), "{needle} is in clear text");
    }
}

/// Interop with the reader operators use. Run with a DuckDB binary:
/// `MIRROR_V3_DUCKDB=/path/to/duckdb cargo test -p mirror-envelope --test encryption -- --ignored`.
#[test]
#[ignore = "needs a DuckDB binary in MIRROR_V3_DUCKDB"]
fn duckdb_reads_it_with_the_key_and_not_without() {
    use base64::engine::general_purpose::STANDARD as B64;
    use base64::Engine;
    let duckdb = std::env::var("MIRROR_V3_DUCKDB").expect("MIRROR_V3_DUCKDB");
    let dir = tempfile::tempdir().unwrap();
    let file = dir
        .path()
        .join("00000000000000000000-00000000000000000002.k-k1.parquet");
    std::fs::write(&file, encrypted()).unwrap();
    let k = B64.encode([7u8; 32]);
    let run = |sql: &str| {
        std::process::Command::new(&duckdb)
            .args(["-csv", "-noheader", "-c", sql])
            .output()
            .expect("run duckdb")
    };
    let ok = run(&format!(
        "PRAGMA add_parquet_key('k1', '{k}'); \
         SELECT \"offset\", key, value FROM read_parquet('{}', encryption_config = {{footer_key: 'k1'}}) ORDER BY \"offset\";",
        file.display()
    ));
    let out = String::from_utf8_lossy(&ok.stdout);
    assert!(
        ok.status.success(),
        "{}",
        String::from_utf8_lossy(&ok.stderr)
    );
    assert!(out.contains("user-secret-key-2"), "{out}");
    assert!(out.contains("person0@example.com"), "{out}");
    let plain = run(&format!(
        "SELECT count(*) FROM read_parquet('{}');",
        file.display()
    ));
    assert!(
        !plain.status.success(),
        "DuckDB must not read it without the key"
    );
}
