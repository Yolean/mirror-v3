//! A backup written by the mirror's own blob sink, for reading back.

#![allow(dead_code)] // each test file uses part of it

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mirror_core::{Header, Record, Sink, TimestampType};
use mirror_envelope::{ColumnType, Format, Keyring, ParquetCompression};
use mirror_fs::blob::{system_unix_clock, BlobSink, BlobStore};
use mirror_fs::{BlobEncryption, BlobSpec, FlushTriggers};
use mirror_restore::{plan_chain, BackupSource, ChainObject};

pub use mirror_fs::FsStore;

pub const TOPIC: &str = "operations";

pub fn source() -> BackupSource {
    BackupSource {
        topic: TOPIC.into(),
        partition: 0,
    }
}

/// A record with every field the envelope carries set: a null value
/// (tombstone) on every fifth, a header with a null value, and a
/// LogAppendTime timestamp on every seventh.
pub fn rec(offset: u64) -> Record {
    Record {
        topic: TOPIC.into(),
        partition: 0,
        source_offset: offset,
        timestamp_ms: Some(1_700_000_000_000 + offset as i64),
        timestamp_type: if offset.is_multiple_of(7) {
            TimestampType::LogAppendTime
        } else {
            TimestampType::CreateTime
        },
        key: Some(format!("key-{}", offset % 3).into_bytes()),
        value: (!offset.is_multiple_of(5)).then(|| format!("{{\"n\":{offset}}}").into_bytes()),
        headers: vec![
            Header {
                key: "trace".into(),
                value: Some(format!("t{offset}").into_bytes()),
            },
            Header {
                key: "empty".into(),
                value: None,
            },
        ],
    }
}

/// Two keys, k1 and k2, in a keys-dir.
pub fn keyring(dir: &Path) -> Arc<Keyring> {
    std::fs::write(
        dir.join("k1"),
        "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
    )
    .unwrap();
    std::fs::write(
        dir.join("k2"),
        "AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=",
    )
    .unwrap();
    Arc::new(Keyring::load(dir).unwrap())
}

pub fn encryption(keyring: &Arc<Keyring>, key_id: &str) -> Option<BlobEncryption> {
    Some(BlobEncryption {
        key_id: key_id.into(),
        keyring: Arc::clone(keyring),
    })
}

/// Append `records` to the backup in `dir` with the mirror's blob sink,
/// an object every `per_object` records, encrypted as given.
pub async fn write_backup(
    dir: &Path,
    records: &[Record],
    per_object: u64,
    encryption: Option<BlobEncryption>,
) {
    std::fs::create_dir_all(dir).unwrap();
    let spec = BlobSpec {
        format: Format::Parquet,
        compression: ParquetCompression::Zstd1,
        keys: ColumnType::Utf8,
        values: ColumnType::Utf8,
        compaction: None,
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets: per_object,
            daily_at_utc_seconds: None,
        },
        encryption,
    };
    let mut sink = BlobSink::open_store(FsStore::new(dir.to_path_buf()), spec, system_unix_clock())
        .await
        .unwrap();
    for r in records {
        sink.write(r.clone()).await.unwrap();
    }
    sink.flush().await.unwrap();
}

pub async fn chain(store: &FsStore, keyring: Option<&Keyring>) -> Vec<ChainObject> {
    let names = store.list().await.unwrap();
    plan_chain(&names, Format::Parquet, 0, keyring).unwrap()
}
