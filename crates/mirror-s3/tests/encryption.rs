//! An encrypted S3 destination: blob names carry the key id, contents
//! are Parquet encrypted with that key, and the chain resumes across
//! keys and from plain blobs.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use mirror_core::{Record, Sink, TimestampType};
use mirror_envelope::{Format, Keyring, ParquetCompression};
use mirror_s3::{BlobEncryption, CompactionMode, FlushTriggers, S3Sink, S3SinkConfig};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;

fn rec(offset: u64) -> Record {
    Record {
        topic: "userstate".into(),
        partition: 0,
        source_offset: offset,
        timestamp_ms: Some(1_700_000_000_000),
        timestamp_type: TimestampType::CreateTime,
        key: Some(format!("user-{}", offset % 2).into_bytes()),
        value: Some(format!("value-{offset}").into_bytes()),
        headers: vec![],
    }
}

fn keyring(dir: &std::path::Path) -> Arc<Keyring> {
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

fn cfg(
    store: Arc<dyn ObjectStore>,
    encryption: Option<BlobEncryption>,
    compaction: Option<CompactionMode>,
) -> S3SinkConfig {
    S3SinkConfig {
        read_store: Arc::clone(&store),
        write_store: store,
        prefix: None,
        destination_name: "userstate".into(),
        partition: 0,
        format: Format::Parquet,
        compression: ParquetCompression::Zstd1,
        keys: mirror_envelope::ColumnType::Utf8,
        values: mirror_envelope::ColumnType::Utf8,
        compaction,
        encryption,
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets: 2,
            daily_at_utc_seconds: None,
        },
    }
}

async fn objects(store: &dyn ObjectStore) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    let mut s = store.list(Some(&Path::from("userstate/0")));
    while let Some(m) = s.next().await {
        let m = m.unwrap();
        let bytes = store.get(&m.location).await.unwrap().bytes().await.unwrap();
        out.push((m.location.filename().unwrap().to_string(), bytes.to_vec()));
    }
    out.sort();
    out
}

#[tokio::test]
async fn names_carry_the_key_id_and_contents_need_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let ring = keyring(dir.path());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let enc = |id: &str| {
        Some(BlobEncryption {
            key_id: id.into(),
            keyring: Arc::clone(&ring),
        })
    };

    // Two plain blobs, then encryption with k1, then a rotation to k2.
    let mut sink = S3Sink::open(cfg(Arc::clone(&store), None, None))
        .await
        .unwrap();
    sink.write(rec(0)).await.unwrap();
    sink.write(rec(1)).await.unwrap();
    drop(sink);
    let mut sink = S3Sink::open(cfg(Arc::clone(&store), enc("k1"), None))
        .await
        .unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 2);
    sink.write(rec(2)).await.unwrap();
    sink.write(rec(3)).await.unwrap();
    drop(sink);
    let mut sink = S3Sink::open(cfg(Arc::clone(&store), enc("k2"), None))
        .await
        .unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 4);
    sink.write(rec(4)).await.unwrap();
    sink.write(rec(5)).await.unwrap();

    let objs = objects(store.as_ref()).await;
    let names: Vec<&str> = objs.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "00000000000000000000-00000000000000000001.parquet",
            "00000000000000000002-00000000000000000003.k-k1.parquet",
            "00000000000000000004-00000000000000000005.k-k2.parquet",
        ]
    );
    let k1 = &objs[1].1;
    assert!(mirror_envelope::parquet::decode_batch(k1).is_err());
    assert!(mirror_envelope::parquet::decode_batch_encrypted(k1, ring.get("k2").unwrap()).is_err());
    let back =
        mirror_envelope::parquet::decode_batch_encrypted(k1, ring.get("k1").unwrap()).unwrap();
    assert_eq!(back, vec![rec(2), rec(3)]);
}

#[tokio::test]
async fn compaction_snapshots_are_read_back_with_their_key() {
    let dir = tempfile::tempdir().unwrap();
    let ring = keyring(dir.path());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let enc = Some(BlobEncryption {
        key_id: "k1".into(),
        keyring: Arc::clone(&ring),
    });
    let mut sink = S3Sink::open(cfg(
        Arc::clone(&store),
        enc.clone(),
        Some(CompactionMode::Log),
    ))
    .await
    .unwrap();
    sink.write(rec(0)).await.unwrap();
    sink.write(rec(1)).await.unwrap();
    drop(sink);
    // Reopening decrypts the latest snapshot to rebuild the view.
    let mut sink = S3Sink::open(cfg(Arc::clone(&store), enc, Some(CompactionMode::Log)))
        .await
        .unwrap();
    sink.write(rec(2)).await.unwrap();
    sink.write(rec(3)).await.unwrap();
    drop(sink);
    // Without the key the snapshot cannot be read, and opening says so.
    let err = S3Sink::open(cfg(Arc::clone(&store), None, Some(CompactionMode::Log)))
        .await
        .err()
        .expect("an encrypted snapshot needs its key");
    assert!(
        format!("{err}").contains("encrypted with Parquet key k1"),
        "{err}"
    );
}

#[tokio::test]
async fn the_active_key_must_be_in_the_directory() {
    let dir = tempfile::tempdir().unwrap();
    let ring = keyring(dir.path());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let err = S3Sink::open(cfg(
        store,
        Some(BlobEncryption {
            key_id: "k9".into(),
            keyring: ring,
        }),
        None,
    ))
    .await
    .err()
    .expect("missing key");
    assert!(format!("{err}").contains("has no key file k9"), "{err}");
}
