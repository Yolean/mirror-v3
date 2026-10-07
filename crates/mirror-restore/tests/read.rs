//! The verify pass reads back what the mirror's blob sink wrote,
//! encrypted, across a key rotation, with holes.

mod common;

use common::*;
use mirror_core::Record;
use mirror_envelope::{Format, Keyring};
use mirror_restore::{Reader, RestoreError};

fn reader<'a>(
    store: &'a FsStore,
    keyring: Option<&'a Keyring>,
    source: &'a mirror_restore::BackupSource,
) -> Reader<'a, FsStore> {
    Reader {
        store,
        format: Format::Parquet,
        keyring,
        source,
    }
}

#[tokio::test]
async fn encrypted_objects_read_back_as_written_across_a_key_rotation() {
    let keys = tempfile::tempdir().unwrap();
    let ring = keyring(keys.path());
    let dir = tempfile::tempdir().unwrap();
    let written: Vec<Record> = (0..25).map(rec).collect();
    write_backup(dir.path(), &written[..12], 5, encryption(&ring, "k1")).await;
    write_backup(dir.path(), &written[12..], 5, encryption(&ring, "k2")).await;

    let store = FsStore::new(dir.path().to_path_buf());
    let chain = chain(&store, Some(&ring)).await;
    let ids: Vec<&str> = chain.iter().filter_map(|o| o.key_id.as_deref()).collect();
    assert_eq!(ids, ["k1", "k1", "k1", "k2", "k2", "k2"]);

    let source = source();
    let r = reader(&store, Some(&ring), &source);
    let mut read = Vec::new();
    for object in &chain {
        read.extend(r.read_object(object).await.unwrap());
    }
    assert_eq!(read, written);

    let summary = r.verify(&chain).await.unwrap();
    assert_eq!(summary.objects.len(), 6);
    assert_eq!((summary.first_offset, summary.last_offset), (0, 24));
    assert_eq!(summary.records, 25);
    assert_eq!(summary.holes, 0);
    assert_eq!(summary.first_hole, None);
}

#[tokio::test]
async fn holes_are_counted_per_object() {
    let dir = tempfile::tempdir().unwrap();
    // Objects of two records: 0-1 (0, 1), 2-4 (3, 4), 5-8 (7, 8), 9-9 (9).
    let written: Vec<Record> = [0, 1, 3, 4, 7, 8, 9].into_iter().map(rec).collect();
    write_backup(dir.path(), &written, 2, None).await;
    let store = FsStore::new(dir.path().to_path_buf());
    let chain = chain(&store, None).await;
    let source = source();
    let summary = reader(&store, None, &source).verify(&chain).await.unwrap();
    let per_object: Vec<(u64, u64, Option<u64>)> = summary
        .objects
        .iter()
        .map(|o| (o.records, o.holes, o.first_hole))
        .collect();
    assert_eq!(
        per_object,
        [(2, 0, None), (2, 1, Some(2)), (2, 2, Some(5)), (1, 0, None)]
    );
    assert_eq!(
        (summary.records, summary.holes, summary.first_hole),
        (7, 3, Some(2))
    );
    assert_eq!((summary.first_offset, summary.last_offset), (0, 9));
}

#[tokio::test]
async fn an_object_with_the_wrong_key_does_not_decode() {
    let keys = tempfile::tempdir().unwrap();
    let ring = keyring(keys.path());
    let dir = tempfile::tempdir().unwrap();
    let written: Vec<Record> = (0..3).map(rec).collect();
    write_backup(dir.path(), &written, 10, encryption(&ring, "k1")).await;
    // A keys-dir whose k1 is another key.
    let other = tempfile::tempdir().unwrap();
    std::fs::write(
        other.path().join("k1"),
        "AwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM=",
    )
    .unwrap();
    let wrong = Keyring::load(other.path()).unwrap();
    let store = FsStore::new(dir.path().to_path_buf());
    let chain = chain(&store, Some(&wrong)).await;
    let source = source();
    let err = reader(&store, Some(&wrong), &source)
        .verify(&chain)
        .await
        .unwrap_err();
    assert!(matches!(err, RestoreError::Object(_)), "{err}");
}

#[tokio::test]
async fn an_object_whose_content_contradicts_its_name_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let written: Vec<Record> = (0..5).map(rec).collect();
    write_backup(dir.path(), &written, 10, None).await;
    let store = FsStore::new(dir.path().to_path_buf());
    let real = chain(&store, None).await.remove(0);
    // Renamed to claim one more offset than it holds.
    let longer = mirror_fs::naming::blob_filename(0, 5, None, "parquet");
    std::fs::rename(dir.path().join(&real.name), dir.path().join(&longer)).unwrap();
    let chain = chain(&store, None).await;
    let source = source();
    let err = reader(&store, None, &source)
        .verify(&chain)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("its last record has offset 4, and its name says 5"),
        "{err}"
    );
}

#[tokio::test]
async fn records_of_another_source_are_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let written: Vec<Record> = (0..2).map(rec).collect();
    write_backup(dir.path(), &written, 10, None).await;
    let store = FsStore::new(dir.path().to_path_buf());
    let chain = chain(&store, None).await;
    let other = mirror_restore::BackupSource {
        topic: "user-states".into(),
        partition: 0,
    };
    let err = reader(&store, None, &other)
        .verify(&chain)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("is from operations/0, and this backup is of user-states/0"),
        "{err}"
    );
}
