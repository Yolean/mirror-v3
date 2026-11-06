//! `trim`: the floor moves past expired objects, the objects below it
//! are deleted, and the mirror's sink resumes on what is left.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use mirror_core::{Record, Sink, TimestampType};
use mirror_envelope::{ColumnType, Format, Keyring, ParquetCompression};
use mirror_fs::blob::{BlobError, BlobStore, NewObject, ObjectVersion, RecordTimes};
use mirror_fs::floor::FloorMarker;
use mirror_fs::{naming, FilesystemSink, FilesystemSinkConfig, FlushTriggers, FsStore};
use mirror_restore::{plan_floor_chain, trim, FloorTarget, Stop, TrimConfig, TrimReport};
use mirror_s3::{BlobEncryption, S3Sink, S3SinkConfig, S3Store};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;

/// Records are one second apart from T0.
const T0: i64 = 1_700_000_000_000;
const NOW: u64 = 1_800_000_000;

fn rec(offset: u64) -> Record {
    rec_at(offset, Some(T0 + offset as i64 * 1000))
}

fn rec_at(offset: u64, timestamp_ms: Option<i64>) -> Record {
    Record {
        topic: "audit".into(),
        partition: 0,
        source_offset: offset,
        timestamp_ms,
        timestamp_type: TimestampType::CreateTime,
        key: Some(format!("k{offset}").into_bytes()),
        value: Some(format!("v{offset}").into_bytes()),
        headers: vec![],
    }
}

fn flush() -> FlushTriggers {
    FlushTriggers {
        max_time: Duration::from_secs(3600),
        max_bytes: u64::MAX,
        max_offsets: 2,
        daily_at_utc_seconds: None,
    }
}

fn fs_cfg(root: &std::path::Path) -> FilesystemSinkConfig {
    FilesystemSinkConfig {
        root: root.to_path_buf(),
        destination_name: "audit".into(),
        partition: 0,
        format: Format::Parquet,
        compression: ParquetCompression::Zstd1,
        keys: ColumnType::Utf8,
        values: ColumnType::Utf8,
        compaction: None,
        flush: flush(),
    }
}

fn fs_dir(root: &std::path::Path) -> std::path::PathBuf {
    naming::partition_dir(root, "audit", 0)
}

/// A filesystem backup of `records`, two per object.
async fn fs_backup(root: &std::path::Path, records: Vec<Record>) -> FsStore {
    let mut sink = FilesystemSink::open(fs_cfg(root)).unwrap();
    for r in records {
        sink.write(r).await.unwrap();
    }
    sink.flush().await.unwrap();
    FsStore::new(fs_dir(root))
}

fn n(from: u64, to: u64) -> String {
    naming::blob_filename(from, to, None, "parquet")
}

/// Cutoff `ms` after T0: records at offsets below `ms / 1000` are older.
fn cutoff(ms: i64) -> FloorTarget {
    FloorTarget::Cutoff {
        cutoff_ms: (T0 + ms) as u64,
    }
}

async fn run<S: BlobStore>(store: &S, target: FloorTarget) -> Result<TrimReport, String> {
    trim(TrimConfig {
        store,
        format: Format::Parquet,
        target,
        now: NOW,
        dry_run: false,
    })
    .await
    .map_err(|e| e.to_string())
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

#[tokio::test]
async fn trim_removes_the_head_and_a_reopened_mirror_resumes_after_the_chain() {
    let tmp = tempfile::tempdir().unwrap();
    let store = fs_backup(tmp.path(), (0..10).map(rec).collect()).await;
    // Offsets 0-3 are older than the cutoff; 4-5 holds offset 5, newer.
    let report = run(&store, cutoff(4_500)).await.unwrap();
    assert_eq!(report.previous, None);
    assert_eq!(report.floor, 4);
    assert_eq!(report.chain_end, 10);
    assert_eq!(report.stop, Stop::Newer { object: n(4, 5) });
    assert_eq!(report.oldest_ms, Some((T0 + 4_000) as u64));
    assert_eq!(sorted(report.deleted.clone()), vec![n(0, 1), n(2, 3)]);
    assert!(report.failed.is_empty());
    let marker = FloorMarker {
        offset: 4,
        at: NOW,
        oldest_ms: Some((T0 + 4_000) as u64),
    }
    .filename();
    assert_eq!(report.marker.as_deref(), Some(marker.as_str()));
    assert_eq!(
        sorted(store.list().await.unwrap()),
        vec![n(4, 5), n(6, 7), n(8, 9), marker.clone()]
    );
    let content = std::fs::read_to_string(fs_dir(tmp.path()).join(&marker)).unwrap();
    assert!(content.contains("\"floor\": 4"), "{content}");
    assert!(content.contains("\"set_by\": \"retention\""), "{content}");

    let mut sink = FilesystemSink::open(fs_cfg(tmp.path())).unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 10);
    assert_eq!(sink.floor().map(|f| f.offset), Some(4));
    sink.write(rec(10)).await.unwrap();
    sink.write(rec(11)).await.unwrap();
    drop(sink);
    let mut sink = FilesystemSink::open(fs_cfg(tmp.path())).unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 12);
}

#[tokio::test]
async fn a_second_trim_moves_the_floor_on_and_keeps_the_old_marker() {
    let tmp = tempfile::tempdir().unwrap();
    let store = fs_backup(tmp.path(), (0..10).map(rec).collect()).await;
    let first = run(&store, cutoff(4_500)).await.unwrap();
    let second = run(&store, cutoff(8_500)).await.unwrap();
    assert_eq!(second.previous.map(|f| f.offset), Some(4));
    assert_eq!(second.floor, 8);
    assert_eq!(sorted(second.deleted.clone()), vec![n(4, 5), n(6, 7)]);
    let names = store.list().await.unwrap();
    assert!(names.contains(first.marker.as_ref().unwrap()), "{names:?}");
    assert!(names.contains(second.marker.as_ref().unwrap()), "{names:?}");
    // Nothing more has expired: no marker, nothing deleted.
    let third = run(&store, cutoff(8_500)).await.unwrap();
    assert_eq!(third.floor, 8);
    assert_eq!(third.marker, None);
    assert!(third.deleted.is_empty());
}

#[tokio::test]
async fn every_object_expired_leaves_the_floor_at_the_end_and_the_mirror_resumes_there() {
    let tmp = tempfile::tempdir().unwrap();
    let store = fs_backup(tmp.path(), (0..4).map(rec).collect()).await;
    let report = run(&store, cutoff(60_000)).await.unwrap();
    assert_eq!(report.floor, 4);
    assert_eq!(report.stop, Stop::End);
    assert_eq!(report.oldest_ms, None);
    let names = store.list().await.unwrap();
    assert_eq!(names.len(), 1, "only the marker: {names:?}");
    let mut sink = FilesystemSink::open(fs_cfg(tmp.path())).unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 4);
    sink.write(rec(4)).await.unwrap();
    sink.write(rec(5)).await.unwrap();
    drop(sink);
    // The first object above the floor makes its oldest record known.
    let report = run(&store, cutoff(60_000)).await.unwrap();
    assert_eq!(report.floor, 6);
    let report = run(&store, cutoff(0)).await.unwrap();
    assert_eq!(report.marker, None, "the floor did not move");
}

#[tokio::test]
async fn the_oldest_record_becomes_known_with_a_marker_at_the_same_floor() {
    let tmp = tempfile::tempdir().unwrap();
    let store = fs_backup(tmp.path(), (0..4).map(rec).collect()).await;
    run(&store, cutoff(60_000)).await.unwrap();
    let mut sink = FilesystemSink::open(fs_cfg(tmp.path())).unwrap();
    sink.write(rec(4)).await.unwrap();
    sink.write(rec(5)).await.unwrap();
    drop(sink);
    let report = run(&store, cutoff(0)).await.unwrap();
    assert_eq!(report.floor, 4);
    assert_eq!(report.oldest_ms, Some((T0 + 4_000) as u64));
    assert!(report.marker.is_some());
    let (floor, markers) = mirror_fs::floor::floor_of(&store.list().await.unwrap()).unwrap();
    assert_eq!(markers.len(), 2);
    let floor = floor.unwrap();
    assert_eq!(floor.oldest_ms, Some((T0 + 4_000) as u64));
}

/// A filesystem store whose deletes of some objects fail, as a retention
/// lock makes them.
struct Locked {
    inner: FsStore,
    locked: Mutex<Vec<String>>,
}

#[async_trait]
impl BlobStore for Locked {
    async fn list(&self) -> Result<Vec<String>, BlobError> {
        self.inner.list().await
    }
    async fn put_new(
        &self,
        name: &str,
        bytes: Vec<u8>,
        object: &NewObject,
    ) -> Result<Option<u64>, BlobError> {
        self.inner.put_new(name, bytes, object).await
    }
    async fn get(&self, name: &str) -> Result<Vec<u8>, BlobError> {
        self.inner.get(name).await
    }
    async fn list_after(&self, after: Option<&str>) -> Result<Vec<String>, BlobError> {
        self.inner.list_after(after).await
    }
    fn location(&self, name: &str) -> String {
        self.inner.location(name)
    }
    async fn record_times(&self, name: &str) -> Result<Option<RecordTimes>, BlobError> {
        self.inner.record_times(name).await
    }
    async fn delete(&self, name: &str) -> Result<(), BlobError> {
        if self.locked.lock().unwrap().iter().any(|l| l == name) {
            return Err(BlobError::Store(format!("{name}: object is locked")));
        }
        self.inner.delete(name).await
    }
}

#[tokio::test]
async fn objects_below_the_floor_that_do_not_delete_are_ignored_and_deleted_later() {
    let tmp = tempfile::tempdir().unwrap();
    let inner = fs_backup(tmp.path(), (0..10).map(rec).collect()).await;
    let store = Locked {
        inner,
        locked: Mutex::new(vec![n(0, 1)]),
    };
    let report = run(&store, cutoff(4_500)).await.unwrap();
    assert_eq!(report.floor, 4);
    assert_eq!(report.deleted, vec![n(2, 3)]);
    assert_eq!(report.failed.len(), 1);
    assert_eq!(report.failed[0].0, n(0, 1));

    let mut sink = FilesystemSink::open(fs_cfg(tmp.path())).unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 10);
    assert_eq!(sink.below_floor_at_open(), 1);
    drop(sink);

    store.locked.lock().unwrap().clear();
    let report = run(&store, cutoff(4_500)).await.unwrap();
    assert_eq!(report.floor, 4);
    assert_eq!(report.marker, None);
    assert_eq!(report.deleted, vec![n(0, 1)]);
    let sink = FilesystemSink::open(fs_cfg(tmp.path())).unwrap();
    assert_eq!(sink.below_floor_at_open(), 0);
}

#[tokio::test]
async fn a_floor_lower_than_the_current_one_or_past_the_chain_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let store = fs_backup(tmp.path(), (0..10).map(rec).collect()).await;
    run(&store, cutoff(4_500)).await.unwrap();
    let e = run(&store, FloorTarget::Offset(2)).await.unwrap_err();
    assert!(
        e.contains("lower than the current one, 4, is refused"),
        "{e}"
    );
    let e = run(&store, FloorTarget::Offset(11)).await.unwrap_err();
    assert!(
        e.contains("above the end of the chain, 10, is refused"),
        "{e}"
    );
    // The current floor again changes nothing.
    let same = run(&store, FloorTarget::Offset(4)).await.unwrap();
    assert_eq!(same.marker, None);
    // Up to the end of the chain is allowed.
    let end = run(&store, FloorTarget::Offset(10)).await.unwrap();
    assert_eq!(end.floor, 10);
    assert_eq!(end.deleted.len(), 3);
}

#[tokio::test]
async fn trim_floor_adopts_a_head_removed_by_other_means() {
    let tmp = tempfile::tempdir().unwrap();
    let store = fs_backup(tmp.path(), (0..10).map(rec).collect()).await;
    for name in [n(0, 1), n(2, 3)] {
        std::fs::remove_file(fs_dir(tmp.path()).join(name)).unwrap();
    }
    let e = FilesystemSink::open(fs_cfg(tmp.path())).err().unwrap();
    assert!(e.to_string().contains("trim --floor 4"), "{e}");
    let e = run(&store, cutoff(4_500)).await.unwrap_err();
    assert!(e.contains("trim --floor 4"), "{e}");
    // Not below where the chain now starts, nor in a gap.
    let e = run(&store, FloorTarget::Offset(3)).await.unwrap_err();
    assert!(e.contains("offsets 3-3 are in no object"), "{e}");

    let report = run(&store, FloorTarget::Offset(4)).await.unwrap();
    assert_eq!(report.floor, 4);
    assert_eq!(report.stop, Stop::Given);
    assert_eq!(report.oldest_ms, Some((T0 + 4_000) as u64));
    let content = std::fs::read_to_string(fs_dir(tmp.path()).join(report.marker.unwrap())).unwrap();
    assert!(
        content.contains("\"set_by\": \"trim --floor\""),
        "{content}"
    );
    let mut sink = FilesystemSink::open(fs_cfg(tmp.path())).unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 10);
}

#[tokio::test]
async fn a_gap_above_the_floor_is_still_a_corrupt_chain() {
    let tmp = tempfile::tempdir().unwrap();
    let store = fs_backup(tmp.path(), (0..10).map(rec).collect()).await;
    run(&store, cutoff(2_500)).await.unwrap();
    std::fs::remove_file(fs_dir(tmp.path()).join(n(6, 7))).unwrap();
    let e = FilesystemSink::open(fs_cfg(tmp.path())).err().unwrap();
    assert!(
        e.to_string()
            .contains("gap or overlap in chain: expected from=6"),
        "{e}"
    );
    let e = run(&store, cutoff(9_500)).await.unwrap_err();
    assert!(e.contains("gap in the chain: offsets 6-7"), "{e}");
}

#[tokio::test]
async fn records_whose_age_is_unknown_block_the_expiry() {
    let tmp = tempfile::tempdir().unwrap();
    let mut records: Vec<Record> = (0..10).map(rec).collect();
    records[5] = rec_at(5, None);
    let store = fs_backup(tmp.path(), records).await;
    let report = run(&store, cutoff(60_000)).await.unwrap();
    assert_eq!(report.floor, 4);
    let (object, reason) = report.blocked().unwrap();
    assert_eq!(object, n(4, 5));
    assert!(
        reason.contains("1 record(s) without a timestamp"),
        "{reason}"
    );
    assert_eq!(report.oldest_ms, Some((T0 + 4_000) as u64));

    let tmp = tempfile::tempdir().unwrap();
    let mut records: Vec<Record> = (0..10).map(rec).collect();
    records[3] = rec_at(3, Some((NOW as i64 + 86_400) * 1000));
    let store = fs_backup(tmp.path(), records).await;
    let report = run(&store, cutoff(60_000)).await.unwrap();
    assert_eq!(report.floor, 2);
    let (object, reason) = report.blocked().unwrap();
    assert_eq!(object, n(2, 3));
    assert!(reason.contains("in the future"), "{reason}");
}

#[tokio::test]
async fn a_dry_run_writes_and_deletes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let store = fs_backup(tmp.path(), (0..10).map(rec).collect()).await;
    let before = sorted(store.list().await.unwrap());
    let report = trim(TrimConfig {
        store: &store,
        format: Format::Parquet,
        target: cutoff(4_500),
        now: NOW,
        dry_run: true,
    })
    .await
    .unwrap();
    assert_eq!(report.floor, 4);
    assert!(report.marker.is_some());
    assert_eq!(report.deleted.len(), 2);
    assert_eq!(sorted(store.list().await.unwrap()), before);
}

/// An S3 store whose objects may not be read: trim must do with names
/// and HEADs.
struct NoGet(S3Store);

#[async_trait]
impl BlobStore for NoGet {
    async fn list(&self) -> Result<Vec<String>, BlobError> {
        self.0.list().await
    }
    async fn put_new(
        &self,
        name: &str,
        bytes: Vec<u8>,
        object: &NewObject,
    ) -> Result<Option<u64>, BlobError> {
        self.0.put_new(name, bytes, object).await
    }
    async fn get(&self, name: &str) -> Result<Vec<u8>, BlobError> {
        panic!("trim read {name}");
    }
    async fn list_after(&self, after: Option<&str>) -> Result<Vec<String>, BlobError> {
        self.0.list_after(after).await
    }
    fn location(&self, name: &str) -> String {
        self.0.location(name)
    }
    async fn record_times(&self, name: &str) -> Result<Option<RecordTimes>, BlobError> {
        self.0.record_times(name).await
    }
    async fn delete(&self, name: &str) -> Result<(), BlobError> {
        self.0.delete(name).await
    }
}

fn s3_cfg(store: Arc<dyn ObjectStore>, encryption: Option<BlobEncryption>) -> S3SinkConfig {
    S3SinkConfig {
        read_store: Arc::clone(&store),
        write_store: store,
        prefix: Some(Path::from("archive")),
        destination_name: "audit".into(),
        partition: 0,
        format: Format::Parquet,
        compression: ParquetCompression::Zstd1,
        keys: ColumnType::Utf8,
        values: ColumnType::Utf8,
        compaction: None,
        flush: flush(),
        encryption,
        lock: None,
    }
}

fn s3_trim_store(mem: &Arc<dyn ObjectStore>) -> NoGet {
    NoGet(S3Store::new(
        Arc::clone(mem),
        Arc::clone(mem),
        Path::from("archive/audit/0"),
    ))
}

#[tokio::test]
async fn trim_of_an_encrypted_s3_backup_needs_neither_content_nor_keys() {
    let keys = tempfile::tempdir().unwrap();
    std::fs::write(
        keys.path().join("k1"),
        "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
    )
    .unwrap();
    let encryption = BlobEncryption {
        key_id: "k1".into(),
        keyring: Arc::new(Keyring::load(keys.path()).unwrap()),
    };
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut sink = S3Sink::open(s3_cfg(Arc::clone(&mem), Some(encryption.clone())))
        .await
        .unwrap();
    for o in 0..10 {
        sink.write(rec(o)).await.unwrap();
    }
    drop(sink);

    let store = s3_trim_store(&mem);
    let report = run(&store, cutoff(4_500)).await.unwrap();
    assert_eq!(report.floor, 4);
    let k = |a, b| naming::blob_filename(a, b, Some("k1"), "parquet");
    assert_eq!(sorted(report.deleted.clone()), vec![k(0, 1), k(2, 3)]);
    assert_eq!(report.oldest_ms, Some((T0 + 4_000) as u64));

    let mut sink = S3Sink::open(s3_cfg(mem, Some(encryption))).await.unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 10);
    assert_eq!(sink.floor().map(|f| f.offset), Some(4));
}

#[tokio::test]
async fn s3_objects_without_record_time_metadata_block_until_adopted() {
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut sink = S3Sink::open(s3_cfg(Arc::clone(&mem), None)).await.unwrap();
    for o in 0..10 {
        sink.write(rec(o)).await.unwrap();
    }
    drop(sink);
    // Objects 0-1 and 2-3 as an older mirror-v3 wrote them: no metadata.
    for name in [n(0, 1), n(2, 3)] {
        let path = Path::from(format!("archive/audit/0/{name}"));
        let bytes = mem.get(&path).await.unwrap().bytes().await.unwrap();
        mem.put(&path, bytes.into()).await.unwrap();
    }
    let store = s3_trim_store(&mem);
    let report = run(&store, cutoff(9_500)).await.unwrap();
    assert_eq!(report.floor, 0);
    let (object, reason) = report.blocked().unwrap();
    assert_eq!(object, n(0, 1));
    assert!(reason.contains("no record-time metadata"), "{reason}");
    assert!(report.deleted.is_empty());
    // The first trim still writes a marker: the floor at 0.
    assert!(report.marker.is_some());

    // A person checked the records of 0-3 and moves the floor past them.
    let adopted = run(&store, FloorTarget::Offset(4)).await.unwrap();
    assert_eq!(adopted.floor, 4);
    let report = run(&store, cutoff(9_500)).await.unwrap();
    assert_eq!(report.floor, 10);
    assert_eq!(report.stop, Stop::End);
    let planned = plan_floor_chain(&store.list().await.unwrap(), Format::Parquet, None).unwrap();
    assert!(planned.objects.is_empty());
    assert!(planned.below_floor.is_empty());
}

/// A filesystem backup as a versioned bucket shows it: every object has
/// one version, `v-<name>`, and `extra` adds versions a DELETE already
/// hid behind delete markers. Records what trim writes and deletes;
/// versions in `locked` refuse to delete, as under a retention lock.
struct Versioned {
    inner: FsStore,
    extra: Vec<ObjectVersion>,
    locked: Vec<String>,
    puts: Mutex<Vec<(String, NewObject)>>,
    deleted_versions: Mutex<Vec<(String, String)>>,
    plain_deletes: Mutex<Vec<String>>,
}

impl Versioned {
    fn new(inner: FsStore, extra: Vec<ObjectVersion>, locked: Vec<String>) -> Self {
        Self {
            inner,
            extra,
            locked,
            puts: Mutex::new(Vec::new()),
            deleted_versions: Mutex::new(Vec::new()),
            plain_deletes: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl BlobStore for Versioned {
    async fn list(&self) -> Result<Vec<String>, BlobError> {
        self.inner.list().await
    }
    async fn put_new(
        &self,
        name: &str,
        bytes: Vec<u8>,
        object: &NewObject,
    ) -> Result<Option<u64>, BlobError> {
        self.puts.lock().unwrap().push((name.to_string(), *object));
        self.inner.put_new(name, bytes, object).await
    }
    async fn get(&self, name: &str) -> Result<Vec<u8>, BlobError> {
        self.inner.get(name).await
    }
    async fn list_after(&self, after: Option<&str>) -> Result<Vec<String>, BlobError> {
        self.inner.list_after(after).await
    }
    fn location(&self, name: &str) -> String {
        self.inner.location(name)
    }
    async fn record_times(&self, name: &str) -> Result<Option<RecordTimes>, BlobError> {
        self.inner.record_times(name).await
    }
    async fn delete(&self, name: &str) -> Result<(), BlobError> {
        self.plain_deletes.lock().unwrap().push(name.to_string());
        Ok(())
    }
    async fn versions(&self) -> Result<Option<Vec<ObjectVersion>>, BlobError> {
        let mut versions: Vec<ObjectVersion> = self
            .inner
            .list()
            .await?
            .into_iter()
            .map(|name| ObjectVersion {
                version_id: format!("v-{name}"),
                name,
                delete_marker: false,
            })
            .collect();
        versions.extend(self.extra.iter().cloned());
        Ok(Some(versions))
    }
    async fn delete_version(&self, name: &str, version_id: &str) -> Result<(), BlobError> {
        if self.locked.iter().any(|l| l == version_id) {
            return Err(BlobError::Store(format!(
                "{name} {version_id}: AccessDenied"
            )));
        }
        self.deleted_versions
            .lock()
            .unwrap()
            .push((name.to_string(), version_id.to_string()));
        Ok(())
    }
}

#[tokio::test]
async fn on_a_versioned_store_trim_deletes_every_version_below_the_floor_by_id() {
    let tmp = tempfile::tempdir().unwrap();
    let inner = fs_backup(tmp.path(), (0..10).map(rec).collect()).await;
    // 0-1 was deleted without a version id before: its data is a
    // noncurrent version behind a delete marker, and it is not listed.
    std::fs::remove_file(fs_dir(tmp.path()).join(n(0, 1))).unwrap();
    let hidden = |id: &str, delete_marker| ObjectVersion {
        name: n(0, 1),
        version_id: id.into(),
        delete_marker,
    };
    let foreign = ObjectVersion {
        name: "notes.txt".into(),
        version_id: "f1".into(),
        delete_marker: false,
    };
    std::fs::write(
        fs_dir(tmp.path()).join(
            FloorMarker {
                offset: 2,
                at: NOW - 86_400,
                oldest_ms: None,
            }
            .filename(),
        ),
        b"{}",
    )
    .unwrap();
    let store = Versioned::new(
        inner,
        vec![hidden("old", false), hidden("dm", true), foreign],
        vec![format!("v-{}", n(4, 5))],
    );
    let report = run(&store, cutoff(6_500)).await.unwrap();
    assert_eq!(report.floor, 6);
    assert!(report.versioned);
    assert!(
        store.plain_deletes.lock().unwrap().is_empty(),
        "no DELETE without a version id"
    );
    assert_eq!(
        sorted(
            store
                .deleted_versions
                .lock()
                .unwrap()
                .iter()
                .map(|(_, id)| id.clone())
                .collect()
        ),
        vec![
            "dm".to_string(),
            "old".to_string(),
            format!("v-{}", n(2, 3))
        ],
        "the hidden object too; not the floor markers, not foreign objects, not above the floor"
    );
    assert_eq!(
        sorted(report.deleted.clone()),
        vec![
            format!("{} (delete marker dm)", n(0, 1)),
            format!("{} (version old)", n(0, 1)),
            format!("{} (version v-{})", n(2, 3), n(2, 3)),
        ]
    );
    assert_eq!(
        report
            .failed
            .iter()
            .map(|(label, _)| label.clone())
            .collect::<Vec<_>>(),
        vec![format!("{} (version v-{})", n(4, 5), n(4, 5))],
        "a version still under its lock is left for the next trim"
    );
}

#[tokio::test]
async fn the_floor_marker_is_written_as_a_marker_for_the_store_to_lock() {
    let tmp = tempfile::tempdir().unwrap();
    let inner = fs_backup(tmp.path(), (0..10).map(rec).collect()).await;
    let store = Versioned::new(inner, vec![], vec![]);
    let report = run(&store, cutoff(4_500)).await.unwrap();
    let marker = report.marker.expect("a marker");
    assert_eq!(
        *store.puts.lock().unwrap(),
        vec![(marker, NewObject::Marker { written_at: NOW })]
    );
}
