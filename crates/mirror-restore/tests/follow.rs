//! Continuous restore: a backup read as a mirror's source by
//! `run_mirror`, into a target that, like a Kafka topic, takes a record
//! only at its high watermark and cannot hold holes.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common::*;
use mirror_core::{
    run_mirror_with_heartbeat, MirrorError, Record, Sink, SinkError, Source, TimestampType,
};
use mirror_envelope::Format;
use mirror_fs::blob::{BlobError, BlobStore};
use mirror_restore::{same_record, ChainSource, ChainSourceConfig, OffsetMode};

/// A partition shared with the test: what it holds is readable while
/// the loop runs and after it ends.
#[derive(Clone, Default)]
struct Target(Arc<Mutex<Vec<Record>>>);

impl Target {
    fn holding(records: Vec<Record>) -> Self {
        Self(Arc::new(Mutex::new(records)))
    }

    fn records(&self) -> Vec<Record> {
        self.0.lock().unwrap().clone()
    }

    fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}

#[async_trait]
impl Sink for Target {
    async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
        Ok(self.len() as u64)
    }

    async fn write(&mut self, record: Record) -> Result<(), SinkError> {
        let mut held = self.0.lock().unwrap();
        if record.source_offset != held.len() as u64 {
            return Err(SinkError::UnexpectedPosition {
                expected: record.source_offset,
                actual: held.len() as u64,
            });
        }
        held.push(record);
        Ok(())
    }
}

struct Follow {
    shutdown: tokio::sync::oneshot::Sender<()>,
    run: tokio::task::JoinHandle<Result<(), MirrorError>>,
}

impl Follow {
    async fn start(dir: &std::path::Path, mode: OffsetMode, target: &Target) -> Self {
        Self::polling(dir, mode, target, Duration::from_millis(20)).await
    }

    async fn polling(
        dir: &std::path::Path,
        mode: OffsetMode,
        target: &Target,
        poll_interval: Duration,
    ) -> Self {
        let source = ChainSource::open(ChainSourceConfig {
            store: Arc::new(FsStore::new(dir.to_path_buf())),
            location: dir.display().to_string(),
            format: Format::Parquet,
            keyring: None,
            source: source(),
            mode,
            chain_start: 0,
            poll_interval,
        })
        .await
        .unwrap();
        let (shutdown, rx) = tokio::sync::oneshot::channel::<()>();
        // A heartbeat shorter than the poll interval: the loop drops
        // poll_one mid-wait, and mid-read, all the time.
        let run = tokio::spawn(run_mirror_with_heartbeat(
            source,
            target.clone(),
            async move {
                let _ = rx.await;
            },
            Duration::from_millis(3),
        ));
        Self { shutdown, run }
    }

    async fn stop(self) -> Result<(), MirrorError> {
        let _ = self.shutdown.send(());
        self.run.await.unwrap()
    }

    async fn ended(self) -> MirrorError {
        tokio::time::timeout(Duration::from_secs(10), self.run)
            .await
            .expect("the loop ends")
            .unwrap()
            .unwrap_err()
    }
}

async fn wait_for(target: &Target, records: usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while target.len() < records {
        assert!(
            std::time::Instant::now() < deadline,
            "the target holds {} of {records} records",
            target.len()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn renumbered(records: &[Record]) -> Vec<Record> {
    records
        .iter()
        .enumerate()
        .map(|(i, r)| Record {
            source_offset: i as u64,
            ..r.clone()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preserve_follows_a_backup_as_it_grows() {
    let dir = tempfile::tempdir().unwrap();
    let written: Vec<Record> = (0..17).map(rec).collect();
    write_backup(dir.path(), &written[..10], 5, None).await;
    let target = Target::default();
    let follow = Follow::start(dir.path(), OffsetMode::Preserve, &target).await;
    wait_for(&target, 10).await;
    write_backup(dir.path(), &written[10..], 4, None).await;
    wait_for(&target, 17).await;
    follow.stop().await.unwrap();
    assert_eq!(target.records(), written);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_backup_is_waited_for() {
    let dir = tempfile::tempdir().unwrap();
    let target = Target::default();
    let follow = Follow::start(dir.path(), OffsetMode::Preserve, &target).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let written: Vec<Record> = (0..6).map(rec).collect();
    write_backup(dir.path(), &written, 4, None).await;
    wait_for(&target, 6).await;
    follow.stop().await.unwrap();
    assert_eq!(target.records(), written);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preserve_resumes_at_the_targets_high_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let written: Vec<Record> = (0..12).map(rec).collect();
    write_backup(dir.path(), &written, 5, None).await;
    // Mid-object: 7 is in 5-9.
    let target = Target::holding(written[..7].to_vec());
    let follow = Follow::start(dir.path(), OffsetMode::Preserve, &target).await;
    wait_for(&target, 12).await;
    follow.stop().await.unwrap();
    assert_eq!(target.records(), written);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renumber_closes_holes_and_resumes_by_record_count() {
    let dir = tempfile::tempdir().unwrap();
    let written: Vec<Record> = [0, 1, 3, 4, 7, 8, 9, 12].into_iter().map(rec).collect();
    write_backup(dir.path(), &written, 2, None).await;
    let expected = renumbered(&written);
    // 3 records restored: the next is the second of object 2-4.
    let target = Target::holding(expected[..3].to_vec());
    let follow = Follow::start(dir.path(), OffsetMode::Renumber, &target).await;
    wait_for(&target, 8).await;
    follow.stop().await.unwrap();
    assert_eq!(target.records(), expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preserve_stops_at_a_hole_with_what_came_before_it_restored() {
    let dir = tempfile::tempdir().unwrap();
    let written: Vec<Record> = [0, 1, 2, 4, 5].into_iter().map(rec).collect();
    write_backup(dir.path(), &written, 3, None).await;
    let target = Target::default();
    let err = Follow::start(dir.path(), OffsetMode::Preserve, &target)
        .await
        .ended()
        .await;
    assert!(
        err.to_string().contains("offset 3 is a hole in the backup"),
        "{err}"
    );
    assert!(!err.is_transient(), "{err}");
    assert_eq!(target.records(), written[..3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_target_past_the_backup_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let written: Vec<Record> = (0..5).map(rec).collect();
    write_backup(dir.path(), &written, 5, None).await;
    let target = Target::holding((0..6).map(rec).collect());
    let err = Follow::start(dir.path(), OffsetMode::Preserve, &target)
        .await
        .ended()
        .await;
    assert!(
        matches!(
            err,
            MirrorError::SinkAheadOfSource {
                sink_offset: 6,
                source_hwm: 5
            }
        ),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gap_in_new_objects_ends_the_restore() {
    let dir = tempfile::tempdir().unwrap();
    let written: Vec<Record> = (0..20).map(rec).collect();
    write_backup(dir.path(), &written[..10], 5, None).await;
    let target = Target::default();
    let follow = Follow::start(dir.path(), OffsetMode::Preserve, &target).await;
    wait_for(&target, 10).await;
    // 15-19 appears; 10-14 never does.
    let elsewhere = tempfile::tempdir().unwrap();
    write_backup(elsewhere.path(), &written, 5, None).await;
    let name = mirror_fs::naming::blob_filename(15, 19, None, "parquet");
    std::fs::copy(elsewhere.path().join(&name), dir.path().join(&name)).unwrap();
    let err = follow.ended().await;
    assert!(
        err.to_string().contains("gap in the chain: offsets 10-14"),
        "{err}"
    );
    assert_eq!(target.len(), 10);
}

#[tokio::test]
async fn record_at_is_the_record_a_restored_target_holds() {
    let dir = tempfile::tempdir().unwrap();
    let written: Vec<Record> = [0, 1, 3, 4, 7].into_iter().map(rec).collect();
    write_backup(dir.path(), &written, 2, None).await;
    let open = |mode| {
        ChainSource::open(ChainSourceConfig {
            store: Arc::new(FsStore::new(dir.path().to_path_buf())),
            location: dir.path().display().to_string(),
            format: Format::Parquet,
            keyring: None,
            source: source(),
            mode,
            chain_start: 0,
            poll_interval: Duration::from_millis(20),
        })
    };
    let mut preserve = open(OffsetMode::Preserve).await.unwrap();
    assert_eq!(preserve.record_at(3).await.unwrap(), Some(rec(3)));
    assert_eq!(preserve.record_at(2).await.unwrap(), None, "a hole");
    assert_eq!(preserve.record_at(8).await.unwrap(), None, "past the end");
    let mut renumber = open(OffsetMode::Renumber).await.unwrap();
    assert_eq!(
        renumber.record_at(3).await.unwrap(),
        Some(renumbered(&written)[3].clone())
    );
    assert_eq!(renumber.record_at(5).await.unwrap(), None);
}

#[tokio::test]
async fn preserve_of_a_chain_that_starts_after_zero_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let err = ChainSource::open(ChainSourceConfig {
        store: Arc::new(FsStore::new(dir.path().to_path_buf())),
        location: dir.path().display().to_string(),
        format: Format::Parquet,
        keyring: None,
        source: source(),
        mode: OffsetMode::Preserve,
        chain_start: 5,
        poll_interval: Duration::from_millis(20),
    })
    .await
    .err()
    .expect("refused");
    assert!(err.to_string().contains("starts at offset 5"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_idle_drift_check_runs_with_a_poll_interval_longer_than_the_heartbeat() {
    let dir = tempfile::tempdir().unwrap();
    let written: Vec<Record> = (0..5).map(rec).collect();
    write_backup(dir.path(), &written, 5, None).await;
    let target = Target::default();
    // The heartbeat (3 ms) drops poll_one about 70 times per wait.
    let follow = Follow::polling(
        dir.path(),
        OffsetMode::Preserve,
        &target,
        Duration::from_millis(200),
    )
    .await;
    wait_for(&target, 5).await;
    // Another writer.
    target.0.lock().unwrap().push(rec(5));
    let err = follow.ended().await;
    assert!(
        matches!(
            err,
            MirrorError::DestinationDrift {
                expected: 5,
                actual: 6
            }
        ),
        "{err}"
    );
}

#[tokio::test]
async fn a_target_is_resumed_only_if_it_ends_with_the_backups_record() {
    let dir = tempfile::tempdir().unwrap();
    let written: Vec<Record> = [0, 1, 3, 4].into_iter().map(rec).collect();
    write_backup(dir.path(), &written, 2, None).await;
    let open = |mode| {
        ChainSource::open(ChainSourceConfig {
            store: Arc::new(FsStore::new(dir.path().to_path_buf())),
            location: dir.path().display().to_string(),
            format: Format::Parquet,
            keyring: None,
            source: source(),
            mode,
            chain_start: 0,
            poll_interval: Duration::from_millis(20),
        })
    };
    let mut preserve = open(OffsetMode::Preserve).await.unwrap();
    preserve.check_resume(0, &rec(0)).await.unwrap();
    preserve.check_resume(4, &rec(3)).await.unwrap();
    let other = Record {
        value: Some(b"other".to_vec()),
        ..rec(3)
    };
    let err = preserve.check_resume(4, &other).await.unwrap_err();
    assert!(
        err.to_string().contains("not the backup's record there"),
        "{err}"
    );
    let err = preserve.check_resume(9, &rec(8)).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("the backup has no record at offset 8"),
        "{err}"
    );
    // With renumber the target's offset 2 holds the record of offset 3.
    let mut renumber = open(OffsetMode::Renumber).await.unwrap();
    renumber.check_resume(3, &rec(3)).await.unwrap();
    let err = renumber.check_resume(3, &rec(1)).await.unwrap_err();
    assert!(
        err.to_string().contains("not the backup's record there"),
        "{err}"
    );
}

#[test]
fn a_backup_record_without_a_timestamp_matches_any_timestamp() {
    let ours = Record {
        timestamp_ms: None,
        ..rec(1)
    };
    let theirs = Record {
        timestamp_ms: Some(42),
        ..rec(1)
    };
    assert!(same_record(&ours, &theirs));
    assert!(!same_record(&rec(1), &theirs));
    let other_headers = Record {
        headers: Vec::new(),
        ..rec(1)
    };
    assert!(!same_record(&rec(1), &other_headers));
}

/// A filesystem store that counts the objects read from it.
struct Counting {
    inner: FsStore,
    gets: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl BlobStore for Counting {
    async fn list(&self) -> Result<Vec<String>, BlobError> {
        self.inner.list().await
    }
    async fn put_new(&self, name: &str, bytes: Vec<u8>) -> Result<(), BlobError> {
        self.inner.put_new(name, bytes).await
    }
    async fn get(&self, name: &str) -> Result<Vec<u8>, BlobError> {
        self.gets.lock().unwrap().push(name.to_string());
        self.inner.get(name).await
    }
    async fn list_after(&self, after: Option<&str>) -> Result<Vec<String>, BlobError> {
        self.inner.list_after(after).await
    }
    fn location(&self, name: &str) -> String {
        self.inner.location(name)
    }
}

async fn counting(
    dir: &std::path::Path,
    mode: OffsetMode,
) -> (ChainSource<Counting>, Arc<Mutex<Vec<String>>>) {
    let gets = Arc::new(Mutex::new(Vec::new()));
    let source = ChainSource::open(ChainSourceConfig {
        store: Arc::new(Counting {
            inner: FsStore::new(dir.to_path_buf()),
            gets: Arc::clone(&gets),
        }),
        location: dir.display().to_string(),
        format: Format::Parquet,
        keyring: None,
        source: source(),
        mode,
        chain_start: 0,
        poll_interval: Duration::from_millis(20),
    })
    .await
    .unwrap();
    (source, gets)
}

#[tokio::test]
async fn a_start_reads_only_the_objects_before_the_targets_position_and_each_once() {
    let dir = tempfile::tempdir().unwrap();
    let written: Vec<Record> = [0, 1, 3, 4, 7, 8, 9, 12].into_iter().map(rec).collect();
    write_backup(dir.path(), &written, 2, None).await;
    // Into an empty target: only the first object is read, once, before
    // its first record is yielded.
    let (mut source, gets) = counting(dir.path(), OffsetMode::Renumber).await;
    assert_eq!(
        source.high_watermark().await.unwrap(),
        13,
        "positions, holes included"
    );
    source.seek(0).await.unwrap();
    assert_eq!(
        source.poll_one().await.unwrap(),
        Some(renumbered(&written)[0].clone())
    );
    let object = |from, to| mirror_fs::naming::blob_filename(from, to, None, "parquet");
    assert_eq!(*gets.lock().unwrap(), [object(0, 1)]);
    // Resuming at 3, as the CLI does: the resume check, then the run
    // loop. Objects 0-1 and 2-4 (which holds record 3) are read once.
    let (mut source, gets) = counting(dir.path(), OffsetMode::Renumber).await;
    let expected = renumbered(&written);
    source.check_resume(3, &expected[2]).await.unwrap();
    source.high_watermark().await.unwrap();
    source.seek(3).await.unwrap();
    assert_eq!(source.poll_one().await.unwrap(), Some(expected[3].clone()));
    assert_eq!(*gets.lock().unwrap(), [object(0, 1), object(2, 4)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_renumber_target_past_the_backups_records_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    // 4 records over positions 0-4.
    let written: Vec<Record> = [0, 1, 3, 4].into_iter().map(rec).collect();
    write_backup(dir.path(), &written, 2, None).await;
    let mut held = renumbered(&written);
    held.push(rec(4));
    let target = Target::holding(held);
    let err = Follow::start(dir.path(), OffsetMode::Renumber, &target)
        .await
        .ended()
        .await;
    assert!(
        err.to_string()
            .contains("the target is at offset 5, past the backup's end at 4"),
        "{err}"
    );
}

#[test]
fn a_target_that_stamps_log_append_time_matches_on_everything_else() {
    let theirs = Record {
        timestamp_ms: Some(42),
        timestamp_type: TimestampType::LogAppendTime,
        ..rec(1)
    };
    assert!(same_record(&rec(1), &theirs));
    let other = Record {
        value: Some(b"other".to_vec()),
        ..theirs
    };
    assert!(!same_record(&rec(1), &other));
}
