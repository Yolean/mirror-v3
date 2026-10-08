//! The produce pass against a mock of the Kafka destination: a sink
//! whose next offset is its high watermark and that accepts a record
//! only at exactly that offset.

mod common;

use common::*;
use mirror_core::mock::MockSink;
use mirror_core::{Record, SinkError};
use mirror_envelope::{Format, Keyring};
use mirror_restore::{
    plan_chain, produce, BackupSource, BackupSummary, ChainObject, OffsetMode, Reader,
    RestoreError, RestoreReport,
};

struct Backup {
    _dir: tempfile::TempDir,
    store: FsStore,
    source: BackupSource,
}

impl Backup {
    async fn write(records: &[Record], per_object: u64) -> Self {
        let dir = tempfile::tempdir().unwrap();
        write_backup(dir.path(), records, per_object, None).await;
        Self {
            store: FsStore::new(dir.path().to_path_buf()),
            _dir: dir,
            source: source(),
        }
    }

    fn reader(&self) -> Reader<'_, FsStore> {
        Reader {
            store: &self.store,
            format: Format::Parquet,
            keyring: None::<&Keyring>,
            source: &self.source,
        }
    }

    async fn verified(&self, chain_start: u64) -> (Vec<ChainObject>, BackupSummary) {
        use mirror_fs::blob::BlobStore;
        let names = self.store.list().await.unwrap();
        let chain = plan_chain(&names, Format::Parquet, chain_start, None).unwrap();
        let summary = self.reader().verify(&chain).await.unwrap();
        (chain, summary)
    }

    async fn restore(
        &self,
        chain_start: u64,
        mode: OffsetMode,
        target: &mut MockSink,
    ) -> Result<RestoreReport, RestoreError> {
        let (chain, summary) = self.verified(chain_start).await;
        produce(&self.reader(), &chain, &summary, mode, target).await
    }
}

#[tokio::test]
async fn preserve_produces_every_record_at_its_offset() {
    let written: Vec<Record> = (0..23).map(rec).collect();
    let backup = Backup::write(&written, 5).await;
    let mut target = MockSink::starting_at(0);
    let report = backup
        .restore(0, OffsetMode::Preserve, &mut target)
        .await
        .unwrap();
    assert_eq!(
        report,
        RestoreReport {
            records: 23,
            high_watermark: 23
        }
    );
    assert_eq!(target.writes, written);
}

#[tokio::test]
async fn preserve_refuses_a_backup_with_holes_before_producing_anything() {
    let written: Vec<Record> = [0, 1, 3, 4].into_iter().map(rec).collect();
    let backup = Backup::write(&written, 2).await;
    let mut target = MockSink::starting_at(0);
    let err = backup
        .restore(0, OffsetMode::Preserve, &mut target)
        .await
        .unwrap_err();
    assert!(matches!(err, RestoreError::Mode(_)), "{err}");
    assert!(err.to_string().contains("the first at offset 2"), "{err}");
    assert!(target.writes.is_empty());
}

#[tokio::test]
async fn renumber_closes_holes_and_keeps_everything_else_in_order() {
    let written: Vec<Record> = [0, 1, 3, 4, 7, 8, 9].into_iter().map(rec).collect();
    let backup = Backup::write(&written, 2).await;
    let mut target = MockSink::starting_at(0);
    let report = backup
        .restore(0, OffsetMode::Renumber, &mut target)
        .await
        .unwrap();
    assert_eq!(report.records, 7);
    assert_eq!(report.high_watermark, 7);
    let expected: Vec<Record> = written
        .iter()
        .enumerate()
        .map(|(i, r)| Record {
            source_offset: i as u64,
            ..r.clone()
        })
        .collect();
    assert_eq!(target.writes, expected);
}

#[tokio::test]
async fn a_chain_that_starts_after_zero_renumbers_but_cannot_be_preserved() {
    let written: Vec<Record> = (0..10).map(rec).collect();
    let dir = tempfile::tempdir().unwrap();
    write_backup(dir.path(), &written, 5, None).await;
    // Objects before offset 5 removed on purpose.
    std::fs::remove_file(
        dir.path()
            .join(mirror_fs::naming::blob_filename(0, 4, None, "parquet")),
    )
    .unwrap();
    let backup = Backup {
        store: FsStore::new(dir.path().to_path_buf()),
        _dir: dir,
        source: source(),
    };
    let mut target = MockSink::starting_at(0);
    let err = backup
        .restore(5, OffsetMode::Preserve, &mut target)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("the backup starts at offset 5"),
        "{err}"
    );
    assert!(target.writes.is_empty());

    let report = backup
        .restore(5, OffsetMode::Renumber, &mut target)
        .await
        .unwrap();
    assert_eq!(report.records, 5);
    let values: Vec<_> = target.writes.iter().map(|r| r.value.clone()).collect();
    let originals: Vec<_> = written[5..].iter().map(|r| r.value.clone()).collect();
    assert_eq!(values, originals);
}

#[tokio::test]
async fn a_target_that_is_not_empty_is_refused() {
    let backup = Backup::write(&[rec(0), rec(1)], 5).await;
    for mode in [OffsetMode::Preserve, OffsetMode::Renumber] {
        let mut target = MockSink::starting_at(3);
        let err = backup.restore(0, mode, &mut target).await.unwrap_err();
        assert!(matches!(err, RestoreError::TargetNotEmpty(3)), "{err}");
        assert!(target.writes.is_empty());
    }
}

#[tokio::test]
async fn a_produce_the_target_refuses_ends_the_restore() {
    let backup = Backup::write(&(0..4).map(rec).collect::<Vec<_>>(), 5).await;
    // Another writer moved the topic: the gate refuses.
    let mut target = MockSink::starting_at(0).with_write_error(SinkError::UnexpectedPosition {
        expected: 0,
        actual: 1,
    });
    let err = backup
        .restore(0, OffsetMode::Preserve, &mut target)
        .await
        .unwrap_err();
    assert!(matches!(err, RestoreError::Target(_)), "{err}");
    assert!(err.to_string().contains("producing offset 0"), "{err}");
    assert!(target.writes.is_empty());
}

#[tokio::test]
async fn a_high_watermark_that_does_not_match_at_the_end_is_an_error() {
    let backup = Backup::write(&(0..4).map(rec).collect::<Vec<_>>(), 5).await;
    // Empty at the start; 5 at the end, after 4 records.
    let mut target = MockSink::starting_at(0).with_position_program([0, 5]);
    let err = backup
        .restore(0, OffsetMode::Preserve, &mut target)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("produced 4 of the backup's 4 records, and the target's high watermark is 5"),
        "{err}"
    );
}
