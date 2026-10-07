//! The produce pass against a mock target: it records what is sent,
//! reports a high watermark of what it holds, and fails where told to.

mod common;

use async_trait::async_trait;
use common::*;
use mirror_core::Record;
use mirror_envelope::{Format, Keyring};
use mirror_restore::{
    plan_chain, produce, BackupSource, BackupSummary, ChainObject, OffsetMode, Reader,
    RestoreError, RestoreReport, RestoreTarget,
};

/// A partition that holds `start` records before the restore.
#[derive(Default)]
struct MockTarget {
    start: u64,
    /// Every high watermark read after the first reports this instead
    /// of what the target holds.
    later_watermark: Option<u64>,
    watermark_reads: u32,
    sent: Vec<(u64, Record)>,
    refuse_offset: Option<u64>,
    finish_error: Option<String>,
}

impl MockTarget {
    fn at(start: u64) -> Self {
        Self {
            start,
            ..Self::default()
        }
    }

    fn records(&self) -> Vec<Record> {
        self.sent.iter().map(|(_, r)| r.clone()).collect()
    }
}

#[async_trait]
impl RestoreTarget for MockTarget {
    async fn high_watermark(&mut self) -> Result<u64, String> {
        self.watermark_reads += 1;
        match self.later_watermark {
            Some(w) if self.watermark_reads > 1 => Ok(w),
            _ => Ok(self.start + self.sent.len() as u64),
        }
    }

    async fn send(&mut self, record: &Record, offset: u64) -> Result<(), String> {
        if self.refuse_offset == Some(offset) {
            return Err(format!(
                "sent for offset {offset}, the broker stored it at 7"
            ));
        }
        self.sent.push((offset, record.clone()));
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), String> {
        self.finish_error.clone().map_or(Ok(()), Err)
    }
}

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
        target: &mut MockTarget,
    ) -> Result<RestoreReport, RestoreError> {
        let (chain, summary) = self.verified(chain_start).await;
        produce(&self.reader(), &chain, &summary, mode, target).await
    }
}

#[tokio::test]
async fn preserve_produces_every_record_at_its_offset() {
    let written: Vec<Record> = (0..23).map(rec).collect();
    let backup = Backup::write(&written, 5).await;
    let mut target = MockTarget::at(0);
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
    assert_eq!(target.records(), written);
    let offsets: Vec<u64> = target.sent.iter().map(|(o, _)| *o).collect();
    assert_eq!(offsets, (0..23).collect::<Vec<u64>>());
}

#[tokio::test]
async fn preserve_refuses_a_backup_with_holes_before_producing_anything() {
    let written: Vec<Record> = [0, 1, 3, 4].into_iter().map(rec).collect();
    let backup = Backup::write(&written, 2).await;
    let mut target = MockTarget::at(0);
    let err = backup
        .restore(0, OffsetMode::Preserve, &mut target)
        .await
        .unwrap_err();
    assert!(matches!(err, RestoreError::Mode(_)), "{err}");
    assert!(err.to_string().contains("the first at offset 2"), "{err}");
    assert!(target.sent.is_empty());
}

#[tokio::test]
async fn renumber_closes_holes_and_keeps_everything_else_in_order() {
    let written: Vec<Record> = [0, 1, 3, 4, 7, 8, 9].into_iter().map(rec).collect();
    let backup = Backup::write(&written, 2).await;
    let mut target = MockTarget::at(0);
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
    assert_eq!(target.records(), expected);
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
    let mut target = MockTarget::at(0);
    let err = backup
        .restore(5, OffsetMode::Preserve, &mut target)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("the backup starts at offset 5"),
        "{err}"
    );
    assert!(target.sent.is_empty());

    let report = backup
        .restore(5, OffsetMode::Renumber, &mut target)
        .await
        .unwrap();
    assert_eq!(report.records, 5);
    let values: Vec<_> = target.sent.iter().map(|(_, r)| r.value.clone()).collect();
    let originals: Vec<_> = written[5..].iter().map(|r| r.value.clone()).collect();
    assert_eq!(values, originals);
}

#[tokio::test]
async fn a_target_that_is_not_empty_is_refused() {
    let backup = Backup::write(&[rec(0), rec(1)], 5).await;
    for mode in [OffsetMode::Preserve, OffsetMode::Renumber] {
        let mut target = MockTarget::at(3);
        let err = backup.restore(0, mode, &mut target).await.unwrap_err();
        assert!(matches!(err, RestoreError::TargetNotEmpty(3)), "{err}");
        assert!(target.sent.is_empty());
    }
}

#[tokio::test]
async fn a_produce_the_target_refuses_ends_the_restore() {
    let backup = Backup::write(&(0..4).map(rec).collect::<Vec<_>>(), 5).await;
    let mut target = MockTarget {
        refuse_offset: Some(2),
        ..MockTarget::at(0)
    };
    let err = backup
        .restore(0, OffsetMode::Preserve, &mut target)
        .await
        .unwrap_err();
    assert!(matches!(err, RestoreError::Target(_)), "{err}");
    assert!(err.to_string().contains("producing offset 2"), "{err}");
    assert_eq!(target.sent.len(), 2);
}

#[tokio::test]
async fn a_record_that_does_not_land_at_the_end_ends_the_restore() {
    let backup = Backup::write(&(0..4).map(rec).collect::<Vec<_>>(), 5).await;
    let mut target = MockTarget {
        finish_error: Some("offset 3 was not delivered: timed out".into()),
        ..MockTarget::at(0)
    };
    let err = backup
        .restore(0, OffsetMode::Preserve, &mut target)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("waiting for the last records: offset 3 was not delivered"),
        "{err}"
    );
}

#[tokio::test]
async fn a_high_watermark_that_does_not_match_at_the_end_is_an_error() {
    let backup = Backup::write(&(0..4).map(rec).collect::<Vec<_>>(), 5).await;
    // Empty at the start; 5 at the end, after 4 records.
    let mut target = MockTarget {
        later_watermark: Some(5),
        ..MockTarget::at(0)
    };
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

#[tokio::test]
async fn an_object_replaced_after_the_verify_pass_ends_the_restore() {
    let backup = Backup::write(&(0..10).map(rec).collect::<Vec<_>>(), 5).await;
    let (chain, summary) = backup.verified(0).await;
    // The same offsets, other values, under the same name.
    let other = tempfile::tempdir().unwrap();
    let replaced: Vec<Record> = (0..10)
        .map(|o| Record {
            value: Some(b"replaced".to_vec()),
            ..rec(o)
        })
        .collect();
    write_backup(other.path(), &replaced, 5, None).await;
    let name = mirror_fs::naming::blob_filename(5, 9, None, "parquet");
    std::fs::copy(other.path().join(&name), backup._dir.path().join(&name)).unwrap();
    let mut target = MockTarget::at(0);
    let err = produce(
        &backup.reader(),
        &chain,
        &summary,
        OffsetMode::Preserve,
        &mut target,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("changed after the verify pass read it"),
        "{err}"
    );
    assert!(err.to_string().contains(&name), "{err}");
    assert_eq!(target.sent.len(), 5, "the first object was produced");
}
