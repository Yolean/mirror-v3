//! Read a validated chain back: every object decoded (with the key its
//! name carries) and checked against its name and the backup's source.
//!
//! An object `<from>-<to>` holds the records of consumer positions
//! `from..=to` that the source still had, in offset order, the last one
//! at `to`. The positions without a record are the source's offset
//! holes (compaction, transaction markers): restore counts them, and
//! `--offsets=preserve` refuses a backup that has any.

use mirror_core::Record;
use mirror_envelope::{Format, Keyring};
use mirror_fs::blob::{decode_blob, BlobStore};

use crate::{ChainObject, RestoreError};

/// What a backup is a backup of: every record must carry this source
/// topic and partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupSource {
    pub topic: String,
    pub partition: i32,
}

/// How to read a backup's objects.
pub struct Reader<'a, S> {
    pub store: &'a S,
    pub format: Format,
    /// `None` for a source with `encryption: none`.
    pub keyring: Option<&'a Keyring>,
    pub source: &'a BackupSource,
}

/// One object, as read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectSummary {
    pub name: String,
    pub records: u64,
    /// Positions of the object's range without a record.
    pub holes: u64,
    pub first_hole: Option<u64>,
}

/// The verify pass's result: the "is the backup complete" answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupSummary {
    pub objects: Vec<ObjectSummary>,
    /// The chain's first position (`from` of its first object).
    pub first_offset: u64,
    /// The chain's last position (`to` of its last object): the offset
    /// of the last record backed up.
    pub last_offset: u64,
    pub records: u64,
    pub holes: u64,
    pub first_hole: Option<u64>,
}

impl<S: BlobStore> Reader<'_, S> {
    /// Read one object and check it: records in strictly increasing
    /// offset order inside `from..=to`, the last at `to`, every one of
    /// the backup's source.
    pub async fn read_object(&self, object: &ChainObject) -> Result<Vec<Record>, RestoreError> {
        let location = self.store.location(&object.name);
        let bytes = self
            .store
            .get(&object.name)
            .await
            .map_err(|e| RestoreError::Store(format!("read {location}: {e}")))?;
        let records = decode_blob(&object.name, &location, &bytes, self.format, self.keyring)
            .map_err(|e| RestoreError::Object(e.to_string()))?;
        let bad = |what: String| RestoreError::Object(format!("{location}: {what}"));
        let Some(last) = records.last() else {
            return Err(bad("holds no records".into()));
        };
        if last.source_offset != object.to {
            return Err(bad(format!(
                "its last record has offset {}, and its name says {}",
                last.source_offset, object.to
            )));
        }
        let mut previous: Option<u64> = None;
        for r in &records {
            if r.source_offset < object.from {
                return Err(bad(format!(
                    "record offset {} is below the object's range {}-{}",
                    r.source_offset, object.from, object.to
                )));
            }
            if previous.is_some_and(|p| r.source_offset <= p) {
                return Err(bad(format!(
                    "record offsets are not increasing: {} after {}",
                    r.source_offset,
                    previous.expect("checked is_some")
                )));
            }
            if r.topic != self.source.topic || r.partition != self.source.partition {
                return Err(bad(format!(
                    "record at offset {} is from {}/{}, and this backup is of {}/{}",
                    r.source_offset, r.topic, r.partition, self.source.topic, self.source.partition
                )));
            }
            previous = Some(r.source_offset);
        }
        Ok(records)
    }

    /// The verify pass: read every object of `chain` and sum it up.
    pub async fn verify(&self, chain: &[ChainObject]) -> Result<BackupSummary, RestoreError> {
        let (Some(first), Some(last)) = (chain.first(), chain.last()) else {
            return Err(RestoreError::Chain("the chain has no objects".into()));
        };
        let mut objects = Vec::with_capacity(chain.len());
        for object in chain {
            let records = self.read_object(object).await?;
            let summary = summarize(object, &records);
            tracing::debug!(
                object = %summary.name,
                records = summary.records,
                holes = summary.holes,
                "verified"
            );
            objects.push(summary);
        }
        Ok(BackupSummary {
            first_offset: first.from,
            last_offset: last.to,
            records: objects.iter().map(|o| o.records).sum(),
            holes: objects.iter().map(|o| o.holes).sum(),
            first_hole: objects.iter().find_map(|o| o.first_hole),
            objects,
        })
    }
}

fn summarize(object: &ChainObject, records: &[Record]) -> ObjectSummary {
    let positions = object.to - object.from + 1;
    let mut next = object.from;
    let mut first_hole = None;
    for r in records {
        if r.source_offset > next && first_hole.is_none() {
            first_hole = Some(next);
        }
        next = r.source_offset + 1;
    }
    ObjectSummary {
        name: object.name.clone(),
        records: records.len() as u64,
        holes: positions - records.len() as u64,
        first_hole,
    }
}
