//! Write a verified backup to the target, record by record, in order.
//!
//! The target is a [`Sink`]: in production the Kafka destination, whose
//! `write` reads the topic's high watermark before every produce and
//! requires it to equal the record's offset, produces with zero retries
//! and requires the broker to report that offset back. Restore adds
//! the checks around it: the target must be empty before the first
//! record, and its high watermark must equal the records produced
//! after the last.

use mirror_core::Sink;
use mirror_fs::blob::BlobStore;

use crate::{BackupSummary, ChainObject, Reader, RestoreError};

/// Which offsets the records get in the target. There is no default:
/// the choice depends on what reads the topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetMode {
    /// Every record at its original offset. A Kafka topic starts at 0
    /// and cannot be written with holes, so the backup must start at 0
    /// and have none.
    Preserve,
    /// Records at 0, 1, 2, ... in their original order: the source's
    /// holes close up, and every offset after the first hole changes.
    Renumber,
}

impl OffsetMode {
    /// Whether a verified backup can be restored with these offsets.
    pub fn check(self, summary: &BackupSummary) -> Result<(), RestoreError> {
        match self {
            OffsetMode::Renumber => Ok(()),
            OffsetMode::Preserve => {
                if summary.first_offset != 0 {
                    return Err(RestoreError::Mode(format!(
                        "--offsets=preserve: the backup starts at offset {}, and a Kafka topic \
                         starts at 0",
                        summary.first_offset
                    )));
                }
                match summary.first_hole {
                    None => Ok(()),
                    Some(hole) => Err(RestoreError::Mode(format!(
                        "--offsets=preserve: the backup has {} offset hole(s), the first at \
                         offset {hole} (records removed by compaction, or transaction markers), \
                         and a Kafka topic cannot be written with holes",
                        summary.holes
                    ))),
                }
            }
        }
    }
}

/// What a restore produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreReport {
    pub records: u64,
    /// The target's high watermark after the last record.
    pub high_watermark: u64,
}

/// Produce every record of `chain` (verified as `summary`) to `target`.
pub async fn produce<S: BlobStore>(
    reader: &Reader<'_, S>,
    chain: &[ChainObject],
    summary: &BackupSummary,
    mode: OffsetMode,
    target: &mut dyn Sink,
) -> Result<RestoreReport, RestoreError> {
    mode.check(summary)?;
    if chain.len() != summary.objects.len() {
        return Err(RestoreError::Chain(format!(
            "{} objects to restore, and the verify pass read {}",
            chain.len(),
            summary.objects.len()
        )));
    }
    let start = target
        .next_expected_offset()
        .await
        .map_err(|e| RestoreError::Target(format!("reading its high watermark: {e}")))?;
    if start != 0 {
        return Err(RestoreError::TargetNotEmpty(start));
    }
    let mut next = 0u64;
    for (object, verified) in chain.iter().zip(&summary.objects) {
        let records = reader.read_object(object).await?;
        if records.len() as u64 != verified.records {
            return Err(RestoreError::Object(format!(
                "{} holds {} records, and the verify pass read {}",
                reader.store.location(&object.name),
                records.len(),
                verified.records
            )));
        }
        for mut record in records {
            match mode {
                OffsetMode::Preserve => {
                    if record.source_offset != next {
                        return Err(RestoreError::Mode(format!(
                            "--offsets=preserve: offset {next} is missing (next record is at {})",
                            record.source_offset
                        )));
                    }
                }
                OffsetMode::Renumber => record.source_offset = next,
            }
            target
                .write(record)
                .await
                .map_err(|e| RestoreError::Target(format!("producing offset {next}: {e}")))?;
            next += 1;
        }
        tracing::info!(
            object = %reader.store.location(&object.name),
            records = verified.records,
            target_next = next,
            "restored"
        );
    }
    let high_watermark = target
        .next_expected_offset()
        .await
        .map_err(|e| RestoreError::Target(format!("reading its high watermark: {e}")))?;
    if high_watermark != next || next != summary.records {
        return Err(RestoreError::Target(format!(
            "produced {next} of the backup's {} records, and the target's high watermark is \
             {high_watermark}",
            summary.records
        )));
    }
    Ok(RestoreReport {
        records: next,
        high_watermark,
    })
}
