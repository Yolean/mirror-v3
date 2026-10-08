//! Write a verified backup to the target, in order.
//!
//! The target is a [`RestoreTarget`]: in production
//! `mirror_kafka::RestoreProducer`, which keeps many records in
//! flight and requires each to land at the offset it was sent for.
//! Restore adds the checks around it: the target must be empty before
//! the first record, and its high watermark must equal the records
//! produced after the last. A restore that fails part way is not
//! resumed: the topic is deleted, created again, and restored again.

use mirror_core::RestoreTarget;
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
    target: &mut dyn RestoreTarget,
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
        .high_watermark()
        .await
        .map_err(|e| RestoreError::Target(format!("reading its high watermark: {e}")))?;
    if start != 0 {
        return Err(RestoreError::TargetNotEmpty(start));
    }
    let mut next = 0u64;
    if let Err(e) = send_all(reader, chain, summary, mode, target, &mut next).await {
        if next == 0 {
            // Nothing was sent: the target is as it was.
            return Err(e);
        }
        // Whatever failed, records are in flight: wait for them, so that
        // the error is the last thing that happens to the target.
        let drained = match target.finish().await {
            Ok(()) => String::new(),
            Err(d) => format!(" (and waiting for the records in flight: {d})"),
        };
        return Err(RestoreError::Produce(format!(
            "after sending {next} records: {e}{drained}"
        )));
    }
    target
        .finish()
        .await
        .map_err(|e| RestoreError::Produce(format!("target: waiting for the last records: {e}")))?;
    let high_watermark = target
        .high_watermark()
        .await
        .map_err(|e| RestoreError::Produce(format!("target: reading its high watermark: {e}")))?;
    if high_watermark != next || next != summary.records {
        return Err(RestoreError::Produce(format!(
            "target: produced {next} of the backup's {} records, and its high watermark is \
             {high_watermark}",
            summary.records
        )));
    }
    Ok(RestoreReport {
        records: next,
        high_watermark,
    })
}

/// Send every record of `chain` in order, counting in `next`.
async fn send_all<S: BlobStore>(
    reader: &Reader<'_, S>,
    chain: &[ChainObject],
    summary: &BackupSummary,
    mode: OffsetMode,
    target: &mut dyn RestoreTarget,
    next: &mut u64,
) -> Result<(), RestoreError> {
    for (object, verified) in chain.iter().zip(&summary.objects) {
        let (records, digest) = reader.read_object_with_digest(object).await?;
        if digest != verified.digest {
            return Err(RestoreError::Object(format!(
                "{} changed after the verify pass read it",
                reader.store.location(&object.name)
            )));
        }
        for mut record in records {
            match mode {
                OffsetMode::Preserve => {
                    if record.source_offset != *next {
                        return Err(RestoreError::Mode(format!(
                            "--offsets=preserve: offset {next} is missing (next record is at {})",
                            record.source_offset
                        )));
                    }
                }
                OffsetMode::Renumber => record.source_offset = *next,
            }
            // The error names its own offset: a delivery that failed is
            // reported for a record sent earlier.
            target
                .send(&record, *next)
                .await
                .map_err(RestoreError::Target)?;
            *next += 1;
        }
        tracing::info!(
            object = %reader.store.location(&object.name),
            records = verified.records,
            target_next = *next,
            "sent"
        );
    }
    Ok(())
}
