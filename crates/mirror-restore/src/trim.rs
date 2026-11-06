//! Expire the head of an append-mode blob backup: move its floor (see
//! [`mirror_fs::floor`]) and delete the objects below it.
//!
//! The floor moves from object names and object metadata alone: an
//! object's record times are a HEAD away ([`BlobStore::record_times`]),
//! so the identity that trims needs no object content and no Parquet key.
//! An object expires when every record in it, and in every object before
//! it, is older than the cutoff: the walk from the floor stops at the
//! first object that holds a newer record, and at one whose age it
//! cannot tell (no metadata, a record without a timestamp, a timestamp in
//! the future), which is a stuck expiry that a person has to look at.
//!
//! The marker is written before anything is deleted, so a trim that
//! stops half way leaves a chain that validates. Objects below the floor
//! that fail to delete (a retention lock) are retried by the next trim.
//! Old markers stay: they are the store's record of each expiry.

use futures::StreamExt;
use mirror_envelope::Format;
use mirror_fs::blob::{BlobError, BlobStore, RecordTimes};
use mirror_fs::floor::{self, Floor, FloorMarker};
use mirror_fs::naming;

use crate::{plan_floor_chain, ChainObject, RestoreError};

/// How far ahead of the trim's clock a record timestamp may be before
/// trim takes it for a wrong clock rather than skew.
pub const FUTURE_SKEW_MS: u64 = 3_600_000;

/// HEADs and DELETEs in flight at once.
const CONCURRENCY: usize = 16;

/// Where the floor goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloorTarget {
    /// Past every object whose records, and the records of every object
    /// before it, are older than this (unix milliseconds).
    Cutoff { cutoff_ms: u64 },
    /// To this offset: a head that was removed by other means (a bucket
    /// lifecycle rule), or objects written before they carried record
    /// times, adopted by a person.
    Offset(u64),
}

pub struct TrimConfig<'a, S> {
    pub store: &'a S,
    pub format: Format,
    pub target: FloorTarget,
    /// Unix seconds: the new marker's time, and what a record timestamp
    /// is compared with.
    pub now: u64,
    /// Plan only: write and delete nothing.
    pub dry_run: bool,
}

/// Where the walk from the floor stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stop {
    /// `FloorTarget::Offset`: no walk.
    Given,
    /// No object is left at or above the floor.
    End,
    /// This object holds a record newer than the cutoff.
    Newer { object: String },
    /// Trim cannot tell whether this object expired, and will not until
    /// a person looks (see [`FloorTarget::Offset`]).
    Blocked { object: String, reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrimReport {
    /// The floor before this trim.
    pub previous: Option<Floor>,
    /// The floor after it (the planned one with `dry_run`).
    pub floor: u64,
    /// The marker written (to be written, with `dry_run`).
    pub marker: Option<String>,
    /// The position after the chain.
    pub chain_end: u64,
    pub stop: Stop,
    /// The oldest record of the first object at or above the new floor,
    /// unix milliseconds, where its metadata tells.
    pub oldest_ms: Option<u64>,
    /// Objects deleted (to be deleted, with `dry_run`): every object that
    /// ends below the floor.
    pub deleted: Vec<String>,
    /// Objects below the floor that did not delete, and why.
    pub failed: Vec<(String, String)>,
}

impl TrimReport {
    /// `Some((object, reason))` when the expiry is stuck.
    pub fn blocked(&self) -> Option<(&str, &str)> {
        match &self.stop {
            Stop::Blocked { object, reason } => Some((object, reason)),
            _ => None,
        }
    }
}

/// What the record times of an object say about its expiry.
enum Verdict {
    Expired,
    Newer,
    Blocked(String),
}

fn verdict(times: Option<RecordTimes>, cutoff_ms: u64, now_ms: u64) -> Verdict {
    let Some(times) = times else {
        return Verdict::Blocked(
            "no record-time metadata (written by a mirror-v3 that did not record it): check \
             the age of its records, then move the floor past it with `trim --floor`"
                .into(),
        );
    };
    if times.without > 0 {
        return Verdict::Blocked(format!(
            "{} record(s) without a timestamp: their age is unknown",
            times.without
        ));
    }
    match times.max_ms {
        Some(max) if max > now_ms + FUTURE_SKEW_MS => Verdict::Blocked(format!(
            "a record timestamp in the future, {} (a producer's clock): it would keep this \
             object until then",
            floor::utc_rfc3339(max / 1000)
        )),
        Some(max) if max >= cutoff_ms => Verdict::Newer,
        // Older than the cutoff, or no records at all.
        _ => Verdict::Expired,
    }
}

pub async fn trim<S: BlobStore>(cfg: TrimConfig<'_, S>) -> Result<TrimReport, RestoreError> {
    let store = cfg.store;
    let names = store.list().await.map_err(store_error("listing"))?;
    let (previous, _) = floor::floor_of(&names).map_err(RestoreError::Chain)?;
    let previous_offset = previous.map_or(0, |f| f.offset);
    let now_ms = cfg.now.saturating_mul(1000);

    let (planned, floor_offset, stop, oldest_ms) = match cfg.target {
        FloorTarget::Offset(at) => {
            if at < previous_offset {
                return Err(RestoreError::Chain(format!(
                    "a floor lower than the current one, {previous_offset}, is refused: the \
                     floor only moves forward"
                )));
            }
            let ext = cfg.format.extension();
            let end = names
                .iter()
                .filter_map(|n| naming::parse_blob_name(n, ext))
                .map(|b| b.to + 1)
                .max()
                .unwrap_or(previous_offset)
                .max(previous_offset);
            if at > end {
                return Err(RestoreError::Chain(format!(
                    "a floor above the end of the chain, {end}, is refused: it would make the \
                     mirror skip offsets {end}-{}",
                    at - 1
                )));
            }
            let planned = plan_floor_chain(&names, cfg.format, Some(at))?;
            let oldest_ms = match planned.objects.first() {
                Some(o) => store
                    .record_times(&o.name)
                    .await
                    .map_err(store_error("reading record times"))?
                    .and_then(|t| t.min_ms),
                None => None,
            };
            (planned, at, Stop::Given, oldest_ms)
        }
        FloorTarget::Cutoff { cutoff_ms } => {
            let planned = plan_floor_chain(&names, cfg.format, None)?;
            let mut floor_offset = previous_offset;
            let mut stop = Stop::End;
            let mut oldest_ms = None;
            let mut heads = futures::stream::iter(planned.objects.iter())
                .map(|o| async move { (o, store.record_times(&o.name).await) })
                .buffered(CONCURRENCY);
            while let Some((o, times)) = heads.next().await {
                let times = times.map_err(store_error("reading record times"))?;
                match verdict(times, cutoff_ms, now_ms) {
                    Verdict::Expired => floor_offset = floor_offset.max(o.to + 1),
                    Verdict::Newer => {
                        oldest_ms = times.and_then(|t| t.min_ms);
                        stop = Stop::Newer {
                            object: o.name.clone(),
                        };
                        break;
                    }
                    Verdict::Blocked(reason) => {
                        oldest_ms = times.and_then(|t| t.min_ms);
                        stop = Stop::Blocked {
                            object: o.name.clone(),
                            reason,
                        };
                        break;
                    }
                }
            }
            drop(heads);
            (planned, floor_offset, stop, oldest_ms)
        }
    };
    let chain_end = planned.end();

    // A marker when the floor moves, on the first trim (so that the floor,
    // and the age of the oldest record above it, show from then on), and
    // when the oldest record above an unchanged floor becomes known.
    let write_marker = floor_offset > previous_offset
        || previous.is_none()
        || (previous.is_some_and(|p| p.oldest_ms.is_none()) && oldest_ms.is_some());
    let marker = write_marker.then_some(FloorMarker {
        offset: floor_offset,
        at: cfg.now,
        oldest_ms,
    });
    if let (Some(m), false) = (marker, cfg.dry_run) {
        let content = marker_content(&m, previous, chain_end, cfg.target, &stop);
        let name = m.filename();
        store
            .put_new(&name, content.into_bytes(), &[])
            .await
            .map_err(|e| match e {
                BlobError::AlreadyExists(_) => RestoreError::Store(format!(
                    "{} already exists: another trim wrote it",
                    store.location(&name)
                )),
                e => RestoreError::Store(format!("writing {}: {e}", store.location(&name))),
            })?;
        tracing::info!(
            marker = %store.location(&name),
            previous = previous_offset,
            floor = floor_offset,
            chain_end,
            "wrote the chain's floor"
        );
    }

    let expired = planned
        .objects
        .iter()
        .filter(|o| o.to < floor_offset)
        .map(|o: &ChainObject| o.name.clone());
    let deleted: Vec<String> = planned.below_floor.iter().cloned().chain(expired).collect();
    let mut failed = Vec::new();
    if !cfg.dry_run {
        let mut deletes = futures::stream::iter(deleted.iter())
            .map(|name| async move { (name, store.delete(name).await) })
            .buffer_unordered(CONCURRENCY);
        while let Some((name, result)) = deletes.next().await {
            if let Err(e) = result {
                tracing::warn!(
                    object = %store.location(name),
                    error = %e,
                    "an object below the floor did not delete; the next trim tries again"
                );
                failed.push((name.clone(), e.to_string()));
            }
        }
        failed.sort();
    }
    let deleted = deleted
        .into_iter()
        .filter(|n| !failed.iter().any(|(f, _)| f == n))
        .collect();
    Ok(TrimReport {
        previous,
        floor: floor_offset,
        marker: marker.map(|m| m.filename()),
        chain_end,
        stop,
        oldest_ms,
        deleted,
        failed,
    })
}

fn store_error(what: &'static str) -> impl Fn(BlobError) -> RestoreError {
    move |e| RestoreError::Store(format!("{what}: {e}"))
}

/// The marker's content: how its floor was computed, for a person
/// reading the store. Nothing parses it; the name is what counts.
fn marker_content(
    marker: &FloorMarker,
    previous: Option<Floor>,
    chain_end: u64,
    target: FloorTarget,
    stop: &Stop,
) -> String {
    let rfc_ms = |ms: u64| floor::utc_rfc3339(ms / 1000);
    let (cutoff, set_by) = match target {
        FloorTarget::Cutoff { cutoff_ms } => (Some(rfc_ms(cutoff_ms)), "retention"),
        FloorTarget::Offset(_) => (None, "trim --floor"),
    };
    let stop = match stop {
        Stop::Given => serde_json::Value::Null,
        Stop::End => "no object is left at or above the floor".into(),
        Stop::Newer { object } => format!("{object} holds records newer than the cutoff").into(),
        Stop::Blocked { object, reason } => format!("blocked at {object}: {reason}").into(),
    };
    let content = serde_json::json!({
        "floor": marker.offset,
        "previous_floor": previous.map(|p| p.offset),
        "moved_at": floor::utc_rfc3339(marker.at),
        "set_by": set_by,
        "cutoff": cutoff,
        "chain_end": chain_end,
        "oldest_record_above": marker.oldest_ms.map(rfc_ms),
        "stopped": stop,
    });
    format!(
        "{}\n",
        serde_json::to_string_pretty(&content).expect("a JSON value serializes")
    )
}
