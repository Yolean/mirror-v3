//! The retention lock a destination with `lock-days` gives each new
//! object: COMPLIANCE, set in the PUT that creates it, until its record
//! time plus `lock-days`. A bucket's default retention counts from the
//! write time instead, which keeps an object written late with old
//! records (a backfill, a mirror catching up) a full period past the
//! time trim is due to delete it.

use mirror_fs::blob::{NewObject, RecordTimes, FUTURE_SKEW_MS};

/// The least time a lock is set for: a retain-until closer to the write
/// than this is moved out to it, so that a store whose clock runs ahead
/// does not refuse it as past. It holds an object at most this much
/// longer, and never releases one early.
pub const LOCK_LEAD_SECS: u64 = 3600;

const DAY_MS: u64 = 86_400_000;

/// The retain-until time (unix seconds) of a new object on a destination
/// with `lock_days`, or `None` for no lock:
///
/// - an append-mode object counts from its newest record timestamp, or
///   from its write time when that cannot be trusted (a record without a
///   timestamp, or the newest more than [`FUTURE_SKEW_MS`] ahead of the
///   write: trim blocks at such an object too);
/// - a floor marker counts from its write time;
/// - no lock when the retain-until is already past at the write (records
///   older than `lock_days`), for an object with no records, and for a
///   compaction snapshot.
pub fn retain_until(object: &NewObject, lock_days: u32) -> Option<u64> {
    let (from_ms, written_at) = match *object {
        NewObject::Records { times, written_at } => {
            (record_time_ms(&times, written_at)?, written_at)
        }
        NewObject::Marker { written_at } => (written_at.saturating_mul(1000), written_at),
        NewObject::Snapshot => return None,
    };
    let until_ms = from_ms.saturating_add(u64::from(lock_days).saturating_mul(DAY_MS));
    let until = until_ms.div_ceil(1000);
    if until <= written_at {
        return None;
    }
    Some(until.max(written_at.saturating_add(LOCK_LEAD_SECS)))
}

/// What a lock counts from: the newest record timestamp, or the write
/// time when the record times cannot be trusted. `None` for no records.
fn record_time_ms(times: &RecordTimes, written_at: u64) -> Option<u64> {
    let now_ms = written_at.saturating_mul(1000);
    match (times.max_ms, times.without) {
        (None, 0) => None,
        (Some(max), 0) if max <= now_ms.saturating_add(FUTURE_SKEW_MS) => Some(max),
        _ => Some(now_ms),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_800_000_000;
    const DAY: u64 = 86_400;

    fn records(max_ms: Option<u64>, without: u64) -> NewObject {
        NewObject::Records {
            times: RecordTimes {
                min_ms: max_ms,
                max_ms,
                without,
            },
            written_at: NOW,
        }
    }

    #[test]
    fn counts_from_the_newest_record() {
        let ten_days_ago_ms = (NOW - 10 * DAY) * 1000 + 1;
        assert_eq!(
            retain_until(&records(Some(ten_days_ago_ms), 0), 396),
            Some(NOW - 10 * DAY + 396 * DAY + 1),
            "rounded up to the second"
        );
    }

    #[test]
    fn counts_from_the_write_when_record_times_cannot_be_trusted() {
        let write_lock = Some(NOW + 396 * DAY);
        assert_eq!(retain_until(&records(Some(NOW * 1000), 1), 396), write_lock);
        assert_eq!(retain_until(&records(None, 3), 396), write_lock);
        let future = (NOW + 3601) * 1000;
        assert_eq!(retain_until(&records(Some(future), 0), 396), write_lock);
        let skew = (NOW + 3600) * 1000;
        assert_eq!(
            retain_until(&records(Some(skew), 0), 396),
            Some(NOW + 3600 + 396 * DAY)
        );
    }

    #[test]
    fn a_backfill_older_than_the_lock_is_not_locked() {
        let old = (NOW - 400 * DAY) * 1000;
        assert_eq!(retain_until(&records(Some(old), 0), 396), None);
        let exactly = (NOW - 396 * DAY) * 1000;
        assert_eq!(retain_until(&records(Some(exactly), 0), 396), None);
    }

    #[test]
    fn a_lock_about_to_lapse_is_held_for_the_lead() {
        let almost = (NOW - 396 * DAY) * 1000 + 1000;
        assert_eq!(
            retain_until(&records(Some(almost), 0), 396),
            Some(NOW + LOCK_LEAD_SECS)
        );
    }

    #[test]
    fn markers_count_from_their_write_and_snapshots_are_not_locked() {
        assert_eq!(
            retain_until(&NewObject::Marker { written_at: NOW }, 396),
            Some(NOW + 396 * DAY)
        );
        assert_eq!(retain_until(&NewObject::Snapshot, 396), None);
        assert_eq!(retain_until(&records(None, 0), 396), None, "no records");
    }
}
