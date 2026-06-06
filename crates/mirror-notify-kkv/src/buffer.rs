//! Debounce buffer for the `trigger.on: source-consume` notify mode.
//!
//! Accumulates `(key, source_offset)` per record handed to
//! [`crate::KkvV1Notifier::on_record`] and emits a single
//! batch-ready payload when either:
//!   * `max-records` records have been appended since the last
//!     drain, OR
//!   * `max-time-ms` has elapsed since the *first* record of the
//!     current batch landed.
//!
//! Per `WEBHOOKS.md § Interaction with compaction: log`, keys are
//! set-deduped within a batch (the kkv-v1 body's `updates` is a
//! key → null map; duplicates over the same window collapse). The
//! `offsets` field carries the **maximum** source offset across the
//! batch; the consumer's `requireOffset` constraint then pins the
//! follow-up `/cache/v1/raw/<key>` read to post-batch state.

use std::time::Instant;

use indexmap::{IndexMap, IndexSet};

/// Mutable buffer that on_record / the timer task share via a
/// `tokio::sync::Mutex`. Not directly exposed.
#[derive(Default, Debug)]
pub(crate) struct Buffer {
    /// Distinct keys in insertion order. `IndexSet` over `HashSet`
    /// keeps the on-wire JSON deterministic, which matters for
    /// integration-test assertions.
    keys: IndexSet<String>,
    /// Highest source offset across the batch.
    max_offset: u64,
    /// Number of records appended since the last drain. The
    /// `max-records` trigger fires on *record count*, not on dedup-
    /// bucket cardinality; otherwise a hot key getting repeated
    /// hits could stall the trigger and grow the buffer's wall-clock
    /// age indefinitely.
    seen_records: u64,
    /// When the first record landed in the currently-open batch.
    /// Drives the `max-time-ms` drain check; reset on every drain.
    first_at: Option<Instant>,
}

impl Buffer {
    pub fn append(&mut self, key: String, source_offset: u64) {
        if self.first_at.is_none() {
            self.first_at = Some(Instant::now());
        }
        self.keys.insert(key);
        // `max_offset` only goes up. The consumer's `requireOffset`
        // semantics require us to report the highest offset the
        // batch carries; out-of-order arrivals are possible if the
        // source ever fans across partitions (not today, but the
        // safety net is free).
        if self.seen_records == 0 || source_offset > self.max_offset {
            self.max_offset = source_offset;
        }
        self.seen_records = self.seen_records.saturating_add(1);
    }

    pub fn seen_records(&self) -> u64 {
        self.seen_records
    }

    pub fn is_empty(&self) -> bool {
        self.seen_records == 0
    }

    pub fn first_at(&self) -> Option<Instant> {
        self.first_at
    }

    /// Drain the buffer and return a payload-ready batch. Empty
    /// buffer returns `None`. After this call, the buffer is
    /// guaranteed empty.
    pub fn take(&mut self, partition: i32) -> Option<DrainedBatch> {
        if self.is_empty() {
            return None;
        }
        let mut offsets = IndexMap::with_capacity(1);
        offsets.insert(partition.to_string(), self.max_offset);
        let updates: IndexMap<String, serde_json::Value> = self
            .keys
            .drain(..)
            .map(|k| (k, serde_json::Value::Null))
            .collect();
        self.max_offset = 0;
        self.seen_records = 0;
        self.first_at = None;
        Some(DrainedBatch { offsets, updates })
    }
}

/// Owned payload-ready batch handed off to the dispatcher.
#[derive(Debug)]
pub(crate) struct DrainedBatch {
    pub offsets: IndexMap<String, u64>,
    pub updates: IndexMap<String, serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_take_returns_none() {
        let mut b = Buffer::default();
        assert!(b.take(0).is_none());
    }

    #[test]
    fn append_then_take_carries_keys_and_max_offset() {
        let mut b = Buffer::default();
        b.append("a".into(), 10);
        b.append("b".into(), 11);
        b.append("c".into(), 12);
        let batch = b.take(3).unwrap();
        assert_eq!(batch.offsets.get("3"), Some(&12));
        assert_eq!(batch.updates.len(), 3);
        assert!(b.is_empty(), "take must reset");
    }

    #[test]
    fn duplicate_keys_collapse_but_record_count_still_climbs() {
        let mut b = Buffer::default();
        b.append("hot".into(), 1);
        b.append("hot".into(), 2);
        b.append("hot".into(), 3);
        assert_eq!(b.seen_records(), 3, "max-records must count appends");
        let batch = b.take(0).unwrap();
        assert_eq!(batch.updates.len(), 1, "key set must dedup");
        assert_eq!(batch.offsets["0"], 3, "max offset must be 3");
    }

    #[test]
    fn out_of_order_offsets_still_report_max() {
        let mut b = Buffer::default();
        b.append("a".into(), 5);
        b.append("b".into(), 9);
        b.append("c".into(), 7);
        let batch = b.take(0).unwrap();
        assert_eq!(batch.offsets["0"], 9);
    }

    #[test]
    fn first_at_is_set_on_first_append_and_cleared_on_drain() {
        let mut b = Buffer::default();
        assert!(b.first_at().is_none());
        b.append("a".into(), 1);
        let t = b.first_at().expect("first append sets the timer");
        b.append("b".into(), 2);
        assert_eq!(
            b.first_at(),
            Some(t),
            "later appends must NOT shift first_at; the debounce window measures from the first record"
        );
        b.take(0);
        assert!(b.first_at().is_none(), "drain resets first_at");
    }
}
