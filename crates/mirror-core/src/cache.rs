//! Shared in-memory cache view for `http-access: { api: cache-v1 }`
//! mirrors.
//!
//! mirror-v3's KKV-compatibility mode keeps a merged `key → latest
//! value` map of every record consumed by every opt-in mirror. This
//! module owns the cross-task state behind an `Arc<CacheState>`: the
//! sinks update it from the consume loop (per-record, *not* per-flush
//! — freshness is independent of bucket-write cadence), and the HTTP
//! handlers in `mirror-cache` read from it.
//!
//! ## Monotonicity
//!
//! `apply_record` only advances the per-partition offset forward. If
//! a future feature ever rewinds source consumption without restart,
//! the cache view stays at the highest offset it has seen — KKV
//! semantics (dependents must not transiently observe an older
//! state on reload).
//!
//! ## Readiness
//!
//! Each participating mirror declares a `bootstrap_hwm` at sink
//! open (`fetch_high_watermark` against the source partition). Once a
//! mirror's last-applied offset has caught up to its bootstrap
//! watermark, it is "ready"; once *every* registered mirror is
//! ready, [`CacheState::is_ready`] flips to `true` and stays true.
//! HTTP handlers gate on this; they return 503 until it flips.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;

use crate::Record;

/// Per-partition identity used as the offset-map key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: u32,
}

/// `{topic, partition, offset}` triple as exposed in the
/// `x-kkv-last-seen-offsets` header. Mirrors KKV's `TopicPartitionOffset`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartitionOffset {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
}

/// Per-mirror readiness slot. The supervisor (mirror-bin) creates
/// one per opt-in mirror at startup, populates `bootstrap_hwm`, and
/// stores the slot in [`CacheState`]. The sink's per-record path
/// flips the slot to `caught_up` once its last-seen offset has
/// crossed `bootstrap_hwm`.
#[derive(Debug)]
struct MirrorReadiness {
    bootstrap_hwm: u64,
    caught_up: AtomicBool,
}

#[derive(Debug, Default)]
pub struct CacheState {
    /// Merged key → latest-value across every opt-in mirror.
    view: RwLock<BTreeMap<String, Vec<u8>>>,
    /// Last-seen source offset per (topic, partition). Monotonic.
    offsets: RwLock<HashMap<TopicPartition, u64>>,
    /// Per-mirror readiness slots, keyed by the mirror's
    /// configuration name (unique per process).
    mirrors: RwLock<HashMap<String, MirrorReadiness>>,
    /// Sticky global ready flag. Flips to `true` once every
    /// registered mirror has caught up; never flips back.
    ready: AtomicBool,
}

impl CacheState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an opt-in mirror with its source-partition high
    /// watermark captured at startup. Must be called once per mirror
    /// before any `apply_record` for that mirror runs.
    ///
    /// `bootstrap_hwm` is the Kafka high-watermark (one past the last
    /// existing offset). An empty topic has `bootstrap_hwm = 0` and
    /// the mirror is immediately considered caught up.
    pub fn register_mirror(&self, mirror_name: &str, bootstrap_hwm: u64) {
        let caught_up = bootstrap_hwm == 0;
        {
            let mut m = self.mirrors.write().expect("cache mirrors poisoned");
            m.insert(
                mirror_name.to_string(),
                MirrorReadiness {
                    bootstrap_hwm,
                    caught_up: AtomicBool::new(caught_up),
                },
            );
        }
        if caught_up {
            self.recheck_ready();
        }
    }

    /// Apply a record from the source consume loop to the in-memory
    /// view and offset map. The supervisor passes `mirror_name` so we
    /// can flip the mirror's readiness slot once the bootstrap
    /// watermark is reached.
    ///
    /// Monotonic: if `record.source_offset` is not strictly greater
    /// than the partition's last-applied offset (rewind / replay),
    /// this is a no-op for both the view and the offset map.
    pub fn apply_record(&self, mirror_name: &str, record: &Record) {
        let tp = TopicPartition {
            topic: record.topic.clone(),
            partition: record.partition as u32,
        };
        {
            let mut offsets = self.offsets.write().expect("cache offsets poisoned");
            if let Some(&last) = offsets.get(&tp) {
                if record.source_offset <= last {
                    return; // monotonic guard — never rewind the cache
                }
            }
            offsets.insert(tp, record.source_offset);
        }
        // Drop the offsets lock before touching the view lock.
        // Order is consistent across all paths to avoid deadlocks.
        let key = match record
            .key
            .as_ref()
            .and_then(|k| std::str::from_utf8(k).ok())
        {
            Some(k) => k.to_string(),
            // No key, or non-UTF-8 — by validation the cache only
            // sees UTF-8 keys, so this branch is unreachable in
            // production. Skip silently rather than panicking.
            None => return,
        };
        let mut view = self.view.write().expect("cache view poisoned");
        match record.value.as_ref() {
            Some(v) => {
                view.insert(key, v.clone());
            }
            None => {
                view.remove(&key);
            }
        }
        drop(view);
        // Readiness check after the view update so observers seeing
        // ready=true also see the record applied.
        if !self.ready.load(Ordering::Acquire) {
            self.maybe_flip_mirror_ready(mirror_name, record.source_offset);
        }
    }

    fn maybe_flip_mirror_ready(&self, mirror_name: &str, last_offset: u64) {
        let mut all_ready = true;
        let mirrors = self.mirrors.read().expect("cache mirrors poisoned");
        if let Some(slot) = mirrors.get(mirror_name) {
            // The slot can have been flipped to caught_up either by
            // register (empty topic) or by a previous record on the
            // same mirror. Either way: once the mirror's
            // last-applied offset hits `bootstrap_hwm - 1`, flip.
            if !slot.caught_up.load(Ordering::Acquire) && last_offset + 1 >= slot.bootstrap_hwm {
                slot.caught_up.store(true, Ordering::Release);
            }
        }
        for slot in mirrors.values() {
            if !slot.caught_up.load(Ordering::Acquire) {
                all_ready = false;
                break;
            }
        }
        drop(mirrors);
        if all_ready {
            self.ready.store(true, Ordering::Release);
        }
    }

    fn recheck_ready(&self) {
        let mirrors = self.mirrors.read().expect("cache mirrors poisoned");
        let all_ready = mirrors
            .values()
            .all(|s| s.caught_up.load(Ordering::Acquire));
        drop(mirrors);
        if all_ready {
            self.ready.store(true, Ordering::Release);
        }
    }

    /// Cross-cluster readiness gate. Sticky once flipped to `true`.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Lookup for `GET /cache/v1/raw/{key}`. Returns `None` if the
    /// key is absent (404 territory).
    pub fn get_value(&self, key: &str) -> Option<Vec<u8>> {
        let view = self.view.read().expect("cache view poisoned");
        view.get(key).cloned()
    }

    /// Snapshot of every key currently in the merged view, in
    /// BTreeMap (lexicographic) order. Materializes under a single
    /// read lock so callers see a consistent set.
    pub fn snapshot_keys(&self) -> Vec<String> {
        let view = self.view.read().expect("cache view poisoned");
        view.keys().cloned().collect()
    }

    /// Snapshot of every value currently in the merged view, in the
    /// same order as [`snapshot_keys`](Self::snapshot_keys).
    pub fn snapshot_values(&self) -> Vec<Vec<u8>> {
        let view = self.view.read().expect("cache view poisoned");
        view.values().cloned().collect()
    }

    /// Last-seen offset for one source (topic, partition), or `None`
    /// if no record has been applied to that partition yet.
    pub fn get_offset(&self, topic: &str, partition: u32) -> Option<u64> {
        let offsets = self.offsets.read().expect("cache offsets poisoned");
        offsets
            .get(&TopicPartition {
                topic: topic.to_string(),
                partition,
            })
            .copied()
    }

    /// Snapshot of every `(topic, partition) → offset` entry, sorted
    /// for deterministic header output.
    pub fn snapshot_offsets(&self) -> Vec<TopicPartitionOffset> {
        let offsets = self.offsets.read().expect("cache offsets poisoned");
        let mut out: Vec<TopicPartitionOffset> = offsets
            .iter()
            .map(|(tp, off)| TopicPartitionOffset {
                topic: tp.topic.clone(),
                partition: tp.partition,
                offset: *off,
            })
            .collect();
        out.sort_by(|a, b| a.topic.cmp(&b.topic).then(a.partition.cmp(&b.partition)));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Header, TimestampType};

    fn rec(topic: &str, partition: i32, offset: u64, key: &str, value: Option<&[u8]>) -> Record {
        Record {
            topic: topic.into(),
            partition,
            source_offset: offset,
            timestamp_ms: Some(1_700_000_000_000),
            timestamp_type: TimestampType::CreateTime,
            key: Some(key.as_bytes().to_vec()),
            value: value.map(|v| v.to_vec()),
            headers: Vec::<Header>::new(),
        }
    }

    #[test]
    fn empty_state_starts_not_ready_with_no_mirrors_registered() {
        // With zero registered mirrors there's nothing to wait for;
        // the global ready flag stays `false` (no producers means
        // there's no useful cache yet).
        let s = CacheState::new();
        assert!(!s.is_ready());
        assert!(s.snapshot_keys().is_empty());
        assert!(s.snapshot_offsets().is_empty());
    }

    #[test]
    fn register_empty_topic_marks_mirror_ready_immediately() {
        let s = CacheState::new();
        s.register_mirror("ops", 0);
        assert!(s.is_ready(), "empty topic = hwm 0 = immediately ready");
    }

    #[test]
    fn readiness_flips_only_after_bootstrap_hwm_reached() {
        let s = CacheState::new();
        s.register_mirror("ops", 3); // need offsets 0..=2
        assert!(!s.is_ready());
        s.apply_record("ops", &rec("ops", 0, 0, "k0", Some(b"v0")));
        assert!(!s.is_ready());
        s.apply_record("ops", &rec("ops", 0, 1, "k1", Some(b"v1")));
        assert!(!s.is_ready());
        s.apply_record("ops", &rec("ops", 0, 2, "k2", Some(b"v2")));
        assert!(s.is_ready(), "after offset hwm-1 the mirror must be ready");
    }

    #[test]
    fn multiple_mirrors_all_must_catch_up() {
        let s = CacheState::new();
        s.register_mirror("a", 2);
        s.register_mirror("b", 1);
        assert!(!s.is_ready());
        s.apply_record("a", &rec("topic-a", 0, 0, "ka0", Some(b"va0")));
        s.apply_record("a", &rec("topic-a", 0, 1, "ka1", Some(b"va1")));
        assert!(!s.is_ready(), "a is caught up but b is not");
        s.apply_record("b", &rec("topic-b", 0, 0, "kb0", Some(b"vb0")));
        assert!(s.is_ready());
    }

    #[test]
    fn tombstone_removes_key() {
        let s = CacheState::new();
        s.register_mirror("ops", 2);
        s.apply_record("ops", &rec("ops", 0, 0, "user-1", Some(br#"{"v":1}"#)));
        assert_eq!(
            s.get_value("user-1").as_deref(),
            Some(br#"{"v":1}"#.as_ref())
        );
        s.apply_record("ops", &rec("ops", 0, 1, "user-1", None)); // tombstone
        assert!(s.get_value("user-1").is_none());
    }

    #[test]
    fn rewind_does_not_overwrite_or_remove() {
        let s = CacheState::new();
        s.register_mirror("ops", 1);
        s.apply_record("ops", &rec("ops", 0, 0, "k", Some(b"first")));
        s.apply_record("ops", &rec("ops", 0, 1, "k", Some(b"second")));
        // Now feed a record with an older offset (simulated rewind).
        s.apply_record("ops", &rec("ops", 0, 0, "k", Some(b"first-again")));
        assert_eq!(
            s.get_value("k").as_deref(),
            Some(b"second".as_ref()),
            "rewind must not overwrite the latest value"
        );
        // Equal-offset record is also rejected.
        s.apply_record("ops", &rec("ops", 0, 1, "k", None));
        assert_eq!(
            s.get_value("k").as_deref(),
            Some(b"second".as_ref()),
            "equal-offset replay must not tombstone"
        );
    }

    #[test]
    fn snapshot_offsets_is_deterministic_order() {
        let s = CacheState::new();
        s.register_mirror("m", 10);
        s.apply_record("m", &rec("z-topic", 1, 5, "k", Some(b"v")));
        s.apply_record("m", &rec("a-topic", 3, 4, "k2", Some(b"v")));
        s.apply_record("m", &rec("a-topic", 1, 6, "k3", Some(b"v")));
        let snap = s.snapshot_offsets();
        let order: Vec<_> = snap
            .iter()
            .map(|tpo| (tpo.topic.clone(), tpo.partition))
            .collect();
        assert_eq!(
            order,
            vec![
                ("a-topic".to_string(), 1),
                ("a-topic".to_string(), 3),
                ("z-topic".to_string(), 1),
            ]
        );
    }

    #[test]
    fn snapshot_keys_in_lexicographic_order() {
        let s = CacheState::new();
        s.register_mirror("m", 0);
        s.apply_record("m", &rec("t", 0, 0, "c", Some(b"v")));
        s.apply_record("m", &rec("t", 0, 1, "a", Some(b"v")));
        s.apply_record("m", &rec("t", 0, 2, "b", Some(b"v")));
        assert_eq!(s.snapshot_keys(), vec!["a", "b", "c"]);
    }
}
