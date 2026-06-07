//! Per-mirror in-memory cache views for `http-access: { cache-v1: {} }`
//! mirrors.
//!
//! Each opt-in mirror owns its own `key → latest value` map and
//! `(topic, partition) → offset` map; the sinks update those from
//! the consume loop (per-record, *not* per-flush — freshness is
//! independent of bucket-write cadence), and the HTTP handlers in
//! `mirror-cache` read them out under
//! `/cache/v1/{mirror}/...`. A single mirror may additionally
//! enable `cache-v1-main`, in which case `mirror-cache` mounts the
//! unprefixed `/cache/v1/...` paths onto that mirror's view.
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
//! watermark, it is "ready"; per-mirror HTTP handlers gate on
//! [`CacheState::is_mirror_ready`] and return 503 until that mirror
//! flips. The aggregate [`CacheState::is_ready`] flips only when
//! *every* registered mirror is ready, and backs the `/q/health/ready`
//! drop-in.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use indexmap::IndexMap;

use crate::Record;

/// Per-partition identity used as the offset-map key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: u32,
}

/// Pairs a shared [`CacheState`] with the mirror's operator-chosen
/// `name` so the run loop can route per-record updates into the
/// right readiness slot. The supervisor (mirror-bin) materialises
/// one binding per opt-in mirror and hands it to whichever sink
/// (today: TeeSink at the loop level; FilesystemSink / S3Sink for
/// bootstrap-replay on open) needs to talk to the cache.
///
/// Canonical home is mirror-core so the trait surface is consistent
/// across sink crates and the run loop. `mirror-fs` and `mirror-s3`
/// re-export this type for backwards compatibility.
#[derive(Clone, Debug)]
pub struct CacheBinding {
    pub state: Arc<CacheState>,
    pub mirror_name: String,
}

/// `{topic, partition, offset}` triple as exposed in the
/// `x-kkv-last-seen-offsets` header. Mirrors KKV's `TopicPartitionOffset`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartitionOffset {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
}

/// Enum status for a registered mirror. Carries the names + lag
/// values needed for the structured `/q/health/ready` body so an
/// on-call engineer can grep the response for the unhealthy source
/// or destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MirrorStatus {
    /// Has not yet reached `bootstrap_hwm` for the first time since
    /// this process started. Cache HTTP returns 503; notify
    /// dispatcher continues to suppress per the per-record
    /// threshold check.
    Warming,
    /// Source assignment OK, lag within tolerance, no gating
    /// destination is behind. Cache HTTP returns 200.
    Ready,
    /// Source-side lag exceeds the readiness tolerance. Cache HTTP
    /// returns 503. `lag = broker_end_offset - last_applied_offset`.
    LagBehindSource { lag: u64 },
    /// The Kafka consumer's `assignment()` doesn't include this
    /// mirror's (topic, partition). Set by the supervisor's
    /// assignment poller (lands in commit 8); cleared when the
    /// partition reappears.
    SourceUnassigned { topic: String, partition: u32 },
    /// A gating destination is behind on its `flushed_through`.
    /// Reported by the supervisor's per-destination ack tracker
    /// (mirror-bin); never set by `CacheState` itself.
    DestinationLagging { name: String, lag: u64 },
}

/// One mirror's row in a [`CacheState::status_snapshot`] result.
/// Serialised verbatim into the structured `/q/health/ready` body
/// and into the per-mirror cache 503 body, so a downstream consumer
/// can parse a single shape across both endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorStatusSnapshot {
    pub name: String,
    pub topic: String,
    pub partition: u32,
    pub source_assigned: bool,
    pub last_applied_offset: u64,
    pub broker_end_offset: u64,
    pub status: MirrorStatus,
}

#[derive(Debug)]
struct MirrorSlot {
    bootstrap_hwm: u64,
    /// Offset strictly below which the notify dispatcher suppresses
    /// records. Computed at register time as
    /// `max(last_committed_offset, bootstrap_hwm if no commit)`:
    ///
    ///   * Fresh deploy (no broker-committed offset for the group):
    ///     `suppression_threshold = bootstrap_hwm`. Records during
    ///     the first replay-to-current window don't fan webhooks
    ///     out to consumers.
    ///   * Returning deploy (group has a previously-committed
    ///     offset `C`): `suppression_threshold = C`. Records `[C,
    ///     bootstrap_hwm)` represent the between-pods gap and DO
    ///     fire webhooks — the previous pod was supposed to deliver
    ///     them but exited before doing so. Records below `C` are
    ///     suppressed because the previous pod already delivered
    ///     them.
    ///
    /// Set once at registration; read-only thereafter. Stored as
    /// `u64` rather than `AtomicU64` because it never mutates.
    suppression_threshold: u64,
    /// Source-partition identity. Used by the assignment-loss path
    /// and the structured readiness response body.
    topic: String,
    partition: u32,
    /// Atomically updated by [`apply_record`]. The slot's view of
    /// "highest source offset I've applied" for this mirror,
    /// independent of the per-`TopicPartition` `offsets` map (which
    /// has finer granularity but isn't read on the readiness path).
    last_applied_offset: AtomicU64,
    /// Broker end offset for the mirror's source partition. Initial
    /// value `bootstrap_hwm`; updated by the supervisor's end-offset
    /// poller (commit 8). Used by the readiness predicate as
    /// `lag = broker_end_offset - last_applied_offset`.
    broker_end_offset: AtomicU64,
    /// `true` when the Kafka consumer reports the mirror's
    /// `(topic, partition)` in its `assignment()`. Set by the
    /// supervisor's assignment poller (commit 8); flipped to `false`
    /// transitions the slot to [`MirrorStatus::SourceUnassigned`].
    source_assigned: AtomicBool,
    /// Cached current status. Recomputed by the supervisor or by
    /// `apply_record` whenever an input atom changes. The HTTP
    /// handlers take a read lock here on every probe.
    status: RwLock<MirrorStatus>,
    /// `key → latest-value` for this mirror only. Iteration order is
    /// insertion order (the position a key gets the *first* time
    /// it's seen). Overwrites don't change position. Tombstones
    /// shift subsequent keys down.
    view: RwLock<IndexMap<String, Vec<u8>>>,
    /// Last-seen source offset per (topic, partition) within this
    /// mirror. Monotonic.
    offsets: RwLock<HashMap<TopicPartition, u64>>,
}

#[derive(Debug, Default)]
pub struct CacheState {
    /// Per-mirror slots, keyed by the mirror's configuration name
    /// (unique per process).
    mirrors: RwLock<HashMap<String, MirrorSlot>>,
    /// Name of the mirror that opted into `cache-v1-main`, if any.
    /// `mirror-cache` consults this to decide whether to mount the
    /// unprefixed `/cache/v1/...` routes and which slot to dispatch
    /// them to. Sticky for the lifetime of the process — set at
    /// startup, never re-assigned. Validator enforces at-most-one.
    main_mirror: RwLock<Option<String>>,
    /// Lag (in offsets) tolerated before [`MirrorStatus::Ready`]
    /// flips to [`MirrorStatus::LagBehindSource`]. Default is
    /// `0` (any positive lag fires); the supervisor overrides via
    /// [`Self::with_readiness_lag_tolerance`] from
    /// `MIRROR_V3_READINESS_LAG`.
    readiness_lag_tolerance: u64,
}

impl CacheState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the per-`MirrorSlot` lag tolerance. The supervisor
    /// reads `MIRROR_V3_READINESS_LAG` and calls this before
    /// registering any mirror. Tests use it to construct a slot that
    /// tolerates a deliberately-injected lag value.
    pub fn with_readiness_lag_tolerance(mut self, tolerance: u64) -> Self {
        self.readiness_lag_tolerance = tolerance;
        self
    }

    /// Register an opt-in mirror with its source-partition high
    /// watermark captured at startup. Must be called once per mirror
    /// before any `apply_record` for that mirror runs.
    ///
    /// `bootstrap_hwm` is the Kafka high-watermark (one past the last
    /// existing offset). An empty topic has `bootstrap_hwm = 0` and
    /// the mirror is immediately considered caught up.
    ///
    /// `last_committed_offset` is the value the supervisor read from
    /// the broker's `__consumer_offsets` for this group at startup
    /// (`Source::fetch_committed_offset`). `Some(c)` means the prior
    /// pod committed through `c` and webhook suppression resumes at
    /// `c` rather than at `bootstrap_hwm`; `None` is a fresh group
    /// and suppression uses `bootstrap_hwm`.
    ///
    /// `is_main` selects this mirror as the one `cache-v1-main`
    /// mounts the unprefixed `/cache/v1/...` paths onto; the
    /// validator enforces at-most-one, so the supervisor's last call
    /// wins if it ever passes multiple `true`s (defensive — should
    /// never happen).
    pub fn register_mirror(
        &self,
        mirror_name: &str,
        bootstrap_hwm: u64,
        last_committed_offset: Option<u64>,
        is_main: bool,
    ) {
        self.register_mirror_with_topic(
            mirror_name,
            bootstrap_hwm,
            last_committed_offset,
            is_main,
            "",
            0,
        );
    }

    /// Same as [`Self::register_mirror`] plus the source identity
    /// (`topic`, `partition`). The identity is surfaced in the
    /// [`MirrorStatus::SourceUnassigned`] body so the structured
    /// readiness response names the partition that disappeared.
    /// `register_mirror` calls this with placeholder identity so
    /// tests that don't care can keep the shorter signature.
    pub fn register_mirror_with_topic(
        &self,
        mirror_name: &str,
        bootstrap_hwm: u64,
        last_committed_offset: Option<u64>,
        is_main: bool,
        topic: &str,
        partition: u32,
    ) {
        // Returning-deploy commit wins when present; otherwise the
        // fresh-deploy fallback skips historical backlog up to the
        // broker's high-watermark.
        let suppression_threshold = last_committed_offset.unwrap_or(bootstrap_hwm);
        // Empty topic (`bootstrap_hwm = 0`) is immediately ready;
        // every other case starts in `Warming` and transitions via
        // `apply_record` / the supervisor's pollers.
        let initial_status = if bootstrap_hwm == 0 {
            MirrorStatus::Ready
        } else {
            MirrorStatus::Warming
        };
        let mut m = self.mirrors.write().expect("cache mirrors poisoned");
        m.insert(
            mirror_name.to_string(),
            MirrorSlot {
                bootstrap_hwm,
                suppression_threshold,
                topic: topic.to_string(),
                partition,
                last_applied_offset: AtomicU64::new(0),
                broker_end_offset: AtomicU64::new(bootstrap_hwm),
                source_assigned: AtomicBool::new(true),
                status: RwLock::new(initial_status),
                view: RwLock::new(IndexMap::new()),
                offsets: RwLock::new(HashMap::new()),
            },
        );
        drop(m);
        if is_main {
            *self
                .main_mirror
                .write()
                .expect("cache main_mirror poisoned") = Some(mirror_name.to_string());
        }
    }

    /// True iff the notify dispatcher should drop a record at
    /// `source_offset` for `mirror_name`. Compared against the
    /// per-mirror `suppression_threshold` set at register time. An
    /// unknown mirror returns `false` (no info, don't suppress) so
    /// the legacy behaviour of "fire if not registered" is
    /// preserved.
    pub fn is_record_suppressed(&self, mirror_name: &str, source_offset: u64) -> bool {
        let mirrors = self.mirrors.read().expect("cache mirrors poisoned");
        mirrors
            .get(mirror_name)
            .map(|slot| source_offset < slot.suppression_threshold)
            .unwrap_or(false)
    }

    /// Apply a record from the source consume loop to the named
    /// mirror's in-memory view and offset map. Flips the mirror's
    /// readiness slot once the bootstrap watermark is reached.
    ///
    /// Monotonic: if `record.source_offset` is not strictly greater
    /// than the partition's last-applied offset on this mirror
    /// (rewind / replay), the call is a no-op for both the view and
    /// the offset map.
    pub fn apply_record(&self, mirror_name: &str, record: &Record) {
        let mirrors = self.mirrors.read().expect("cache mirrors poisoned");
        let Some(slot) = mirrors.get(mirror_name) else {
            // No registered slot for this mirror; sinks that route
            // through a `CacheBinding` are wired to one that always
            // matches. Treat an unknown name as a no-op rather than
            // panic so a future refactor that decouples destinations
            // from registration can't crash the consume loop.
            return;
        };
        let tp = TopicPartition {
            topic: record.topic.clone(),
            partition: record.partition as u32,
        };
        {
            let mut offsets = slot.offsets.write().expect("mirror offsets poisoned");
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
        {
            let mut view = slot.view.write().expect("mirror view poisoned");
            match record.value.as_ref() {
                Some(v) => {
                    // IndexMap::insert keeps the existing position on
                    // overwrite and appends only on first sighting —
                    // which is the contract clients want for `/keys`
                    // ordering ("new keys appear at the end").
                    view.insert(key, v.clone());
                }
                None => {
                    // shift_remove preserves the relative order of
                    // the remaining entries; swap_remove would be
                    // faster but shuffle the trailing key into the
                    // gap, breaking determinism.
                    view.shift_remove(&key);
                }
            }
        }
        // Advance the per-mirror `last_applied_offset` and recompute
        // the status. Both the per-`TopicPartition` `offsets` map
        // above and this atom are kept; the atom is what the
        // readiness predicate reads.
        slot.last_applied_offset
            .fetch_max(record.source_offset + 1, Ordering::AcqRel);
        Self::recompute_status_locked(slot, self.readiness_lag_tolerance);
    }

    /// Compute the current status of a slot from its atomic
    /// counters. Called by every input mutator: `apply_record`,
    /// `set_broker_end_offset`, `mark_source_assigned`. Holds the
    /// status RwLock briefly.
    ///
    /// Order of precedence (highest wins):
    ///   1. `SourceUnassigned` — the consume loop is effectively dead
    ///      until the partition reappears in the assignment.
    ///   2. `Warming` — never caught up to `bootstrap_hwm` since
    ///      process start.
    ///   3. `DestinationLagging` — already encoded in the current
    ///      status by the mirror-bin setter; preserved here so
    ///      destination-side state doesn't get clobbered by a
    ///      source-side recompute.
    ///   4. `LagBehindSource` — lag exceeds tolerance.
    ///   5. `Ready`.
    fn recompute_status_locked(slot: &MirrorSlot, tolerance: u64) {
        let mut current = slot.status.write().expect("status poisoned");
        // Preserve a destination-lagging signal — only mirror-bin's
        // destination-lag setter can set or clear that variant. The
        // source-side recompute leaves it alone so a destination
        // problem isn't masked by a fresh source-side ack.
        if matches!(*current, MirrorStatus::DestinationLagging { .. }) {
            return;
        }
        if !slot.source_assigned.load(Ordering::Acquire) {
            *current = MirrorStatus::SourceUnassigned {
                topic: slot.topic.clone(),
                partition: slot.partition,
            };
            return;
        }
        let last_applied = slot.last_applied_offset.load(Ordering::Acquire);
        let broker_end = slot.broker_end_offset.load(Ordering::Acquire);
        if last_applied < slot.bootstrap_hwm {
            *current = MirrorStatus::Warming;
            return;
        }
        let lag = broker_end.saturating_sub(last_applied);
        if lag > tolerance {
            *current = MirrorStatus::LagBehindSource { lag };
        } else {
            *current = MirrorStatus::Ready;
        }
    }

    /// Set the broker's current end offset for `mirror_name`. The
    /// supervisor's end-offset poller (commit 8) calls this every
    /// `MIRROR_V3_READINESS_POLL_MS`; the resulting recompute may
    /// flip the slot into [`MirrorStatus::LagBehindSource`] or back
    /// to [`MirrorStatus::Ready`].
    pub fn set_broker_end_offset(&self, mirror_name: &str, end_offset: u64) {
        let mirrors = self.mirrors.read().expect("cache mirrors poisoned");
        let Some(slot) = mirrors.get(mirror_name) else {
            return;
        };
        // Monotonic — broker end-offset only advances.
        slot.broker_end_offset
            .fetch_max(end_offset, Ordering::AcqRel);
        Self::recompute_status_locked(slot, self.readiness_lag_tolerance);
    }

    /// Mark the source partition as unassigned. The supervisor's
    /// assignment poller (commit 8) calls this when
    /// `consumer.assignment()` no longer includes the mirror's
    /// partition.
    pub fn mark_source_unassigned(&self, mirror_name: &str) {
        let mirrors = self.mirrors.read().expect("cache mirrors poisoned");
        let Some(slot) = mirrors.get(mirror_name) else {
            return;
        };
        slot.source_assigned.store(false, Ordering::Release);
        Self::recompute_status_locked(slot, self.readiness_lag_tolerance);
    }

    /// Mark the source partition as re-assigned. Inverse of
    /// [`Self::mark_source_unassigned`].
    pub fn mark_source_assigned(&self, mirror_name: &str) {
        let mirrors = self.mirrors.read().expect("cache mirrors poisoned");
        let Some(slot) = mirrors.get(mirror_name) else {
            return;
        };
        slot.source_assigned.store(true, Ordering::Release);
        Self::recompute_status_locked(slot, self.readiness_lag_tolerance);
    }

    /// Record that a gating destination is behind. The supervisor's
    /// per-destination lag check sets this; clearing it requires a
    /// follow-up call to [`Self::clear_destination_lagging`].
    pub fn mark_destination_lagging(&self, mirror_name: &str, dest_name: &str, lag: u64) {
        let mirrors = self.mirrors.read().expect("cache mirrors poisoned");
        let Some(slot) = mirrors.get(mirror_name) else {
            return;
        };
        let mut s = slot.status.write().expect("status poisoned");
        *s = MirrorStatus::DestinationLagging {
            name: dest_name.to_string(),
            lag,
        };
    }

    /// Clear a destination-lagging signal and let the next
    /// source-side recompute pick a fresh status. The supervisor
    /// calls this when every gating destination is back within
    /// tolerance.
    pub fn clear_destination_lagging(&self, mirror_name: &str) {
        let mirrors = self.mirrors.read().expect("cache mirrors poisoned");
        let Some(slot) = mirrors.get(mirror_name) else {
            return;
        };
        // Reset to Warming so the next recompute picks the right
        // source-side status. Direct write here so the existing
        // DestinationLagging guard in `recompute_status_locked`
        // doesn't see a stale DestinationLagging.
        *slot.status.write().expect("status poisoned") = MirrorStatus::Warming;
        Self::recompute_status_locked(slot, self.readiness_lag_tolerance);
    }

    /// Cross-mirror readiness gate. Non-sticky: returns `true` iff
    /// at least one mirror is registered and every registered
    /// mirror currently reports [`MirrorStatus::Ready`].
    pub fn is_ready(&self) -> bool {
        let mirrors = self.mirrors.read().expect("cache mirrors poisoned");
        !mirrors.is_empty()
            && mirrors.values().all(|slot| {
                matches!(
                    *slot.status.read().expect("status poisoned"),
                    MirrorStatus::Ready
                )
            })
    }

    /// Per-mirror readiness gate. Returns `true` iff `mirror_name`
    /// is registered AND its current status is
    /// [`MirrorStatus::Ready`]. Non-sticky: a mirror that drops out
    /// of Ready (lag, assignment loss, destination problem) flips
    /// this to `false`.
    pub fn is_mirror_ready(&self, mirror_name: &str) -> bool {
        self.status_for(mirror_name)
            .is_some_and(|s| matches!(s, MirrorStatus::Ready))
    }

    /// Snapshot the current status for a registered mirror. Returns
    /// `None` if the name is unknown. Used by the structured
    /// `/q/health/ready` body and by tests.
    pub fn status_for(&self, mirror_name: &str) -> Option<MirrorStatus> {
        let mirrors = self.mirrors.read().expect("cache mirrors poisoned");
        mirrors
            .get(mirror_name)
            .map(|slot| slot.status.read().expect("status poisoned").clone())
    }

    /// Snapshot every registered mirror's per-mirror readiness state
    /// in a single pass. Used by the structured `/q/health/ready`
    /// HTTP handler and by the per-mirror cache 503 body, both of
    /// which want a consistent view across mirrors without taking
    /// the slot lock multiple times.
    ///
    /// Entries are emitted in arbitrary order; the caller sorts when
    /// stable ordering matters (the readiness handler does).
    pub fn status_snapshot(&self) -> Vec<MirrorStatusSnapshot> {
        let mirrors = self.mirrors.read().expect("cache mirrors poisoned");
        mirrors
            .iter()
            .map(|(name, slot)| MirrorStatusSnapshot {
                name: name.clone(),
                topic: slot.topic.clone(),
                partition: slot.partition,
                source_assigned: slot.source_assigned.load(Ordering::Acquire),
                last_applied_offset: slot.last_applied_offset.load(Ordering::Acquire),
                broker_end_offset: slot.broker_end_offset.load(Ordering::Acquire),
                status: slot.status.read().expect("status poisoned").clone(),
            })
            .collect()
    }

    /// Name of the mirror that opted into `cache-v1-main`, or
    /// `None` if no mirror selected the singleton. The cache HTTP
    /// router uses this to decide whether to mount the unprefixed
    /// `/cache/v1/...` paths and which slot to dispatch them to.
    pub fn main_mirror(&self) -> Option<String> {
        self.main_mirror
            .read()
            .expect("cache main_mirror poisoned")
            .clone()
    }

    /// Lookup for `GET /cache/v1/{mirror}/raw/{key}`. Returns `None`
    /// when the mirror has no such key (404 territory) and also when
    /// `mirror_name` is unknown — the HTTP handler maps unknown
    /// mirrors to 404 anyway, so the call sites stay tight.
    pub fn get_value_for(&self, mirror_name: &str, key: &str) -> Option<Vec<u8>> {
        let mirrors = self.mirrors.read().expect("cache mirrors poisoned");
        let slot = mirrors.get(mirror_name)?;
        let view = slot.view.read().expect("mirror view poisoned");
        view.get(key).cloned()
    }

    /// Snapshot of every key currently in the named mirror's view,
    /// in insertion order. Returns `None` if the mirror is unknown.
    pub fn snapshot_keys_for(&self, mirror_name: &str) -> Option<Vec<String>> {
        let mirrors = self.mirrors.read().expect("cache mirrors poisoned");
        let slot = mirrors.get(mirror_name)?;
        let view = slot.view.read().expect("mirror view poisoned");
        Some(view.keys().cloned().collect())
    }

    /// Snapshot of every value in the named mirror's view, in the
    /// same order as [`Self::snapshot_keys_for`]. `None` for unknown
    /// mirrors.
    pub fn snapshot_values_for(&self, mirror_name: &str) -> Option<Vec<Vec<u8>>> {
        let mirrors = self.mirrors.read().expect("cache mirrors poisoned");
        let slot = mirrors.get(mirror_name)?;
        let view = slot.view.read().expect("mirror view poisoned");
        Some(view.values().cloned().collect())
    }

    /// Last-seen offset within `mirror_name` for one source
    /// (topic, partition). `None` if the mirror is unknown or has
    /// not seen a record on that partition yet.
    pub fn get_offset_for(&self, mirror_name: &str, topic: &str, partition: u32) -> Option<u64> {
        let mirrors = self.mirrors.read().expect("cache mirrors poisoned");
        let slot = mirrors.get(mirror_name)?;
        let offsets = slot.offsets.read().expect("mirror offsets poisoned");
        offsets
            .get(&TopicPartition {
                topic: topic.to_string(),
                partition,
            })
            .copied()
    }

    /// Snapshot of `(topic, partition) → offset` entries for the
    /// named mirror, sorted for deterministic header output. `None`
    /// if the mirror is unknown.
    pub fn snapshot_offsets_for(&self, mirror_name: &str) -> Option<Vec<TopicPartitionOffset>> {
        let mirrors = self.mirrors.read().expect("cache mirrors poisoned");
        let slot = mirrors.get(mirror_name)?;
        let offsets = slot.offsets.read().expect("mirror offsets poisoned");
        let mut out: Vec<TopicPartitionOffset> = offsets
            .iter()
            .map(|(tp, off)| TopicPartitionOffset {
                topic: tp.topic.clone(),
                partition: tp.partition,
                offset: *off,
            })
            .collect();
        out.sort_by(|a, b| a.topic.cmp(&b.topic).then(a.partition.cmp(&b.partition)));
        Some(out)
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
    fn is_mirror_ready_reports_per_mirror_status() {
        // Per-mirror gate is the kkv-v1 notifier's suppression knob:
        // it lets one mirror start emitting webhooks while another is
        // still warming up against its bootstrap_hwm. Verify the three
        // states the notifier cares about: unknown name, registered
        // but pre-hwm, registered and caught up.
        let s = CacheState::new();
        assert!(
            !s.is_mirror_ready("unknown"),
            "unknown name must report false so an uninstrumented \
             notifier can't accidentally fire"
        );
        s.register_mirror("warming", 3, None, false);
        assert!(!s.is_mirror_ready("warming"), "hwm 3, no records yet");
        s.apply_record("warming", &rec("warming", 0, 0, "k0", Some(b"v")));
        s.apply_record("warming", &rec("warming", 0, 1, "k1", Some(b"v")));
        assert!(!s.is_mirror_ready("warming"), "still 1 offset short of hwm");
        s.apply_record("warming", &rec("warming", 0, 2, "k2", Some(b"v")));
        assert!(s.is_mirror_ready("warming"), "offset hwm-1 flips the slot");
        // Independent slot stays at its own state.
        s.register_mirror("empty", 0, None, false);
        assert!(
            s.is_mirror_ready("empty"),
            "hwm 0 = immediately ready, independent of other mirrors"
        );
    }

    #[test]
    fn empty_state_starts_not_ready_with_no_mirrors_registered() {
        // With zero registered mirrors there's nothing to wait for;
        // the global ready flag stays `false` (no producers means
        // there's no useful cache yet).
        let s = CacheState::new();
        assert!(!s.is_ready());
        assert!(s.main_mirror().is_none());
        assert!(s.snapshot_keys_for("missing").is_none());
        assert!(s.snapshot_offsets_for("missing").is_none());
    }

    #[test]
    fn register_empty_topic_marks_mirror_ready_immediately() {
        let s = CacheState::new();
        s.register_mirror("ops", 0, None, false);
        assert!(s.is_ready(), "empty topic = hwm 0 = immediately ready");
    }

    #[test]
    fn readiness_flips_only_after_bootstrap_hwm_reached() {
        let s = CacheState::new();
        s.register_mirror("ops", 3, None, false); // need offsets 0..=2
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
        s.register_mirror("a", 2, None, false);
        s.register_mirror("b", 1, None, false);
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
        s.register_mirror("ops", 2, None, false);
        s.apply_record("ops", &rec("ops", 0, 0, "user-1", Some(br#"{"v":1}"#)));
        assert_eq!(
            s.get_value_for("ops", "user-1").as_deref(),
            Some(br#"{"v":1}"#.as_ref())
        );
        s.apply_record("ops", &rec("ops", 0, 1, "user-1", None)); // tombstone
        assert!(s.get_value_for("ops", "user-1").is_none());
    }

    #[test]
    fn rewind_does_not_overwrite_or_remove() {
        let s = CacheState::new();
        s.register_mirror("ops", 1, None, false);
        s.apply_record("ops", &rec("ops", 0, 0, "k", Some(b"first")));
        s.apply_record("ops", &rec("ops", 0, 1, "k", Some(b"second")));
        // Now feed a record with an older offset (simulated rewind).
        s.apply_record("ops", &rec("ops", 0, 0, "k", Some(b"first-again")));
        assert_eq!(
            s.get_value_for("ops", "k").as_deref(),
            Some(b"second".as_ref()),
            "rewind must not overwrite the latest value"
        );
        // Equal-offset record is also rejected.
        s.apply_record("ops", &rec("ops", 0, 1, "k", None));
        assert_eq!(
            s.get_value_for("ops", "k").as_deref(),
            Some(b"second".as_ref()),
            "equal-offset replay must not tombstone"
        );
    }

    #[test]
    fn snapshot_offsets_is_deterministic_order() {
        let s = CacheState::new();
        s.register_mirror("m", 10, None, false);
        s.apply_record("m", &rec("z-topic", 1, 5, "k", Some(b"v")));
        s.apply_record("m", &rec("a-topic", 3, 4, "k2", Some(b"v")));
        s.apply_record("m", &rec("a-topic", 1, 6, "k3", Some(b"v")));
        let snap = s.snapshot_offsets_for("m").unwrap();
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
    fn snapshot_keys_in_insertion_order() {
        let s = CacheState::new();
        s.register_mirror("m", 0, None, false);
        s.apply_record("m", &rec("t", 0, 0, "c", Some(b"v")));
        s.apply_record("m", &rec("t", 0, 1, "a", Some(b"v")));
        s.apply_record("m", &rec("t", 0, 2, "b", Some(b"v")));
        assert_eq!(s.snapshot_keys_for("m").unwrap(), vec!["c", "a", "b"]);
    }

    #[test]
    fn overwrite_keeps_position_in_listing() {
        let s = CacheState::new();
        s.register_mirror("m", 0, None, false);
        s.apply_record("m", &rec("t", 0, 0, "x", Some(b"v0")));
        s.apply_record("m", &rec("t", 0, 1, "y", Some(b"v1")));
        s.apply_record("m", &rec("t", 0, 2, "x", Some(b"v0-updated")));
        assert_eq!(s.snapshot_keys_for("m").unwrap(), vec!["x", "y"]);
        assert_eq!(
            s.snapshot_values_for("m").unwrap(),
            vec![b"v0-updated".to_vec(), b"v1".to_vec()]
        );
    }

    #[test]
    fn tombstone_preserves_order_of_remaining() {
        let s = CacheState::new();
        s.register_mirror("m", 0, None, false);
        s.apply_record("m", &rec("t", 0, 0, "a", Some(b"va")));
        s.apply_record("m", &rec("t", 0, 1, "b", Some(b"vb")));
        s.apply_record("m", &rec("t", 0, 2, "c", Some(b"vc")));
        s.apply_record("m", &rec("t", 0, 3, "b", None)); // tombstone middle
        assert_eq!(s.snapshot_keys_for("m").unwrap(), vec!["a", "c"]);
    }

    #[test]
    fn per_mirror_views_are_independent() {
        // Two mirrors writing through their own slots: a key in
        // mirror A must not show up in mirror B's view, and an
        // unregistered mirror name returns None across the board.
        let s = CacheState::new();
        s.register_mirror("a", 0, None, false);
        s.register_mirror("b", 0, None, false);
        s.apply_record("a", &rec("topic-a", 0, 0, "k-a", Some(b"va")));
        s.apply_record("b", &rec("topic-b", 0, 0, "k-b", Some(b"vb")));
        assert_eq!(s.get_value_for("a", "k-a").as_deref(), Some(b"va".as_ref()));
        assert!(s.get_value_for("a", "k-b").is_none());
        assert_eq!(s.get_value_for("b", "k-b").as_deref(), Some(b"vb".as_ref()));
        assert!(s.get_value_for("missing", "anything").is_none());
        assert!(s.snapshot_keys_for("missing").is_none());
    }

    #[test]
    fn register_mirror_tracks_main_mirror_singleton() {
        let s = CacheState::new();
        assert!(s.main_mirror().is_none());
        s.register_mirror("ops", 0, None, false);
        assert!(
            s.main_mirror().is_none(),
            "is_main=false does not assign the singleton"
        );
        s.register_mirror("users", 0, None, true);
        assert_eq!(s.main_mirror().as_deref(), Some("users"));
    }
}

#[cfg(test)]
mod threshold_tests {
    use super::*;

    #[test]
    fn fresh_deploy_suppresses_below_bootstrap_hwm() {
        let s = CacheState::new();
        s.register_mirror("m", 10, None, false);
        for off in 0..10 {
            assert!(
                s.is_record_suppressed("m", off),
                "fresh deploy must suppress offset {off} (< hwm 10)"
            );
        }
        assert!(
            !s.is_record_suppressed("m", 10),
            "offset == hwm must NOT be suppressed (first live record)"
        );
        assert!(!s.is_record_suppressed("m", 50));
    }

    #[test]
    fn returning_deploy_suppresses_below_committed_offset() {
        let s = CacheState::new();
        s.register_mirror("m", 10, Some(5), false);
        for off in 0..5 {
            assert!(
                s.is_record_suppressed("m", off),
                "returning deploy must suppress offset {off} below committed 5"
            );
        }
        for off in 5..15 {
            assert!(
                !s.is_record_suppressed("m", off),
                "offset {off} must fire (>= committed 5)"
            );
        }
    }

    #[test]
    fn unknown_mirror_is_not_suppressed() {
        let s = CacheState::new();
        assert!(
            !s.is_record_suppressed("never-registered", 0),
            "unknown mirror returns false (no info, don't suppress)"
        );
    }

    #[test]
    fn empty_topic_no_committed_suppresses_nothing() {
        let s = CacheState::new();
        s.register_mirror("m", 0, None, false);
        assert!(!s.is_record_suppressed("m", 0));
        assert!(!s.is_record_suppressed("m", 99));
    }
}

#[cfg(test)]
mod status_transition_tests {
    use super::*;

    fn rec(topic: &str, partition: i32, offset: u64, key: &str) -> Record {
        Record {
            topic: topic.into(),
            partition,
            source_offset: offset,
            timestamp_ms: Some(1_700_000_000_000),
            timestamp_type: crate::TimestampType::CreateTime,
            key: Some(key.as_bytes().to_vec()),
            value: Some(b"v".to_vec()),
            headers: Vec::<crate::Header>::new(),
        }
    }

    #[test]
    fn empty_topic_starts_ready() {
        let s = CacheState::new();
        s.register_mirror_with_topic("m", 0, None, false, "t", 0);
        assert_eq!(s.status_for("m"), Some(MirrorStatus::Ready));
        assert!(s.is_mirror_ready("m"));
        assert!(s.is_ready(), "aggregate is true once every mirror is Ready");
    }

    #[test]
    fn non_empty_topic_starts_warming_and_flips_on_catch_up() {
        let s = CacheState::new();
        s.register_mirror_with_topic("m", 5, None, false, "t", 0);
        assert_eq!(s.status_for("m"), Some(MirrorStatus::Warming));
        assert!(!s.is_mirror_ready("m"));

        // Apply offsets 0..3 — still Warming because last_applied (= 4 after offset 3 sets `last_applied_offset = 4`) is below bootstrap_hwm 5.
        for off in 0..4 {
            s.apply_record("m", &rec("t", 0, off, &format!("k{off}")));
        }
        assert_eq!(s.status_for("m"), Some(MirrorStatus::Warming));

        // Apply offset 4 — last_applied = 5, which equals bootstrap_hwm → Ready.
        s.apply_record("m", &rec("t", 0, 4, "k4"));
        assert_eq!(s.status_for("m"), Some(MirrorStatus::Ready));
        assert!(s.is_mirror_ready("m"));
    }

    #[test]
    fn poller_pushes_lag_then_recovers() {
        // After warming, the broker advances. With tolerance=0, even
        // one offset of lag flips the slot to LagBehindSource. A
        // follow-up apply_record at the new end offset recovers to
        // Ready.
        let s = CacheState::new();
        s.register_mirror_with_topic("m", 1, None, false, "t", 0);
        s.apply_record("m", &rec("t", 0, 0, "k0")); // catch up; last_applied = 1
        assert_eq!(s.status_for("m"), Some(MirrorStatus::Ready));

        s.set_broker_end_offset("m", 5);
        assert_eq!(
            s.status_for("m"),
            Some(MirrorStatus::LagBehindSource { lag: 4 })
        );
        assert!(!s.is_mirror_ready("m"));
        assert!(!s.is_ready());

        s.apply_record("m", &rec("t", 0, 1, "k1"));
        s.apply_record("m", &rec("t", 0, 2, "k2"));
        s.apply_record("m", &rec("t", 0, 3, "k3"));
        s.apply_record("m", &rec("t", 0, 4, "k4"));
        assert_eq!(s.status_for("m"), Some(MirrorStatus::Ready));
        assert!(s.is_mirror_ready("m"));
    }

    #[test]
    fn source_unassigned_overrides_other_states() {
        let s = CacheState::new();
        s.register_mirror_with_topic("m", 0, None, false, "user-states", 7);
        assert_eq!(s.status_for("m"), Some(MirrorStatus::Ready));

        s.mark_source_unassigned("m");
        match s.status_for("m") {
            Some(MirrorStatus::SourceUnassigned { topic, partition }) => {
                assert_eq!(topic, "user-states");
                assert_eq!(partition, 7);
            }
            other => panic!("expected SourceUnassigned, got {other:?}"),
        }
        assert!(!s.is_mirror_ready("m"));

        // Source comes back; recompute returns to Ready (empty
        // topic, no lag).
        s.mark_source_assigned("m");
        assert_eq!(s.status_for("m"), Some(MirrorStatus::Ready));
    }

    #[test]
    fn destination_lagging_is_set_and_cleared_externally() {
        let s = CacheState::new();
        s.register_mirror_with_topic("m", 0, None, false, "t", 0);
        assert_eq!(s.status_for("m"), Some(MirrorStatus::Ready));

        s.mark_destination_lagging("m", "users-gcs", 42);
        match s.status_for("m") {
            Some(MirrorStatus::DestinationLagging { name, lag }) => {
                assert_eq!(name, "users-gcs");
                assert_eq!(lag, 42);
            }
            other => panic!("expected DestinationLagging, got {other:?}"),
        }
        assert!(!s.is_mirror_ready("m"));

        // An incoming apply_record must NOT clobber DestinationLagging.
        s.apply_record("m", &rec("t", 0, 0, "k0"));
        assert!(matches!(
            s.status_for("m"),
            Some(MirrorStatus::DestinationLagging { .. })
        ));

        // Clearing returns to source-side state.
        s.clear_destination_lagging("m");
        assert_eq!(s.status_for("m"), Some(MirrorStatus::Ready));
    }

    #[test]
    fn aggregate_is_ready_ands_every_slot() {
        let s = CacheState::new();
        s.register_mirror_with_topic("ready", 0, None, false, "t1", 0);
        s.register_mirror_with_topic("warming", 5, None, false, "t2", 0);
        assert!(
            !s.is_ready(),
            "aggregate is false while one slot is Warming"
        );
        for off in 0..5 {
            s.apply_record("warming", &rec("t2", 0, off, &format!("k{off}")));
        }
        assert!(s.is_ready(), "aggregate flips to true when both Ready");
    }

    #[test]
    fn aggregate_is_not_ready_when_no_mirrors_are_registered() {
        let s = CacheState::new();
        assert!(
            !s.is_ready(),
            "aggregate is false when nothing has been registered"
        );
    }

    #[test]
    fn lag_tolerance_lets_a_small_lag_stay_ready() {
        let s = CacheState::new().with_readiness_lag_tolerance(10);
        s.register_mirror_with_topic("m", 1, None, false, "t", 0);
        s.apply_record("m", &rec("t", 0, 0, "k0")); // Ready, lag=0
        s.set_broker_end_offset("m", 8); // lag=7 <= 10
        assert_eq!(s.status_for("m"), Some(MirrorStatus::Ready));
        s.set_broker_end_offset("m", 100); // lag=99 > 10
        assert_eq!(
            s.status_for("m"),
            Some(MirrorStatus::LagBehindSource { lag: 99 })
        );
    }
}
