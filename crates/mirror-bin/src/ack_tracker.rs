//! Per-mirror ack tracking and the periodic source-commit task.
//!
//! The supervisor builds one [`AckTracker`] per spawned mirror at
//! startup. The tracker aggregates two kinds of "we delivered through
//! offset N" signals:
//!
//! * A notify-side signal from `KkvV1Notifier` / `FlushDispatcher`
//!   (when the mirror has a `notify:` block). The notifier installs
//!   the tracker as its [`mirror_core::AckSink`] via
//!   `with_ack_sink`; every successful drain calls
//!   `note_through(batch.high_offset + 1)`.
//! * One per-destination signal, fed by [`FlushAckShim`] (blob
//!   sinks) or [`WriteAckShim`] (Kafka sinks). Each shim sits on a
//!   destination's existing observer hook and bumps the matching
//!   [`DestAckSlot::flushed_through`] on every flush / write.
//!
//! The periodic commit task in [`spawn_periodic_commit_task`] reads
//! [`AckTracker::commit_offset`] every
//! `MIRROR_V3_OFFSET_COMMIT_INTERVAL_MS` (default 5 s), stages the
//! result via [`mirror_kafka::KafkaCommitHandle::commit_through`],
//! and flushes it with `commit_pending`. The commit handle is a
//! cheap clone of an `Arc<StreamConsumer>` so the task can run
//! independently of the source-owning run loop.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mirror_core::{AckSink, FlushObserver, WriteObserver};
use mirror_kafka::KafkaCommitHandle;
use tokio::sync::watch;

const DEFAULT_COMMIT_INTERVAL: Duration = Duration::from_secs(5);

/// Read the commit interval from `MIRROR_V3_OFFSET_COMMIT_INTERVAL_MS`,
/// falling back to [`DEFAULT_COMMIT_INTERVAL`]. A value of `0`
/// disables the periodic task (the supervisor then never advances
/// the broker-side committed offset and the mirror behaves as it did
/// before this work).
pub fn commit_interval_from_env() -> Duration {
    match std::env::var("MIRROR_V3_OFFSET_COMMIT_INTERVAL_MS")
        .ok()
        .as_deref()
    {
        Some(s) => match s.parse::<u64>() {
            Ok(ms) => Duration::from_millis(ms),
            Err(_) => DEFAULT_COMMIT_INTERVAL,
        },
        None => DEFAULT_COMMIT_INTERVAL,
    }
}

/// One destination's ack slot. Held by both the supervisor (in the
/// [`AckTracker`]) and by the shim observer installed on the
/// inner sink, via `Arc::clone`.
#[derive(Debug)]
pub struct DestAckSlot {
    /// Operator-chosen destination name; surfaces in logs and (in a
    /// later commit) in the structured `/q/health/ready` body.
    #[allow(dead_code)] // surfaced in commit 7 + commit 10
    pub name: String,
    /// Highest offset strictly *below which* this destination has
    /// durably accepted everything. Monotonic via `fetch_max`.
    pub flushed_through: AtomicU64,
    /// Whether this destination's ack gates source-side readiness
    /// (and, for non-notify mirrors, the source commit). Per-
    /// destination YAML field lands in a later commit; for now the
    /// supervisor passes `true` for every destination.
    #[allow(dead_code)] // honoured in commit 7 + commit 9
    pub affects_readiness: bool,
}

impl DestAckSlot {
    pub fn new(name: String, affects_readiness: bool) -> Self {
        Self {
            name,
            flushed_through: AtomicU64::new(0),
            affects_readiness,
        }
    }

    pub fn note_through(&self, through: u64) {
        self.flushed_through.fetch_max(through, Ordering::AcqRel);
    }
}

/// Per-mirror ack tracker. The `notify` slot is `Some` when the
/// mirror has a `notify:` block (source-consume or destination-
/// flush); the destinations list always has one entry per
/// destination in the YAML.
pub struct AckTracker {
    notify: Option<AtomicU64>,
    destinations: Vec<Arc<DestAckSlot>>,
}

impl AckTracker {
    pub fn new(notify_present: bool, destinations: Vec<Arc<DestAckSlot>>) -> Self {
        let notify = if notify_present {
            Some(AtomicU64::new(0))
        } else {
            None
        };
        Self {
            notify,
            destinations,
        }
    }

    /// The offset the supervisor's periodic commit task should
    /// stage. Returns 0 when nothing has been delivered yet (the
    /// commit task interprets 0 as "skip this tick").
    ///
    /// For notify mirrors the notify-side ack is authoritative;
    /// destinations are observability-only. For non-notify mirrors
    /// the highest destination ack wins — the supervisor commits the
    /// fastest destination's progress, matching the
    /// `DELIVERY_SEMANTICS_REVISIT.md § 2` rule that non-notify
    /// commits are observability rather than restart-resume state.
    pub fn commit_offset(&self) -> u64 {
        if let Some(notify) = self.notify.as_ref() {
            notify.load(Ordering::Acquire)
        } else {
            self.destinations
                .iter()
                .map(|d| d.flushed_through.load(Ordering::Acquire))
                .max()
                .unwrap_or(0)
        }
    }
}

impl AckSink for AckTracker {
    fn note_through(&self, through: u64) {
        // Only the notify slot is fed via the AckSink trait surface;
        // destinations have their own shim observers writing
        // directly to their `DestAckSlot`s.
        if let Some(notify) = self.notify.as_ref() {
            notify.fetch_max(through, Ordering::AcqRel);
        }
    }
}

/// Bridges a blob sink's `FlushObserver` callback into a per-
/// destination ack slot. The slot's `flushed_through` advances to
/// `to + 1` after each flush.
pub struct FlushAckShim {
    pub dest: Arc<DestAckSlot>,
}

impl FlushObserver for FlushAckShim {
    fn on_flushed(&self, _from: u64, to: u64) {
        self.dest.note_through(to + 1);
    }
}

/// Bridges a Kafka sink's `WriteObserver` callback into a per-
/// destination ack slot. The slot's `flushed_through` advances to
/// `source_offset + 1` after each accepted produce.
pub struct WriteAckShim {
    pub dest: Arc<DestAckSlot>,
}

impl WriteObserver for WriteAckShim {
    fn on_written(&self, source_offset: u64) {
        self.dest.note_through(source_offset + 1);
    }
}

/// Spawn the periodic commit task for one mirror. Returns the
/// `JoinHandle`; callers can drop it (the task self-terminates when
/// `shutdown_rx` flips `true` or the process exits).
///
/// The task is best-effort: it logs and continues on any commit
/// error rather than crashing the supervisor. The next tick retries,
/// and the destination chain's own restart-correctness logic is
/// what protects against lost records — the broker-side committed
/// offset is an *optimisation* (closes the between-pods notify gap
/// on next restart) plus an observability handle, not the durable
/// source of truth.
pub fn spawn_periodic_commit_task(
    handle: KafkaCommitHandle,
    tracker: Arc<AckTracker>,
    interval: Duration,
    mirror_name: String,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if interval.is_zero() {
            tracing::info!(
                mirror = %mirror_name,
                "MIRROR_V3_OFFSET_COMMIT_INTERVAL_MS=0; periodic commit task disabled"
            );
            return;
        }
        let mut iv = tokio::time::interval(interval);
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Consume the immediate tick `tokio::time::interval` fires.
        iv.tick().await;
        let mut last_committed: u64 = 0;
        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::debug!(
                            mirror = %mirror_name,
                            "shutdown requested; periodic commit task exiting"
                        );
                        return;
                    }
                }
                _ = iv.tick() => {
                    let off = tracker.commit_offset();
                    if off == 0 || off == last_committed {
                        continue;
                    }
                    if let Err(e) = handle.commit_through(off) {
                        tracing::warn!(
                            mirror = %mirror_name,
                            offset = off,
                            error = %e,
                            "commit_through failed; will retry next tick"
                        );
                        continue;
                    }
                    if let Err(e) = handle.commit_pending() {
                        tracing::warn!(
                            mirror = %mirror_name,
                            offset = off,
                            error = %e,
                            "commit_pending failed; offset is staged, retry next tick"
                        );
                        continue;
                    }
                    tracing::debug!(
                        mirror = %mirror_name,
                        offset = off,
                        prev = last_committed,
                        "committed source offset"
                    );
                    last_committed = off;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notify_tracker_commit_offset_reflects_note_through() {
        let tracker = AckTracker::new(true, vec![]);
        assert_eq!(tracker.commit_offset(), 0);
        tracker.note_through(5);
        assert_eq!(tracker.commit_offset(), 5);
        tracker.note_through(7);
        assert_eq!(tracker.commit_offset(), 7);
    }

    #[test]
    fn notify_tracker_ignores_regressions() {
        let tracker = AckTracker::new(true, vec![]);
        tracker.note_through(7);
        tracker.note_through(3);
        assert_eq!(
            tracker.commit_offset(),
            7,
            "fetch_max means a lower value cannot regress the slot"
        );
    }

    #[test]
    fn non_notify_tracker_uses_max_destination_ack() {
        let a = Arc::new(DestAckSlot::new("a".into(), true));
        let b = Arc::new(DestAckSlot::new("b".into(), true));
        let tracker = AckTracker::new(false, vec![Arc::clone(&a), Arc::clone(&b)]);
        assert_eq!(tracker.commit_offset(), 0);
        a.note_through(10);
        assert_eq!(tracker.commit_offset(), 10);
        b.note_through(5);
        assert_eq!(tracker.commit_offset(), 10, "max wins");
        b.note_through(20);
        assert_eq!(tracker.commit_offset(), 20);
    }

    #[test]
    fn non_notify_tracker_ignores_ack_sink_note_through() {
        let dest = Arc::new(DestAckSlot::new("d".into(), true));
        let tracker = AckTracker::new(false, vec![Arc::clone(&dest)]);
        // `AckSink::note_through` only feeds the notify slot. A
        // mirror with no notify block has no notify slot, so this
        // call is silently dropped — destinations are the only
        // signal source.
        tracker.note_through(42);
        assert_eq!(tracker.commit_offset(), 0);
        dest.note_through(7);
        assert_eq!(tracker.commit_offset(), 7);
    }

    #[test]
    fn flush_ack_shim_advances_dest_to_to_plus_one() {
        let dest = Arc::new(DestAckSlot::new("fs".into(), true));
        let shim = FlushAckShim {
            dest: Arc::clone(&dest),
        };
        shim.on_flushed(0, 9);
        assert_eq!(dest.flushed_through.load(Ordering::Acquire), 10);
        shim.on_flushed(10, 19);
        assert_eq!(dest.flushed_through.load(Ordering::Acquire), 20);
    }

    #[test]
    fn write_ack_shim_advances_dest_to_offset_plus_one() {
        let dest = Arc::new(DestAckSlot::new("kafka".into(), true));
        let shim = WriteAckShim {
            dest: Arc::clone(&dest),
        };
        for off in 0..5 {
            shim.on_written(off);
        }
        assert_eq!(dest.flushed_through.load(Ordering::Acquire), 5);
    }
}
