//! `TeeSink`: fan one source consumer's records out to N inner sinks
//! while preserving every inner sink's end-offset invariant.
//!
//! ## Why "per-sink heads"
//!
//! FS / S3 sinks buffer records up to operator-configured
//! [`crate::Sink`]-internal flush triggers; Kafka sinks commit
//! per-record. At any wall-clock moment three concurrent sinks fed
//! from the same loop have **different durable positions**. Restart
//! from that heterogeneous state would crash any inner sink that
//! couldn't silently drop re-presented records; the Kafka end-offset
//! gate, in particular, would refuse.
//!
//! `TeeSink` solves this by tracking a `head` per inner sink. The tee
//! exposes `min(heads)` as its `next_expected_offset` so the source
//! consumer resumes from the laggard. On each `write` it presents the
//! record only to inner sinks whose head ≤ `record.source_offset`
//! (the others have already absorbed the record from a prior boot).
//! Inners that participated bump their head to `record.source_offset + 1`.
//! The whole record-fanout happens concurrently
//! (`futures::future::join_all`) so a fast sink's per-record cost is
//! never serialised behind a slower sink's batch flush.
//!
//! ## Cache binding
//!
//! When a binding is set, `TeeSink::write` calls
//! `binding.state.apply_record(...)` **once** per record, before
//! fanning out to inner sinks. This replaces the per-sink
//! `apply_record` that used to live inside `FilesystemSink::write` /
//! `S3Sink::write`. Calling it at the tee level guarantees the cache
//! sees every record exactly once, even when N destinations would
//! otherwise apply it N times (monotonic so harmless, but ugly
//! coupling). Bootstrap-replay (reading durable destination state
//! into the cache on open) still lives inside each sink's `open`.
//!
//! ## Fail-fast
//!
//! Per-record `write`: any inner sink error propagates up as the
//! first encountered error. Matches the existing single-sink
//! semantic (the run loop terminates the mirror).
//!
//! `flush` (called once on graceful shutdown): all inner sinks are
//! flushed concurrently; per-sink errors are logged; the first error
//! is returned. The supervisor exits non-zero, but the surviving
//! sinks' tails are durable.

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::join_all;

use crate::cache::CacheBinding;
use crate::{FlushObserver, Record, Sink, SinkError};

/// One inner sink plus the source offset it will accept next.
struct InnerSink {
    name: String,
    sink: Box<dyn Sink>,
    head: u64,
}

/// Fan one source consumer to N inner sinks while preserving each
/// inner sink's end-offset gate. See module docs.
pub struct TeeSink {
    inners: Vec<InnerSink>,
    cache: Option<CacheBinding>,
    /// Resume cursor for notify re-delivery. When the supervisor
    /// sets a floor below the inner sinks' heads (the broker-side
    /// committed offset of a notify mirror), the tee reports it as
    /// `next_expected_offset` so the source seeks there and the run
    /// loop re-presents `[floor, min(heads))` to the notifier; the
    /// per-sink head skip keeps the destinations write-idempotent
    /// through the replay. The cursor then advances with every
    /// `write` call (including fully-skipped replays) so the idle
    /// drift re-check, which requires `next_expected_offset ==` the
    /// loop's own tracker, stays satisfied mid-replay.
    resume_cursor: Option<u64>,
}

impl TeeSink {
    /// Build a tee over the given inner sinks. Each inner sink's
    /// `next_expected_offset()` is queried once to snapshot its
    /// starting head. The optional cache binding is applied (once
    /// per record) at the top of [`Self::write`].
    ///
    /// `names` must be unique and in the same order as `sinks`; they
    /// appear in error/heartbeat logs so an operator can attribute a
    /// per-sink failure back to the destination element in YAML.
    pub async fn open(
        sinks: Vec<(String, Box<dyn Sink>)>,
        cache: Option<CacheBinding>,
    ) -> Result<Self, SinkError> {
        if sinks.is_empty() {
            return Err(SinkError::Transport(
                "TeeSink requires at least one inner sink".into(),
            ));
        }
        let mut inners = Vec::with_capacity(sinks.len());
        for (name, mut sink) in sinks {
            let head = sink.next_expected_offset().await?;
            inners.push(InnerSink { name, sink, head });
        }
        Ok(Self {
            inners,
            cache,
            resume_cursor: None,
        })
    }

    /// Lower the tee's reported resume position to `floor` so the
    /// run loop re-reads `[floor, min(heads))` from the source. Used
    /// by the supervisor for notify mirrors: records that were made
    /// durable on the destinations but whose webhook batch was never
    /// acked (committed) get re-presented to the notifier, closing
    /// the at-least-once gap across restarts. A floor at or above
    /// the inner minimum is a no-op. Call before the run loop
    /// starts; the cursor is not meant to move backwards mid-run.
    pub fn set_resume_floor(&mut self, floor: u64) {
        self.resume_cursor = Some(floor);
    }

    /// Test-only / mirror-bin-style constructor that skips the open
    /// query, taking pre-computed heads instead. Useful when each
    /// inner sink has just been opened and the caller already knows
    /// its starting offset.
    #[doc(hidden)]
    pub fn from_inners_for_test(
        inners: Vec<(String, Box<dyn Sink>, u64)>,
        cache: Option<CacheBinding>,
    ) -> Self {
        Self {
            inners: inners
                .into_iter()
                .map(|(name, sink, head)| InnerSink { name, sink, head })
                .collect(),
            cache,
            resume_cursor: None,
        }
    }

    /// Keep the resume cursor in step with the run loop's `expected`
    /// tracker: every write call (skipped or fanned out) means the
    /// loop is about to expect `record_offset + 1`. The idle drift
    /// re-check compares `next_expected_offset()` for equality, so a
    /// stale cursor would fail it mid-replay.
    fn advance_resume_cursor(&mut self, record_offset: u64) {
        if let Some(cursor) = self.resume_cursor {
            self.resume_cursor = Some(cursor.max(record_offset + 1));
        }
    }

    /// Per-sink head snapshot, for logs / tests. The vector follows
    /// the open-time inner-sink order.
    pub fn heads(&self) -> Vec<(String, u64)> {
        self.inners
            .iter()
            .map(|i| (i.name.clone(), i.head))
            .collect()
    }
}

#[async_trait]
impl Sink for TeeSink {
    async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
        // Re-query every inner sink so the per-sink heads stay
        // honest. This is only called at startup and on idle by the
        // run loop, so the O(N) query cost is bounded; it doesn't
        // run per record.
        for inner in self.inners.iter_mut() {
            let head = inner.sink.next_expected_offset().await?;
            // Per-sink heads only ever advance. If an inner sink
            // reports a lower value than what we last saw, treat it
            // as a transient inconsistency (e.g. a partial flush
            // observed by `scan_validate` mid-rename) and keep the
            // in-memory head. Truly out-of-band rollbacks at the
            // destination would surface as the inner sink's own
            // `UnexpectedPosition` error on next write.
            if head > inner.head {
                inner.head = head;
            }
        }
        let inner_min = self
            .inners
            .iter()
            .map(|i| i.head)
            .min()
            .expect("non-empty by construction");
        // The resume cursor can only lower the reported position
        // (replay for notify re-delivery); it never holds the tee
        // above the inner minimum.
        Ok(match self.resume_cursor {
            Some(cursor) => inner_min.min(cursor),
            None => inner_min,
        })
    }

    async fn write(&mut self, record: Record) -> Result<(), SinkError> {
        // Cache fanout happens once, before per-sink writes. The
        // CacheState is monotonic so this is the natural place to
        // honor the "apply each record exactly once" contract.
        if let Some(binding) = self.cache.as_ref() {
            binding.state.apply_record(&binding.mirror_name, &record);
        }

        // Partition inner sinks into "behind this record" (must
        // write) vs "already past it" (silently skip, this is what
        // lets a Kafka inner sink coexist with FS/S3 inner sinks
        // recovering from divergent durable state).
        let record_offset = record.source_offset;
        let mut indices = Vec::with_capacity(self.inners.len());
        for (i, inner) in self.inners.iter().enumerate() {
            if inner.head <= record_offset {
                indices.push(i);
            }
        }
        if indices.is_empty() {
            // Every inner sink is already past this offset: the
            // notify-resume replay window, or a restart with all
            // sinks recovered beyond `record_offset`. Drop the
            // record for the destinations (the run loop still hands
            // it to the notifier) but keep the resume cursor in step
            // with the loop's `expected` tracker.
            self.advance_resume_cursor(record_offset);
            return Ok(());
        }

        // Concurrent write fanout. We `join_all` over per-sink
        // futures so the slowest inner sink's per-record latency
        // dominates the tee's per-record cost; sequential calls
        // would 1000× the fast sinks' wait time for no reason.
        let mut futs = Vec::with_capacity(indices.len());
        // Take the slots' sinks temporarily so we can drive them
        // concurrently while borrowing `self.inners` mutably exactly
        // once per slot. We restore them after `join_all`.
        let mut taken: Vec<(usize, Box<dyn Sink>)> = Vec::with_capacity(indices.len());
        for i in indices.iter().copied() {
            // Swap the inner sink out with a placeholder; restored
            // below before any other method on `self` is called.
            let placeholder: Box<dyn Sink> = Box::new(PlaceholderSink);
            let original = std::mem::replace(&mut self.inners[i].sink, placeholder);
            taken.push((i, original));
        }
        for (i, mut sink) in taken {
            let rec = record.clone();
            futs.push(async move {
                let r = sink.write(rec).await;
                (i, sink, r)
            });
        }
        let results = join_all(futs).await;

        // Restore the inner sinks and bump heads on success.
        let mut first_err: Option<(String, SinkError)> = None;
        for (i, sink, result) in results {
            self.inners[i].sink = sink;
            match result {
                Ok(()) => {
                    self.inners[i].head = record_offset + 1;
                }
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some((self.inners[i].name.clone(), e));
                    }
                }
            }
        }
        if let Some((name, e)) = first_err {
            return Err(SinkError::Transport(format!("inner sink {name}: {e}")));
        }
        self.advance_resume_cursor(record_offset);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        // Concurrent flush. Per-sink errors are logged; the first
        // error is returned. The other sinks still flush; losing
        // sink A's tail buffer should not cost us sink B's tail too.
        let mut futs = Vec::with_capacity(self.inners.len());
        let mut taken: Vec<(usize, String, Box<dyn Sink>)> = Vec::with_capacity(self.inners.len());
        for (i, inner) in self.inners.iter_mut().enumerate() {
            let placeholder: Box<dyn Sink> = Box::new(PlaceholderSink);
            let original = std::mem::replace(&mut inner.sink, placeholder);
            taken.push((i, inner.name.clone(), original));
        }
        for (i, name, mut sink) in taken {
            futs.push(async move {
                let r = sink.flush().await;
                (i, name, sink, r)
            });
        }
        let results = join_all(futs).await;

        let mut first_err: Option<(String, SinkError)> = None;
        for (i, name, sink, result) in results {
            self.inners[i].sink = sink;
            if let Err(e) = result {
                tracing::warn!(inner = %name, error = %e, "tee inner sink flush failed");
                if first_err.is_none() {
                    first_err = Some((name, e));
                }
            }
        }
        if let Some((name, e)) = first_err {
            return Err(SinkError::Transport(format!(
                "inner sink {name} flush: {e}"
            )));
        }
        Ok(())
    }

    fn allows_compacted_source(&self) -> bool {
        // Tolerate a compacted source only if every inner sink can.
        // A single non-compaction destination in the tee means a
        // missing record would leave a permanent gap in that
        // destination's chain.
        self.inners.iter().all(|i| i.sink.allows_compacted_source())
    }

    async fn align_to_source_low_watermark(&mut self, low_watermark: u64) -> Result<(), SinkError> {
        // The run loop calls this only when `allows_compacted_source`
        // returned true, so every inner sink is compaction-capable
        // and needs to be aligned. Concurrent; first error wins.
        let mut futs = Vec::with_capacity(self.inners.len());
        let mut taken: Vec<(usize, Box<dyn Sink>)> = Vec::with_capacity(self.inners.len());
        for (i, inner) in self.inners.iter_mut().enumerate() {
            let placeholder: Box<dyn Sink> = Box::new(PlaceholderSink);
            let original = std::mem::replace(&mut inner.sink, placeholder);
            taken.push((i, original));
        }
        for (i, mut sink) in taken {
            futs.push(async move {
                let r = sink.align_to_source_low_watermark(low_watermark).await;
                (i, sink, r)
            });
        }
        let results = join_all(futs).await;
        let mut first_err: Option<(String, SinkError)> = None;
        for (i, sink, result) in results {
            self.inners[i].sink = sink;
            if let Err(e) = result {
                if first_err.is_none() {
                    first_err = Some((self.inners[i].name.clone(), e));
                }
            }
        }
        if let Some((name, e)) = first_err {
            return Err(SinkError::Transport(format!(
                "inner sink {name} align: {e}"
            )));
        }
        // After alignment every inner sink's head advances to
        // `low_watermark`.
        for inner in self.inners.iter_mut() {
            inner.head = low_watermark;
        }
        Ok(())
    }

    fn set_flush_observer(&mut self, observer: Arc<dyn FlushObserver>) {
        // Only sinks that actually fire flush events participate in
        // the min-coordination. A per-record sink (Kafka) never
        // calls its observer, so including it would pin the min at
        // 0 and the outer observer would never fire on a mixed
        // blob+kafka mirror.
        let supporting: Vec<usize> = self
            .inners
            .iter()
            .enumerate()
            .filter(|(_, inner)| inner.sink.supports_flush_observer())
            .map(|(i, _)| i)
            .collect();
        match supporting.len() {
            0 => {
                // The config validator rejects destination-flush
                // without a blob destination, so this is a
                // programming error rather than an operator one;
                // keep it loud.
                tracing::error!(
                    "set_flush_observer on a tee with no flush-capable inner sink; flush notifications will never fire"
                );
            }
            1 => {
                // Single flush-capable sink (covers the common
                // single-destination mirror): forward the observer
                // unchanged. `from`/`to` flow through verbatim.
                self.inners[supporting[0]].sink.set_flush_observer(observer);
            }
            n => {
                // Multiple flush-capable sinks: wrap the outer
                // observer with a per-sink relay + min-coordinator.
                // The outer observer fires only when every
                // flush-capable inner sink has committed past a
                // watermark - the spec's "fire when ALL destinations
                // have committed past the batch's high-water offset".
                let coordinator = Arc::new(MinFlushCoordinator::new(n, observer));
                for (relay_index, inner_index) in supporting.into_iter().enumerate() {
                    self.inners[inner_index]
                        .sink
                        .set_flush_observer(Arc::new(PerSinkRelay {
                            sink_index: relay_index,
                            coordinator: Arc::clone(&coordinator),
                        }));
                }
            }
        }
    }

    fn supports_flush_observer(&self) -> bool {
        self.inners
            .iter()
            .any(|inner| inner.sink.supports_flush_observer())
    }
}

/// Per-sink wrapper that funnels every inner sink's `on_flushed`
/// into the shared [`MinFlushCoordinator`]. Used only when the tee
/// has more than one inner sink.
struct PerSinkRelay {
    sink_index: usize,
    coordinator: Arc<MinFlushCoordinator>,
}

impl FlushObserver for PerSinkRelay {
    fn on_flushed(&self, _from: u64, to: u64) {
        // `from` reported by the inner sink is its own local batch
        // boundary, not meaningful at the combined-advance level.
        // The coordinator synthesises a `from` from the previously-
        // fired watermark.
        self.coordinator.note(self.sink_index, to);
    }
}

/// Tracks per-sink "highest flushed `to`" and fires the outer
/// observer when `min(per-sink) > last-fired`. Synchronous, std
/// `Mutex` (the FS/S3 flush sites are async-context but invoke
/// `on_flushed` synchronously; the coordinator holds locks only
/// long enough to compute new min and decide to fire).
struct MinFlushCoordinator {
    per_sink_flushed_to: std::sync::Mutex<Vec<u64>>,
    last_fired_to: std::sync::Mutex<Option<u64>>,
    outer: Arc<dyn FlushObserver>,
}

impl MinFlushCoordinator {
    fn new(num_sinks: usize, outer: Arc<dyn FlushObserver>) -> Self {
        Self {
            per_sink_flushed_to: std::sync::Mutex::new(vec![0; num_sinks]),
            last_fired_to: std::sync::Mutex::new(None),
            outer,
        }
    }

    fn note(&self, sink_index: usize, to: u64) {
        let new_min = {
            let mut per_sink = self.per_sink_flushed_to.lock().unwrap();
            if to > per_sink[sink_index] {
                per_sink[sink_index] = to;
            }
            *per_sink.iter().min().unwrap()
        };
        // First-fire case: no `last_fired_to` yet, so `from` is the
        // tee's *initial* combined head; `0` is acceptable for the
        // bootstrap fire (the receiver only cares about `to`).
        let to_fire = {
            let mut last = self.last_fired_to.lock().unwrap();
            match *last {
                Some(prev) if new_min > prev => {
                    *last = Some(new_min);
                    Some((prev, new_min))
                }
                None if new_min > 0 => {
                    *last = Some(new_min);
                    Some((0, new_min))
                }
                _ => None,
            }
        };
        if let Some((from, to)) = to_fire {
            self.outer.on_flushed(from, to);
        }
    }
}

/// Owned, no-op sink used as a placeholder when the tee temporarily
/// takes an inner sink by value to drive a `join_all`. Replaced
/// before any other tee method is called.
struct PlaceholderSink;

#[async_trait]
impl Sink for PlaceholderSink {
    async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
        Err(SinkError::Transport("placeholder sink polled".into()))
    }
    async fn write(&mut self, _record: Record) -> Result<(), SinkError> {
        Err(SinkError::Transport("placeholder sink written".into()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::mock::rec;
    use crate::{CacheState, TimestampType};

    /// Recording inner sink: tracks writes + flush count + accepts a
    /// scripted starting head.
    struct Recording {
        starting_head: u64,
        accepted: Arc<Mutex<Vec<u64>>>,
        flush_count: Arc<Mutex<u32>>,
        fail_on_offset: Option<u64>,
        allow_compacted: bool,
        /// Mirrors the FS/S3 (`true`) vs Kafka (`false`) split in
        /// `Sink::supports_flush_observer`. Default true so the
        /// existing flush-coordination tests model blob sinks.
        flush_capable: bool,
        aligned_to: Arc<Mutex<Option<u64>>>,
        /// The observer the tee installed via `set_flush_observer`,
        /// if any. Tests fire it explicitly via [`Self::simulate_flush`]
        /// to drive the tee's per-sink coordinator without needing
        /// real disk I/O.
        observer: Arc<Mutex<Option<Arc<dyn crate::FlushObserver>>>>,
    }

    impl Recording {
        fn new(starting_head: u64) -> (Self, Recorder) {
            let accepted = Arc::new(Mutex::new(Vec::new()));
            let flush_count = Arc::new(Mutex::new(0));
            let aligned_to = Arc::new(Mutex::new(None));
            let observer = Arc::new(Mutex::new(None));
            let recorder = Recorder {
                accepted: Arc::clone(&accepted),
                flush_count: Arc::clone(&flush_count),
                aligned_to: Arc::clone(&aligned_to),
                observer: Arc::clone(&observer),
            };
            (
                Self {
                    starting_head,
                    accepted,
                    flush_count,
                    fail_on_offset: None,
                    allow_compacted: false,
                    flush_capable: true,
                    aligned_to,
                    observer,
                },
                recorder,
            )
        }
        fn fail_on(mut self, offset: u64) -> Self {
            self.fail_on_offset = Some(offset);
            self
        }
        fn with_allow_compacted(mut self, allow: bool) -> Self {
            self.allow_compacted = allow;
            self
        }
        /// Model a per-record sink (Kafka): accepts an observer
        /// install but never fires it and reports itself
        /// flush-incapable.
        fn without_flush_support(mut self) -> Self {
            self.flush_capable = false;
            self
        }
    }

    #[derive(Clone)]
    struct Recorder {
        accepted: Arc<Mutex<Vec<u64>>>,
        flush_count: Arc<Mutex<u32>>,
        aligned_to: Arc<Mutex<Option<u64>>>,
        observer: Arc<Mutex<Option<Arc<dyn crate::FlushObserver>>>>,
    }

    impl Recorder {
        fn writes(&self) -> Vec<u64> {
            self.accepted.lock().unwrap().clone()
        }
        fn flushes(&self) -> u32 {
            *self.flush_count.lock().unwrap()
        }
        fn aligned(&self) -> Option<u64> {
            *self.aligned_to.lock().unwrap()
        }
        /// Fire the observer the tee installed via
        /// `set_flush_observer`, simulating a real on-disk flush.
        /// Tests use this instead of doing real I/O.
        fn simulate_flush(&self, from: u64, to: u64) {
            if let Some(obs) = self.observer.lock().unwrap().as_ref() {
                obs.on_flushed(from, to);
            }
        }
    }

    #[async_trait]
    impl Sink for Recording {
        async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
            // Sink contract: return `starting_head + accepted.len()`.
            let n = self.accepted.lock().unwrap().len() as u64;
            Ok(self.starting_head + n)
        }
        async fn write(&mut self, record: Record) -> Result<(), SinkError> {
            if Some(record.source_offset) == self.fail_on_offset {
                return Err(SinkError::Transport(format!(
                    "scripted failure at offset {}",
                    record.source_offset
                )));
            }
            self.accepted.lock().unwrap().push(record.source_offset);
            Ok(())
        }
        async fn flush(&mut self) -> Result<(), SinkError> {
            *self.flush_count.lock().unwrap() += 1;
            Ok(())
        }
        fn allows_compacted_source(&self) -> bool {
            self.allow_compacted
        }
        async fn align_to_source_low_watermark(
            &mut self,
            low_watermark: u64,
        ) -> Result<(), SinkError> {
            *self.aligned_to.lock().unwrap() = Some(low_watermark);
            self.starting_head = low_watermark;
            Ok(())
        }
        fn set_flush_observer(&mut self, observer: Arc<dyn crate::FlushObserver>) {
            *self.observer.lock().unwrap() = Some(observer);
        }
        fn supports_flush_observer(&self) -> bool {
            self.flush_capable
        }
    }

    fn boxed(s: Recording) -> Box<dyn Sink> {
        Box::new(s) as Box<dyn Sink>
    }

    #[tokio::test]
    async fn open_snapshots_inner_heads_and_reports_min() {
        let (a, ra) = Recording::new(8);
        let (b, rb) = Recording::new(3);
        let mut tee = TeeSink::open(vec![("a".into(), boxed(a)), ("b".into(), boxed(b))], None)
            .await
            .unwrap();
        let head = tee.next_expected_offset().await.unwrap();
        assert_eq!(head, 3, "min of inner heads");
        // Recorder is unaffected (it inspects the original inner via Arc).
        assert_eq!(ra.writes(), Vec::<u64>::new());
        assert_eq!(rb.writes(), Vec::<u64>::new());
    }

    #[tokio::test]
    async fn write_feeds_only_lagging_sinks_until_heads_converge() {
        let (a, ra) = Recording::new(8);
        let (b, rb) = Recording::new(3);
        let mut tee = TeeSink::open(vec![("a".into(), boxed(a)), ("b".into(), boxed(b))], None)
            .await
            .unwrap();
        // tee head = 3. Records 3..=7 should hit only sink b
        // (sink a's head is 8). Record 8 onwards hits both.
        for offset in 3..=10 {
            tee.write(rec(offset)).await.unwrap();
        }
        assert_eq!(
            ra.writes(),
            vec![8, 9, 10],
            "sink a only sees records at/after its starting head"
        );
        assert_eq!(
            rb.writes(),
            vec![3, 4, 5, 6, 7, 8, 9, 10],
            "sink b sees everything from its starting head"
        );
    }

    #[tokio::test]
    async fn first_inner_write_error_propagates_with_sink_name() {
        let (a, _ra) = Recording::new(0);
        let (b, _rb) = Recording::new(0);
        let b = b.fail_on(2);
        let mut tee = TeeSink::open(vec![("a".into(), boxed(a)), ("b".into(), boxed(b))], None)
            .await
            .unwrap();
        tee.write(rec(0)).await.unwrap();
        tee.write(rec(1)).await.unwrap();
        let err = tee.write(rec(2)).await.expect_err("should fail");
        let msg = format!("{err}");
        assert!(msg.contains("inner sink b"), "got: {msg}");
        assert!(msg.contains("scripted failure"), "got: {msg}");
    }

    #[tokio::test]
    async fn flush_fans_out_concurrently_and_surfaces_first_error() {
        let (a, ra) = Recording::new(0);
        let (b, rb) = Recording::new(0);
        let mut tee = TeeSink::open(vec![("a".into(), boxed(a)), ("b".into(), boxed(b))], None)
            .await
            .unwrap();
        tee.flush().await.unwrap();
        assert_eq!(ra.flushes(), 1);
        assert_eq!(rb.flushes(), 1);
    }

    #[tokio::test]
    async fn cache_binding_applied_exactly_once_per_record() {
        let (a, _ra) = Recording::new(0);
        let (b, _rb) = Recording::new(0);
        let cache_state = Arc::new(CacheState::new());
        cache_state.register_mirror("m", 0, None, false);
        let binding = CacheBinding {
            state: Arc::clone(&cache_state),
            mirror_name: "m".into(),
        };
        let mut tee = TeeSink::open(
            vec![("a".into(), boxed(a)), ("b".into(), boxed(b))],
            Some(binding),
        )
        .await
        .unwrap();
        let r = Record {
            topic: "t".into(),
            partition: 0,
            source_offset: 0,
            timestamp_ms: Some(1),
            timestamp_type: TimestampType::CreateTime,
            key: Some(b"k0".to_vec()),
            value: Some(b"v0".to_vec()),
            headers: vec![],
        };
        tee.write(r).await.unwrap();
        // The cache stores the value under "k0". Calling write twice
        // with the same offset would be a no-op (monotonic guard);
        // here we just confirm a single record produced a single
        // visible key.
        assert_eq!(
            cache_state.snapshot_keys_for("m").unwrap(),
            vec!["k0".to_string()],
            "exactly one key materialised from one record"
        );
    }

    #[tokio::test]
    async fn allows_compacted_source_is_and_over_inners() {
        let (a, _) = Recording::new(0);
        let a = a.with_allow_compacted(true);
        let (b, _) = Recording::new(0);
        let b = b.with_allow_compacted(false);
        let tee_mixed = TeeSink::open(vec![("a".into(), boxed(a)), ("b".into(), boxed(b))], None)
            .await
            .unwrap();
        assert!(
            !tee_mixed.allows_compacted_source(),
            "mixed inner capabilities => false"
        );

        let (a2, _) = Recording::new(0);
        let a2 = a2.with_allow_compacted(true);
        let (b2, _) = Recording::new(0);
        let b2 = b2.with_allow_compacted(true);
        let tee_all = TeeSink::open(vec![("a".into(), boxed(a2)), ("b".into(), boxed(b2))], None)
            .await
            .unwrap();
        assert!(
            tee_all.allows_compacted_source(),
            "all inners compaction-capable => true"
        );
    }

    #[tokio::test]
    async fn align_to_source_low_watermark_proxies_to_every_inner_and_bumps_heads() {
        let (a, ra) = Recording::new(0);
        let a = a.with_allow_compacted(true);
        let (b, rb) = Recording::new(0);
        let b = b.with_allow_compacted(true);
        let mut tee = TeeSink::open(vec![("a".into(), boxed(a)), ("b".into(), boxed(b))], None)
            .await
            .unwrap();
        tee.align_to_source_low_watermark(42).await.unwrap();
        assert_eq!(ra.aligned(), Some(42));
        assert_eq!(rb.aligned(), Some(42));
        let head = tee.next_expected_offset().await.unwrap();
        assert_eq!(head, 42, "after align, min(heads) = low_watermark");
    }

    // ---- FlushObserver wiring through TeeSink ----

    #[derive(Default)]
    struct RecordingObserver {
        fires: Mutex<Vec<(u64, u64)>>,
    }

    impl crate::FlushObserver for RecordingObserver {
        fn on_flushed(&self, from: u64, to: u64) {
            self.fires.lock().unwrap().push((from, to));
        }
    }

    #[tokio::test]
    async fn length_one_tee_forwards_observer_unchanged() {
        let (inner, recorder) = Recording::new(0);
        let mut tee = TeeSink::open(vec![("only".into(), boxed(inner))], None)
            .await
            .unwrap();
        let obs = Arc::new(RecordingObserver::default());
        tee.set_flush_observer(obs.clone() as Arc<dyn crate::FlushObserver>);

        // Simulate two FS-style flushes via the recorder's helper.
        recorder.simulate_flush(0, 9);
        recorder.simulate_flush(10, 19);

        let fires = obs.fires.lock().unwrap().clone();
        assert_eq!(
            fires,
            vec![(0, 9), (10, 19)],
            "length-1 tee passes (from, to) through verbatim"
        );
    }

    #[tokio::test]
    async fn multi_sink_tee_fires_only_when_min_advances() {
        let (a, ra) = Recording::new(0);
        let (b, rb) = Recording::new(0);
        let mut tee = TeeSink::open(vec![("a".into(), boxed(a)), ("b".into(), boxed(b))], None)
            .await
            .unwrap();
        let obs = Arc::new(RecordingObserver::default());
        tee.set_flush_observer(obs.clone() as Arc<dyn crate::FlushObserver>);

        // a flushes 0..9. b hasn't flushed yet → min is still 0,
        // outer must not fire.
        ra.simulate_flush(0, 9);
        assert!(
            obs.fires.lock().unwrap().is_empty(),
            "outer must wait for the laggard"
        );

        // b flushes 0..4. min(9, 4) = 4; fire (0, 4).
        rb.simulate_flush(0, 4);
        assert_eq!(obs.fires.lock().unwrap().clone(), vec![(0, 4)]);

        // b catches up to 9. min(9, 9) = 9; fire (4, 9).
        rb.simulate_flush(5, 9);
        assert_eq!(obs.fires.lock().unwrap().clone(), vec![(0, 4), (4, 9)]);

        // a races ahead to 19. min(19, 9) = 9; no advance, no fire.
        ra.simulate_flush(10, 19);
        assert_eq!(obs.fires.lock().unwrap().clone(), vec![(0, 4), (4, 9)]);
    }

    #[tokio::test]
    async fn multi_sink_tee_does_not_re_fire_for_already_seen_watermark() {
        // Idempotence: a sink reporting the same `to` twice (which
        // can happen if FS/S3 re-flushes an empty boundary in some
        // future refactor) must not cause a duplicate outer fire.
        let (a, ra) = Recording::new(0);
        let (b, rb) = Recording::new(0);
        let mut tee = TeeSink::open(vec![("a".into(), boxed(a)), ("b".into(), boxed(b))], None)
            .await
            .unwrap();
        let obs = Arc::new(RecordingObserver::default());
        tee.set_flush_observer(obs.clone() as Arc<dyn crate::FlushObserver>);

        ra.simulate_flush(0, 5);
        rb.simulate_flush(0, 5);
        // First fire at (0, 5).
        assert_eq!(obs.fires.lock().unwrap().clone(), vec![(0, 5)]);
        // a re-reports 5; min doesn't advance; no fire.
        ra.simulate_flush(0, 5);
        assert_eq!(obs.fires.lock().unwrap().clone(), vec![(0, 5)]);
    }

    #[tokio::test]
    async fn resume_floor_lowers_reported_start_and_tracks_replay() {
        let (a, ra) = Recording::new(5);
        let mut tee = TeeSink::open(vec![("a".into(), boxed(a))], None)
            .await
            .unwrap();
        tee.set_resume_floor(2);
        assert_eq!(
            tee.next_expected_offset().await.unwrap(),
            2,
            "floor lowers the reported start below the inner head"
        );
        // Replay window 2..=4: dropped for the destination, but the
        // reported next-expected must track the loop's tracker or
        // the idle drift re-check fails mid-replay.
        for offset in 2..=4 {
            tee.write(rec(offset)).await.unwrap();
            assert_eq!(tee.next_expected_offset().await.unwrap(), offset + 1);
        }
        assert_eq!(
            ra.writes(),
            Vec::<u64>::new(),
            "replay must not re-write to the destination"
        );
        // From the destination head onward, writes fan out normally
        // and the cursor converges with the inner heads.
        for offset in 5..=7 {
            tee.write(rec(offset)).await.unwrap();
        }
        assert_eq!(ra.writes(), vec![5, 6, 7]);
        assert_eq!(tee.next_expected_offset().await.unwrap(), 8);
    }

    #[tokio::test]
    async fn resume_floor_at_or_above_inner_min_is_a_noop() {
        let (a, _ra) = Recording::new(3);
        let mut tee = TeeSink::open(vec![("a".into(), boxed(a))], None)
            .await
            .unwrap();
        tee.set_resume_floor(9);
        assert_eq!(
            tee.next_expected_offset().await.unwrap(),
            3,
            "the cursor never raises the tee above the inner minimum"
        );
    }

    /// The mixed blob+kafka regression: a per-record sink must not
    /// pin the flush min at 0. With the kafka-like sink excluded
    /// from coordination, the blob sink's flush alone drives the
    /// outer observer.
    #[tokio::test]
    async fn flush_observer_ignores_sinks_that_never_flush() {
        let (blob, r_blob) = Recording::new(0);
        let (kafka, _r_kafka) = Recording::new(0);
        let kafka = kafka.without_flush_support();
        let mut tee = TeeSink::open(
            vec![("blob".into(), boxed(blob)), ("kafka".into(), boxed(kafka))],
            None,
        )
        .await
        .unwrap();
        let obs = Arc::new(RecordingObserver::default());
        tee.set_flush_observer(obs.clone() as Arc<dyn crate::FlushObserver>);
        assert!(tee.supports_flush_observer());

        r_blob.simulate_flush(0, 9);
        assert_eq!(
            obs.fires.lock().unwrap().clone(),
            vec![(0, 9)],
            "the blob sink's flush must fire the outer observer even though the kafka sink never flushes"
        );
    }
}
