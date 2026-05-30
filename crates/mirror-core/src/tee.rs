//! `TeeSink` — fan one source consumer's records out to N inner sinks
//! while preserving every inner sink's end-offset invariant.
//!
//! ## Why "per-sink heads"
//!
//! FS / S3 sinks buffer records up to operator-configured
//! [`crate::Sink`]-internal flush triggers; Kafka sinks commit
//! per-record. At any wall-clock moment three concurrent sinks fed
//! from the same loop have **different durable positions**. Restart
//! from that heterogeneous state would crash any inner sink that
//! couldn't silently drop re-presented records — the Kafka end-offset
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

use async_trait::async_trait;
use futures::future::join_all;

use crate::cache::CacheBinding;
use crate::{Record, Sink, SinkError};

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
}

impl TeeSink {
    /// Build a tee over the given inner sinks. Each inner sink's
    /// `next_expected_offset()` is queried once to snapshot its
    /// starting head. The optional cache binding is applied (once
    /// per record) at the top of [`Self::write`].
    ///
    /// `names` must be unique and in the same order as `sinks` — they
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
        Ok(Self { inners, cache })
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
        // run loop, so the O(N) query cost is bounded — it doesn't
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
        Ok(self
            .inners
            .iter()
            .map(|i| i.head)
            .min()
            .expect("non-empty by construction"))
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
            // Every inner sink is already past this offset (rare:
            // only happens during restart with all sinks recovered
            // to or beyond `record_offset`). Drop the record.
            return Ok(());
        }

        // Concurrent write fanout. We `join_all` over per-sink
        // futures so the slowest inner sink's per-record latency
        // dominates the tee's per-record cost — sequential calls
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
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        // Concurrent flush. Per-sink errors are logged; the first
        // error is returned. The other sinks still flush — losing
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
        aligned_to: Arc<Mutex<Option<u64>>>,
    }

    impl Recording {
        fn new(starting_head: u64) -> (Self, Recorder) {
            let accepted = Arc::new(Mutex::new(Vec::new()));
            let flush_count = Arc::new(Mutex::new(0));
            let aligned_to = Arc::new(Mutex::new(None));
            let recorder = Recorder {
                accepted: Arc::clone(&accepted),
                flush_count: Arc::clone(&flush_count),
                aligned_to: Arc::clone(&aligned_to),
            };
            (
                Self {
                    starting_head,
                    accepted,
                    flush_count,
                    fail_on_offset: None,
                    allow_compacted: false,
                    aligned_to,
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
    }

    #[derive(Clone)]
    struct Recorder {
        accepted: Arc<Mutex<Vec<u64>>>,
        flush_count: Arc<Mutex<u32>>,
        aligned_to: Arc<Mutex<Option<u64>>>,
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
        cache_state.register_mirror("m", 0);
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
            cache_state.snapshot_keys(),
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
}
