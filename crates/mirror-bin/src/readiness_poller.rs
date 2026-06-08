//! Per-mirror readiness poller.
//!
//! The supervisor spawns one of these per registered mirror at
//! startup. Every `MIRROR_V3_READINESS_POLL_MS` (default 2 s) the
//! task:
//!
//!   1. Fetches the source partition's high-watermark via
//!      `mirror_kafka::fetch_high_watermark` (cheap; one
//!      `BaseConsumer` per call) and pushes it into
//!      `CacheState::set_broker_end_offset`. The cache's status
//!      predicate then recomputes lag = end_offset - last_applied.
//!   2. Reads the run loop's consumer assignment via the shared
//!      `KafkaCommitHandle`. If `(topic, partition)` is no longer
//!      assigned, calls `CacheState::mark_source_unassigned`; if it
//!      reappears, calls `mark_source_assigned`.
//!
//! The task is best-effort: a transient fetch error logs and
//! continues. It exits when the supervisor's shutdown signal flips.

use std::sync::Arc;
use std::time::Duration;

use mirror_core::CacheState;
use mirror_kafka::KafkaCommitHandle;
use tokio::sync::watch;

const DEFAULT_READINESS_POLL: Duration = Duration::from_secs(2);

/// Read the poll interval from `MIRROR_V3_READINESS_POLL_MS`,
/// falling back to [`DEFAULT_READINESS_POLL`]. A value of `0`
/// disables the poller.
pub fn readiness_poll_interval_from_env() -> Duration {
    match std::env::var("MIRROR_V3_READINESS_POLL_MS").ok().as_deref() {
        Some(s) => match s.parse::<u64>() {
            Ok(ms) => Duration::from_millis(ms),
            Err(_) => DEFAULT_READINESS_POLL,
        },
        None => DEFAULT_READINESS_POLL,
    }
}

/// Read the lag tolerance from `MIRROR_V3_READINESS_LAG`, falling
/// back to `0` (any positive lag fires `LagBehindSource`).
pub fn readiness_lag_tolerance_from_env() -> u64 {
    std::env::var("MIRROR_V3_READINESS_LAG")
        .ok()
        .as_deref()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

pub struct PollSpec {
    pub mirror_name: String,
    pub bootstrap_servers: String,
    pub topic: String,
    pub partition: i32,
    pub commit_handle: KafkaCommitHandle,
    pub cache: Arc<CacheState>,
}

/// Spawn the readiness poller for one mirror. Returns the
/// `JoinHandle`; callers can drop it (the task self-terminates when
/// the shutdown signal flips).
pub fn spawn_readiness_poller(
    spec: PollSpec,
    interval: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if interval.is_zero() {
            tracing::info!(
                mirror = %spec.mirror_name,
                "MIRROR_V3_READINESS_POLL_MS=0; readiness poller disabled"
            );
            return;
        }
        let mut iv = tokio::time::interval(interval);
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Consume the immediate tick `tokio::time::interval` fires.
        iv.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::debug!(
                            mirror = %spec.mirror_name,
                            "shutdown; readiness poller exiting"
                        );
                        return;
                    }
                }
                _ = iv.tick() => {
                    // Step 1: source HWM
                    let bootstrap = spec.bootstrap_servers.clone();
                    let topic = spec.topic.clone();
                    let partition = spec.partition;
                    let hwm_result = tokio::task::spawn_blocking(move || {
                        mirror_kafka::fetch_high_watermark(
                            &bootstrap,
                            &topic,
                            partition,
                            Duration::from_secs(5),
                        )
                    })
                    .await;
                    match hwm_result {
                        Ok(Ok(hwm)) => {
                            spec.cache
                                .set_broker_end_offset(&spec.mirror_name, hwm.max(0) as u64);
                        }
                        Ok(Err(e)) => {
                            tracing::warn!(
                                mirror = %spec.mirror_name,
                                error = %e,
                                "readiness poller: fetch_high_watermark failed; will retry"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                mirror = %spec.mirror_name,
                                error = %e,
                                "readiness poller: hwm join failed"
                            );
                        }
                    }

                    // Step 2: assignment check
                    match spec.commit_handle.current_assignment_includes() {
                        Ok(true) => {
                            spec.cache.mark_source_assigned(&spec.mirror_name);
                        }
                        Ok(false) => {
                            tracing::warn!(
                                mirror = %spec.mirror_name,
                                topic = %spec.topic,
                                partition = spec.partition,
                                "readiness poller: source partition is no longer assigned"
                            );
                            spec.cache.mark_source_unassigned(&spec.mirror_name);
                        }
                        Err(e) => {
                            tracing::warn!(
                                mirror = %spec.mirror_name,
                                error = %e,
                                "readiness poller: assignment check failed"
                            );
                        }
                    }
                }
            }
        }
    })
}
