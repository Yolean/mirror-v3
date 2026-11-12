//! Per-mirror readiness poller.
//!
//! The supervisor spawns one of these per registered mirror at
//! startup. Every `MIRROR_V3_READINESS_POLL_MS` (default 2 s) the
//! task:
//!
//!   1. Fetches the source partition's high-watermark through one
//!      `mirror_kafka::WatermarkClient`, created at the first tick and
//!      kept for the poller's life (a librdkafka client per tick
//!      retains heap at every create/destroy cycle), and pushes it into
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
use mirror_kafka::{KafkaCommitHandle, WatermarkClient};
use tokio::sync::watch;

/// How long one high-watermark query may wait for the broker.
const HWM_TIMEOUT: Duration = Duration::from_secs(5);

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
        let mut hwm = HwmProbe::new(
            spec.bootstrap_servers.clone(),
            spec.topic.clone(),
            spec.partition,
            HWM_TIMEOUT,
        );
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
                    match hwm.fetch().await {
                        Ok(hwm) => {
                            spec.cache
                                .set_broker_end_offset(&spec.mirror_name, hwm);
                            // Lag is a metric, not an HTTP status (readiness
                            // is sticky once caught up).
                            if let Some(s) = spec
                                .cache
                                .status_snapshot()
                                .into_iter()
                                .find(|s| s.name == spec.mirror_name)
                            {
                                metrics::gauge!(
                                    "mirror_v3_source_lag_offsets",
                                    "topic" => spec.topic.clone(),
                                    "partition" => spec.partition.to_string(),
                                    "mirror" => spec.mirror_name.clone(),
                                )
                                .set(s.broker_end_offset.saturating_sub(s.last_applied_offset) as f64);
                            }
                        }
                        Err(HwmError::Kafka(e)) => {
                            tracing::warn!(
                                mirror = %spec.mirror_name,
                                error = %e,
                                "readiness poller: fetch_high_watermark failed; will retry"
                            );
                        }
                        Err(HwmError::Join(e)) => {
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

enum HwmError {
    Kafka(mirror_kafka::KafkaError),
    Join(tokio::task::JoinError),
}

/// The source partition's high watermark, asked through one
/// [`WatermarkClient`]. The client is created at the first fetch, and a
/// failed creation is tried again at the next. A failed query keeps the
/// client: librdkafka reconnects by itself, and a new client per failure
/// would bring back the per-client heap growth while the broker is away.
struct HwmProbe {
    bootstrap: String,
    topic: String,
    partition: i32,
    timeout: Duration,
    client: Option<Arc<WatermarkClient>>,
    clients_created: u64,
}

impl HwmProbe {
    fn new(bootstrap: String, topic: String, partition: i32, timeout: Duration) -> Self {
        Self {
            bootstrap,
            topic,
            partition,
            timeout,
            client: None,
            clients_created: 0,
        }
    }

    async fn fetch(&mut self) -> Result<u64, HwmError> {
        let client = match &self.client {
            Some(client) => Arc::clone(client),
            None => {
                let client =
                    Arc::new(WatermarkClient::new(&self.bootstrap).map_err(HwmError::Kafka)?);
                self.clients_created += 1;
                self.client = Some(Arc::clone(&client));
                client
            }
        };
        let topic = self.topic.clone();
        let partition = self.partition;
        let timeout = self.timeout;
        tokio::task::spawn_blocking(move || client.high_watermark(&topic, partition, timeout))
            .await
            .map_err(HwmError::Join)?
            .map_err(HwmError::Kafka)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every tick asks through the same client, also while the broker is
    /// out of reach.
    #[tokio::test]
    async fn the_probe_keeps_one_client_across_failed_queries() {
        // Port 1 on loopback refuses connections.
        let mut probe = HwmProbe::new(
            "127.0.0.1:1".to_string(),
            "t".to_string(),
            0,
            Duration::from_millis(50),
        );
        for _ in 0..3 {
            assert!(matches!(probe.fetch().await, Err(HwmError::Kafka(_))));
        }
        assert_eq!(probe.clients_created, 1);
    }
}
