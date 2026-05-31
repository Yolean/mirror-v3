//! Regression test for `KafkaSource::low_watermark`'s contract:
//! returns the broker's *actual* low watermark when called on a
//! freshly-opened source, before any `seek` / `poll_one` has run.
//!
//! ## Why this exists
//!
//! The first implementation called `fetch_watermarks` directly on
//! the underlying `StreamConsumer`. That worked in the local
//! Redpanda test rig (single broker, immediate metadata) but
//! returned `Ok((0, 0))` in production against a real multi-broker
//! Kafka — librdkafka's `StreamConsumer` had not yet connected /
//! fetched metadata at bootstrap time, and the synchronous
//! `fetch_watermarks` short-circuited to the "unknown" sentinel
//! (mapped to `0`). The bootstrap branch in
//! `mirror_core::run_mirror_with_heartbeat` therefore saw
//! `low_watermark=0`, decided no skip was needed, and the mirror
//! crashed on the broker's first delivered offset.
//!
//! Fix: route the watermark query through a fresh `BaseConsumer`
//! via `spawn_blocking` (the same pattern the cache-readiness gate
//! and `KafkaSink::fetch_high_watermark` use, both of which work
//! reliably in production).
//!
//! Test exercises the CONTRACT, not the implementation: open a
//! source against a trimmed topic, call `low_watermark` once, and
//! assert the broker's actual value comes back. If a future
//! maintainer reverts to the StreamConsumer-based call and the
//! local Redpanda happens to make that pass, this test will still
//! at least document the requirement.

use std::time::Duration;

use mirror_core::Source;
use mirror_e2e::docker::DockerProvisioner;
use mirror_e2e::kafka_helpers::{create_compacted_topic, produce_records, trim_records_before};
use mirror_e2e::{ProvisionedStack, Provisioner};
use mirror_kafka::{KafkaSource, KafkaSourceConfig};

const TOPIC: &str = "mirror-e2e-low-watermark-contract";
const PARTITION: i32 = 0;

fn install_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_source_low_watermark_reports_broker_value_before_any_seek() {
    install_tracing();

    let stack = DockerProvisioner.provision().await.expect("provision");
    let source_bootstrap = stack.source_bootstrap();

    create_compacted_topic(&source_bootstrap, TOPIC, 1)
        .await
        .expect("create topic");

    let seed: Vec<(String, String)> = (0..12)
        .map(|i| (format!("k{}", i % 4), format!("v{i}")))
        .collect();
    produce_records(&source_bootstrap, TOPIC, PARTITION, &seed)
        .await
        .expect("seed");
    let new_low = trim_records_before(&source_bootstrap, TOPIC, PARTITION, 8)
        .await
        .expect("trim");
    assert_eq!(
        new_low, 8,
        "test setup invariant: delete-records must report low=8"
    );

    let mut source = KafkaSource::open(KafkaSourceConfig::new(
        source_bootstrap,
        "mirror-e2e-low-watermark-contract",
        TOPIC,
        PARTITION,
    ))
    .expect("open KafkaSource");

    // The point of this test: low_watermark must return 8 on the
    // FIRST call, before seek/poll. The buggy implementation
    // returned 0 here against real Kafka.
    let reported = tokio::time::timeout(Duration::from_secs(15), source.low_watermark())
        .await
        .expect("low_watermark must complete within 15s")
        .expect("low_watermark must succeed");
    assert_eq!(
        reported, 8,
        "KafkaSource::low_watermark must return the broker's actual low watermark \
         on a fresh source before any seek/poll; got {reported}"
    );
}
