//! Happy-path e2e: kafka-native → mirror-v3 → redpanda.
//!
//! Verifies the load-bearing guarantee end-to-end:
//! - byte-identical key/value round-trip
//! - source offset == target offset for every record
//! - exactly N records on target after producing N to source

use std::time::Duration;

use mirror_e2e::docker::DockerProvisioner;
use mirror_e2e::kafka_helpers::{
    create_topic, drain_partition, produce_records, wait_for_high_watermark,
};
use mirror_e2e::mirror_runner::{spawn_kafka_to_kafka, MirrorSpec};
use mirror_e2e::{ProvisionedStack, Provisioner};

const TOPIC: &str = "mirror-e2e-happy";
const N: usize = 100;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mirrors_records_byte_identical_with_offsets_preserved() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    let stack = DockerProvisioner
        .provision()
        .await
        .expect("provision stack");
    let source = stack.source_bootstrap();
    let target = stack.target_kafka_bootstrap().unwrap();
    tracing::info!(source = %source, target = %target, "stack ready");

    create_topic(&source, TOPIC, 1).await.expect("source topic");
    create_topic(&target, TOPIC, 1).await.expect("target topic");

    // Fixture: 100 records with stable, distinguishable keys/values.
    let fixtures: Vec<(String, String)> = (0..N)
        .map(|i| (format!("k{i:04}"), format!("v{i:04}")))
        .collect();
    produce_records(&source, TOPIC, 0, &fixtures)
        .await
        .expect("produce fixtures");

    // Start the mirror.
    let mirror = spawn_kafka_to_kafka(MirrorSpec {
        source_bootstrap: source.clone(),
        target_bootstrap: target.clone(),
        source_topic: TOPIC.into(),
        target_topic: TOPIC.into(),
        partition: 0,
        group_id: "mirror-e2e-happy".into(),
    })
    .expect("spawn mirror");

    // Wait for the target to catch up.
    wait_for_high_watermark(&target, TOPIC, 0, N as i64, Duration::from_secs(60))
        .await
        .expect("target reached high watermark");

    // Drain and assert.
    let drained =
        drain_partition(&target, TOPIC, 0, Duration::from_secs(30)).expect("drain target");
    assert_eq!(drained.len(), N, "record count");
    for (i, rec) in drained.iter().enumerate() {
        assert_eq!(rec.offset, i as i64, "offset at index {i}");
        assert_eq!(
            rec.key.as_deref(),
            Some(format!("k{i:04}").as_bytes()),
            "key at offset {i}"
        );
        assert_eq!(
            rec.value.as_deref(),
            Some(format!("v{i:04}").as_bytes()),
            "value at offset {i}"
        );
    }

    mirror.abort();
}
