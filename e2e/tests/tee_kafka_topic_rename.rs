//! E2e: a single Kafka destination, source topic = `A`, destination
//! topic = `B`. Verifies the new optional `topic` field on
//! `KafkaDestination` (defaults to source topic; overridable for
//! mirror-with-rename).
//!
//! The test goes through `spawn_kafka_to_tee` (a length-1 tee) so
//! the same code path exercised by mirror-bin in production is what
//! we're testing.

use std::time::Duration;

use mirror_e2e::docker::DockerProvisioner;
use mirror_e2e::kafka_helpers::{create_topic, drain_partition, produce_records};
use mirror_e2e::mirror_runner::{spawn_kafka_to_tee, KafkaInnerSpec, TeeInnerSpec, TeeMirrorSpec};
use mirror_e2e::{ProvisionedStack, Provisioner};

const SOURCE_TOPIC: &str = "mirror-e2e-tee-rename-src";
const TARGET_TOPIC: &str = "mirror-e2e-tee-rename-dst";

fn install_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tee_with_kafka_destination_topic_rename() {
    install_tracing();

    let stack = DockerProvisioner.provision().await.expect("provision");
    let source = stack.source_bootstrap();
    let target = stack
        .target_kafka_bootstrap()
        .expect("stack must expose target Kafka bootstrap");

    create_topic(&source, SOURCE_TOPIC, 1)
        .await
        .expect("create source topic");
    create_topic(&target, TARGET_TOPIC, 1)
        .await
        .expect("create target topic with the renamed name");

    let fixtures: Vec<(String, String)> = (0..10)
        .map(|i| (format!("k{i}"), format!("v{i}")))
        .collect();
    produce_records(&source, SOURCE_TOPIC, 0, &fixtures)
        .await
        .expect("produce source records");

    let mirror = spawn_kafka_to_tee(TeeMirrorSpec {
        source_bootstrap: source.clone(),
        source_topic: SOURCE_TOPIC.into(),
        partition: 0,
        group_id: "mirror-e2e-tee-rename".into(),
        destinations: vec![TeeInnerSpec::Kafka(KafkaInnerSpec {
            name: "rename".into(),
            bootstrap_servers: target.clone(),
            // The whole point of this test: destination topic differs
            // from source topic. Without an explicit `topic` field on
            // the Kafka destination, the only way to rename today
            // would be to rename the mirror itself.
            topic: TARGET_TOPIC.into(),
        })],
        cache: None,
    })
    .await
    .expect("spawn tee mirror");

    // Wait for the mirror to mirror everything across, then drain
    // the destination.
    tokio::time::sleep(Duration::from_secs(3)).await;
    mirror.shutdown().await.expect("graceful shutdown");

    let drained =
        drain_partition(&target, TARGET_TOPIC, 0, Duration::from_secs(15)).expect("drain target");
    assert_eq!(drained.len(), 10, "destination topic must have 10 records");
    for (i, rec) in drained.iter().enumerate() {
        assert_eq!(rec.offset, i as i64);
        assert_eq!(rec.key.as_deref(), Some(format!("k{i}").as_bytes()));
        assert_eq!(rec.value.as_deref(), Some(format!("v{i}").as_bytes()));
    }
}
