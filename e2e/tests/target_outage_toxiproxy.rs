//! Toxiproxy-based fault-injection e2e: target broker becomes
//! unreachable mid-stream, then comes back.
//!
//! Drives the load-bearing claim under a real network failure:
//! after the mirror errors out because the destination is
//! unreachable, a fresh mirror against the same destination must
//! resume at exactly `max(to)+1` and produce no duplicates.

use std::time::Duration;

use mirror_e2e::docker::KafkaNativeToRedpandaToxiTargetStack;
use mirror_e2e::kafka_helpers::{
    create_topic, drain_partition, produce_records, wait_for_high_watermark,
};
use mirror_e2e::mirror_runner::{spawn_kafka_to_kafka, MirrorSpec};
use mirror_e2e::ProvisionedStack;

const TOPIC: &str = "mirror-e2e-toxi-target";

fn install_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

fn spec(source: &str, target: &str, group: &str) -> MirrorSpec {
    MirrorSpec {
        source_bootstrap: source.to_string(),
        target_bootstrap: target.to_string(),
        source_topic: TOPIC.into(),
        target_topic: TOPIC.into(),
        partition: 0,
        group_id: group.into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn target_outage_mid_stream_recovers_with_no_duplicates() {
    install_tracing();

    let stack = KafkaNativeToRedpandaToxiTargetStack::start()
        .await
        .expect("provision toxi stack");
    let source = stack.source_bootstrap();
    let target = stack.target_kafka_bootstrap().unwrap();
    tracing::info!(source = %source, target = %target, "stack ready");

    create_topic(&source, TOPIC, 1).await.expect("source topic");
    create_topic(&target, TOPIC, 1).await.expect("target topic");

    // Stage 1: 25 records, mirror them all through the proxy.
    let stage_a: Vec<(String, String)> = (0..25)
        .map(|i| (format!("k{i:03}"), format!("v{i:03}")))
        .collect();
    produce_records(&source, TOPIC, 0, &stage_a)
        .await
        .expect("produce stage_a");

    let m1 = spawn_kafka_to_kafka(spec(&source, &target, "toxi-target")).expect("spawn m1");
    wait_for_high_watermark(&target, TOPIC, 0, 25, Duration::from_secs(60))
        .await
        .expect("target catches up before outage");

    // Cut the target. The mirror is now in an idle poll loop; its next
    // `fetch_high_watermark` call will time out, surfacing as a
    // SinkError::Transport, which run_mirror returns as MirrorError.
    tracing::info!("disabling target proxy");
    stack.target_down().await.expect("target_down");

    // Wait for m1 to terminate with an error. fetch_watermarks
    // timeout is 10s; allow generous slack.
    let res = tokio::time::timeout(Duration::from_secs(45), m1.wait_for_termination()).await;
    let res = res.expect("m1 should terminate within 45s");
    let err = res.expect_err("m1 must surface an error from the cut");
    tracing::info!(error = %err, "m1 errored as expected");

    // Restore the target. New connections from this point on succeed.
    stack.target_up().await.expect("target_up");

    // Produce 10 more records to source (offsets 25-34).
    let stage_b: Vec<(String, String)> = (25..35)
        .map(|i| (format!("k{i:03}"), format!("v{i:03}")))
        .collect();
    produce_records(&source, TOPIC, 0, &stage_b)
        .await
        .expect("produce stage_b");

    // Start a fresh mirror. It must seek source to the target's
    // current high watermark (25) and pick up offsets 25..34.
    let m2 = spawn_kafka_to_kafka(spec(&source, &target, "toxi-target-2")).expect("spawn m2");

    wait_for_high_watermark(&target, TOPIC, 0, 35, Duration::from_secs(60))
        .await
        .expect("target reaches 35 after restart");

    // The killer assertion: target has exactly 35 records, offsets
    // 0..34, byte-identical to the union of stage_a and stage_b. No
    // duplicates ever wrote past the cut.
    let drained = drain_partition(&target, TOPIC, 0, Duration::from_secs(30)).expect("drain");
    assert_eq!(drained.len(), 35, "exactly 35 records on target");
    for (i, rec) in drained.iter().enumerate() {
        assert_eq!(rec.offset, i as i64, "offset at index {i}");
        assert_eq!(
            rec.key.as_deref(),
            Some(format!("k{i:03}").as_bytes()),
            "key at offset {i}"
        );
        assert_eq!(
            rec.value.as_deref(),
            Some(format!("v{i:03}").as_bytes()),
            "value at offset {i}"
        );
    }

    m2.abort();
}
