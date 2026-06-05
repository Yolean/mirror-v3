//! E2e: kafka → mirror-v3 (filesystem) with `notify` enabled,
//! against a real axum-backed webhook receiver in-process. Verifies
//! the full surface end-to-end:
//!   * `trigger.on: source-consume` POSTs match the kkv-v1 wire
//!     contract (path, headers, body).
//!   * `trigger.on: destination-flush` fires one POST per durable
//!     flush, with `updates: {}` per spec.
//!   * The receiver receives every record's key under source-consume
//!     debounce.

use std::time::Duration;

use mirror_config::{
    FanOut, Notify, NotifyApi, NotifyDebounce, NotifyOutcomes, NotifyRetry, NotifyTarget,
    NotifyTrigger, TriggerOn,
};
use mirror_e2e::docker::DockerProvisioner;
use mirror_e2e::kafka_helpers::{create_topic, produce_records};
use mirror_e2e::mirror_runner::{spawn_kafka_to_fs_with_notify, FsMirrorSpec};
use mirror_e2e::webhook_receiver::WebhookReceiver;
use mirror_e2e::{ProvisionedStack, Provisioner};
use mirror_fs::FlushTriggers;
use serde_json::Value;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

fn notify_pointing_at_with_trigger(
    addr: std::net::SocketAddr,
    trigger: NotifyTrigger,
    max_attempts: u32,
) -> Notify {
    Notify {
        api: NotifyApi::KkvV1,
        targets: vec![NotifyTarget {
            url: format!("http://{addr}"),
            path: None,
            fan_out: FanOut::None,
        }],
        trigger,
        timeout_ms: 2000,
        retry: NotifyRetry {
            max_attempts,
            backoff_ms: 50,
        },
        outcomes: NotifyOutcomes::default(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_consume_dispatches_kkv_v1_posts_for_produced_records() {
    init_tracing();
    let stack = DockerProvisioner.provision().await.expect("provision");
    let source = stack.source_bootstrap();
    let root = tempfile::tempdir().expect("tempdir");
    let topic = "notify-kkv-source-consume";

    create_topic(&source, topic, 1).await.expect("topic");

    let receiver = WebhookReceiver::start().await;
    let notify = notify_pointing_at_with_trigger(
        receiver.addr,
        NotifyTrigger {
            on: TriggerOn::SourceConsume,
            // Tight debounce so 10 produced records collapse into
            // one or two POSTs.
            debounce: Some(NotifyDebounce {
                max_records: 10,
                max_time_ms: 200,
            }),
        },
        3,
    );

    let flush = FlushTriggers {
        max_time: Duration::from_secs(3600),
        max_bytes: u64::MAX,
        max_offsets: 1_000,
        daily_at_utc_seconds: None,
    };
    let mirror = spawn_kafka_to_fs_with_notify(
        FsMirrorSpec::ndjson(
            source.clone(),
            topic.into(),
            0,
            "notify-source-consume".into(),
            root.path().to_path_buf(),
            "ops".into(),
            flush,
        ),
        notify,
    )
    .await
    .expect("spawn mirror");

    let fixtures: Vec<(String, String)> = (0..10)
        .map(|i| (format!("user-{i}"), format!("payload-{i}")))
        .collect();
    produce_records(&source, topic, 0, &fixtures)
        .await
        .expect("produce");

    // Wait for the receiver to see at least one POST. The debounce
    // window is 200ms; we give it generous slack for Kafka delivery
    // + dispatcher latency.
    let captured = receiver.wait_for(1, Duration::from_secs(15)).await;

    // Sanity on the first POST's contract.
    let req = &captured[0];
    assert_eq!(
        req.path, "/kafka-keyvalue/v1/updates",
        "default kkv-v1 path"
    );
    assert_eq!(
        req.headers
            .get("x-kkv-topic")
            .and_then(|v| v.to_str().ok())
            .unwrap_or(""),
        topic
    );
    assert_eq!(
        req.headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or(""),
        "application/json"
    );
    let body: Value = serde_json::from_slice(&req.body).expect("body JSON");
    assert_eq!(body["topic"], topic);
    // Each captured POST must carry a non-empty updates map (all
    // produced keys are kkv-routable strings).
    let updates = body["updates"]
        .as_object()
        .expect("updates is a JSON object");
    assert!(
        !updates.is_empty(),
        "first POST must carry at least one key"
    );
    // The highest source offset in the batch must equal the largest
    // 0-based offset of the keys it carries; since we produced
    // contiguously from 0, the offset must be one of 0..9.
    let high = body["offsets"]["0"]
        .as_u64()
        .expect("offsets.0 must be u64");
    assert!(
        (0..10).contains(&high),
        "highest offset out of range, got {high}"
    );

    // Across ALL POSTs, every produced key must appear at least once
    // (a key may collapse twice into the same batch if produced
    // bursts overlap a debounce window; "at least once" is the
    // load-bearing contract for cache invalidation).
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for r in &captured {
        let body: Value = serde_json::from_slice(&r.body).expect("body JSON");
        if let Some(updates) = body["updates"].as_object() {
            for k in updates.keys() {
                seen.insert(k.clone());
            }
        }
    }
    for (k, _) in &fixtures {
        assert!(
            seen.contains(k),
            "produced key {k:?} never appeared in any notify POST"
        );
    }

    mirror.shutdown().await.expect("graceful shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn destination_flush_dispatches_one_post_per_flush_with_empty_updates() {
    init_tracing();
    let stack = DockerProvisioner.provision().await.expect("provision");
    let source = stack.source_bootstrap();
    let root = tempfile::tempdir().expect("tempdir");
    let topic = "notify-kkv-dest-flush";

    create_topic(&source, topic, 1).await.expect("topic");

    let receiver = WebhookReceiver::start().await;
    let notify = notify_pointing_at_with_trigger(
        receiver.addr,
        NotifyTrigger {
            on: TriggerOn::DestinationFlush,
            debounce: None,
        },
        3,
    );

    // Flush every 5 records → 2 flushes for 10 produced records.
    let flush = FlushTriggers {
        max_time: Duration::from_secs(3600),
        max_bytes: u64::MAX,
        max_offsets: 5,
        daily_at_utc_seconds: None,
    };
    let mirror = spawn_kafka_to_fs_with_notify(
        FsMirrorSpec::ndjson(
            source.clone(),
            topic.into(),
            0,
            "notify-dest-flush".into(),
            root.path().to_path_buf(),
            "ops".into(),
            flush,
        ),
        notify,
    )
    .await
    .expect("spawn mirror");

    let fixtures: Vec<(String, String)> = (0..10)
        .map(|i| (format!("k{i}"), format!("v{i}")))
        .collect();
    produce_records(&source, topic, 0, &fixtures)
        .await
        .expect("produce");

    // Two flushes expected — wait for both POSTs to land.
    let captured = receiver.wait_for(2, Duration::from_secs(20)).await;
    assert_eq!(
        captured.len(),
        2,
        "exactly two POSTs (one per max-offsets=5 flush)"
    );

    let body_0: Value = serde_json::from_slice(&captured[0].body).expect("body 0");
    let body_1: Value = serde_json::from_slice(&captured[1].body).expect("body 1");

    // Empty updates per spec for destination-flush.
    assert_eq!(body_0["updates"], serde_json::json!({}));
    assert_eq!(body_1["updates"], serde_json::json!({}));

    // Offsets in dispatch order: first flush covers 0..4 → high=4;
    // second covers 5..9 → high=9.
    assert_eq!(body_0["offsets"]["0"], serde_json::json!(4));
    assert_eq!(body_1["offsets"]["0"], serde_json::json!(9));

    mirror.shutdown().await.expect("graceful shutdown");
}
