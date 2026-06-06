//! Pin the kkv-v1 wire contract. The `@yolean/kafka-keyvalue` Node
//! client parses POSTs to `/kafka-keyvalue/v1/updates` with this exact
//! shape: header keys, body field names, `null` update values. Drift
//! here breaks every existing consumer silently.

mod common;

use std::time::Duration;

use common::{notify_pointing_at, Reply, TestServer};
use mirror_config::{NotifyOutcomes, NotifyRetry};
use mirror_core::{Notifier, Record, TimestampType};
use mirror_notify_kkv::{KkvV1Notifier, KKV_V1_DEFAULT_PATH};
use serde_json::Value;

fn rec(offset: u64, key: &str, value: &str) -> Record {
    Record {
        topic: "events".into(),
        partition: 3,
        source_offset: offset,
        timestamp_ms: Some(1_700_000_000_000),
        timestamp_type: TimestampType::CreateTime,
        key: Some(key.as_bytes().to_vec()),
        value: Some(value.as_bytes().to_vec()),
        headers: vec![],
    }
}

fn fast_retry() -> NotifyRetry {
    NotifyRetry {
        max_attempts: 1,
        backoff_ms: 1,
    }
}

#[tokio::test]
async fn posts_to_default_kkv_path_with_canonical_body() {
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), fast_retry(), 1000);
    let mut notifier = KkvV1Notifier::from_config(&cfg, "events".into(), 3).unwrap();

    notifier
        .on_record(&rec(42, "user-7", "ignored"))
        .await
        .unwrap();

    let captured = server.captured().await;
    assert_eq!(
        captured.len(),
        1,
        "one record, max_records=1 helper, expect one POST"
    );
    let req = &captured[0];

    assert_eq!(
        req.path, KKV_V1_DEFAULT_PATH,
        "default path must match the legacy ON_UPDATE_DEFAULT_PATH constant the Node client mounts"
    );

    let topic_hdr = req.headers.get("x-kkv-topic").expect("missing x-kkv-topic");
    assert_eq!(topic_hdr.to_str().unwrap(), "events");

    let offsets_hdr = req
        .headers
        .get("x-kkv-offsets")
        .expect("missing x-kkv-offsets");
    let offsets_hdr_val: Value = serde_json::from_str(offsets_hdr.to_str().unwrap()).unwrap();
    assert_eq!(offsets_hdr_val, serde_json::json!({"3": 42}));

    let content_type = req.headers.get("content-type").unwrap();
    assert_eq!(content_type.to_str().unwrap(), "application/json");

    let body: Value = serde_json::from_slice(&req.body).unwrap();
    assert_eq!(
        body,
        serde_json::json!({
            "topic": "events",
            "offsets": { "3": 42 },
            "updates": { "user-7": null }
        }),
        "body must match the legacy KafkaKeyValue.js parser shape exactly"
    );
}

#[tokio::test]
async fn null_key_serializes_as_empty_string() {
    // The Node consumer keys cache invalidations by string; a missing
    // key turns into "" so it has SOMETHING to call `getValue("")`
    // with; same as the legacy kkv null handling.
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), fast_retry(), 1000);
    let mut notifier = KkvV1Notifier::from_config(&cfg, "events".into(), 0).unwrap();

    let mut record = rec(7, "", "v");
    record.key = None;
    notifier.on_record(&record).await.unwrap();

    let body: Value = serde_json::from_slice(&server.captured().await[0].body).unwrap();
    assert_eq!(body["updates"], serde_json::json!({"": null}));
}

#[tokio::test]
async fn respects_explicit_target_path_override() {
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let mut cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), fast_retry(), 1000);
    cfg.targets[0].path = Some("/custom/route".into());

    let mut notifier = KkvV1Notifier::from_config(&cfg, "t".into(), 0).unwrap();
    notifier.on_record(&rec(1, "k", "v")).await.unwrap();

    let captured = server.captured().await;
    assert_eq!(captured[0].path, "/custom/route");
}

#[tokio::test]
async fn timeout_classification_uses_timeout_outcome() {
    // Server replies after 200ms; client timeout is 50ms; outcomes
    // table maps `timeout` to `retry: false, final: fail` so the
    // single attempt errors out immediately.
    use mirror_config::{FinalAction, NotifyOutcome};
    let outcomes = NotifyOutcomes {
        timeout: NotifyOutcome {
            retry: false,
            final_: FinalAction::Fail,
        },
        ..NotifyOutcomes::default()
    };
    let server = TestServer::start(Reply::SlowOk(Duration::from_millis(200)), vec![]).await;
    let cfg = notify_pointing_at(server.addr, outcomes, fast_retry(), 50);
    let mut notifier = KkvV1Notifier::from_config(&cfg, "t".into(), 0).unwrap();

    let err = notifier
        .on_record(&rec(1, "k", "v"))
        .await
        .expect_err("timeout outcome with final:fail must surface");
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("timed out") || msg.to_lowercase().contains("timeout"),
        "error should mention timeout, got: {msg}"
    );
}

#[tokio::test]
async fn connection_refused_classification_uses_connrefused_outcome() {
    // Pick a port nothing is listening on. The OS-level refusal must
    // map to the `connrefused` outcome bucket.
    use mirror_config::{FinalAction, NotifyOutcome};
    let outcomes = NotifyOutcomes {
        connrefused: NotifyOutcome {
            retry: false,
            final_: FinalAction::Fail,
        },
        ..NotifyOutcomes::default()
    };
    // 127.0.0.1:1 is reliably refused on all Unixes (root-only port,
    // never bound).
    let addr: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
    let cfg = notify_pointing_at(addr, outcomes, fast_retry(), 1000);
    let mut notifier = KkvV1Notifier::from_config(&cfg, "t".into(), 0).unwrap();

    let err = notifier
        .on_record(&rec(1, "k", "v"))
        .await
        .expect_err("connrefused outcome with final:fail must surface");
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("refused") || msg.contains("connect"),
        "error should mention connection failure, got: {msg}"
    );
}
