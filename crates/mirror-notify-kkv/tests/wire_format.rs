//! Pin the kkv-v1 wire contract. The `@yolean/kafka-keyvalue` Node
//! client parses POSTs to `/kafka-keyvalue/v1/updates` with this exact
//! shape: header keys, body field names, `null` update values. Drift
//! here breaks every existing consumer silently.

mod common;

use std::time::Duration;

use common::{notify_pointing_at, ready_cache, terminal_error, wait_until, Reply, TestServer};
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

const WAIT: Duration = Duration::from_secs(5);

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
    let mut notifier =
        KkvV1Notifier::from_config(&cfg, "events".into(), 3, ready_cache("m"), "m".into()).unwrap();

    notifier
        .on_record(&rec(42, "user-7", "ignored"))
        .await
        .unwrap();
    wait_until("one POST", WAIT, || server.request_count() == 1).await;

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
            "v": 1,
            "topic": "events",
            "offsets": { "3": 42 },
            "updates": { "user-7": null }
        }),
        "body must match the legacy KafkaKeyValue.js parser shape exactly, \
         including the `v: 1` protocol-version field that the consumer \
         enforces with an early throw"
    );
}

/// kafka-keyvalue does not notify a record without a key (it used to
/// go out here as `""`, which no consumer can re-read meaningfully).
#[tokio::test]
async fn a_record_without_a_key_is_not_notified() {
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), fast_retry(), 1000);
    let mut notifier =
        KkvV1Notifier::from_config(&cfg, "events".into(), 0, ready_cache("m"), "m".into()).unwrap();

    let mut record = rec(7, "", "v");
    record.key = None;
    notifier.on_record(&record).await.unwrap();
    notifier.on_record(&rec(8, "k8", "v")).await.unwrap();
    wait_until("one POST", WAIT, || server.request_count() == 1).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let body: Value = serde_json::from_slice(&server.captured().await[0].body).unwrap();
    assert_eq!(body["updates"], serde_json::json!({"k8": null}));
    assert_eq!(server.request_count(), 1);
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
    let mut notifier =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into()).unwrap();

    notifier.on_record(&rec(1, "k", "v")).await.unwrap();
    let err = terminal_error(&notifier, WAIT).await;
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
    let mut notifier =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into()).unwrap();

    notifier.on_record(&rec(1, "k", "v")).await.unwrap();
    let err = terminal_error(&notifier, WAIT).await;
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("refused") || msg.contains("connect"),
        "error should mention connection failure, got: {msg}"
    );
}
