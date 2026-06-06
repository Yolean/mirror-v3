//! Tests for the source-consume debounce buffer.
//!
//! The buffer batches `(key, source_offset)` per record, emits a
//! single POST when `max-records` records have arrived OR
//! `max-time-ms` has elapsed since the first record landed, and
//! collapses repeats of the same key while carrying the *max* source
//! offset on the wire.

mod common;

use std::time::Duration;

use common::{notify_pointing_at, notify_pointing_at_debounced, ready_cache, Reply, TestServer};
use mirror_config::{NotifyDebounce, NotifyOutcomes, NotifyRetry};
use mirror_core::{Notifier, Record, TimestampType};
use mirror_notify_kkv::KkvV1Notifier;
use serde_json::Value;

fn rec(offset: u64, key: &str) -> Record {
    Record {
        topic: "t".into(),
        partition: 0,
        source_offset: offset,
        timestamp_ms: Some(1_700_000_000_000),
        timestamp_type: TimestampType::CreateTime,
        key: Some(key.as_bytes().to_vec()),
        value: Some(b"v".to_vec()),
        headers: vec![],
    }
}

fn retry(attempts: u32) -> NotifyRetry {
    NotifyRetry {
        max_attempts: attempts,
        backoff_ms: 1,
    }
}

#[tokio::test]
async fn drains_when_max_records_reached() {
    // max-records=3, very long max-time so only the record count
    // can trigger.
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_pointing_at_debounced(
        server.addr,
        NotifyOutcomes::default(),
        retry(1),
        1000,
        NotifyDebounce {
            max_records: 3,
            max_time_ms: 60_000,
        },
    );
    let mut n =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into()).unwrap();

    n.on_record(&rec(10, "a")).await.unwrap();
    n.on_record(&rec(11, "b")).await.unwrap();
    assert_eq!(
        server.request_count(),
        0,
        "no drain yet; only 2 of 3 records buffered"
    );
    n.on_record(&rec(12, "c")).await.unwrap();
    assert_eq!(
        server.request_count(),
        1,
        "third record must drain the batch inline"
    );

    let body: Value = serde_json::from_slice(&server.captured().await[0].body).unwrap();
    assert_eq!(
        body,
        serde_json::json!({
            "v": 1,
            "topic": "t",
            "offsets": { "0": 12 },
            "updates": { "a": null, "b": null, "c": null }
        })
    );
}

#[tokio::test]
async fn drains_when_max_time_ms_elapses() {
    // max-records very high, max-time-ms small. Send 1 record, sleep
    // past the window, expect the background timer to have drained.
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_pointing_at_debounced(
        server.addr,
        NotifyOutcomes::default(),
        retry(1),
        1000,
        NotifyDebounce {
            max_records: 1_000,
            max_time_ms: 50,
        },
    );
    let mut n =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into()).unwrap();

    n.on_record(&rec(7, "x")).await.unwrap();
    assert_eq!(
        server.request_count(),
        0,
        "no inline drain; record buffered"
    );

    // Sleep comfortably past the window plus dispatch slop.
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        server.request_count(),
        1,
        "timer task must have drained the single-record batch"
    );
    let body: Value = serde_json::from_slice(&server.captured().await[0].body).unwrap();
    assert_eq!(body["offsets"], serde_json::json!({"0": 7}));
    assert_eq!(body["updates"], serde_json::json!({"x": null}));
}

#[tokio::test]
async fn key_dedup_keeps_one_entry_with_max_offset() {
    // Three records with the same key. The batch's `updates` must
    // carry the key once; `offsets` must reflect the highest source
    // offset across all three.
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_pointing_at_debounced(
        server.addr,
        NotifyOutcomes::default(),
        retry(1),
        1000,
        NotifyDebounce {
            max_records: 3,
            max_time_ms: 60_000,
        },
    );
    let mut n =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into()).unwrap();

    n.on_record(&rec(20, "hot")).await.unwrap();
    n.on_record(&rec(21, "hot")).await.unwrap();
    n.on_record(&rec(22, "hot")).await.unwrap();

    let body: Value = serde_json::from_slice(&server.captured().await[0].body).unwrap();
    assert_eq!(
        body["updates"],
        serde_json::json!({"hot": null}),
        "duplicate keys must collapse to one entry"
    );
    assert_eq!(
        body["offsets"],
        serde_json::json!({"0": 22}),
        "offsets must carry the max source offset across the batch"
    );
}

#[tokio::test]
async fn shutdown_drains_pending_batch() {
    // Non-trivial buffer (under max-records, well within max-time),
    // shutdown must POST it before returning.
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_pointing_at_debounced(
        server.addr,
        NotifyOutcomes::default(),
        retry(1),
        1000,
        NotifyDebounce {
            max_records: 1_000,
            max_time_ms: 60_000,
        },
    );
    let mut n =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into()).unwrap();

    n.on_record(&rec(1, "a")).await.unwrap();
    n.on_record(&rec(2, "b")).await.unwrap();
    assert_eq!(server.request_count(), 0);

    n.shutdown().await.expect("shutdown drain must succeed");
    assert_eq!(
        server.request_count(),
        1,
        "shutdown must drain whatever's in the buffer"
    );
    let body: Value = serde_json::from_slice(&server.captured().await[0].body).unwrap();
    assert_eq!(body["offsets"], serde_json::json!({"0": 2}));
    assert_eq!(body["updates"], serde_json::json!({"a": null, "b": null}));
}

#[tokio::test]
async fn shutdown_with_empty_buffer_is_a_noop() {
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), retry(1), 1000);
    let mut n =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into()).unwrap();

    n.shutdown().await.expect("empty shutdown must succeed");
    assert_eq!(server.request_count(), 0, "no records → no POST");
}

#[tokio::test]
async fn timer_drain_failure_surfaces_on_next_on_record() {
    // Server returns 503 forever; outcome 5xx default is {retry: true,
    // final: fail}. The timer-task drain hits this, stashes the
    // NotifyError, and the next on_record returns it.
    let server = TestServer::start(Reply::Status(503), vec![]).await;
    let cfg = notify_pointing_at_debounced(
        server.addr,
        NotifyOutcomes::default(),
        retry(2),
        1000,
        NotifyDebounce {
            max_records: 1_000,
            max_time_ms: 50,
        },
    );
    let mut n =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into()).unwrap();

    n.on_record(&rec(1, "a")).await.unwrap();
    // Wait long enough for the timer to fire, exhaust retries
    // (2 attempts × 1ms backoff), and stash the error.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let err = n
        .on_record(&rec(2, "b"))
        .await
        .expect_err("subsequent on_record must surface the timer-task error");
    let s = format!("{err}");
    assert!(s.contains("exhausted"), "got: {s}");
}

#[tokio::test]
async fn buffer_continues_to_accept_after_inline_drain() {
    // After a max-records drain, the buffer is empty and ready to
    // accumulate the next batch independently.
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_pointing_at_debounced(
        server.addr,
        NotifyOutcomes::default(),
        retry(1),
        1000,
        NotifyDebounce {
            max_records: 2,
            max_time_ms: 60_000,
        },
    );
    let mut n =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into()).unwrap();

    // First batch
    n.on_record(&rec(10, "a")).await.unwrap();
    n.on_record(&rec(11, "b")).await.unwrap();
    assert_eq!(
        server.request_count(),
        1,
        "first batch must drain at max-records"
    );

    // Second batch
    n.on_record(&rec(12, "c")).await.unwrap();
    n.on_record(&rec(13, "d")).await.unwrap();
    assert_eq!(
        server.request_count(),
        2,
        "second batch must drain independently"
    );

    let captured = server.captured().await;
    let body0: Value = serde_json::from_slice(&captured[0].body).unwrap();
    let body1: Value = serde_json::from_slice(&captured[1].body).unwrap();
    assert_eq!(body0["offsets"], serde_json::json!({"0": 11}));
    assert_eq!(body1["offsets"], serde_json::json!({"0": 13}));
}
