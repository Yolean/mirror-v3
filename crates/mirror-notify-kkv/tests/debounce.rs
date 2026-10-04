//! Tests for the source-consume debounce buffer.
//!
//! The buffer batches `(key, source_offset)` per record, emits a
//! single POST when `max-records` records have arrived OR
//! `max-time-ms` has elapsed since the first record landed, and
//! collapses repeats of the same key while carrying the *max* source
//! offset on the wire.

mod common;

use std::time::Duration;

use common::{
    notify_pointing_at, notify_pointing_at_debounced, ready_cache, wait_until, Reply, TestServer,
};
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

const WAIT: Duration = Duration::from_secs(5);

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
    wait_until("the third record's batch", WAIT, || {
        server.request_count() == 1
    })
    .await;

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
    wait_until("one POST", WAIT, || server.request_count() == 1).await;

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
    // final: fail}. Delivery of the timer's batch exhausts, stashes the
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
    wait_until("two attempts", WAIT, || server.request_count() == 2).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let err = n
        .on_record(&rec(2, "b"))
        .await
        .expect_err("subsequent on_record must surface the delivery error");
    let s = format!("{err}");
    assert!(s.contains("exhausted"), "got: {s}");
}

/// kkv sent only the first record of a poll and
/// held the rest until the next record arrived, hours on a quiet topic.
/// Here a burst's tail is a batch of its own once max-time-ms passes,
/// with no further record.
#[tokio::test]
async fn burst_tail_is_sent_without_a_further_record() {
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_pointing_at_debounced(
        server.addr,
        NotifyOutcomes::default(),
        retry(1),
        1000,
        NotifyDebounce {
            max_records: 2,
            max_time_ms: 50,
        },
    );
    let mut n =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into()).unwrap();
    for (o, k) in [(1, "k1"), (2, "k2"), (3, "k3"), (4, "k4"), (5, "k5")] {
        n.on_record(&rec(o, k)).await.unwrap();
    }
    wait_until("three POSTs", WAIT, || server.request_count() == 3).await;
    let last: Value = serde_json::from_slice(&server.captured().await[2].body).unwrap();
    assert_eq!(last["updates"], serde_json::json!({"k5": null}));
    assert_eq!(last["offsets"], serde_json::json!({"0": 5}));
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
    wait_until("first batch at max-records", WAIT, || {
        server.request_count() == 1
    })
    .await;

    // Second batch
    n.on_record(&rec(12, "c")).await.unwrap();
    n.on_record(&rec(13, "d")).await.unwrap();
    wait_until("second batch", WAIT, || server.request_count() == 2).await;

    let captured = server.captured().await;
    let body0: Value = serde_json::from_slice(&captured[0].body).unwrap();
    let body1: Value = serde_json::from_slice(&captured[1].body).unwrap();
    assert_eq!(body0["offsets"], serde_json::json!({"0": 11}));
    assert_eq!(body1["offsets"], serde_json::json!({"0": 13}));
}

/// Idle-topic variant of timer-drain failure: with no further
/// on_record call there is nothing to surface the stashed error, so
/// the supervisor needs the terminal-error watch to learn the
/// notify pipeline is dead.
#[tokio::test]
async fn terminal_error_watch_fires_on_timer_exhaustion_without_further_records() {
    let server = TestServer::start(Reply::Status(500), vec![]).await;
    let cfg = notify_pointing_at_debounced(
        server.addr,
        NotifyOutcomes::default(),
        NotifyRetry {
            max_attempts: 2,
            backoff_ms: 1,
        },
        1000,
        NotifyDebounce {
            max_records: 100,
            max_time_ms: 20,
        },
    );
    let mut notifier =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into()).unwrap();
    let watch = notifier.terminal_error_watch();

    // One record, then silence: the timer drains it after
    // max-time-ms and exhausts retries against the 500-only server.
    notifier.on_record(&rec(0, "k0")).await.unwrap();

    let err = tokio::time::timeout(Duration::from_secs(5), watch.wait())
        .await
        .expect("watch must resolve on timer-drain exhaustion");
    assert!(format!("{err}").to_lowercase().contains("exhausted"));
}
