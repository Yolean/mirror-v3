//! Pin the per-mirror suppression-threshold gate for both notify
//! triggers. `KkvV1Notifier::on_record` and
//! `FlushDispatcher::on_flushed` must drop events whose source
//! offset is strictly below the mirror's `suppression_threshold` in
//! `CacheState`. The threshold is `max(last_committed_offset,
//! bootstrap_hwm if no commit)`, set at register time. Without this,
//! a cold restart fans historical-replay updates out to every
//! consumer pod (fresh deploy) or re-fires updates the previous pod
//! already delivered (returning deploy).

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{notify_pointing_at, Reply, TestServer};
use mirror_config::{
    FanOut, Notify, NotifyApi, NotifyOutcomes, NotifyRetry, NotifyTarget, NotifyTrigger, TriggerOn,
};
use mirror_core::{CacheState, FlushObserver, Notifier, Record, TimestampType};
use mirror_notify_kkv::{FlushDispatcher, KkvV1Notifier};
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

fn fast_retry() -> NotifyRetry {
    NotifyRetry {
        max_attempts: 1,
        backoff_ms: 1,
    }
}

#[tokio::test]
async fn source_consume_suppresses_below_threshold_fresh_deploy() {
    // Fresh deploy: no committed offset, threshold = bootstrap_hwm.
    // Mirror "m" has hwm=101. Records 50, 99, 100 (all < 101) are
    // suppressed; records 101 onward fire.
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), fast_retry(), 1000);

    let cache = Arc::new(CacheState::new());
    cache.register_mirror("m", 101, None, false);
    let mut notifier =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, Arc::clone(&cache), "m".into()).unwrap();

    // Below the threshold the dispatcher accepts the call but drops
    // the record. `apply_record` keeps the cache's per-mirror view
    // in sync (unrelated to the suppression check).
    for offset in [50_u64, 99, 100] {
        let r = rec(offset, &format!("k{offset}"));
        cache.apply_record("m", &r);
        notifier.on_record(&r).await.expect("suppressed: Ok(())");
    }
    assert_eq!(
        server.request_count(),
        0,
        "no POST may go out for offsets below threshold 101"
    );

    // Offset 101 == threshold; first record that fires.
    let r101 = rec(101, "k101");
    cache.apply_record("m", &r101);
    notifier
        .on_record(&r101)
        .await
        .expect("at threshold dispatch");

    let r102 = rec(102, "k102");
    cache.apply_record("m", &r102);
    notifier
        .on_record(&r102)
        .await
        .expect("above threshold dispatch");

    let captured = server.captured().await;
    assert_eq!(
        captured.len(),
        2,
        "exactly the two at-or-above-threshold records must POST"
    );
    let body0: Value = serde_json::from_slice(&captured[0].body).unwrap();
    assert_eq!(body0["updates"], serde_json::json!({"k101": null}));
    assert_eq!(body0["offsets"], serde_json::json!({"0": 101}));
    let body1: Value = serde_json::from_slice(&captured[1].body).unwrap();
    assert_eq!(body1["updates"], serde_json::json!({"k102": null}));
    assert_eq!(body1["offsets"], serde_json::json!({"0": 102}));
}

#[tokio::test]
async fn source_consume_suppresses_below_threshold_returning_deploy() {
    // Returning deploy: committed=5, bootstrap_hwm=20. Threshold = 5.
    // Records 0..4 suppressed (prior pod delivered them); 5..19 fire
    // (between-pods gap); 20+ fires (live). This is the dev2-bug fix
    // — without the committed-offset threshold this test would have
    // suppressed records 5..19 too, dropping every between-pods
    // record on the floor.
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), fast_retry(), 1000);

    let cache = Arc::new(CacheState::new());
    cache.register_mirror("m", 20, Some(5), false);
    let mut notifier =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, Arc::clone(&cache), "m".into()).unwrap();

    for offset in [0_u64, 1, 4] {
        let r = rec(offset, &format!("k{offset}"));
        cache.apply_record("m", &r);
        notifier.on_record(&r).await.unwrap();
    }
    assert_eq!(
        server.request_count(),
        0,
        "offsets below committed 5 must suppress"
    );

    // The between-pods gap: 5..19. All must fire.
    for offset in 5..10 {
        let r = rec(offset, &format!("k{offset}"));
        cache.apply_record("m", &r);
        notifier.on_record(&r).await.unwrap();
    }
    assert_eq!(
        server.request_count(),
        5,
        "the between-pods gap (5..10) must fire one POST per record"
    );
}

fn notify_dest_flush(addr: std::net::SocketAddr) -> Notify {
    Notify {
        api: NotifyApi::KkvV1,
        targets: vec![NotifyTarget {
            url: format!("http://{addr}"),
            path: None,
            fan_out: FanOut::None,
        }],
        trigger: NotifyTrigger {
            on: TriggerOn::DestinationFlush,
            // destination-flush forbids debounce per validator.
            debounce: None,
        },
        timeout_ms: 1000,
        retry: fast_retry(),
        outcomes: NotifyOutcomes::default(),
    }
}

async fn wait_for_requests(
    server: &TestServer,
    n: usize,
    timeout: Duration,
) -> Vec<common::CapturedRequest> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let captured = server.captured().await;
        if captured.len() >= n {
            return captured;
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for {n} requests; got {}", captured.len());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn destination_flush_suppresses_below_threshold() {
    // Same gate, different trigger surface. `on_flushed` is sync;
    // the drainer is a background task. Flushes whose high-water
    // offset `to` is below the suppression threshold must never
    // make it onto the channel; flushes at or above the threshold
    // POST normally.
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_dest_flush(server.addr);

    let cache = Arc::new(CacheState::new());
    // Fresh deploy with bootstrap_hwm=101 ⇒ threshold = 101.
    cache.register_mirror("m", 101, None, false);
    let dispatcher =
        FlushDispatcher::from_config(&cfg, "t".into(), 0, Arc::clone(&cache), "m".into())
            .expect("must build");

    // Two flushes whose `to` < 101 are dropped at the gate; the
    // channel never sees them, the drainer task stays idle.
    dispatcher.on_flushed(0, 49);
    dispatcher.on_flushed(50, 99);
    // Give the (idle) drainer a moment to prove no POST happens.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        server.request_count(),
        0,
        "no POST may go out for `to` below threshold 101"
    );

    // `to`=109 is above the threshold — fires.
    dispatcher.on_flushed(100, 109);

    let captured = wait_for_requests(&server, 1, Duration::from_secs(2)).await;
    assert_eq!(
        captured.len(),
        1,
        "only the at-or-above-threshold flush dispatches"
    );
    let body: Value = serde_json::from_slice(&captured[0].body).unwrap();
    assert_eq!(body["offsets"], serde_json::json!({"0": 109}));
    assert_eq!(body["updates"], serde_json::json!({}));
}
