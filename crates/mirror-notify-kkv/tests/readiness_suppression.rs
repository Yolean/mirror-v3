//! Pin the per-mirror bootstrap-hwm suppression gate for both notify
//! triggers. `KkvV1Notifier::on_record` and
//! `FlushDispatcher::on_flushed` must drop events whose mirror slot
//! in `CacheState` has not yet flipped to `caught_up`. Maps onto the
//! legacy kkv `KafkaCache` Stage gate that suppressed push
//! notifications until `Polling`. Without this, a cold restart fans
//! historical-replay updates out to every consumer pod and breaks
//! the cache-invalidation contract for the live view.

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
async fn source_consume_suppresses_until_caught_up() {
    // Mirror "m" needs to see offset hwm-1 (100) before its slot
    // flips. Records at 50 and 99 (both pre-flip) must be silently
    // dropped; the record at 100 flips the slot via the destination
    // write path's `apply_record` (100 + 1 >= 101), after which 100
    // and 101 dispatch as single-record POSTs (debounce.max_records=1
    // in the helper).
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), fast_retry(), 1000);

    let cache = Arc::new(CacheState::new());
    cache.register_mirror("m", 101, false);
    let mut notifier =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, Arc::clone(&cache), "m".into()).unwrap();

    // Pre-watermark: simulate the run loop driving both the cache
    // (via TeeSink.apply_record) and the notifier per record. Below
    // the hwm both are no-ops on the wire.
    for offset in [50_u64, 99] {
        let r = rec(offset, &format!("k{offset}"));
        cache.apply_record("m", &r);
        notifier.on_record(&r).await.expect("suppressed: Ok(())");
    }
    assert!(
        !cache.is_mirror_ready("m"),
        "still 1 offset short of hwm 101 (last_offset+1 = 101 needed)"
    );
    assert_eq!(
        server.request_count(),
        0,
        "no POST may go out before caught_up"
    );

    // Offset 100 crosses the threshold (100 + 1 >= 101). apply_record
    // flips the slot, on_record then dispatches the record.
    let r100 = rec(100, "k100");
    cache.apply_record("m", &r100);
    assert!(cache.is_mirror_ready("m"), "offset 100 flips the slot");
    notifier.on_record(&r100).await.expect("post-hwm dispatch");

    let r101 = rec(101, "k101");
    cache.apply_record("m", &r101);
    notifier.on_record(&r101).await.expect("post-hwm dispatch");

    let captured = server.captured().await;
    assert_eq!(
        captured.len(),
        2,
        "exactly the two post-hwm records must POST"
    );
    let body0: Value = serde_json::from_slice(&captured[0].body).unwrap();
    assert_eq!(body0["updates"], serde_json::json!({"k100": null}));
    assert_eq!(body0["offsets"], serde_json::json!({"0": 100}));
    let body1: Value = serde_json::from_slice(&captured[1].body).unwrap();
    assert_eq!(body1["updates"], serde_json::json!({"k101": null}));
    assert_eq!(body1["offsets"], serde_json::json!({"0": 101}));
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
async fn destination_flush_suppresses_until_caught_up() {
    // Same gate, different trigger surface. `on_flushed` is sync; the
    // drainer is a background task. Flushes arriving before the
    // mirror's slot flips must never make it onto the channel; the
    // post-flip flush must POST.
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_dest_flush(server.addr);

    let cache = Arc::new(CacheState::new());
    cache.register_mirror("m", 101, false);
    let dispatcher =
        FlushDispatcher::from_config(&cfg, "t".into(), 0, Arc::clone(&cache), "m".into())
            .expect("must build");

    // Two pre-watermark flushes are dropped at the gate; channel
    // never sees them, drainer task stays idle.
    dispatcher.on_flushed(0, 49);
    dispatcher.on_flushed(50, 99);
    // Give the (idle) drainer a moment to prove no POST happens.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        server.request_count(),
        0,
        "no POST may go out before caught_up"
    );

    // Flip the slot via apply_record at offset hwm-1 (100 + 1 >= 101),
    // matching what TeeSink does on the production write path. Then
    // drive a flush.
    let r100 = rec(100, "k100");
    cache.apply_record("m", &r100);
    assert!(cache.is_mirror_ready("m"));
    dispatcher.on_flushed(100, 109);

    let captured = wait_for_requests(&server, 1, Duration::from_secs(2)).await;
    assert_eq!(captured.len(), 1, "only the post-hwm flush dispatches");
    let body: Value = serde_json::from_slice(&captured[0].body).unwrap();
    assert_eq!(body["offsets"], serde_json::json!({"0": 109}));
    assert_eq!(body["updates"], serde_json::json!({}));
}
