//! The committed offset never passes an undelivered batch.
//!
//! The supervisor's tracker keeps the highest `note_through` it is
//! given (`fetch_max`), and the periodic source commit is the offset a
//! restart re-delivers from. If a later batch could be acked while an
//! earlier one is still undelivered to some address, a restart would
//! suppress the earlier one forever. Per-address key sets make this
//! hold by construction: a failed batch is merged into the address's
//! next attempt, and the address is acked only through what it
//! accepted.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{notify_pointing_at, ready_cache, wait_until, AckRecorder, Reply, TestServer};
use mirror_config::{FinalAction, NotifyOutcome, NotifyOutcomes, NotifyRetry};
use mirror_core::{AckSink, Notifier, Record, TimestampType};
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

/// Records every value, to assert none was reported early.
#[derive(Default)]
struct AllAcks(std::sync::Mutex<Vec<u64>>);

impl AckSink for AllAcks {
    fn note_through(&self, through: u64) {
        self.0.lock().unwrap().push(through);
    }
}

#[tokio::test]
async fn a_later_batch_is_not_acked_past_a_failed_earlier_one() {
    let outcomes = NotifyOutcomes {
        five_xx: NotifyOutcome {
            retry: true,
            final_: FinalAction::Skip,
        },
        ..NotifyOutcomes::default()
    };
    // k0 is accepted; the POST of k1 alone gets a 503; k5 arrives
    // during the 200 ms backoff, so the retry carries both.
    let server = TestServer::start(
        Reply::Status(200),
        vec![Reply::Status(200), Reply::Status(503)],
    )
    .await;
    let cfg = notify_pointing_at(
        server.addr,
        outcomes,
        NotifyRetry {
            max_attempts: 3,
            backoff_ms: 200,
        },
        1000,
    );
    let acks = Arc::new(AllAcks::default());
    let mut n = KkvV1Notifier::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into())
        .unwrap()
        .with_ack_sink(acks.clone());

    n.on_record(&rec(0, "k0")).await.unwrap();
    wait_until("k0 delivered", Duration::from_secs(5), || {
        acks.0.lock().unwrap().last() == Some(&1)
    })
    .await;
    n.on_record(&rec(1, "k1")).await.unwrap();
    wait_until("the 503", Duration::from_secs(5), || {
        server.request_count() == 2
    })
    .await;
    n.on_record(&rec(5, "k5")).await.unwrap();
    wait_until("ack through 6", Duration::from_secs(5), || {
        acks.0.lock().unwrap().last() == Some(&6)
    })
    .await;
    assert_eq!(
        *acks.0.lock().unwrap(),
        vec![1, 6],
        "never 2 before k1 was accepted"
    );
    let last: Value = serde_json::from_slice(&server.captured().await[2].body).unwrap();
    assert_eq!(last["updates"], serde_json::json!({"k1": null, "k5": null}));
    assert_eq!(last["offsets"], serde_json::json!({"0": 5}));
}

/// A consumer that hangs does not stall the consume loop: `on_record`
/// returns at once whatever the targets do (the inline
/// drain blocked the loop ~26 s per batch on a hung pod).
#[tokio::test]
async fn on_record_never_waits_for_a_target() {
    let server = TestServer::start(Reply::SlowOk(Duration::from_secs(2)), vec![]).await;
    let cfg = notify_pointing_at(
        server.addr,
        NotifyOutcomes::default(),
        NotifyRetry {
            max_attempts: 1,
            backoff_ms: 1,
        },
        5000,
    );
    let ack = Arc::new(AckRecorder::default());
    let mut n = KkvV1Notifier::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into())
        .unwrap()
        .with_ack_sink(ack.clone());
    let started = std::time::Instant::now();
    for o in 0..50 {
        n.on_record(&rec(o, &format!("k{o}"))).await.unwrap();
    }
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "50 records took {:?}",
        started.elapsed()
    );
    wait_until("ack through 50", Duration::from_secs(10), || {
        ack.get() == 50
    })
    .await;
}
