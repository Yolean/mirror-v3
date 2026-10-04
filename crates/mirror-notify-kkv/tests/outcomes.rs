//! Pin the (retry × final-action) cells across the six outcome buckets
//! from `WEBHOOKS.md § Outcomes and retry policy`, as delivered per
//! address: `fail` ends delivery with a terminal error after the retry
//! budget, `accept` counts the batch as delivered after it, and `skip`
//! keeps the batch for the address and retries until it is accepted.

mod common;

use std::time::Duration;

use std::sync::Arc;

use common::{
    notify_pointing_at, ready_cache, terminal_error, wait_until, AckRecorder, Reply, TestServer,
};
use mirror_config::{FinalAction, NotifyOutcome, NotifyOutcomes, NotifyRetry};
use mirror_core::{Notifier, NotifyError, Record, TimestampType};
use mirror_notify_kkv::KkvV1Notifier;

const WAIT: Duration = Duration::from_secs(5);

fn notifier(cfg: &mirror_config::Notify) -> (KkvV1Notifier, Arc<AckRecorder>) {
    let ack = Arc::new(AckRecorder::default());
    let n = KkvV1Notifier::from_config(cfg, "t".into(), 0, ready_cache("m"), "m".into())
        .unwrap()
        .with_ack_sink(ack.clone());
    (n, ack)
}

fn rec(offset: u64) -> Record {
    Record {
        topic: "t".into(),
        partition: 0,
        source_offset: offset,
        timestamp_ms: Some(1_700_000_000_000),
        timestamp_type: TimestampType::CreateTime,
        key: Some(format!("k{offset}").into_bytes()),
        value: Some(b"v".to_vec()),
        headers: vec![],
    }
}

/// Tight retry policy so the timeout tests don't drag.
fn retry(attempts: u32) -> NotifyRetry {
    NotifyRetry {
        max_attempts: attempts,
        backoff_ms: 1,
    }
}

/// Build an outcomes table that maps every bucket the test exercises
/// to a single `(retry, final)` pair, leaving the rest at defaults.
fn outcomes_overriding(target: TargetBucket, policy: NotifyOutcome) -> NotifyOutcomes {
    let mut o = NotifyOutcomes::default();
    match target {
        TargetBucket::Timeout => o.timeout = policy,
        TargetBucket::ConnRefused => o.connrefused = policy,
        TargetBucket::TwoXx => o.two_xx = policy,
        TargetBucket::ThreeXx => o.three_xx = policy,
        TargetBucket::FourXx => o.four_xx = policy,
        TargetBucket::FiveXx => o.five_xx = policy,
    }
    o
}

#[derive(Clone, Copy)]
#[allow(dead_code)] // variants exist for completeness; not every one is exercised here.
enum TargetBucket {
    Timeout,
    ConnRefused,
    TwoXx,
    ThreeXx,
    FourXx,
    FiveXx,
}

// ----------------- 2xx -----------------

#[tokio::test]
async fn outcome_2xx_default_accepts_after_one_attempt() {
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), retry(5), 1000);
    let (mut n, ack) = notifier(&cfg);

    n.on_record(&rec(1)).await.unwrap();
    wait_until("ack through 2", WAIT, || ack.get() == 2).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(server.request_count(), 1, "2xx must not retry");
}

// ----------------- 4xx -----------------

#[tokio::test]
async fn outcome_4xx_default_fails_immediately() {
    let server = TestServer::start(Reply::Status(404), vec![]).await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), retry(5), 1000);
    let (mut n, ack) = notifier(&cfg);

    n.on_record(&rec(1)).await.unwrap();
    let err = terminal_error(&n, WAIT).await;
    assert!(
        matches!(err, NotifyError::Exhausted { attempts: 1, .. }),
        "got {err:?}"
    );
    assert_eq!(server.request_count(), 1, "default 4xx is retry: false");
    assert_eq!(ack.get(), 0, "a failed batch is not acked");
}

/// `skip` used to drop the batch ("targets routinely 404 during rolling
/// restart, don't crash on that"). With one replica a dropped push is
/// never healed, so it now keeps the batch for the address and retries
/// it until the address accepts; it still never fails the mirror.
#[tokio::test]
async fn outcome_4xx_with_skip_keeps_the_batch_until_accepted() {
    let outcomes = outcomes_overriding(
        TargetBucket::FourXx,
        NotifyOutcome {
            retry: false,
            final_: FinalAction::Skip,
        },
    );
    let server = TestServer::start(
        Reply::Status(200),
        vec![Reply::Status(404), Reply::Status(404)],
    )
    .await;
    let cfg = notify_pointing_at(server.addr, outcomes, retry(5), 1000);
    let (mut n, ack) = notifier(&cfg);

    n.on_record(&rec(1)).await.unwrap();
    wait_until("ack through 2", WAIT, || ack.get() == 2).await;
    assert_eq!(server.request_count(), 3);
    let bodies = server.captured().await;
    assert!(bodies
        .iter()
        .all(|r| String::from_utf8_lossy(&r.body).contains("\"k1\"")));
}

#[tokio::test]
async fn outcome_4xx_with_retry_and_accept_treats_as_delivered_after_exhaustion() {
    // Unusual combination but spec-permitted (`retry: true, final:
    // accept`).
    let outcomes = outcomes_overriding(
        TargetBucket::FourXx,
        NotifyOutcome {
            retry: true,
            final_: FinalAction::Accept,
        },
    );
    let server = TestServer::start(Reply::Status(400), vec![]).await;
    let cfg = notify_pointing_at(server.addr, outcomes, retry(3), 1000);
    let (mut n, ack) = notifier(&cfg);

    n.on_record(&rec(1)).await.unwrap();
    wait_until("ack through 2", WAIT, || ack.get() == 2).await;
    assert_eq!(
        server.request_count(),
        3,
        "must exhaust the retry budget (3 attempts) before accepting"
    );
}

// ----------------- 5xx -----------------

#[tokio::test]
async fn outcome_5xx_default_retries_then_fails() {
    let server = TestServer::start(Reply::Status(503), vec![]).await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), retry(4), 1000);
    let (mut n, _ack) = notifier(&cfg);

    n.on_record(&rec(1)).await.unwrap();
    match terminal_error(&n, WAIT).await {
        NotifyError::Exhausted { attempts, .. } => assert_eq!(attempts, 4),
        other => panic!("expected Exhausted, got {other:?}"),
    }
    assert_eq!(server.request_count(), 4, "must hit max-attempts first");
    // The next record surfaces nothing more: the watch consumed it.
    n.on_record(&rec(2)).await.unwrap();
}

#[tokio::test]
async fn outcome_5xx_recovers_when_server_starts_returning_2xx() {
    let server = TestServer::start(
        Reply::Status(200),
        vec![Reply::Status(503), Reply::Status(503)],
    )
    .await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), retry(5), 1000);
    let (mut n, ack) = notifier(&cfg);

    n.on_record(&rec(1)).await.unwrap();
    wait_until("ack through 2", WAIT, || ack.get() == 2).await;
    assert_eq!(server.request_count(), 3, "two retries plus the success");
}

#[tokio::test]
async fn outcome_5xx_with_skip_keeps_retrying_past_the_budget() {
    let outcomes = outcomes_overriding(
        TargetBucket::FiveXx,
        NotifyOutcome {
            retry: true,
            final_: FinalAction::Skip,
        },
    );
    let mut scripted = vec![Reply::Status(500); 5];
    scripted.push(Reply::Status(200));
    let server = TestServer::start(Reply::Status(200), scripted).await;
    let cfg = notify_pointing_at(server.addr, outcomes, retry(3), 1000);
    let (mut n, ack) = notifier(&cfg);

    n.on_record(&rec(1)).await.unwrap();
    wait_until("ack through 2", WAIT, || ack.get() == 2).await;
    assert_eq!(server.request_count(), 6);
}

// ----------------- 3xx -----------------

#[tokio::test]
async fn outcome_3xx_default_fails_immediately() {
    // A webhook receiver shouldn't be redirecting; default policy is
    // surface it loudly.
    let server = TestServer::start(Reply::Status(301), vec![]).await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), retry(5), 1000);
    let (mut n, _ack) = notifier(&cfg);

    n.on_record(&rec(1)).await.unwrap();
    let err = terminal_error(&n, WAIT).await;
    assert!(
        matches!(err, NotifyError::Exhausted { attempts: 1, .. }),
        "got {err:?}"
    );
    assert_eq!(server.request_count(), 1);
}

// ----------------- timeout -----------------

#[tokio::test]
async fn outcome_timeout_default_retries_then_fails() {
    // Server sleeps 200ms; client timeout is 30ms. Every attempt
    // times out. Default outcome is retry: true, final: fail.
    let server = TestServer::start(Reply::SlowOk(Duration::from_millis(200)), vec![]).await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), retry(3), 30);
    let (mut n, _ack) = notifier(&cfg);

    n.on_record(&rec(1)).await.unwrap();
    match terminal_error(&n, WAIT).await {
        NotifyError::Exhausted { attempts, .. } => assert_eq!(attempts, 3),
        other => panic!("expected Exhausted, got {other:?}"),
    }
    assert_eq!(server.request_count(), 3);
}

#[tokio::test]
async fn outcome_timeout_with_no_retry_fails_after_first_attempt() {
    let outcomes = outcomes_overriding(
        TargetBucket::Timeout,
        NotifyOutcome {
            retry: false,
            final_: FinalAction::Fail,
        },
    );
    let server = TestServer::start(Reply::SlowOk(Duration::from_millis(200)), vec![]).await;
    let cfg = notify_pointing_at(server.addr, outcomes, retry(5), 30);
    let (mut n, _ack) = notifier(&cfg);

    n.on_record(&rec(1)).await.unwrap();
    let err = terminal_error(&n, WAIT).await;
    assert!(
        matches!(err, NotifyError::Exhausted { attempts: 1, .. }),
        "got {err:?}"
    );
    assert_eq!(
        server.request_count(),
        1,
        "must not retry under retry: false"
    );
}

// ----------------- connrefused -----------------

#[tokio::test]
async fn outcome_connrefused_default_retries_then_fails() {
    // No server bound; 127.0.0.1:1 reliably refuses on Unix.
    let addr: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
    let cfg = notify_pointing_at(addr, NotifyOutcomes::default(), retry(3), 1000);
    let (mut n, _ack) = notifier(&cfg);

    n.on_record(&rec(1)).await.unwrap();
    match terminal_error(&n, WAIT).await {
        NotifyError::Exhausted { attempts, .. } => assert_eq!(attempts, 3),
        other => panic!("expected Exhausted, got {other:?}"),
    }
}
