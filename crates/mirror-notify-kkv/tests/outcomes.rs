//! Pin every (retry × final-action) combination across the six
//! outcome buckets from `WEBHOOKS.md § Outcomes and retry policy`.
//! The matrix is intentionally orthogonal; the user-facing knob is
//! "any of `accept | skip | fail` for any outcome, with or without
//! retry first"; so each cell needs a test.

mod common;

use std::time::Duration;

use common::{notify_pointing_at, Reply, TestServer};
use mirror_config::{FinalAction, NotifyOutcome, NotifyOutcomes, NotifyRetry};
use mirror_core::{Notifier, NotifyError, Record, TimestampType};
use mirror_notify_kkv::KkvV1Notifier;

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
    let mut n = KkvV1Notifier::from_config(&cfg, "t".into(), 0).unwrap();

    n.on_record(&rec(1)).await.expect("2xx must accept");
    assert_eq!(
        server.request_count(),
        1,
        "2xx must not retry under the default policy"
    );
}

// ----------------- 4xx -----------------

#[tokio::test]
async fn outcome_4xx_default_fails_immediately() {
    let server = TestServer::start(Reply::Status(404), vec![]).await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), retry(5), 1000);
    let mut n = KkvV1Notifier::from_config(&cfg, "t".into(), 0).unwrap();

    let err = n.on_record(&rec(1)).await.unwrap_err();
    assert!(
        matches!(err, NotifyError::Exhausted { attempts: 1, .. }),
        "got {err:?}"
    );
    assert_eq!(server.request_count(), 1, "default 4xx is retry: false");
}

#[tokio::test]
async fn outcome_4xx_with_skip_drops_batch_silently() {
    // "Targets routinely 404 during rolling restart, don't crash on
    // that"; the spec-named knob.
    let outcomes = outcomes_overriding(
        TargetBucket::FourXx,
        NotifyOutcome {
            retry: false,
            final_: FinalAction::Skip,
        },
    );
    let server = TestServer::start(Reply::Status(404), vec![]).await;
    let cfg = notify_pointing_at(server.addr, outcomes, retry(5), 1000);
    let mut n = KkvV1Notifier::from_config(&cfg, "t".into(), 0).unwrap();

    n.on_record(&rec(1)).await.expect("skip must surface as Ok");
    assert_eq!(server.request_count(), 1);
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
    let mut n = KkvV1Notifier::from_config(&cfg, "t".into(), 0).unwrap();

    n.on_record(&rec(1))
        .await
        .expect("retry+accept must Ok after exhaustion");
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
    let mut n = KkvV1Notifier::from_config(&cfg, "t".into(), 0).unwrap();

    let err = n.on_record(&rec(1)).await.unwrap_err();
    match err {
        NotifyError::Exhausted { attempts, .. } => assert_eq!(attempts, 4),
        other => panic!("expected Exhausted, got {other:?}"),
    }
    assert_eq!(
        server.request_count(),
        4,
        "must hit max-attempts before giving up"
    );
}

#[tokio::test]
async fn outcome_5xx_recovers_when_server_starts_returning_2xx() {
    // First two attempts return 503, third returns 200. Retry budget
    // allows it, so the batch ultimately succeeds with no error.
    let server = TestServer::start(
        Reply::Status(200),
        vec![Reply::Status(503), Reply::Status(503)],
    )
    .await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), retry(5), 1000);
    let mut n = KkvV1Notifier::from_config(&cfg, "t".into(), 0).unwrap();

    n.on_record(&rec(1))
        .await
        .expect("must succeed on attempt 3");
    assert_eq!(server.request_count(), 3, "two retries plus the success");
}

#[tokio::test]
async fn outcome_5xx_with_skip_drops_batch_after_exhaustion() {
    // "Receiver is flaky, never fail the mirror on it"; pure
    // best-effort notify.
    let outcomes = outcomes_overriding(
        TargetBucket::FiveXx,
        NotifyOutcome {
            retry: true,
            final_: FinalAction::Skip,
        },
    );
    let server = TestServer::start(Reply::Status(500), vec![]).await;
    let cfg = notify_pointing_at(server.addr, outcomes, retry(3), 1000);
    let mut n = KkvV1Notifier::from_config(&cfg, "t".into(), 0).unwrap();

    n.on_record(&rec(1))
        .await
        .expect("skip on exhaustion must Ok");
    assert_eq!(server.request_count(), 3);
}

// ----------------- 3xx -----------------

#[tokio::test]
async fn outcome_3xx_default_fails_immediately() {
    // A webhook receiver shouldn't be redirecting; default policy is
    // surface it loudly.
    let server = TestServer::start(Reply::Status(301), vec![]).await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), retry(5), 1000);
    let mut n = KkvV1Notifier::from_config(&cfg, "t".into(), 0).unwrap();

    let err = n.on_record(&rec(1)).await.unwrap_err();
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
    let mut n = KkvV1Notifier::from_config(&cfg, "t".into(), 0).unwrap();

    let err = n.on_record(&rec(1)).await.unwrap_err();
    match err {
        NotifyError::Exhausted { attempts, .. } => assert_eq!(attempts, 3),
        other => panic!("expected Exhausted, got {other:?}"),
    }
    assert_eq!(server.request_count(), 3);
}

#[tokio::test]
async fn outcome_timeout_with_no_retry_fails_after_first_attempt() {
    // "Fail fast on slow receivers instead of waiting through retry"
    //; the spec-named knob.
    let outcomes = outcomes_overriding(
        TargetBucket::Timeout,
        NotifyOutcome {
            retry: false,
            final_: FinalAction::Fail,
        },
    );
    let server = TestServer::start(Reply::SlowOk(Duration::from_millis(200)), vec![]).await;
    let cfg = notify_pointing_at(server.addr, outcomes, retry(5), 30);
    let mut n = KkvV1Notifier::from_config(&cfg, "t".into(), 0).unwrap();

    let err = n.on_record(&rec(1)).await.unwrap_err();
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
    use mirror_config::{FanOut, NotifyTarget};
    // No server bound; 127.0.0.1:1 reliably refuses on Unix.
    let addr: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
    let mut cfg = notify_pointing_at(addr, NotifyOutcomes::default(), retry(3), 1000);
    // Sanity: the fan_out / path settings are exercised even though
    // there's no server here.
    cfg.targets = vec![NotifyTarget {
        url: format!("http://{addr}"),
        path: None,
        fan_out: FanOut::None,
    }];
    let mut n = KkvV1Notifier::from_config(&cfg, "t".into(), 0).unwrap();

    let err = n.on_record(&rec(1)).await.unwrap_err();
    match err {
        NotifyError::Exhausted { attempts, .. } => assert_eq!(attempts, 3),
        other => panic!("expected Exhausted, got {other:?}"),
    }
}
