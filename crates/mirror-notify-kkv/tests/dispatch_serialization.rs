//! Pin the dispatch-serialization contract between the background
//! timer drain and the inline (max-records / shutdown) drain.
//!
//! The two paths share one `AckSink`, and the supervisor's tracker
//! aggregates acks with `fetch_max`. If a later batch could dispatch
//! while an earlier batch is still in flight, the later batch's
//! success would ack past the earlier one; the periodic source
//! commit would then advance past records that were never delivered,
//! and a restart would suppress them forever (at-least-once
//! violated). The notifier therefore serializes take-dispatch-ack
//! under one lock and refuses to dispatch after a terminal error.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{notify_pointing_at_debounced, Reply, TestServer};
use mirror_config::{NotifyDebounce, NotifyOutcomes, NotifyRetry};
use mirror_core::{AckSink, CacheState, Notifier, Record, TimestampType};
use mirror_notify_kkv::KkvV1Notifier;

#[derive(Debug, Default)]
struct RecordingAck {
    values: Mutex<Vec<u64>>,
}

impl AckSink for RecordingAck {
    fn note_through(&self, through: u64) {
        self.values.lock().unwrap().push(through);
    }
}

fn ready_cache(name: &str) -> Arc<CacheState> {
    let s = Arc::new(CacheState::new());
    s.register_mirror(name, 0, None, false);
    s
}

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

/// Poll until the server has seen at least `n` requests. The timer
/// drain runs on its own task, so the test has to wait for its
/// dispatch to be provably in flight before scripting the interleave.
async fn wait_for_requests(server: &TestServer, n: usize, deadline: Duration) {
    let started = std::time::Instant::now();
    while server.request_count() < n {
        assert!(
            started.elapsed() < deadline,
            "server never reached {n} requests (got {})",
            server.request_count()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The dev2 follow-up scenario ("commit-during-debounce", flagged
/// 2026-06-10): timer takes batch A, A's target 5xxes into a long
/// retry cycle; meanwhile the consume loop fills the buffer to
/// max-records and inline-drains batch B. B must neither dispatch
/// nor ack: it blocks behind A, and once A fails terminally the
/// inline drain surfaces A's error instead of dispatching.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn later_inline_drain_cannot_ack_past_failing_timer_batch() {
    // Two scripted 500s cover exactly batch A's two attempts
    // (max-attempts: 2); anything after that would get the default
    // 200 and succeed, which is precisely what must not happen.
    let server = TestServer::start(
        Reply::Status(200),
        vec![Reply::Status(500), Reply::Status(500)],
    )
    .await;
    let retry = NotifyRetry {
        max_attempts: 2,
        // Long enough that batch B's inline drain trips while A is
        // parked between attempts.
        backoff_ms: 300,
    };
    let cfg = notify_pointing_at_debounced(
        server.addr,
        NotifyOutcomes::default(),
        retry,
        1000,
        NotifyDebounce {
            max_records: 3,
            max_time_ms: 20,
        },
    );
    let ack = Arc::new(RecordingAck::default());
    let mut notifier =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into())
            .unwrap()
            .with_ack_sink(ack.clone() as Arc<dyn AckSink>);

    // Record 0 sits alone in the buffer; the timer takes it as
    // batch A after max_time_ms and starts dispatching.
    notifier.on_record(&rec(0, "k0")).await.unwrap();
    wait_for_requests(&server, 1, Duration::from_secs(5)).await;

    // Batch B: the third record trips the inline max-records drain
    // while A is mid-retry.
    notifier.on_record(&rec(1, "k1")).await.unwrap();
    notifier.on_record(&rec(2, "k2")).await.unwrap();
    let err = notifier.on_record(&rec(3, "k3")).await.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("exhausted"),
        "the inline drain must surface batch A's terminal error, got: {msg}"
    );

    assert_eq!(
        server.request_count(),
        2,
        "only batch A's two attempts may reach the receiver; batch B must not dispatch"
    );
    assert!(
        ack.values.lock().unwrap().is_empty(),
        "no ack may be recorded once an earlier batch is undelivered"
    );
}

/// Happy-path ordering: when the timer batch eventually succeeds,
/// the blocked inline drain proceeds and acks arrive in source-offset
/// order. Guards against a future "fix" that unblocks the inline
/// path by skipping the lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_drain_blocks_behind_slow_timer_batch_and_acks_in_order() {
    let server = TestServer::start(
        Reply::Status(200),
        vec![Reply::SlowOk(Duration::from_millis(400))],
    )
    .await;
    let retry = NotifyRetry {
        max_attempts: 2,
        backoff_ms: 1,
    };
    let cfg = notify_pointing_at_debounced(
        server.addr,
        NotifyOutcomes::default(),
        retry,
        // Comfortably above SlowOk so the slow reply is a success,
        // not a client-side timeout.
        5000,
        NotifyDebounce {
            max_records: 3,
            max_time_ms: 20,
        },
    );
    let ack = Arc::new(RecordingAck::default());
    let mut notifier =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into())
            .unwrap()
            .with_ack_sink(ack.clone() as Arc<dyn AckSink>);

    notifier.on_record(&rec(0, "k0")).await.unwrap();
    wait_for_requests(&server, 1, Duration::from_secs(5)).await;

    notifier.on_record(&rec(1, "k1")).await.unwrap();
    notifier.on_record(&rec(2, "k2")).await.unwrap();
    // Blocks behind A's slow POST, then dispatches B.
    notifier.on_record(&rec(3, "k3")).await.unwrap();

    assert_eq!(server.request_count(), 2, "one POST per batch");
    assert_eq!(
        ack.values.lock().unwrap().clone(),
        vec![1, 4],
        "acks must arrive in batch (source-offset) order"
    );
}
