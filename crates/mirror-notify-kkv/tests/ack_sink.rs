//! Pin the ack contract of `KkvV1Notifier` and `FlushDispatcher`:
//!   * after a successful drain/POST, the installed `AckSink`
//!     receives `note_through(high_offset + 1)`,
//!   * after a retry-then-fail dispatch, no ack is recorded,
//!   * records suppressed by the per-mirror readiness gate don't
//!     buffer and therefore don't ack.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{notify_pointing_at, Reply, TestServer};
use mirror_config::{NotifyOutcomes, NotifyRetry};
use mirror_core::{AckSink, CacheState, FlushObserver, Notifier, Record, TimestampType};
use mirror_notify_kkv::{FlushDispatcher, KkvV1Notifier};

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
    // bootstrap_hwm = 0 => the slot is immediately ready.
    s.register_mirror(name, 0, None, false);
    s
}

fn warming_cache(name: &str, hwm: u64) -> Arc<CacheState> {
    let s = Arc::new(CacheState::new());
    s.register_mirror(name, hwm, None, false);
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

fn tight_retry() -> NotifyRetry {
    NotifyRetry {
        max_attempts: 2,
        backoff_ms: 1,
    }
}

#[tokio::test]
async fn kkv_v1_notifier_acks_through_high_offset_plus_one_on_success() {
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), tight_retry(), 1000);
    let ack = Arc::new(RecordingAck::default());
    let mut notifier =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into())
            .unwrap()
            .with_ack_sink(ack.clone() as Arc<dyn AckSink>);

    // `notify_pointing_at` defaults `max_records: 1` so the drain is
    // inline; one record per call.
    notifier.on_record(&rec(0, "k0")).await.unwrap();
    notifier.on_record(&rec(1, "k1")).await.unwrap();
    notifier.on_record(&rec(7, "k7")).await.unwrap();

    assert_eq!(
        ack.values.lock().unwrap().clone(),
        vec![1, 2, 8],
        "ack must be high_offset + 1 per successful drain"
    );
}

#[tokio::test]
async fn kkv_v1_notifier_does_not_ack_when_dispatch_exhausts() {
    // Server always returns 503; default 5xx outcome is retry: true,
    // final: fail. Dispatch returns `Exhausted`; the on_record call
    // surfaces it as an error. No ack must be recorded.
    let server = TestServer::start(Reply::Status(503), vec![]).await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), tight_retry(), 1000);
    let ack = Arc::new(RecordingAck::default());
    let mut notifier =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into())
            .unwrap()
            .with_ack_sink(ack.clone() as Arc<dyn AckSink>);

    let err = notifier.on_record(&rec(0, "k0")).await.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("exhausted") || msg.contains("Exhausted"),
        "got: {msg}"
    );
    assert!(
        ack.values.lock().unwrap().is_empty(),
        "no ack must be recorded when dispatch exhausts retries"
    );
}

#[tokio::test]
async fn kkv_v1_notifier_does_not_ack_when_suppressed_below_threshold() {
    // Bootstrap_hwm=10, so records with offset < 9 are suppressed
    // (the mirror's `caught_up` is false until last_applied + 1
    // reaches hwm). Suppressed records never enter the buffer,
    // therefore never dispatch and never ack.
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let cfg = notify_pointing_at(server.addr, NotifyOutcomes::default(), tight_retry(), 1000);
    let ack = Arc::new(RecordingAck::default());
    let mut notifier =
        KkvV1Notifier::from_config(&cfg, "t".into(), 0, warming_cache("m", 10), "m".into())
            .unwrap()
            .with_ack_sink(ack.clone() as Arc<dyn AckSink>);

    for off in 0..5 {
        notifier
            .on_record(&rec(off, &format!("k{off}")))
            .await
            .unwrap();
    }

    assert_eq!(
        server.request_count(),
        0,
        "no POST must fire while suppressed"
    );
    assert!(
        ack.values.lock().unwrap().is_empty(),
        "suppressed records must not feed the ack tracker"
    );
}

#[tokio::test]
async fn flush_dispatcher_acks_through_to_plus_one_on_success() {
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    use mirror_config::{FanOut, Notify, NotifyApi, NotifyTarget, NotifyTrigger, TriggerOn};
    let cfg = Notify {
        api: NotifyApi::KkvV1,
        targets: vec![NotifyTarget {
            url: format!("http://{}", server.addr),
            path: None,
            fan_out: FanOut::None,
        }],
        trigger: NotifyTrigger {
            on: TriggerOn::DestinationFlush,
            debounce: None,
        },
        timeout_ms: 1000,
        retry: tight_retry(),
        outcomes: NotifyOutcomes::default(),
    };
    let ack = Arc::new(RecordingAck::default());
    let dispatcher =
        FlushDispatcher::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into())
            .unwrap()
            .with_ack_sink(ack.clone() as Arc<dyn AckSink>);

    // Drive the observer; each call enqueues a POST.
    dispatcher.on_flushed(0, 4);
    dispatcher.on_flushed(5, 9);

    // The drainer is async; poll until both POSTs land.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while server.request_count() < 2 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(server.request_count(), 2);

    // Drainer fires note_through synchronously inside the loop;
    // poll briefly until both values appear.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let snapshot = ack.values.lock().unwrap().clone();
        if snapshot.len() >= 2 {
            assert_eq!(
                snapshot,
                vec![5, 10],
                "destination-flush acks through to + 1 per successful POST"
            );
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("ack didn't arrive: {snapshot:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn flush_dispatcher_does_not_ack_when_dispatch_exhausts() {
    let server = TestServer::start(Reply::Status(503), vec![]).await;
    use mirror_config::{FanOut, Notify, NotifyApi, NotifyTarget, NotifyTrigger, TriggerOn};
    let cfg = Notify {
        api: NotifyApi::KkvV1,
        targets: vec![NotifyTarget {
            url: format!("http://{}", server.addr),
            path: None,
            fan_out: FanOut::None,
        }],
        trigger: NotifyTrigger {
            on: TriggerOn::DestinationFlush,
            debounce: None,
        },
        timeout_ms: 1000,
        retry: tight_retry(),
        outcomes: NotifyOutcomes::default(),
    };
    let ack = Arc::new(RecordingAck::default());
    let dispatcher =
        FlushDispatcher::from_config(&cfg, "t".into(), 0, ready_cache("m"), "m".into())
            .unwrap()
            .with_ack_sink(ack.clone() as Arc<dyn AckSink>);

    dispatcher.on_flushed(0, 9);
    // Wait long enough for the drainer to exhaust retries
    // (`max_attempts=2`, `backoff_ms=1`) and stash the error.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while dispatcher.last_error().await.is_none() && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        ack.values.lock().unwrap().is_empty(),
        "no ack when dispatch exhausts: {:?}",
        ack.values.lock().unwrap()
    );
}
