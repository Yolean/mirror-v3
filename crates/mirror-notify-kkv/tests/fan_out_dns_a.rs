//! `fan-out: dns-a`: one target, every address its name resolves to,
//! each with its own undelivered key set.
//!
//! The servers listen on distinct `127.0.0.1` ports and a stub
//! [`DnsAResolver`] returns their addresses; tests change what it
//! returns to model pods coming, going, and DNS failing.

mod common;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common::{ready_cache, terminal_error, wait_until, AckRecorder, Reply, TestServer};
use mirror_config::{
    FanOut, FinalAction, Notify, NotifyApi, NotifyDebounce, NotifyOutcome, NotifyOutcomes,
    NotifyRetry, NotifyTarget, NotifyTrigger, TriggerOn,
};
use mirror_core::{Notifier, NotifyError, Record, TimestampType};
use mirror_notify_kkv::{DnsAResolver, KkvV1Notifier};
use serde_json::Value;

const WAIT: Duration = Duration::from_secs(5);

/// Returns whatever the test last set; `None` is a resolution failure.
struct StubResolver {
    addrs: Mutex<Option<Vec<SocketAddr>>>,
    calls: AtomicUsize,
}

impl StubResolver {
    fn new(addrs: Vec<SocketAddr>) -> Arc<Self> {
        Arc::new(Self {
            addrs: Mutex::new(Some(addrs)),
            calls: AtomicUsize::new(0),
        })
    }

    fn set(&self, addrs: Option<Vec<SocketAddr>>) {
        *self.addrs.lock().unwrap() = addrs;
    }
}

#[async_trait]
impl DnsAResolver for StubResolver {
    async fn resolve(&self, _host: &str, _port: u16) -> std::io::Result<Vec<SocketAddr>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.addrs
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| std::io::Error::other("stub: resolution failed"))
    }
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

fn keep() -> NotifyOutcome {
    NotifyOutcome {
        retry: true,
        final_: FinalAction::Skip,
    }
}

/// `fan-out: dns-a` at a stand-in name; `max_records: 1` makes every
/// record its own batch. `keep_failures` maps every failure outcome
/// to `skip` (kept and retried), as a deployment may configure it.
fn notify_dns_a(keep_failures: bool) -> Notify {
    let mut outcomes = NotifyOutcomes::default();
    if keep_failures {
        outcomes.timeout = keep();
        outcomes.connrefused = keep();
        outcomes.four_xx = keep();
        outcomes.five_xx = keep();
    }
    Notify {
        api: NotifyApi::KkvV1,
        targets: vec![NotifyTarget {
            url: "http://stub-host.invalid".into(),
            path: None,
            fan_out: FanOut::DnsA,
        }],
        trigger: NotifyTrigger {
            on: TriggerOn::SourceConsume,
            debounce: Some(NotifyDebounce {
                max_records: 1,
                max_time_ms: 60_000,
            }),
        },
        timeout_ms: 1000,
        retry: NotifyRetry {
            max_attempts: 3,
            backoff_ms: 5,
        },
        outcomes,
    }
}

fn notifier(cfg: &Notify, resolver: Arc<StubResolver>) -> (KkvV1Notifier, Arc<AckRecorder>) {
    let ack = Arc::new(AckRecorder::default());
    let n = KkvV1Notifier::from_config_with_resolver(
        cfg,
        "t".into(),
        0,
        ready_cache("m"),
        "m".into(),
        resolver,
    )
    .unwrap()
    .with_ack_sink(ack.clone());
    (n, ack)
}

async fn keys_received(server: &TestServer) -> Vec<String> {
    let mut keys = Vec::new();
    for r in server.captured().await {
        let body: Value = serde_json::from_slice(&r.body).unwrap();
        for k in body["updates"].as_object().unwrap().keys() {
            keys.push(k.clone());
        }
    }
    keys
}

#[tokio::test]
async fn posts_to_every_resolved_address() {
    let a = TestServer::start(Reply::Status(200), vec![]).await;
    let b = TestServer::start(Reply::Status(200), vec![]).await;
    let resolver = StubResolver::new(vec![a.addr, b.addr]);
    let (mut n, ack) = notifier(&notify_dns_a(false), resolver);

    n.on_record(&rec(1)).await.unwrap();
    wait_until("ack through 2", WAIT, || ack.get() == 2).await;
    assert_eq!(a.request_count(), 1);
    assert_eq!(b.request_count(), 1);
}

/// A target Service scaled to zero (no address) made
/// the first batch exit the mirror, and the restart crash-looped on
/// it. No address now means nothing to deliver.
#[tokio::test]
async fn no_address_means_nothing_to_deliver() {
    let resolver = StubResolver::new(vec![]);
    let (mut n, ack) = notifier(&notify_dns_a(false), resolver);
    n.on_record(&rec(4)).await.unwrap();
    wait_until("ack through 5", WAIT, || ack.get() == 5).await;
    n.shutdown().await.unwrap();
}

/// A failed resolution (DNS down, timeout) keeps the addresses known
/// from the last good one; it neither fails the mirror nor drops keys.
#[tokio::test]
async fn resolution_failure_keeps_the_known_addresses() {
    let a = TestServer::start(Reply::Status(200), vec![]).await;
    let resolver = StubResolver::new(vec![a.addr]);
    let (mut n, ack) = notifier(&notify_dns_a(false), Arc::clone(&resolver));
    n.on_record(&rec(1)).await.unwrap();
    wait_until("first ack", WAIT, || ack.get() == 2).await;

    resolver.set(None);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    n.on_record(&rec(2)).await.unwrap();
    wait_until("second ack", WAIT, || ack.get() == 3).await;
    assert_eq!(keys_received(&a).await, vec!["k1", "k2"]);
}

/// M1: an address that fails keeps its keys and gets them, merged with
/// newer ones, once it accepts; the other addresses are not held up,
/// and the committed offset waits for the slow one.
#[tokio::test]
async fn a_failing_address_keeps_its_keys_without_holding_up_the_others() {
    let a = TestServer::start(Reply::Status(200), vec![]).await;
    let b = TestServer::start(Reply::Status(200), vec![Reply::Status(503); 6]).await;
    let resolver = StubResolver::new(vec![a.addr, b.addr]);
    let (mut n, ack) = notifier(&notify_dns_a(true), resolver);

    n.on_record(&rec(1)).await.unwrap();
    n.on_record(&rec(2)).await.unwrap();
    n.on_record(&rec(3)).await.unwrap();
    wait_until("A has all three keys", WAIT, || a.request_count() >= 1).await;
    wait_until("ack through 4", WAIT, || ack.get() == 4).await;
    let mut a_keys = keys_received(&a).await;
    a_keys.sort();
    assert_eq!(a_keys, vec!["k1", "k2", "k3"]);
    let b_keys = keys_received(&b).await;
    for k in ["k1", "k2", "k3"] {
        assert!(b_keys.contains(&k.to_string()), "B lacks {k}: {b_keys:?}");
    }
    assert!(b.request_count() >= 7, "six failures, then a success");
}

/// M2: an address that resolves before its server listens (a starting
/// consumer behind a Service that publishes not-ready addresses) gets
/// every batch from its first resolution on, once it listens.
#[tokio::test]
async fn an_address_that_is_not_listening_yet_gets_every_batch_once_it_listens() {
    let a = TestServer::start(Reply::Status(200), vec![]).await;
    let late = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    let resolver = StubResolver::new(vec![a.addr, late]);
    let (mut n, ack) = notifier(&notify_dns_a(true), resolver);

    n.on_record(&rec(1)).await.unwrap();
    n.on_record(&rec(2)).await.unwrap();
    wait_until("A got both", WAIT, || a.request_count() >= 2).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(ack.get(), 0, "the late address holds the committed offset");

    let late_server = TestServer::start_on(late, Reply::Status(200)).await;
    wait_until("ack through 3", Duration::from_secs(40), || ack.get() == 3).await;
    let mut keys = keys_received(&late_server).await;
    keys.sort();
    assert_eq!(keys, vec!["k1", "k2"]);
}

/// A consumer that appears later (scale-up) gets the batches from its
/// first resolution on, not the ones before it existed.
#[tokio::test]
async fn a_new_address_gets_batches_from_its_first_resolution() {
    let a = TestServer::start(Reply::Status(200), vec![]).await;
    let b = TestServer::start(Reply::Status(200), vec![]).await;
    let resolver = StubResolver::new(vec![a.addr]);
    let (mut n, ack) = notifier(&notify_dns_a(false), Arc::clone(&resolver));

    n.on_record(&rec(1)).await.unwrap();
    wait_until("first ack", WAIT, || ack.get() == 2).await;
    resolver.set(Some(vec![a.addr, b.addr]));
    tokio::time::sleep(Duration::from_millis(1100)).await;
    n.on_record(&rec(2)).await.unwrap();
    wait_until("second ack", WAIT, || ack.get() == 3).await;
    assert_eq!(keys_received(&a).await, vec!["k1", "k2"]);
    assert_eq!(keys_received(&b).await, vec!["k2"]);
}

/// With the default outcomes (`5xx: retry, fail`) one address that
/// keeps failing still ends the mirror after the retry budget.
#[tokio::test]
async fn default_outcomes_still_fail_the_mirror() {
    let a = TestServer::start(Reply::Status(200), vec![]).await;
    let b = TestServer::start(Reply::Status(503), vec![]).await;
    let resolver = StubResolver::new(vec![a.addr, b.addr]);
    let (mut n, _ack) = notifier(&notify_dns_a(false), resolver);
    n.on_record(&rec(1)).await.unwrap();
    match terminal_error(&n, WAIT).await {
        NotifyError::Exhausted { attempts, .. } => assert_eq!(attempts, 3),
        other => panic!("expected Exhausted, got {other:?}"),
    }
    assert_eq!(a.request_count(), 1);
}
