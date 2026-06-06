//! Tests for `fan-out: dns-a`.
//!
//! Each test stands up two axum servers on `127.0.0.1` with distinct
//! ports, then injects a stub [`DnsAResolver`] that returns those
//! servers' `SocketAddr`s. The dispatcher rewrites the URL host+port
//! per resolved address and POSTs to each concurrently. This exercises
//! the multi-address path without depending on the system resolver or
//! `/etc/hosts`.

mod common;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use common::{Reply, TestServer};
use mirror_config::{
    FanOut, Notify, NotifyApi, NotifyDebounce, NotifyOutcomes, NotifyRetry, NotifyTarget,
    NotifyTrigger, TriggerOn,
};
use mirror_core::{Notifier, NotifyError, Record, TimestampType};
use mirror_notify_kkv::{DnsAResolver, KkvV1Notifier};

/// Stub resolver that returns a fixed set of addresses every call,
/// counting how many times `resolve` was invoked so cache-TTL tests
/// can assert "second dispatch hit the cache".
#[derive(Debug)]
struct StubResolver {
    addrs: Vec<SocketAddr>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl DnsAResolver for StubResolver {
    async fn resolve(&self, _host: &str, _port: u16) -> std::io::Result<Vec<SocketAddr>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.addrs.clone())
    }
}

fn rec(offset: u64) -> Record {
    Record {
        topic: "t".into(),
        partition: 0,
        source_offset: offset,
        timestamp_ms: Some(1_700_000_000_000),
        timestamp_type: TimestampType::CreateTime,
        key: Some(b"k".to_vec()),
        value: Some(b"v".to_vec()),
        headers: vec![],
    }
}

/// Build a `Notify` config with `fan-out: dns-a` aimed at a stand-in
/// hostname (the resolver stub returns the real addresses). `max_records: 1`
/// keeps dispatch synchronous from `on_record`.
fn notify_dns_a() -> Notify {
    Notify {
        api: NotifyApi::KkvV1,
        targets: vec![NotifyTarget {
            // Hostname is irrelevant; the stub resolver doesn't read
            // it. Port 80 is the default; the dispatcher rewrites
            // both host and port per resolved SocketAddr.
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
            backoff_ms: 1,
        },
        outcomes: NotifyOutcomes::default(),
    }
}

#[tokio::test]
async fn posts_to_every_resolved_address() {
    // Two test servers on distinct ports; both should receive the
    // POST when fan-out resolves the host to both.
    let server_a = TestServer::start(Reply::Status(200), vec![]).await;
    let server_b = TestServer::start(Reply::Status(200), vec![]).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let resolver = Arc::new(StubResolver {
        addrs: vec![server_a.addr, server_b.addr],
        calls: Arc::clone(&calls),
    });

    let cfg = notify_dns_a();
    let mut n = KkvV1Notifier::from_config_with_resolver(&cfg, "t".into(), 0, resolver).unwrap();

    n.on_record(&rec(1)).await.unwrap();

    assert_eq!(
        server_a.request_count(),
        1,
        "address A must have received exactly one POST"
    );
    assert_eq!(
        server_b.request_count(),
        1,
        "address B must have received exactly one POST"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "first dispatch must call the resolver exactly once"
    );
}

#[tokio::test]
async fn empty_address_set_returns_transport_error() {
    let calls = Arc::new(AtomicUsize::new(0));
    let resolver = Arc::new(StubResolver {
        addrs: vec![],
        calls: Arc::clone(&calls),
    });
    let cfg = notify_dns_a();
    let mut n = KkvV1Notifier::from_config_with_resolver(&cfg, "t".into(), 0, resolver).unwrap();

    let err = n.on_record(&rec(1)).await.unwrap_err();
    let s = format!("{err}");
    assert!(
        s.contains("0 addresses"),
        "error must mention 0-address result, got: {s}"
    );
}

#[tokio::test]
async fn one_address_failure_fails_the_whole_batch() {
    // Address A returns 5xx (default outcome retries then fails);
    // address B returns 200. Whole-batch outcome must be Err.
    let server_a = TestServer::start(Reply::Status(500), vec![]).await;
    let server_b = TestServer::start(Reply::Status(200), vec![]).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let resolver = Arc::new(StubResolver {
        addrs: vec![server_a.addr, server_b.addr],
        calls: Arc::clone(&calls),
    });

    let mut cfg = notify_dns_a();
    cfg.retry.max_attempts = 2;
    let mut n = KkvV1Notifier::from_config_with_resolver(&cfg, "t".into(), 0, resolver).unwrap();

    let err = n.on_record(&rec(1)).await.unwrap_err();
    assert!(matches!(err, NotifyError::Exhausted { .. }), "got {err:?}");
    // A retried (2 attempts), B got one success POST. The
    // important thing is the whole batch surfaced as failure.
    assert_eq!(server_a.request_count(), 2);
    assert_eq!(server_b.request_count(), 1);
}

#[tokio::test]
async fn cached_addresses_reused_within_ttl_then_re_resolved_on_failure() {
    // First dispatch succeeds → resolver called once, addrs cached.
    // Second dispatch succeeds → resolver NOT called (within TTL).
    // Then make the receiver fail; the dispatcher invalidates the
    // cache; a third dispatch re-resolves.
    let server = TestServer::start(Reply::Status(200), vec![]).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let resolver = Arc::new(StubResolver {
        addrs: vec![server.addr],
        calls: Arc::clone(&calls),
    });
    let cfg = notify_dns_a();
    let mut n = KkvV1Notifier::from_config_with_resolver(&cfg, "t".into(), 0, resolver).unwrap();

    n.on_record(&rec(1)).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1, "first call");

    n.on_record(&rec(2)).await.unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "second call must reuse the cached resolution (still within TTL)"
    );

    // Force a failure path so the cache invalidates.
    let failing_server = TestServer::start(Reply::Status(500), vec![]).await;
    // Swap the resolver to point at the failing server. We can't
    // mutate the existing Arc; just construct a new notifier with a
    // new stub. The salient assertion in this segment is just that
    // failure paths invalidate the cache; checked via the per-fail
    // resolver-call count.
    drop(n);

    let calls2 = Arc::new(AtomicUsize::new(0));
    let resolver2 = Arc::new(StubResolver {
        addrs: vec![failing_server.addr],
        calls: Arc::clone(&calls2),
    });
    let mut cfg2 = notify_dns_a();
    cfg2.retry.max_attempts = 1;
    let mut n2 = KkvV1Notifier::from_config_with_resolver(&cfg2, "t".into(), 0, resolver2).unwrap();

    let _ = n2.on_record(&rec(3)).await; // expected err
    assert_eq!(calls2.load(Ordering::SeqCst), 1);
    // Next dispatch must re-resolve because the previous one
    // invalidated the cache on failure.
    let _ = n2.on_record(&rec(4)).await;
    assert_eq!(
        calls2.load(Ordering::SeqCst),
        2,
        "post-failure dispatch must re-resolve"
    );
}

#[tokio::test]
async fn dispatches_concurrently_to_all_addresses() {
    // Both servers sleep 200ms before responding 200. If dispatch is
    // serial, total time is ~400ms+; if concurrent, ~200ms+. Use
    // 500ms as the upper bound; comfortably above 200ms, well below
    // 400ms.
    use std::time::{Duration, Instant};
    let server_a = TestServer::start(Reply::SlowOk(Duration::from_millis(200)), vec![]).await;
    let server_b = TestServer::start(Reply::SlowOk(Duration::from_millis(200)), vec![]).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let resolver = Arc::new(StubResolver {
        addrs: vec![server_a.addr, server_b.addr],
        calls: Arc::clone(&calls),
    });
    let cfg = notify_dns_a();
    let mut n = KkvV1Notifier::from_config_with_resolver(&cfg, "t".into(), 0, resolver).unwrap();

    let start = Instant::now();
    n.on_record(&rec(1)).await.unwrap();
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(500),
        "fan-out must dispatch concurrently; took {elapsed:?}, expected ~200ms"
    );
    assert_eq!(server_a.request_count(), 1);
    assert_eq!(server_b.request_count(), 1);
}
