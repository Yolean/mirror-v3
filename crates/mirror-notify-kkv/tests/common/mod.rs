//! Test helpers shared by the `mirror-notify-kkv` integration tests.
//!
//! The pattern: bind a tiny axum router on port 0, capture every
//! POST it receives (headers + body), and let the test script the
//! per-request status code response. The notifier-under-test points
//! at `127.0.0.1:<port>` and we assert on the captured requests.

// Each `tests/*.rs` binary compiles `common` independently and any
// unused helpers in *that* binary produce dead-code warnings. The
// helpers are used across binaries, so silence the per-binary noise.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use mirror_config::{
    FanOut, Notify, NotifyApi, NotifyDebounce, NotifyOutcomes, NotifyRetry, NotifyTarget,
    NotifyTrigger, TriggerOn,
};
use mirror_core::CacheState;
use tokio::sync::Mutex;

/// A single captured POST.
#[derive(Debug, Clone)]
pub struct CapturedRequest {
    pub path: String,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

/// What status code (or transport behaviour) the test server should
/// return for a given request, in order.
#[derive(Debug, Clone, Copy)]
pub enum Reply {
    /// Plain HTTP status reply.
    Status(u16),
    /// Sleep for `Duration` then return 200; used to trigger client
    /// timeouts when `notify.timeout-ms` is set below this.
    SlowOk(Duration),
}

pub struct ServerState {
    pub requests: Mutex<Vec<CapturedRequest>>,
    pub replies: Mutex<Vec<Reply>>,
    pub default_reply: Mutex<Reply>,
    /// Number of times the handler was invoked. Useful for asserting
    /// "no retry beyond max-attempts" from outside.
    pub request_count: AtomicUsize,
}

pub struct TestServer {
    pub addr: SocketAddr,
    pub state: Arc<ServerState>,
    _shutdown_tx: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<()>,
}

impl TestServer {
    /// Bind on 127.0.0.1:0 with the given `default_reply` used for
    /// every request, plus an optional per-request `Reply` queue
    /// applied before the default takes over.
    pub async fn start(default_reply: Reply, scripted: Vec<Reply>) -> Self {
        let state = Arc::new(ServerState {
            requests: Mutex::new(Vec::new()),
            replies: Mutex::new(scripted),
            default_reply: Mutex::new(default_reply),
            request_count: AtomicUsize::new(0),
        });
        let router = Router::new()
            .route("/{*path}", post(handle_post))
            .route("/", post(handle_post))
            .with_state(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let join = tokio::spawn(async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        TestServer {
            addr,
            state,
            _shutdown_tx: shutdown_tx,
            _join: join,
        }
    }

    pub async fn captured(&self) -> Vec<CapturedRequest> {
        self.state.requests.lock().await.clone()
    }

    pub fn request_count(&self) -> usize {
        self.state.request_count.load(Ordering::SeqCst)
    }
}

async fn handle_post(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    request: axum::extract::Request,
) -> (StatusCode, &'static str) {
    state.request_count.fetch_add(1, Ordering::SeqCst);
    let path = request.uri().path().to_string();
    let body = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .unwrap();
    state.requests.lock().await.push(CapturedRequest {
        path,
        headers,
        body: body.to_vec(),
    });
    let reply = {
        let mut q = state.replies.lock().await;
        if q.is_empty() {
            *state.default_reply.lock().await
        } else {
            q.remove(0)
        }
    };
    match reply {
        Reply::Status(code) => (
            StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            "",
        ),
        Reply::SlowOk(d) => {
            tokio::time::sleep(d).await;
            (StatusCode::OK, "")
        }
    }
}

/// `CacheState` whose mirror slot is already marked caught-up so the
/// notifier's per-mirror bootstrap_hwm gate lets every record through.
/// Use in any test whose focus isn't the readiness gate itself.
/// `register_mirror(name, 0)` declares an empty source partition, so
/// the slot's `caught_up` flag is `true` at registration time.
pub fn ready_cache(mirror_name: &str) -> Arc<CacheState> {
    let state = Arc::new(CacheState::new());
    state.register_mirror(mirror_name, 0);
    state
}

/// Build a `Notify` config with an explicit debounce window. Used by
/// the buffer tests where the default-helper's `max_records: 1`
/// would force per-record inline drains.
pub fn notify_pointing_at_debounced(
    addr: SocketAddr,
    outcomes: NotifyOutcomes,
    retry: NotifyRetry,
    timeout_ms: u64,
    debounce: NotifyDebounce,
) -> Notify {
    let mut n = notify_pointing_at(addr, outcomes, retry, timeout_ms);
    n.trigger.debounce = Some(debounce);
    n
}

/// Build a minimal `Notify` config pointed at the given local addr.
/// Tests override individual fields by mutating the returned value.
pub fn notify_pointing_at(
    addr: SocketAddr,
    outcomes: NotifyOutcomes,
    retry: NotifyRetry,
    timeout_ms: u64,
) -> Notify {
    Notify {
        api: NotifyApi::KkvV1,
        targets: vec![NotifyTarget {
            url: format!("http://{addr}"),
            path: None,
            fan_out: FanOut::None,
        }],
        trigger: NotifyTrigger {
            on: TriggerOn::SourceConsume,
            // `max_records: 1` keeps the default helper's
            // `on_record` dispatch synchronous so the existing
            // wire-format / outcomes tests can assert against
            // `server.captured()` immediately after on_record
            // returns. Debounce-specific tests configure their own
            // window via `notify_pointing_at_debounced`.
            debounce: Some(NotifyDebounce {
                max_records: 1,
                max_time_ms: 60_000,
            }),
        },
        timeout_ms,
        retry,
        outcomes,
    }
}
