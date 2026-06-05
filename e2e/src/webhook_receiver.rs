//! In-process axum webhook receiver used by the notify e2e tests.
//!
//! Stands up an HTTP server on `127.0.0.1:0` that records every
//! `POST /kafka-keyvalue/v1/updates` (and any other path) into a
//! shared state vector. Tests build a `notify` config pointing at
//! the server, run a real mirror against a real Kafka, and assert
//! on the captured POSTs.
//!
//! This is the e2e counterpart of `mirror-notify-kkv`'s in-crate
//! `tests/common/mod.rs` axum harness; lifted out here so the e2e
//! tests can share it without depending on the notify crate's
//! test-only modules.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use tokio::sync::Mutex;

/// One captured POST: path, headers, body bytes.
#[derive(Debug, Clone)]
pub struct CapturedRequest {
    pub path: String,
    pub headers: HeaderMap,
    pub body: Bytes,
}

#[derive(Default)]
struct State_ {
    captured: Mutex<Vec<CapturedRequest>>,
    /// Number of times the handler was invoked (incremented BEFORE
    /// the request is captured, so tests can poll for "at least N
    /// requests have hit me" without taking the captured-vec lock).
    count: AtomicUsize,
    /// HTTP status to return for every request. Default 200.
    reply_status: Mutex<StatusCode>,
}

pub struct WebhookReceiver {
    pub addr: SocketAddr,
    state: Arc<State_>,
    _shutdown_tx: tokio::sync::oneshot::Sender<()>,
    _handle: tokio::task::JoinHandle<()>,
}

impl WebhookReceiver {
    /// Bind a new receiver on `127.0.0.1:0`. The returned address is
    /// safe to plug straight into a `notify.targets[].url`.
    pub async fn start() -> Self {
        let state = Arc::new(State_ {
            reply_status: Mutex::new(StatusCode::OK),
            ..Default::default()
        });
        let router = Router::new()
            .route("/{*path}", post(handle))
            .route("/", post(handle))
            .with_state(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        Self {
            addr,
            state,
            _shutdown_tx: shutdown_tx,
            _handle: handle,
        }
    }

    pub async fn captured(&self) -> Vec<CapturedRequest> {
        self.state.captured.lock().await.clone()
    }

    pub fn request_count(&self) -> usize {
        self.state.count.load(Ordering::SeqCst)
    }

    /// Wait until the receiver has captured at least `n` requests, or
    /// `timeout` elapses. Returns the captured set.
    pub async fn wait_for(&self, n: usize, timeout: Duration) -> Vec<CapturedRequest> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if self.request_count() >= n {
                return self.captured().await;
            }
            if std::time::Instant::now() >= deadline {
                let captured = self.captured().await;
                panic!(
                    "webhook receiver: timed out waiting for {n} POSTs (got {})",
                    captured.len()
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Make subsequent requests return this status. Useful for
    /// retry / outage-style tests.
    pub async fn set_reply_status(&self, status: u16) {
        let mut s = self.state.reply_status.lock().await;
        *s = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    }
}

async fn handle(
    State(state): State<Arc<State_>>,
    headers: HeaderMap,
    request: Request,
) -> (StatusCode, &'static str) {
    state.count.fetch_add(1, Ordering::SeqCst);
    let path = request.uri().path().to_string();
    let body = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .unwrap_or_default();
    state.captured.lock().await.push(CapturedRequest {
        path,
        headers,
        body,
    });
    let status = *state.reply_status.lock().await;
    (status, "")
}
