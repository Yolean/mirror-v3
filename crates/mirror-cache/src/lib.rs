//! HTTP surface for mirror-v3's KKV-compatibility mode.
//!
//! Hosts a drop-in replacement for the `GET /cache/v1/{raw,offset,keys,values}`
//! endpoints from [Yolean/kafka-keyvalue](https://github.com/Yolean/kafka-keyvalue).
//! Reads come from the shared [`CacheState`] owned by `mirror-core`;
//! the sinks (mirror-fs / mirror-s3) populate it per-record from the
//! consume loop, so freshness is independent of bucket-write cadence.
//!
//! The server also exposes:
//!
//! - `POST /_admin/v1/shutdown` and `POST /_admin/v1/shutdown/{exitcode}` — operator hooks.
//! - `GET /openapi.json` and `GET /openapi.yaml` — auto-generated OpenAPI 3.1 spec.
//! - `GET /docs` — Scalar UI rendering the spec.
//!
//! Readiness: every endpoint under `/cache/v1` returns `503 Service
//! Unavailable` until `CacheState::is_ready()` flips to `true`
//! (every registered mirror has caught up to its bootstrap
//! high-watermark). The flag is sticky — once ready, always ready.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use mirror_core::cache::TopicPartitionOffset;
use mirror_core::CacheState;
use serde::Serialize;
use tokio::sync::oneshot;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;
use utoipa_scalar::{Scalar, Servable};

/// Header name used to surface the last-seen offsets snapshot on
/// every `/cache/v1` read. Mirrors KKV's `x-kkv-last-seen-offsets`
/// byte-for-byte so unchanged clients keep parsing the response.
pub const KKV_OFFSETS_HEADER: &str = "x-kkv-last-seen-offsets";

/// `{topic, partition, offset}` shape serialized into the
/// `x-kkv-last-seen-offsets` header. Mirrors KKV's
/// `TopicPartitionOffset`, including JSON property order.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct TopicPartitionOffsetJson {
    pub offset: u64,
    pub partition: u32,
    pub topic: String,
}

impl From<&TopicPartitionOffset> for TopicPartitionOffsetJson {
    fn from(tpo: &TopicPartitionOffset) -> Self {
        Self {
            offset: tpo.offset,
            partition: tpo.partition,
            topic: tpo.topic.clone(),
        }
    }
}

/// Server-side state shared across handlers.
#[derive(Clone)]
struct AppState {
    cache: Arc<CacheState>,
    shutdown_tx: Arc<tokio::sync::Mutex<Option<oneshot::Sender<i32>>>>,
}

/// Assemble the `OpenApiRouter` with every handler's `#[utoipa::path]`
/// metadata attached. Shared between [`build_router`] (live serving)
/// and [`openapi_doc`] (spec generation) so the wire surface and the
/// committed spec can't drift.
fn open_api_router(state: AppState) -> OpenApiRouter {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(raw_by_key))
        .routes(routes!(offset_for_partition))
        .routes(routes!(keys))
        .routes(routes!(values))
        .routes(routes!(admin_shutdown))
        .routes(routes!(admin_shutdown_with_exit_code))
        .with_state(state)
}

/// The fully-populated OpenAPI 3.1 document, including every handler's
/// path entry. This is what `openapi.json`, `openapi.yaml`, and the
/// committed `schemas/mirror-v3.cache.openapi.json` should all match.
pub fn openapi_doc() -> utoipa::openapi::OpenApi {
    // No real state needed; route registration only depends on the
    // attribute metadata. Build a placeholder so the type checks.
    let placeholder = AppState {
        cache: Arc::new(CacheState::new()),
        shutdown_tx: Arc::new(tokio::sync::Mutex::new(None)),
    };
    open_api_router(placeholder).split_for_parts().1
}

/// Build the full router for the cache HTTP server, including
/// `/cache/v1`, `/_admin/v1`, the OpenAPI spec endpoints, and the
/// Scalar `/docs` UI. The returned router is ready to serve.
///
/// `shutdown_tx` is consumed by `POST /_admin/v1/shutdown[/{exitcode}]`
/// to signal the supervisor that a clean exit is requested.
pub fn build_router(cache: Arc<CacheState>, shutdown_tx: oneshot::Sender<i32>) -> axum::Router {
    let state = AppState {
        cache,
        shutdown_tx: Arc::new(tokio::sync::Mutex::new(Some(shutdown_tx))),
    };
    let (api_router, api) = open_api_router(state).split_for_parts();

    let openapi_json = api.clone();
    let openapi_yaml = api.clone();
    api_router
        .route(
            "/openapi.json",
            axum::routing::get(move || async move { axum::Json(openapi_json).into_response() }),
        )
        .route(
            "/openapi.yaml",
            axum::routing::get(move || async move {
                let yaml = serde_yaml::to_string(&openapi_yaml)
                    .unwrap_or_else(|_| "openapi: 3.1.0\n".into());
                (
                    [(axum::http::header::CONTENT_TYPE, "application/yaml")],
                    yaml,
                )
                    .into_response()
            }),
        )
        .merge(axum::Router::from(Scalar::with_url("/docs", api)))
}

/// Spawn the HTTP server on `addr` and run until the supervisor
/// receives the shutdown signal. Returns the requested exit code
/// (passed via `POST /_admin/v1/shutdown/{exitcode}`, default 0).
pub async fn serve(
    addr: SocketAddr,
    cache: Arc<CacheState>,
    shutdown_signal: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<i32, Box<ServeError>> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<i32>();
    let router = build_router(cache, shutdown_tx);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Box::new(ServeError::Bind(addr, e)))?;
    tracing::info!(%addr, "cache-v1 HTTP server listening");
    let exit_code_holder = Arc::new(tokio::sync::Mutex::new(0_i32));
    let exit_code_holder_for_admin = Arc::clone(&exit_code_holder);
    let combined_shutdown = async move {
        tokio::select! {
            _ = shutdown_signal => {
                tracing::info!("supervisor signalled shutdown of cache server");
            }
            code = shutdown_rx => {
                let code = code.unwrap_or(0);
                tracing::info!(code, "admin endpoint signalled shutdown");
                *exit_code_holder_for_admin.lock().await = code;
            }
        }
    };
    axum::serve(listener, router)
        .with_graceful_shutdown(combined_shutdown)
        .await
        .map_err(|e| Box::new(ServeError::Serve(e)))?;
    let code = *exit_code_holder.lock().await;
    Ok(code)
}

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("bind {0}: {1}")]
    Bind(SocketAddr, std::io::Error),
    #[error("serve: {0}")]
    Serve(std::io::Error),
}

/// Aggregate OpenAPI 3.1 document. Endpoints are registered through
/// `OpenApiRouter::routes!(...)` so the spec stays in lock-step with
/// the actual handler set — adding or removing a route here without
/// updating the router (or vice versa) is impossible.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "mirror-v3 cache",
        description = "Drop-in HTTP surface for Yolean/kafka-keyvalue's /cache/v1. \
                       The state is a merged in-memory `key → latest-value` view \
                       across every mirror with `http-access: { api: cache-v1 }`. \
                       Updates are per-record from the consume loop; reads return \
                       503 until every registered mirror has caught up to its \
                       startup high-watermark.",
        version = "1.0.0",
    ),
    components(schemas(TopicPartitionOffsetJson)),
    tags(
        (name = "cache", description = "Read-only cache API (KKV-compatible)"),
        (name = "admin", description = "Operator endpoints"),
    ),
)]
struct ApiDoc;

// Allowed locally: the `Err` payload IS the response — boxing it
// would force every readiness-gated handler to deref before
// returning, with zero observable benefit.
#[allow(clippy::result_large_err)]
fn ready_or_503(state: &AppState) -> Result<(), Response> {
    if state.cache.is_ready() {
        Ok(())
    } else {
        Err(StatusCode::SERVICE_UNAVAILABLE.into_response())
    }
}

fn offsets_header(state: &AppState) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let offsets = state.cache.snapshot_offsets();
    let payload: Vec<TopicPartitionOffsetJson> =
        offsets.iter().map(TopicPartitionOffsetJson::from).collect();
    if let Ok(value) = serde_json::to_string(&payload) {
        if let Ok(v) = HeaderValue::from_str(&value) {
            headers.insert(KKV_OFFSETS_HEADER, v);
        }
    }
    headers
}

/// GET /cache/v1/raw/{key} — fetch a value by key.
#[utoipa::path(
    get,
    path = "/cache/v1/raw/{key}",
    tag = "cache",
    params(
        ("key" = String, Path, description = "URL-encoded key (UTF-8 string)")
    ),
    responses(
        (status = 200, description = "Value bytes for the requested key", body = Vec<u8>, content_type = "application/octet-stream"),
        (status = 400, description = "Empty or invalid key"),
        (status = 404, description = "Key not in cache"),
        (status = 503, description = "Cache is not yet caught up to the source"),
    ),
)]
async fn raw_by_key(State(state): State<AppState>, Path(key): Path<String>) -> Response {
    if let Err(r) = ready_or_503(&state) {
        return r;
    }
    if key.is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    match state.cache.get_value(&key) {
        None => StatusCode::NOT_FOUND.into_response(),
        Some(bytes) => {
            let mut headers = offsets_header(&state);
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
            (StatusCode::OK, headers, bytes).into_response()
        }
    }
}

/// GET /cache/v1/offset/{topic}/{partition} — last-seen offset.
#[utoipa::path(
    get,
    path = "/cache/v1/offset/{topic}/{partition}",
    tag = "cache",
    params(
        ("topic" = String, Path, description = "Source topic name"),
        ("partition" = u32, Path, description = "Source partition"),
    ),
    responses(
        (status = 200, description = "Decimal offset of the last applied record, or empty if none yet", body = String, content_type = "text/plain"),
        (status = 400, description = "Empty topic"),
    ),
)]
async fn offset_for_partition(
    State(state): State<AppState>,
    Path((topic, partition)): Path<(String, u32)>,
) -> Response {
    if topic.is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let body = state
        .cache
        .get_offset(&topic, partition)
        .map(|o| o.to_string())
        .unwrap_or_default();
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        )],
        body,
    )
        .into_response()
}

/// GET /cache/v1/keys — newline-separated key list, every line
/// (including the last) terminated by `\n`. Order is the order each
/// key was first seen by the cache (insertion order).
///
/// `Content-Type` is `application/octet-stream` to match KKV's
/// byte-for-byte response shape. A possible future enhancement (gated
/// on operator demand) is to surface the topic schema in the content
/// type — see the `values` handler for the same hook.
#[utoipa::path(
    get,
    path = "/cache/v1/keys",
    tag = "cache",
    responses(
        (status = 200, description = "Newline-separated keys (UTF-8, trailing newline included)", body = Vec<u8>, content_type = "application/octet-stream"),
        (status = 503, description = "Cache is not yet caught up to the source"),
    ),
)]
async fn keys(State(state): State<AppState>) -> Response {
    if let Err(r) = ready_or_503(&state) {
        return r;
    }
    let mut body = Vec::new();
    for k in state.cache.snapshot_keys() {
        body.extend_from_slice(k.as_bytes());
        body.push(b'\n');
    }
    let mut headers = offsets_header(&state);
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    (StatusCode::OK, headers, body).into_response()
}

/// GET /cache/v1/values — newline-separated values (raw bytes).
/// Order matches `/cache/v1/keys`. Every line — including the last —
/// is terminated by `\n`. Binary-safe **only** when none of the values
/// contain a `0x0A` byte; binary topics should pin
/// `values: { type: bytes-base64 }` so the cache returns the
/// base64-encoded form here.
///
/// `Content-Type` is `text/plain; charset=utf-8` regardless of the
/// configured value type. Future work — gated on operator demand —
/// is to adapt the response content type to the topic schema:
///
/// | `values.type`        | proposed `Content-Type`            |
/// | -------------------- | ---------------------------------- |
/// | `bytes-base64`       | `application/octet-stream`         |
/// | `utf8`               | `text/plain; charset=utf-8`        |
/// | `json` / `json-parseable` | `application/x-ndjson`        |
///
/// Not implemented today to keep parity with KKV's
/// `text/plain;charset=UTF-8` (mirror-v3 emits the RFC-normalised
/// equivalent).
#[utoipa::path(
    get,
    path = "/cache/v1/values",
    tag = "cache",
    responses(
        (status = 200, description = "Newline-separated raw values with trailing newline; binary-safe iff no value contains 0x0A", body = Vec<u8>, content_type = "text/plain"),
        (status = 503, description = "Cache is not yet caught up to the source"),
    ),
)]
async fn values(State(state): State<AppState>) -> Response {
    if let Err(r) = ready_or_503(&state) {
        return r;
    }
    let mut body = Vec::new();
    for v in state.cache.snapshot_values() {
        body.extend_from_slice(&v);
        body.push(b'\n');
    }
    let mut headers = offsets_header(&state);
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    (StatusCode::OK, headers, body).into_response()
}

/// POST /_admin/v1/shutdown — request graceful exit.
#[utoipa::path(
    post,
    path = "/_admin/v1/shutdown",
    tag = "admin",
    responses(
        (status = 202, description = "Shutdown accepted; the supervisor will exit shortly with code 0"),
    ),
)]
async fn admin_shutdown(State(state): State<AppState>) -> Response {
    trigger_shutdown(&state, 0).await;
    StatusCode::ACCEPTED.into_response()
}

/// POST /_admin/v1/shutdown/{exitcode} — request graceful exit with a specific code.
#[utoipa::path(
    post,
    path = "/_admin/v1/shutdown/{exitcode}",
    tag = "admin",
    params(
        ("exitcode" = i32, Path, description = "Exit code the supervisor will return"),
    ),
    responses(
        (status = 202, description = "Shutdown accepted; the supervisor will exit with the requested code"),
    ),
)]
async fn admin_shutdown_with_exit_code(
    State(state): State<AppState>,
    Path(exitcode): Path<i32>,
) -> Response {
    trigger_shutdown(&state, exitcode).await;
    StatusCode::ACCEPTED.into_response()
}

async fn trigger_shutdown(state: &AppState, code: i32) {
    let mut slot = state.shutdown_tx.lock().await;
    if let Some(tx) = slot.take() {
        let _ = tx.send(code);
    }
}

/// Render the OpenAPI 3.1 document as pretty JSON. Used by the
/// `xtask gen-openapi` command and by the schema-gate test.
pub fn openapi_json_pretty() -> String {
    serde_json::to_string_pretty(&openapi_doc()).expect("openapi serialization is infallible")
}
