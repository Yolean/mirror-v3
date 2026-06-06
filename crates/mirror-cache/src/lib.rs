//! HTTP surface for mirror-v3's KKV-compatibility mode.
//!
//! Two route trees serve the kkv-shaped read surface:
//!
//! - `/cache/v1/{mirror}/...` is always mounted; one entry per
//!   `http-access.cache-v1` opt-in mirror. Each path dispatches to
//!   that mirror's own per-mirror view and gates on its per-mirror
//!   `caught_up` flag (503 until the slot crosses
//!   `bootstrap_hwm - 1`).
//! - `/cache/v1/...` (unprefixed) is mounted iff some mirror opted
//!   into `http-access.cache-v1-main`; the validator enforces
//!   at-most-one and `[`CacheState::main_mirror`] tracks which one.
//!   It is a thin alias onto that singleton mirror's per-mirror
//!   routes — a migration aid for consumers that haven't picked up
//!   the per-mirror paths yet.
//!
//! The server also exposes:
//!
//! - `GET /q/health/ready`: drop-in compat alias for the legacy
//!   Quarkus kkv health endpoint. Returns `200 OK` with an empty
//!   body once `CacheState::is_ready()`, `503 Service Unavailable`
//!   otherwise. Kept off the OpenAPI spec because it's purely a
//!   compat shim for the existing `@yolean/kafka-keyvalue` Node
//!   client, whose `onReady()` polls
//!   `KKV_CACHE_HOST_READINESS_ENDPOINT` (default `/q/health/ready`).
//! - `POST /_admin/v1/shutdown` and `POST /_admin/v1/shutdown/{exitcode}`: operator hooks.
//! - `GET /openapi.json` and `GET /openapi.yaml`: auto-generated OpenAPI 3.1 spec.
//! - `GET /docs`: Scalar UI rendering the spec.
//!
//! Readiness: every `/cache/v1` route gates on its target mirror's
//! per-mirror `caught_up` flag and returns 503 until the flag flips.
//! The aggregate `is_ready()` (every registered mirror caught up)
//! backs `/q/health/ready`. Both flags are sticky-true today; the
//! mirror-degraded re-suppression case is tracked as a follow-up.

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
///
/// Only the per-mirror routes are committed to the spec; the
/// unprefixed `cache-v1-main` aliases are runtime-conditional and
/// described in the per-mirror operation's description instead.
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
/// per-mirror `/cache/v1/{mirror}/...` routes, the unprefixed
/// `/cache/v1/...` `cache-v1-main` alias (when set),
/// `/_admin/v1`, the OpenAPI spec endpoints, and the Scalar `/docs`
/// UI. The returned router is ready to serve.
///
/// `shutdown_tx` is consumed by `POST /_admin/v1/shutdown[/{exitcode}]`
/// to signal the supervisor that a clean exit is requested.
pub fn build_router(cache: Arc<CacheState>, shutdown_tx: oneshot::Sender<i32>) -> axum::Router {
    // Hold extra clones for closures registered after the main
    // `state.cache` is moved into the OpenAPI router via
    // `open_api_router(state)`.
    let cache_for_ready = Arc::clone(&cache);
    let main_mirror = cache.main_mirror();
    let state = AppState {
        cache,
        shutdown_tx: Arc::new(tokio::sync::Mutex::new(Some(shutdown_tx))),
    };
    let main_state = state.clone();
    let (api_router, api) = open_api_router(state).split_for_parts();

    let openapi_json = api.clone();
    let openapi_yaml = api.clone();
    let mut router = api_router
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
        // Drop-in for the Yolean/kafka-keyvalue Quarkus binary's
        // `/q/health/ready` SmallRye-Health endpoint. The default
        // value of `KKV_CACHE_HOST_READINESS_ENDPOINT` in the
        // `@yolean/kafka-keyvalue` Node client is `/q/health/ready`;
        // that client's `onReady()` polls it every 3 s and gates
        // downstream consumer-pod readiness on a `200`. Returning
        // the same `200`/`503` shape here makes mirror-v3 a true
        // drop-in: existing consumers work unmodified, no
        // `KKV_CACHE_HOST_READINESS_ENDPOINT` override needed.
        //
        // Kept off the OpenAPI spec; it's purely a compat shim for
        // an existing client, not a public surface mirror-v3 wants
        // to commit to. The Quarkus `/q/...` path namespace is
        // unlikely to collide with anything else mirror-v3 might
        // want to add.
        .route(
            "/q/health/ready",
            axum::routing::get(move || {
                let cache = Arc::clone(&cache_for_ready);
                async move {
                    if cache.is_ready() {
                        StatusCode::OK.into_response()
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE.into_response()
                    }
                }
            }),
        );

    // `cache-v1-main` mounts the unprefixed `/cache/v1/...` paths
    // onto the named mirror's view; without it, the unprefixed
    // paths are not served at all (consumers must use the
    // per-mirror `/cache/v1/{mirror}/...` paths). The handlers reuse
    // the per-mirror code paths with the resolved name; kept off
    // the OpenAPI spec because the route set is config-conditional.
    if let Some(name) = main_mirror {
        router = router
            .route(
                "/cache/v1/raw/{key}",
                axum::routing::get({
                    let name = name.clone();
                    let state = main_state.clone();
                    move |Path(key): Path<String>| {
                        let name = name.clone();
                        let state = state.clone();
                        async move { raw_by_key(State(state), Path((name, key))).await }
                    }
                }),
            )
            .route(
                "/cache/v1/offset/{topic}/{partition}",
                axum::routing::get({
                    let name = name.clone();
                    let state = main_state.clone();
                    move |Path((topic, partition)): Path<(String, u32)>| {
                        let name = name.clone();
                        let state = state.clone();
                        async move {
                            offset_for_partition(State(state), Path((name, topic, partition))).await
                        }
                    }
                }),
            )
            .route(
                "/cache/v1/keys",
                axum::routing::get({
                    let name = name.clone();
                    let state = main_state.clone();
                    move || {
                        let name = name.clone();
                        let state = state.clone();
                        async move { keys(State(state), Path(name)).await }
                    }
                }),
            )
            .route(
                "/cache/v1/values",
                axum::routing::get({
                    let name = name.clone();
                    let state = main_state.clone();
                    move || {
                        let name = name.clone();
                        let state = state.clone();
                        async move { values(State(state), Path(name)).await }
                    }
                }),
            );
    } else {
        // No main mirror: the `main_state` clone exists only because
        // the compiler captures both branches into the same scope.
        // Drop it explicitly so clippy doesn't warn about an unused
        // binding in the no-main path.
        drop(main_state);
    }

    router.merge(axum::Router::from(Scalar::with_url("/docs", api)))
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
/// the actual handler set; adding or removing a route here without
/// updating the router (or vice versa) is impossible.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "mirror-v3 cache",
        description = "Drop-in HTTP surface for Yolean/kafka-keyvalue's /cache/v1. \
                       Each opt-in mirror (`http-access.cache-v1`) owns its own \
                       in-memory `key → latest-value` view, exposed under \
                       `/cache/v1/{mirror}/...`. A single mirror may additionally \
                       opt into `cache-v1-main`, which mounts the unprefixed \
                       `/cache/v1/...` paths onto its view as a migration alias \
                       for legacy kkv consumers; these unprefixed routes are \
                       config-conditional and intentionally omitted from this \
                       spec. Updates are per-record from the consume loop; reads \
                       return 503 until the target mirror has caught up to its \
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

/// Decide which mirror a `/cache/v1/{mirror}/...` request hits and
/// gate on its per-mirror readiness flag. Returns `Ok(mirror_name)`
/// for the handler to use against the per-mirror getters, or an
/// already-built response for the failure cases:
///
/// - 404 if the named mirror is not registered (and so isn't an
///   opt-in `cache-v1` mirror in this process);
/// - 503 if the mirror is registered but has not yet crossed its
///   bootstrap high-watermark.
///
/// Allowed locally: the `Err` payload IS the response; boxing it
/// would force every readiness-gated handler to deref before
/// returning, with zero observable benefit.
#[allow(clippy::result_large_err)]
fn resolve_mirror(state: &AppState, mirror: &str) -> Result<(), Response> {
    if state.cache.snapshot_keys_for(mirror).is_none() {
        return Err(StatusCode::NOT_FOUND.into_response());
    }
    if !state.cache.is_mirror_ready(mirror) {
        return Err(StatusCode::SERVICE_UNAVAILABLE.into_response());
    }
    Ok(())
}

fn offsets_header_for(state: &AppState, mirror: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let Some(offsets) = state.cache.snapshot_offsets_for(mirror) else {
        return headers;
    };
    let payload: Vec<TopicPartitionOffsetJson> =
        offsets.iter().map(TopicPartitionOffsetJson::from).collect();
    if let Ok(value) = serde_json::to_string(&payload) {
        if let Ok(v) = HeaderValue::from_str(&value) {
            headers.insert(KKV_OFFSETS_HEADER, v);
        }
    }
    headers
}

/// GET /cache/v1/{mirror}/raw/{key}; fetch a value by key from the
/// named mirror's view. The unprefixed `/cache/v1/raw/{key}` alias
/// is mounted by `build_router` when one mirror opted into
/// `http-access.cache-v1-main`, and dispatches here with that
/// mirror's name.
#[utoipa::path(
    get,
    path = "/cache/v1/{mirror}/raw/{key}",
    tag = "cache",
    params(
        ("mirror" = String, Path, description = "Name of the `http-access.cache-v1` mirror to read from"),
        ("key" = String, Path, description = "URL-encoded key (UTF-8 string)")
    ),
    responses(
        (status = 200, description = "Value bytes for the requested key", body = Vec<u8>, content_type = "application/octet-stream"),
        (status = 400, description = "Empty or invalid key"),
        (status = 404, description = "Mirror unknown, or key not in cache"),
        (status = 503, description = "Mirror is not yet caught up to its source"),
    ),
)]
async fn raw_by_key(
    State(state): State<AppState>,
    Path((mirror, key)): Path<(String, String)>,
) -> Response {
    if let Err(r) = resolve_mirror(&state, &mirror) {
        return r;
    }
    if key.is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    match state.cache.get_value_for(&mirror, &key) {
        None => StatusCode::NOT_FOUND.into_response(),
        Some(bytes) => {
            let mut headers = offsets_header_for(&state, &mirror);
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
            (StatusCode::OK, headers, bytes).into_response()
        }
    }
}

/// GET /cache/v1/{mirror}/offset/{topic}/{partition}; last-seen
/// offset for that (topic, partition) within the named mirror.
#[utoipa::path(
    get,
    path = "/cache/v1/{mirror}/offset/{topic}/{partition}",
    tag = "cache",
    params(
        ("mirror" = String, Path, description = "Name of the `http-access.cache-v1` mirror to read from"),
        ("topic" = String, Path, description = "Source topic name"),
        ("partition" = u32, Path, description = "Source partition"),
    ),
    responses(
        (status = 200, description = "Decimal offset of the last applied record on this mirror, or empty if none yet", body = String, content_type = "text/plain"),
        (status = 400, description = "Empty topic"),
        (status = 404, description = "Mirror unknown"),
    ),
)]
async fn offset_for_partition(
    State(state): State<AppState>,
    Path((mirror, topic, partition)): Path<(String, String, u32)>,
) -> Response {
    if state.cache.snapshot_keys_for(&mirror).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    if topic.is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let body = state
        .cache
        .get_offset_for(&mirror, &topic, partition)
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

/// GET /cache/v1/{mirror}/keys; newline-separated key list for the
/// named mirror's view. Every line (including the last) is
/// terminated by `\n`. Order is insertion order (the position a key
/// gets the *first* time the mirror sees it).
///
/// `Content-Type` is `application/octet-stream` to match KKV's
/// byte-for-byte response shape.
#[utoipa::path(
    get,
    path = "/cache/v1/{mirror}/keys",
    tag = "cache",
    params(
        ("mirror" = String, Path, description = "Name of the `http-access.cache-v1` mirror to read from"),
    ),
    responses(
        (status = 200, description = "Newline-separated keys (UTF-8, trailing newline included)", body = Vec<u8>, content_type = "application/octet-stream"),
        (status = 404, description = "Mirror unknown"),
        (status = 503, description = "Mirror is not yet caught up to its source"),
    ),
)]
async fn keys(State(state): State<AppState>, Path(mirror): Path<String>) -> Response {
    if let Err(r) = resolve_mirror(&state, &mirror) {
        return r;
    }
    let Some(snapshot) = state.cache.snapshot_keys_for(&mirror) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut body = Vec::new();
    for k in snapshot {
        body.extend_from_slice(k.as_bytes());
        body.push(b'\n');
    }
    let mut headers = offsets_header_for(&state, &mirror);
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    (StatusCode::OK, headers, body).into_response()
}

/// GET /cache/v1/{mirror}/values; newline-separated values for the
/// named mirror's view, in `keys` order. Binary-safe **only** when
/// none of the values contain a `0x0A` byte; binary topics should
/// pin `values: { type: bytes-base64 }` so the cache returns the
/// base64-encoded form here.
#[utoipa::path(
    get,
    path = "/cache/v1/{mirror}/values",
    tag = "cache",
    params(
        ("mirror" = String, Path, description = "Name of the `http-access.cache-v1` mirror to read from"),
    ),
    responses(
        (status = 200, description = "Newline-separated raw values with trailing newline; binary-safe iff no value contains 0x0A", body = Vec<u8>, content_type = "text/plain"),
        (status = 404, description = "Mirror unknown"),
        (status = 503, description = "Mirror is not yet caught up to its source"),
    ),
)]
async fn values(State(state): State<AppState>, Path(mirror): Path<String>) -> Response {
    if let Err(r) = resolve_mirror(&state, &mirror) {
        return r;
    }
    let Some(snapshot) = state.cache.snapshot_values_for(&mirror) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut body = Vec::new();
    for v in snapshot {
        body.extend_from_slice(&v);
        body.push(b'\n');
    }
    let mut headers = offsets_header_for(&state, &mirror);
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    (StatusCode::OK, headers, body).into_response()
}

/// POST /_admin/v1/shutdown; request graceful exit.
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

/// POST /_admin/v1/shutdown/{exitcode}; request graceful exit with a specific code.
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
