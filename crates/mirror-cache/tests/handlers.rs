//! Direct tests against `build_router`. We hit the router with
//! `tower::ServiceExt::oneshot` so we don't need to bind a real port,
//! which keeps these tests fast and not subject to port-allocation
//! flakes.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use mirror_cache::{build_router, KKV_OFFSETS_HEADER};
use mirror_core::{CacheState, Header, Record, TimestampType};
use tokio::sync::oneshot;
use tower::ServiceExt;

fn rec(topic: &str, partition: i32, offset: u64, key: &str, value: Option<&[u8]>) -> Record {
    Record {
        topic: topic.into(),
        partition,
        source_offset: offset,
        timestamp_ms: Some(1_700_000_000_000),
        timestamp_type: TimestampType::CreateTime,
        key: Some(key.as_bytes().to_vec()),
        value: value.map(|v| v.to_vec()),
        headers: Vec::<Header>::new(),
    }
}

fn router_with(cache: Arc<CacheState>) -> axum::Router {
    let (tx, _rx) = oneshot::channel::<i32>();
    build_router(cache, tx)
}

async fn body_bytes(resp: axum::http::Response<Body>) -> Vec<u8> {
    to_bytes(resp.into_body(), 64 * 1024)
        .await
        .unwrap()
        .to_vec()
}

#[tokio::test]
async fn raw_returns_503_until_caught_up() {
    let cache = Arc::new(CacheState::new());
    cache.register_mirror("ops", 2, true); // needs offsets 0..=1; main mirror
    let app = router_with(Arc::clone(&cache));
    let resp = app
        .clone()
        .oneshot(
            Request::get("/cache/v1/raw/k0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

    cache.apply_record("ops", &rec("ops", 0, 0, "k0", Some(b"v0")));
    cache.apply_record("ops", &rec("ops", 0, 1, "k1", Some(b"v1")));
    let resp = app
        .oneshot(
            Request::get("/cache/v1/raw/k0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().contains_key(KKV_OFFSETS_HEADER));
    assert_eq!(body_bytes(resp).await, b"v0");
}

#[tokio::test]
async fn raw_404_for_missing_key() {
    let cache = Arc::new(CacheState::new());
    cache.register_mirror("m", 0, true); // empty topic → immediately ready
    let app = router_with(Arc::clone(&cache));
    let resp = app
        .oneshot(
            Request::get("/cache/v1/raw/absent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn tombstone_makes_key_404() {
    let cache = Arc::new(CacheState::new());
    cache.register_mirror("m", 2, true);
    cache.apply_record("m", &rec("t", 0, 0, "alice", Some(br#"{"v":1}"#)));
    cache.apply_record("m", &rec("t", 0, 1, "alice", None)); // tombstone
    let app = router_with(Arc::clone(&cache));
    let resp = app
        .oneshot(
            Request::get("/cache/v1/raw/alice")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn keys_and_values_are_newline_terminated_in_insertion_order() {
    let cache = Arc::new(CacheState::new());
    cache.register_mirror("m", 0, true);
    cache.apply_record("m", &rec("t", 0, 0, "b", Some(b"vb")));
    cache.apply_record("m", &rec("t", 0, 1, "a", Some(b"va")));
    cache.apply_record("m", &rec("t", 0, 2, "c", Some(b"vc")));
    let app = router_with(Arc::clone(&cache));

    let resp = app
        .clone()
        .oneshot(Request::get("/cache/v1/keys").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().contains_key(KKV_OFFSETS_HEADER));
    assert_eq!(
        resp.headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/octet-stream"),
        "/keys content-type matches KKV"
    );
    let body = String::from_utf8(body_bytes(resp).await).unwrap();
    assert_eq!(
        body, "b\na\nc\n",
        "insertion order; every line ends with \\n"
    );

    let resp = app
        .oneshot(
            Request::get("/cache/v1/values")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_bytes(resp).await;
    assert_eq!(body, b"vb\nva\nvc\n");
}

#[tokio::test]
async fn offset_endpoint_returns_decimal_or_empty() {
    let cache = Arc::new(CacheState::new());
    cache.register_mirror("m", 0, true);
    cache.apply_record("m", &rec("orders", 1, 7, "k", Some(b"v")));
    let app = router_with(Arc::clone(&cache));

    let resp = app
        .clone()
        .oneshot(
            Request::get("/cache/v1/offset/orders/1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = String::from_utf8(body_bytes(resp).await).unwrap();
    assert_eq!(body, "7");

    // Unknown partition: empty body, 200.
    let resp = app
        .oneshot(
            Request::get("/cache/v1/offset/orders/99")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = String::from_utf8(body_bytes(resp).await).unwrap();
    assert!(body.is_empty(), "got: {body:?}");
}

#[tokio::test]
async fn openapi_json_and_yaml_are_served() {
    let cache = Arc::new(CacheState::new());
    cache.register_mirror("m", 0, true);
    let app = router_with(Arc::clone(&cache));

    let resp = app
        .clone()
        .oneshot(Request::get("/openapi.json").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = String::from_utf8(body_bytes(resp).await).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&body).expect("OpenAPI JSON must parse");
    assert_eq!(parsed["openapi"], "3.1.0");
    assert!(parsed["paths"]["/cache/v1/{mirror}/raw/{key}"].is_object());
    assert!(
        parsed["paths"]["/cache/v1/raw/{key}"].is_null(),
        "unprefixed cache-v1-main aliases must stay off the static spec"
    );

    let resp = app
        .oneshot(Request::get("/openapi.yaml").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = String::from_utf8(body_bytes(resp).await).unwrap();
    assert!(
        body.contains("/cache/v1/{mirror}/raw/{key}"),
        "yaml must include the per-mirror cache route: {body}"
    );
}

#[tokio::test]
async fn offsets_header_contents_match_snapshot() {
    let cache = Arc::new(CacheState::new());
    cache.register_mirror("m", 0, true);
    cache.apply_record("m", &rec("orders", 0, 5, "k", Some(b"v")));
    cache.apply_record("m", &rec("orders", 1, 3, "k2", Some(b"v")));
    let app = router_with(Arc::clone(&cache));
    let resp = app
        .oneshot(Request::get("/cache/v1/keys").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let value = resp
        .headers()
        .get(KKV_OFFSETS_HEADER)
        .expect("offsets header present")
        .to_str()
        .unwrap()
        .to_string();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&value).unwrap();
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0]["topic"], "orders");
    assert_eq!(parsed[0]["partition"], 0);
    assert_eq!(parsed[0]["offset"], 5);
    assert_eq!(parsed[1]["partition"], 1);
    assert_eq!(parsed[1]["offset"], 3);
}

#[tokio::test]
async fn q_health_ready_returns_503_until_caught_up_then_200() {
    // Drop-in for the Yolean/kafka-keyvalue Quarkus binary's
    // `/q/health/ready` SmallRye-Health endpoint. The
    // `@yolean/kafka-keyvalue` Node client's `onReady()` polls it
    // every 3 s; consumer pods that don't see a `200` never become
    // Ready themselves. Same readiness gate as `/cache/v1`.
    let cache = Arc::new(CacheState::new());
    cache.register_mirror("userstate", 2, true); // needs offsets 0..=1; main mirror
    let app = router_with(Arc::clone(&cache));

    let resp = app
        .clone()
        .oneshot(Request::get("/q/health/ready").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

    cache.apply_record("userstate", &rec("userstate", 0, 0, "k0", Some(b"v0")));
    cache.apply_record("userstate", &rec("userstate", 0, 1, "k1", Some(b"v1")));

    let resp = app
        .oneshot(Request::get("/q/health/ready").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // Empty body; Quarkus's SmallRye-Health returns a JSON document,
    // but the kkv Node client only checks the status code, so we
    // keep the body empty (200 implies ready, no further parsing).
    assert!(body_bytes(resp).await.is_empty());
}

#[tokio::test]
async fn per_mirror_paths_serve_only_that_mirrors_view() {
    // Two mirrors, each with its own keyspace. Hitting one mirror's
    // /raw/{key} must not surface the other's keys, and vice-versa.
    // Neither is `cache-v1-main`; the unprefixed paths must 404.
    let cache = Arc::new(CacheState::new());
    cache.register_mirror("a", 0, false);
    cache.register_mirror("b", 0, false);
    cache.apply_record("a", &rec("topic-a", 0, 0, "k-a", Some(b"va")));
    cache.apply_record("b", &rec("topic-b", 0, 0, "k-b", Some(b"vb")));
    let app = router_with(Arc::clone(&cache));

    let resp = app
        .clone()
        .oneshot(
            Request::get("/cache/v1/a/raw/k-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_bytes(resp).await, b"va");

    // Cross-mirror miss: mirror b doesn't have k-a.
    let resp = app
        .clone()
        .oneshot(
            Request::get("/cache/v1/b/raw/k-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // No cache-v1-main: unprefixed paths route to nothing.
    let resp = app
        .oneshot(
            Request::get("/cache/v1/raw/k-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "no main mirror => unprefixed path not mounted"
    );
}

#[tokio::test]
async fn per_mirror_path_unknown_mirror_is_404() {
    let cache = Arc::new(CacheState::new());
    cache.register_mirror("real", 0, false);
    let app = router_with(Arc::clone(&cache));
    let resp = app
        .oneshot(
            Request::get("/cache/v1/missing/raw/anything")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn per_mirror_path_503_until_that_mirror_caught_up() {
    // Per-mirror readiness gates each route independently: one
    // mirror can already serve while the other is still warming up.
    let cache = Arc::new(CacheState::new());
    cache.register_mirror("ready-now", 0, false); // hwm 0 => ready
    cache.register_mirror("warming", 2, false); // needs offsets 0..=1
    let app = router_with(Arc::clone(&cache));

    let resp = app
        .clone()
        .oneshot(
            Request::get("/cache/v1/ready-now/keys")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .clone()
        .oneshot(
            Request::get("/cache/v1/warming/keys")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn unprefixed_paths_dispatch_to_main_mirror_view() {
    // Two mirrors; `main-m` is cache-v1-main. The unprefixed
    // /cache/v1/keys must return main-m's keys only.
    let cache = Arc::new(CacheState::new());
    cache.register_mirror("main-m", 0, true);
    cache.register_mirror("other", 0, false);
    cache.apply_record("main-m", &rec("t", 0, 0, "main-key", Some(b"vm")));
    cache.apply_record("other", &rec("t", 0, 0, "other-key", Some(b"vo")));
    let app = router_with(Arc::clone(&cache));

    let resp = app
        .clone()
        .oneshot(Request::get("/cache/v1/keys").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_bytes(resp).await, b"main-key\n");

    let resp = app
        .oneshot(
            Request::get("/cache/v1/raw/other-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "unprefixed path does not fall through to the non-main mirror"
    );
}

#[tokio::test]
async fn q_health_ready_is_not_in_openapi_spec() {
    // Compat shim, intentionally undocumented; public surface is
    // `/cache/v1` and `/_admin/v1` only.
    let cache = Arc::new(CacheState::new());
    cache.register_mirror("m", 0, true);
    let app = router_with(Arc::clone(&cache));
    let resp = app
        .oneshot(Request::get("/openapi.json").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = String::from_utf8(body_bytes(resp).await).unwrap();
    assert!(
        !body.contains("/q/health/ready"),
        "/q/health/ready must stay off the OpenAPI spec; got: {body}"
    );
}
