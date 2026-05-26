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
    cache.register_mirror("ops", 2); // needs offsets 0..=1
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
    cache.register_mirror("m", 0); // empty topic → immediately ready
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
    cache.register_mirror("m", 2);
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
async fn keys_and_values_are_newline_separated_and_ordered() {
    let cache = Arc::new(CacheState::new());
    cache.register_mirror("m", 0);
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
    let body = String::from_utf8(body_bytes(resp).await).unwrap();
    assert_eq!(body, "a\nb\nc");

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
    assert_eq!(body, b"va\nvb\nvc");
}

#[tokio::test]
async fn offset_endpoint_returns_decimal_or_empty() {
    let cache = Arc::new(CacheState::new());
    cache.register_mirror("m", 0);
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
    cache.register_mirror("m", 0);
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
    assert!(parsed["paths"]["/cache/v1/raw/{key}"].is_object());

    let resp = app
        .oneshot(Request::get("/openapi.yaml").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = String::from_utf8(body_bytes(resp).await).unwrap();
    assert!(
        body.contains("/cache/v1/raw/{key}"),
        "yaml must include the cache route: {body}"
    );
}

#[tokio::test]
async fn offsets_header_contents_match_snapshot() {
    let cache = Arc::new(CacheState::new());
    cache.register_mirror("m", 0);
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
