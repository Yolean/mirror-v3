//! Sink invariants on top of `object_store::memory::InMemory`.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use mirror_core::{Record, Sink};
use mirror_s3::{FlushTriggers, S3Sink, S3SinkConfig};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;

fn rec(offset: u64) -> Record {
    Record {
        source_offset: offset,
        key: Some(format!("k{offset}").into_bytes()),
        value: Some(format!("v{offset}").into_bytes()),
        timestamp_ms: Some(1_700_000_000_000 + offset as i64),
        headers: vec![],
    }
}

fn cfg(store: Arc<dyn ObjectStore>, max_offsets: u64) -> S3SinkConfig {
    S3SinkConfig {
        store,
        prefix: Some(Path::from("archive")),
        destination_name: "ops".into(),
        partition: 0,
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets,
        },
    }
}

async fn list_names(store: &dyn ObjectStore, prefix: &Path) -> Vec<String> {
    let mut stream = store.list(Some(prefix));
    let mut names = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta.unwrap();
        names.push(
            meta.location
                .filename()
                .map(|s| s.to_string())
                .unwrap_or_default(),
        );
    }
    names.sort();
    names
}

#[tokio::test]
async fn empty_store_starts_at_zero() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut sink = S3Sink::open(cfg(Arc::clone(&store), 100)).await.unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 0);
}

#[tokio::test]
async fn count_trigger_flushes_one_object_per_batch() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut sink = S3Sink::open(cfg(Arc::clone(&store), 3)).await.unwrap();

    sink.write(rec(0)).await.unwrap();
    sink.write(rec(1)).await.unwrap();
    assert!(list_names(store.as_ref(), &Path::from("archive/ops/0"))
        .await
        .is_empty());
    sink.write(rec(2)).await.unwrap(); // flush
    sink.write(rec(3)).await.unwrap();
    sink.write(rec(4)).await.unwrap();
    sink.write(rec(5)).await.unwrap(); // flush

    let names = list_names(store.as_ref(), &Path::from("archive/ops/0")).await;
    assert_eq!(
        names,
        vec![
            "00000000000000000000-00000000000000000002.ndjson".to_string(),
            "00000000000000000003-00000000000000000005.ndjson".to_string(),
        ]
    );
}

#[tokio::test]
async fn restart_recomputes_position_from_listing() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    {
        let mut sink = S3Sink::open(cfg(Arc::clone(&store), 2)).await.unwrap();
        sink.write(rec(0)).await.unwrap();
        sink.write(rec(1)).await.unwrap(); // flush 0-1
    }
    let mut restarted = S3Sink::open(cfg(Arc::clone(&store), 2)).await.unwrap();
    assert_eq!(restarted.next_expected_offset().await.unwrap(), 2);
    let err = restarted
        .write(rec(0))
        .await
        .expect_err("offset 0 must be rejected post-restart");
    assert!(format!("{err}").contains("expected"), "got {err}");
}

#[tokio::test]
async fn rejects_out_of_order_write() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut sink = S3Sink::open(cfg(Arc::clone(&store), 100)).await.unwrap();
    sink.write(rec(0)).await.unwrap();
    let err = sink.write(rec(2)).await.expect_err("gap must error");
    assert!(format!("{err}").contains("expected"), "got {err}");
}

#[tokio::test]
async fn put_mode_create_rejects_overwrite() {
    // First sink flushes 0-1, then we open a second one and try to
    // re-flush at the same `from`. The second flush must fail via
    // the PutMode::Create gate (InMemory honors it).
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    {
        let mut sink = S3Sink::open(cfg(Arc::clone(&store), 2)).await.unwrap();
        sink.write(rec(0)).await.unwrap();
        sink.write(rec(1)).await.unwrap();
    }
    // Manually inject a sink whose internal state is stale: pretend
    // we never wrote anything. The flush should hit AlreadyExists.
    let mut sink = S3Sink::open(cfg(Arc::clone(&store), 2)).await.unwrap();
    // Force the sink to believe it should write at offset 0 by
    // hand-rolling a write at offset 2 (real position). Then prove the
    // path is contention-safe by attempting another writer flushing
    // at offset 0:
    let store2: Arc<dyn ObjectStore> = Arc::clone(&store);
    let mut competitor = S3Sink::open(S3SinkConfig {
        store: store2,
        prefix: Some(Path::from("archive")),
        destination_name: "ops".into(),
        partition: 0,
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets: 100,
        },
    })
    .await
    .unwrap();
    // Both opened: each sees `durable_position=2`. The original
    // (`sink`) progresses to 2,3 then flushes — that produces
    // 2-3.ndjson. Competitor produces 2,3 too and on flush_now hits
    // AlreadyExists.
    sink.write(rec(2)).await.unwrap();
    sink.write(rec(3)).await.unwrap();
    sink.flush_now().await.unwrap();
    competitor.write(rec(2)).await.unwrap();
    competitor.write(rec(3)).await.unwrap();
    let err = competitor
        .flush_now()
        .await
        .expect_err("PutMode::Create must reject overlap");
    assert!(
        format!("{err}").to_lowercase().contains("destination")
            || format!("{err}").to_lowercase().contains("expected"),
        "got {err}"
    );
}

#[tokio::test]
async fn corrupt_chain_is_rejected_on_open() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    use bytes::Bytes;
    use object_store::PutPayload;
    // Two overlapping objects at from=0.
    store
        .put(
            &Path::from("archive/ops/0/00000000000000000000-00000000000000000004.ndjson"),
            PutPayload::from(Bytes::from_static(b"")),
        )
        .await
        .unwrap();
    store
        .put(
            &Path::from("archive/ops/0/00000000000000000000-00000000000000000009.ndjson"),
            PutPayload::from(Bytes::from_static(b"")),
        )
        .await
        .unwrap();

    let err = S3Sink::open(cfg(Arc::clone(&store), 100))
        .await
        .err()
        .expect("must reject overlap");
    assert!(
        format!("{err}").to_lowercase().contains("gap")
            || format!("{err}").to_lowercase().contains("corrupt"),
        "got {err}"
    );
}
