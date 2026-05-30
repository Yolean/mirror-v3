//! E2e: a Kafka topic whose earliest available offset has been
//! pushed forward (here via `delete-records`, which from the
//! consumer's perspective is indistinguishable from broker-side
//! compaction having reclaimed earlier segments) must be consumable
//! by a mirror configured `compaction: log`.
//!
//! The bootstrap branch added to `run_mirror_with_heartbeat` queries
//! the source's low watermark and, when the sink reports
//! `allows_compacted_source = true`, advances `expected` to that
//! watermark. The destination snapshot ends up reflecting whichever
//! key→value pairs are still in the live tail; the trimmed-away
//! prefix is silently skipped (which is correct: in a compaction
//! topic the missing keys are either already represented by a later
//! record or have been superseded).

use std::time::Duration;

use mirror_e2e::docker::DockerProvisioner;
use mirror_e2e::kafka_helpers::{create_compacted_topic, produce_records, trim_records_before};
use mirror_e2e::mirror_runner::{spawn_kafka_to_filesystem, FsMirrorSpec};
use mirror_e2e::{ProvisionedStack, Provisioner};
use mirror_fs::{CompactionMode, FlushTriggers};

const TOPIC: &str = "mirror-e2e-compacted-log";
const PARTITION: i32 = 0;

fn install_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_log_mirror_resumes_from_low_watermark_on_trimmed_source() {
    install_tracing();

    let stack = DockerProvisioner.provision().await.expect("provision");
    let source = stack.source_bootstrap();
    let root = tempfile::tempdir().expect("tempdir");
    create_compacted_topic(&source, TOPIC, 1)
        .await
        .expect("create compacted topic");

    // 12 records across 4 keys, with overwrites. After trimming below
    // offset 8, the broker retains offsets 8..=11 which are the last
    // record for each of the 4 keys — exactly the "snapshot" the
    // compaction:log mirror should converge to.
    let seed: Vec<(String, String)> = (0..12)
        .map(|i| {
            let key = format!("k{}", i % 4);
            let val = format!("v{i}");
            (key, val)
        })
        .collect();
    produce_records(&source, TOPIC, PARTITION, &seed)
        .await
        .expect("seed");

    // Trim everything before offset 8. From the consumer's viewpoint
    // this is the same shape as broker-side log compaction having
    // reclaimed offsets 0..7.
    let new_low = trim_records_before(&source, TOPIC, PARTITION, 8)
        .await
        .expect("trim");
    assert_eq!(
        new_low, 8,
        "delete-records must report low watermark == 8, got {new_low}"
    );

    // Spawn the mirror with compaction:log. This is the case that
    // would have failed before the bootstrap branch existed:
    //   sink.next_expected_offset() = 0
    //   source.seek(0) → broker delivers 8
    //   strict check: 8 != 0 → crash
    let flush = FlushTriggers {
        max_time: Duration::from_millis(500),
        max_bytes: u64::MAX,
        max_offsets: u64::MAX,
        daily_at_utc_seconds: None,
    };
    let mut spec = FsMirrorSpec::ndjson(
        source.clone(),
        TOPIC.into(),
        PARTITION,
        "mirror-e2e-compacted-log".into(),
        root.path().to_path_buf(),
        "snapshot".into(),
        flush,
    );
    spec.compaction = Some(CompactionMode::Log);
    let mirror = spawn_kafka_to_filesystem(spec).expect("spawn mirror");

    // Let the consumer catch up to the trimmed tail, then graceful-
    // shutdown so the in-memory snapshot view is flushed to disk.
    // Going through `shutdown()` (rather than abort + fs poll) means
    // any error from the mirror task — bootstrap, recv, write —
    // surfaces here instead of being silently dropped.
    tokio::time::sleep(Duration::from_secs(15)).await;
    mirror.shutdown().await.expect("graceful shutdown");

    let dir = mirror_fs::naming::partition_dir(root.path(), "snapshot", PARTITION as u32);
    let snapshot_files: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
        .expect("read destination dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.extension().and_then(|e| e.to_str()) == Some("ndjson")
                && !p.to_string_lossy().contains(".tmp.")
        })
        .collect();
    assert!(
        !snapshot_files.is_empty(),
        "no snapshot file appeared in {dir:?} after graceful shutdown; check logs above"
    );

    // Read the final snapshot — the latest file by `to`-offset is the
    // authoritative one in compaction:log mode. Assert it contains
    // exactly the four live keys (k0..k3), each with its latest value.
    // The four "latest values" are the records at offsets 8, 9, 10, 11
    // — i.e. v8 / v9 / v10 / v11 for k0 / k1 / k2 / k3 respectively.
    let records =
        mirror_fs::read_all_records(&dir, mirror_envelope::Format::Ndjson).expect("read snapshot");
    // `read_all_records` returns the concatenation of every file's
    // contents in offset order. With one flushed snapshot file, this
    // is the snapshot's row set.
    let mut by_key: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for r in &records {
        let k = String::from_utf8(r.key.clone().expect("key")).expect("utf8 key");
        let v = String::from_utf8(r.value.clone().expect("value")).expect("utf8 val");
        by_key.insert(k, v);
    }
    let mut entries: Vec<(String, String)> = by_key.into_iter().collect();
    entries.sort();
    assert_eq!(
        entries,
        vec![
            ("k0".to_string(), "v8".to_string()),
            ("k1".to_string(), "v9".to_string()),
            ("k2".to_string(), "v10".to_string()),
            ("k3".to_string(), "v11".to_string()),
        ],
        "snapshot must contain the four live keys with their latest values from the post-trim tail"
    );
}
