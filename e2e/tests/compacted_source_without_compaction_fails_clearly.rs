//! E2e companion to [`compacted_source_with_compaction_log`]: the
//! *append-mode* mirror against the same trimmed source must fail
//! fast at bootstrap with `SourceCompactedBelowExpected`. The
//! contract is that the error names the broker's low watermark and
//! tells the operator exactly what to do (set `compaction: log` or
//! seed the destination). Anything quieter — a silent skip, a delayed
//! `SourceOffsetMismatch` after polling — would leave operators
//! debugging a gap in the destination chain that mirror-v3 knew
//! about all along.
//!
//! Naming note: the "old `SourceOffsetMismatch`" referenced in the
//! body of this file has since been split into `SourceWentBackwards`
//! and `SourceGapAboveExpected`; the latter is the variant that
//! would fire here today if the bootstrap branch didn't pre-empt it.

use std::time::Duration;

use mirror_e2e::docker::DockerProvisioner;
use mirror_e2e::kafka_helpers::{create_compacted_topic, produce_records, trim_records_before};
use mirror_e2e::mirror_runner::{spawn_kafka_to_filesystem, FsMirrorSpec};
use mirror_e2e::{ProvisionedStack, Provisioner};
use mirror_fs::FlushTriggers;

const TOPIC: &str = "mirror-e2e-compacted-append";
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
async fn append_mode_mirror_against_trimmed_source_errors_with_low_watermark() {
    install_tracing();

    let stack = DockerProvisioner.provision().await.expect("provision");
    let source = stack.source_bootstrap();
    let root = tempfile::tempdir().expect("tempdir");
    create_compacted_topic(&source, TOPIC, 1)
        .await
        .expect("create compacted topic");

    let seed: Vec<(String, String)> = (0..12)
        .map(|i| (format!("k{}", i % 4), format!("v{i}")))
        .collect();
    produce_records(&source, TOPIC, PARTITION, &seed)
        .await
        .expect("seed");
    let new_low = trim_records_before(&source, TOPIC, PARTITION, 8)
        .await
        .expect("trim");
    assert_eq!(new_low, 8);

    // Append-mode mirror (compaction defaulted to None in
    // `FsMirrorSpec::ndjson`). The destination is empty so
    // sink.next_expected_offset() = 0, the source's low watermark is
    // 8, and the sink does NOT tolerate compacted sources → the loop
    // must abort at bootstrap.
    let flush = FlushTriggers {
        max_time: Duration::from_secs(60),
        max_bytes: u64::MAX,
        max_offsets: u64::MAX,
        daily_at_utc_seconds: None,
    };
    let spec = FsMirrorSpec::ndjson(
        source.clone(),
        TOPIC.into(),
        PARTITION,
        "mirror-e2e-compacted-append".into(),
        root.path().to_path_buf(),
        "archive".into(),
        flush,
    );
    let mirror = spawn_kafka_to_filesystem(spec).expect("spawn mirror");

    // The mirror task should terminate on its own with the
    // SourceCompactedBelowExpected error. The MirrorHandle wraps the
    // MirrorError into an anyhow with prefix "mirror loop: …", so we
    // assert on the displayed substring.
    let err = mirror
        .wait_for_termination()
        .await
        .expect_err("append-mode mirror must abort on compacted source");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("source has been compacted past start offset"),
        "expected SourceCompactedBelowExpected message, got: {msg}"
    );
    assert!(
        msg.contains("broker's earliest is 8"),
        "error must name the broker's low watermark, got: {msg}"
    );
    assert!(
        msg.contains("`compaction: log`"),
        "error must point operators at the compaction:log fix, got: {msg}"
    );

    // And the destination must be untouched — no file was ever
    // flushed, because the bootstrap branch errors out before the
    // first poll. This is the difference between
    // `SourceCompactedBelowExpected` (zero-write) and the old
    // `SourceOffsetMismatch` (which fires after the first poll, but
    // the loop has already side-effected metrics / log lines).
    let dir = mirror_fs::naming::partition_dir(root.path(), "archive", PARTITION as u32);
    if let Ok(entries) = std::fs::read_dir(&dir) {
        let files: Vec<_> = entries
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.extension().and_then(|e| e.to_str()) == Some("ndjson")
                    && !p.to_string_lossy().contains(".tmp.")
            })
            .collect();
        assert!(
            files.is_empty(),
            "append-mode mirror must not write any file before aborting; found: {files:?}"
        );
    }
}
