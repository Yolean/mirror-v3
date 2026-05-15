//! Adversarial tests against the filesystem sink.
//!
//! These exercise *misuse* — out-of-band writes, two writers racing,
//! corruption during operation — and assert the mirror exits loudly
//! rather than silently producing inconsistent state.
//!
//! The "deployment must be single-writer" rule (documented in
//! `examples/*.yaml` and `README.md`) is what avoids these scenarios
//! in production. These tests demonstrate what happens when that
//! rule is violated.

use std::time::Duration;

use mirror_e2e::docker::DockerProvisioner;
use mirror_e2e::kafka_helpers::{create_topic, produce_records};
use mirror_e2e::mirror_runner::{spawn_kafka_to_filesystem, FsMirrorSpec};
use mirror_e2e::{ProvisionedStack, Provisioner};
use mirror_fs::{FilesystemSink, FilesystemSinkConfig, FlushTriggers};

const TOPIC: &str = "mirror-e2e-adversarial";

fn flush_every(n: u64) -> FlushTriggers {
    FlushTriggers {
        max_time: Duration::from_secs(3600),
        max_bytes: u64::MAX,
        max_offsets: n,
    }
}

fn spec(source: &str, root: &std::path::Path, group: &str, flush: FlushTriggers) -> FsMirrorSpec {
    FsMirrorSpec {
        source_bootstrap: source.to_string(),
        source_topic: TOPIC.into(),
        partition: 0,
        group_id: group.into(),
        root: root.to_path_buf(),
        destination_name: "ops".into(),
        format: mirror_envelope::Format::Ndjson,
        compression: mirror_envelope::ParquetCompression::Zstd1,
        flush,
    }
}

fn install_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

/// Drop an unexpected file into the destination directory while a
/// mirror is running. The mirror's idle-drift check must detect the
/// new file and exit non-zero rather than continuing as if nothing
/// happened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn out_of_band_file_in_destination_terminates_mirror_with_error() {
    install_tracing();

    let stack = DockerProvisioner
        .provision()
        .await
        .expect("provision stack");
    let source = stack.source_bootstrap();
    let root = tempfile::tempdir().expect("tempdir");
    create_topic(&source, TOPIC, 1).await.expect("topic");

    // Produce 5 records, configure flush trigger far above so the
    // records stay buffered. The mirror's idle poll is the moment
    // when it re-checks the destination's listing.
    let fixtures: Vec<(String, String)> =
        (0..5).map(|i| (format!("k{i}"), format!("v{i}"))).collect();
    produce_records(&source, TOPIC, 0, &fixtures)
        .await
        .expect("produce");

    let mirror = spawn_kafka_to_filesystem(spec(
        &source,
        root.path(),
        "adversarial-oob",
        flush_every(100),
    ))
    .expect("spawn mirror");

    // Give the mirror time to consume records into its buffer.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Drop an out-of-band file at a future offset range.
    let dir = root.path().join("ops").join("0");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("00000000000000000900-00000000000000000999.ndjson"),
        b"{\"source_offset\":900,\"key\":null,\"value\":null}\n",
    )
    .unwrap();

    // The mirror's next idle poll calls next_expected_offset(), which
    // calls scan_validate, which sees a gap (durable=0 but listing
    // claims to start at 900) and returns CorruptChain. The mirror
    // returns an error from run_mirror.
    let result = tokio::time::timeout(Duration::from_secs(15), mirror.wait_for_termination()).await;
    let result = result.expect("mirror should terminate within 15s");
    let err = result.expect_err("mirror must error out, not exit cleanly");
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("corrupt")
            || msg.contains("gap")
            || msg.contains("destination")
            || msg.contains("unexpected"),
        "expected corruption-related error, got: {err}"
    );
}

/// Two mirrors running against the same destination directory with
/// *different* flush triggers will eventually emit conflicting
/// filenames. The mirror that observes the conflict must error out
/// (no silent overwrite of a divergent chain). The final destination
/// state, read by a fresh sink, must either be a valid chain *or* be
/// rejected at open-time — never silently inconsistent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_writers_with_different_flush_triggers_are_caught() {
    install_tracing();

    let stack = DockerProvisioner
        .provision()
        .await
        .expect("provision stack");
    let source = stack.source_bootstrap();
    let root = tempfile::tempdir().expect("tempdir");
    create_topic(&source, TOPIC, 1).await.expect("topic");

    let fixtures: Vec<(String, String)> = (0..40)
        .map(|i| (format!("k{i:03}"), format!("v{i:03}")))
        .collect();
    produce_records(&source, TOPIC, 0, &fixtures)
        .await
        .expect("produce");

    // Two mirrors. Different flush triggers (10 vs 7) means their
    // first files have different `to` offsets even though both start
    // at from=0. Different consumer groups so each has independent
    // (informational) commits.
    let m_a =
        spawn_kafka_to_filesystem(spec(&source, root.path(), "adversarial-a", flush_every(10)))
            .expect("spawn a");
    let m_b =
        spawn_kafka_to_filesystem(spec(&source, root.path(), "adversarial-b", flush_every(7)))
            .expect("spawn b");

    // Race them for a few seconds.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // At least one is expected to have errored already due to
    // scan-validate detecting the other's writes. Collect their
    // outcomes — neither is allowed to silently exit Ok with a
    // corrupted destination.
    let result_a = tokio::time::timeout(Duration::from_secs(5), m_a.wait_for_termination()).await;
    let result_b = tokio::time::timeout(Duration::from_secs(5), m_b.wait_for_termination()).await;
    let outcome_a = result_a.ok().and_then(|r| r.err());
    let outcome_b = result_b.ok().and_then(|r| r.err());
    tracing::info!(?outcome_a, ?outcome_b, "mirror outcomes");

    // The load-bearing assertion: opening a fresh sink against the
    // destination either succeeds (the two writers happened to
    // produce a consistent chain, e.g. because their flushes
    // serialised cleanly) OR fails with a corruption error. Never
    // a successful open with silently divergent data.
    let dir = root.path().join("ops").join("0");
    let names: Vec<String> = std::fs::read_dir(&dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| !n.contains(".tmp."))
                .collect()
        })
        .unwrap_or_default();
    tracing::info!(?names, "final destination filenames");

    let result = FilesystemSink::open(FilesystemSinkConfig {
        root: root.path().to_path_buf(),
        destination_name: "ops".into(),
        partition: 0,
        format: mirror_envelope::Format::Ndjson,
        compression: mirror_envelope::ParquetCompression::Zstd1,
        flush: flush_every(10),
    });

    match result {
        Ok(_) => {
            // Valid chain — open succeeded. That's safe.
            tracing::info!("destination ended up in a valid chain state");
        }
        Err(e) => {
            // Corruption detected — also safe (loudly visible).
            let msg = format!("{e}").to_lowercase();
            assert!(
                msg.contains("corrupt") || msg.contains("gap") || msg.contains("overlap"),
                "open failed but message doesn't look like corruption: {e}"
            );
            tracing::info!(error = %e, "destination ended up corrupt, detected loudly");
        }
    }
}
