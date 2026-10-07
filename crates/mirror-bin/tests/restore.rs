//! `mirror-v3 restore` against a filesystem backup written by the
//! mirror's own sink. Producing needs a broker: see
//! e2e/tests/restore_roundtrip.rs.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Duration;

use mirror_core::{Record, Sink, TimestampType};
use mirror_envelope::{ColumnType, Format, ParquetCompression};
use mirror_fs::{FilesystemSink, FilesystemSinkConfig, FlushTriggers};

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_mirror-v3"))
}

fn rec(offset: u64) -> Record {
    Record {
        topic: "operations".into(),
        partition: 0,
        source_offset: offset,
        timestamp_ms: Some(1_700_000_000_000),
        timestamp_type: TimestampType::CreateTime,
        key: Some(format!("k{offset}").into_bytes()),
        value: Some(format!("v{offset}").into_bytes()),
        headers: vec![],
    }
}

/// A config with one filesystem backup mirror of operations/0 under
/// `root`, and a backup with records at `offsets`, two per object.
fn backup(root: &Path, offsets: &[u64]) -> PathBuf {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut sink = FilesystemSink::open(FilesystemSinkConfig {
            root: root.join("data"),
            destination_name: "operations".into(),
            partition: 0,
            format: Format::Parquet,
            compression: ParquetCompression::Zstd1,
            keys: ColumnType::Utf8,
            values: ColumnType::Utf8,
            compaction: None,
            flush: FlushTriggers {
                max_time: Duration::from_secs(3600),
                max_bytes: u64::MAX,
                max_offsets: 2,
                daily_at_utc_seconds: None,
            },
        })
        .unwrap();
        for o in offsets {
            sink.write(rec(*o)).await.unwrap();
        }
        sink.flush().await.unwrap();
    });
    let cfg = root.join("mirror-v3.yaml");
    std::fs::write(
        &cfg,
        format!(
            r#"
mirrors:
  - name: operations-backup
    source: {{ bootstrap-servers: "localhost:1" }}
    topic: operations
    partition: 0
    destinations:
      - type: filesystem
        name: operations
        root: {}
    format: parquet
    flush: {{ max-time-ms: 60000, max-bytes: 1048576, max-offsets: 2 }}
  - name: userstate-snapshots
    source: {{ bootstrap-servers: "localhost:1" }}
    topic: user-states
    partition: 0
    destinations:
      - type: filesystem
        root: {}
    format: parquet
    compaction: log
    flush: {{ max-time-ms: 60000, max-bytes: 1048576, max-offsets: 2 }}
"#,
            root.join("data").display(),
            root.join("data").display()
        ),
    )
    .unwrap();
    cfg
}

fn restore(cfg: &Path, args: &[&str]) -> Output {
    Command::new(bin())
        .arg("restore")
        .arg("--config")
        .arg(cfg)
        .args(args)
        .output()
        .expect("spawn mirror-v3 restore")
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).to_string()
}

#[test]
fn verify_only_prints_the_summary_of_a_complete_backup() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = backup(dir.path(), &[0, 1, 3, 4, 7, 8, 9]);
    let out = restore(
        &cfg,
        &[
            "--mirror",
            "operations-backup",
            "--offsets",
            "renumber",
            "--verify-only",
        ],
    );
    let stdout = text(&out.stdout);
    assert!(out.status.success(), "{stdout}\n{}", text(&out.stderr));
    for line in [
        "source: operations/0",
        "objects: 4",
        "offsets: 0-9",
        "records: 7",
        "holes: 3, the first at offset 2",
        "verified: the backup can be restored with these offsets",
    ] {
        assert!(stdout.contains(line), "missing {line:?} in:\n{stdout}");
    }
}

#[test]
fn verify_only_with_preserve_fails_on_a_hole() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = backup(dir.path(), &[0, 1, 3, 4]);
    let out = restore(
        &cfg,
        &[
            "--mirror",
            "operations-backup",
            "--offsets",
            "preserve",
            "--verify-only",
        ],
    );
    assert!(!out.status.success());
    assert!(text(&out.stdout).contains("holes: 1, the first at offset 2"));
    let stderr = text(&out.stderr);
    assert!(stderr.contains("cannot be written with holes"), "{stderr}");
}

#[test]
fn verify_only_fails_on_a_gap_in_the_chain() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = backup(dir.path(), &(0..6).collect::<Vec<_>>());
    let part = dir.path().join("data/operations/0");
    std::fs::remove_file(part.join(mirror_fs::naming::blob_filename(2, 3, None, "parquet")))
        .unwrap();
    let out = restore(
        &cfg,
        &[
            "--mirror",
            "operations-backup",
            "--offsets",
            "renumber",
            "--verify-only",
        ],
    );
    assert!(!out.status.success());
    let stderr = text(&out.stderr);
    assert!(stderr.contains("gap in the chain: offsets 2-3"), "{stderr}");
}

#[test]
fn a_compaction_mirror_and_an_unknown_mirror_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = backup(dir.path(), &[0]);
    let out = restore(
        &cfg,
        &[
            "--mirror",
            "userstate-snapshots",
            "--offsets",
            "renumber",
            "--verify-only",
        ],
    );
    assert!(!out.status.success());
    assert!(text(&out.stderr).contains("`compaction: log`"));
    let out = restore(
        &cfg,
        &["--mirror", "nope", "--offsets", "renumber", "--verify-only"],
    );
    assert!(!out.status.success());
    assert!(text(&out.stderr).contains("has no mirror \"nope\""));
}

#[test]
fn restore_to_an_unreachable_broker_fails_before_producing() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = backup(dir.path(), &[0, 1]);
    let out = restore(
        &cfg,
        &[
            "--mirror",
            "operations-backup",
            "--offsets",
            "preserve",
            "--bootstrap-servers",
            "localhost:1",
            "--topic",
            "operations",
        ],
    );
    assert!(!out.status.success());
    let stderr = text(&out.stderr);
    assert!(stderr.contains("reading its high watermark"), "{stderr}");
}
