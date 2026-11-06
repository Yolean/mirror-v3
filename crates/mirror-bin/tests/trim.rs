//! `mirror-v3 trim` against a filesystem backup written by the mirror's
//! own sink, and what `status` and `restore --verify-only` make of the
//! trimmed chain.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mirror_core::{Record, Sink, TimestampType};
use mirror_envelope::{ColumnType, Format, ParquetCompression};
use mirror_fs::{naming, FilesystemSink, FilesystemSinkConfig, FlushTriggers};

const DAY_MS: i64 = 86_400_000;

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_mirror-v3"))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn rec(offset: u64, timestamp_ms: Option<i64>) -> Record {
    Record {
        topic: "audit".into(),
        partition: 0,
        source_offset: offset,
        timestamp_ms,
        timestamp_type: TimestampType::CreateTime,
        key: Some(format!("k{offset}").into_bytes()),
        value: Some(format!("v{offset}").into_bytes()),
        headers: vec![],
    }
}

/// A config with one filesystem backup of audit/0 kept for 30 days,
/// and a backup of `records`, two per object.
fn backup(root: &Path, records: Vec<Record>) -> PathBuf {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut sink = FilesystemSink::open(FilesystemSinkConfig {
            root: root.join("data"),
            destination_name: "audit".into(),
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
        for r in records {
            sink.write(r).await.unwrap();
        }
        sink.flush().await.unwrap();
    });
    let cfg = root.join("mirror-v3.yaml");
    std::fs::write(
        &cfg,
        format!(
            r#"
mirrors:
  - name: audit-backup
    source: {{ bootstrap-servers: "localhost:1" }}
    topic: audit
    partition: 0
    destinations:
      - type: filesystem
        name: audit
        root: {}
        retention: {{ max-age-days: 30 }}
    format: parquet
    flush: {{ max-time-ms: 60000, max-bytes: 1048576, max-offsets: 2 }}
"#,
            root.join("data").display(),
        ),
    )
    .unwrap();
    cfg
}

/// Offsets 0-3 are 40 days old, 4-9 a day old.
fn aged() -> Vec<Record> {
    let now = now_ms();
    (0..10)
        .map(|o| {
            let age = if o < 4 { 40 * DAY_MS } else { DAY_MS };
            rec(o, Some(now - age + o as i64))
        })
        .collect()
}

fn mirror_v3(cfg: &Path, cmd: &str, args: &[&str]) -> Output {
    Command::new(bin())
        .arg(cmd)
        .arg("--config")
        .arg(cfg)
        .args(args)
        .output()
        .expect("spawn mirror-v3")
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).to_string()
}

fn objects(root: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(root.join("data/audit/0"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    names.sort();
    names
}

#[test]
fn trim_expires_the_head_and_status_and_verify_follow_the_floor() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = backup(dir.path(), aged());

    let dry = mirror_v3(&cfg, "trim", &["--dry-run"]);
    let stdout = text(&dry.stdout);
    assert!(dry.status.success(), "{stdout}\n{}", text(&dry.stderr));
    assert!(stdout.contains("dry run, nothing changed"), "{stdout}");
    assert!(stdout.contains("would delete: 2 object(s)"), "{stdout}");
    assert_eq!(objects(dir.path()).len(), 5);

    let out = mirror_v3(&cfg, "trim", &[]);
    let stdout = text(&out.stdout);
    assert!(out.status.success(), "{stdout}\n{}", text(&out.stderr));
    for line in [
        "trim audit-backup.audit: ",
        "floor: 0 -> 4 (floor-00000000000000000004.at-",
        "chain end: 10",
        "deleted: 2 object(s)",
        "stopped: 00000000000000000004-00000000000000000005.parquet holds records newer than \
         the cutoff",
    ] {
        assert!(stdout.contains(line), "missing {line:?} in:\n{stdout}");
    }
    let names = objects(dir.path());
    assert_eq!(names.len(), 4, "{names:?}");
    assert!(names[0].starts_with("00000000000000000004-"), "{names:?}");

    // The broker is out of reach; the destination still shows its floor.
    let status = mirror_v3(&cfg, "status", &["--format", "json"]);
    let stdout = text(&status.stdout);
    let rows: serde_json::Value = serde_json::from_str(stdout.trim()).expect(&stdout);
    let row = &rows[0];
    assert_eq!(row["dest_next"], 10, "{row}");
    assert_eq!(row["floor"], 4, "{row}");
    assert_eq!(row["below_floor_objects"], 0, "{row}");
    assert!(
        row["floor_moved_at"].as_str().unwrap().ends_with('Z'),
        "{row}"
    );
    assert!(row["oldest_record_above_floor"].is_string(), "{row}");

    let verify = mirror_v3(
        &cfg,
        "restore",
        &[
            "--mirror",
            "audit-backup",
            "--offsets",
            "renumber",
            "--verify-only",
        ],
    );
    let stdout = text(&verify.stdout);
    assert!(
        verify.status.success(),
        "{stdout}\n{}",
        text(&verify.stderr)
    );
    for line in [
        "floor: 4 (since ",
        "objects: 3",
        "offsets: 4-9",
        "records: 6",
        "verified",
    ] {
        assert!(stdout.contains(line), "missing {line:?} in:\n{stdout}");
    }
    // --chain-start still means exactly there.
    let exact = mirror_v3(
        &cfg,
        "restore",
        &[
            "--mirror",
            "audit-backup",
            "--offsets",
            "renumber",
            "--verify-only",
            "--chain-start",
            "0",
        ],
    );
    assert!(!exact.status.success());
    let stderr = text(&exact.stderr);
    assert!(stderr.contains("the backup's floor is 4"), "{stderr}");
    // A topic restored with its offsets starts at 0.
    let preserve = mirror_v3(
        &cfg,
        "restore",
        &[
            "--mirror",
            "audit-backup",
            "--offsets",
            "preserve",
            "--verify-only",
        ],
    );
    assert!(!preserve.status.success());
    assert!(text(&preserve.stderr).contains("the backup starts at offset 4"));

    // Nothing more has expired.
    let again = mirror_v3(&cfg, "trim", &[]);
    let stdout = text(&again.stdout);
    assert!(again.status.success(), "{stdout}");
    assert!(stdout.contains("floor: 4 (unchanged)"), "{stdout}");
}

#[test]
fn trim_floor_refuses_a_lower_floor_and_adopts_a_removed_head() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = backup(dir.path(), aged());
    let part = dir.path().join("data/audit/0");
    std::fs::remove_file(part.join(naming::blob_filename(0, 1, None, "parquet"))).unwrap();

    let refused = mirror_v3(&cfg, "trim", &[]);
    assert!(!refused.status.success());
    let stdout = text(&refused.stdout);
    assert!(stdout.contains("trim --floor 2"), "{stdout}");

    let adopt = mirror_v3(&cfg, "trim", &["--mirror", "audit-backup", "--floor", "2"]);
    let stdout = text(&adopt.stdout);
    assert!(adopt.status.success(), "{stdout}\n{}", text(&adopt.stderr));
    assert!(stdout.contains("floor: 0 -> 2"), "{stdout}");
    assert!(stdout.contains("set by: --floor"), "{stdout}");

    let lower = mirror_v3(&cfg, "trim", &["--mirror", "audit-backup", "--floor", "1"]);
    assert!(!lower.status.success());
    let stdout = text(&lower.stdout);
    assert!(
        stdout.contains("a floor lower than the current one, 2, is refused"),
        "{stdout}"
    );
    let missing = Command::new(bin())
        .args(["trim", "--config"])
        .arg(&cfg)
        .args(["--floor", "3"])
        .output()
        .unwrap();
    assert!(!missing.status.success(), "--floor needs --mirror");
}

#[test]
fn a_blocked_expiry_fails_the_trim() {
    let dir = tempfile::tempdir().unwrap();
    let mut records = aged();
    records[2] = rec(2, None);
    let cfg = backup(dir.path(), records);
    let out = mirror_v3(&cfg, "trim", &[]);
    assert!(!out.status.success());
    let stdout = text(&out.stdout);
    assert!(stdout.contains("floor: 0 -> 2"), "{stdout}");
    assert!(
        stdout.contains(
            "BLOCKED at 00000000000000000002-00000000000000000003.parquet: 1 record(s) without \
             a timestamp"
        ),
        "{stdout}"
    );
}
