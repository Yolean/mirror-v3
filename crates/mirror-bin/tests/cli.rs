use std::path::PathBuf;
use std::process::Command;

fn bin_path() -> PathBuf {
    // CARGO_BIN_EXE_<name> is set by Cargo for integration tests of
    // the same crate that owns the binary.
    PathBuf::from(env!("CARGO_BIN_EXE_mirror-v3"))
}

fn examples_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples")
}

#[test]
fn validate_accepts_each_example() {
    for entry in std::fs::read_dir(examples_dir()).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|s| s.to_str()) != Some("yaml") {
            continue;
        }
        let output = Command::new(bin_path())
            .arg("validate")
            .arg("--config")
            .arg(&path)
            .output()
            .expect("spawn mirror-v3");
        assert!(
            output.status.success(),
            "validate failed for {}:\nstdout: {}\nstderr: {}",
            path.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

#[test]
fn status_against_unreachable_broker_exits_with_error_row() {
    let dir = tempdir();
    let cfg = dir.join("status.yaml");
    // Bootstrap that we expect to fail-fast: localhost:1 is almost
    // certainly closed, and the watermark fetch in `status` has a 5s
    // timeout so this test wraps up quickly.
    let root = dir.join("data");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        &cfg,
        format!(
            r#"
destination:
  type: filesystem
  root: {}
  flush:
    max-time-ms: 1000
    max-bytes: 1024
    max-offsets: 10
mirrors:
  - name: probe
    source:
      bootstrap-servers: localhost:1
    topic: nope
    partition: 0
"#,
            root.display()
        ),
    )
    .unwrap();
    let output = std::process::Command::new(bin_path())
        .arg("status")
        .arg("--config")
        .arg(&cfg)
        .output()
        .expect("spawn mirror-v3 status");
    assert!(
        !output.status.success(),
        "status must exit non-zero when source is unreachable"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("MIRROR") && stdout.contains("SOURCE-HIGH"),
        "table header missing from stdout: {stdout}"
    );
    assert!(
        stdout.contains("error:"),
        "error row not visible in stdout: {stdout}"
    );
}

#[test]
fn status_json_format_is_valid_json() {
    let dir = tempdir();
    let cfg = dir.join("status.yaml");
    let root = dir.join("data");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        &cfg,
        format!(
            r#"
destination:
  type: filesystem
  root: {}
  flush:
    max-time-ms: 1000
    max-bytes: 1024
    max-offsets: 10
mirrors:
  - name: probe
    source:
      bootstrap-servers: localhost:1
    topic: nope
    partition: 0
"#,
            root.display()
        ),
    )
    .unwrap();
    let output = std::process::Command::new(bin_path())
        .arg("status")
        .arg("--config")
        .arg(&cfg)
        .args(["--format", "json"])
        .output()
        .expect("spawn mirror-v3 status --format json");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("not valid JSON ({e}): {stdout}"));
    let arr = parsed.as_array().expect("top-level must be an array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], "probe");
    assert!(arr[0]["error"].is_string(), "must carry an error string");
}

#[test]
fn validate_rejects_invalid_config() {
    let dir = tempdir();
    let bad = dir.join("bad.yaml");
    std::fs::write(&bad, "destination:\n  type: kafka\nmirrors: []\n").unwrap();
    let output = Command::new(bin_path())
        .arg("validate")
        .arg("--config")
        .arg(&bad)
        .output()
        .expect("spawn mirror-v3");
    assert!(
        !output.status.success(),
        "expected non-zero exit for missing bootstrap-servers; stdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

fn tempdir() -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "mirror-v3-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}
