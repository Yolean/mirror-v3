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
