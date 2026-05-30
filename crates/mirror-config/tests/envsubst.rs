//! Integration tests for env interpolation in config loading.
//!
//! These exercise the wiring between `envsubst::expand` and the
//! `Config` parser, not just the expansion algorithm itself (which
//! is covered by unit tests inside the `envsubst` module).

use std::collections::HashMap;

use mirror_config::envsubst::Env;
use mirror_config::{load_from_str_with_env, Destination};

struct MockEnv(HashMap<&'static str, &'static str>);

impl Env for MockEnv {
    fn lookup(&self, name: &str) -> Option<String> {
        self.0.get(name).map(|s| s.to_string())
    }
}

fn env(pairs: &[(&'static str, &'static str)]) -> MockEnv {
    MockEnv(pairs.iter().copied().collect())
}

#[test]
fn dry_dual_write_via_env_vars() {
    // The motivating use case: two destinations that share most
    // settings, with only the per-region differences resolved from
    // env vars at startup.
    let yaml = r#"
mirrors:
  - name: ${MIRROR_NAME}
    source:
      bootstrap-servers: ${SOURCE_BROKER}
      group-id: ${MIRROR_NAME}-${ENV}
    topic: ${TOPIC}
    partition: 0
    destinations:
      - type: s3
        name: ${TOPIC}-us
        region: us-east-1
        bucket: ${BUCKET_PREFIX}-us-east-1
        endpoint: ${S3_ENDPOINT:-}
        prefix: archive/
      - type: s3
        name: ${TOPIC}-eu
        region: eu-west-1
        bucket: ${BUCKET_PREFIX}-eu-west-1
        endpoint: ${S3_ENDPOINT:-}
        prefix: archive/
    format: parquet
    compression: zstd-1
    flush:
      max-time-ms: 5000
      max-bytes: 67108864
      max-offsets: 10000
"#;
    let e = env(&[
        ("MIRROR_NAME", "orders"),
        ("SOURCE_BROKER", "kafka.prod:9092"),
        ("ENV", "prod"),
        ("TOPIC", "orders"),
        ("BUCKET_PREFIX", "yolean-mirror"),
        // S3_ENDPOINT intentionally unset; the `:-` default leaves
        // the endpoint blank (AWS regional endpoint inferred from
        // region).
    ]);
    let cfg = load_from_str_with_env(yaml, &e).expect("must parse");
    let m = &cfg.mirrors[0];
    assert_eq!(m.name, "orders");
    assert_eq!(m.source.bootstrap_servers, "kafka.prod:9092");
    assert_eq!(m.source.group_id.as_deref(), Some("orders-prod"));
    assert_eq!(m.topic, "orders");
    assert_eq!(m.destinations.len(), 2);
    match &m.destinations[0] {
        Destination::S3(s3) => {
            assert_eq!(s3.name.as_deref(), Some("orders-us"));
            assert_eq!(s3.region, "us-east-1");
            assert_eq!(s3.bucket, "yolean-mirror-us-east-1");
            // ${S3_ENDPOINT:-} with no env override expands to the
            // empty string. After substitution the YAML scalar
            // `endpoint: ` is empty, which serde_yaml deserialises
            // as YAML null and `Option<String>` as `None`. This is
            // the operator-friendly outcome: AWS regional endpoint
            // inferred from the region.
            assert_eq!(s3.endpoint, None);
        }
        _ => panic!("expected S3"),
    }
    match &m.destinations[1] {
        Destination::S3(s3) => {
            assert_eq!(s3.region, "eu-west-1");
            assert_eq!(s3.bucket, "yolean-mirror-eu-west-1");
        }
        _ => panic!("expected S3"),
    }
}

#[test]
fn missing_required_var_errors() {
    let yaml = r#"
mirrors:
  - name: ops
    source: { bootstrap-servers: ${MISSING_BROKER} }
    topic: ops
    partition: 0
    destinations:
      - type: kafka
        bootstrap-servers: ${MISSING_TARGET}
"#;
    let err = load_from_str_with_env(yaml, &env(&[])).expect_err("must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("env interpolation") && msg.contains("MISSING_BROKER"),
        "got: {msg}"
    );
}

#[test]
fn default_is_used_for_unset_var() {
    let yaml = r#"
mirrors:
  - name: ops
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
    destinations:
      - type: kafka
        bootstrap-servers: ${DST_BROKER:-localhost:9092}
"#;
    let cfg = load_from_str_with_env(yaml, &env(&[])).expect("must parse");
    match &cfg.mirrors[0].destinations[0] {
        Destination::Kafka(k) => assert_eq!(k.bootstrap_servers, "localhost:9092"),
        _ => panic!("expected kafka"),
    }
}

#[test]
fn double_dollar_escapes_to_literal_in_yaml() {
    let yaml = r#"
mirrors:
  - name: ops
    source: { bootstrap-servers: "kafka:9092" }
    topic: "ops$$1"     # operator wants a literal `$1` in the topic name
    partition: 0
    destinations:
      - type: kafka
        bootstrap-servers: "kafka:9092"
"#;
    let cfg = load_from_str_with_env(yaml, &env(&[])).expect("must parse");
    assert_eq!(cfg.mirrors[0].topic, "ops$1");
}

#[test]
fn plain_config_without_env_refs_round_trips() {
    // Regression: a config with no `${...}` must parse identically
    // whether env-subst is enabled or not.
    let yaml = r#"
mirrors:
  - name: ops
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
    destinations:
      - type: kafka
        bootstrap-servers: redpanda:9092
"#;
    let cfg = load_from_str_with_env(yaml, &env(&[])).expect("must parse");
    assert_eq!(cfg.mirrors.len(), 1);
    assert_eq!(cfg.mirrors[0].source.bootstrap_servers, "kafka:9092");
}
