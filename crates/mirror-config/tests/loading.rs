use mirror_config::{
    load_from_str, ColumnConfig, ColumnType, Compaction, Config, Destination, DestinationFormat,
    FilesystemDestination, FlushTriggers, HttpAccess, HttpAccessApi, KafkaDestination, KafkaSource,
    Mirror, S3Destination, TimestampMode,
};
use std::path::PathBuf;

const MINIMAL_KAFKA: &str = r#"
mirrors:
  - name: operations
    source:
      bootstrap-servers: kafka-source:9092
    topic: operations-v1
    partition: 0
    destinations:
      - type: kafka
        bootstrap-servers: redpanda:9092
"#;

#[test]
fn parses_minimal_kafka_config() {
    let cfg = load_from_str(MINIMAL_KAFKA).expect("must parse");
    assert_eq!(
        cfg,
        Config {
            mirrors: vec![Mirror {
                name: "operations".into(),
                source: KafkaSource {
                    bootstrap_servers: "kafka-source:9092".into(),
                    group_id: None,
                },
                topic: "operations-v1".into(),
                partition: 0,
                destinations: vec![Destination::Kafka(KafkaDestination {
                    name: None,
                    bootstrap_servers: "redpanda:9092".into(),
                    topic: None,
                })],
                format: None,
                compression: None,
                keys: None,
                values: None,
                compaction: None,
                flush: None,
                timestamp_mode: None,
                http_access: None,
                enabled: None,
                notify: None,
            }],
        }
    );
    // Kafka destination defaults: name → mirror.name, topic → mirror.topic
    let dest = &cfg.mirrors[0].destinations[0];
    assert_eq!(dest.effective_name(&cfg.mirrors[0].name), "operations");
    // Defaults: enabled is None in YAML, is_enabled() reports true.
    assert!(cfg.mirrors[0].is_enabled());
}

#[test]
fn kafka_destination_topic_override_parses() {
    let yaml = r#"
mirrors:
  - name: archive
    source: { bootstrap-servers: source:9092 }
    topic: orders
    partition: 0
    destinations:
      - type: kafka
        bootstrap-servers: backup:9092
        topic: orders-backup
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    let dest = match &cfg.mirrors[0].destinations[0] {
        Destination::Kafka(k) => k,
        _ => panic!("expected kafka destination"),
    };
    assert_eq!(dest.topic.as_deref(), Some("orders-backup"));
}

#[test]
fn partition_is_required() {
    let yaml = r#"
mirrors:
  - name: operations
    source:
      bootstrap-servers: kafka-source:9092
    topic: operations-v1
    destinations:
      - type: kafka
        bootstrap-servers: redpanda:9092
"#;
    let err = load_from_str(yaml).expect_err("partition is required");
    let msg = format!("{err}");
    assert!(
        msg.contains("partition"),
        "error must mention the missing field, got: {msg}"
    );
}

#[test]
fn parses_filesystem_destination_with_per_mirror_encoding() {
    let yaml = r#"
mirrors:
  - name: operations
    source:
      bootstrap-servers: kafka-source:9092
    topic: operations-v1
    partition: 0
    destinations:
      - type: filesystem
        root: /var/mirror-v3
    flush:
      max-time-ms: 5000
      max-bytes: 1048576
      max-offsets: 1000
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(
        cfg.mirrors[0].destinations[0],
        Destination::Filesystem(FilesystemDestination {
            name: None,
            root: PathBuf::from("/var/mirror-v3"),
        })
    );
    let m = &cfg.mirrors[0];
    assert_eq!(m.format, None);
    assert_eq!(m.keys, None);
    assert_eq!(m.values, None);
    assert_eq!(m.compaction, None);
    assert_eq!(
        m.flush,
        Some(FlushTriggers {
            max_time_ms: 5000,
            max_bytes: 1_048_576,
            max_offsets: 1000,
            daily: None,
        })
    );
}

#[test]
fn parses_s3_destination_with_endpoint_only() {
    let yaml = r#"
mirrors:
  - name: operations
    source:
      bootstrap-servers: kafka-source:9092
    topic: operations-v1
    partition: 0
    destinations:
      - type: s3
        endpoint: http://versitygw:7070
        region: us-east-1
        bucket: mirror-v3
        prefix: archive/
    flush:
      max-time-ms: 60000
      max-bytes: 16777216
      max-offsets: 10000
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(
        cfg.mirrors[0].destinations[0],
        Destination::S3(S3Destination {
            name: None,
            endpoint: Some("http://versitygw:7070".into()),
            region: "us-east-1".into(),
            bucket: "mirror-v3".into(),
            prefix: Some("archive/".into()),
        })
    );
}

#[test]
fn tee_fs_and_s3_with_explicit_names_parses() {
    // The PoC payoff: one mirror, two destinations, distinct names.
    let yaml = r#"
mirrors:
  - name: orders
    source: { bootstrap-servers: source:9092 }
    topic: orders
    partition: 0
    destinations:
      - type: filesystem
        name: local-archive
        root: /var/mirror
      - type: s3
        name: offsite-archive
        region: us-east-1
        bucket: orders-archive
    format: parquet
    flush:
      max-time-ms: 5000
      max-bytes: 67108864
      max-offsets: 10000
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(cfg.mirrors[0].destinations.len(), 2);
    assert_eq!(
        cfg.mirrors[0].destinations[0].effective_name(&cfg.mirrors[0].name),
        "local-archive"
    );
    assert_eq!(
        cfg.mirrors[0].destinations[1].effective_name(&cfg.mirrors[0].name),
        "offsite-archive"
    );
}

#[test]
fn parses_two_mirrors_with_distinct_encoding() {
    // One process, two mirrors against one S3 bucket, each with its
    // own encoding profile.
    let yaml = r#"
mirrors:
  - name: orders
    source: { bootstrap-servers: redpanda:9092 }
    topic: orders
    partition: 0
    destinations:
      - type: s3
        endpoint: http://versitygw:7070
        region: us-east-1
        bucket: cache
        prefix: archive/
    format: parquet
    compression: zstd-1
    values: { type: json }
    flush:
      max-time-ms: 5000
      max-bytes: 67108864
      max-offsets: 10000
  - name: user-states
    source: { bootstrap-servers: redpanda:9092 }
    topic: user-states
    partition: 0
    destinations:
      - type: s3
        endpoint: http://versitygw:7070
        region: us-east-1
        bucket: cache
        prefix: archive/
    format: parquet
    compression: zstd-1
    compaction: log
    flush:
      max-time-ms: 5000
      max-bytes: 67108864
      max-offsets: 10000
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(cfg.mirrors.len(), 2);
    let orders = &cfg.mirrors[0];
    let user_states = &cfg.mirrors[1];
    assert_eq!(orders.format, Some(DestinationFormat::Parquet));
    assert_eq!(
        orders.values,
        Some(ColumnConfig {
            kind: ColumnType::Json
        })
    );
    assert_eq!(orders.compaction, None);
    assert_eq!(user_states.compaction, Some(Compaction::Log));
    assert_eq!(user_states.values, None);
}

#[test]
fn unknown_field_on_destination_is_rejected() {
    let yaml = r#"
mirrors:
  - name: operations
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
    destinations:
      - type: kafka
        bootstrap-servers: redpanda:9092
        typo_field: 123
"#;
    let err = load_from_str(yaml).expect_err("unknown field must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("typo_field") || msg.contains("unknown field"),
        "got: {msg}"
    );
}

#[test]
fn unknown_field_on_mirror_is_rejected() {
    let yaml = r#"
mirrors:
  - name: operations
    source:
      bootstrap-servers: kafka-source:9092
    topic: operations-v1
    partition: 0
    destinations:
      - type: kafka
        bootstrap-servers: redpanda:9092
    typo_field: 123
"#;
    let err = load_from_str(yaml).expect_err("unknown field must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("typo_field") || msg.contains("unknown field"),
        "got: {msg}"
    );
}

#[test]
fn encoding_fields_forbidden_for_kafka_only_mirrors() {
    let yaml = r#"
mirrors:
  - name: operations
    source:
      bootstrap-servers: kafka-source:9092
    topic: operations-v1
    partition: 0
    destinations:
      - type: kafka
        bootstrap-servers: redpanda:9092
    format: parquet
"#;
    let err = load_from_str(yaml).expect_err("format on kafka-only mirror must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("format") && msg.contains("filesystem/s3"),
        "got: {msg}"
    );
}

#[test]
fn timestamp_mode_forbidden_when_no_kafka_destination() {
    let yaml = r#"
mirrors:
  - name: operations
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
    destinations:
      - type: filesystem
        root: /tmp/mirror
    timestamp-mode: destination
    flush:
      max-time-ms: 5000
      max-bytes: 1000
      max-offsets: 100
"#;
    let err = load_from_str(yaml).expect_err("timestamp-mode on fs-only mirror must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("timestamp-mode") && msg.contains("Kafka"),
        "got: {msg}"
    );
}

#[test]
fn flush_required_for_blob_destinations() {
    let yaml = r#"
mirrors:
  - name: operations
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
    destinations:
      - type: filesystem
        root: /tmp/mirror
"#;
    let err = load_from_str(yaml).expect_err("missing flush must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("flush") && msg.contains("required"),
        "got: {msg}"
    );
}

#[test]
fn parses_kafka_destination_with_per_mirror_timestamp_mode() {
    let yaml = r#"
mirrors:
  - name: operations
    source: { bootstrap-servers: kafka-source:9092 }
    topic: ops
    partition: 0
    destinations:
      - type: kafka
        bootstrap-servers: redpanda:9092
    timestamp-mode: destination
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(
        cfg.mirrors[0].timestamp_mode,
        Some(TimestampMode::Destination)
    );
}

#[test]
fn compaction_log_with_default_keys_parses() {
    let yaml = r#"
mirrors:
  - name: states
    source: { bootstrap-servers: kafka:9092 }
    topic: states
    partition: 0
    destinations:
      - type: filesystem
        root: /tmp/mirror
    compaction: log
    flush:
      max-time-ms: 5000
      max-bytes: 1000
      max-offsets: 100
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(cfg.mirrors[0].compaction, Some(Compaction::Log));
    assert_eq!(cfg.mirrors[0].keys, None);
}

#[test]
fn compaction_log_requires_parquet_format() {
    let yaml = r#"
mirrors:
  - name: states
    source: { bootstrap-servers: kafka:9092 }
    topic: states
    partition: 0
    destinations:
      - type: filesystem
        root: /tmp/mirror
    format: ndjson
    compaction: log
    flush:
      max-time-ms: 5000
      max-bytes: 1000
      max-offsets: 100
"#;
    let err = load_from_str(yaml).expect_err("compaction + ndjson must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("compaction") && msg.contains("parquet"),
        "got: {msg}"
    );
}

#[test]
fn keys_and_values_default_to_utf8() {
    let yaml = r#"
mirrors:
  - name: ops
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
    destinations:
      - type: filesystem
        root: /tmp/mirror
    keys: {}
    values: {}
    flush:
      max-time-ms: 5000
      max-bytes: 1000
      max-offsets: 100
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(
        cfg.mirrors[0].keys,
        Some(ColumnConfig {
            kind: ColumnType::Utf8
        })
    );
    assert_eq!(
        cfg.mirrors[0].values,
        Some(ColumnConfig {
            kind: ColumnType::Utf8
        })
    );
}

#[test]
fn keys_bytes_base64_parses() {
    let yaml = r#"
mirrors:
  - name: ops
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
    destinations:
      - type: filesystem
        root: /tmp/mirror
    keys: { type: bytes }
    flush:
      max-time-ms: 5000
      max-bytes: 1000
      max-offsets: 100
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(
        cfg.mirrors[0].keys,
        Some(ColumnConfig {
            kind: ColumnType::Bytes
        })
    );
}

#[test]
fn ndjson_rejects_non_default_keys_or_values() {
    let yaml = r#"
mirrors:
  - name: ops
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
    destinations:
      - type: filesystem
        root: /tmp/mirror
    format: ndjson
    values: { type: json }
    flush:
      max-time-ms: 5000
      max-bytes: 1000
      max-offsets: 100
"#;
    let err = load_from_str(yaml).expect_err("ndjson + non-default values must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("ndjson") && msg.contains("parquet"),
        "got: {msg}"
    );
}

#[test]
fn duplicate_mirror_names_are_rejected() {
    let yaml = r#"
mirrors:
  - name: ops
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
    destinations:
      - type: kafka
        bootstrap-servers: redpanda:9092
  - name: ops
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 1
    destinations:
      - type: kafka
        bootstrap-servers: redpanda:9092
"#;
    let err = load_from_str(yaml).expect_err("duplicate mirror name must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("ops") && msg.contains("more than once"),
        "got: {msg}"
    );
}

#[test]
fn multiple_mirrors_parse() {
    let yaml = r#"
mirrors:
  - name: ops-p0
    source: { bootstrap-servers: kafka:9092 }
    topic: operations-v1
    partition: 0
    destinations:
      - type: filesystem
        root: /var/mirror-v3
    flush:
      max-time-ms: 5000
      max-bytes: 1048576
      max-offsets: 1000
  - name: ops-p1
    source: { bootstrap-servers: kafka:9092 }
    topic: operations-v1
    partition: 1
    destinations:
      - type: filesystem
        root: /var/mirror-v3
    flush:
      max-time-ms: 5000
      max-bytes: 1048576
      max-offsets: 1000
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(cfg.mirrors.len(), 2);
    assert_eq!(cfg.mirrors[1].partition, 1);
}

#[test]
fn http_access_cache_v1_parses() {
    let yaml = r#"
mirrors:
  - name: user-states
    source: { bootstrap-servers: kafka:9092 }
    topic: user-states
    partition: 0
    destinations:
      - type: filesystem
        root: /tmp/mirror
    http-access:
      api: cache-v1
    flush:
      max-time-ms: 5000
      max-bytes: 1000
      max-offsets: 100
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(
        cfg.mirrors[0].http_access,
        Some(HttpAccess {
            api: HttpAccessApi::CacheV1
        })
    );
}

#[test]
fn http_access_forbidden_for_kafka_only_mirrors() {
    let yaml = r#"
mirrors:
  - name: operations
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
    destinations:
      - type: kafka
        bootstrap-servers: redpanda:9092
    http-access:
      api: cache-v1
"#;
    let err = load_from_str(yaml).expect_err("http-access on kafka-only mirror must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("http-access") && msg.contains("filesystem/s3"),
        "got: {msg}"
    );
}

#[test]
fn http_access_rejects_bytes_keys() {
    let yaml = r#"
mirrors:
  - name: ops
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
    destinations:
      - type: filesystem
        root: /tmp/mirror
    keys: { type: bytes }
    http-access:
      api: cache-v1
    flush:
      max-time-ms: 5000
      max-bytes: 1000
      max-offsets: 100
"#;
    let err = load_from_str(yaml).expect_err("http-access + bytes-keys must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("http-access") && msg.contains("utf8"),
        "got: {msg}"
    );
}

#[test]
fn http_access_works_without_compaction() {
    let yaml = r#"
mirrors:
  - name: cache-only
    source: { bootstrap-servers: kafka:9092 }
    topic: orders
    partition: 0
    destinations:
      - type: filesystem
        root: /tmp/mirror
    http-access:
      api: cache-v1
    flush:
      max-time-ms: 5000
      max-bytes: 1000
      max-offsets: 100
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(cfg.mirrors[0].compaction, None);
    assert!(cfg.mirrors[0].http_access.is_some());
}

#[test]
fn empty_destinations_rejected() {
    let yaml = r#"
mirrors:
  - name: ops
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
    destinations: []
"#;
    let err = load_from_str(yaml).expect_err("empty destinations must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("destinations") && msg.contains("at least one"),
        "got: {msg}"
    );
}

#[test]
fn multi_destination_without_explicit_names_rejected() {
    // Two destinations defaulting to mirror.name → collision; force
    // operator to set names so logs/metrics can attribute correctly.
    let yaml = r#"
mirrors:
  - name: ops
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
    destinations:
      - type: filesystem
        root: /tmp/a
      - type: filesystem
        root: /tmp/b
    flush:
      max-time-ms: 5000
      max-bytes: 1000
      max-offsets: 100
"#;
    let err = load_from_str(yaml).expect_err("must reject duplicate default names");
    let msg = format!("{err}");
    assert!(msg.contains("explicit `name`"), "got: {msg}");
}

#[test]
fn multi_destination_with_duplicate_explicit_names_rejected() {
    let yaml = r#"
mirrors:
  - name: ops
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
    destinations:
      - type: filesystem
        name: same
        root: /tmp/a
      - type: filesystem
        name: same
        root: /tmp/b
    flush:
      max-time-ms: 5000
      max-bytes: 1000
      max-offsets: 100
"#;
    let err = load_from_str(yaml).expect_err("must reject duplicate dest names");
    let msg = format!("{err}");
    assert!(
        msg.contains("destination name") && msg.contains("more than once"),
        "got: {msg}"
    );
}

#[test]
fn mixed_kafka_and_blob_destinations_parse() {
    // The mixed-tee case: Kafka destination alongside a blob
    // destination. flush is required (for the blob); timestamp-mode
    // is allowed (for the Kafka). format/compression apply to the
    // blob.
    let yaml = r#"
mirrors:
  - name: orders
    source: { bootstrap-servers: source:9092 }
    topic: orders
    partition: 0
    destinations:
      - type: kafka
        name: mirror-broker
        bootstrap-servers: mirror:9092
      - type: filesystem
        name: archive
        root: /var/mirror
    format: parquet
    timestamp-mode: source
    flush:
      max-time-ms: 5000
      max-bytes: 67108864
      max-offsets: 10000
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(cfg.mirrors[0].destinations.len(), 2);
    assert_eq!(cfg.mirrors[0].timestamp_mode, Some(TimestampMode::Source));
    assert_eq!(cfg.mirrors[0].format, Some(DestinationFormat::Parquet));
}
