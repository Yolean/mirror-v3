use mirror_config::{
    load_from_str, ColumnConfig, ColumnType, Compaction, Config, Destination, DestinationFormat,
    FilesystemDestination, FlushTriggers, KafkaDestination, KafkaSource, Mirror, S3Destination,
    TimestampMode,
};
use std::path::PathBuf;

const MINIMAL_KAFKA: &str = r#"
destination:
  type: kafka
  bootstrap-servers: redpanda:9092
mirrors:
  - name: operations
    source:
      bootstrap-servers: kafka-source:9092
    topic: operations-v1
    partition: 0
"#;

#[test]
fn parses_minimal_kafka_config() {
    let cfg = load_from_str(MINIMAL_KAFKA).expect("must parse");
    assert_eq!(
        cfg,
        Config {
            destination: Destination::Kafka(KafkaDestination {
                bootstrap_servers: "redpanda:9092".into(),
            }),
            mirrors: vec![Mirror {
                name: "operations".into(),
                source: KafkaSource {
                    bootstrap_servers: "kafka-source:9092".into(),
                    group_id: None,
                },
                topic: "operations-v1".into(),
                partition: 0,
                format: None,
                compression: None,
                keys: None,
                values: None,
                compaction: None,
                flush: None,
                timestamp_mode: None,
            }],
        }
    );
}

#[test]
fn partition_is_required() {
    let yaml = r#"
destination:
  type: kafka
  bootstrap-servers: redpanda:9092
mirrors:
  - name: operations
    source:
      bootstrap-servers: kafka-source:9092
    topic: operations-v1
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
destination:
  type: filesystem
  root: /var/mirror-v3
mirrors:
  - name: operations
    source:
      bootstrap-servers: kafka-source:9092
    topic: operations-v1
    partition: 0
    flush:
      max-time-ms: 5000
      max-bytes: 1048576
      max-offsets: 1000
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(
        cfg.destination,
        Destination::Filesystem(FilesystemDestination {
            root: PathBuf::from("/var/mirror-v3"),
        })
    );
    assert_eq!(cfg.mirrors.len(), 1);
    let m = &cfg.mirrors[0];
    assert_eq!(m.name, "operations");
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
destination:
  type: s3
  endpoint: http://versitygw:7070
  region: us-east-1
  bucket: mirror-v3
  prefix: archive/
mirrors:
  - name: operations
    source:
      bootstrap-servers: kafka-source:9092
    topic: operations-v1
    partition: 0
    flush:
      max-time-ms: 60000
      max-bytes: 16777216
      max-offsets: 10000
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(
        cfg.destination,
        Destination::S3(S3Destination {
            endpoint: Some("http://versitygw:7070".into()),
            region: "us-east-1".into(),
            bucket: "mirror-v3".into(),
            prefix: Some("archive/".into()),
        })
    );
}

#[test]
fn parses_two_mirrors_with_distinct_encoding() {
    // The PoC payoff: one process, two mirrors against one bucket, each
    // with its own encoding profile.
    let yaml = r#"
destination:
  type: s3
  endpoint: http://versitygw:7070
  region: us-east-1
  bucket: cache
  prefix: archive/
mirrors:
  - name: orders
    source: { bootstrap-servers: redpanda:9092 }
    topic: orders
    partition: 0
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
    assert_eq!(user_states.values, None); // defaults to utf8
}

#[test]
fn unknown_field_on_destination_is_rejected() {
    let yaml = r#"
destination:
  type: kafka
  bootstrap-servers: redpanda:9092
  typo_field: 123
mirrors: []
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
destination:
  type: kafka
  bootstrap-servers: redpanda:9092
mirrors:
  - name: operations
    source:
      bootstrap-servers: kafka-source:9092
    topic: operations-v1
    partition: 0
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
fn encoding_fields_forbidden_for_kafka_destinations() {
    let yaml = r#"
destination:
  type: kafka
  bootstrap-servers: redpanda:9092
mirrors:
  - name: operations
    source:
      bootstrap-servers: kafka-source:9092
    topic: operations-v1
    partition: 0
    format: parquet
"#;
    let err = load_from_str(yaml).expect_err("format on kafka mirror must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("format") && msg.contains("filesystem/s3"),
        "got: {msg}"
    );
}

#[test]
fn timestamp_mode_forbidden_for_non_kafka_destinations() {
    let yaml = r#"
destination:
  type: filesystem
  root: /tmp/mirror
mirrors:
  - name: operations
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
    timestamp-mode: destination
    flush:
      max-time-ms: 5000
      max-bytes: 1000
      max-offsets: 100
"#;
    let err = load_from_str(yaml).expect_err("timestamp-mode on fs mirror must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("timestamp-mode") && msg.contains("kafka"),
        "got: {msg}"
    );
}

#[test]
fn flush_required_for_blob_destinations() {
    let yaml = r#"
destination:
  type: filesystem
  root: /tmp/mirror
mirrors:
  - name: operations
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
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
destination:
  type: kafka
  bootstrap-servers: redpanda:9092
mirrors:
  - name: operations
    source: { bootstrap-servers: kafka-source:9092 }
    topic: ops
    partition: 0
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
destination:
  type: filesystem
  root: /tmp/mirror
mirrors:
  - name: states
    source: { bootstrap-servers: kafka:9092 }
    topic: states
    partition: 0
    compaction: log
    flush:
      max-time-ms: 5000
      max-bytes: 1000
      max-offsets: 100
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(cfg.mirrors[0].compaction, Some(Compaction::Log));
    // keys defaults to utf8, which is compatible with compaction.
    assert_eq!(cfg.mirrors[0].keys, None);
}

#[test]
fn compaction_log_requires_parquet_format() {
    let yaml = r#"
destination:
  type: filesystem
  root: /tmp/mirror
mirrors:
  - name: states
    source: { bootstrap-servers: kafka:9092 }
    topic: states
    partition: 0
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
fn compaction_log_rejects_bytes_keys() {
    let yaml = r#"
destination:
  type: filesystem
  root: /tmp/mirror
mirrors:
  - name: states
    source: { bootstrap-servers: kafka:9092 }
    topic: states
    partition: 0
    keys: { type: bytes }
    compaction: log
    flush:
      max-time-ms: 5000
      max-bytes: 1000
      max-offsets: 100
"#;
    let err = load_from_str(yaml).expect_err("compaction + bytes-keys must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("compaction") && msg.contains("utf8"),
        "got: {msg}"
    );
}

#[test]
fn keys_and_values_default_to_utf8() {
    let yaml = r#"
destination:
  type: filesystem
  root: /tmp/mirror
mirrors:
  - name: ops
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
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
fn keys_bytes_parses() {
    let yaml = r#"
destination:
  type: filesystem
  root: /tmp/mirror
mirrors:
  - name: ops
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
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
destination:
  type: filesystem
  root: /tmp/mirror
mirrors:
  - name: ops
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
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
destination:
  type: kafka
  bootstrap-servers: redpanda:9092
mirrors:
  - name: ops
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
  - name: ops
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 1
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
destination:
  type: filesystem
  root: /var/mirror-v3
mirrors:
  - name: ops-p0
    source: { bootstrap-servers: kafka:9092 }
    topic: operations-v1
    partition: 0
    flush:
      max-time-ms: 5000
      max-bytes: 1048576
      max-offsets: 1000
  - name: ops-p1
    source: { bootstrap-servers: kafka:9092 }
    topic: operations-v1
    partition: 1
    flush:
      max-time-ms: 5000
      max-bytes: 1048576
      max-offsets: 1000
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(cfg.mirrors.len(), 2);
    assert_eq!(cfg.mirrors[1].partition, 1);
}
