use mirror_config::{
    load_from_str, Config, Destination, DestinationFormat, FilesystemDestination, FlushTriggers,
    KafkaDestination, KafkaSource, Mirror, ParquetCompression, S3Destination,
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
                destination_name_override: None,
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
fn parses_filesystem_destination() {
    let yaml = r#"
destination:
  type: filesystem
  root: /var/mirror-v3
  flush:
    max-time-ms: 5000
    max-bytes: 1048576
    max-offsets: 1000
mirrors:
  - name: operations
    source:
      bootstrap-servers: kafka-source:9092
    topic: operations-v1
    partition: 0
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(
        cfg.destination,
        Destination::Filesystem(FilesystemDestination {
            root: PathBuf::from("/var/mirror-v3"),
            format: DestinationFormat::default(),
            compression: ParquetCompression::default(),
            flush: FlushTriggers {
                max_time_ms: 5000,
                max_bytes: 1_048_576,
                max_offsets: 1000,
            },
        })
    );
}

#[test]
fn parses_s3_destination_with_endpoint() {
    let yaml = r#"
destination:
  type: s3
  endpoint: http://versitygw:7070
  region: us-east-1
  bucket: mirror-v3
  prefix: archive/
  flush:
    max-time-ms: 60000
    max-bytes: 16777216
    max-offsets: 10000
mirrors:
  - name: operations
    source:
      bootstrap-servers: kafka-source:9092
    topic: operations-v1
    partition: 0
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(
        cfg.destination,
        Destination::S3(S3Destination {
            endpoint: Some("http://versitygw:7070".into()),
            region: "us-east-1".into(),
            bucket: "mirror-v3".into(),
            prefix: Some("archive/".into()),
            format: DestinationFormat::default(),
            compression: ParquetCompression::default(),
            flush: FlushTriggers {
                max_time_ms: 60_000,
                max_bytes: 16_777_216,
                max_offsets: 10_000,
            },
        })
    );
}

#[test]
fn unknown_field_is_rejected() {
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
    let err = load_from_str(yaml).expect_err("unknown fields must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("typo_field") || msg.contains("unknown field"),
        "got: {msg}"
    );
}

#[test]
fn destination_name_override_parses() {
    let yaml = r#"
destination:
  type: kafka
  bootstrap-servers: redpanda:9092
mirrors:
  - name: operations
    source:
      bootstrap-servers: kafka-source:9092
    topic: operations-v1
    partition: 3
    destination-name-override: operations-mirrored
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(cfg.mirrors.len(), 1);
    assert_eq!(cfg.mirrors[0].partition, 3);
    assert_eq!(
        cfg.mirrors[0].destination_name_override.as_deref(),
        Some("operations-mirrored")
    );
}

#[test]
fn multiple_mirrors_parse() {
    let yaml = r#"
destination:
  type: filesystem
  root: /var/mirror-v3
  flush:
    max-time-ms: 5000
    max-bytes: 1048576
    max-offsets: 1000
mirrors:
  - name: ops-p0
    source: { bootstrap-servers: kafka:9092 }
    topic: operations-v1
    partition: 0
  - name: ops-p1
    source: { bootstrap-servers: kafka:9092 }
    topic: operations-v1
    partition: 1
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    assert_eq!(cfg.mirrors.len(), 2);
    assert_eq!(cfg.mirrors[1].partition, 1);
}
