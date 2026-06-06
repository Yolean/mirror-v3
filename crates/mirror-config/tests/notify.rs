//! Parse + validation tests for the `notify` block (WEBHOOKS.md).
//!
//! Each rule from "Validation" in WEBHOOKS.md is one test. The
//! positive-path tests are also worth keeping because they pin
//! the spec's defaults - if a future commit changes
//! `notify.timeout-ms`'s default from 5000, `defaults_apply_when_omitted`
//! fails and the operator-facing semantics get reviewed.

use mirror_config::{
    load_from_str, FinalAction, NotifyApi, NotifyDebounce, NotifyOutcome, NotifyRetry, TriggerOn,
};

/// Helper: minimal mirror with destinations + http-access + a kkv-v1
/// notify block. Used by the positive-path tests so each assertion
/// only varies the field under test.
const MINIMAL_WITH_NOTIFY: &str = r#"
mirrors:
  - name: events
    source: { bootstrap-servers: kafka:9092 }
    topic: events-stream
    partition: 0
    destinations:
      - type: filesystem
        root: /var/mirror
    format: parquet
    compression: zstd-1
    http-access: { api: cache-v1 }
    flush:
      max-time-ms: 60000
      max-bytes: 67108864
      max-offsets: 10000
    notify:
      api: kkv-v1
      targets:
        - url: http://events-cache:8080
"#;

#[test]
fn minimal_notify_block_parses_with_all_defaults() {
    let cfg = load_from_str(MINIMAL_WITH_NOTIFY).expect("must parse");
    let m = &cfg.mirrors[0];
    let notify = m.notify.as_ref().expect("notify must be present");

    assert_eq!(notify.api, NotifyApi::KkvV1);
    assert_eq!(notify.targets.len(), 1);
    assert_eq!(notify.targets[0].url, "http://events-cache:8080");
    assert_eq!(notify.targets[0].path, None);
    assert_eq!(notify.targets[0].fan_out, mirror_config::FanOut::None);

    // Spec-default trigger + debounce.
    assert_eq!(notify.trigger.on, TriggerOn::SourceConsume);
    assert_eq!(
        notify.trigger.debounce,
        Some(NotifyDebounce {
            max_records: 100,
            max_time_ms: 250
        })
    );

    // Spec-default timeout / retry.
    assert_eq!(notify.timeout_ms, 5000);
    assert_eq!(
        notify.retry,
        NotifyRetry {
            max_attempts: 5,
            backoff_ms: 100
        }
    );

    // Spec-default outcomes table.
    let o = notify.outcomes;
    assert_eq!(o.timeout, ok_retry_fail());
    assert_eq!(o.connrefused, ok_retry_fail());
    assert_eq!(o.two_xx, no_retry_accept());
    assert_eq!(o.three_xx, no_retry_fail());
    assert_eq!(o.four_xx, no_retry_fail());
    assert_eq!(o.five_xx, ok_retry_fail());
}

#[test]
fn explicit_outcomes_override_per_field() {
    // Operators can override only the outcomes they care about; the
    // rest still fall back to spec defaults. Test sets 4xx to skip,
    // expects others to stay default.
    let yaml = format!(
        "{MINIMAL_WITH_NOTIFY}      outcomes:\n        4xx: {{ retry: false, final: skip }}\n"
    );
    let cfg = load_from_str(&yaml).expect("must parse");
    let o = cfg.mirrors[0].notify.as_ref().unwrap().outcomes;
    assert_eq!(
        o.four_xx,
        NotifyOutcome {
            retry: false,
            final_: FinalAction::Skip
        }
    );
    // Others kept their defaults.
    assert_eq!(o.timeout, ok_retry_fail());
    assert_eq!(o.two_xx, no_retry_accept());
}

#[test]
fn destination_flush_trigger_parses_without_debounce() {
    let yaml = format!("{MINIMAL_WITH_NOTIFY}      trigger:\n        on: destination-flush\n");
    let cfg = load_from_str(&yaml).expect("must parse");
    let trigger = &cfg.mirrors[0].notify.as_ref().unwrap().trigger;
    assert_eq!(trigger.on, TriggerOn::DestinationFlush);
    assert_eq!(trigger.debounce, None);
}

#[test]
fn target_path_and_fanout_parse_when_set() {
    let yaml = r#"
mirrors:
  - name: events
    source: { bootstrap-servers: kafka:9092 }
    topic: events
    partition: 0
    destinations:
      - type: filesystem
        root: /var/mirror
    http-access: { api: cache-v1 }
    flush:
      max-time-ms: 60000
      max-bytes: 67108864
      max-offsets: 10000
    notify:
      api: kkv-v1
      targets:
        - url: http://my-headless-service:8080
          path: /custom/path
          fan-out: dns-a
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    let t = &cfg.mirrors[0].notify.as_ref().unwrap().targets[0];
    assert_eq!(t.path.as_deref(), Some("/custom/path"));
    assert_eq!(t.fan_out, mirror_config::FanOut::DnsA);
}

// ============================================================
//   Validation failures
// ============================================================

#[test]
fn notify_without_http_access_rejected() {
    let yaml = r#"
mirrors:
  - name: events
    source: { bootstrap-servers: kafka:9092 }
    topic: events
    partition: 0
    destinations:
      - type: filesystem
        root: /var/mirror
    flush:
      max-time-ms: 60000
      max-bytes: 67108864
      max-offsets: 10000
    notify:
      api: kkv-v1
      targets:
        - url: http://events-cache:8080
"#;
    let err = load_from_str(yaml).expect_err("must reject");
    let msg = format!("{err}");
    assert!(
        msg.contains("notify") && msg.contains("http-access"),
        "got: {msg}"
    );
}

#[test]
fn notify_with_empty_targets_rejected() {
    let yaml = r#"
mirrors:
  - name: events
    source: { bootstrap-servers: kafka:9092 }
    topic: events
    partition: 0
    destinations:
      - type: filesystem
        root: /var/mirror
    http-access: { api: cache-v1 }
    flush:
      max-time-ms: 60000
      max-bytes: 67108864
      max-offsets: 10000
    notify:
      api: kkv-v1
      targets: []
"#;
    let err = load_from_str(yaml).expect_err("must reject");
    let msg = format!("{err}");
    assert!(
        msg.contains("notify.targets") && msg.contains("at least one"),
        "got: {msg}"
    );
}

#[test]
fn notify_target_with_invalid_url_rejected() {
    let yaml = r#"
mirrors:
  - name: events
    source: { bootstrap-servers: kafka:9092 }
    topic: events
    partition: 0
    destinations:
      - type: filesystem
        root: /var/mirror
    http-access: { api: cache-v1 }
    flush:
      max-time-ms: 60000
      max-bytes: 67108864
      max-offsets: 10000
    notify:
      api: kkv-v1
      targets:
        - url: "not a url at all"
"#;
    let err = load_from_str(yaml).expect_err("must reject");
    let msg = format!("{err}");
    assert!(
        msg.contains("notify.targets[0].url") && msg.contains("not a valid URL"),
        "got: {msg}"
    );
}

#[test]
fn notify_target_with_non_http_scheme_rejected() {
    let yaml = r#"
mirrors:
  - name: events
    source: { bootstrap-servers: kafka:9092 }
    topic: events
    partition: 0
    destinations:
      - type: filesystem
        root: /var/mirror
    http-access: { api: cache-v1 }
    flush:
      max-time-ms: 60000
      max-bytes: 67108864
      max-offsets: 10000
    notify:
      api: kkv-v1
      targets:
        - url: ftp://still-a-url-but-wrong-scheme:21
"#;
    let err = load_from_str(yaml).expect_err("must reject");
    let msg = format!("{err}");
    assert!(
        msg.contains("scheme http or https") && msg.contains("ftp"),
        "got: {msg}"
    );
}

#[test]
fn destination_flush_trigger_with_explicit_debounce_rejected() {
    let yaml = r#"
mirrors:
  - name: events
    source: { bootstrap-servers: kafka:9092 }
    topic: events
    partition: 0
    destinations:
      - type: filesystem
        root: /var/mirror
    http-access: { api: cache-v1 }
    flush:
      max-time-ms: 60000
      max-bytes: 67108864
      max-offsets: 10000
    notify:
      api: kkv-v1
      targets:
        - url: http://events-cache:8080
      trigger:
        on: destination-flush
        debounce: { max-records: 100, max-time-ms: 250 }
"#;
    let err = load_from_str(yaml).expect_err("must reject");
    let msg = format!("{err}");
    assert!(
        msg.contains("debounce") && msg.contains("destination-flush"),
        "got: {msg}"
    );
}

#[test]
fn debounce_zero_max_records_rejected() {
    let yaml = r#"
mirrors:
  - name: events
    source: { bootstrap-servers: kafka:9092 }
    topic: events
    partition: 0
    destinations:
      - type: filesystem
        root: /var/mirror
    http-access: { api: cache-v1 }
    flush:
      max-time-ms: 60000
      max-bytes: 67108864
      max-offsets: 10000
    notify:
      api: kkv-v1
      targets:
        - url: http://events-cache:8080
      trigger:
        on: source-consume
        debounce: { max-records: 0, max-time-ms: 250 }
"#;
    let err = load_from_str(yaml).expect_err("must reject");
    let msg = format!("{err}");
    assert!(
        msg.contains("debounce.max-records") && msg.contains(">= 1"),
        "got: {msg}"
    );
}

#[test]
fn zero_timeout_ms_rejected() {
    let yaml = r#"
mirrors:
  - name: events
    source: { bootstrap-servers: kafka:9092 }
    topic: events
    partition: 0
    destinations:
      - type: filesystem
        root: /var/mirror
    http-access: { api: cache-v1 }
    flush:
      max-time-ms: 60000
      max-bytes: 67108864
      max-offsets: 10000
    notify:
      api: kkv-v1
      targets:
        - url: http://events-cache:8080
      timeout-ms: 0
"#;
    let err = load_from_str(yaml).expect_err("must reject");
    let msg = format!("{err}");
    assert!(
        msg.contains("timeout-ms") && msg.contains(">= 1"),
        "got: {msg}"
    );
}

#[test]
fn zero_retry_max_attempts_rejected() {
    let yaml = r#"
mirrors:
  - name: events
    source: { bootstrap-servers: kafka:9092 }
    topic: events
    partition: 0
    destinations:
      - type: filesystem
        root: /var/mirror
    http-access: { api: cache-v1 }
    flush:
      max-time-ms: 60000
      max-bytes: 67108864
      max-offsets: 10000
    notify:
      api: kkv-v1
      targets:
        - url: http://events-cache:8080
      retry:
        max-attempts: 0
        backoff-ms: 100
"#;
    let err = load_from_str(yaml).expect_err("must reject");
    let msg = format!("{err}");
    assert!(
        msg.contains("retry.max-attempts") && msg.contains(">= 1"),
        "got: {msg}"
    );
}

// ============================================================
//   Notify-only mirrors (destinations: [])
// ============================================================

#[test]
fn notify_only_mirror_parses() {
    let yaml = r#"
mirrors:
  - name: invalidator
    source: { bootstrap-servers: kafka:9092 }
    topic: events
    partition: 0
    destinations: []
    notify:
      api: kkv-v1
      targets:
        - url: http://cache-target:8080
          fan-out: dns-a
"#;
    let cfg = load_from_str(yaml).expect("must parse");
    let m = &cfg.mirrors[0];
    assert!(m.destinations.is_empty());
    assert!(m.notify.is_some());
}

#[test]
fn destinations_empty_without_notify_still_rejected() {
    // Regression: the pre-WEBHOOKS rule (destinations must be
    // non-empty) survives unless notify is present.
    let yaml = r#"
mirrors:
  - name: empty
    source: { bootstrap-servers: kafka:9092 }
    topic: events
    partition: 0
    destinations: []
"#;
    let err = load_from_str(yaml).expect_err("must reject");
    let msg = format!("{err}");
    assert!(
        msg.contains("destinations") && msg.contains("at least one"),
        "got: {msg}"
    );
}

#[test]
fn notify_only_with_destination_flush_trigger_rejected() {
    let yaml = r#"
mirrors:
  - name: invalidator
    source: { bootstrap-servers: kafka:9092 }
    topic: events
    partition: 0
    destinations: []
    notify:
      api: kkv-v1
      targets:
        - url: http://cache-target:8080
      trigger:
        on: destination-flush
"#;
    let err = load_from_str(yaml).expect_err("must reject");
    let msg = format!("{err}");
    assert!(
        msg.contains("notify-only") && msg.contains("source-consume"),
        "got: {msg}"
    );
}

#[test]
fn notify_only_with_http_access_rejected() {
    let yaml = r#"
mirrors:
  - name: invalidator
    source: { bootstrap-servers: kafka:9092 }
    topic: events
    partition: 0
    destinations: []
    http-access: { api: cache-v1 }
    notify:
      api: kkv-v1
      targets:
        - url: http://cache-target:8080
"#;
    let err = load_from_str(yaml).expect_err("must reject");
    let msg = format!("{err}");
    assert!(
        msg.contains("notify-only") && msg.contains("http-access"),
        "got: {msg}"
    );
}

#[test]
fn notify_only_with_format_rejected() {
    let yaml = r#"
mirrors:
  - name: invalidator
    source: { bootstrap-servers: kafka:9092 }
    topic: events
    partition: 0
    destinations: []
    format: parquet
    notify:
      api: kkv-v1
      targets:
        - url: http://cache-target:8080
"#;
    let err = load_from_str(yaml).expect_err("must reject");
    let msg = format!("{err}");
    assert!(
        msg.contains("notify-only") && msg.contains("format"),
        "got: {msg}"
    );
}

#[test]
fn notify_only_with_flush_rejected() {
    let yaml = r#"
mirrors:
  - name: invalidator
    source: { bootstrap-servers: kafka:9092 }
    topic: events
    partition: 0
    destinations: []
    flush:
      max-time-ms: 5000
      max-bytes: 1000
      max-offsets: 100
    notify:
      api: kkv-v1
      targets:
        - url: http://cache-target:8080
"#;
    let err = load_from_str(yaml).expect_err("must reject");
    let msg = format!("{err}");
    assert!(
        msg.contains("notify-only") && msg.contains("flush"),
        "got: {msg}"
    );
}

#[test]
fn notify_only_with_empty_targets_rejected() {
    let yaml = r#"
mirrors:
  - name: invalidator
    source: { bootstrap-servers: kafka:9092 }
    topic: events
    partition: 0
    destinations: []
    notify:
      api: kkv-v1
      targets: []
"#;
    let err = load_from_str(yaml).expect_err("must reject");
    let msg = format!("{err}");
    assert!(
        msg.contains("notify-only") && msg.contains("targets"),
        "got: {msg}"
    );
}

// ============================================================
//   Helpers
// ============================================================

fn ok_retry_fail() -> NotifyOutcome {
    NotifyOutcome {
        retry: true,
        final_: FinalAction::Fail,
    }
}

fn no_retry_fail() -> NotifyOutcome {
    NotifyOutcome {
        retry: false,
        final_: FinalAction::Fail,
    }
}

fn no_retry_accept() -> NotifyOutcome {
    NotifyOutcome {
        retry: false,
        final_: FinalAction::Accept,
    }
}

#[test]
fn destination_flush_with_only_kafka_destination_is_rejected_transitively() {
    // Per WEBHOOKS.md: "A mirror with no blob destinations (kafka-
    // only) cannot use `destination-flush`". The validator enforces
    // this transitively: notify requires http-access, http-access
    // requires ≥1 blob destination - so kafka-only + notify is
    // already rejected, regardless of trigger mode. This test pins
    // that the rejection happens.
    let yaml = r#"
mirrors:
  - name: events
    source: { bootstrap-servers: kafka:9092 }
    topic: events
    partition: 0
    destinations:
      - type: kafka
        bootstrap-servers: kafka:9092
    notify:
      api: kkv-v1
      targets:
        - url: http://target:8080
      trigger:
        on: destination-flush
"#;
    let err = load_from_str(yaml).expect_err("kafka-only + notify must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("notify") || msg.contains("http-access"),
        "got: {msg}"
    );
}

#[test]
fn destination_flush_with_filesystem_destination_is_accepted() {
    let yaml = format!("{MINIMAL_WITH_NOTIFY}      trigger:\n        on: destination-flush\n");
    let cfg = load_from_str(&yaml).expect("must parse");
    assert_eq!(
        cfg.mirrors[0].notify.as_ref().unwrap().trigger.on,
        TriggerOn::DestinationFlush
    );
}
