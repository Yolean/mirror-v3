//! Tests for the optional `enabled` field on mirrors.
//!
//! The field is a plain YAML boolean — `true`/`false` only (plus
//! the YAML-1.2 case variants the YAML spec / serde_yaml already
//! accept natively). YAML-1.1 truthy strings (`yes`/`no`/`on`/`off`)
//! are deliberately rejected so operators don't have two ways to
//! express the same thing.
//!
//! Env interpolation works because `${REQUESTS_ENABLED:-false}`
//! expands to the literal text `false` *before* YAML parsing,
//! which YAML parses as the boolean false.

use std::collections::HashMap;

use mirror_config::envsubst::Env;
use mirror_config::{load_from_str, load_from_str_with_env};

struct MockEnv(HashMap<&'static str, &'static str>);
impl Env for MockEnv {
    fn lookup(&self, name: &str) -> Option<String> {
        self.0.get(name).map(|s| s.to_string())
    }
}
fn env(pairs: &[(&'static str, &'static str)]) -> MockEnv {
    MockEnv(pairs.iter().copied().collect())
}

const TEMPLATE: &str = r#"
mirrors:
  - name: ops
    source: { bootstrap-servers: kafka:9092 }
    topic: ops
    partition: 0
    destinations:
      - type: kafka
        bootstrap-servers: redpanda:9092
"#;

fn with_enabled(enabled_line: &str) -> String {
    let mut yaml = TEMPLATE.to_string();
    yaml.push_str("    ");
    yaml.push_str(enabled_line);
    yaml.push('\n');
    yaml
}

#[test]
fn missing_enabled_defaults_to_true() {
    let cfg = load_from_str(TEMPLATE).expect("must parse");
    assert_eq!(cfg.mirrors[0].enabled, None);
    assert!(cfg.mirrors[0].is_enabled());
}

#[test]
fn explicit_yaml_booleans_parse() {
    for (literal, expected) in [
        ("enabled: true", true),
        ("enabled: false", false),
        ("enabled: True", true),
        ("enabled: FALSE", false),
    ] {
        let yaml = with_enabled(literal);
        let cfg = load_from_str(&yaml).unwrap_or_else(|e| panic!("must parse {literal:?}: {e}"));
        assert_eq!(
            cfg.mirrors[0].enabled,
            Some(expected),
            "for literal {literal:?}"
        );
        assert_eq!(cfg.mirrors[0].is_enabled(), expected);
    }
}

#[test]
fn yaml_1_1_truthy_strings_are_rejected() {
    // Deliberately not accepted. Operators get one way to express
    // a boolean — `true` or `false` — so config diffs across
    // mirrors stay grep-able.
    for literal in [
        "enabled: yes",
        "enabled: no",
        "enabled: on",
        "enabled: off",
        "enabled: y",
        "enabled: n",
    ] {
        let yaml = with_enabled(literal);
        let err = load_from_str(&yaml).expect_err(&format!("must reject {literal:?}"));
        let msg = format!("{err}");
        assert!(
            msg.contains("bool") || msg.contains("boolean"),
            "expected a boolean-type error for {literal:?}, got: {msg}"
        );
    }
}

#[test]
fn invalid_string_is_rejected() {
    let yaml = with_enabled("enabled: maybe");
    let err = load_from_str(&yaml).expect_err("must reject");
    let msg = format!("{err}");
    assert!(
        msg.contains("bool") || msg.contains("boolean"),
        "expected a boolean-type error, got: {msg}"
    );
}

#[test]
fn env_interp_default_true_keeps_mirror_enabled() {
    let yaml = with_enabled("enabled: ${REQUESTS_ENABLED:-true}");
    let cfg = load_from_str_with_env(&yaml, &env(&[])).expect("must parse");
    assert!(cfg.mirrors[0].is_enabled());
}

#[test]
fn env_interp_default_false_disables_mirror() {
    // The example from the spec: `enabled: ${REQUESTS_ENABLED:-false}`
    // — mirror starts disabled unless the operator opts in via env.
    let yaml = with_enabled("enabled: ${REQUESTS_ENABLED:-false}");
    let cfg = load_from_str_with_env(&yaml, &env(&[])).expect("must parse");
    assert!(!cfg.mirrors[0].is_enabled());
}

#[test]
fn env_interp_resolves_via_env_var() {
    let yaml = with_enabled("enabled: ${REQUESTS_ENABLED}");
    let cfg_on = load_from_str_with_env(&yaml, &env(&[("REQUESTS_ENABLED", "true")]))
        .expect("must parse true");
    assert!(cfg_on.mirrors[0].is_enabled());

    let cfg_off = load_from_str_with_env(&yaml, &env(&[("REQUESTS_ENABLED", "false")]))
        .expect("must parse false");
    assert!(!cfg_off.mirrors[0].is_enabled());
}

#[test]
fn env_interp_invalid_value_is_rejected() {
    let yaml = with_enabled("enabled: ${REQUESTS_ENABLED}");
    let err = load_from_str_with_env(&yaml, &env(&[("REQUESTS_ENABLED", "yes")]))
        .expect_err("YAML-1.1 truthy still rejected after env expansion");
    let msg = format!("{err}");
    assert!(
        msg.contains("bool") || msg.contains("boolean"),
        "expected a boolean-type error, got: {msg}"
    );
}
