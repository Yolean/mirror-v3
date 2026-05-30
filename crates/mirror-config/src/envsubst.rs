//! Shell-style `${VAR}` interpolation for YAML config text.
//!
//! ## Syntax (same as [`Yolean/y-cluster`](https://github.com/Yolean/y-cluster)'s `envsubst`)
//!
//!   `${VAR}`            — required; errors if VAR is not set.
//!   `${VAR:-default}`   — optional; uses `default` if VAR is unset.
//!                          `default` may be empty (`${VAR:-}`).
//!   `$$`                — literal `$` (escape; no expansion).
//!
//! Variable names match `[A-Za-z_][A-Za-z0-9_]*`. Bare `$VAR` (no
//! braces) is intentionally not supported: braces make the scan
//! unambiguous and avoid surprises with shell-like word boundaries.
//! A stray `$` that isn't `$$` and isn't followed by `{` is passed
//! through literally.
//!
//! Substitution is **single-pass**: an expanded value is not
//! re-scanned for further `${...}`. This matches `envsubst(1)`'s
//! default and keeps behaviour predictable.
//!
//! ## Scope (v1: pre-parse text-level expansion)
//!
//! [`Self::expand`] operates on the YAML file's raw text *before*
//! parsing. Every `${...}` reference is expanded, regardless of
//! whether it appears in a value position the schema considers
//! "substitutable". Operators get the DRY win that motivated the
//! feature ([`see KafkaSource.bootstrap_servers`,
//! [`S3Destination.bucket`], etc.]) and forward-compat protection
//! is documented but not enforced (no per-field opt-in via struct
//! tags). If a future operator needs to put a literal `${VAR}` in
//! a value and have it NOT expand, they can escape with `$$`.

use std::fmt;

#[derive(Debug, PartialEq, Eq)]
pub enum EnvSubstError {
    /// `${VAR}` referenced an unset variable that did not provide
    /// `:-default`. The string is the variable name.
    UndefinedVariable(String),
    /// An unterminated `${...}` reference (no closing `}`).
    Unterminated,
}

impl fmt::Display for EnvSubstError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UndefinedVariable(name) => write!(f, "undefined variable {name:?}"),
            Self::Unterminated => write!(f, "unterminated `${{...}}` reference"),
        }
    }
}

impl std::error::Error for EnvSubstError {}

/// Trait for the variable resolver. Production callers use
/// [`OsEnv`]; tests can pass a [`HashMap`]-backed `MockEnv` to
/// avoid touching the real process environment.
pub trait Env {
    fn lookup(&self, name: &str) -> Option<String>;
}

/// [`Env`] implementation backed by `std::env::var`.
pub struct OsEnv;

impl Env for OsEnv {
    fn lookup(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
}

/// Expand `${VAR}` and `${VAR:-default}` references in `input`,
/// using `env` to resolve variable names. `$$` becomes a literal
/// `$`. See module docs for the full syntax.
pub fn expand(input: &str, env: &dyn Env) -> Result<String, EnvSubstError> {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.char_indices().peekable();
    while let Some((_i, c)) = chars.next() {
        if c != '$' {
            out.push(c);
            continue;
        }
        // Peek the next char to decide what kind of `$` this is.
        let Some(&(_j, next)) = chars.peek() else {
            out.push('$');
            break;
        };
        match next {
            '$' => {
                // `$$` -> literal `$`. Consume the second `$`.
                chars.next();
                out.push('$');
            }
            '{' => {
                // Consume `{`, then collect the `NAME[:-default]` body.
                chars.next();
                let body = consume_until_close(&mut chars).ok_or(EnvSubstError::Unterminated)?;
                let (name, default) = split_name_default(&body);
                if !is_valid_name(name) {
                    // Unrecognised body: pass through literally as
                    // `${...}`. This matches shell envsubst's "if it
                    // doesn't look like a variable, leave it alone".
                    out.push_str("${");
                    out.push_str(&body);
                    out.push('}');
                    continue;
                }
                match env.lookup(name) {
                    Some(v) => out.push_str(&v),
                    None => match default {
                        Some(d) => out.push_str(d),
                        None => return Err(EnvSubstError::UndefinedVariable(name.to_string())),
                    },
                }
            }
            _ => {
                // Bare `$X` -> pass through literally. Documented
                // limitation: bare-dollar isn't a variable reference
                // in our grammar.
                out.push('$');
            }
        }
    }
    Ok(out)
}

/// Consume characters from the iterator until the matching `}` is
/// found. Returns the body (excluding the closing brace) or `None`
/// if the iterator runs out first.
fn consume_until_close(
    chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>,
) -> Option<String> {
    let mut body = String::new();
    for (_i, c) in chars.by_ref() {
        if c == '}' {
            return Some(body);
        }
        body.push(c);
    }
    None
}

/// Split a `NAME[:-default]` body into `(NAME, default)`. The
/// default is `None` when no `:-` separator is present, and
/// `Some("")` when present but empty.
fn split_name_default(body: &str) -> (&str, Option<&str>) {
    match body.find(":-") {
        Some(idx) => (&body[..idx], Some(&body[idx + 2..])),
        None => (body, None),
    }
}

fn is_valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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
    fn plain_text_passes_through() {
        let e = env(&[]);
        assert_eq!(expand("hello world", &e).unwrap(), "hello world");
        assert_eq!(expand("", &e).unwrap(), "");
    }

    #[test]
    fn var_required_present_is_substituted() {
        let e = env(&[("REGION", "us-east-1")]);
        assert_eq!(expand("region=${REGION}", &e).unwrap(), "region=us-east-1");
    }

    #[test]
    fn var_required_missing_errors() {
        let e = env(&[]);
        let err = expand("region=${REGION}", &e).expect_err("must err");
        assert_eq!(err, EnvSubstError::UndefinedVariable("REGION".into()));
    }

    #[test]
    fn var_with_default_used_when_missing() {
        let e = env(&[]);
        assert_eq!(
            expand("region=${REGION:-us-east-1}", &e).unwrap(),
            "region=us-east-1"
        );
    }

    #[test]
    fn var_with_default_overridden_when_set() {
        let e = env(&[("REGION", "eu-west-1")]);
        assert_eq!(
            expand("region=${REGION:-us-east-1}", &e).unwrap(),
            "region=eu-west-1"
        );
    }

    #[test]
    fn empty_default_is_distinct_from_no_default() {
        let e = env(&[]);
        assert_eq!(expand("x=${X:-}", &e).unwrap(), "x=");
        assert!(matches!(
            expand("x=${X}", &e),
            Err(EnvSubstError::UndefinedVariable(_))
        ));
    }

    #[test]
    fn double_dollar_escapes_to_literal() {
        let e = env(&[("FOO", "BAD")]);
        assert_eq!(expand("$$FOO", &e).unwrap(), "$FOO");
        assert_eq!(expand("$${FOO}", &e).unwrap(), "${FOO}");
        assert_eq!(expand("$$$$", &e).unwrap(), "$$");
    }

    #[test]
    fn bare_dollar_passes_through() {
        let e = env(&[("FOO", "BAR")]);
        // `$FOO` (no braces) is not a variable reference; left alone.
        assert_eq!(expand("price=$5", &e).unwrap(), "price=$5");
        assert_eq!(expand("$FOO", &e).unwrap(), "$FOO");
    }

    #[test]
    fn trailing_dollar_passes_through() {
        let e = env(&[]);
        assert_eq!(expand("foo$", &e).unwrap(), "foo$");
    }

    #[test]
    fn unterminated_braces_errors() {
        let e = env(&[]);
        assert_eq!(expand("oops ${FOO", &e), Err(EnvSubstError::Unterminated));
    }

    #[test]
    fn invalid_variable_name_passes_through_literal() {
        // ${123} is not a valid name; we don't substitute, but we
        // also don't error -- pass through as literal so a user can
        // put `${1.2.3}` etc. in YAML strings (e.g. version refs).
        let e = env(&[]);
        assert_eq!(expand("${123}", &e).unwrap(), "${123}");
        assert_eq!(expand("${1.2.3}", &e).unwrap(), "${1.2.3}");
    }

    #[test]
    fn multiple_references_in_one_string() {
        let e = env(&[("A", "1"), ("B", "2")]);
        assert_eq!(expand("${A}-${B}-${A}", &e).unwrap(), "1-2-1");
    }

    #[test]
    fn substituted_values_are_not_rescanned() {
        // Single-pass: if A expands to "${B}", we DO NOT then expand B.
        let e = env(&[("A", "${B}"), ("B", "wrong")]);
        assert_eq!(expand("${A}", &e).unwrap(), "${B}");
    }

    #[test]
    fn first_undefined_is_reported() {
        let e = env(&[]);
        // Two undefined refs; first one wins (matches y-cluster's
        // behaviour of reporting the first error).
        let err = expand("${MISSING_A}-${MISSING_B}", &e).expect_err("must err");
        assert_eq!(err, EnvSubstError::UndefinedVariable("MISSING_A".into()));
    }
}
