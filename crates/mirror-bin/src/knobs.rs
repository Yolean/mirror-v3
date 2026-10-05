//! Process settings from `MIRROR_V3_*` environment variables, read once
//! at startup. Unset means the default; a value that does not parse is a
//! startup error naming the variable (they used to fall back to the
//! default silently, so a typo changed behaviour without a trace).

use std::time::Duration;

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Knobs {
    /// `MIRROR_V3_CACHE_PORT` (8080): the `/cache/v1` and health server.
    pub cache_port: u16,
    /// `MIRROR_V3_METRICS_PORT` (9090): the Prometheus exporter.
    pub metrics_port: u16,
    /// `MIRROR_V3_READINESS_LAG` (0): offsets of lag before the readiness
    /// body's status says `lag_behind_source` (not the HTTP status).
    pub readiness_lag: u64,
    /// `MIRROR_V3_READINESS_POLL_MS` (2000): how often each mirror's
    /// source high watermark and assignment are re-read; 0 disables.
    pub readiness_poll: Duration,
    /// `MIRROR_V3_OFFSET_COMMIT_INTERVAL_MS` (5000): how often delivered
    /// progress is committed to the source group; 0 disables.
    pub commit_interval: Duration,
    /// `MIRROR_V3_HEARTBEAT_SECS` (30): the debug heartbeat; 0 disables.
    pub heartbeat: Duration,
}

impl Knobs {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        fn num<T: std::str::FromStr>(
            get: &impl Fn(&str) -> Option<String>,
            name: &str,
            default: T,
        ) -> Result<T>
        where
            T::Err: std::error::Error + Send + Sync + 'static,
        {
            match get(name) {
                None => Ok(default),
                Some(v) => v.parse().with_context(|| {
                    format!("environment variable {name}={v:?} is not a valid number")
                }),
            }
        }
        Ok(Self {
            cache_port: num(&get, "MIRROR_V3_CACHE_PORT", 8080)?,
            metrics_port: num(&get, "MIRROR_V3_METRICS_PORT", 9090)?,
            readiness_lag: num(&get, "MIRROR_V3_READINESS_LAG", 0)?,
            readiness_poll: Duration::from_millis(num(&get, "MIRROR_V3_READINESS_POLL_MS", 2000)?),
            commit_interval: Duration::from_millis(num(
                &get,
                "MIRROR_V3_OFFSET_COMMIT_INTERVAL_MS",
                5000,
            )?),
            heartbeat: Duration::from_secs(num(&get, "MIRROR_V3_HEARTBEAT_SECS", 30)?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_means_default() {
        let k = Knobs::from_lookup(|_| None).unwrap();
        assert_eq!(k.cache_port, 8080);
        assert_eq!(k.commit_interval, Duration::from_secs(5));
    }

    #[test]
    fn a_value_that_does_not_parse_is_an_error() {
        let err = Knobs::from_lookup(|n| (n == "MIRROR_V3_CACHE_PORT").then(|| "80a".into()))
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("MIRROR_V3_CACHE_PORT=\"80a\""),
            "{err:#}"
        );
    }
}
