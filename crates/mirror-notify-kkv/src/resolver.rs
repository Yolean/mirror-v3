//! DNS resolver trait used by the `fan-out: dns-a` dispatch path.
//!
//! Production uses [`SystemDnsResolver`] which wraps
//! `tokio::net::lookup_host`. Tests inject a stub that returns canned
//! `SocketAddr`s — that lets the multi-address fan-out path be
//! exercised against axum servers bound on different ports without
//! depending on the system resolver or `/etc/hosts`.
//!
//! All addresses returned by a single call share the URL's port in
//! production (lookup_host appends the port to every result). The
//! trait nonetheless returns `SocketAddr`s so test stubs can supply
//! arbitrary `(IP, port)` pairs.

use std::net::SocketAddr;

use async_trait::async_trait;

#[async_trait]
pub trait DnsAResolver: Send + Sync {
    /// Resolve `host:port` to the full A/AAAA address set.
    async fn resolve(&self, host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>>;
}

/// `tokio::net::lookup_host` wrapper — the default resolver used by
/// [`crate::KkvV1Notifier::from_config`].
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemDnsResolver;

#[async_trait]
impl DnsAResolver for SystemDnsResolver {
    async fn resolve(&self, host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
        // `lookup_host` accepts both `"host:port"` strings and
        // `(host, port)` tuples; the tuple form skips the
        // `&str → SocketAddr` parsing fast-path's allocation when
        // `host` is a name.
        let mut out = Vec::new();
        for sa in tokio::net::lookup_host((host, port)).await? {
            out.push(sa);
        }
        Ok(out)
    }
}
