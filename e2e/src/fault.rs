//! Fault-injection vocabulary.
//!
//! Concrete fault-injectors (Toxiproxy in Phase 2 follow-up, others
//! later) translate these into their own primitives.

#[derive(Debug, Clone, Copy)]
pub enum FaultTarget {
    /// The link between the mirror and the source broker.
    SourceLink,
    /// The link between the mirror and the target broker / endpoint.
    TargetLink,
}

#[derive(Debug, Clone)]
pub enum Fault {
    /// Drop the connection entirely.
    Down,
    /// Add `latency_ms` ± `jitter_ms` of latency per packet.
    Latency { latency_ms: u32, jitter_ms: u32 },
    /// Cap the connection's throughput, in kilobytes per second.
    Bandwidth { kbps: u32 },
}
