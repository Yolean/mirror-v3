//! End-to-end test harness for mirror-v3.
//!
//! ## Why this crate exists
//!
//! Unit tests verify the loop logic against mocks (`mirror-core`). The
//! Kafka transport (`mirror-kafka`) only gets exercised by spinning up
//! a real broker. Test infra also has to be **pluggable**: the user
//! wants new ways to provision an environment (different runners,
//! different fault-injectors) added without rewriting the tests.
//!
//! The two trait seams below — [`Provisioner`] and
//! [`ProvisionedStack`] — are that pluggable surface. The first impl
//! is [`docker::DockerProvisioner`]; future impls (kind, real cloud)
//! drop in next to it without touching the test files in
//! `e2e/tests/`.

pub mod docker;
pub mod fault;
pub mod kafka_helpers;
pub mod mirror_runner;

use async_trait::async_trait;

/// A way to bring a test environment online.
#[async_trait]
pub trait Provisioner: Sized + Send {
    type Stack: ProvisionedStack;
    async fn provision(self) -> anyhow::Result<Self::Stack>;
}

/// A running test environment. Endpoints come out of here; faults go
/// in. Cleanup is `Drop`-based by convention so even panicking tests
/// release containers.
#[async_trait]
pub trait ProvisionedStack: Send + Sync {
    /// Source Kafka bootstrap, always present.
    fn source_bootstrap(&self) -> String;

    /// Target Kafka bootstrap for Kafka-sink stacks. `None` for blob
    /// destinations.
    fn target_kafka_bootstrap(&self) -> Option<String> {
        None
    }

    /// S3 endpoint URL for S3-sink stacks (Phase 4). `None` otherwise.
    fn target_s3_endpoint(&self) -> Option<String> {
        None
    }

    /// Inject a network fault. Default: not supported. Stacks that
    /// wrap their endpoints with a fault-injector (e.g. Toxiproxy)
    /// override this.
    async fn inject_fault(
        &mut self,
        _target: fault::FaultTarget,
        _fault: fault::Fault,
    ) -> anyhow::Result<()> {
        anyhow::bail!("this stack does not support fault injection")
    }
}
