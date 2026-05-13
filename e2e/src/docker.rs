//! Docker-based provisioner (first impl of [`crate::Provisioner`]).
//!
//! Containers are managed by `testcontainers`. Both brokers advertise
//! `localhost:<host_port>` so the test process — running on the host —
//! can connect via the mapped port. The host port is pre-picked with
//! `portpicker` because we have to know the advertised value *before*
//! the container starts.

use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use crate::{ProvisionedStack, Provisioner};

const REDPANDA_IMAGE: &str = "docker.io/redpandadata/redpanda";
const REDPANDA_TAG: &str = "latest";
const KAFKA_NATIVE_IMAGE: &str = "quay.io/ogunalp/kafka-native";
const KAFKA_NATIVE_TAG: &str = "latest";

/// Provision a `kafka-native (source) → redpanda (target)` stack on
/// the local Docker daemon.
pub struct DockerProvisioner;

#[async_trait]
impl Provisioner for DockerProvisioner {
    type Stack = KafkaNativeToRedpandaStack;
    async fn provision(self) -> Result<Self::Stack> {
        KafkaNativeToRedpandaStack::start().await
    }
}

pub struct KafkaNativeToRedpandaStack {
    _source: ContainerAsync<GenericImage>,
    _target: ContainerAsync<GenericImage>,
    source_port: u16,
    target_port: u16,
}

impl KafkaNativeToRedpandaStack {
    pub async fn start() -> Result<Self> {
        let source_port = portpicker::pick_unused_port().context("no free port for source")?;
        let target_port = portpicker::pick_unused_port().context("no free port for target")?;

        let source = start_kafka_native(source_port).await?;
        let target = start_redpanda(target_port).await?;

        // Quick liveness probe — fetch metadata to confirm the broker
        // is actually serving on the advertised port. Without this we
        // get flaky "connection refused" mid-test.
        wait_for_metadata(&format!("localhost:{source_port}")).await?;
        wait_for_metadata(&format!("localhost:{target_port}")).await?;

        Ok(Self {
            _source: source,
            _target: target,
            source_port,
            target_port,
        })
    }
}

async fn start_kafka_native(host_port: u16) -> Result<ContainerAsync<GenericImage>> {
    GenericImage::new(KAFKA_NATIVE_IMAGE, KAFKA_NATIVE_TAG)
        .with_exposed_port(ContainerPort::Tcp(9092))
        .with_wait_for(WaitFor::message_on_stdout("Kafka broker started"))
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!("PLAINTEXT://localhost:{host_port}"),
        )
        .with_env_var(
            "KAFKA_LISTENERS",
            "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
        )
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
        )
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .with_mapped_port(host_port, ContainerPort::Tcp(9092))
        .start()
        .await
        .context("starting kafka-native container")
}

async fn start_redpanda(host_port: u16) -> Result<ContainerAsync<GenericImage>> {
    GenericImage::new(REDPANDA_IMAGE, REDPANDA_TAG)
        .with_exposed_port(9092.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Successfully started Redpanda"))
        .with_cmd([
            "redpanda".into(),
            "start".into(),
            "--mode".into(),
            "dev-container".into(),
            "--smp".into(),
            "1".into(),
            "--kafka-addr".into(),
            "PLAINTEXT://0.0.0.0:9092".into(),
            "--advertise-kafka-addr".into(),
            format!("PLAINTEXT://localhost:{host_port}"),
        ])
        .with_mapped_port(host_port, ContainerPort::Tcp(9092))
        .start()
        .await
        .context("starting redpanda container")
}

async fn wait_for_metadata(bootstrap: &str) -> Result<()> {
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::util::Timeout;

    let bootstrap = bootstrap.to_string();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let bs = bootstrap.clone();
        let ok = tokio::task::spawn_blocking(move || -> Result<()> {
            let consumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &bs)
                .set("group.id", "mirror-e2e-probe")
                .create()
                .context("probe consumer")?;
            consumer
                .fetch_metadata(None, Timeout::After(Duration::from_secs(2)))
                .map(|_| ())
                .map_err(|e| anyhow::anyhow!("fetch_metadata: {e}"))
        })
        .await
        .map_err(|e| anyhow::anyhow!("join: {e}"))?;
        if ok.is_ok() {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            return ok;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[async_trait]
impl ProvisionedStack for KafkaNativeToRedpandaStack {
    fn source_bootstrap(&self) -> String {
        format!("localhost:{}", self.source_port)
    }
    fn target_kafka_bootstrap(&self) -> Option<String> {
        Some(format!("localhost:{}", self.target_port))
    }
}
