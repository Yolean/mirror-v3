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
const VERSITYGW_IMAGE: &str = "docker.io/versity/versitygw";
const VERSITYGW_TAG: &str = "latest";

/// Test credentials used by the VersityGW stack.
pub const VERSITYGW_ACCESS_KEY: &str = "admin";
pub const VERSITYGW_SECRET_KEY: &str = "password";

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

/// `kafka-native (source) -> versitygw (target S3)` stack.
pub struct KafkaNativeToVersityGWStack {
    _source: ContainerAsync<GenericImage>,
    _target: ContainerAsync<GenericImage>,
    _data_dir: tempfile::TempDir,
    source_port: u16,
    s3_port: u16,
    pub bucket: String,
}

impl KafkaNativeToVersityGWStack {
    pub async fn start(bucket: &str) -> Result<Self> {
        let source_port = portpicker::pick_unused_port().context("no free port for source")?;
        let s3_port = portpicker::pick_unused_port().context("no free port for s3")?;
        let source = start_kafka_native(source_port).await?;
        // Pre-create the bucket directory on the host and bind-mount
        // it as VersityGW's data dir. The POSIX backend treats every
        // top-level directory under `/data` as a bucket.
        let data_dir = tempfile::tempdir().context("data dir")?;
        std::fs::create_dir(data_dir.path().join(bucket)).context("bucket dir")?;
        let target = start_versitygw(s3_port, data_dir.path()).await?;
        wait_for_metadata(&format!("localhost:{source_port}")).await?;
        wait_for_s3_listing(s3_port, bucket).await?;
        Ok(Self {
            _source: source,
            _target: target,
            _data_dir: data_dir,
            source_port,
            s3_port,
            bucket: bucket.to_string(),
        })
    }

    pub fn s3_endpoint(&self) -> String {
        format!("http://localhost:{}", self.s3_port)
    }
}

#[async_trait]
impl ProvisionedStack for KafkaNativeToVersityGWStack {
    fn source_bootstrap(&self) -> String {
        format!("localhost:{}", self.source_port)
    }
    fn target_s3_endpoint(&self) -> Option<String> {
        Some(self.s3_endpoint())
    }
}

async fn start_versitygw(
    host_port: u16,
    data_dir: &std::path::Path,
) -> Result<ContainerAsync<GenericImage>> {
    use testcontainers::core::Mount;

    let host_path = data_dir
        .to_str()
        .context("data dir path is not valid utf-8")?
        .to_string();
    GenericImage::new(VERSITYGW_IMAGE, VERSITYGW_TAG)
        .with_exposed_port(ContainerPort::Tcp(7070))
        .with_wait_for(WaitFor::message_on_stdout("Admin/S3 service listening on"))
        .with_mapped_port(host_port, ContainerPort::Tcp(7070))
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_env_var("ROOT_ACCESS_KEY_ID", VERSITYGW_ACCESS_KEY)
        .with_env_var("ROOT_SECRET_ACCESS_KEY", VERSITYGW_SECRET_KEY)
        // The POSIX backend stores object metadata in xattrs by
        // default. macOS bind mounts don't support xattrs; --nometa
        // disables xattr-backed metadata entirely. This is a
        // dev-loop concession; production POSIX mounts (Linux
        // ext4/xfs) keep the default.
        .with_cmd([
            "posix".to_string(),
            "--nometa".to_string(),
            "/data".to_string(),
        ])
        .start()
        .await
        .context("starting versitygw container")
}

async fn wait_for_s3_listing(host_port: u16, bucket: &str) -> Result<()> {
    use object_store::aws::AmazonS3Builder;
    use object_store::ObjectStore;

    let endpoint = format!("http://localhost:{host_port}");
    let bucket = bucket.to_string();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let store_result = AmazonS3Builder::new()
            .with_endpoint(&endpoint)
            .with_allow_http(true)
            .with_region("us-east-1")
            .with_bucket_name(&bucket)
            .with_access_key_id(VERSITYGW_ACCESS_KEY)
            .with_secret_access_key(VERSITYGW_SECRET_KEY)
            .build();
        if let Ok(store) = store_result {
            use futures::StreamExt;
            let mut stream = store.list(None);
            // We only need ONE successful poll on the stream to know
            // the server is responding.
            match stream.next().await {
                Some(Ok(_)) | None => return Ok(()),
                Some(Err(_)) => {}
            }
        }
        if std::time::Instant::now() > deadline {
            anyhow::bail!("versitygw never became ready");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
