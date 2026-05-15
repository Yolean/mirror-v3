//! Run a single mirror in-process for a test, returning a handle that
//! cancels on drop.

use std::time::Duration;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use mirror_core::{run_mirror, MirrorError};
use mirror_fs::{FilesystemSink, FilesystemSinkConfig};
use mirror_kafka::{KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig};
use mirror_s3::{S3Sink, S3SinkConfig};
use object_store::ObjectStore;

pub struct MirrorHandle {
    handle: tokio::task::JoinHandle<Result<(), MirrorError>>,
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl MirrorHandle {
    /// Hard-cancel the mirror task (no graceful flush).
    pub fn abort(self) {
        self.handle.abort();
    }

    /// Request graceful shutdown (flush sink, return Ok) and wait
    /// for the task to finish. Used by tests that need to assert on
    /// the post-flush state of the destination.
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown.send(true);
        match self.handle.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(anyhow::anyhow!("mirror loop: {e}")),
            Err(e) => Err(anyhow::anyhow!("task join: {e}")),
        }
    }

    /// Await the task without requesting shutdown. Used by adversarial
    /// tests that expect the mirror to terminate on its own because
    /// of an error (e.g. destination drift detection). Returns
    /// `Ok(())` only if the mirror exits gracefully — a non-cancelled
    /// `Err` is propagated and a cancellation is reported.
    pub async fn wait_for_termination(self) -> Result<()> {
        match self.handle.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(anyhow::anyhow!("mirror loop: {e}")),
            Err(e) if e.is_cancelled() => Err(anyhow::anyhow!("task cancelled")),
            Err(e) => Err(anyhow::anyhow!("task join: {e}")),
        }
    }
}

fn shutdown_pair() -> (
    tokio::sync::watch::Sender<bool>,
    impl std::future::Future<Output = ()> + Send + 'static,
) {
    let (tx, mut rx) = tokio::sync::watch::channel(false);
    let fut = async move {
        if *rx.borrow() {
            return;
        }
        let _ = rx.changed().await;
    };
    (tx, fut)
}

pub struct MirrorSpec {
    pub source_bootstrap: String,
    pub target_bootstrap: String,
    pub source_topic: String,
    pub target_topic: String,
    pub partition: i32,
    pub group_id: String,
}

pub fn spawn_kafka_to_kafka(spec: MirrorSpec) -> Result<MirrorHandle> {
    let src_cfg = {
        let mut c = KafkaSourceConfig::new(
            spec.source_bootstrap,
            spec.group_id,
            spec.source_topic,
            spec.partition,
        );
        c.poll_timeout = Duration::from_millis(500);
        c
    };
    let snk_cfg = KafkaSinkConfig::new(spec.target_bootstrap, spec.target_topic, spec.partition);

    let source = KafkaSource::open(src_cfg).context("open KafkaSource")?;
    let sink = KafkaSink::open(snk_cfg).context("open KafkaSink")?;

    let (shutdown, signal) = shutdown_pair();
    let handle = tokio::spawn(async move { run_mirror(source, sink, signal).await });
    Ok(MirrorHandle { handle, shutdown })
}

pub struct FsMirrorSpec {
    pub source_bootstrap: String,
    pub source_topic: String,
    pub partition: i32,
    pub group_id: String,
    pub root: PathBuf,
    pub destination_name: String,
    pub format: mirror_envelope::Format,
    pub compression: mirror_envelope::ParquetCompression,
    pub flush: mirror_fs::FlushTriggers,
}

impl FsMirrorSpec {
    /// Convenience: ndjson with default compression. Mirrors the
    /// shape existing tests want, so they don't have to spell the
    /// envelope fields out every time.
    pub fn ndjson(
        source_bootstrap: String,
        source_topic: String,
        partition: i32,
        group_id: String,
        root: PathBuf,
        destination_name: String,
        flush: mirror_fs::FlushTriggers,
    ) -> Self {
        Self {
            source_bootstrap,
            source_topic,
            partition,
            group_id,
            root,
            destination_name,
            format: mirror_envelope::Format::Ndjson,
            compression: mirror_envelope::ParquetCompression::Zstd1,
            flush,
        }
    }
}

pub fn spawn_kafka_to_filesystem(spec: FsMirrorSpec) -> Result<MirrorHandle> {
    let src_cfg = {
        let mut c = KafkaSourceConfig::new(
            spec.source_bootstrap,
            spec.group_id,
            spec.source_topic,
            spec.partition,
        );
        c.poll_timeout = Duration::from_millis(500);
        c
    };
    let source = KafkaSource::open(src_cfg).context("open KafkaSource")?;
    let sink_cfg = FilesystemSinkConfig {
        root: spec.root,
        destination_name: spec.destination_name,
        partition: spec.partition as u32,
        format: spec.format,
        compression: spec.compression,
        flush: spec.flush,
    };
    let sink = FilesystemSink::open(sink_cfg).context("open FilesystemSink")?;
    let (shutdown, signal) = shutdown_pair();
    let handle = tokio::spawn(async move { run_mirror(source, sink, signal).await });
    Ok(MirrorHandle { handle, shutdown })
}

pub struct S3MirrorSpec {
    pub source_bootstrap: String,
    pub source_topic: String,
    pub partition: i32,
    pub group_id: String,
    pub store: Arc<dyn ObjectStore>,
    pub prefix: Option<object_store::path::Path>,
    pub destination_name: String,
    pub format: mirror_envelope::Format,
    pub compression: mirror_envelope::ParquetCompression,
    pub flush: mirror_s3::FlushTriggers,
}

pub async fn spawn_kafka_to_s3(spec: S3MirrorSpec) -> Result<MirrorHandle> {
    let src_cfg = {
        let mut c = KafkaSourceConfig::new(
            spec.source_bootstrap,
            spec.group_id,
            spec.source_topic,
            spec.partition,
        );
        c.poll_timeout = Duration::from_millis(500);
        c
    };
    let source = KafkaSource::open(src_cfg).context("open KafkaSource")?;
    let sink_cfg = S3SinkConfig {
        store: spec.store,
        prefix: spec.prefix,
        destination_name: spec.destination_name,
        partition: spec.partition as u32,
        format: spec.format,
        compression: spec.compression,
        flush: spec.flush,
    };
    let sink = S3Sink::open(sink_cfg).await.context("open S3Sink")?;
    let (shutdown, signal) = shutdown_pair();
    let handle = tokio::spawn(async move { run_mirror(source, sink, signal).await });
    Ok(MirrorHandle { handle, shutdown })
}
