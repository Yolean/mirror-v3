//! Run a single mirror in-process for a test, returning a handle that
//! cancels on drop.

use std::time::Duration;

use std::path::PathBuf;

use anyhow::{Context, Result};
use mirror_core::{run_mirror, MirrorError};
use mirror_fs::{FilesystemSink, FilesystemSinkConfig};
use mirror_kafka::{KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig};

pub struct MirrorHandle {
    handle: tokio::task::JoinHandle<Result<std::convert::Infallible, MirrorError>>,
}

impl MirrorHandle {
    pub fn abort(self) {
        self.handle.abort();
    }
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

    let handle = tokio::spawn(async move { run_mirror(source, sink).await });
    Ok(MirrorHandle { handle })
}

pub struct FsMirrorSpec {
    pub source_bootstrap: String,
    pub source_topic: String,
    pub partition: i32,
    pub group_id: String,
    pub root: PathBuf,
    pub destination_name: String,
    pub flush: mirror_fs::FlushTriggers,
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
        flush: spec.flush,
    };
    let sink = FilesystemSink::open(sink_cfg).context("open FilesystemSink")?;
    let handle = tokio::spawn(async move { run_mirror(source, sink).await });
    Ok(MirrorHandle { handle })
}
