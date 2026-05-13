//! Run a single mirror in-process for a test, returning a handle that
//! cancels on drop.

use std::time::Duration;

use anyhow::{Context, Result};
use mirror_core::{run_mirror, MirrorError};
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
