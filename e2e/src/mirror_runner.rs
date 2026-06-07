//! Run a single mirror in-process for a test, returning a handle that
//! cancels on drop.

use std::time::Duration;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use mirror_core::{run_mirror, run_mirror_with_notifier, MirrorError, NoOpNotifier, Sink, TeeSink};
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
    /// `Ok(())` only if the mirror exits gracefully; a non-cancelled
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
    pub keys: mirror_envelope::ColumnType,
    pub values: mirror_envelope::ColumnType,
    pub compaction: Option<mirror_fs::CompactionMode>,
    pub cache: Option<mirror_fs::CacheBinding>,
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
            keys: mirror_envelope::ColumnType::Utf8,
            values: mirror_envelope::ColumnType::Utf8,
            compaction: None,
            cache: None,
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
    let dest_name = spec.destination_name.clone();
    let cache_for_bootstrap = spec.cache.clone();
    let cache_for_tee = spec.cache.clone();
    let sink_cfg = FilesystemSinkConfig {
        root: spec.root,
        destination_name: spec.destination_name,
        partition: spec.partition as u32,
        format: spec.format,
        compression: spec.compression,
        keys: spec.keys,
        values: spec.values,
        compaction: spec.compaction,
        cache: cache_for_bootstrap,
        flush: spec.flush,
    };
    let sink = FilesystemSink::open(sink_cfg).context("open FilesystemSink")?;
    let (shutdown, signal) = shutdown_pair();
    let handle = tokio::spawn(async move {
        // Even the single-destination path routes through a length-1
        // TeeSink so the per-record cache-binding apply is owned at
        // the tee level (matches mirror-bin's production path).
        let tee = TeeSink::open(
            vec![(dest_name, Box::new(sink) as Box<dyn mirror_core::Sink>)],
            cache_for_tee,
        )
        .await
        .map_err(MirrorError::Sink)?;
        run_mirror(source, tee, signal).await
    });
    Ok(MirrorHandle { handle, shutdown })
}

/// Inner-sink description for [`spawn_kafka_to_tee`]. Mirrors the
/// production-side per-destination configuration in `mirror-bin`.
pub enum TeeInnerSpec {
    Filesystem(FsInnerSpec),
    S3(S3InnerSpec),
    Kafka(KafkaInnerSpec),
}

pub struct FsInnerSpec {
    pub name: String,
    pub root: PathBuf,
    pub format: mirror_envelope::Format,
    pub compression: mirror_envelope::ParquetCompression,
    pub keys: mirror_envelope::ColumnType,
    pub values: mirror_envelope::ColumnType,
    pub compaction: Option<mirror_fs::CompactionMode>,
    pub flush: mirror_fs::FlushTriggers,
}

pub struct S3InnerSpec {
    pub name: String,
    pub store: Arc<dyn ObjectStore>,
    pub prefix: Option<object_store::path::Path>,
    pub format: mirror_envelope::Format,
    pub compression: mirror_envelope::ParquetCompression,
    pub keys: mirror_envelope::ColumnType,
    pub values: mirror_envelope::ColumnType,
    pub compaction: Option<mirror_s3::CompactionMode>,
    pub flush: mirror_s3::FlushTriggers,
}

pub struct KafkaInnerSpec {
    pub name: String,
    pub bootstrap_servers: String,
    pub topic: String,
}

pub struct TeeMirrorSpec {
    pub source_bootstrap: String,
    pub source_topic: String,
    pub partition: i32,
    pub group_id: String,
    pub destinations: Vec<TeeInnerSpec>,
    pub cache: Option<mirror_core::CacheBinding>,
}

pub async fn spawn_kafka_to_tee(spec: TeeMirrorSpec) -> Result<MirrorHandle> {
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

    let cache_for_bootstrap = spec.cache.clone();
    let mut inners: Vec<(String, Box<dyn mirror_core::Sink>)> =
        Vec::with_capacity(spec.destinations.len());
    for inner in spec.destinations {
        match inner {
            TeeInnerSpec::Filesystem(fs) => {
                let cfg = FilesystemSinkConfig {
                    root: fs.root,
                    destination_name: fs.name.clone(),
                    partition: spec.partition as u32,
                    format: fs.format,
                    compression: fs.compression,
                    keys: fs.keys,
                    values: fs.values,
                    compaction: fs.compaction,
                    cache: cache_for_bootstrap.clone(),
                    flush: fs.flush,
                };
                let sink = FilesystemSink::open(cfg).context("open FilesystemSink")?;
                inners.push((fs.name, Box::new(sink)));
            }
            TeeInnerSpec::S3(s3) => {
                let cfg = S3SinkConfig {
                    store: s3.store,
                    prefix: s3.prefix,
                    destination_name: s3.name.clone(),
                    partition: spec.partition as u32,
                    format: s3.format,
                    compression: s3.compression,
                    keys: s3.keys,
                    values: s3.values,
                    compaction: s3.compaction,
                    cache: cache_for_bootstrap.clone(),
                    flush: s3.flush,
                };
                let sink = S3Sink::open(cfg).await.context("open S3Sink")?;
                inners.push((s3.name, Box::new(sink)));
            }
            TeeInnerSpec::Kafka(k) => {
                let cfg = KafkaSinkConfig::new(k.bootstrap_servers, k.topic, spec.partition);
                let sink = KafkaSink::open(cfg).context("open KafkaSink")?;
                inners.push((k.name, Box::new(sink)));
            }
        }
    }
    let tee = TeeSink::open(inners, spec.cache.clone())
        .await
        .map_err(|e| anyhow::anyhow!("open TeeSink: {e}"))?;
    let (shutdown, signal) = shutdown_pair();
    let handle = tokio::spawn(async move { run_mirror(source, tee, signal).await });
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
    pub keys: mirror_envelope::ColumnType,
    pub values: mirror_envelope::ColumnType,
    pub compaction: Option<mirror_s3::CompactionMode>,
    pub cache: Option<mirror_s3::CacheBinding>,
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
    let dest_name = spec.destination_name.clone();
    let cache_for_bootstrap = spec.cache.clone();
    let cache_for_tee = spec.cache.clone();
    let sink_cfg = S3SinkConfig {
        store: spec.store,
        prefix: spec.prefix,
        destination_name: spec.destination_name,
        partition: spec.partition as u32,
        format: spec.format,
        compression: spec.compression,
        keys: spec.keys,
        values: spec.values,
        compaction: spec.compaction,
        cache: cache_for_bootstrap,
        flush: spec.flush,
    };
    let sink = S3Sink::open(sink_cfg).await.context("open S3Sink")?;
    let (shutdown, signal) = shutdown_pair();
    let handle = tokio::spawn(async move {
        let tee = TeeSink::open(
            vec![(dest_name, Box::new(sink) as Box<dyn mirror_core::Sink>)],
            cache_for_tee,
        )
        .await
        .map_err(MirrorError::Sink)?;
        run_mirror(source, tee, signal).await
    });
    Ok(MirrorHandle { handle, shutdown })
}

/// Spawn a kafka → filesystem mirror with a `notify` block attached.
/// Mirrors `mirror-bin`'s `spawn_mirror` wiring: source-consume
/// builds a `KkvV1Notifier`; destination-flush builds a
/// `FlushDispatcher` and attaches it to the TeeSink as a flush
/// observer.
pub async fn spawn_kafka_to_fs_with_notify(
    spec: FsMirrorSpec,
    notify: mirror_config::Notify,
) -> Result<MirrorHandle> {
    let src_cfg = {
        let mut c = KafkaSourceConfig::new(
            spec.source_bootstrap,
            spec.group_id,
            spec.source_topic.clone(),
            spec.partition,
        );
        c.poll_timeout = Duration::from_millis(500);
        c
    };
    let source = KafkaSource::open(src_cfg).context("open KafkaSource")?;
    let dest_name = spec.destination_name.clone();
    let topic = spec.source_topic.clone();
    let partition = spec.partition;
    let mirror_name = dest_name.clone();
    // `KkvV1Notifier::from_config` and `FlushDispatcher::from_config`
    // need a `CacheState` so the per-mirror suppression gate can read
    // `is_mirror_ready`. If the caller didn't pass one we build a
    // fresh state and register this mirror at `bootstrap_hwm = 0` so
    // the slot is immediately ready — the test scenarios that opt
    // out of cache binding don't care about suppression timing.
    let (cache_state, cache_for_tee) = match spec.cache.clone() {
        Some(binding) => (Arc::clone(&binding.state), Some(binding)),
        None => {
            let state = Arc::new(mirror_core::CacheState::new());
            state.register_mirror(&mirror_name, 0, false);
            (state, None)
        }
    };
    let cache_for_bootstrap = spec.cache.clone();
    let sink_cfg = FilesystemSinkConfig {
        root: spec.root,
        destination_name: spec.destination_name,
        partition: spec.partition as u32,
        format: spec.format,
        compression: spec.compression,
        keys: spec.keys,
        values: spec.values,
        compaction: spec.compaction,
        cache: cache_for_bootstrap,
        flush: spec.flush,
    };
    let sink = FilesystemSink::open(sink_cfg).context("open FilesystemSink")?;
    let trigger_mode = notify.trigger.on;
    let (shutdown, signal) = shutdown_pair();
    let handle = tokio::spawn(async move {
        let mut tee = TeeSink::open(
            vec![(dest_name, Box::new(sink) as Box<dyn Sink>)],
            cache_for_tee,
        )
        .await
        .map_err(MirrorError::Sink)?;

        match trigger_mode {
            mirror_config::TriggerOn::SourceConsume => {
                let notifier = mirror_notify_kkv::KkvV1Notifier::from_config(
                    &notify,
                    topic,
                    partition,
                    cache_state,
                    mirror_name,
                )
                .map_err(|e| MirrorError::Sink(mirror_core::SinkError::Transport(e.to_string())))?;
                run_mirror_with_notifier(
                    source,
                    tee,
                    notifier,
                    signal,
                    mirror_core::DEFAULT_HEARTBEAT_INTERVAL,
                )
                .await
            }
            mirror_config::TriggerOn::DestinationFlush => {
                let dispatcher = mirror_notify_kkv::FlushDispatcher::from_config(
                    &notify,
                    topic,
                    partition,
                    cache_state,
                    mirror_name,
                )
                .map_err(|e| MirrorError::Sink(mirror_core::SinkError::Transport(e.to_string())))?;
                tee.set_flush_observer(std::sync::Arc::new(dispatcher));
                run_mirror_with_notifier(
                    source,
                    tee,
                    NoOpNotifier,
                    signal,
                    mirror_core::DEFAULT_HEARTBEAT_INTERVAL,
                )
                .await
            }
        }
    });
    Ok(MirrorHandle { handle, shutdown })
}
