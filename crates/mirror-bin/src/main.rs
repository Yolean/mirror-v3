use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use mirror_config::{Destination, Mirror};
use mirror_core::{run_mirror, MetricLabels, MIRROR_LABELS};
use mirror_fs::{FilesystemSink, FilesystemSinkConfig};
use mirror_kafka::{KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig};
use mirror_s3::{S3Sink, S3SinkConfig};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "mirror-v3",
    version,
    about = "Exactly-once Kafka topic+partition mirror"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Parse a config file and exit non-zero on any error.
    Validate {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Run the configured mirrors. Exits non-zero on any failure.
    Run {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// One-shot health check: per mirror, print the source high
    /// watermark, the destination's next-expected-offset, and the
    /// lag (source high - destination next). Exits non-zero if any
    /// mirror failed to query.
    Status {
        #[arg(short, long)]
        config: PathBuf,
        /// Output format. `table` is the default kubectl-friendly
        /// aligned text; `json` is machine-readable.
        #[arg(long, default_value = "table")]
        format: StatusFormat,
    },
}

#[derive(Copy, Clone, clap::ValueEnum)]
enum StatusFormat {
    Table,
    Json,
}

fn main() -> ExitCode {
    init_tracing();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Validate { config } => match run_validate(config) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("error: {err:?}");
                ExitCode::from(1)
            }
        },
        Cmd::Status { config, format } => {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(err) => {
                    eprintln!("error: tokio init: {err}");
                    return ExitCode::from(1);
                }
            };
            match rt.block_on(run_status(config, format)) {
                Ok(any_errors) => {
                    if any_errors {
                        ExitCode::from(1)
                    } else {
                        ExitCode::SUCCESS
                    }
                }
                Err(err) => {
                    eprintln!("error: {err:?}");
                    ExitCode::from(1)
                }
            }
        }
        Cmd::Run { config } => {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(err) => {
                    eprintln!("error: tokio init: {err}");
                    return ExitCode::from(1);
                }
            };
            match rt.block_on(run(config)) {
                Ok(()) => ExitCode::SUCCESS,
                Err(err) => {
                    tracing::error!(error = %format!("{err:?}"), "mirror exited with error");
                    ExitCode::from(1)
                }
            }
        }
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // tracing_subscriber::fmt() defaults to stdout. Force stderr so
    // stdout stays available for structured output (e.g. `status
    // --format json`) and standard `1>` / `2>` redirects do the
    // expected thing.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_writer(std::io::stderr)
        .try_init();
}

fn run_validate(path: PathBuf) -> Result<()> {
    let cfg = mirror_config::load_from_path(&path)
        .with_context(|| format!("loading {}", path.display()))?;
    let total_destinations: usize = cfg.mirrors.iter().map(|m| m.destinations.len()).sum();
    println!(
        "OK: {} mirror(s), {total_destinations} destination(s) total",
        cfg.mirrors.len()
    );
    for m in &cfg.mirrors {
        let kinds: Vec<&str> = m.destinations.iter().map(|d| destination_type(d)).collect();
        println!(
            "  mirror {:?}: destinations = [{}]",
            m.name,
            kinds.join(", ")
        );
    }
    Ok(())
}

fn destination_type(d: &Destination) -> &'static str {
    match d {
        Destination::Kafka(_) => "kafka",
        Destination::Filesystem(_) => "filesystem",
        Destination::S3(_) => "s3",
    }
}

fn format_to_envelope(f: mirror_config::DestinationFormat) -> mirror_envelope::Format {
    match f {
        mirror_config::DestinationFormat::Parquet => mirror_envelope::Format::Parquet,
        mirror_config::DestinationFormat::Ndjson => mirror_envelope::Format::Ndjson,
    }
}

fn compression_to_envelope(
    c: mirror_config::ParquetCompression,
) -> mirror_envelope::ParquetCompression {
    use mirror_config::ParquetCompression as Cfg;
    use mirror_envelope::ParquetCompression as E;
    match c {
        Cfg::Zstd1 => E::Zstd1,
        Cfg::Zstd3 => E::Zstd3,
        Cfg::Snappy => E::Snappy,
        Cfg::Lz4 => E::Lz4,
        Cfg::Uncompressed => E::Uncompressed,
    }
}

fn column_type_to_envelope(k: mirror_config::ColumnType) -> mirror_envelope::ColumnType {
    match k {
        mirror_config::ColumnType::Bytes => mirror_envelope::ColumnType::Bytes,
        mirror_config::ColumnType::Utf8 => mirror_envelope::ColumnType::Utf8,
        mirror_config::ColumnType::Json => mirror_envelope::ColumnType::Json,
        mirror_config::ColumnType::JsonParseable => mirror_envelope::ColumnType::JsonParseable,
    }
}

fn compaction_to_fs(c: Option<mirror_config::Compaction>) -> Option<mirror_fs::CompactionMode> {
    c.map(|mirror_config::Compaction::Log| mirror_fs::CompactionMode::Log)
}

fn compaction_to_s3(c: Option<mirror_config::Compaction>) -> Option<mirror_s3::CompactionMode> {
    c.map(|mirror_config::Compaction::Log| mirror_s3::CompactionMode::Log)
}

/// Human label for the mirror's compaction mode, used in logs so an
/// operator can tell from `kubectl logs` which mode a given mirror
/// is running in.
fn compaction_label(c: Option<mirror_config::Compaction>) -> &'static str {
    match c {
        None => "append",
        Some(mirror_config::Compaction::Log) => "log",
    }
}

fn timestamp_mode_to_kafka(m: mirror_config::TimestampMode) -> mirror_kafka::TimestampMode {
    match m {
        mirror_config::TimestampMode::Source => mirror_kafka::TimestampMode::Source,
        mirror_config::TimestampMode::Destination => mirror_kafka::TimestampMode::Destination,
    }
}

/// Bundle of per-mirror encoding/flush values resolved against
/// defaults. Pulled out so the FS and S3 sink-config builders are
/// trivial.
struct BlobMirrorParams {
    format: mirror_envelope::Format,
    compression: mirror_envelope::ParquetCompression,
    keys: mirror_envelope::ColumnType,
    values: mirror_envelope::ColumnType,
    flush: mirror_fs::FlushTriggers,
}

fn resolve_blob_params(mirror: &Mirror) -> Result<BlobMirrorParams> {
    let format = format_to_envelope(mirror.format.unwrap_or_default());
    let compression = compression_to_envelope(mirror.compression.unwrap_or_default());
    let keys = column_type_to_envelope(mirror.keys.unwrap_or_default().kind);
    let values = column_type_to_envelope(mirror.values.unwrap_or_default().kind);
    let cfg_flush = mirror
        .flush
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("mirror {:?}: missing `flush`", mirror.name))?;
    let flush = mirror_fs::FlushTriggers {
        max_time: std::time::Duration::from_millis(cfg_flush.max_time_ms),
        max_bytes: cfg_flush.max_bytes,
        max_offsets: cfg_flush.max_offsets,
        daily_at_utc_seconds: cfg_flush
            .daily
            .as_ref()
            .map(|d| d.parse_at_utc())
            .transpose()
            .with_context(|| format!("mirror {:?}: daily.at-utc", mirror.name))?,
    };
    Ok(BlobMirrorParams {
        format,
        compression,
        keys,
        values,
        flush,
    })
}

#[derive(Debug, serde::Serialize)]
struct StatusRow {
    name: String,
    source_high: Option<i64>,
    dest_next: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl StatusRow {
    fn lag(&self) -> Option<i64> {
        match (self.source_high, self.dest_next) {
            (Some(h), Some(n)) => Some(h.saturating_sub(n as i64).max(0)),
            _ => None,
        }
    }
}

async fn run_status(path: PathBuf, format: StatusFormat) -> Result<bool> {
    let cfg = mirror_config::load_from_path(&path)
        .with_context(|| format!("loading {}", path.display()))?;
    let mut rows = Vec::new();
    for mirror in &cfg.mirrors {
        for dest in &mirror.destinations {
            rows.push(compute_status_row(mirror, dest).await);
        }
    }
    let any_errors = rows.iter().any(|r| r.error.is_some());
    match format {
        StatusFormat::Table => print_status_table(&rows),
        StatusFormat::Json => println!("{}", serde_json::to_string_pretty(&rows)?),
    }
    Ok(any_errors)
}

async fn compute_status_row(mirror: &Mirror, destination: &Destination) -> StatusRow {
    let dest_name = destination.effective_name(&mirror.name);
    let row_name = if dest_name == mirror.name {
        mirror.name.clone()
    } else {
        format!("{}.{dest_name}", mirror.name)
    };
    let mut row = StatusRow {
        name: row_name,
        source_high: None,
        dest_next: None,
        error: None,
    };
    let bootstrap = mirror.source.bootstrap_servers.clone();
    let topic = mirror.topic.clone();
    let partition = mirror.partition as i32;
    let source_result = tokio::task::spawn_blocking(move || {
        mirror_kafka::fetch_high_watermark(
            &bootstrap,
            &topic,
            partition,
            std::time::Duration::from_secs(5),
        )
    })
    .await;
    match source_result {
        Ok(Ok(high)) => row.source_high = Some(high),
        Ok(Err(e)) => {
            row.error = Some(format!("source watermark: {e}"));
            return row;
        }
        Err(e) => {
            row.error = Some(format!("source watermark task: {e}"));
            return row;
        }
    }
    match query_destination_next(mirror, destination).await {
        Ok(next) => row.dest_next = Some(next),
        Err(e) => row.error = Some(format!("destination: {e}")),
    }
    row
}

async fn query_destination_next(mirror: &Mirror, destination: &Destination) -> Result<u64> {
    use mirror_core::Sink;
    let dest_name = destination.effective_name(&mirror.name);
    match destination {
        Destination::Kafka(k) => {
            let topic = k.topic.clone().unwrap_or_else(|| mirror.topic.clone());
            let cfg =
                KafkaSinkConfig::new(k.bootstrap_servers.clone(), topic, mirror.partition as i32);
            let mut sink = KafkaSink::open(cfg).map_err(|e| anyhow::anyhow!("{e}"))?;
            sink.next_expected_offset()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))
        }
        Destination::Filesystem(fs) => {
            let params = resolve_blob_params(mirror)?;
            let cfg = FilesystemSinkConfig {
                root: fs.root.clone(),
                destination_name: dest_name,
                partition: mirror.partition,
                format: params.format,
                compression: params.compression,
                keys: params.keys,
                values: params.values,
                compaction: compaction_to_fs(mirror.compaction),
                cache: None,
                flush: params.flush,
            };
            let mut sink = FilesystemSink::open(cfg).map_err(|e| anyhow::anyhow!("{e}"))?;
            sink.next_expected_offset()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))
        }
        Destination::S3(s3) => {
            let params = resolve_blob_params(mirror)?;
            let mut builder = object_store::aws::AmazonS3Builder::from_env()
                .with_region(&s3.region)
                .with_bucket_name(&s3.bucket);
            if let Some(endpoint) = &s3.endpoint {
                builder = builder.with_endpoint(endpoint);
                if endpoint.starts_with("http://") {
                    builder = builder.with_allow_http(true);
                }
            }
            let store = builder.build().context("building S3 store")?;
            let cfg = S3SinkConfig {
                store: Arc::new(store),
                prefix: s3.prefix.as_deref().map(object_store::path::Path::from),
                destination_name: dest_name,
                partition: mirror.partition,
                format: params.format,
                compression: params.compression,
                keys: params.keys,
                values: params.values,
                compaction: compaction_to_s3(mirror.compaction),
                cache: None,
                flush: mirror_s3::FlushTriggers {
                    max_time: params.flush.max_time,
                    max_bytes: params.flush.max_bytes,
                    max_offsets: params.flush.max_offsets,
                    daily_at_utc_seconds: params.flush.daily_at_utc_seconds,
                },
            };
            let mut sink = S3Sink::open(cfg)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            sink.next_expected_offset()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))
        }
    }
}

fn print_status_table(rows: &[StatusRow]) {
    let name_width = rows.iter().map(|r| r.name.len()).max().unwrap_or(6).max(6);
    println!(
        "{:<width$}  {:>14}  {:>14}  {:>10}",
        "MIRROR",
        "SOURCE-HIGH",
        "DEST-NEXT",
        "LAG",
        width = name_width
    );
    for r in rows {
        if let Some(e) = &r.error {
            println!("{:<width$}  error: {}", r.name, e, width = name_width);
            continue;
        }
        println!(
            "{:<width$}  {:>14}  {:>14}  {:>10}",
            r.name,
            r.source_high.map(|v| v.to_string()).unwrap_or_default(),
            r.dest_next.map(|v| v.to_string()).unwrap_or_default(),
            r.lag().map(|v| v.to_string()).unwrap_or_default(),
            width = name_width
        );
    }
}

async fn run(path: PathBuf) -> Result<()> {
    let cfg = mirror_config::load_from_path(&path)
        .with_context(|| format!("loading {}", path.display()))?;
    let total_destinations: usize = cfg.mirrors.iter().map(|m| m.destinations.len()).sum();
    tracing::info!(
        config = %path.display(),
        mirrors = cfg.mirrors.len(),
        destinations = total_destinations,
        "starting mirror-v3"
    );
    install_metrics_exporter();

    // One shutdown channel, cloned per mirror. Listening for Ctrl-C
    // here means SIGINT triggers graceful flush; in containers,
    // SIGTERM will arrive on the same path because tokio's
    // ctrl_c handler is the platform's INT handler — for full SIGTERM
    // support a unix-signals branch can be added next.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let signal_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("received SIGINT; requesting graceful shutdown");
            let _ = signal_tx.send(true);
        }
    });
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
            let term_tx = shutdown_tx.clone();
            tokio::spawn(async move {
                if sigterm.recv().await.is_some() {
                    tracing::info!("received SIGTERM; requesting graceful shutdown");
                    let _ = term_tx.send(true);
                }
            });
        }
    }

    // Build a shared CacheState if any mirror opted into http-access.
    // Capture each opt-in mirror's source-partition high-watermark *now*
    // so the readiness gate flips only after we've consumed past
    // whatever was already there at startup. (KKV semantics — dependents
    // must not see a partially-rebuilt cache after a reload.)
    let cache_state = if cfg.mirrors.iter().any(|m| m.http_access.is_some()) {
        let state = std::sync::Arc::new(mirror_core::CacheState::new());
        for m in &cfg.mirrors {
            if m.http_access.is_some() {
                let hwm = fetch_hwm_for_mirror(m).await?;
                tracing::info!(
                    mirror = %m.name,
                    topic = %m.topic,
                    partition = m.partition,
                    bootstrap_hwm = hwm,
                    "registering mirror with cache readiness gate"
                );
                state.register_mirror(&m.name, hwm);
            }
        }
        Some(state)
    } else {
        None
    };

    // Spawn the cache HTTP server if any mirror has opt-in. Server
    // runs until shutdown_rx flips OR /_admin/v1/shutdown is hit.
    if let Some(state) = cache_state.as_ref() {
        let addr = cache_listen_addr();
        let state = std::sync::Arc::clone(state);
        let cache_shutdown_rx = shutdown_rx.clone();
        let cache_shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            let signal = shutdown_signal(cache_shutdown_rx);
            match mirror_cache::serve(addr, state, signal).await {
                Ok(_code) => {
                    // Admin shutdown signalled. Propagate to the
                    // mirror loops so the whole process exits.
                    let _ = cache_shutdown_tx.send(true);
                }
                Err(e) => {
                    tracing::error!(error = %e, "cache HTTP server failed");
                    let _ = cache_shutdown_tx.send(true);
                }
            }
        });
    }

    let mut handles = Vec::with_capacity(cfg.mirrors.len());
    for mirror in &cfg.mirrors {
        let binding = mirror_cache_binding(mirror, cache_state.as_ref());
        let handle = spawn_mirror(mirror.clone(), shutdown_rx.clone(), binding).await?;
        handles.push((mirror.name.clone(), handle));
    }

    // Wait for the first task to terminate. Any termination collapses
    // the whole process. Successful (graceful) termination is Ok(())
    // so the process exits zero on shutdown.
    let (which, result) = wait_first(handles).await;
    if result.is_ok() {
        tracing::info!(mirror = %which, "mirror task terminated gracefully");
    } else {
        tracing::error!(mirror = %which, "mirror task errored; exiting process");
    }
    result
}

/// Pick the listen address for the cache HTTP server. Defaults to
/// 0.0.0.0:8080, overridable via `MIRROR_V3_CACHE_PORT`.
fn cache_listen_addr() -> std::net::SocketAddr {
    let port = std::env::var("MIRROR_V3_CACHE_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(8080);
    std::net::SocketAddr::from(([0, 0, 0, 0], port))
}

/// Materialise a `CacheBinding` for the given mirror if it has
/// `http-access` set and the supervisor built a shared CacheState.
fn mirror_cache_binding(
    mirror: &Mirror,
    cache: Option<&std::sync::Arc<mirror_core::CacheState>>,
) -> Option<mirror_core::CacheBinding> {
    match (mirror.http_access.as_ref(), cache) {
        (Some(_), Some(state)) => Some(mirror_core::CacheBinding {
            state: std::sync::Arc::clone(state),
            mirror_name: mirror.name.clone(),
        }),
        _ => None,
    }
}

/// Per-mirror bootstrap watermark. Run in a `spawn_blocking` task
/// because `mirror_kafka::fetch_high_watermark` uses the synchronous
/// `BaseConsumer` API under the hood.
async fn fetch_hwm_for_mirror(mirror: &Mirror) -> Result<u64> {
    let bootstrap = mirror.source.bootstrap_servers.clone();
    let topic = mirror.topic.clone();
    let partition = mirror.partition as i32;
    let mirror_name = mirror.name.clone();
    let hwm = tokio::task::spawn_blocking(move || {
        mirror_kafka::fetch_high_watermark(
            &bootstrap,
            &topic,
            partition,
            std::time::Duration::from_secs(10),
        )
    })
    .await
    .with_context(|| format!("mirror {mirror_name}: hwm task join"))?
    .with_context(|| format!("mirror {mirror_name}: fetch high watermark"))?;
    Ok(hwm.max(0) as u64)
}

async fn shutdown_signal(mut rx: tokio::sync::watch::Receiver<bool>) {
    if *rx.borrow() {
        return;
    }
    let _ = rx.changed().await;
}

/// Install the Prometheus exporter on `0.0.0.0:<port>`. Port defaults
/// to 9090; override with `MIRROR_V3_METRICS_PORT` (set to `0` to
/// disable). A failure to bind logs at warn level and is non-fatal —
/// the operator's observability story degrades, but the mirror keeps
/// running.
fn install_metrics_exporter() {
    let port = std::env::var("MIRROR_V3_METRICS_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(9090);
    if port == 0 {
        tracing::info!("metrics exporter disabled (MIRROR_V3_METRICS_PORT=0)");
        return;
    }
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    match metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
    {
        Ok(()) => tracing::info!(%addr, "metrics exporter listening on /metrics"),
        Err(e) => tracing::warn!(error = %e, %addr, "metrics exporter failed; continuing"),
    }
}

async fn spawn_mirror(
    mirror: Mirror,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    cache: Option<mirror_core::CacheBinding>,
) -> Result<tokio::task::JoinHandle<Result<()>>> {
    let source_cfg = KafkaSourceConfig::new(
        mirror.source.bootstrap_servers.clone(),
        mirror
            .source
            .group_id
            .clone()
            .unwrap_or_else(|| format!("mirror-v3-{}", mirror.name)),
        mirror.topic.clone(),
        mirror.partition as i32,
    );
    let source = KafkaSource::open(source_cfg)
        .with_context(|| format!("opening source for mirror {}", mirror.name))?;

    let name = mirror.name.clone();
    let labels = MetricLabels {
        topic: mirror.topic.clone(),
        partition: mirror.partition,
    };
    let compaction = compaction_label(mirror.compaction);

    // Build one inner Sink per destination, then wrap them in a tee.
    // The single-destination case routes through a length-1 tee too —
    // this keeps the cache binding's per-record fanout on a single
    // code path.
    let mut inners: Vec<(String, Box<dyn mirror_core::Sink>)> =
        Vec::with_capacity(mirror.destinations.len());
    let mut dest_descriptions: Vec<String> = Vec::with_capacity(mirror.destinations.len());
    for dest in &mirror.destinations {
        let inner_name = dest.effective_name(&mirror.name);
        let kind = destination_type(dest);
        dest_descriptions.push(format!("{inner_name}({kind})"));
        let sink: Box<dyn mirror_core::Sink> =
            open_inner_sink(dest, &mirror, &inner_name, cache.as_ref()).await?;
        inners.push((inner_name, sink));
    }
    let tee = mirror_core::TeeSink::open(inners, cache.clone())
        .await
        .map_err(|e| anyhow::anyhow!("opening tee for mirror {name}: {e}"))?;

    let destinations_log = dest_descriptions.join(",");
    Ok(tokio::spawn(async move {
        tracing::info!(
            mirror = %name,
            destinations = %destinations_log,
            compaction,
            "loop start"
        );
        let result = MIRROR_LABELS
            .scope(
                labels,
                run_mirror(source, tee, shutdown_signal(shutdown_rx)),
            )
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("mirror {name}: {e}")),
        }
    }))
}

async fn open_inner_sink(
    dest: &Destination,
    mirror: &Mirror,
    inner_name: &str,
    cache_for_bootstrap: Option<&mirror_core::CacheBinding>,
) -> Result<Box<dyn mirror_core::Sink>> {
    match dest {
        Destination::Kafka(k) => {
            let topic = k.topic.clone().unwrap_or_else(|| mirror.topic.clone());
            let mut sink_cfg =
                KafkaSinkConfig::new(k.bootstrap_servers.clone(), topic, mirror.partition as i32);
            sink_cfg.timestamp_mode =
                timestamp_mode_to_kafka(mirror.timestamp_mode.unwrap_or_default());
            sink_cfg.keys = column_type_to_envelope(mirror.keys.unwrap_or_default().kind);
            sink_cfg.values = column_type_to_envelope(mirror.values.unwrap_or_default().kind);
            let sink = KafkaSink::open(sink_cfg).with_context(|| {
                format!(
                    "opening kafka sink for mirror {} destination {inner_name}",
                    mirror.name
                )
            })?;
            Ok(Box::new(sink))
        }
        Destination::Filesystem(fs) => {
            let params = resolve_blob_params(mirror)?;
            // Cache bootstrap-replay happens at sink-open time in
            // each blob sink. The tee's `cache` binding is what
            // matters for the per-record path; passing the binding
            // to every inner blob sink seeds the cache from durable
            // state on restart. CacheState is monotonic so multiple
            // inner sinks bootstrapping the same binding is safe.
            let sink_cfg = FilesystemSinkConfig {
                root: fs.root.clone(),
                destination_name: inner_name.to_string(),
                partition: mirror.partition,
                format: params.format,
                compression: params.compression,
                keys: params.keys,
                values: params.values,
                compaction: compaction_to_fs(mirror.compaction),
                cache: cache_for_bootstrap.cloned(),
                flush: params.flush,
            };
            let sink = FilesystemSink::open(sink_cfg).with_context(|| {
                format!(
                    "opening fs sink for mirror {} destination {inner_name}",
                    mirror.name
                )
            })?;
            Ok(Box::new(sink))
        }
        Destination::S3(s3) => {
            let params = resolve_blob_params(mirror)?;
            let mut builder = object_store::aws::AmazonS3Builder::from_env()
                .with_region(&s3.region)
                .with_bucket_name(&s3.bucket);
            if let Some(endpoint) = &s3.endpoint {
                builder = builder.with_endpoint(endpoint);
                if endpoint.starts_with("http://") {
                    builder = builder.with_allow_http(true);
                }
            }
            let store = builder.build().with_context(|| {
                format!(
                    "building S3 store for mirror {} destination {inner_name}",
                    mirror.name
                )
            })?;
            let sink_cfg = S3SinkConfig {
                store: Arc::new(store),
                prefix: s3.prefix.as_deref().map(object_store::path::Path::from),
                destination_name: inner_name.to_string(),
                partition: mirror.partition,
                format: params.format,
                compression: params.compression,
                keys: params.keys,
                values: params.values,
                compaction: compaction_to_s3(mirror.compaction),
                cache: cache_for_bootstrap.cloned(),
                flush: mirror_s3::FlushTriggers {
                    max_time: params.flush.max_time,
                    max_bytes: params.flush.max_bytes,
                    max_offsets: params.flush.max_offsets,
                    daily_at_utc_seconds: params.flush.daily_at_utc_seconds,
                },
            };
            let sink = S3Sink::open(sink_cfg).await.with_context(|| {
                format!(
                    "opening s3 sink for mirror {} destination {inner_name}",
                    mirror.name
                )
            })?;
            Ok(Box::new(sink))
        }
    }
}

async fn wait_first(
    handles: Vec<(String, tokio::task::JoinHandle<Result<()>>)>,
) -> (String, Result<()>) {
    if handles.is_empty() {
        return (
            "(none)".into(),
            Err(anyhow::anyhow!("no mirrors configured")),
        );
    }
    let mut futures = Vec::with_capacity(handles.len());
    for (name, handle) in handles {
        futures.push(Box::pin(async move {
            let r = handle.await;
            (
                name,
                match r {
                    Ok(inner) => inner,
                    Err(join) => Err(anyhow::anyhow!("task join: {join}")),
                },
            )
        }));
    }
    let ((name, result), _idx, _rest) = futures_select_all(futures).await;
    (name, result)
}

/// Tiny stand-in for `futures::future::select_all` to avoid pulling
/// the `futures` crate just for one combinator.
async fn futures_select_all<T, F>(
    mut futures: Vec<std::pin::Pin<Box<F>>>,
) -> (T, usize, Vec<std::pin::Pin<Box<F>>>)
where
    F: std::future::Future<Output = T> + ?Sized,
{
    use std::future::poll_fn;
    use std::task::Poll;
    poll_fn(move |cx| {
        for (i, fut) in futures.iter_mut().enumerate() {
            if let Poll::Ready(v) = fut.as_mut().poll(cx) {
                let rest: Vec<_> = futures
                    .drain(..)
                    .enumerate()
                    .filter(|(j, _)| *j != i)
                    .map(|(_, f)| f)
                    .collect();
                return Poll::Ready((v, i, rest));
            }
        }
        Poll::Pending
    })
    .await
}
