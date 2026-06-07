use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use mirror_config::{Destination, HttpAccess, Mirror};

mod ack_tracker;
mod readiness_poller;
use ack_tracker::{
    commit_interval_from_env, spawn_periodic_commit_task, AckTracker, DestAckSlot, FlushAckShim,
    WriteAckShim,
};
use mirror_core::{
    heartbeat_interval_from_env, run_mirror_with_notifier, MetricLabels, NoOpNotifier, Record,
    Sink, SinkError, MIRROR_LABELS,
};
use mirror_fs::{FilesystemSink, FilesystemSinkConfig};
use mirror_kafka::{KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig};
use mirror_s3::{S3Sink, S3SinkConfig};
use readiness_poller::{
    readiness_lag_tolerance_from_env, readiness_poll_interval_from_env, spawn_readiness_poller,
    PollSpec,
};
use tracing::Instrument;
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
    let disabled = cfg.mirrors.iter().filter(|m| !m.is_enabled()).count();
    if disabled == 0 {
        println!(
            "OK: {} mirror(s), {total_destinations} destination(s) total",
            cfg.mirrors.len()
        );
    } else {
        println!(
            "OK: {} mirror(s) ({} enabled, {disabled} disabled), {total_destinations} destination(s) total",
            cfg.mirrors.len(),
            cfg.mirrors.len() - disabled,
        );
    }
    for m in &cfg.mirrors {
        let kinds: Vec<&str> = m.destinations.iter().map(|d| destination_type(d)).collect();
        let enabled_tag = if m.is_enabled() { "" } else { " [DISABLED]" };
        println!(
            "  mirror {:?}{enabled_tag}: destinations = [{}]",
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

    // Drop disabled mirrors before anything else so the cache state,
    // readiness gate and spawn loop only see what we'll actually run.
    // Disabled mirrors are validated identically to enabled ones, so
    // flipping `enabled: false` → `true` won't surface latent bugs.
    let total_mirrors = cfg.mirrors.len();
    let enabled_mirrors: Vec<&mirror_config::Mirror> =
        cfg.mirrors.iter().filter(|m| m.is_enabled()).collect();
    for m in &cfg.mirrors {
        if !m.is_enabled() {
            tracing::info!(mirror = %m.name, "mirror disabled via `enabled: false`; not spawning");
        }
    }
    if enabled_mirrors.is_empty() {
        anyhow::bail!(
            "all {} mirror(s) are disabled (enabled: false); nothing to do - \
             enable at least one mirror or scale this deployment to zero replicas",
            total_mirrors
        );
    }

    let total_destinations: usize = enabled_mirrors.iter().map(|m| m.destinations.len()).sum();
    tracing::info!(
        config = %path.display(),
        mirrors_enabled = enabled_mirrors.len(),
        mirrors_total = total_mirrors,
        destinations = total_destinations,
        "starting mirror-v3"
    );
    install_metrics_exporter();

    // One shutdown channel, cloned per mirror. Listening for Ctrl-C
    // here means SIGINT triggers graceful flush; in containers,
    // SIGTERM will arrive on the same path because tokio's
    // ctrl_c handler is the platform's INT handler - for full SIGTERM
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

    // Build a shared CacheState if any *enabled* mirror needs a
    // readiness slot - either to host the per-mirror /cache/v1
    // surface (`http_access`) or to gate the kkv-v1 notifier's
    // bootstrap-hwm suppression (`notify`). Capture each registered
    // mirror's source-partition high-watermark *now* so the gate
    // flips only after we've consumed past whatever was already
    // there at startup (KKV semantics: dependents must not see a
    // partially-rebuilt cache, and webhook subscribers must not see
    // historical-replay invalidations). Disabled mirrors never
    // register: otherwise their slot would never flip ready and
    // the aggregate /q/health/ready would sit at 503 forever.
    let needs_slot = |m: &Mirror| m.http_access.is_some() || m.notify.is_some();
    let cache_state = if enabled_mirrors.iter().copied().any(needs_slot) {
        let tolerance = readiness_lag_tolerance_from_env();
        let state = std::sync::Arc::new(
            mirror_core::CacheState::new().with_readiness_lag_tolerance(tolerance),
        );
        for m in &enabled_mirrors {
            if !needs_slot(m) {
                continue;
            }
            let hwm = fetch_hwm_for_mirror(m).await?;
            let last_committed = fetch_committed_offset_for_mirror(m).await?;
            let is_main = m
                .http_access
                .as_ref()
                .is_some_and(|h| h.cache_v1_main.is_some());
            tracing::info!(
                mirror = %m.name,
                topic = %m.topic,
                partition = m.partition,
                bootstrap_hwm = hwm,
                last_committed = ?last_committed,
                is_main,
                lag_tolerance = tolerance,
                "registering mirror with cache readiness gate"
            );
            state.register_mirror_with_topic(
                &m.name,
                hwm,
                last_committed,
                is_main,
                &m.topic,
                m.partition,
            );
        }
        Some(state)
    } else {
        None
    };

    // Spawn the cache HTTP server if any mirror opted into a route
    // surface (`cache-v1` or `cache-v1-main`). Mirrors that only
    // need the bootstrap-hwm gate (notify-only) don't pull in the
    // server. Runs until shutdown_rx flips OR /_admin/v1/shutdown is hit.
    let wants_http_routes = enabled_mirrors
        .iter()
        .any(|m| m.http_access.as_ref().is_some_and(HttpAccess::any_enabled));
    if let (Some(state), true) = (cache_state.as_ref(), wants_http_routes) {
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

    let mut handles = Vec::with_capacity(enabled_mirrors.len());
    for mirror in &enabled_mirrors {
        let binding = mirror_cache_binding(mirror, cache_state.as_ref());
        let handle = spawn_mirror((*mirror).clone(), shutdown_rx.clone(), binding).await?;
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

/// Materialise a `CacheBinding` for the given mirror if it has a
/// registered slot in the shared CacheState. Slots are registered
/// for any mirror that opts into `http_access` (for the HTTP read
/// surface) or `notify` (for the bootstrap-hwm suppression gate);
/// the binding wires the consume loop's TeeSink to that slot so
/// `apply_record` flips the slot's `caught_up` at the right offset.
fn mirror_cache_binding(
    mirror: &Mirror,
    cache: Option<&std::sync::Arc<mirror_core::CacheState>>,
) -> Option<mirror_core::CacheBinding> {
    let needs_slot = mirror.http_access.is_some() || mirror.notify.is_some();
    match (needs_slot, cache) {
        (true, Some(state)) => Some(mirror_core::CacheBinding {
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

/// Read the broker's `__consumer_offsets` for this mirror's group
/// at startup. `Ok(None)` means the group has no committed value yet
/// (fresh deploy); the `CacheState` then falls back to
/// `bootstrap_hwm` for the suppression threshold. Like `fetch_hwm_for_mirror`,
/// this hits `BaseConsumer` synchronously under `spawn_blocking`.
async fn fetch_committed_offset_for_mirror(mirror: &Mirror) -> Result<Option<u64>> {
    let bootstrap = mirror.source.bootstrap_servers.clone();
    let group_id = mirror
        .source
        .group_id
        .clone()
        .unwrap_or_else(|| format!("mirror-v3-{}", mirror.name));
    let topic = mirror.topic.clone();
    let partition = mirror.partition as i32;
    let mirror_name = mirror.name.clone();
    let committed = tokio::task::spawn_blocking(move || {
        mirror_kafka::fetch_committed_offset(
            &bootstrap,
            &group_id,
            &topic,
            partition,
            std::time::Duration::from_secs(10),
        )
    })
    .await
    .with_context(|| format!("mirror {mirror_name}: committed task join"))?
    .with_context(|| format!("mirror {mirror_name}: fetch committed offset"))?;
    Ok(committed)
}

async fn shutdown_signal(mut rx: tokio::sync::watch::Receiver<bool>) {
    if *rx.borrow() {
        return;
    }
    let _ = rx.changed().await;
}

/// Install the Prometheus exporter on `0.0.0.0:<port>`. Port defaults
/// to 9090; override with `MIRROR_V3_METRICS_PORT` (set to `0` to
/// disable). A failure to bind logs at warn level and is non-fatal -
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
    // Snapshot two commit handles before the run loop takes
    // ownership of the source. Each `KafkaCommitHandle` clones the
    // underlying `Arc<StreamConsumer>` (cheap); the periodic commit
    // task and the readiness poller each get their own.
    let commit_handle = source.commit_handle();
    let commit_handle_for_poller = source.commit_handle();

    let name = mirror.name.clone();
    let labels = MetricLabels {
        topic: mirror.topic.clone(),
        partition: mirror.partition,
    };
    let compaction = compaction_label(mirror.compaction);

    // Build one inner Sink per destination, then wrap them in a tee.
    // The single-destination case routes through a length-1 tee too -
    // this keeps the cache binding's per-record fanout on a single
    // code path. A *notify-only* mirror (no destinations + a notify
    // block, validated upstream) wraps a single in-memory
    // [`NotifyOnlySink`] in the tee so the rest of the run loop -
    // bootstrap, low-watermark alignment, idle-drift checks - keeps
    // its existing shape.
    let mut inners: Vec<(String, Box<dyn Sink>)> = Vec::with_capacity(
        // +1 reserved for the notify-only path; harmless when
        // destinations is non-empty.
        mirror.destinations.len().max(1),
    );
    let mut dest_descriptions: Vec<String> = Vec::with_capacity(mirror.destinations.len());
    // Per-destination ack slots, shared by Arc with the shims
    // installed on each inner sink and with the AckTracker that the
    // periodic commit task reads. `affects_readiness` is set from the
    // YAML `affects-readiness:` field on each destination (default
    // true): a destination with `affects-readiness: false` still
    // records `flushed_through` for observability but is skipped when
    // computing `MirrorStatus::DestinationLagging`.
    let mut dest_ack_slots: Vec<Arc<DestAckSlot>> = Vec::with_capacity(mirror.destinations.len());
    for dest in &mirror.destinations {
        let inner_name = dest.effective_name(&mirror.name);
        let kind = destination_type(dest);
        dest_descriptions.push(format!("{inner_name}({kind})"));
        let mut sink: Box<dyn Sink> =
            open_inner_sink(dest, &mirror, &inner_name, cache.as_ref()).await?;
        let slot = Arc::new(DestAckSlot::new(
            inner_name.clone(),
            dest.affects_readiness(),
        ));
        // Pick the right observer hook per destination type. Blob
        // sinks fire `FlushObserver` per buffered flush; Kafka sinks
        // commit per-record and fire `WriteObserver`. The shim feeds
        // the destination ack slot in either case.
        //
        // Note: when destination-flush trigger is enabled (only on
        // mirrors with at least one blob destination), the tee-level
        // `set_flush_observer` call further down replaces the per-
        // sink FlushObserver installed here with a tee-coordinated
        // version. That's intentional: in destination-flush mode the
        // notify ack is authoritative for source-side commits, so
        // losing the per-destination ack signal for blob sinks is
        // acceptable.
        match dest {
            Destination::Kafka(_) => {
                sink.set_write_observer(Arc::new(WriteAckShim {
                    dest: Arc::clone(&slot),
                }));
            }
            Destination::Filesystem(_) | Destination::S3(_) => {
                sink.set_flush_observer(Arc::new(FlushAckShim {
                    dest: Arc::clone(&slot),
                }));
            }
        }
        dest_ack_slots.push(slot);
        inners.push((inner_name, sink));
    }
    if inners.is_empty() {
        // Notify-only mirror: spec says "On every startup the source
        // seeks to the broker's low watermark". `NotifyOnlySink`
        // declares `allows_compacted_source = true` so the run loop's
        // bootstrap branch aligns the (in-memory) head to
        // `low_watermark`. The notifier sees every record from there
        // forward.
        inners.push((
            "notify-only".to_string(),
            Box::new(NotifyOnlySink::default()) as Box<dyn Sink>,
        ));
        dest_descriptions.push("notify-only".to_string());
    }
    let mut tee = mirror_core::TeeSink::open(inners, cache.clone())
        .await
        .map_err(|e| anyhow::anyhow!("opening tee for mirror {name}: {e}"))?;

    // Build the per-mirror ack tracker. Notify-side slot exists iff
    // the mirror has a `notify:` block; destinations always
    // contribute (commit 9 wires `affects-readiness` to filter).
    let notify_present = mirror.notify.is_some();
    let ack_tracker = Arc::new(AckTracker::new(notify_present, dest_ack_slots));

    // Branch on the notify trigger mode (validated upstream in
    // mirror-config; see WEBHOOKS.md § Trigger):
    //   * source-consume → build `KkvV1Notifier`, pass as the run
    //     loop's `N: Notifier`.
    //   * destination-flush → build `FlushDispatcher`, attach as the
    //     TeeSink's `FlushObserver`; the run loop's notifier is
    //     `NoOpNotifier` (records flow through unobserved).
    //
    // In both modes the notifier's `with_ack_sink` installs the
    // per-mirror `AckTracker` so each successful drain/POST feeds
    // the periodic commit task's view of "delivered through N".
    let trigger_mode = mirror.notify.as_ref().map(|n| n.trigger.on);
    let ack_sink_for_notifier: Arc<dyn mirror_core::AckSink> =
        Arc::clone(&ack_tracker) as Arc<dyn mirror_core::AckSink>;
    let notifier_opt = match trigger_mode {
        Some(mirror_config::TriggerOn::SourceConsume) => {
            build_source_consume_notifier(&mirror, cache.as_ref())?
                .map(|n| n.with_ack_sink(Arc::clone(&ack_sink_for_notifier)))
        }
        _ => None,
    };
    if matches!(
        trigger_mode,
        Some(mirror_config::TriggerOn::DestinationFlush)
    ) {
        let dispatcher = build_flush_dispatcher(&mirror, cache.as_ref())?
            .with_ack_sink(Arc::clone(&ack_sink_for_notifier));
        tee.set_flush_observer(std::sync::Arc::new(dispatcher));
    }

    // Spawn the periodic source-commit task. It reads
    // `AckTracker::commit_offset()` every
    // `MIRROR_V3_OFFSET_COMMIT_INTERVAL_MS` (default 5 s), stages
    // it via the Kafka commit handle, and flushes to the broker.
    // The handle clones an `Arc<StreamConsumer>` internally so this
    // task runs independently of the source-owning run loop.
    let _commit_task = spawn_periodic_commit_task(
        commit_handle,
        Arc::clone(&ack_tracker),
        commit_interval_from_env(),
        name.clone(),
        shutdown_rx.clone(),
    );

    // Spawn the per-mirror readiness poller when a cache slot
    // exists (i.e. the mirror has `http_access` or `notify`). The
    // poller refreshes the broker end offset for the lag-based
    // readiness predicate and detects source-assignment loss.
    if let Some(binding) = cache.as_ref() {
        let _poller = spawn_readiness_poller(
            PollSpec {
                mirror_name: name.clone(),
                bootstrap_servers: mirror.source.bootstrap_servers.clone(),
                topic: mirror.topic.clone(),
                partition: mirror.partition as i32,
                commit_handle: commit_handle_for_poller,
                cache: Arc::clone(&binding.state),
            },
            readiness_poll_interval_from_env(),
            shutdown_rx.clone(),
        );
    } else {
        // No cache slot => no readiness gate to drive. Drop the
        // extra handle.
        drop(commit_handle_for_poller);
    }

    let destinations_log = dest_descriptions.join(",");
    let notify_log = match &mirror.notify {
        Some(n) => {
            let targets: Vec<&str> = n.targets.iter().map(|t| t.url.as_str()).collect();
            let trigger = match n.trigger.on {
                mirror_config::TriggerOn::SourceConsume => "source-consume",
                mirror_config::TriggerOn::DestinationFlush => "destination-flush",
            };
            format!(" notify=kkv-v1[{}] trigger={trigger}", targets.join(","))
        }
        None => String::new(),
    };

    // Single span carries `mirror = <name>` onto every event emitted
    // from the spawned task - including the mirror-core logs
    // (`starting mirror`, `heartbeat`, etc.) that don't otherwise have
    // access to the operator-chosen mirror name. MIRROR_LABELS still
    // carries topic+partition for metric labeling separately.
    let span = tracing::info_span!("mirror", name = %name);
    Ok(tokio::spawn(
        async move {
            tracing::info!(
                destinations = %destinations_log,
                compaction,
                notify = %notify_log,
                "loop start"
            );
            let heartbeat = heartbeat_interval_from_env();
            let shutdown = shutdown_signal(shutdown_rx);
            // Match-on-notifier so the generic `N: Notifier`
            // monomorphises with the right concrete type per branch
            // without a `Box<dyn Notifier>` allocation.
            let result = match notifier_opt {
                Some(n) => {
                    MIRROR_LABELS
                        .scope(
                            labels,
                            run_mirror_with_notifier(source, tee, n, shutdown, heartbeat),
                        )
                        .await
                }
                None => {
                    MIRROR_LABELS
                        .scope(
                            labels,
                            run_mirror_with_notifier(
                                source,
                                tee,
                                NoOpNotifier,
                                shutdown,
                                heartbeat,
                            ),
                        )
                        .await
                }
            };
            match result {
                Ok(()) => Ok(()),
                Err(e) => Err(anyhow::anyhow!("mirror {name}: {e}")),
            }
        }
        .instrument(span),
    ))
}

/// Construct the `KkvV1Notifier` for a mirror with
/// `trigger.on: source-consume`. Returns `None` when the mirror has
/// no notify block or uses a different trigger (the supervisor
/// handles the destination-flush case via [`build_flush_dispatcher`]).
/// Failures bubble up so the supervisor refuses to spawn a mirror
/// whose webhook surface can't possibly work.
///
/// `cache` carries the shared `CacheState` and the per-mirror name
/// used by the notifier's bootstrap_hwm suppression gate.
/// `mirror-config` validation requires `http-access: cache-v1`
/// whenever `notify` is set, so this binding is always present for
/// any mirror that reaches this branch.
fn build_source_consume_notifier(
    mirror: &Mirror,
    cache: Option<&mirror_core::CacheBinding>,
) -> Result<Option<mirror_notify_kkv::KkvV1Notifier>> {
    let Some(notify) = mirror.notify.as_ref() else {
        return Ok(None);
    };
    let binding = cache.ok_or_else(|| {
        anyhow::anyhow!(
            "mirror {} has notify but no cache binding; validator should reject this",
            mirror.name
        )
    })?;
    // Only kkv-v1 exists today; validator rejects other api: values.
    let notifier = mirror_notify_kkv::KkvV1Notifier::from_config(
        notify,
        mirror.topic.clone(),
        mirror.partition as i32,
        std::sync::Arc::clone(&binding.state),
        binding.mirror_name.clone(),
    )
    .with_context(|| format!("building notify dispatcher for mirror {}", mirror.name))?;
    Ok(Some(notifier))
}

/// Construct the `FlushDispatcher` for a mirror with
/// `trigger.on: destination-flush`. Validator guarantees the mirror
/// has notify set; this asserts on the trigger variant.
fn build_flush_dispatcher(
    mirror: &Mirror,
    cache: Option<&mirror_core::CacheBinding>,
) -> Result<mirror_notify_kkv::FlushDispatcher> {
    let notify = mirror
        .notify
        .as_ref()
        .expect("build_flush_dispatcher called with no notify block");
    debug_assert!(matches!(
        notify.trigger.on,
        mirror_config::TriggerOn::DestinationFlush
    ));
    let binding = cache.ok_or_else(|| {
        anyhow::anyhow!(
            "mirror {} has notify but no cache binding; validator should reject this",
            mirror.name
        )
    })?;
    let dispatcher = mirror_notify_kkv::FlushDispatcher::from_config(
        notify,
        mirror.topic.clone(),
        mirror.partition as i32,
        std::sync::Arc::clone(&binding.state),
        binding.mirror_name.clone(),
    )
    .with_context(|| {
        format!(
            "building notify flush dispatcher for mirror {}",
            mirror.name
        )
    })?;
    Ok(dispatcher)
}

/// In-memory sink for `destinations: []` notify-only mirrors. Holds
/// only its own "next expected offset" and accepts any record at or
/// above it. `allows_compacted_source = true` so the run loop's
/// bootstrap branch can align the head to the broker's low
/// watermark - matching the spec's "seeks to low watermark on every
/// startup" behaviour for notify-only mirrors.
#[derive(Debug, Default)]
struct NotifyOnlySink {
    position: u64,
}

#[async_trait::async_trait]
impl Sink for NotifyOnlySink {
    async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
        Ok(self.position)
    }

    async fn write(&mut self, record: Record) -> Result<(), SinkError> {
        if record.source_offset < self.position {
            return Err(SinkError::UnexpectedPosition {
                expected: self.position,
                actual: record.source_offset,
            });
        }
        // Accept forward gaps under compaction:log; bump position to
        // `record.source_offset + 1`. Matches the loosened write
        // contract in `mirror-fs` / `mirror-s3` for compacted sources.
        self.position = record.source_offset + 1;
        Ok(())
    }

    fn allows_compacted_source(&self) -> bool {
        true
    }

    async fn align_to_source_low_watermark(&mut self, low_watermark: u64) -> Result<(), SinkError> {
        self.position = low_watermark;
        Ok(())
    }
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
