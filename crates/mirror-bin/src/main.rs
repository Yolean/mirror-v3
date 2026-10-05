use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use mirror_config::{Destination, HttpAccess, Mirror};

mod ack_tracker;
mod knobs;
mod readiness_poller;
use ack_tracker::{
    final_commit, spawn_periodic_commit_task, AckTracker, DestAckSlot, FlushAckShim, WriteAckShim,
};
use knobs::Knobs;
use mirror_core::{
    run_mirror_with_notifier, MetricLabels, NoOpNotifier, Record, Sink, SinkError, MIRROR_LABELS,
};
use mirror_fs::{FilesystemSink, FilesystemSinkConfig};
use mirror_kafka::{KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig};
use mirror_s3::{S3Sink, S3SinkConfig};
use readiness_poller::{spawn_readiness_poller, PollSpec};
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
    // Colours only on a terminal: in a pod the escape codes end up in
    // the log store.
    use std::io::IsTerminal;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_ansi(std::io::stderr().is_terminal())
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
    source_high: Option<u64>,
    dest_next: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl StatusRow {
    /// Source high watermark minus destination next offset; negative
    /// when the destination is ahead (a recreated topic).
    fn lag(&self) -> Option<i64> {
        match (self.source_high, self.dest_next) {
            (Some(h), Some(n)) => Some(h as i64 - n as i64),
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
                flush: params.flush,
            };
            let mut sink = FilesystemSink::open(cfg).map_err(|e| anyhow::anyhow!("{e}"))?;
            sink.next_expected_offset()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))
        }
        Destination::S3(s3) => {
            let cfg = s3_sink_config(s3, mirror, &dest_name)?;
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
    let knobs = Knobs::from_env()?;

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
    install_metrics_exporter(knobs.metrics_port)?;

    // One shutdown channel, cloned per mirror. SIGINT and SIGTERM
    // trigger a graceful flush.
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
        let mut sigterm =
            signal(SignalKind::terminate()).context("installing the SIGTERM handler")?;
        let term_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            if sigterm.recv().await.is_some() {
                tracing::info!("received SIGTERM; requesting graceful shutdown");
                let _ = term_tx.send(true);
            }
        });
    }

    // Every *enabled* mirror gets a `CacheState` slot, regardless of
    // whether it has `http_access` or `notify`. The slot is what the
    // structured `/q/health/ready` body enumerates; downstream
    // features (HTTP routes, notify suppression gate, source-commit
    // task) only attach when the mirror opts into them. Disabled
    // mirrors never register: otherwise their slot would never flip
    // ready and the aggregate /q/health/ready would sit at 503
    // forever. Capture each registered mirror's source-partition
    // high-watermark *now* so the gate flips only after we've
    // consumed past whatever was already there at startup (KKV
    // semantics: dependents must not see a partially-rebuilt cache,
    // and webhook subscribers must not see historical-replay
    // invalidations).
    let cache_state = if enabled_mirrors.is_empty() {
        None
    } else {
        let tolerance = knobs.readiness_lag;
        let state = std::sync::Arc::new(
            mirror_core::CacheState::new().with_readiness_lag_tolerance(tolerance),
        );
        for m in &enabled_mirrors {
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
            if serves_cache(m) {
                state.register_mirror_with_topic(
                    &m.name,
                    hwm,
                    last_committed,
                    is_main,
                    &m.topic,
                    m.partition,
                );
            } else {
                state.register_progress_only(&m.name, hwm, &m.topic, m.partition);
            }
        }
        Some(state)
    };

    // Spawn the cache HTTP server if any mirror opted into a route
    // surface (`cache-v1` or `cache-v1-main`). Mirrors that only
    // need the bootstrap-hwm gate (notify-only) don't pull in the
    // server. Runs until shutdown_rx flips OR /_admin/v1/shutdown is hit.
    let wants_http_routes = enabled_mirrors
        .iter()
        .any(|m| m.http_access.as_ref().is_some_and(HttpAccess::any_enabled));
    let mut handles = Vec::with_capacity(enabled_mirrors.len() + 1);
    if let (Some(state), true) = (cache_state.as_ref(), wants_http_routes) {
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], knobs.cache_port));
        let state = std::sync::Arc::clone(state);
        let cache_shutdown_rx = shutdown_rx.clone();
        let cache_shutdown_tx = shutdown_tx.clone();
        // The server is waited for like a mirror: its failure (a port
        // in use) ends the process non-zero, where it used to request a
        // graceful shutdown and exit 0.
        handles.push((
            "cache-http".to_string(),
            tokio::spawn(async move {
                let signal = shutdown_signal(cache_shutdown_rx);
                let result = mirror_cache::serve(addr, state, signal).await;
                // Admin shutdown, or our own: stop the mirrors too.
                let _ = cache_shutdown_tx.send(true);
                result
                    .map(|_code| ())
                    .map_err(|e| anyhow::anyhow!("cache HTTP server: {e}"))
            }),
        ));
    }

    for mirror in &enabled_mirrors {
        let binding = mirror_cache_binding(mirror, cache_state.as_ref());
        let handle = if restarts_in_process(mirror) {
            tokio::spawn(supervise_in_process(
                (*mirror).clone(),
                knobs.clone(),
                shutdown_rx.clone(),
                binding,
            ))
        } else {
            spawn_mirror((*mirror).clone(), &knobs, shutdown_rx.clone(), binding).await?
        };
        handles.push((mirror.name.clone(), handle));
    }

    // The first error ends the process. A mirror that ends without
    // error has seen the shutdown signal, so every other mirror is
    // stopping too: wait for all of them, so each one's final flush and
    // commit completes before the runtime is dropped
    // (returning on the first graceful exit cancelled the others'
    // in-flight PUTs and final commits).
    let result = wait_mirrors(handles).await;
    if let Err(e) = &result {
        tracing::error!(error = %format!("{e:#}"), "mirror task errored; exiting process");
    }
    result
}

/// Wait for every mirror task; return the first error as soon as it
/// happens, or `Ok` once all have ended without one.
async fn wait_mirrors(handles: Vec<(String, tokio::task::JoinHandle<Result<()>>)>) -> Result<()> {
    let mut pending: futures::stream::FuturesUnordered<_> = handles
        .into_iter()
        .map(|(name, handle)| async move {
            let r = match handle.await {
                Ok(inner) => inner,
                Err(join) => Err(anyhow::anyhow!("task join: {join}")),
            };
            (name, r)
        })
        .collect();
    use futures::StreamExt;
    while let Some((name, result)) = pending.next().await {
        match result {
            Ok(()) => tracing::info!(mirror = %name, "mirror task terminated gracefully"),
            Err(e) => return Err(e.context(format!("mirror {name}"))),
        }
    }
    Ok(())
}

/// Whether the mirror serves `/cache/v1` and so holds every key's latest
/// value in memory. Only these mirrors keep values; the cache is built by
/// reading the source topic from its low watermark (kkv's bootstrap), never
/// from a destination, so a cache does not depend on S3 being up and a blob
/// mirror does not download its archive at startup.
fn serves_cache(mirror: &Mirror) -> bool {
    mirror
        .http_access
        .as_ref()
        .is_some_and(HttpAccess::any_enabled)
}

/// Whether a failed mirror is restarted inside the process instead of
/// ending it, when its failure is transient ([`is_transient`]). A mirror
/// with neither a cache nor notify holds no state but its destinations,
/// and opening it again re-derives its position from them, exactly as a
/// process restart would. Restarting it alone keeps a destination outage
/// (S3 down, a full disk) from taking the process, and with it the caches
/// and notifications other mirrors serve, down with it. A cache or notify mirror ends the process: its in-memory
/// state belongs to the process and is rebuilt by the orchestrator's
/// restart.
fn restarts_in_process(mirror: &Mirror) -> bool {
    !serves_cache(mirror) && mirror.notify.is_none()
}

/// Whether a failed run of a mirror can succeed when it is opened again:
/// the source or a destination could not be reached. Anything else, a
/// destination that contradicts the source (an offset mismatch, a foreign
/// object, a corrupt chain), a lost source position, or data or
/// configuration that cannot work, repeats on every attempt; it ends the
/// process so that it shows as a crash loop, as it does for a cache or
/// notify mirror.
fn is_transient(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if let Some(e) = cause.downcast_ref::<mirror_core::MirrorError>() {
            return e.is_transient();
        }
        if let Some(e) = cause.downcast_ref::<SinkError>() {
            return e.is_transient();
        }
        if let Some(e) = cause.downcast_ref::<mirror_core::SourceError>() {
            return e.is_transient();
        }
        if let Some(e) = cause.downcast_ref::<mirror_fs::BlobError>() {
            return matches!(e, mirror_fs::BlobError::Store(_));
        }
    }
    false
}

const RESTART_BACKOFF_MIN: std::time::Duration = std::time::Duration::from_secs(1);
const RESTART_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(60);

/// Run a mirror and, while the process is not shutting down, open and run
/// it again after each transient failure, with a backoff from 1 s doubling
/// to 60 s (reset after a run that lasted longer than the cap). Every such
/// failure is logged as an error and counted in
/// `mirror_v3_mirror_restarts_total`; any other failure is returned and
/// ends the process.
async fn supervise_in_process(
    mirror: Mirror,
    knobs: Knobs,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    cache: Option<mirror_core::CacheBinding>,
) -> Result<()> {
    let mut backoff = RESTART_BACKOFF_MIN;
    loop {
        let started = std::time::Instant::now();
        let result =
            match spawn_mirror(mirror.clone(), &knobs, shutdown_rx.clone(), cache.clone()).await {
                Ok(handle) => match handle.await {
                    Ok(r) => r,
                    Err(join) => Err(anyhow::anyhow!("task join: {join}")),
                },
                Err(e) => Err(e),
            };
        let err = match result {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        if *shutdown_rx.borrow() || !is_transient(&err) {
            return Err(err);
        }
        if started.elapsed() > RESTART_BACKOFF_MAX {
            backoff = RESTART_BACKOFF_MIN;
        }
        tracing::error!(
            mirror = %mirror.name,
            error = %format!("{err:#}"),
            retry_in_s = backoff.as_secs(),
            "mirror failed; it holds no state but its destinations, so it is opened again in this process"
        );
        metrics::counter!(
            "mirror_v3_mirror_restarts_total",
            "topic" => mirror.topic.clone(),
            "partition" => mirror.partition.to_string(),
        )
        .increment(1);
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown_signal(shutdown_rx.clone()) => return Ok(()),
        }
        backoff = (backoff * 2).min(RESTART_BACKOFF_MAX);
    }
}

/// Materialise a `CacheBinding` for the given mirror. Every enabled
/// mirror now registers a slot in the shared CacheState (the
/// supervisor enumerates them in the structured `/q/health/ready`
/// body), so the binding is materialised whenever a `CacheState`
/// exists at all. The binding wires the consume loop's TeeSink to
/// that slot so `apply_record` advances the slot's
/// `last_applied_offset` and flips the readiness gate at the right
/// point.
fn mirror_cache_binding(
    mirror: &Mirror,
    cache: Option<&std::sync::Arc<mirror_core::CacheState>>,
) -> Option<mirror_core::CacheBinding> {
    cache.map(|state| mirror_core::CacheBinding {
        state: std::sync::Arc::clone(state),
        mirror_name: mirror.name.clone(),
    })
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
    Ok(hwm)
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

/// Broker low watermark for a mirror's source partition, used to
/// clamp the notify resume floor: a committed offset that has aged
/// past retention cannot be re-read, so the replay starts at the
/// earliest available offset instead of erroring the append-mode
/// bootstrap gate.
async fn fetch_low_watermark_for_mirror(mirror: &Mirror) -> Result<u64> {
    let bootstrap = mirror.source.bootstrap_servers.clone();
    let topic = mirror.topic.clone();
    let partition = mirror.partition as i32;
    let mirror_name = mirror.name.clone();
    let low = tokio::task::spawn_blocking(move || {
        mirror_kafka::fetch_low_watermark(
            &bootstrap,
            &topic,
            partition,
            std::time::Duration::from_secs(5),
        )
    })
    .await
    .with_context(|| format!("mirror {mirror_name}: low watermark task join"))?
    .with_context(|| format!("mirror {mirror_name}: fetch low watermark"))?;
    Ok(low)
}

async fn shutdown_signal(mut rx: tokio::sync::watch::Receiver<bool>) {
    if *rx.borrow() {
        return;
    }
    let _ = rx.changed().await;
}

/// Install the Prometheus exporter on `0.0.0.0:<port>`. A failure is a
/// startup error: an unmonitored mirror is not running as configured.
fn install_metrics_exporter(port: u16) -> Result<()> {
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .with_context(|| format!("installing the metrics exporter on {addr}"))?;
    tracing::info!(%addr, "metrics exporter listening on /metrics");
    Ok(())
}

async fn spawn_mirror(
    mirror: Mirror,
    knobs: &Knobs,
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
    // Snapshot three commit handles before the run loop takes
    // ownership of the source. Each `KafkaCommitHandle` clones the
    // underlying `Arc<StreamConsumer>` (cheap); the periodic commit
    // task, the readiness poller and the final shutdown commit each
    // get their own.
    let commit_handle = source.commit_handle();
    let commit_handle_for_poller = source.commit_handle();
    let commit_handle_final = source.commit_handle();

    let name = mirror.name.clone();
    let labels = MetricLabels {
        topic: mirror.topic.clone(),
        partition: mirror.partition,
    };
    let compaction = compaction_label(mirror.compaction);

    // Build one inner Sink per destination, then wrap them in a tee.
    // The single-destination case routes through a length-1 tee too -
    // this keeps the cache binding's per-record fanout on a single
    // code path. A mirror without destinations (a cache, a notify
    // feed, or both; validated upstream) wraps a single in-memory
    // [`NoDestinationSink`] in the tee so the rest of the run loop -
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
        let mut sink: Box<dyn Sink> = open_inner_sink(dest, &mirror, &inner_name).await?;
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
        // No destinations: the source is read from the broker's low
        // watermark on every startup. `NoDestinationSink` declares
        // `allows_compacted_source = true` so the run loop's bootstrap
        // branch aligns its (in-memory) head to `low_watermark`; the
        // cache and the notifier see every record from there.
        inners.push((
            "none".to_string(),
            Box::new(NoDestinationSink::default()) as Box<dyn Sink>,
        ));
        dest_descriptions.push("none".to_string());
    }
    let mut tee = mirror_core::TeeSink::open(inners, cache.clone())
        .await
        .with_context(|| format!("opening tee for mirror {name}"))?;

    // Build the per-mirror ack tracker. Notify-side slot exists iff
    // the mirror has a `notify:` block; destinations always
    // contribute (commit 9 wires `affects-readiness` to filter).
    let notify_present = mirror.notify.is_some();
    let ack_tracker = Arc::new(AckTracker::new(notify_present, dest_ack_slots));
    let ack_tracker_final = Arc::clone(&ack_tracker);

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
    // Notify re-delivery across restarts: the destinations' durable
    // heads can be ahead of the broker-committed (= notify-acked)
    // offset, e.g. when the webhook receiver was down at shutdown.
    // The destination state alone would resume above those records
    // and their invalidations would never fire. `[committed,
    // min(heads))` is that gap; each trigger mode closes it below.
    let notify_committed = if trigger_mode.is_some() {
        fetch_committed_offset_for_mirror(&mirror).await?
    } else {
        None
    };
    let min_head = tee
        .heads()
        .iter()
        .map(|(_, h)| *h)
        .min()
        .expect("tee is non-empty by construction");
    let notifier_opt = match trigger_mode {
        Some(mirror_config::TriggerOn::SourceConsume) => {
            build_source_consume_notifier(&mirror, cache.as_ref())?
                .map(|n| n.with_ack_sink(Arc::clone(&ack_sink_for_notifier)))
        }
        _ => None,
    };
    if serves_cache(&mirror) && !mirror.destinations.is_empty() {
        // The cache holds the latest value of every key, so it reads the
        // topic from the low watermark like kkv. The destinations are
        // past that already: the tee skips their writes below each head,
        // and the notifier's suppression threshold (the committed offset,
        // or the bootstrap watermark on a fresh group) keeps the replay
        // from firing webhooks for records the previous pod notified.
        let low = fetch_low_watermark_for_mirror(&mirror).await?;
        tracing::info!(
            mirror = %name,
            low_watermark = low,
            destination_min_head = min_head,
            "cache mirror: reading the source from its low watermark"
        );
        tee.set_resume_floor(low);
    } else if notifier_opt.is_some() {
        if let Some(committed) = notify_committed {
            if committed < min_head {
                // Re-read the gap from the source; the tee skips the
                // destination writes and the notifier's suppression
                // threshold (== committed) lets exactly these
                // records fire.
                let low = fetch_low_watermark_for_mirror(&mirror).await?;
                if committed < low {
                    tracing::warn!(
                        mirror = %name,
                        committed,
                        low_watermark = low,
                        "committed offset is below the broker low watermark; notify re-delivery starts at the earliest available offset and the records below it are lost"
                    );
                }
                let floor = committed.max(low);
                tracing::info!(
                    mirror = %name,
                    committed,
                    destination_min_head = min_head,
                    resume_floor = floor,
                    "re-reading un-acked notify window from the source"
                );
                tee.set_resume_floor(floor);
            }
        }
    }
    // The run loop races this watch so a terminal dispatch failure
    // errors the mirror even when nothing else would surface it:
    // the flush drainer's death is otherwise invisible (its events
    // are not regenerated on restart), and the notifier's timer
    // error only surfaces on the next record, which an idle topic
    // never delivers.
    let mut dispatch_error_watch = notifier_opt.as_ref().map(|n| n.terminal_error_watch());
    let mut flush_dispatcher_shutdown: Option<Arc<mirror_notify_kkv::FlushDispatcher>> = None;
    if matches!(
        trigger_mode,
        Some(mirror_config::TriggerOn::DestinationFlush)
    ) {
        let dispatcher = build_flush_dispatcher(&mirror, cache.as_ref())?
            .with_ack_sink(Arc::clone(&ack_sink_for_notifier));
        dispatch_error_watch = Some(dispatcher.terminal_error_watch());
        // Destination-flush variant of the un-acked window: a replay
        // cannot regenerate flush events for data that is already
        // durable, so synthesize one catch-up event covering
        // `[committed, min_head)`. Suppression (threshold ==
        // committed) admits it; a fresh deploy (no commit) skips it.
        if let Some(committed) = notify_committed {
            if committed < min_head {
                tracing::info!(
                    mirror = %name,
                    committed,
                    destination_min_head = min_head,
                    "dispatching synthetic flush notification for the un-acked window"
                );
                use mirror_core::FlushObserver;
                dispatcher.on_flushed(committed, min_head - 1);
            }
        }
        // The tee owns the dispatcher as its FlushObserver; the
        // supervisor keeps a second handle so the graceful-shutdown
        // path can drain queued flush events before the process
        // exits (they are not regenerated on restart).
        let dispatcher = Arc::new(dispatcher);
        flush_dispatcher_shutdown = Some(Arc::clone(&dispatcher));
        tee.set_flush_observer(dispatcher);
    }

    // Spawn the periodic source-commit task. It reads
    // `AckTracker::commit_offset()` every
    // `MIRROR_V3_OFFSET_COMMIT_INTERVAL_MS` (default 5 s), stages
    // it via the Kafka commit handle, and flushes to the broker.
    // The handle clones an `Arc<StreamConsumer>` internally so this
    // task runs independently of the source-owning run loop.
    let commit_task = spawn_periodic_commit_task(
        commit_handle,
        Arc::clone(&ack_tracker),
        knobs.commit_interval,
        name.clone(),
        shutdown_rx.clone(),
    );

    // Spawn the per-mirror readiness poller when a cache slot
    // exists (i.e. the mirror has `http_access` or `notify`). The
    // poller refreshes the broker end offset for the lag-based
    // readiness predicate and detects source-assignment loss.
    let mut poller = None;
    if let Some(binding) = cache.as_ref() {
        poller = Some(spawn_readiness_poller(
            PollSpec {
                mirror_name: name.clone(),
                bootstrap_servers: mirror.source.bootstrap_servers.clone(),
                topic: mirror.topic.clone(),
                partition: mirror.partition as i32,
                commit_handle: commit_handle_for_poller,
                cache: Arc::clone(&binding.state),
            },
            knobs.readiness_poll,
            shutdown_rx.clone(),
        ));
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
    let knobs_heartbeat = knobs.heartbeat;
    Ok(tokio::spawn(
        async move {
            tracing::info!(
                destinations = %destinations_log,
                compaction,
                notify = %notify_log,
                "loop start"
            );
            let heartbeat = knobs_heartbeat;
            let shutdown = shutdown_signal(shutdown_rx);
            // Match-on-notifier so the generic `N: Notifier`
            // monomorphises with the right concrete type per branch
            // without a `Box<dyn Notifier>` allocation.
            let result = match notifier_opt {
                Some(n) => {
                    MIRROR_LABELS
                        .scope(
                            labels,
                            run_racing_dispatch_error(
                                run_mirror_with_notifier(source, tee, n, shutdown, heartbeat),
                                dispatch_error_watch,
                            ),
                        )
                        .await
                }
                None => {
                    MIRROR_LABELS
                        .scope(
                            labels,
                            run_racing_dispatch_error(
                                run_mirror_with_notifier(
                                    source,
                                    tee,
                                    NoOpNotifier,
                                    shutdown,
                                    heartbeat,
                                ),
                                dispatch_error_watch,
                            ),
                        )
                        .await
                }
            };
            // Drain the flush dispatcher before committing: the run
            // loop's final sink.flush() may have queued flush events
            // that are not regenerated on restart, and their acks
            // belong in the final commit below.
            let flush_drain = match flush_dispatcher_shutdown {
                Some(d) => d.drain_and_stop().await,
                None => Ok(()),
            };
            // One final sync commit of whatever the notifier acked:
            // the periodic commit task exits on the shutdown signal
            // before the run loop's final drain acks, and its async
            // commit mode would race process exit anyway. Runs on
            // the error path too - acked offsets are delivered
            // regardless of why the loop stopped, and committing
            // them shrinks the duplicate-webhook replay on restart.
            final_commit(commit_handle_final, &ack_tracker_final, &name).await;
            // The commit task and the poller hold this run's consumer;
            // they end with the run, also when the process goes on (a
            // mirror restarted in process opens a new consumer).
            commit_task.abort();
            if let Some(p) = poller {
                p.abort();
            }
            match (result, flush_drain) {
                (Ok(()), Ok(())) => Ok(()),
                (Err(e), _) => Err(e.context(format!("mirror {name}"))),
                (Ok(()), Err(e)) => Err(anyhow::anyhow!(
                    "mirror {name}: flush notify drain on shutdown: {e}"
                )),
            }
        }
        .instrument(span),
    ))
}

/// Race the mirror run loop against the notify pipeline's terminal
/// dispatch error. Whichever resolves first errors (or completes)
/// the mirror; the loser is dropped. With no watch installed the
/// second branch is pending forever and this is exactly the run
/// loop.
async fn run_racing_dispatch_error<Fut>(
    run: Fut,
    watch: Option<mirror_notify_kkv::TerminalErrorWatch>,
) -> Result<(), anyhow::Error>
where
    Fut: std::future::Future<Output = Result<(), mirror_core::MirrorError>>,
{
    let watch_error = async move {
        match watch {
            Some(w) => w.wait().await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        r = run => r.map_err(anyhow::Error::from),
        e = watch_error => Err(anyhow::anyhow!(
            "notify dispatch failed terminally: {e}"
        )),
    }
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
/// watermark - the "seeks to low watermark on every startup" behaviour
/// of mirrors without destinations (caches, notify feeds).
#[derive(Debug, Default)]
struct NoDestinationSink {
    position: u64,
}

#[async_trait::async_trait]
impl Sink for NoDestinationSink {
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

    fn allows_offset_holes(&self) -> bool {
        true
    }

    async fn align_to_source_low_watermark(&mut self, low_watermark: u64) -> Result<(), SinkError> {
        self.position = low_watermark;
        Ok(())
    }
}

/// The object store for one S3 identity. Configured explicitly: no
/// `AWS_*` variable other than the two named in the config changes its
/// behaviour, and missing keys are an error (from_env() would silently
/// pick up AWS_CONDITIONAL_PUT and friends, and fall back to instance
/// metadata probing for credentials).
fn s3_store(
    s3: &mirror_config::S3Destination,
    key: &mirror_config::S3Key,
) -> Result<Arc<dyn object_store::ObjectStore>> {
    let env = |name: &str| {
        std::env::var(name).with_context(|| {
            format!(
                "S3 destination bucket {}: environment variable {name} (named in its credentials) is not set",
                s3.bucket
            )
        })
    };
    let mut builder = object_store::aws::AmazonS3Builder::new()
        .with_region(&s3.region)
        .with_bucket_name(&s3.bucket)
        .with_access_key_id(env(&key.access_key_id_env)?)
        .with_secret_access_key(env(&key.secret_access_key_env)?);
    if let Some(endpoint) = &s3.endpoint {
        builder = builder.with_endpoint(endpoint);
        if endpoint.starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
    }
    let store = builder
        .build()
        .with_context(|| format!("building the S3 client for bucket {}", s3.bucket))?;
    Ok(Arc::new(store))
}

fn s3_sink_config(
    s3: &mirror_config::S3Destination,
    mirror: &Mirror,
    destination_name: &str,
) -> Result<S3SinkConfig> {
    let params = resolve_blob_params(mirror)?;
    Ok(S3SinkConfig {
        read_store: s3_store(s3, &s3.credentials.read)?,
        write_store: s3_store(s3, &s3.credentials.write)?,
        prefix: s3.prefix.as_deref().map(object_store::path::Path::from),
        destination_name: destination_name.to_string(),
        partition: mirror.partition,
        format: params.format,
        compression: params.compression,
        keys: params.keys,
        values: params.values,
        compaction: compaction_to_s3(mirror.compaction),
        flush: mirror_s3::FlushTriggers {
            max_time: params.flush.max_time,
            max_bytes: params.flush.max_bytes,
            max_offsets: params.flush.max_offsets,
            daily_at_utc_seconds: params.flush.daily_at_utc_seconds,
        },
        encryption: match &s3.encryption {
            mirror_config::Encryption::None(_) => None,
            mirror_config::Encryption::ParquetKeys(k) => {
                let keyring = mirror_envelope::Keyring::load(&k.keys_dir)
                    .with_context(|| format!("S3 destination bucket {}", s3.bucket))?;
                keyring
                    .get(&k.key_id)
                    .with_context(|| format!("S3 destination bucket {}", s3.bucket))?;
                Some(mirror_s3::BlobEncryption {
                    key_id: k.key_id.clone(),
                    keyring: Arc::new(keyring),
                })
            }
        },
    })
}

async fn open_inner_sink(
    dest: &Destination,
    mirror: &Mirror,
    inner_name: &str,
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
            let sink_cfg = FilesystemSinkConfig {
                root: fs.root.clone(),
                destination_name: inner_name.to_string(),
                partition: mirror.partition,
                format: params.format,
                compression: params.compression,
                keys: params.keys,
                values: params.values,
                compaction: compaction_to_fs(mirror.compaction),
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
            let sink_cfg = s3_sink_config(s3, mirror, inner_name)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn task(
        delay_ms: u64,
        result: Result<()>,
        done: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) -> tokio::task::JoinHandle<Result<()>> {
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            result
        })
    }

    #[tokio::test]
    async fn graceful_exit_waits_for_every_mirror() {
        let done = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handles = vec![
            ("fast".to_string(), task(1, Ok(()), done.clone())),
            ("slow".to_string(), task(80, Ok(()), done.clone())),
        ];
        wait_mirrors(handles).await.unwrap();
        assert_eq!(done.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn first_error_ends_the_wait() {
        let done = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handles = vec![
            (
                "broken".to_string(),
                task(1, Err(anyhow::anyhow!("boom")), done.clone()),
            ),
            ("slow".to_string(), task(5_000, Ok(()), done.clone())),
        ];
        let started = std::time::Instant::now();
        let err = wait_mirrors(handles).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("mirror broken: boom"),
            "{err:#}"
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
    }

    fn mirror(yaml: &str) -> Mirror {
        mirror_config::load_from_str(yaml)
            .unwrap()
            .mirrors
            .remove(0)
    }

    #[test]
    fn only_mirrors_without_cache_or_notify_restart_in_process() {
        let blob = mirror(
            r#"
mirrors:
  - name: ops
    source: { bootstrap-servers: k:9092 }
    topic: ops
    partition: 0
    destinations: [{ type: filesystem, root: /tmp/x }]
    flush: { max-time-ms: 1000, max-bytes: 1000, max-offsets: 10 }
"#,
        );
        assert!(restarts_in_process(&blob));
        let cache = mirror(
            r#"
mirrors:
  - name: userstate
    source: { bootstrap-servers: k:9092 }
    topic: userstate
    partition: 0
    destinations: []
    http-access: { cache-v1: {}, cache-v1-main: {} }
"#,
        );
        assert!(!restarts_in_process(&cache));
    }

    #[test]
    fn only_unreachable_sources_and_destinations_are_transient() {
        use mirror_core::{MirrorError, SourceError};
        use mirror_fs::BlobError;
        let transient = [
            anyhow::Error::from(MirrorError::Sink(SinkError::Transport("503".into())))
                .context("mirror ops"),
            anyhow::Error::from(MirrorError::Source(SourceError::Transport("down".into()))),
            anyhow::Error::from(BlobError::Store("timeout".into())).context("opening s3 sink"),
            anyhow::Error::from(SinkError::Transport("503".into())).context("opening tee"),
        ];
        for e in &transient {
            assert!(is_transient(e), "{e:#}");
        }
        let fatal = [
            anyhow::Error::from(MirrorError::SinkAheadOfSource {
                sink_offset: 150,
                source_hwm: 100,
            }),
            anyhow::Error::from(MirrorError::Sink(SinkError::UnexpectedPosition {
                expected: 5,
                actual: 6,
            })),
            anyhow::Error::from(MirrorError::Sink(SinkError::Inconsistent("foreign".into())))
                .context("mirror ops"),
            anyhow::Error::from(MirrorError::Source(SourceError::PositionLost(
                "gone".into(),
            ))),
            anyhow::Error::from(BlobError::CorruptChain("overlap".into())).context("opening"),
            anyhow::anyhow!("environment variable S3_KEY is not set"),
        ];
        for e in &fatal {
            assert!(!is_transient(e), "{e:#}");
        }
    }
}
