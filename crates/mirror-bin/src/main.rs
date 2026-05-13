use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use mirror_config::{Destination, Mirror};
use mirror_core::run_mirror;
use mirror_fs::{FilesystemSink, FilesystemSinkConfig};
use mirror_kafka::{KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig};
use mirror_s3::{S3Sink, S3SinkConfig};
use std::sync::Arc;
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
    println!(
        "OK: {} mirror(s), destination type = {}",
        cfg.mirrors.len(),
        destination_type(&cfg.destination)
    );
    Ok(())
}

fn destination_type(d: &Destination) -> &'static str {
    match d {
        Destination::Kafka(_) => "kafka",
        Destination::Filesystem(_) => "filesystem",
        Destination::S3(_) => "s3",
    }
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
    let mut rows = Vec::with_capacity(cfg.mirrors.len());
    for mirror in &cfg.mirrors {
        rows.push(compute_status_row(mirror, &cfg.destination).await);
    }
    let any_errors = rows.iter().any(|r| r.error.is_some());
    match format {
        StatusFormat::Table => print_status_table(&rows),
        StatusFormat::Json => println!("{}", serde_json::to_string_pretty(&rows)?),
    }
    Ok(any_errors)
}

async fn compute_status_row(mirror: &Mirror, destination: &Destination) -> StatusRow {
    let mut row = StatusRow {
        name: mirror.name.clone(),
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
    let destination_name = mirror
        .destination_name_override
        .clone()
        .unwrap_or_else(|| mirror.topic.clone());
    match destination {
        Destination::Kafka(k) => {
            let cfg = KafkaSinkConfig::new(
                k.bootstrap_servers.clone(),
                destination_name,
                mirror.partition as i32,
            );
            let mut sink = KafkaSink::open(cfg).map_err(|e| anyhow::anyhow!("{e}"))?;
            sink.next_expected_offset()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))
        }
        Destination::Filesystem(fs) => {
            // Flush triggers don't matter for a read-only query — they
            // only fire on write — but the constructor requires them.
            let cfg = FilesystemSinkConfig {
                root: fs.root.clone(),
                destination_name,
                partition: mirror.partition,
                flush: mirror_fs::FlushTriggers {
                    max_time: std::time::Duration::from_millis(fs.flush.max_time_ms),
                    max_bytes: fs.flush.max_bytes,
                    max_offsets: fs.flush.max_offsets,
                },
            };
            let mut sink = FilesystemSink::open(cfg).map_err(|e| anyhow::anyhow!("{e}"))?;
            sink.next_expected_offset()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))
        }
        Destination::S3(s3) => {
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
                destination_name,
                partition: mirror.partition,
                flush: mirror_s3::FlushTriggers {
                    max_time: std::time::Duration::from_millis(s3.flush.max_time_ms),
                    max_bytes: s3.flush.max_bytes,
                    max_offsets: s3.flush.max_offsets,
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
    tracing::info!(
        config = %path.display(),
        mirrors = cfg.mirrors.len(),
        destination = destination_type(&cfg.destination),
        "starting mirror-v3"
    );

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

    let mut handles = Vec::with_capacity(cfg.mirrors.len());
    for mirror in &cfg.mirrors {
        let handle = spawn_mirror(mirror.clone(), cfg.destination.clone(), shutdown_rx.clone())?;
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

async fn shutdown_signal(mut rx: tokio::sync::watch::Receiver<bool>) {
    if *rx.borrow() {
        return;
    }
    let _ = rx.changed().await;
}

fn spawn_mirror(
    mirror: Mirror,
    destination: Destination,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
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
    let destination_name = mirror
        .destination_name_override
        .clone()
        .unwrap_or_else(|| mirror.topic.clone());

    match destination {
        Destination::Kafka(k) => {
            let sink_cfg = KafkaSinkConfig::new(
                k.bootstrap_servers,
                destination_name,
                mirror.partition as i32,
            );
            let sink = KafkaSink::open(sink_cfg)
                .with_context(|| format!("opening sink for mirror {name}"))?;
            Ok(tokio::spawn(async move {
                tracing::info!(mirror = %name, "loop start");
                match run_mirror(source, sink, shutdown_signal(shutdown_rx)).await {
                    Ok(()) => Ok(()),
                    Err(e) => Err(anyhow::anyhow!("mirror {name}: {e}")),
                }
            }))
        }
        Destination::Filesystem(fs) => {
            let sink_cfg = FilesystemSinkConfig {
                root: fs.root,
                destination_name,
                partition: mirror.partition,
                flush: mirror_fs::FlushTriggers {
                    max_time: std::time::Duration::from_millis(fs.flush.max_time_ms),
                    max_bytes: fs.flush.max_bytes,
                    max_offsets: fs.flush.max_offsets,
                },
            };
            let sink = FilesystemSink::open(sink_cfg)
                .with_context(|| format!("opening sink for mirror {name}"))?;
            Ok(tokio::spawn(async move {
                tracing::info!(mirror = %name, "loop start");
                match run_mirror(source, sink, shutdown_signal(shutdown_rx)).await {
                    Ok(()) => Ok(()),
                    Err(e) => Err(anyhow::anyhow!("mirror {name}: {e}")),
                }
            }))
        }
        Destination::S3(s3) => {
            let mut builder = object_store::aws::AmazonS3Builder::from_env()
                .with_region(&s3.region)
                .with_bucket_name(&s3.bucket);
            if let Some(endpoint) = &s3.endpoint {
                builder = builder.with_endpoint(endpoint);
                if endpoint.starts_with("http://") {
                    builder = builder.with_allow_http(true);
                }
            }
            let store = builder
                .build()
                .with_context(|| format!("building S3 store for mirror {name}"))?;
            let sink_cfg = S3SinkConfig {
                store: Arc::new(store),
                prefix: s3.prefix.as_deref().map(object_store::path::Path::from),
                destination_name,
                partition: mirror.partition,
                flush: mirror_s3::FlushTriggers {
                    max_time: std::time::Duration::from_millis(s3.flush.max_time_ms),
                    max_bytes: s3.flush.max_bytes,
                    max_offsets: s3.flush.max_offsets,
                },
            };
            Ok(tokio::spawn(async move {
                tracing::info!(mirror = %name, "loop start");
                let sink = match S3Sink::open(sink_cfg).await {
                    Ok(s) => s,
                    Err(e) => return Err(anyhow::anyhow!("mirror {name} open S3 sink: {e}")),
                };
                match run_mirror(source, sink, shutdown_signal(shutdown_rx)).await {
                    Ok(()) => Ok(()),
                    Err(e) => Err(anyhow::anyhow!("mirror {name}: {e}")),
                }
            }))
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
