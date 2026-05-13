use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use mirror_config::{Destination, Mirror};
use mirror_core::run_mirror;
use mirror_fs::{FilesystemSink, FilesystemSinkConfig};
use mirror_kafka::{KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig};
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
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
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

async fn run(path: PathBuf) -> Result<()> {
    let cfg = mirror_config::load_from_path(&path)
        .with_context(|| format!("loading {}", path.display()))?;
    tracing::info!(
        config = %path.display(),
        mirrors = cfg.mirrors.len(),
        destination = destination_type(&cfg.destination),
        "starting mirror-v3"
    );

    let mut handles = Vec::with_capacity(cfg.mirrors.len());
    for mirror in &cfg.mirrors {
        let handle = spawn_mirror(mirror.clone(), cfg.destination.clone())?;
        handles.push((mirror.name.clone(), handle));
    }

    // Wait for the first task to terminate. Any termination — error or
    // (impossible) "Ok" — collapses the whole process.
    let (which, result) = wait_first(handles).await;
    tracing::error!(mirror = %which, "mirror task terminated; exiting process");
    result
}

fn spawn_mirror(
    mirror: Mirror,
    destination: Destination,
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
                match run_mirror(source, sink).await {
                    Ok(never) => match never {},
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
                match run_mirror(source, sink).await {
                    Ok(never) => match never {},
                    Err(e) => Err(anyhow::anyhow!("mirror {name}: {e}")),
                }
            }))
        }
        Destination::S3(_) => anyhow::bail!("s3 destination lands in Phase 4"),
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
