use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

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
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err:?}");
            ExitCode::from(1)
        }
    }
}

fn run(cli: Cli) -> Result<()> {
    match cli.cmd {
        Cmd::Validate { config } => {
            let cfg = mirror_config::load_from_path(&config)
                .with_context(|| format!("loading {}", config.display()))?;
            println!(
                "OK: {} mirror(s), destination type = {}",
                cfg.mirrors.len(),
                destination_type(&cfg.destination)
            );
            Ok(())
        }
    }
}

fn destination_type(d: &mirror_config::Destination) -> &'static str {
    match d {
        mirror_config::Destination::Kafka(_) => "kafka",
        mirror_config::Destination::Filesystem(_) => "filesystem",
        mirror_config::Destination::S3(_) => "s3",
    }
}
