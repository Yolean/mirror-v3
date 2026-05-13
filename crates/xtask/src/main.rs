use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(about = "Repo automation for mirror-v3 (schema generation and CI gates)")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Regenerate schemas/mirror-v3.config.schema.json.
    GenSchema,
    /// Fail if the committed schema does not match the structs.
    CheckSchema,
}

fn schema_path() -> PathBuf {
    // CARGO_MANIFEST_DIR is crates/xtask; schema is at workspace root.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("schemas")
        .join("mirror-v3.config.schema.json")
}

fn render_schema() -> Result<String> {
    let schema = mirror_config::schema();
    let mut s = serde_json::to_string_pretty(&schema)?;
    s.push('\n');
    Ok(s)
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::GenSchema => {
            let path = schema_path();
            let rendered = render_schema()?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("creating {}", parent.display()))?;
            }
            std::fs::write(&path, &rendered)
                .with_context(|| format!("writing {}", path.display()))?;
            println!("wrote {}", path.display());
        }
        Cmd::CheckSchema => {
            let path = schema_path();
            let on_disk = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            let rendered = render_schema()?;
            if on_disk != rendered {
                eprintln!("committed schema is stale; run: cargo run -p xtask -- gen-schema");
                std::process::exit(1);
            }
            println!("schema OK ({})", path.display());
        }
    }
    Ok(())
}
