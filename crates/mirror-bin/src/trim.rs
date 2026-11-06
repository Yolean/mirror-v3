//! `mirror-v3 trim`: expire the head of every blob destination that has a
//! `retention`, or move one destination's floor by hand (`--floor`).
//!
//! One-shot, for a schedule (a CronJob). Each destination is trimmed
//! with the identity that may delete (`credentials.trim` for S3), and
//! whether one fails or not the others are trimmed too. The exit code is
//! non-zero when a destination failed or its expiry is blocked; objects
//! that did not delete (a retention lock) only warn, since the floor
//! already hides them and the next trim tries again.

use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use mirror_config::{Destination, Mirror};
use mirror_fs::floor::utc_rfc3339;
use mirror_restore::{trim, FloorTarget, Stop, TrimConfig, TrimReport};

pub struct TrimArgs {
    pub config: PathBuf,
    pub mirror: Option<String>,
    pub destination: Option<String>,
    pub floor: Option<u64>,
    pub dry_run: bool,
}

/// Whether every destination was trimmed and none is blocked.
pub async fn run_trim(args: TrimArgs) -> Result<bool> {
    let cfg = mirror_config::load_from_path(&args.config)
        .with_context(|| format!("loading {}", args.config.display()))?;
    let targets = pick_targets(&cfg.mirrors, &args)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("the clock is before 1970")?
        .as_secs();
    let mut all_ok = true;
    for (mirror, destination) in targets {
        let dest_name = destination.effective_name(&mirror.name);
        let label = if dest_name == mirror.name {
            mirror.name.clone()
        } else {
            format!("{}.{dest_name}", mirror.name)
        };
        match trim_one(
            mirror,
            destination,
            &dest_name,
            args.floor,
            now,
            args.dry_run,
        )
        .await
        {
            Ok((location, target, report)) => {
                print_report(&label, &location, target, &report, args.dry_run);
                if let Some((object, reason)) = report.blocked() {
                    tracing::error!(
                        destination = %label,
                        object,
                        reason,
                        "the expiry is blocked: records past their retention are kept until a \
                         person looks"
                    );
                    all_ok = false;
                }
            }
            Err(e) => {
                println!("trim {label}: error: {e:#}");
                tracing::error!(destination = %label, error = %format!("{e:#}"), "trim failed");
                all_ok = false;
            }
        }
    }
    Ok(all_ok)
}

/// `--floor`: the one destination named. Otherwise every blob
/// destination with a `retention`, of the mirror named or of all of
/// them, disabled mirrors included (their archives expire too).
fn pick_targets<'a>(
    mirrors: &'a [Mirror],
    args: &TrimArgs,
) -> Result<Vec<(&'a Mirror, &'a Destination)>> {
    let named = match &args.mirror {
        Some(name) => Some(mirrors.iter().find(|m| &m.name == name).ok_or_else(|| {
            let names: Vec<&str> = mirrors.iter().map(|m| m.name.as_str()).collect();
            anyhow!(
                "{} has no mirror {name:?} (it has {names:?})",
                args.config.display()
            )
        })?),
        None => None,
    };
    if args.floor.is_some() {
        let mirror = named.expect("clap requires --mirror with --floor");
        let destination = super::restore::pick_destination(mirror, args.destination.as_deref())?;
        if mirror.compaction.is_some() {
            bail!(
                "mirror {:?} has `compaction: log`: its chain allows gaps and has no floor",
                mirror.name
            );
        }
        return Ok(vec![(mirror, destination)]);
    }
    let mut targets = Vec::new();
    for mirror in named.map_or(mirrors, std::slice::from_ref) {
        for d in &mirror.destinations {
            let wanted = args
                .destination
                .as_deref()
                .is_none_or(|n| d.effective_name(&mirror.name) == n);
            if wanted && d.retention().is_some() {
                targets.push((mirror, d));
            }
        }
    }
    if targets.is_empty() {
        bail!(match (&args.mirror, &args.destination) {
            (Some(m), Some(d)) => format!(
                "destination {d:?} of mirror {m:?} has no `retention` (or no such destination); \
                 pass --floor to move its floor by hand"
            ),
            (Some(m), None) => format!("mirror {m:?} has no destination with a `retention`"),
            _ => format!(
                "{} has no destination with a `retention`",
                args.config.display()
            ),
        });
    }
    Ok(targets)
}

async fn trim_one(
    mirror: &Mirror,
    destination: &Destination,
    dest_name: &str,
    floor: Option<u64>,
    now: u64,
    dry_run: bool,
) -> Result<(String, FloorTarget, TrimReport)> {
    let format = super::resolve_blob_params(mirror)?.format;
    let target = match (floor, destination.retention()) {
        (Some(offset), _) => FloorTarget::Offset(offset),
        (None, Some(r)) => FloorTarget::Cutoff {
            cutoff_ms: now
                .saturating_sub(u64::from(r.max_age_days) * 86_400)
                .saturating_mul(1000),
        },
        (None, None) => unreachable!("pick_targets returns destinations with a retention"),
    };
    let run = |location: String| move |report| (location, target, report);
    match destination {
        Destination::Filesystem(fs) => {
            let dir = mirror_fs::naming::partition_dir(&fs.root, dest_name, mirror.partition);
            if !dir.is_dir() {
                bail!("the destination directory {} does not exist", dir.display());
            }
            let location = dir.display().to_string();
            let store = mirror_fs::FsStore::new(dir);
            let report = trim(TrimConfig {
                store: &store,
                format,
                target,
                now,
                dry_run,
            })
            .await
            .with_context(|| location.clone())?;
            Ok(run(location)(report))
        }
        Destination::S3(s3) => {
            let key = s3.credentials.trim.as_ref().ok_or_else(|| {
                anyhow!(
                    "S3 destination bucket {} has no `credentials.trim`, the identity that \
                     lists, HEADs, writes floor markers and deletes",
                    s3.bucket
                )
            })?;
            let identity = super::s3_store(s3, key)?;
            let root = s3.prefix.as_deref().map(object_store::path::Path::from);
            let prefix = mirror_s3::partition_prefix(root.as_ref(), dest_name, mirror.partition);
            let location = format!("s3://{}/{prefix}", s3.bucket);
            let store = mirror_s3::S3Store::new(identity.clone(), identity, prefix);
            let report = trim(TrimConfig {
                store: &store,
                format,
                target,
                now,
                dry_run,
            })
            .await
            .with_context(|| location.clone())?;
            Ok(run(location)(report))
        }
        Destination::Kafka(_) => unreachable!("trim reads blob destinations"),
    }
}

fn print_report(
    label: &str,
    location: &str,
    target: FloorTarget,
    report: &TrimReport,
    dry_run: bool,
) {
    let (would, deleted) = if dry_run {
        ("dry run, nothing changed: ", "would delete")
    } else {
        ("", "deleted")
    };
    println!("trim {label}: {would}{location}");
    let previous = report.previous.map_or(0, |f| f.offset);
    match &report.marker {
        Some(marker) if report.floor != previous || report.previous.is_none() => {
            println!("  floor: {previous} -> {} ({marker})", report.floor)
        }
        Some(marker) => println!("  floor: {} ({marker})", report.floor),
        None => println!("  floor: {} (unchanged)", report.floor),
    }
    match target {
        FloorTarget::Cutoff { cutoff_ms } => {
            println!("  cutoff: {}", utc_rfc3339(cutoff_ms / 1000))
        }
        FloorTarget::Offset(_) => println!("  set by: --floor"),
    }
    println!("  chain end: {}", report.chain_end);
    if let Some(ms) = report.oldest_ms {
        println!(
            "  oldest record above the floor: {}",
            utc_rfc3339(ms / 1000)
        );
    }
    println!("  {deleted}: {} object(s)", report.deleted.len());
    if !report.failed.is_empty() {
        println!(
            "  not deleted: {} object(s), below the floor and ignored; the next trim tries again",
            report.failed.len()
        );
        for (name, error) in &report.failed {
            println!("    {name}: {error}");
        }
    }
    match &report.stop {
        Stop::Given => {}
        Stop::End => println!("  stopped: no object is left at or above the floor"),
        Stop::Newer { object } => {
            println!("  stopped: {object} holds records newer than the cutoff")
        }
        Stop::Blocked { object, reason } => println!("  BLOCKED at {object}: {reason}"),
    }
}
