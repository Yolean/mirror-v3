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
            let (location, store) = s3_trim_store(s3, dest_name, mirror.partition)?;
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

/// The partition prefix, reached with the trim identity. Nothing here or
/// in [`trim`] reads the `encryption` block: record times are object
/// metadata, so the trim job needs no Parquet key and mounts none.
fn s3_trim_store(
    s3: &mirror_config::S3Destination,
    dest_name: &str,
    partition: u32,
) -> Result<(String, mirror_s3::S3Store)> {
    let key = s3.credentials.trim.as_ref().ok_or_else(|| {
        anyhow!(
            "S3 destination bucket {} has no `credentials.trim`, the identity that lists, \
             HEADs, writes floor markers and deletes",
            s3.bucket
        )
    })?;
    let identity = super::s3_store(s3, key)?;
    let root = s3.prefix.as_deref().map(object_store::path::Path::from);
    let prefix = mirror_s3::partition_prefix(root.as_ref(), dest_name, partition);
    let location = format!("s3://{}/{prefix}", s3.bucket);
    Ok((
        location,
        mirror_s3::S3Store::new(identity.clone(), identity, prefix),
    ))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The config `run` uses, keys-dir and all, on a trim job's pod,
    /// where no key is mounted.
    fn encrypted_with_keys_dir(keys_dir: &std::path::Path) -> mirror_config::Config {
        mirror_config::load_from_str(&format!(
            r#"
mirrors:
  - name: audit-backup
    source: {{ bootstrap-servers: "localhost:1" }}
    topic: audit
    partition: 0
    destinations:
      - type: s3
        name: audit
        region: example-region
        bucket: b
        prefix: sites/x
        credentials:
          write: {{ access-key-id-env: TRIM_TEST_W_ID, secret-access-key-env: TRIM_TEST_W_SECRET }}
          read: {{ access-key-id-env: TRIM_TEST_R_ID, secret-access-key-env: TRIM_TEST_R_SECRET }}
          trim: {{ access-key-id-env: TRIM_TEST_T_ID, secret-access-key-env: TRIM_TEST_T_SECRET }}
        encryption: {{ keys-dir: {}, key-id: k1 }}
        retention: {{ max-age-days: 400 }}
    format: parquet
    flush: {{ max-time-ms: 60000, max-bytes: 1048576, max-offsets: 10 }}
"#,
            keys_dir.display()
        ))
        .expect("the config loads without its keys")
    }

    #[test]
    fn trim_needs_no_parquet_key() {
        // Only the trim identity's variables are set: run's are not
        // needed either.
        std::env::set_var("TRIM_TEST_T_ID", "id");
        std::env::set_var("TRIM_TEST_T_SECRET", "secret");
        let empty = tempfile::tempdir().unwrap();
        let absent = empty.path().join("not-mounted");
        for keys_dir in [absent.as_path(), empty.path()] {
            let cfg = encrypted_with_keys_dir(keys_dir);
            let mirror = &cfg.mirrors[0];
            let args = TrimArgs {
                config: "mirror-v3.yaml".into(),
                mirror: None,
                destination: None,
                floor: None,
                dry_run: false,
            };
            let targets = pick_targets(&cfg.mirrors, &args).unwrap();
            assert_eq!(targets.len(), 1);
            let Destination::S3(s3) = targets[0].1 else {
                panic!("an S3 destination")
            };
            let (location, _store) = s3_trim_store(s3, "audit", mirror.partition)
                .unwrap_or_else(|e| panic!("keys-dir {}: {e:#}", keys_dir.display()));
            assert_eq!(location, "s3://b/sites/x/audit/0");
            super::super::resolve_blob_params(mirror).unwrap();
        }
    }
}
