//! `mirror-v3 restore`: read one mirror's blob backup back into a Kafka
//! topic, or only verify that it is complete.
//!
//! The backup is named by the config the mirror runs with (`--mirror`,
//! and `--destination` when it has more than one): its directory,
//! format, encryption keys-dir and reading identity are the
//! destination's own. The verify pass always runs first and reads every
//! object, so a restore that cannot complete fails before it produces
//! anything.
//!
//! With `--follow` the backup is a mirror's source instead: the records
//! are produced through the Kafka destination, gate and all, from the
//! target's high watermark on, and objects the backup's mirror adds are
//! produced as they appear, until SIGTERM or SIGINT.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use mirror_config::{Destination, Encryption, Mirror};
use mirror_core::Sink;
use mirror_envelope::Keyring;
use mirror_fs::blob::BlobStore;
use mirror_kafka::{KafkaSink, KafkaSinkConfig, RestoreProducer, TimestampMode};
use mirror_restore::{
    plan_chain, BackupSource, BackupSummary, ChainSource, ChainSourceConfig, OffsetMode, Reader,
};

/// `--offsets`: required, so that the choice is written out.
#[derive(Copy, Clone, Debug, clap::ValueEnum)]
pub enum OffsetsArg {
    /// Every record at its original offset; the backup must start at 0
    /// and have no offset holes.
    Preserve,
    /// Records at 0, 1, 2, ... in their original order; holes close up.
    Renumber,
}

impl From<OffsetsArg> for OffsetMode {
    fn from(a: OffsetsArg) -> Self {
        match a {
            OffsetsArg::Preserve => OffsetMode::Preserve,
            OffsetsArg::Renumber => OffsetMode::Renumber,
        }
    }
}

pub struct RestoreArgs {
    pub config: PathBuf,
    pub mirror: String,
    pub destination: Option<String>,
    pub offsets: OffsetsArg,
    pub chain_start: u64,
    /// `None`: verify only.
    pub target: Option<Target>,
    /// `Some(poll interval)`: follow the backup into the target.
    pub follow: Option<Duration>,
}

pub struct Target {
    pub bootstrap_servers: String,
    pub topic: String,
}

pub async fn run_restore(args: RestoreArgs) -> Result<()> {
    let cfg = mirror_config::load_from_path(&args.config)
        .with_context(|| format!("loading {}", args.config.display()))?;
    let mirror = cfg
        .mirrors
        .iter()
        .find(|m| m.name == args.mirror)
        .ok_or_else(|| {
            let names: Vec<&str> = cfg.mirrors.iter().map(|m| m.name.as_str()).collect();
            anyhow!(
                "{} has no mirror {:?} (it has {names:?})",
                args.config.display(),
                args.mirror
            )
        })?;
    if mirror.compaction.is_some() {
        bail!(
            "mirror {:?} has `compaction: log`: its objects are snapshots of the latest value \
             per key, not the topic's records; restore reads append-mode backups",
            mirror.name
        );
    }
    let destination = pick_destination(mirror, args.destination.as_deref())?;
    let dest_name = destination.effective_name(&mirror.name);
    let params = super::resolve_blob_params(mirror)?;
    let source = BackupSource {
        topic: mirror.topic.clone(),
        partition: i32::try_from(mirror.partition)
            .with_context(|| format!("mirror {:?}: partition", mirror.name))?,
    };
    let backup = Backup {
        mirror,
        dest_name: &dest_name,
        format: params.format,
        chain_start: args.chain_start,
        mode: args.offsets.into(),
        source: &source,
    };
    match destination {
        Destination::Filesystem(fs) => {
            let dir = mirror_fs::naming::partition_dir(&fs.root, &dest_name, mirror.partition);
            // Followed, a backup whose mirror has written nothing yet is waited for.
            if args.follow.is_none() && !dir.is_dir() {
                bail!("the backup directory {} does not exist", dir.display());
            }
            let location = dir.display().to_string();
            let store = mirror_fs::FsStore::new(dir);
            backup
                .dispatch(store, &location, None, args.target, args.follow)
                .await
        }
        Destination::S3(s3) => {
            let read = super::s3_store(s3, &s3.credentials.read)?;
            let root = s3.prefix.as_deref().map(object_store::path::Path::from);
            let prefix = mirror_s3::partition_prefix(root.as_ref(), &dest_name, mirror.partition);
            let location = format!("s3://{}/{prefix}", s3.bucket);
            let keyring = match &s3.encryption {
                Encryption::None(_) => None,
                Encryption::ParquetKeys(k) => Some(
                    Keyring::load(&k.keys_dir)
                        .with_context(|| format!("S3 destination bucket {}", s3.bucket))?,
                ),
            };
            let store = mirror_s3::S3Store::read_only(read, prefix);
            backup
                .dispatch(store, &location, keyring, args.target, args.follow)
                .await
        }
        Destination::Kafka(_) => unreachable!("pick_destination returns blob destinations"),
    }
}

/// The blob destination to read: the one named, or the mirror's only
/// destination.
fn pick_destination<'a>(mirror: &'a Mirror, name: Option<&str>) -> Result<&'a Destination> {
    let dest = match name {
        Some(name) => mirror
            .destinations
            .iter()
            .find(|d| d.effective_name(&mirror.name) == name)
            .ok_or_else(|| {
                anyhow!(
                    "mirror {:?} has no destination {name:?} (it has {:?})",
                    mirror.name,
                    destination_names(mirror)
                )
            })?,
        None => match mirror.destinations.as_slice() {
            [only] => only,
            [] => bail!("mirror {:?} has no destinations", mirror.name),
            _ => bail!(
                "mirror {:?} has several destinations ({:?}); name one with --destination",
                mirror.name,
                destination_names(mirror)
            ),
        },
    };
    if !dest.is_blob() {
        bail!(
            "destination {:?} of mirror {:?} is a Kafka topic; restore reads filesystem and S3 \
             backups",
            dest.effective_name(&mirror.name),
            mirror.name
        );
    }
    Ok(dest)
}

fn destination_names(mirror: &Mirror) -> Vec<String> {
    mirror
        .destinations
        .iter()
        .map(|d| d.effective_name(&mirror.name))
        .collect()
}

struct Backup<'a> {
    mirror: &'a Mirror,
    dest_name: &'a str,
    format: mirror_envelope::Format,
    chain_start: u64,
    mode: OffsetMode,
    source: &'a BackupSource,
}

impl Backup<'_> {
    async fn dispatch<S: BlobStore + Send + Sync + 'static>(
        &self,
        store: S,
        location: &str,
        keyring: Option<Keyring>,
        target: Option<Target>,
        follow: Option<Duration>,
    ) -> Result<()> {
        match (follow, target) {
            (Some(poll_interval), Some(target)) => {
                self.follow(
                    Arc::new(store),
                    location,
                    keyring.map(Arc::new),
                    target,
                    poll_interval,
                )
                .await
            }
            (Some(_), None) => unreachable!("clap requires --topic with --follow"),
            (None, target) => self.run(&store, location, keyring.as_ref(), target).await,
        }
    }

    async fn follow<S: BlobStore + Send + Sync + 'static>(
        &self,
        store: Arc<S>,
        location: &str,
        keyring: Option<Arc<Keyring>>,
        target: Target,
        poll_interval: Duration,
    ) -> Result<()> {
        let mut source = ChainSource::open(ChainSourceConfig {
            store,
            format: self.format,
            keyring,
            source: self.source.clone(),
            mode: self.mode,
            chain_start: self.chain_start,
            poll_interval,
        })
        .await
        .with_context(|| location.to_string())?;
        let mut sink_cfg = KafkaSinkConfig::new(
            target.bootstrap_servers.clone(),
            target.topic.clone(),
            self.source.partition,
        );
        sink_cfg.timestamp_mode = TimestampMode::Source;
        sink_cfg.keys = super::column_type_to_envelope(self.mirror.keys.unwrap_or_default().kind);
        sink_cfg.values =
            super::column_type_to_envelope(self.mirror.values.unwrap_or_default().kind);
        let mut sink = KafkaSink::open(sink_cfg).context("opening the target topic's producer")?;
        let at = sink
            .next_expected_offset()
            .await
            .context("reading the target's high watermark")?;
        if at > 0 {
            self.check_resume(&mut source, &target, at).await?;
        }
        println!(
            "following: {location} into {}/{} from offset {at}",
            target.topic, self.source.partition
        );
        mirror_core::run_mirror(source, sink, shutdown_requested())
            .await
            .with_context(|| {
                format!(
                    "following into {}/{} at {}",
                    target.topic, self.source.partition, target.bootstrap_servers
                )
            })?;
        println!("stopped");
        Ok(())
    }

    /// A target that is not empty is resumed only if its last record is
    /// the backup's record at that position: the target holds this
    /// backup's first records, not another topic's.
    async fn check_resume<S: BlobStore + Send + Sync + 'static>(
        &self,
        source: &mut ChainSource<S>,
        target: &Target,
        at: u64,
    ) -> Result<()> {
        let last = at - 1;
        let Some(ours) = source.record_at(last).await? else {
            bail!(
                "the target holds {at} records, and the backup has no record at offset {last}: \
                 it holds more than this backup, or is another topic"
            );
        };
        let (bootstrap, topic, partition) = (
            target.bootstrap_servers.clone(),
            target.topic.clone(),
            self.source.partition,
        );
        let theirs = tokio::task::spawn_blocking(move || {
            mirror_kafka::read_record_at(
                &bootstrap,
                &topic,
                partition,
                last,
                Duration::from_secs(10),
            )
        })
        .await?
        .context("reading the target's last record")?;
        let same_time = ours.timestamp_ms.is_none() || ours.timestamp_ms == theirs.timestamp_ms;
        if ours.key != theirs.key
            || ours.value != theirs.value
            || ours.headers != theirs.headers
            || !same_time
        {
            bail!(
                "the target's record at offset {last} is not the backup's record there (key, \
                 value, headers or timestamp differ): the target holds another topic's records; \
                 follow into an empty topic, or one this backup was restored into with the same \
                 --offsets"
            );
        }
        tracing::info!(
            offset = last,
            "the target's last record is the backup's; resuming"
        );
        Ok(())
    }

    async fn run<S: BlobStore>(
        &self,
        store: &S,
        location: &str,
        keyring: Option<&Keyring>,
        target: Option<Target>,
    ) -> Result<()> {
        let names = store
            .list()
            .await
            .with_context(|| format!("listing {location}"))?;
        let chain = plan_chain(&names, self.format, self.chain_start, keyring)
            .with_context(|| location.to_string())?;
        tracing::info!(location, objects = chain.len(), "verifying every object");
        let reader = Reader {
            store,
            format: self.format,
            keyring,
            source: self.source,
        };
        let summary = reader.verify(&chain).await?;
        self.print_summary(location, &summary);
        self.mode.check(&summary)?;
        let Some(target) = target else {
            println!("verified: the backup can be restored with these offsets");
            return Ok(());
        };
        let mut producer = RestoreProducer::open(
            target.bootstrap_servers.clone(),
            target.topic.clone(),
            self.source.partition,
            super::column_type_to_envelope(self.mirror.keys.unwrap_or_default().kind),
            super::column_type_to_envelope(self.mirror.values.unwrap_or_default().kind),
        )
        .context("opening the target topic's producer")?;
        tracing::info!(
            topic = %target.topic,
            partition = self.source.partition,
            mode = ?self.mode,
            records = summary.records,
            "producing"
        );
        let report = mirror_restore::produce(&reader, &chain, &summary, self.mode, &mut producer)
            .await
            .with_context(|| {
                format!(
                    "restoring into {}/{} at {}",
                    target.topic, self.source.partition, target.bootstrap_servers
                )
            })?;
        println!(
            "restored: {} records to {}/{}, high watermark {}",
            report.records, target.topic, self.source.partition, report.high_watermark
        );
        Ok(())
    }

    fn print_summary(&self, location: &str, summary: &BackupSummary) {
        println!(
            "backup: {location} (mirror {}, destination {})",
            self.mirror.name, self.dest_name
        );
        println!("source: {}/{}", self.source.topic, self.source.partition);
        println!("objects: {}", summary.objects.len());
        println!("offsets: {}-{}", summary.first_offset, summary.last_offset);
        println!("records: {}", summary.records);
        match summary.first_hole {
            None => println!("holes: 0"),
            Some(first) => println!("holes: {}, the first at offset {first}", summary.holes),
        }
    }
}

/// SIGTERM or SIGINT: the loop then stops after the record in hand.
async fn shutdown_requested() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut term) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
            tracing::info!("shutdown requested");
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown requested");
}
