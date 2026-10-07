//! A backup read as a mirror's source: continuous restore.
//!
//! [`ChainSource`] yields a backup's records in order, and keeps
//! listing the backup for objects the mirror that writes it adds, so
//! `run_mirror` with a Kafka destination keeps a topic restored while
//! the backup grows: a standby, or a topic moved between clusters
//! through a bucket. The destination's gate (the high watermark read
//! before every produce, no retries) is what makes this resumable: the
//! target's high watermark is where the next run continues, in both
//! offset modes, since the target holds exactly the backup's first
//! records in order.
//!
//! Positions are the target's offsets. With [`OffsetMode::Preserve`]
//! they are the records' own offsets, and a hole in the backup is an
//! error when it is reached; with [`OffsetMode::Renumber`] the n-th
//! record of the chain (from 0) goes to offset n.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use mirror_core::{Record, Source, SourceError};
use mirror_envelope::{Format, Keyring};
use mirror_fs::blob::BlobStore;

use crate::{plan_chain, BackupSource, ChainObject, OffsetMode, Reader, RestoreError};

type PendingRead = Pin<Box<dyn Future<Output = Result<Vec<Record>, RestoreError>> + Send>>;

/// How to read a backup as a source.
pub struct ChainSourceConfig<S> {
    pub store: Arc<S>,
    pub format: Format,
    /// `None` for a destination with `encryption: none`.
    pub keyring: Option<Arc<Keyring>>,
    pub source: BackupSource,
    pub mode: OffsetMode,
    /// The offset the backup starts at; 0 unless objects before it were
    /// removed on purpose.
    pub chain_start: u64,
    /// How long to wait between listings once every object is read.
    pub poll_interval: Duration,
}

pub struct ChainSource<S> {
    cfg: ChainSourceConfig<S>,
    /// The objects listed so far, a validated chain.
    chain: Vec<ChainObject>,
    /// Records of `chain[i]`, for the objects counted so far
    /// (renumber only).
    counts: Vec<u64>,
    /// The object read next.
    next_object: usize,
    /// Records at the head of `next_object` that are already in the
    /// target (renumber, after a seek into the object).
    skip: u64,
    /// A read of `next_object` that a cancelled `poll_one` started: the
    /// run loop drops `poll_one` on every heartbeat, and a large object
    /// may take longer than one to read.
    pending: Option<PendingRead>,
    buffer: VecDeque<Record>,
    /// The target position of the next record yielded.
    position: u64,
}

impl<S: BlobStore + Send + Sync + 'static> ChainSource<S> {
    /// List the backup and validate its chain. An empty backup is not an
    /// error: its first object is waited for.
    pub async fn open(cfg: ChainSourceConfig<S>) -> Result<Self, RestoreError> {
        if cfg.mode == OffsetMode::Preserve && cfg.chain_start != 0 {
            return Err(RestoreError::Mode(format!(
                "--offsets=preserve: the backup starts at offset {}, and a Kafka topic starts \
                 at 0",
                cfg.chain_start
            )));
        }
        let mut source = Self {
            cfg,
            chain: Vec::new(),
            counts: Vec::new(),
            next_object: 0,
            skip: 0,
            pending: None,
            buffer: VecDeque::new(),
            position: 0,
        };
        source.list_new().await?;
        Ok(source)
    }

    /// The objects of the chain listed so far.
    pub fn chain(&self) -> &[ChainObject] {
        &self.chain
    }

    /// The record the target holds at `position` if it is this backup,
    /// as it is produced there (renumber: with `position` as offset);
    /// `None` past the end of the backup.
    pub async fn record_at(&mut self, position: u64) -> Result<Option<Record>, RestoreError> {
        let found = match self.cfg.mode {
            OffsetMode::Preserve => self
                .chain
                .iter()
                .position(|o| o.from <= position && position <= o.to)
                .map(|i| (i, None)),
            OffsetMode::Renumber => {
                let mut first = 0;
                let mut found = None;
                for i in 0..self.chain.len() {
                    let n = self.count(i).await?;
                    if position < first + n {
                        found = Some((i, Some(position - first)));
                        break;
                    }
                    first += n;
                }
                found
            }
        };
        let Some((i, index)) = found else {
            return Ok(None);
        };
        let records = self.read(i).await?;
        Ok(match index {
            None => records.into_iter().find(|r| r.source_offset == position),
            Some(k) => records.into_iter().nth(k as usize).map(|r| Record {
                source_offset: position,
                ..r
            }),
        })
    }

    /// Append the objects written since the last listing; whether there
    /// were any.
    async fn list_new(&mut self) -> Result<bool, RestoreError> {
        let after = self.chain.last().map(|o| o.name.clone());
        let names = self
            .cfg
            .store
            .list_after(after.as_deref())
            .await
            .map_err(|e| RestoreError::Store(format!("listing the backup: {e}")))?;
        if names.is_empty() {
            return Ok(false);
        }
        let start = self.chain.last().map_or(self.cfg.chain_start, |o| o.to + 1);
        if !self.chain.is_empty() {
            let ext = self.cfg.format.extension();
            let first = names
                .iter()
                .filter_map(|n| mirror_fs::naming::parse_blob_name(n, ext))
                .map(|b| b.from)
                .min();
            if let Some(first) = first.filter(|&f| f > start) {
                return Err(RestoreError::Chain(format!(
                    "gap in the chain: offsets {start}-{} are in no object",
                    first - 1
                )));
            }
        }
        let new = plan_chain(&names, self.cfg.format, start, self.cfg.keyring.as_deref())?;
        tracing::info!(
            objects = new.len(),
            to = new.last().map(|o| o.to),
            "new backup objects"
        );
        self.chain.extend(new);
        Ok(true)
    }

    fn read(&self, i: usize) -> PendingRead {
        let store = Arc::clone(&self.cfg.store);
        let keyring = self.cfg.keyring.clone();
        let source = self.cfg.source.clone();
        let format = self.cfg.format;
        let object = self.chain[i].clone();
        Box::pin(async move {
            Reader {
                store: store.as_ref(),
                format,
                keyring: keyring.as_deref(),
                source: &source,
            }
            .read_object(&object)
            .await
        })
    }

    /// Records of `chain[i]`, reading the objects not counted yet.
    async fn count(&mut self, i: usize) -> Result<u64, RestoreError> {
        while self.counts.len() <= i {
            let n = self.read(self.counts.len()).await?.len() as u64;
            self.counts.push(n);
        }
        Ok(self.counts[i])
    }

    /// The position after the last record of the chain listed so far.
    async fn end(&mut self) -> Result<u64, RestoreError> {
        match self.cfg.mode {
            OffsetMode::Preserve => Ok(self.chain.last().map_or(0, |o| o.to + 1)),
            OffsetMode::Renumber => {
                let mut total = 0;
                for i in 0..self.chain.len() {
                    total += self.count(i).await?;
                }
                Ok(total)
            }
        }
    }

    /// Read `next_object` into the buffer.
    async fn fill(&mut self) -> Result<(), RestoreError> {
        let i = self.next_object;
        if self.pending.is_none() {
            self.pending = Some(self.read(i));
        }
        let records = self.pending.as_mut().expect("set above").await;
        self.pending = None;
        let records = records?;
        if self.counts.len() == i {
            self.counts.push(records.len() as u64);
        }
        let object = &self.chain[i];
        match self.cfg.mode {
            OffsetMode::Preserve => {
                let mut expected = self.position;
                for r in records {
                    if r.source_offset < expected {
                        continue;
                    }
                    if r.source_offset != expected {
                        return Err(RestoreError::Mode(format!(
                            "--offsets=preserve: offset {expected} is a hole in the backup \
                             ({}: records removed by compaction, or a transaction marker), and \
                             a Kafka topic cannot be written with holes",
                            object.name
                        )));
                    }
                    self.buffer.push_back(r);
                    expected += 1;
                }
            }
            OffsetMode::Renumber => {
                let skip = std::mem::take(&mut self.skip) as usize;
                for (k, r) in records.into_iter().skip(skip).enumerate() {
                    self.buffer.push_back(Record {
                        source_offset: self.position + k as u64,
                        ..r
                    });
                }
            }
        }
        self.next_object += 1;
        Ok(())
    }
}

/// A store that cannot be reached can be tried again; anything else
/// about the backup cannot be fixed by trying again.
fn source_error(e: RestoreError) -> SourceError {
    match e {
        RestoreError::Store(m) => SourceError::Transport(format!("backup store: {m}")),
        other => SourceError::Inconsistent(other.to_string()),
    }
}

// async_trait marks the boxed future of each method #[must_use];
// clippy 1.99 flags that as double_must_use in the expansion.
#[allow(clippy::double_must_use)]
#[async_trait]
impl<S: BlobStore + Send + Sync + 'static> Source for ChainSource<S> {
    async fn seek(&mut self, position: u64) -> Result<(), SourceError> {
        self.list_new().await.map_err(source_error)?;
        let end = self.end().await.map_err(source_error)?;
        if position > end {
            return Err(SourceError::Inconsistent(format!(
                "the target is at offset {position}, past the backup's end at {end}"
            )));
        }
        self.pending = None;
        self.buffer.clear();
        self.skip = 0;
        self.position = position;
        self.next_object = match self.cfg.mode {
            // The object's records below `position` are dropped when it is read.
            OffsetMode::Preserve => self
                .chain
                .iter()
                .position(|o| o.to >= position)
                .unwrap_or(self.chain.len()),
            OffsetMode::Renumber => {
                let mut first = 0;
                let mut i = 0;
                while i < self.chain.len() {
                    let n = self.count(i).await.map_err(source_error)?;
                    if position < first + n {
                        self.skip = position - first;
                        break;
                    }
                    first += n;
                    i += 1;
                }
                i
            }
        };
        Ok(())
    }

    async fn poll_one(&mut self) -> Result<Option<Record>, SourceError> {
        loop {
            if let Some(r) = self.buffer.pop_front() {
                self.position += 1;
                return Ok(Some(r));
            }
            if self.next_object < self.chain.len() {
                self.fill().await.map_err(source_error)?;
                continue;
            }
            if self.list_new().await.map_err(source_error)? {
                continue;
            }
            tokio::time::sleep(self.cfg.poll_interval).await;
            return Ok(None);
        }
    }

    /// The position after the backup's last record: the run loop
    /// refuses a target that is further.
    async fn high_watermark(&mut self) -> Result<u64, SourceError> {
        self.list_new().await.map_err(source_error)?;
        self.end().await.map_err(source_error)
    }
}
