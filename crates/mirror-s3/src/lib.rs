//! S3-compatible blob sink: a [`BlobSink`] over an object-store prefix.
//!
//! ## Atomicity (two-layer)
//!
//! 1. **Preferred**: `PutMode::Create` (`If-None-Match: *`). On AWS S3
//!    this fails a second writer of the *same name* with 412.
//! 2. **Always**: single-writer-per-(topic,partition) by deployment +
//!    chain validation on startup. VersityGW ignores `PutMode::Create`,
//!    and writers with different flush boundaries write different
//!    names; both leave a chain that the next open refuses.
//!
//! ## Restart correctness
//!
//! On open, list every object under the prefix, parse `<from>-<to>.<ext>`
//! names, and validate the chain (see [`mirror_fs::blob::validate_chain`]).

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use mirror_core::{FlushObserver, Record, Sink, SinkError};
use mirror_envelope::{ColumnType, Format, ParquetCompression};
use mirror_fs::blob::{self, BlobError, BlobSink, BlobSpec, BlobStore};
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};

pub use mirror_fs::blob::{CompactionMode, FlushTriggers, UnixClock};

/// Errors from opening an S3 destination.
pub type S3Error = BlobError;

pub struct S3SinkConfig {
    pub store: Arc<dyn ObjectStore>,
    /// Path prefix inside the store: `<prefix>/<destination_name>/<partition>/`.
    pub prefix: Option<Path>,
    pub destination_name: String,
    pub partition: u32,
    pub format: Format,
    pub compression: ParquetCompression,
    /// Storage representation for the record `key`. Caller is
    /// responsible for pairing `Bytes` with `compaction = None`.
    pub keys: ColumnType,
    /// Storage representation for the record `value`.
    pub values: ColumnType,
    /// Optional log-compaction mode. Caller must combine `Some(Log)`
    /// with `Format::Parquet` and `keys` ∈ {`Utf8`, `Json`}.
    pub compaction: Option<CompactionMode>,
    pub flush: FlushTriggers,
}

/// One partition prefix in an object store.
pub struct S3Store {
    store: Arc<dyn ObjectStore>,
    partition_prefix: Path,
}

impl S3Store {
    pub fn new(store: Arc<dyn ObjectStore>, partition_prefix: Path) -> Self {
        Self {
            store,
            partition_prefix,
        }
    }

    fn path(&self, name: &str) -> Path {
        let mut parts: Vec<String> = self
            .partition_prefix
            .parts()
            .map(|p| p.as_ref().to_string())
            .collect();
        parts.push(name.to_string());
        Path::from_iter(parts)
    }
}

#[async_trait]
impl BlobStore for S3Store {
    async fn list(&self) -> Result<Vec<String>, BlobError> {
        let mut names = Vec::new();
        let mut stream = self.store.list(Some(&self.partition_prefix));
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|e| BlobError::Store(format!("object store: {e}")))?;
            if let Some(name) = meta.location.filename() {
                names.push(name.to_string());
            }
        }
        Ok(names)
    }

    async fn put_new(&self, name: &str, bytes: Vec<u8>) -> Result<(), BlobError> {
        let path = self.path(name);
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&path, PutPayload::from(Bytes::from(bytes)), opts)
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. }) => {
                Err(BlobError::AlreadyExists(path.to_string()))
            }
            Err(e) => Err(BlobError::Store(format!("put_opts {path}: {e}"))),
        }
    }

    async fn get(&self, name: &str) -> Result<Vec<u8>, BlobError> {
        let path = self.path(name);
        let got = self
            .store
            .get(&path)
            .await
            .map_err(|e| BlobError::Store(format!("get {path}: {e}")))?;
        let bytes = got
            .bytes()
            .await
            .map_err(|e| BlobError::Store(format!("read {path}: {e}")))?;
        Ok(bytes.to_vec())
    }

    fn location(&self, name: &str) -> String {
        self.path(name).to_string()
    }
}

/// The S3 destination.
pub struct S3Sink(BlobSink<S3Store>);

impl S3Sink {
    pub async fn open(cfg: S3SinkConfig) -> Result<Self, S3Error> {
        Self::open_with_clock(cfg, blob::system_unix_clock()).await
    }

    #[doc(hidden)]
    pub async fn open_with_clock(cfg: S3SinkConfig, clock: UnixClock) -> Result<Self, S3Error> {
        let partition_prefix =
            build_prefix(cfg.prefix.as_ref(), &cfg.destination_name, cfg.partition);
        let spec = BlobSpec {
            format: cfg.format,
            compression: cfg.compression,
            keys: cfg.keys,
            values: cfg.values,
            compaction: cfg.compaction,
            flush: cfg.flush,
        };
        let store = S3Store::new(cfg.store, partition_prefix);
        Ok(Self(BlobSink::open_store(store, spec, clock).await?))
    }

    pub async fn flush_now(&mut self) -> Result<(), SinkError> {
        self.0.flush_now().await
    }
}

#[async_trait]
impl Sink for S3Sink {
    async fn next_expected_offset(&mut self) -> Result<u64, SinkError> {
        self.0.next_expected_offset().await
    }

    async fn write(&mut self, record: Record) -> Result<(), SinkError> {
        self.0.write(record).await
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        self.0.flush().await
    }

    fn allows_compacted_source(&self) -> bool {
        self.0.allows_compacted_source()
    }

    async fn align_to_source_low_watermark(&mut self, low_watermark: u64) -> Result<(), SinkError> {
        self.0.align_to_source_low_watermark(low_watermark).await
    }

    fn set_flush_observer(&mut self, observer: Arc<dyn FlushObserver>) {
        self.0.set_flush_observer(observer)
    }

    fn supports_flush_observer(&self) -> bool {
        self.0.supports_flush_observer()
    }
}

fn build_prefix(root: Option<&Path>, destination_name: &str, partition: u32) -> Path {
    let mut parts: Vec<String> = Vec::new();
    if let Some(p) = root {
        for part in p.parts() {
            parts.push(part.as_ref().to_string());
        }
    }
    parts.push(destination_name.to_string());
    parts.push(partition.to_string());
    Path::from_iter(parts)
}
