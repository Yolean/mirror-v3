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

pub use mirror_fs::blob::{BlobEncryption, CompactionMode, FlushTriggers, UnixClock};

/// Errors from opening an S3 destination.
pub type S3Error = BlobError;

pub struct S3SinkConfig {
    /// Lists the prefix (and reads the latest snapshot in compaction
    /// mode). A least-privilege reading identity may list and read only.
    pub read_store: Arc<dyn ObjectStore>,
    /// Writes objects. A least-privilege writing identity may only PutObject.
    pub write_store: Arc<dyn ObjectStore>,
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
    /// Explicit per destination: `None` writes blobs in clear.
    pub encryption: Option<BlobEncryption>,
}

/// One partition prefix in an object store, reached with two identities:
/// one that lists and reads, one that writes (none for a restore, which
/// only reads).
pub struct S3Store {
    read: Arc<dyn ObjectStore>,
    write: Option<Arc<dyn ObjectStore>>,
    partition_prefix: Path,
}

impl S3Store {
    pub fn new(
        read: Arc<dyn ObjectStore>,
        write: Arc<dyn ObjectStore>,
        partition_prefix: Path,
    ) -> Self {
        Self {
            read,
            write: Some(write),
            partition_prefix,
        }
    }

    /// A store that lists and reads only: a write is an error.
    pub fn read_only(read: Arc<dyn ObjectStore>, partition_prefix: Path) -> Self {
        Self {
            read,
            write: None,
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
        let mut stream = self.read.list(Some(&self.partition_prefix));
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|e| BlobError::Store(format!("object store: {e}")))?;
            if let Some(name) = name_in_directory(&self.partition_prefix, &meta.location) {
                names.push(name);
            }
        }
        Ok(names)
    }

    async fn put_new(&self, name: &str, bytes: Vec<u8>) -> Result<(), BlobError> {
        let path = self.path(name);
        let Some(write) = &self.write else {
            return Err(BlobError::Store(format!(
                "put {path}: this store was opened to read only"
            )));
        };
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match write
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
            .read
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

    async fn list_after(&self, after: Option<&str>) -> Result<Vec<String>, BlobError> {
        let mut stream = match after {
            Some(name) => self
                .read
                .list_with_offset(Some(&self.partition_prefix), &self.path(name)),
            None => self.read.list(Some(&self.partition_prefix)),
        };
        let mut names = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|e| BlobError::Store(format!("object store: {e}")))?;
            if let Some(name) = name_in_directory(&self.partition_prefix, &meta.location) {
                names.push(name);
            }
        }
        Ok(names)
    }
}

/// The name of a listed object within the partition directory `dir`,
/// for the chain validation and the drift check, which treat every name
/// that is not one of the mirror's blobs as a foreign object. `None` for
/// the directory's own placeholder (a key ending in `/`, as the GCS
/// console and gcsfuse create for a folder: object_store lists it as the
/// directory's path), which holds no data. An object in a subdirectory
/// keeps its path below `dir` (`sub/x`), so the error that rejects it
/// says where it is.
fn name_in_directory(dir: &Path, location: &Path) -> Option<String> {
    let mut parts = location.parts();
    for want in dir.parts() {
        if parts.next()? != want {
            // object_store lists below `dir` only; keep any other name
            // whole so it fails as foreign.
            return Some(location.to_string());
        }
    }
    let rest: Vec<String> = parts.map(|p| p.as_ref().to_string()).collect();
    if rest.is_empty() {
        None
    } else {
        Some(rest.join("/"))
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
            partition_prefix(cfg.prefix.as_ref(), &cfg.destination_name, cfg.partition);
        let spec = BlobSpec {
            format: cfg.format,
            compression: cfg.compression,
            keys: cfg.keys,
            values: cfg.values,
            compaction: cfg.compaction,
            flush: cfg.flush,
            encryption: cfg.encryption,
        };
        let store = S3Store::new(cfg.read_store, cfg.write_store, partition_prefix);
        Ok(Self(BlobSink::open_store(store, spec, clock).await?))
    }

    pub async fn flush_now(&mut self) -> Result<(), SinkError> {
        self.0.flush_now().await
    }

    #[doc(hidden)]
    pub fn with_drift_check_interval(self, interval: std::time::Duration) -> Self {
        Self(self.0.with_drift_check_interval(interval))
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

    fn allows_offset_holes(&self) -> bool {
        self.0.allows_offset_holes()
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

/// The directory of one destination's partition:
/// `<prefix>/<destination_name>/<partition>`.
pub fn partition_prefix(root: Option<&Path>, destination_name: &str, partition: u32) -> Path {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mirror_fs::blob::BlobStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;

    use super::{name_in_directory, partition_prefix, S3Store};

    #[tokio::test]
    async fn a_read_only_store_lists_and_reads_and_refuses_writes() {
        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = partition_prefix(Some(&Path::from("sites/x")), "operations", 0);
        assert_eq!(dir.as_ref(), "sites/x/operations/0");
        S3Store::new(Arc::clone(&mem), Arc::clone(&mem), dir.clone())
            .put_new("a", b"x".to_vec())
            .await
            .unwrap();
        let ro = S3Store::read_only(mem, dir);
        assert_eq!(ro.list().await.unwrap(), ["a"]);
        assert_eq!(ro.get("a").await.unwrap(), b"x");
        let err = ro.put_new("b", Vec::new()).await.unwrap_err();
        assert!(err.to_string().contains("opened to read only"), "{err}");
    }

    #[test]
    fn a_folder_placeholder_is_not_an_object_of_the_directory() {
        let dir = Path::from("sites/dev/operations/0");
        assert_eq!(
            name_in_directory(&dir, &Path::parse("sites/dev/operations/0/").unwrap()),
            None
        );
        assert_eq!(
            name_in_directory(&dir, &Path::from("sites/dev/operations/0/0-9.parquet")).as_deref(),
            Some("0-9.parquet")
        );
        assert_eq!(
            name_in_directory(&dir, &Path::from("sites/dev/operations/0/sub/x")).as_deref(),
            Some("sub/x")
        );
    }
}
