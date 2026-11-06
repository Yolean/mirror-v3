//! S3-compatible blob sink: a [`BlobSink`] over an object-store prefix.
//!
//! ## Atomicity (two-layer)
//!
//! 1. **Preferred**: `PutMode::Create` (`If-None-Match: *`). On AWS S3
//!    this fails a second writer of the *same name* with 412.
//! 2. **Always**: single-writer-per-(topic,partition) by deployment +
//!    chain validation on startup. VersityGW run with `--nometa` ignores
//!    `PutMode::Create`, and writers with different flush boundaries
//!    write different names; both leave a chain that the next open
//!    refuses.
//!
//! ## Restart correctness
//!
//! On open, list every object under the prefix, parse `<from>-<to>.<ext>`
//! names, and validate the chain (see [`mirror_fs::blob::validate_chain`]).
//!
//! ## Retention lock
//!
//! A destination with `lock-days` writes each object with a COMPLIANCE
//! retention lock in the PUT that creates it (see [`lock`]), and reads
//! the lock back before it counts the object as written. Open refuses a
//! bucket without object lock. Nothing ever sets the retention of an
//! object that exists.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use mirror_core::{FlushObserver, Record, Sink, SinkError};
use mirror_envelope::{ColumnType, Format, ParquetCompression};
use mirror_fs::blob::{
    self, BlobError, BlobSink, BlobSpec, BlobStore, NewObject, ObjectVersion, RecordTimes,
};
use mirror_fs::floor::{utc_rfc3339, Floor};
use object_store::path::Path;
use object_store::{
    Attribute, Attributes, GetOptions, ObjectStore, PutMode, PutOptions, PutPayload,
};

pub mod api;
pub mod lock;

pub use api::{ApiError, BucketLock, S3Api};
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
    /// `lock-days`: every new object is written with a retention lock.
    pub lock: Option<SinkLock>,
}

/// A destination's retention lock.
pub struct SinkLock {
    pub days: u32,
    /// The bucket, reached with the writing identity.
    pub api: Arc<S3Api>,
}

/// One partition prefix in an object store, reached with two identities:
/// one that lists and reads, one that writes (none for a restore, which
/// only reads).
pub struct S3Store {
    read: Arc<dyn ObjectStore>,
    write: Option<Arc<dyn ObjectStore>>,
    partition_prefix: Path,
    /// Signed requests object_store does not make, with the writing
    /// identity: locked PUTs and their read-back, and versions.
    api: Option<Arc<S3Api>>,
    lock_days: Option<u32>,
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
            api: None,
            lock_days: None,
        }
    }

    /// A store that lists and reads only: a write is an error.
    pub fn read_only(read: Arc<dyn ObjectStore>, partition_prefix: Path) -> Self {
        Self {
            read,
            write: None,
            partition_prefix,
            api: None,
            lock_days: None,
        }
    }

    /// Reach the bucket with `api` (the writing identity's) for what
    /// object_store cannot do: locked PUTs, and versions on a versioned
    /// bucket. Without it, [`BlobStore::versions`] takes the bucket for
    /// unversioned.
    pub fn with_api(mut self, api: Arc<S3Api>) -> Self {
        self.api = Some(api);
        self
    }

    /// Write every new object with a retention lock of `days` (see
    /// [`lock::retain_until`]). Needs [`S3Store::with_api`].
    pub fn with_lock(mut self, days: u32) -> Self {
        self.lock_days = Some(days);
        self
    }

    /// Write `bytes` with a COMPLIANCE lock until `retain_until`, then
    /// read the lock back: an object counts as written only once the
    /// store shows the lock it was written with.
    async fn put_locked(
        &self,
        path: &Path,
        bytes: Vec<u8>,
        metadata: &[(&'static str, String)],
        retain_until: u64,
    ) -> Result<(), BlobError> {
        let Some(api) = &self.api else {
            return Err(BlobError::Lock(format!(
                "put {path}: a retention lock is configured, but this store has no S3 API to \
                 set it with"
            )));
        };
        let key = path.as_ref();
        if let Err(e) = api
            .put_object_locked(key, Bytes::from(bytes), metadata, retain_until)
            .await
        {
            return Err(match e.status {
                Some(412) => BlobError::AlreadyExists(path.to_string()),
                _ if e.is_transient() => BlobError::Store(format!("locked put {path}: {e}")),
                _ => BlobError::Lock(format!(
                    "locked put {path}: {e} (the writing identity needs s3:PutObjectRetention \
                     besides s3:PutObject)"
                )),
            });
        }
        let not_locked = |got: String| {
            BlobError::Lock(format!(
                "{path} was written, but its retention is {got}, not COMPLIANCE until {}: the \
                 store did not apply the lock",
                utc_rfc3339(retain_until)
            ))
        };
        match api.object_retention(key).await {
            Ok(Some(r)) if r.mode == "COMPLIANCE" && r.retain_until >= retain_until => Ok(()),
            Ok(Some(r)) => Err(not_locked(r.to_string())),
            Ok(None) => Err(not_locked("none".into())),
            Err(e) => Err(BlobError::Lock(format!(
                "{path} was written with a retention lock, but reading it back failed: {e} \
                 (the writing identity needs s3:GetObjectRetention)"
            ))),
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

    async fn put_new(
        &self,
        name: &str,
        bytes: Vec<u8>,
        object: &NewObject,
    ) -> Result<Option<u64>, BlobError> {
        let path = self.path(name);
        let Some(write) = &self.write else {
            return Err(BlobError::Store(format!(
                "put {path}: this store was opened to read only"
            )));
        };
        let metadata = match object {
            NewObject::Records { times, .. } => times.to_metadata(),
            NewObject::Marker { .. } | NewObject::Snapshot => Vec::new(),
        };
        if let Some(days) = self.lock_days {
            match lock::retain_until(object, days) {
                Some(until) => {
                    self.put_locked(&path, bytes, &metadata, until).await?;
                    return Ok(Some(until));
                }
                None if matches!(object, NewObject::Records { .. }) => tracing::info!(
                    object = %path,
                    lock_days = days,
                    "writing without a retention lock: its records are older than lock-days"
                ),
                None => {}
            }
        }
        let mut attributes = Attributes::new();
        for (key, value) in metadata {
            attributes.insert(Attribute::Metadata(key.into()), value.into());
        }
        let opts = PutOptions {
            mode: PutMode::Create,
            attributes,
            ..Default::default()
        };
        match write
            .put_opts(&path, PutPayload::from(Bytes::from(bytes)), opts)
            .await
        {
            Ok(_) => Ok(None),
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

    /// A HEAD, through the reading identity: the object's user metadata.
    async fn record_times(&self, name: &str) -> Result<Option<RecordTimes>, BlobError> {
        let path = self.path(name);
        let head = self
            .read
            .get_opts(
                &path,
                GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| BlobError::Store(format!("head {path}: {e}")))?;
        let attributes = head.attributes;
        RecordTimes::from_metadata(|key| {
            attributes
                .get(&Attribute::Metadata(key.to_string().into()))
                .map(|v| v.as_ref().to_string())
        })
        .map_err(|e| BlobError::Store(format!("{path}: {e}")))
    }

    /// Through the writing identity (the trim identity, for `trim`).
    async fn delete(&self, name: &str) -> Result<(), BlobError> {
        let path = self.path(name);
        let Some(write) = &self.write else {
            return Err(BlobError::Store(format!(
                "delete {path}: this store was opened to read only"
            )));
        };
        match write.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(BlobError::Store(format!("delete {path}: {e}"))),
        }
    }

    /// GetBucketVersioning, then ListObjectVersions of the prefix, with
    /// the writing identity (the trim identity, for `trim`).
    async fn versions(&self) -> Result<Option<Vec<ObjectVersion>>, BlobError> {
        let Some(api) = &self.api else {
            return Ok(None);
        };
        let versioning = api.bucket_versioning().await.map_err(|e| {
            BlobError::Store(format!(
                "bucket {}: cannot tell whether it keeps versions: {e} (the identity needs \
                 s3:GetBucketVersioning)",
                api.bucket()
            ))
        })?;
        if versioning == api::Versioning::Off {
            return Ok(None);
        }
        let prefix = format!("{}/", self.partition_prefix);
        let versions = api.list_object_versions(&prefix).await.map_err(|e| {
            BlobError::Store(format!(
                "list versions of {prefix}: {e} (the identity needs s3:ListBucketVersions)"
            ))
        })?;
        Ok(Some(
            versions
                .into_iter()
                .filter_map(|v| {
                    let name = name_in_directory(&self.partition_prefix, &Path::from(v.key))?;
                    Some(ObjectVersion {
                        name,
                        version_id: v.version_id,
                        delete_marker: v.delete_marker,
                    })
                })
                .collect(),
        ))
    }

    async fn delete_version(&self, name: &str, version_id: &str) -> Result<(), BlobError> {
        let path = self.path(name);
        let Some(api) = &self.api else {
            return Err(BlobError::Store(format!(
                "delete {path} version {version_id}: this store has no S3 API"
            )));
        };
        api.delete_object_version(path.as_ref(), version_id)
            .await
            .map_err(|e| BlobError::Store(format!("delete {path} version {version_id}: {e}")))
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

/// At each open of a destination with `lock-days`: the bucket must have
/// object lock, or every locked PUT would fail. The result is logged
/// either way, so the run log shows what the lock rested on.
async fn check_bucket_lock(api: &S3Api, days: u32) -> Result<(), BlobError> {
    let bucket = api.bucket();
    match api.bucket_object_lock().await {
        Ok(BucketLock::Enabled { default: None }) => {
            tracing::info!(
                bucket,
                lock_days = days,
                default_retention = "none",
                "object lock: enabled; each new object is locked COMPLIANCE from its record time"
            );
            Ok(())
        }
        Ok(BucketLock::Enabled {
            default: Some(default),
        }) => {
            tracing::warn!(
                bucket,
                lock_days = days,
                default_retention = %default,
                "object lock: enabled, with a default retention: objects written without a lock \
                 of their own (records older than lock-days) are held by it from their write time"
            );
            Ok(())
        }
        Ok(BucketLock::NotEnabled { reason }) => {
            tracing::error!(bucket, lock_days = days, reason = %reason, "object lock: not enabled");
            Err(BlobError::Lock(format!(
                "lock-days is set, but bucket {bucket} has no object lock ({reason}): create the \
                 bucket with object lock enabled, or remove lock-days"
            )))
        }
        Err(e) if e.is_transient() => Err(BlobError::Store(format!(
            "bucket {bucket}: reading its object lock configuration: {e}"
        ))),
        Err(e) => {
            tracing::error!(bucket, lock_days = days, error = %e, "object lock: cannot tell");
            Err(BlobError::Lock(format!(
                "lock-days is set, but whether bucket {bucket} has object lock cannot be told: \
                 {e} (the writing identity needs s3:GetBucketObjectLockConfiguration)"
            )))
        }
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
        let mut store = S3Store::new(cfg.read_store, cfg.write_store, partition_prefix);
        if let Some(lock) = cfg.lock {
            check_bucket_lock(&lock.api, lock.days).await?;
            store = store.with_api(lock.api).with_lock(lock.days);
        }
        Ok(Self(BlobSink::open_store(store, spec, clock).await?))
    }

    pub async fn flush_now(&mut self) -> Result<(), SinkError> {
        self.0.flush_now().await
    }

    #[doc(hidden)]
    pub fn with_drift_check_interval(self, interval: std::time::Duration) -> Self {
        Self(self.0.with_drift_check_interval(interval))
    }

    /// Append mode: the chain's floor, as last seen.
    pub fn floor(&self) -> Option<&Floor> {
        self.0.floor()
    }

    /// Objects below the floor that were still in the prefix at open.
    pub fn below_floor_at_open(&self) -> usize {
        self.0.below_floor_at_open()
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

    use mirror_fs::blob::{BlobStore, NewObject};
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
            .put_new("a", b"x".to_vec(), &NewObject::Snapshot)
            .await
            .unwrap();
        let ro = S3Store::read_only(mem, dir);
        assert_eq!(ro.list().await.unwrap(), ["a"]);
        assert_eq!(ro.get("a").await.unwrap(), b"x");
        let err = ro
            .put_new("b", Vec::new(), &NewObject::Snapshot)
            .await
            .unwrap_err();
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
