//! Filesystem sink for mirror-v3: a [`BlobSink`] over a local directory.
//!
//! ## Atomicity
//!
//! Each flush writes the encoded batch to a same-directory temporary
//! file (`<final>.tmp.<uuid>`), `fsync`s it, then `rename(2)`s it to the
//! canonical name `<from>-<to>.<ext>` where `<ext>` matches the
//! configured envelope format (parquet / ndjson). POSIX rename is
//! atomic.
//!
//! ## Restart correctness
//!
//! On startup, [`FilesystemSink::open`] lists the partition directory,
//! parses every filename matching the configured extension, and
//! validates the chain (see [`blob::validate_chain`]). Files with
//! another format's extension are an error (no mixed-format dirs).

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use mirror_core::Record;
use mirror_envelope::{ColumnType, Format, ParquetCompression};
use tokio::io::AsyncWriteExt;

pub mod blob;
pub mod naming;

pub use blob::{
    BlobEncryption, BlobError, BlobSink, BlobSpec, BlobStore, CompactionMode, FlushTriggers,
    UnixClock,
};

/// Errors from opening or reading a filesystem destination.
pub type FsError = BlobError;

#[derive(Debug, Clone)]
pub struct FilesystemSinkConfig {
    /// Directory under `root` is `<root>/<destination_name>/<partition>/`.
    pub root: PathBuf,
    pub destination_name: String,
    pub partition: u32,
    pub format: Format,
    pub compression: ParquetCompression,
    /// Storage representation for the record `key`. Caller is
    /// responsible for pairing `Bytes` with `compaction = None`.
    pub keys: ColumnType,
    /// Storage representation for the record `value`.
    pub values: ColumnType,
    /// Optional log-compaction mode. When `Some(CompactionMode::Log)`,
    /// each emitted file is a full materialized snapshot of the
    /// latest value per key. Caller must combine this with
    /// `Format::Parquet` and `keys` ∈ {`Utf8`, `Json`}.
    pub compaction: Option<CompactionMode>,
    pub flush: FlushTriggers,
}

impl FilesystemSinkConfig {
    fn spec(&self) -> BlobSpec {
        BlobSpec {
            format: self.format,
            compression: self.compression,
            keys: self.keys,
            values: self.values,
            compaction: self.compaction,
            flush: self.flush,
            encryption: None,
        }
    }
}

/// One partition directory on the local filesystem.
#[derive(Debug, Clone)]
pub struct FsStore {
    dir: PathBuf,
}

impl FsStore {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Names in the directory, without this store's own temporary
    /// files (`.tmp.` in the name: a write that did not reach its
    /// rename). A missing directory is an empty destination.
    fn list_sync(&self) -> Result<Vec<String>, BlobError> {
        let read = match std::fs::read_dir(&self.dir) {
            Ok(r) => r,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(io_error(&self.dir, e)),
        };
        let mut names = Vec::new();
        for entry in read {
            let entry = entry.map_err(|e| io_error(&self.dir, e))?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.contains(".tmp.") {
                continue;
            }
            names.push(name);
        }
        Ok(names)
    }

    fn get_sync(&self, name: &str) -> Result<Vec<u8>, BlobError> {
        let path = self.dir.join(name);
        std::fs::read(&path).map_err(|e| io_error(&path, e))
    }
}

fn io_error(path: &Path, e: std::io::Error) -> BlobError {
    BlobError::Store(format!("filesystem io {}: {e}", path.display()))
}

#[async_trait]
impl BlobStore for FsStore {
    async fn list(&self) -> Result<Vec<String>, BlobError> {
        self.list_sync()
    }

    async fn put_new(&self, name: &str, bytes: Vec<u8>) -> Result<(), BlobError> {
        let final_path = self.dir.join(name);
        let tmp_path = self
            .dir
            .join(format!("{name}.tmp.{}", uuid::Uuid::new_v4()));
        {
            let mut file = tokio::fs::File::create(&tmp_path)
                .await
                .map_err(|e| io_error(&tmp_path, e))?;
            file.write_all(&bytes)
                .await
                .map_err(|e| io_error(&tmp_path, e))?;
            file.sync_all().await.map_err(|e| io_error(&tmp_path, e))?;
        }
        if let Err(e) = tokio::fs::rename(&tmp_path, &final_path).await {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            return Err(BlobError::Store(format!(
                "rename {} -> {}: {e}",
                tmp_path.display(),
                final_path.display()
            )));
        }
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Vec<u8>, BlobError> {
        self.get_sync(name)
    }

    async fn list_after(&self, after: Option<&str>) -> Result<Vec<String>, BlobError> {
        let mut names = self.list_sync()?;
        if let Some(after) = after {
            names.retain(|n| n.as_str() > after);
        }
        Ok(names)
    }

    fn location(&self, name: &str) -> String {
        self.dir.join(name).display().to_string()
    }
}

/// The filesystem destination.
pub type FilesystemSink = BlobSink<FsStore>;

impl BlobSink<FsStore> {
    pub fn open(cfg: FilesystemSinkConfig) -> Result<Self, FsError> {
        Self::open_with_clock(cfg, blob::system_unix_clock())
    }

    /// Same as [`Self::open`] with a caller-supplied clock (tests).
    #[doc(hidden)]
    pub fn open_with_clock(cfg: FilesystemSinkConfig, clock: UnixClock) -> Result<Self, FsError> {
        let dir = naming::partition_dir(&cfg.root, &cfg.destination_name, cfg.partition);
        std::fs::create_dir_all(&dir).map_err(|e| io_error(&dir, e))?;
        let store = FsStore::new(dir);
        let spec = cfg.spec();
        let names = store.list_sync()?;
        let chain = blob::validate_chain(&names, spec.format, spec.compaction)?;
        let snapshot = match &chain.latest {
            Some(name) => Some(store.get_sync(name)?),
            None => None,
        };
        BlobSink::new(store, spec, chain, snapshot, clock)
    }
}

/// Read the latest compacted snapshot's contents, in key-sorted order.
/// Convenience for tests and operators verifying state in compaction mode.
pub fn read_latest_snapshot(dir: &Path, format: Format) -> Result<Vec<Record>, FsError> {
    let store = FsStore::new(dir.to_path_buf());
    let names = store.list_sync()?;
    let chain = blob::validate_chain(&names, format, Some(CompactionMode::Log))?;
    match chain.latest {
        None => Ok(Vec::new()),
        Some(name) => {
            let bytes = store.get_sync(&name)?;
            let view = blob::decode_view(&name, &store.location(&name), &bytes, format, None)?;
            Ok(view.into_values().collect())
        }
    }
}

/// Read every record from a partition directory in offset order.
/// Convenience for tests and operators verifying state.
pub fn read_all_records(dir: &Path, format: Format) -> Result<Vec<Record>, FsError> {
    let store = FsStore::new(dir.to_path_buf());
    let expected_ext = format.extension();
    let mut entries: Vec<(u64, String)> = store
        .list_sync()?
        .into_iter()
        .filter_map(|name| {
            naming::parse_filename(&name, expected_ext).map(|(from, _)| (from, name))
        })
        .collect();
    entries.sort_unstable();
    let mut out = Vec::new();
    for (_, name) in entries {
        let bytes = store.get_sync(&name)?;
        let decoded = mirror_envelope::decode_batch(format, &bytes).map_err(|e| {
            BlobError::CorruptChain(format!("decode {}: {e}", store.location(&name)))
        })?;
        out.extend(decoded);
    }
    Ok(out)
}
