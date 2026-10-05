//! Parquet encryption keys, in the observability compactor's layout so
//! operators handle one key layout (observability-to-blobs
//! compactor/README.md, "Encryption"): a directory, normally a mounted
//! Secret, with one file per key. The file name is the key id
//! (`[a-z0-9][a-z0-9-]*`, at most 32 characters); the content is the
//! standard base64 of 32 random bytes (`openssl rand -base64 32`).
//! Entries starting with a dot (a Secret mount's own) are skipped; any
//! other file that is not a valid key is an error. Only 32-byte keys:
//! DuckDB takes the base64 text of a 16 or 24 byte key, which is itself
//! 24 or 32 characters long, as the key verbatim, so only a 32-byte
//! key's text means the same key to every reader. Errors name the file,
//! never its content.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;

/// The one key size a key file may hold.
pub const KEY_BYTES: usize = 32;

/// A 32-byte AES key. Debug never shows the bytes.
#[derive(Clone, PartialEq, Eq)]
pub struct ParquetKey(pub [u8; KEY_BYTES]);

impl std::fmt::Debug for ParquetKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ParquetKey(<redacted>)")
    }
}

/// Whether `id` is a key id: `[a-z0-9][a-z0-9-]{0,31}`.
pub fn is_key_id(id: &str) -> bool {
    let b = id.as_bytes();
    !b.is_empty()
        && b.len() <= 32
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

#[derive(Debug, thiserror::Error)]
pub enum KeysError {
    #[error("keys-dir {dir}: {source}")]
    Dir {
        dir: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("keys-dir {dir}: {name:?} is not a key id (want [a-z0-9][a-z0-9-]{{0,31}})")]
    BadName { dir: PathBuf, name: String },
    #[error("keys-dir {dir}: key {id}: {source}")]
    Read {
        dir: PathBuf,
        id: String,
        #[source]
        source: std::io::Error,
    },
    #[error("keys-dir {dir}: key {id}: want the standard base64 of {KEY_BYTES} bytes (openssl rand -base64 {KEY_BYTES})")]
    BadKey { dir: PathBuf, id: String },
    #[error("keys-dir {dir} has no key file {id}")]
    Missing { dir: PathBuf, id: String },
}

/// Every key of a keys directory, by id.
#[derive(Debug, Clone)]
pub struct Keyring {
    dir: PathBuf,
    keys: BTreeMap<String, ParquetKey>,
}

impl Keyring {
    /// Read a keys directory (see the module docs for its layout).
    pub fn load(dir: &Path) -> Result<Self, KeysError> {
        let entries = std::fs::read_dir(dir).map_err(|source| KeysError::Dir {
            dir: dir.to_path_buf(),
            source,
        })?;
        let mut keys = BTreeMap::new();
        for entry in entries {
            let entry = entry.map_err(|source| KeysError::Dir {
                dir: dir.to_path_buf(),
                source,
            })?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with('.') {
                continue;
            }
            if !is_key_id(&name) {
                return Err(KeysError::BadName {
                    dir: dir.to_path_buf(),
                    name,
                });
            }
            let text = std::fs::read_to_string(entry.path()).map_err(|source| KeysError::Read {
                dir: dir.to_path_buf(),
                id: name.clone(),
                source,
            })?;
            let raw = B64
                .decode(text.trim())
                .ok()
                .and_then(|raw| <[u8; KEY_BYTES]>::try_from(raw).ok())
                .ok_or_else(|| KeysError::BadKey {
                    dir: dir.to_path_buf(),
                    id: name.clone(),
                })?;
            keys.insert(name, ParquetKey(raw));
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            keys,
        })
    }

    /// The key `id`, or an error naming the directory and the id.
    pub fn get(&self, id: &str) -> Result<&ParquetKey, KeysError> {
        self.keys.get(id).ok_or_else(|| KeysError::Missing {
            dir: self.dir.clone(),
            id: id.to_string(),
        })
    }

    pub fn ids(&self) -> impl Iterator<Item = &str> {
        self.keys.keys().map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir_with(files: &[(&str, &str)]) -> tempfile::TempDir {
        let d = tempfile::tempdir().unwrap();
        for (name, content) in files {
            std::fs::write(d.path().join(name), content).unwrap();
        }
        d
    }

    const K32: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";

    #[test]
    fn loads_keys_and_skips_dot_entries() {
        let d = dir_with(&[("k1", K32), ("..data", "x"), ("k-2", &format!("{K32}\n"))]);
        let ring = Keyring::load(d.path()).unwrap();
        assert_eq!(ring.ids().collect::<Vec<_>>(), vec!["k-2", "k1"]);
        assert_eq!(ring.get("k1").unwrap().0[31], 31);
        let err = ring.get("k3").unwrap_err().to_string();
        assert!(err.contains("has no key file k3"), "{err}");
    }

    #[test]
    fn rejects_bad_names_and_bad_keys_without_printing_them() {
        let d = dir_with(&[("K1", K32)]);
        assert!(matches!(
            Keyring::load(d.path()),
            Err(KeysError::BadName { .. })
        ));
        let d = dir_with(&[("k1", "c2VjcmV0LXRleHQ=")]);
        let err = Keyring::load(d.path()).unwrap_err().to_string();
        assert!(
            err.contains("want the standard base64 of 32 bytes"),
            "{err}"
        );
        assert!(
            !err.contains("c2VjcmV0"),
            "the content must not be printed: {err}"
        );
    }

    #[test]
    fn key_ids() {
        assert!(is_key_id("k1"));
        assert!(is_key_id("2026-10-key"));
        assert!(!is_key_id("-k"));
        assert!(!is_key_id("K"));
        assert!(!is_key_id(&"k".repeat(33)));
    }
}
