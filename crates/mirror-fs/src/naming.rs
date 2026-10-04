//! File naming convention for blob destinations.
//!
//! Each flush produces one file named `<from>-<to>.<ext>`, where
//! `from` is the previous file's `to` + 1 (0 for the first) and `to`
//! is the source offset of the last record in the batch: the file
//! covers those consumer positions, offset holes included. Both are
//! zero-padded so lexicographic ordering matches numeric ordering up
//! to ~9 × 10^18 records per partition (more than Kafka allows).
//!
//! The padding width is fixed (20 digits, enough for `u64::MAX`).
//! Anything else means we lose lexicographic ordering on listing.

use std::path::{Path, PathBuf};

const OFFSET_WIDTH: usize = 20;

/// Format a flush filename: `<from>-<to>.<ext>`.
pub fn batch_filename(from: u64, to: u64, ext: &str) -> String {
    format!("{from:0width$}-{to:0width$}.{ext}", width = OFFSET_WIDTH)
}

/// Parse a flush filename. Returns `Some((from, to))` if the basename
/// matches our convention with the given extension; `None` otherwise.
pub fn parse_filename(name: &str, ext: &str) -> Option<(u64, u64)> {
    let stem = name.strip_suffix(&format!(".{ext}"))?;
    let (from, to) = stem.split_once('-')?;
    let from: u64 = from.parse().ok()?;
    let to: u64 = to.parse().ok()?;
    Some((from, to))
}

/// A blob name: the range it covers and, for an encrypted blob, the id
/// of the Parquet key it was written with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobName {
    pub from: u64,
    pub to: u64,
    pub key_id: Option<String>,
}

/// `<from>-<to>.<ext>`, or `<from>-<to>.k-<key id>.<ext>` for a blob
/// encrypted with that key: the observability compactor's layout, so a
/// reader selects one key's objects with the glob `*.k-<id>.parquet`.
pub fn blob_filename(from: u64, to: u64, key_id: Option<&str>, ext: &str) -> String {
    match key_id {
        None => batch_filename(from, to, ext),
        Some(id) => format!(
            "{from:0width$}-{to:0width$}.k-{id}.{ext}",
            width = OFFSET_WIDTH
        ),
    }
}

/// Parse a name written by [`blob_filename`] with this extension.
pub fn parse_blob_name(name: &str, ext: &str) -> Option<BlobName> {
    let stem = name.strip_suffix(&format!(".{ext}"))?;
    let (range, key_id) = match stem.split_once(".k-") {
        Some((range, id)) if mirror_envelope::keys::is_key_id(id) => (range, Some(id.to_string())),
        Some(_) => return None,
        None => (stem, None),
    };
    let (from, to) = range.split_once('-')?;
    Some(BlobName {
        from: from.parse().ok()?,
        to: to.parse().ok()?,
        key_id,
    })
}

/// Build the per-partition directory under `root`: `<root>/<name>/<partition>/`.
pub fn partition_dir(root: &Path, destination_name: &str, partition: u32) -> PathBuf {
    root.join(destination_name).join(partition.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filename_round_trip() {
        let name = batch_filename(100, 199, "ndjson");
        assert_eq!(name, "00000000000000000100-00000000000000000199.ndjson");
        assert_eq!(parse_filename(&name, "ndjson"), Some((100, 199)));
    }

    #[test]
    fn parse_rejects_wrong_extension() {
        let name = batch_filename(0, 0, "ndjson");
        assert_eq!(parse_filename(&name, "json"), None);
    }

    #[test]
    fn parse_rejects_garbage() {
        assert_eq!(parse_filename("not-a-batch.ndjson", "ndjson"), None);
        assert_eq!(parse_filename("123.ndjson", "ndjson"), None);
        assert_eq!(parse_filename("abc-def.ndjson", "ndjson"), None);
    }

    #[test]
    fn keyed_names_round_trip_and_plain_parsing_rejects_them() {
        let name = blob_filename(5, 9, Some("k1"), "parquet");
        assert_eq!(
            name,
            "00000000000000000005-00000000000000000009.k-k1.parquet"
        );
        assert_eq!(
            parse_blob_name(&name, "parquet"),
            Some(BlobName {
                from: 5,
                to: 9,
                key_id: Some("k1".into())
            })
        );
        assert_eq!(parse_filename(&name, "parquet"), None);
        assert_eq!(
            parse_blob_name(&blob_filename(0, 1, None, "parquet"), "parquet").map(|b| b.key_id),
            Some(None)
        );
        assert_eq!(
            parse_blob_name(
                "00000000000000000005-00000000000000000009.k-K1.parquet",
                "parquet"
            ),
            None
        );
    }

    #[test]
    fn lexicographic_matches_numeric() {
        let a = batch_filename(9, 9, "x");
        let b = batch_filename(10, 10, "x");
        let c = batch_filename(100, 100, "x");
        assert!(a < b);
        assert!(b < c);
    }
}
