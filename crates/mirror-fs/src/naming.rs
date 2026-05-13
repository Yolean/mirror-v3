//! File naming convention for blob destinations.
//!
//! Each flush produces one file named `<from>-<to>.<ext>`, where
//! `from` is the source offset of the first record in the batch and
//! `to` is the source offset of the last record. Both are
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
    fn lexicographic_matches_numeric() {
        let a = batch_filename(9, 9, "x");
        let b = batch_filename(10, 10, "x");
        let c = batch_filename(100, 100, "x");
        assert!(a < b);
        assert!(b < c);
    }
}
