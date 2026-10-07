//! The chain of a blob backup, from its object names alone.
//!
//! A blob destination in append mode is a directory of objects named
//! `<from>-<to>.<ext>` or `<from>-<to>.k-<key id>.<ext>`. They cover
//! consumer positions: `from` is the previous object's `to` + 1 and `to`
//! is the offset of the last record the object holds, so the names form
//! a contiguous chain even where the topic had offset holes (the holes
//! are visible only inside the objects, see [`crate::read`]). A backup
//! is complete from its names when they form that chain without a gap
//! or an overlap, from the offset it is declared to start at.

use mirror_envelope::{Format, Keyring};
use mirror_fs::naming;

use crate::RestoreError;

/// One object of a validated chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainObject {
    pub name: String,
    pub from: u64,
    pub to: u64,
    /// The Parquet key the object is encrypted with; `None` in clear.
    pub key_id: Option<String>,
}

/// Validate a listing of one partition directory and return its objects
/// in chain order. Every name must be an object of this backup's format
/// (anything else in the directory means it is not the directory it is
/// taken for); the chain must start at `chain_start` and be contiguous;
/// every key an object names must be in `keyring`.
pub fn plan_chain(
    names: &[String],
    format: Format,
    chain_start: u64,
    keyring: Option<&Keyring>,
) -> Result<Vec<ChainObject>, RestoreError> {
    let ext = format.extension();
    let mut objects = Vec::with_capacity(names.len());
    for name in names {
        let Some(blob) = naming::parse_blob_name(name, ext) else {
            return Err(RestoreError::Chain(format!(
                "{name} is not a backup object of format {ext} \
                 (<from>-<to>.{ext}, or <from>-<to>.k-<key id>.{ext})"
            )));
        };
        if blob.to < blob.from {
            return Err(RestoreError::Chain(format!("{name}: to < from")));
        }
        objects.push(ChainObject {
            name: name.clone(),
            from: blob.from,
            to: blob.to,
            key_id: blob.key_id,
        });
    }
    let Some(first) = objects.iter().map(|o| o.from).min() else {
        return Err(RestoreError::Chain(
            "the directory holds no backup objects".into(),
        ));
    };
    objects.sort_by(|a, b| (a.from, a.to, &a.name).cmp(&(b.from, b.to, &b.name)));
    if first != chain_start {
        return Err(RestoreError::Chain(format!(
            "the chain starts at offset {first} ({}), not at {chain_start}{}",
            objects[0].name,
            if chain_start == 0 {
                "; if objects before it were removed on purpose, pass --chain-start"
            } else {
                ""
            }
        )));
    }
    let mut expected = chain_start;
    for o in &objects {
        if o.from > expected {
            return Err(RestoreError::Chain(format!(
                "gap in the chain: offsets {expected}-{} are in no object (next object is {})",
                o.from - 1,
                o.name
            )));
        }
        if o.from < expected {
            return Err(RestoreError::Chain(format!(
                "overlap in the chain: {} starts at {}, and the objects before it reach {}",
                o.name,
                o.from,
                expected - 1
            )));
        }
        expected = o.to + 1;
    }
    let mut missing: Vec<&str> = Vec::new();
    for o in &objects {
        let Some(id) = o.key_id.as_deref() else {
            continue;
        };
        let have = keyring.is_some_and(|k| k.get(id).is_ok());
        if !have && !missing.contains(&id) {
            missing.push(id);
        }
    }
    if !missing.is_empty() {
        return Err(RestoreError::Chain(match keyring {
            None => format!(
                "objects are encrypted with Parquet key(s) {}, and the source has \
                 `encryption: none`",
                missing.join(", ")
            ),
            Some(_) => format!(
                "objects are encrypted with Parquet key(s) {} that the keys-dir does not hold",
                missing.join(", ")
            ),
        }));
    }
    Ok(objects)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(from: u64, to: u64) -> String {
        naming::blob_filename(from, to, None, "parquet")
    }

    fn k(from: u64, to: u64, id: &str) -> String {
        naming::blob_filename(from, to, Some(id), "parquet")
    }

    fn keyring(ids: &[&str]) -> (tempfile::TempDir, Keyring) {
        let dir = tempfile::tempdir().unwrap();
        for id in ids {
            std::fs::write(
                dir.path().join(id),
                "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
            )
            .unwrap();
        }
        let ring = Keyring::load(dir.path()).unwrap();
        (dir, ring)
    }

    fn err(names: &[String], start: u64, keyring: Option<&Keyring>) -> String {
        plan_chain(names, Format::Parquet, start, keyring)
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn a_contiguous_chain_comes_back_sorted() {
        let chain = plan_chain(&[n(5, 9), n(0, 4), n(10, 10)], Format::Parquet, 0, None).unwrap();
        let ranges: Vec<(u64, u64)> = chain.iter().map(|o| (o.from, o.to)).collect();
        assert_eq!(ranges, vec![(0, 4), (5, 9), (10, 10)]);
    }

    #[test]
    fn a_gap_is_an_error_naming_the_missing_offsets() {
        let e = err(&[n(0, 4), n(8, 9)], 0, None);
        assert!(e.contains("gap in the chain: offsets 5-7"), "{e}");
    }

    #[test]
    fn an_overlap_is_an_error() {
        let e = err(&[n(0, 4), n(4, 9)], 0, None);
        assert!(e.contains("overlap in the chain"), "{e}");
        // Two objects of one range (a second writer, another key).
        let e = err(&[n(0, 4), k(0, 4, "k1")], 0, None);
        assert!(e.contains("overlap in the chain"), "{e}");
    }

    #[test]
    fn the_chain_must_start_where_it_is_declared_to() {
        let e = err(&[n(5, 9), n(10, 12)], 0, None);
        assert!(e.contains("starts at offset 5"), "{e}");
        assert!(e.contains("--chain-start"), "{e}");
        assert_eq!(
            plan_chain(&[n(5, 9), n(10, 12)], Format::Parquet, 5, None)
                .unwrap()
                .len(),
            2
        );
        let e = err(&[n(0, 4), n(5, 9)], 5, None);
        assert!(e.contains("starts at offset 0"), "{e}");
    }

    #[test]
    fn an_empty_directory_is_an_error() {
        let e = err(&[], 0, None);
        assert!(e.contains("no backup objects"), "{e}");
    }

    #[test]
    fn a_name_that_is_not_an_object_of_the_format_is_an_error() {
        let e = err(&[n(0, 4), "README".into()], 0, None);
        assert!(e.contains("README is not a backup object"), "{e}");
        let ndjson = naming::blob_filename(5, 9, None, "ndjson");
        let e = err(&[n(0, 4), ndjson], 0, None);
        assert!(
            e.contains("is not a backup object of format parquet"),
            "{e}"
        );
        let e = err(&[n(4, 0)], 0, None);
        assert!(e.contains("to < from"), "{e}");
    }

    #[test]
    fn mixed_key_ids_and_clear_objects_are_one_chain() {
        let (_d, ring) = keyring(&["k1", "k2"]);
        let chain = plan_chain(
            &[n(0, 4), k(5, 9, "k1"), k(10, 19, "k2")],
            Format::Parquet,
            0,
            Some(&ring),
        )
        .unwrap();
        let ids: Vec<Option<&str>> = chain.iter().map(|o| o.key_id.as_deref()).collect();
        assert_eq!(ids, vec![None, Some("k1"), Some("k2")]);
    }

    #[test]
    fn a_key_the_keyring_lacks_is_an_error_before_anything_is_read() {
        let (_d, ring) = keyring(&["k1"]);
        let e = err(
            &[
                k(0, 4, "k1"),
                k(5, 9, "k2"),
                k(10, 12, "k3"),
                k(13, 14, "k2"),
            ],
            0,
            Some(&ring),
        );
        assert!(
            e.contains("key(s) k2, k3 that the keys-dir does not hold"),
            "{e}"
        );
        let e = err(&[k(0, 4, "k1")], 0, None);
        assert!(e.contains("`encryption: none`"), "{e}");
    }
}
