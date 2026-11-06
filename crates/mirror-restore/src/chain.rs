//! The chain of a blob backup, from its object names alone.
//!
//! A blob destination in append mode is a directory of objects named
//! `<from>-<to>.<ext>` or `<from>-<to>.k-<key id>.<ext>`. They cover
//! consumer positions: `from` is the previous object's `to` + 1 and `to`
//! is the offset of the last record the object holds, so the names form
//! a contiguous chain even where the topic had offset holes (the holes
//! are visible only inside the objects, see [`crate::read`]). A backup
//! is complete from its names when they form that chain without a gap
//! or an overlap, from the offset it is declared to start at: its floor
//! ([`plan_floor_chain`], see [`mirror_fs::floor`]), or an offset given
//! by hand ([`plan_chain`]).

use mirror_envelope::{Format, Keyring};
use mirror_fs::floor::{self, Floor, FloorMarker};
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

/// A chain read from its floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FloorChain {
    /// The floor the directory's markers set.
    pub floor: Option<Floor>,
    /// The floor the chain was planned from: the markers', or the one
    /// [`plan_floor_chain`] was given.
    pub floor_offset: u64,
    /// The objects at and above the floor, in chain order; empty when
    /// every object expired.
    pub objects: Vec<ChainObject>,
    /// Objects that end below the floor and are still in the directory.
    pub below_floor: Vec<String>,
    /// Every floor marker in the directory.
    pub markers: Vec<FloorMarker>,
}

impl FloorChain {
    /// The first position of the chain: its first object's `from`, which
    /// may be below the floor, or the floor without objects.
    pub fn start(&self) -> u64 {
        self.objects.first().map_or(self.floor_offset, |o| o.from)
    }

    /// The position after the chain.
    pub fn end(&self) -> u64 {
        self.objects.last().map_or(self.floor_offset, |o| o.to + 1)
    }
}

/// Validate a listing from a floor: the one its markers set, or `at`
/// (`trim --floor`, which checks that it is not below the markers').
/// Objects that end below the floor are not part of the chain, present
/// or not; the first object at or above it must cover it, and from
/// there the chain is validated as [`plan_chain`] does, apart from the
/// keys (`trim` holds none; a reader calls [`check_keys`]). A floor above
/// the end of the chain is an error, unless no object is left at all.
pub fn plan_floor_chain(
    names: &[String],
    format: Format,
    at: Option<u64>,
) -> Result<FloorChain, RestoreError> {
    let ext = format.extension();
    let (floor, markers) = floor::floor_of(names).map_err(RestoreError::Chain)?;
    let floor_offset = at.unwrap_or(floor.map_or(0, |f| f.offset));
    let mut rest = Vec::new();
    let mut below = Vec::new();
    for name in names {
        if floor::is_marker(name) {
            continue;
        }
        match naming::parse_blob_name(name, ext) {
            Some(b) if b.to < floor_offset => below.push((b.to, name.clone())),
            _ => rest.push(name.clone()),
        }
    }
    below.sort_unstable();
    let below_end = below.last().map(|(to, _)| to + 1);
    let below_floor: Vec<String> = below.into_iter().map(|(_, n)| n).collect();
    let first = rest
        .iter()
        .filter_map(|n| naming::parse_blob_name(n, ext).map(|b| (b.from, n)))
        .min();
    let objects = match first {
        None if rest.is_empty() => {
            if let Some(end) = below_end.filter(|&end| end < floor_offset) {
                return Err(RestoreError::Chain(format!(
                    "the chain's floor {floor_offset} is above the end of the chain, {end}: \
                     offsets {end}-{} are in no object",
                    floor_offset - 1
                )));
            }
            Vec::new()
        }
        Some((from, name)) if from > floor_offset => {
            return Err(RestoreError::Chain(if floor_offset == 0 {
                format!(
                    "the chain starts at offset {from} ({name}), not at 0; if objects before \
                     it were removed on purpose, set the chain's floor (mirror-v3 trim --floor \
                     {from}), or pass --chain-start"
                )
            } else {
                format!(
                    "gap in the chain: its floor is {floor_offset}, and the first object at or \
                     above it, {name}, starts at {from}: offsets {floor_offset}-{} are in no \
                     object",
                    from - 1
                )
            }));
        }
        // A name that is not an object of the format: plan_names says so.
        Some((from, _)) => plan_names(&rest, format, from)?,
        None => plan_names(&rest, format, floor_offset)?,
    };
    Ok(FloorChain {
        floor,
        floor_offset,
        objects,
        below_floor,
        markers,
    })
}

/// Validate a listing of one partition directory and return its objects
/// in chain order. Every name must be an object of this backup's format
/// or a floor marker (anything else in the directory means it is not the
/// directory it is taken for); the chain must start at `chain_start` and
/// be contiguous; every key an object names must be in `keyring`.
pub fn plan_chain(
    names: &[String],
    format: Format,
    chain_start: u64,
    keyring: Option<&Keyring>,
) -> Result<Vec<ChainObject>, RestoreError> {
    let objects = plan_names(names, format, chain_start)?;
    check_keys(&objects, keyring)?;
    Ok(objects)
}

fn plan_names(
    names: &[String],
    format: Format,
    chain_start: u64,
) -> Result<Vec<ChainObject>, RestoreError> {
    let ext = format.extension();
    let (floor, _) = floor::floor_of(names).map_err(RestoreError::Chain)?;
    let mut objects = Vec::with_capacity(names.len());
    for name in names {
        if floor::is_marker(name) {
            continue;
        }
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
            match floor {
                Some(f) => format!(
                    "; the backup's floor is {}: without --chain-start the chain starts there",
                    f.offset
                ),
                None if chain_start == 0 => {
                    "; if objects before it were removed on purpose, pass --chain-start".into()
                }
                None => String::new(),
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
    Ok(objects)
}

/// Every key an object names must be in `keyring` (`None`: the source
/// has `encryption: none`), before anything is read.
pub fn check_keys(objects: &[ChainObject], keyring: Option<&Keyring>) -> Result<(), RestoreError> {
    let mut missing: Vec<&str> = Vec::new();
    for o in objects {
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
    Ok(())
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

    fn marker(offset: u64) -> String {
        FloorMarker {
            offset,
            at: 7,
            oldest_ms: None,
        }
        .filename()
    }

    fn floor_chain(names: &[String], at: Option<u64>) -> Result<FloorChain, String> {
        plan_floor_chain(names, Format::Parquet, at).map_err(|e| e.to_string())
    }

    #[test]
    fn a_floor_chain_starts_at_the_floor_and_skips_what_is_below() {
        let c = floor_chain(&[n(0, 4), marker(10), n(10, 14), n(15, 19)], None).unwrap();
        assert_eq!(c.floor.map(|f| f.offset), Some(10));
        assert_eq!((c.start(), c.end()), (10, 20));
        assert_eq!(c.objects.len(), 2);
        assert_eq!(c.below_floor, vec![n(0, 4)]);
        // A floor inside the first object: the chain starts with it.
        let c = floor_chain(&[marker(12), n(10, 14), n(15, 19)], None).unwrap();
        assert_eq!((c.start(), c.end()), (10, 20));
        // Every object expired.
        let c = floor_chain(&[n(0, 4), marker(5)], None).unwrap();
        assert!(c.objects.is_empty());
        assert_eq!((c.start(), c.end()), (5, 5));
        // Without a floor, from 0.
        let c = floor_chain(&[n(0, 4), n(5, 9)], None).unwrap();
        assert_eq!((c.floor, c.start(), c.end()), (None, 0, 10));
    }

    #[test]
    fn a_floor_chain_refuses_gaps_above_the_floor_and_a_floor_past_the_end() {
        let e = floor_chain(&[marker(10), n(12, 19)], None).unwrap_err();
        assert!(e.contains("its floor is 10"), "{e}");
        assert!(e.contains("offsets 10-11 are in no object"), "{e}");
        let e = floor_chain(&[marker(10), n(10, 14), n(16, 19)], None).unwrap_err();
        assert!(e.contains("gap in the chain: offsets 15-15"), "{e}");
        let e = floor_chain(&[n(0, 4), marker(9)], None).unwrap_err();
        assert!(
            e.contains("floor 9 is above the end of the chain, 5"),
            "{e}"
        );
        let e = floor_chain(&[n(5, 9)], None).unwrap_err();
        assert!(e.contains("starts at offset 5"), "{e}");
        assert!(e.contains("trim --floor 5"), "{e}");
    }

    #[test]
    fn a_floor_chain_can_be_planned_from_a_floor_given_by_hand() {
        // A head a lifecycle rule removed, before there was a floor.
        let c = floor_chain(&[n(5, 9), n(10, 14)], Some(5)).unwrap();
        assert_eq!((c.floor, c.start(), c.end()), (None, 5, 15));
        let c = floor_chain(&[n(5, 9), n(10, 14)], Some(15)).unwrap();
        assert!(c.objects.is_empty());
        assert_eq!(c.below_floor.len(), 2);
    }

    #[test]
    fn a_chain_from_a_given_start_skips_markers_and_points_at_the_floor() {
        let chain = plan_chain(&[marker(5), n(5, 9)], Format::Parquet, 5, None).unwrap();
        assert_eq!(chain.len(), 1);
        let e = err(&[marker(5), n(5, 9)], 0, None);
        assert!(e.contains("the backup's floor is 5"), "{e}");
        let e = err(&["floor-5.json".into(), n(0, 4)], 0, None);
        assert!(e.contains("is not a floor marker name"), "{e}");
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
