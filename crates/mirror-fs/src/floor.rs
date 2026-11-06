//! The floor of an append-mode chain: the offset below which objects
//! may be missing because they were removed on purpose (expired by
//! `mirror-v3 trim`, or a head removed by other means and adopted with
//! `trim --floor`).
//!
//! A floor is a marker object in the partition directory, named
//!
//! ```text
//! floor-<offset, 20 digits>.at-<unix seconds>[.oldest-<unix ms>].json
//! ```
//!
//! Everything a mirror needs is in the name, so a reading identity that
//! may only LIST sees the floor. The floor is the highest offset among
//! the markers: moving it writes a new marker, and old markers stay as
//! the store's record of when each expiry happened (their JSON content
//! records how it was computed; nothing parses it). Writing a lower
//! marker changes nothing. Deleting the highest marker lowers the
//! floor, which the chain validation reports as a gap once the objects
//! below the old floor are gone.
//!
//! `at` is when the marker was written. `oldest` is the timestamp of the
//! oldest record in the first object at or above the floor, when the
//! marker was written: those records do not change until the floor
//! moves again, so its age keeps growing while the floor stays, which
//! is what an alert on a stuck expiry watches.
//!
//! Marker names start with a letter and blob names with a digit, so
//! every marker sorts after every blob: a listing that starts after a
//! blob (the drift check, a follower) includes all the markers.

/// What every marker name starts with.
pub const FLOOR_PREFIX: &str = "floor-";

const OFFSET_WIDTH: usize = 20;

/// One floor marker, parsed from its name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FloorMarker {
    pub offset: u64,
    /// Unix seconds.
    pub at: u64,
    /// Unix milliseconds.
    pub oldest_ms: Option<u64>,
}

impl FloorMarker {
    pub fn filename(&self) -> String {
        let mut name = format!(
            "{FLOOR_PREFIX}{:0width$}.at-{}",
            self.offset,
            self.at,
            width = OFFSET_WIDTH
        );
        if let Some(ms) = self.oldest_ms {
            name.push_str(&format!(".oldest-{ms}"));
        }
        name.push_str(".json");
        name
    }

    /// `None` for a name that is not a marker (no `floor-` prefix);
    /// `Some(Err)` for one that has the prefix and does not parse, which
    /// the chain validation reports rather than skips.
    pub fn parse(name: &str) -> Option<Result<Self, String>> {
        let rest = name.strip_prefix(FLOOR_PREFIX)?;
        Some(Self::parse_rest(rest).ok_or_else(|| {
            format!(
                "{name} is not a floor marker name \
                 ({FLOOR_PREFIX}<20-digit offset>.at-<unix seconds>[.oldest-<unix ms>].json)"
            )
        }))
    }

    fn parse_rest(rest: &str) -> Option<Self> {
        let stem = rest.strip_suffix(".json")?;
        let mut parts = stem.split('.');
        let offset = parts.next()?;
        if offset.len() != OFFSET_WIDTH {
            return None;
        }
        let offset = digits(offset)?;
        let at = digits(parts.next()?.strip_prefix("at-")?)?;
        let oldest_ms = match parts.next() {
            None => None,
            Some(p) => Some(digits(p.strip_prefix("oldest-")?)?),
        };
        if parts.next().is_some() {
            return None;
        }
        Some(Self {
            offset,
            at,
            oldest_ms,
        })
    }
}

fn digits(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// A directory's floor, from all its markers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Floor {
    /// The highest marker offset.
    pub offset: u64,
    /// When the floor reached `offset`: the earliest marker at it.
    pub moved_at: u64,
    /// From the latest marker at `offset`.
    pub oldest_ms: Option<u64>,
}

/// The floor markers among `names` and the floor they set (`None`
/// without markers). A name with the marker prefix that does not parse
/// is an error.
pub fn floor_of<'a>(
    names: impl IntoIterator<Item = &'a String>,
) -> Result<(Option<Floor>, Vec<FloorMarker>), String> {
    let mut markers = Vec::new();
    for name in names {
        if let Some(parsed) = FloorMarker::parse(name) {
            markers.push(parsed?);
        }
    }
    markers.sort_unstable();
    let floor = markers.last().map(|top| {
        let at_top = markers.iter().filter(|m| m.offset == top.offset);
        Floor {
            offset: top.offset,
            moved_at: at_top.clone().map(|m| m.at).min().unwrap_or(top.at),
            oldest_ms: at_top.max_by_key(|m| m.at).and_then(|m| m.oldest_ms),
        }
    });
    Ok((floor, markers))
}

/// Whether `name` is a floor marker's (it may still fail to parse).
pub fn is_marker(name: &str) -> bool {
    name.starts_with(FLOOR_PREFIX)
}

/// `2026-11-06T10:15:00Z` for unix seconds, for logs, `status` and
/// marker contents.
pub fn utc_rfc3339(unix_secs: u64) -> String {
    let days = (unix_secs / 86_400) as i64;
    let secs = unix_secs % 86_400;
    // Days since 1970-01-01 to a proleptic Gregorian date (Howard
    // Hinnant's civil_from_days).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        secs / 3600,
        secs % 3600 / 60,
        secs % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(offset: u64, at: u64, oldest_ms: Option<u64>) -> FloorMarker {
        FloorMarker {
            offset,
            at,
            oldest_ms,
        }
    }

    #[test]
    fn names_round_trip() {
        let a = m(1234, 1_762_423_200, Some(1_730_887_200_000));
        assert_eq!(
            a.filename(),
            "floor-00000000000000001234.at-1762423200.oldest-1730887200000.json"
        );
        assert_eq!(FloorMarker::parse(&a.filename()), Some(Ok(a)));
        let b = m(0, 5, None);
        assert_eq!(b.filename(), "floor-00000000000000000000.at-5.json");
        assert_eq!(FloorMarker::parse(&b.filename()), Some(Ok(b)));
    }

    #[test]
    fn other_names_are_not_markers_and_broken_markers_are_errors() {
        assert_eq!(
            FloorMarker::parse("00000000000000000000-00000000000000000004.parquet"),
            None
        );
        for bad in [
            "floor-1234.at-5.json",
            "floor-00000000000000001234.json",
            "floor-00000000000000001234.at-.json",
            "floor-00000000000000001234.at-5.oldest-x.json",
            "floor-00000000000000001234.at-5.oldest-1.extra-2.json",
            "floor-00000000000000001234.at-5",
            "floor-0000000000000000123a.at-5.json",
        ] {
            assert!(matches!(FloorMarker::parse(bad), Some(Err(_))), "{bad}");
        }
    }

    #[test]
    fn markers_sort_after_blobs() {
        let blob = crate::naming::blob_filename(u64::MAX, u64::MAX, Some("k1"), "parquet");
        assert!(m(0, 0, None).filename() > blob);
    }

    #[test]
    fn the_highest_marker_sets_the_floor_and_the_earliest_at_it_when_it_moved() {
        let names: Vec<String> = [
            m(10, 100, Some(1)),
            m(30, 300, None),
            m(30, 400, Some(7)),
            m(20, 500, Some(2)),
        ]
        .iter()
        .map(FloorMarker::filename)
        .chain(["00000000000000000030-00000000000000000039.parquet".to_string()])
        .collect();
        let (floor, markers) = floor_of(&names).unwrap();
        assert_eq!(
            floor,
            Some(Floor {
                offset: 30,
                moved_at: 300,
                oldest_ms: Some(7)
            })
        );
        assert_eq!(markers.len(), 4);
        assert_eq!(floor_of(&Vec::<String>::new()).unwrap(), (None, vec![]));
        let err = floor_of(&vec!["floor-x.json".to_string()]).unwrap_err();
        assert!(err.contains("is not a floor marker name"), "{err}");
    }

    #[test]
    fn rfc3339() {
        assert_eq!(utc_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(utc_rfc3339(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(utc_rfc3339(1_762_424_100), "2025-11-06T10:15:00Z");
        assert_eq!(utc_rfc3339(4_107_542_399), "2100-02-28T23:59:59Z");
    }
}
