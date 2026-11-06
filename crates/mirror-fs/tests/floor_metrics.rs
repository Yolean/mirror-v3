//! The floor gauges a running mirror exports, at open and when a drift
//! check sees the floor move. One test in its own binary: the recorder
//! is process-global.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder, SharedString,
    Unit,
};
use mirror_core::{Record, Sink, TimestampType};
use mirror_envelope::{ColumnType, Format, ParquetCompression};
use mirror_fs::floor::FloorMarker;
use mirror_fs::{naming, FilesystemSink, FilesystemSinkConfig, FlushTriggers};

#[derive(Default)]
struct Value(AtomicU64);

impl GaugeFn for Value {
    fn increment(&self, value: f64) {
        self.set(self.get() + value)
    }
    fn decrement(&self, value: f64) {
        self.set(self.get() - value)
    }
    fn set(&self, value: f64) {
        self.0.store(value.to_bits(), Ordering::SeqCst)
    }
}

impl CounterFn for Value {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::SeqCst);
    }
    fn absolute(&self, value: u64) {
        self.0.store(value, Ordering::SeqCst);
    }
}

impl Value {
    fn get(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::SeqCst))
    }
}

/// Gauges by name; labels are not kept (one sink per test).
#[derive(Default)]
struct Gauges(Mutex<HashMap<String, Arc<Value>>>);

impl Gauges {
    fn get(&self, name: &str) -> Option<f64> {
        self.0.lock().unwrap().get(name).map(|v| v.get())
    }
}

impl Recorder for &'static Gauges {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(Value::default()))
    }
    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        let value = Arc::clone(
            self.0
                .lock()
                .unwrap()
                .entry(key.name().to_string())
                .or_default(),
        );
        Gauge::from_arc(value)
    }
    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

fn gauges() -> &'static Gauges {
    static GAUGES: OnceLock<&'static Gauges> = OnceLock::new();
    GAUGES.get_or_init(|| {
        let g: &'static Gauges = Box::leak(Box::default());
        metrics::set_global_recorder(g).expect("the only recorder in this binary");
        g
    })
}

fn rec(offset: u64) -> Record {
    Record {
        topic: "audit".into(),
        partition: 0,
        source_offset: offset,
        timestamp_ms: Some(1_700_000_000_000),
        timestamp_type: TimestampType::CreateTime,
        key: None,
        value: Some(b"v".to_vec()),
        headers: vec![],
    }
}

#[tokio::test]
async fn the_floor_is_exported_at_open_and_when_it_moves() {
    let g = gauges();
    let tmp = tempfile::tempdir().unwrap();
    let cfg = FilesystemSinkConfig {
        root: tmp.path().to_path_buf(),
        destination_name: "audit".into(),
        partition: 0,
        format: Format::Ndjson,
        compression: ParquetCompression::Zstd1,
        keys: ColumnType::Utf8,
        values: ColumnType::Utf8,
        compaction: None,
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets: 2,
            daily_at_utc_seconds: None,
        },
    };
    let dir = naming::partition_dir(tmp.path(), "audit", 0);
    let mut sink = FilesystemSink::open(cfg.clone()).unwrap();
    for o in 0..6 {
        sink.write(rec(o)).await.unwrap();
    }
    drop(sink);
    assert_eq!(
        g.get("mirror_v3_destination_floor_offset"),
        None,
        "no floor, no gauge"
    );

    let first = FloorMarker {
        offset: 2,
        at: 1_800_000_000,
        oldest_ms: Some(1_700_000_002_000),
    };
    std::fs::write(dir.join(first.filename()), b"{}").unwrap();
    let sink = FilesystemSink::open(cfg).unwrap();
    let mut sink = sink.with_drift_check_interval(Duration::ZERO);
    assert_eq!(g.get("mirror_v3_destination_floor_offset"), Some(2.0));
    assert_eq!(
        g.get("mirror_v3_destination_floor_moved_timestamp_seconds"),
        Some(1_800_000_000.0)
    );
    assert_eq!(
        g.get("mirror_v3_destination_floor_oldest_record_timestamp_seconds"),
        Some(1_700_000_002.0)
    );

    // trim moves it while the mirror runs.
    let second = FloorMarker {
        offset: 4,
        at: 1_800_086_400,
        oldest_ms: None,
    };
    std::fs::write(dir.join(second.filename()), b"{}").unwrap();
    assert_eq!(sink.next_expected_offset().await.unwrap(), 6);
    assert_eq!(g.get("mirror_v3_destination_floor_offset"), Some(4.0));
    assert_eq!(
        g.get("mirror_v3_destination_floor_moved_timestamp_seconds"),
        Some(1_800_086_400.0)
    );
    assert!(g
        .get("mirror_v3_destination_floor_oldest_record_timestamp_seconds")
        .unwrap()
        .is_nan());
}
