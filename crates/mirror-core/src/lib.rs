//! Core mirror loop and trait surface.
//!
//! The loop is generic over [`Source`] and [`Sink`]. The single
//! correctness invariant is enforced here, in [`run_mirror`]:
//!
//! 1. On startup, ask the sink for `next_expected_offset()`. That is
//!    the source offset we will seek to and the source offset we will
//!    refuse to write anything else at.
//! 2. Every record produced by the source must carry that same offset
//!    (`Record::source_offset`). A gap is a hard error.
//! 3. The sink is contracted to write the record only if the
//!    destination is still at exactly `record.source_offset`, and to
//!    error otherwise.
//! 4. On an idle poll (no record available), we re-read the sink's
//!    `next_expected_offset()` and require it to still equal what we
//!    expect. This catches external topic resets / out-of-band writes.

use async_trait::async_trait;
use thiserror::Error;

pub mod cache;
pub mod mock;
pub mod tee;

pub use cache::{CacheBinding, CacheState};
pub use tee::TeeSink;

/// Per-mirror Prometheus labels. `topic` and `partition` together
/// uniquely identify the data stream and join cleanly with broker-
/// side exporters (kafka_exporter, etc.) — the mirror's operator-
/// chosen `name` is *not* a metric label, it lives in `tracing`
/// logs only.
#[derive(Debug, Clone)]
pub struct MetricLabels {
    pub topic: String,
    pub partition: u32,
}

tokio::task_local! {
    /// Set by the supervisor (mirror-bin) inside the spawn closure so
    /// every metric emitted from this mirror's loop and sink is
    /// automatically labeled with `topic` and `partition`. If unset
    /// (e.g. inside `cargo test` outside the supervisor), the labels
    /// fall back to `unknown` / `0` via [`current_labels`].
    pub static MIRROR_LABELS: MetricLabels;
}

/// Resolve the current mirror's labels from the task-local as
/// `(topic, partition_as_string)`, falling back to
/// `("unknown", "0")` when no scope is set.
pub fn current_labels() -> (String, String) {
    MIRROR_LABELS
        .try_with(|l| (l.topic.clone(), l.partition.to_string()))
        .unwrap_or_else(|_| ("unknown".into(), "0".into()))
}

/// A record in transit. `source_offset` is the partition offset on
/// the *source* topic; the loop and the sink both gate on this value.
/// `topic` and `partition` are the source's identity and propagate
/// through to FS/S3 envelopes so each record is self-describing.
/// `timestamp_type` mirrors librdkafka's distinction so a future
/// replay tool can tell whether the broker assigned the timestamp
/// (LogAppendTime) or the producer did (CreateTime).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub topic: String,
    pub partition: i32,
    pub source_offset: u64,
    pub timestamp_ms: Option<i64>,
    pub timestamp_type: TimestampType,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<Header>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampType {
    CreateTime,
    LogAppendTime,
    NotAvailable,
}

impl TimestampType {
    /// Canonical string used in the wire envelope (NDJSON / Parquet).
    pub fn as_str(self) -> &'static str {
        match self {
            TimestampType::CreateTime => "create_time",
            TimestampType::LogAppendTime => "log_append_time",
            TimestampType::NotAvailable => "not_available",
        }
    }

    pub fn from_wire(s: &str) -> Option<Self> {
        match s {
            "create_time" => Some(TimestampType::CreateTime),
            "log_append_time" => Some(TimestampType::LogAppendTime),
            "not_available" => Some(TimestampType::NotAvailable),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

/// Which buffer trigger caused a blob sink (FS / S3) to flush. Used
/// only as a label on the `flushed batch` log line so operators can
/// tell why a given snapshot was emitted without grepping for the
/// matching threshold in the config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushTrigger {
    /// `flush.max-offsets` reached: the buffer accumulated as many
    /// records as the config allows.
    MaxOffsets,
    /// `flush.max-bytes` reached: the buffered payload size hit the
    /// configured byte cap.
    MaxBytes,
    /// `flush.max-time-ms` elapsed since the first record landed in
    /// the buffer.
    MaxTime,
    /// `flush.daily.at-utc` wall-clock boundary crossed.
    Daily,
    /// Explicit `Sink::flush` (graceful shutdown, end-of-test, or any
    /// other operator-driven flush).
    Explicit,
}

impl FlushTrigger {
    pub fn as_str(self) -> &'static str {
        match self {
            FlushTrigger::MaxOffsets => "max-offsets",
            FlushTrigger::MaxBytes => "max-bytes",
            FlushTrigger::MaxTime => "max-time",
            FlushTrigger::Daily => "daily",
            FlushTrigger::Explicit => "explicit",
        }
    }
}

/// Topic-schema declaration for a record column (`key` or `value`).
///
/// This is the runtime representation shared by all sinks. The
/// validation contract is identical across destination types; how
/// each sink *stores* the column (passthrough for Kafka, base64-into-
/// Utf8 for Parquet `Bytes`, etc.) is destination-specific and lives
/// in the sink crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ColumnType {
    /// Arbitrary bytes. No validation.
    Bytes,
    /// UTF-8 string. Non-UTF-8 input is a hard error pointing at the
    /// offending source offset.
    #[default]
    Utf8,
    /// UTF-8 string carrying a JSON document. Validation enforces
    /// UTF-8 only; the payload is *not* parsed.
    Json,
    /// UTF-8 string carrying a JSON document, parseability-gated. In
    /// addition to the UTF-8 check the payload is fed through
    /// `serde_json` (with `IgnoredAny`, so no `Value` tree is
    /// allocated) and unparseable input is a hard error.
    JsonParseable,
}

impl ColumnType {
    /// Run the validation contract for this column type against a
    /// single payload. `None` is always valid (null columns).
    /// `column` and `source_offset` appear in error messages so the
    /// operator can find the offending record.
    pub fn validate(
        &self,
        column: &str,
        source_offset: u64,
        payload: Option<&[u8]>,
    ) -> Result<(), String> {
        let Some(bytes) = payload else {
            return Ok(());
        };
        match self {
            ColumnType::Bytes => Ok(()),
            ColumnType::Utf8 | ColumnType::Json => {
                std::str::from_utf8(bytes).map(|_| ()).map_err(|e| {
                    format!("{column} at source offset {source_offset} is not valid UTF-8: {e}")
                })
            }
            ColumnType::JsonParseable => {
                let s = std::str::from_utf8(bytes).map_err(|e| {
                    format!("{column} at source offset {source_offset} is not valid UTF-8: {e}")
                })?;
                serde_json::from_str::<serde::de::IgnoredAny>(s)
                    .map(|_| ())
                    .map_err(|e| {
                        format!(
                            "{column} at source offset {source_offset} is not parseable JSON: {e}"
                        )
                    })
            }
        }
    }
}

/// A Kafka-shaped record stream pinned to one (topic, partition).
#[async_trait]
pub trait Source: Send {
    /// Position the source so the next `poll_one` returns the record
    /// at `next_offset` (or `None` until one is available).
    async fn seek(&mut self, next_offset: u64) -> Result<(), SourceError>;

    /// Wait up to an implementation-defined poll timeout for the next
    /// record. `Ok(None)` means the window elapsed without one — the
    /// loop will use that as a heartbeat to revalidate the sink.
    async fn poll_one(&mut self) -> Result<Option<Record>, SourceError>;

    /// Earliest offset still retained by the source (Kafka "low
    /// watermark"). On a compacted or `delete-records`-trimmed topic
    /// this can be greater than zero. The run loop consults this at
    /// startup to decide whether seeking to the sink's
    /// `next_expected_offset()` is feasible. Default `Ok(0)` is the
    /// safe choice for tests and any source that doesn't trim.
    async fn low_watermark(&mut self) -> Result<u64, SourceError> {
        Ok(0)
    }
}

/// A destination for exactly-once mirroring. The sink owns the truth
/// about "where we are" — the loop trusts `next_expected_offset`.
#[async_trait]
pub trait Sink: Send {
    /// The source offset the destination will accept next. Must be
    /// re-derived from durable destination state, not cached in memory
    /// (otherwise the idle-drift check is meaningless).
    async fn next_expected_offset(&mut self) -> Result<u64, SinkError>;

    /// Atomically commit `record` at exactly `record.source_offset`.
    /// MUST fail if the destination is not at that offset at the
    /// moment of write.
    async fn write(&mut self, record: Record) -> Result<(), SinkError>;

    /// Flush any buffered state so it's durable. Called on graceful
    /// shutdown. Default is a no-op for sinks that don't buffer
    /// (e.g. Kafka, where every write is durable on return).
    async fn flush(&mut self) -> Result<(), SinkError> {
        Ok(())
    }

    /// Whether this sink can correctly resume from a source whose
    /// earliest available offset is greater than the sink's
    /// `next_expected_offset()`. True for compaction:log destinations,
    /// where the destination is a key→latest-value snapshot and
    /// missing earlier offsets are harmless (their keys are either
    /// already represented in the snapshot or have been superseded
    /// by later writes). False for append-mode destinations, where
    /// missing earlier offsets mean an incomplete chain and the
    /// bootstrap must fail loudly.
    fn allows_compacted_source(&self) -> bool {
        false
    }

    /// Called by the run loop to advance this sink's internal
    /// "next expected offset" to a higher value. The sink must update
    /// state so that:
    ///   1. the next [`Self::next_expected_offset`] call returns the
    ///      new value (idle-drift check stays consistent);
    ///   2. [`Self::write`] accepts the next incoming record at the
    ///      new value (the per-record gate stays consistent);
    ///   3. blob/file naming reflects the new value as the new
    ///      starting offset for the snapshot range.
    ///
    /// Two call sites today, both guarded by
    /// `allows_compacted_source() == true`:
    ///
    /// - **Bootstrap pre-align.** The run loop's bootstrap branch
    ///   calls this with the source's `low_watermark` when the
    ///   source has been compacted/trimmed past the sink's current
    ///   durable position (`sink.next_expected_offset() < low_watermark`).
    ///   The argument is therefore the broker's reported low
    ///   watermark.
    /// - **First-delivery alignment.** Inside the run loop, when the
    ///   broker delivers an offset *above* `expected` (the
    ///   `cleanup.policy=compact` case where `LogStartOffset = 0`
    ///   masks the actual deliverable start), this is called with
    ///   that delivered offset before the record is written.
    ///
    /// Either way the argument is "the new authoritative start offset",
    /// not specifically a watermark. Default impl is a no-op (sinks
    /// that don't override `allows_compacted_source` never see this
    /// call).
    async fn align_to_source_low_watermark(&mut self, low_watermark: u64) -> Result<(), SinkError> {
        let _ = low_watermark;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("source transport: {0}")]
    Transport(String),
}

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("destination advanced: expected next-offset {expected}, found {actual}")]
    UnexpectedPosition { expected: u64, actual: u64 },
    #[error("sink transport: {0}")]
    Transport(String),
}

#[derive(Debug, Error)]
pub enum MirrorError {
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error(transparent)]
    Sink(#[from] SinkError),
    /// Source delivered an offset *below* `expected`. Always a hard
    /// error: a Kafka client bug, a producer that rewound, or a
    /// destination chain that has somehow advanced past the broker.
    #[error("source delivered offset {got}, expected at least {expected} (went backwards)")]
    SourceWentBackwards { expected: u64, got: u64 },
    /// Source delivered an offset *above* `expected`. Hard error in
    /// append mode (would leave a gap in the destination chain).
    /// Recoverable under `compaction: log`: the run loop aligns the
    /// sink to the delivered offset and continues — the broker's
    /// `LogStartOffset` reports 0 for a `cleanup.policy=compact`
    /// topic even when the earliest deliverable record is much later
    /// (compaction deduplicates by key but does not advance the
    /// segment start), so the bootstrap pre-align can only be a hint
    /// and the first-delivery offset is the authoritative starting
    /// point.
    #[error("source delivered offset {got}, expected {expected} (gap above expected)")]
    SourceGapAboveExpected { expected: u64, got: u64 },
    /// Sink's view of next-expected-offset diverged from what we
    /// believed while we were idle. Indicates an out-of-band write or
    /// a topic reset.
    #[error("destination drift while idle: expected next-offset {expected}, found {actual}")]
    DestinationDrift { expected: u64, actual: u64 },
    /// Source's earliest available offset is greater than the sink's
    /// next-expected-offset, and the sink is not willing to skip
    /// records (i.e. it's not a compaction:log destination). This
    /// fires at bootstrap on a compacted or delete-records-trimmed
    /// source topic when the mirror is configured for append mode —
    /// it would leave a gap in the destination chain, which append
    /// mode forbids.
    #[error(
        "source has been compacted past start offset: sink wants {start}, broker's earliest is {low_watermark}. \
         Either set `compaction: log` on this mirror (destination becomes a key→latest-value snapshot, \
         missing earlier offsets are harmless), or seed the destination from a backup that covers up to \
         offset {low_watermark_minus_one}."
    )]
    SourceCompactedBelowExpected {
        start: u64,
        low_watermark: u64,
        low_watermark_minus_one: u64,
    },
}

/// How often the loop emits an INFO-level "heartbeat" log line. This
/// is the operator's `kubectl logs` heartbeat — without it, a quiet
/// mirror (no source traffic, or buffered records that haven't
/// tripped a flush trigger yet) looks indistinguishable from a stuck
/// one. Override via the `MIRROR_V3_HEARTBEAT_SECS` env var; set to
/// `0` to disable.
pub const DEFAULT_HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Read the heartbeat interval from `MIRROR_V3_HEARTBEAT_SECS`,
/// falling back to [`DEFAULT_HEARTBEAT_INTERVAL`]. A value of `0`
/// disables heartbeats.
pub fn heartbeat_interval_from_env() -> std::time::Duration {
    match std::env::var("MIRROR_V3_HEARTBEAT_SECS").ok().as_deref() {
        Some(s) => match s.parse::<u64>() {
            Ok(secs) => std::time::Duration::from_secs(secs),
            Err(_) => DEFAULT_HEARTBEAT_INTERVAL,
        },
        None => DEFAULT_HEARTBEAT_INTERVAL,
    }
}

/// Drive the mirror loop until `shutdown` resolves or an error is
/// returned. On graceful shutdown, the loop calls `sink.flush()` so
/// buffered batches (FS, S3) become durable. Use
/// `std::future::pending::<()>()` for a "run forever" caller (tests).
///
/// Heartbeat interval is read from the environment; pass a fixed
/// interval via [`run_mirror_with_heartbeat`] if you need explicit
/// control (e.g. tests that want to disable heartbeats).
pub async fn run_mirror<S, K, F>(source: S, sink: K, shutdown: F) -> Result<(), MirrorError>
where
    S: Source,
    K: Sink,
    F: std::future::Future<Output = ()> + Send,
{
    run_mirror_with_heartbeat(source, sink, shutdown, heartbeat_interval_from_env()).await
}

pub async fn run_mirror_with_heartbeat<S, K, F>(
    mut source: S,
    mut sink: K,
    shutdown: F,
    heartbeat_interval: std::time::Duration,
) -> Result<(), MirrorError>
where
    S: Source,
    K: Sink,
    F: std::future::Future<Output = ()> + Send,
{
    let sink_start = sink.next_expected_offset().await?;
    let low_watermark = source.low_watermark().await?;
    let compaction_mode = if sink.allows_compacted_source() {
        "log"
    } else {
        "append"
    };
    let expected = if sink_start < low_watermark {
        if sink.allows_compacted_source() {
            tracing::warn!(
                start_offset = sink_start,
                low_watermark,
                compaction = compaction_mode,
                "source has been compacted past start_offset; resuming from broker's earliest available offset"
            );
            // Align the sink's internal "next expected" with the
            // low watermark so the per-record gate and idle-drift
            // checks stay consistent from here on.
            sink.align_to_source_low_watermark(low_watermark).await?;
            low_watermark
        } else {
            return Err(MirrorError::SourceCompactedBelowExpected {
                start: sink_start,
                low_watermark,
                low_watermark_minus_one: low_watermark.saturating_sub(1),
            });
        }
    } else {
        sink_start
    };
    tracing::info!(
        start_offset = expected,
        sink_next_expected = sink_start,
        source_low_watermark = low_watermark,
        compaction = compaction_mode,
        "starting mirror"
    );
    source.seek(expected).await?;
    let mut expected = expected;
    let mut last_heartbeat_offset = expected;
    // Initial /metrics state for this mirror:
    //   - `_offset_verified` carries the destination's startup
    //     position so an idle mirror is visible to Prometheus.
    //   - `_offset_inflight_retry` is the current attempt index
    //     (1-based) for the in-flight write, gauge, resets to 0 on
    //     success. > 0 = the destination is having problems. Today
    //     we don't add a retry layer at the sink boundary so the
    //     visible value is always 0; the slot is reserved so
    //     dashboards can be pre-built. A future retry layer should
    //     `set(n)` before each attempt and `set(0)` on success.
    let (topic, partition) = current_labels();
    metrics::gauge!(
        "mirror_v3_destination_offset_verified",
        "topic" => topic.clone(),
        "partition" => partition.clone(),
    )
    .set(expected as f64);
    metrics::gauge!(
        "mirror_v3_destination_offset_inflight_retry",
        "topic" => topic.clone(),
        "partition" => partition.clone(),
    )
    .set(0.0);

    tokio::pin!(shutdown);
    let mut heartbeat = if heartbeat_interval.is_zero() {
        None
    } else {
        let mut iv = tokio::time::interval(heartbeat_interval);
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Some(iv)
    };

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                tracing::info!("shutdown requested; flushing sink");
                sink.flush().await?;
                return Ok(());
            }
            _ = async {
                match heartbeat.as_mut() {
                    Some(iv) => { iv.tick().await; }
                    None => std::future::pending::<()>().await,
                }
            } => {
                let progressed = expected - last_heartbeat_offset;
                // Heartbeat fires per clock interval, not per record
                // batch. SRE-facing liveness is the
                // `mirror_v3_destination_offset_verified` gauge and
                // the existing `flushed batch` line; the heartbeat is
                // primarily diagnostic for the "no records arriving"
                // case. DEBUG keeps it discoverable via
                // `RUST_LOG=mirror_core=debug` without taking a slot
                // in default operator logs.
                tracing::debug!(
                    expected_offset = expected,
                    progressed,
                    "heartbeat"
                );
                last_heartbeat_offset = expected;
            }
            poll_result = source.poll_one() => {
                match poll_result? {
                    Some(record) => {
                        if record.source_offset < expected {
                            // Always a hard error: cannot rewrite a
                            // record we've already committed to the
                            // destination chain.
                            return Err(MirrorError::SourceWentBackwards {
                                expected,
                                got: record.source_offset,
                            });
                        }
                        if record.source_offset > expected {
                            if sink.allows_compacted_source() {
                                // `cleanup.policy=compact` leaves
                                // `LogStartOffset` at 0 even when the
                                // earliest deliverable record is much
                                // later; the bootstrap pre-align (which
                                // uses `low_watermark`) misses this
                                // case and gaps also surface mid-stream
                                // every time the broker dropped a
                                // superseded record. The sink's `write`
                                // accepts forward gaps under
                                // `compaction:log` so we only bump the
                                // local `expected` tracker here. The
                                // bootstrap-time `align_to_source_low_watermark`
                                // is still called (with an empty
                                // buffer) so the first snapshot file's
                                // `from` reflects the broker's low
                                // watermark when that path applies.
                                //
                                // Not logged per-record: a compacted
                                // topic can have a gap on every
                                // delivered record (one per surviving
                                // key after upstream dedup), so any
                                // log level here scales with millions
                                // of lines per restart. Observability
                                // for gap rate is the dedicated
                                // counter below — plot a rate or
                                // alert on a threshold rather than
                                // reading logs. The startup `loop
                                // start … compaction="log"` INFO
                                // line is the one-shot "expect gaps
                                // here" signal.
                                let (topic_l, partition_l) = current_labels();
                                metrics::counter!(
                                    "mirror_v3_source_offset_gap_records_total",
                                    "topic" => topic_l,
                                    "partition" => partition_l,
                                )
                                .increment(1);
                                expected = record.source_offset;
                            } else {
                                return Err(MirrorError::SourceGapAboveExpected {
                                    expected,
                                    got: record.source_offset,
                                });
                            }
                        }
                        sink.write(record).await?;
                        expected = expected
                            .checked_add(1)
                            .expect("source offset overflowed u64");
                        // Successful write -> reset the retry gauge
                        // back to 0 (idempotent when no retry layer
                        // is wired up yet, but it's the contract).
                        metrics::gauge!(
                            "mirror_v3_destination_offset_inflight_retry",
                            "topic" => topic.clone(),
                            "partition" => partition.clone(),
                        )
                        .set(0.0);
                        metrics::counter!(
                            "mirror_v3_destination_records_total",
                            "topic" => topic.clone(),
                            "partition" => partition.clone(),
                        )
                        .increment(1);
                    }
                    None => {
                        let current = sink.next_expected_offset().await?;
                        if current != expected {
                            return Err(MirrorError::DestinationDrift {
                                expected,
                                actual: current,
                            });
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod column_type_tests {
    use super::ColumnType;

    #[test]
    fn null_payload_always_validates() {
        for ct in [
            ColumnType::Bytes,
            ColumnType::Utf8,
            ColumnType::Json,
            ColumnType::JsonParseable,
        ] {
            assert!(ct.validate("key", 7, None).is_ok(), "ct={ct:?}");
        }
    }

    #[test]
    fn bytes_accepts_anything() {
        assert!(ColumnType::Bytes
            .validate("value", 0, Some(&[0xff, 0xfe, 0xfd]))
            .is_ok());
    }

    #[test]
    fn utf8_rejects_non_utf8_with_offending_offset() {
        let err = ColumnType::Utf8
            .validate("value", 42, Some(&[0xff]))
            .expect_err("must reject");
        assert!(err.contains("value") && err.contains("offset 42") && err.contains("UTF-8"));
    }

    #[test]
    fn json_does_not_parse_payload() {
        // Valid UTF-8 but not parseable JSON — Json must accept it.
        assert!(ColumnType::Json
            .validate("value", 0, Some(b"{this is not json"))
            .is_ok());
    }

    #[test]
    fn json_parseable_accepts_valid_json() {
        for payload in [
            br#"{"a":1}"#.as_slice(),
            br#"[]"#.as_slice(),
            br#""s""#.as_slice(),
            br#"42"#.as_slice(),
            br#"null"#.as_slice(),
        ] {
            assert!(
                ColumnType::JsonParseable
                    .validate("value", 0, Some(payload))
                    .is_ok(),
                "valid JSON rejected: {:?}",
                std::str::from_utf8(payload)
            );
        }
    }

    #[test]
    fn json_parseable_rejects_malformed_json() {
        let err = ColumnType::JsonParseable
            .validate("value", 5, Some(b"{unbalanced"))
            .expect_err("must reject");
        assert!(
            err.contains("value") && err.contains("offset 5") && err.contains("parseable JSON")
        );
    }

    #[test]
    fn json_parseable_reports_utf8_error_before_json_error() {
        let err = ColumnType::JsonParseable
            .validate("value", 9, Some(&[0xff]))
            .expect_err("must reject");
        assert!(
            err.contains("UTF-8") && !err.contains("parseable JSON"),
            "got: {err}"
        );
    }
}
