//! Newline-delimited JSON envelope.
//!
//! One JSON object per line. Binary fields (`key`, `value`, header
//! values) are base64-encoded so the line is always valid UTF-8 and
//! `jq` can chew on it. Schema is implicit in field names.

use base64::Engine;
use mirror_core::{Header, Record, TimestampType};
use serde::{Deserialize, Serialize};

use crate::EnvelopeError;

#[derive(Debug, Serialize, Deserialize)]
struct PersistedRecord {
    topic: String,
    partition: i32,
    offset: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    timestamp_ms: Option<i64>,
    timestamp_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "opt_base64")]
    key: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "opt_base64")]
    value: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    headers: Vec<PersistedHeader>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedHeader {
    key: String,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "opt_base64")]
    value: Option<Vec<u8>>,
}

impl From<&Record> for PersistedRecord {
    fn from(r: &Record) -> Self {
        PersistedRecord {
            topic: r.topic.clone(),
            partition: r.partition,
            offset: r.source_offset,
            timestamp_ms: r.timestamp_ms,
            timestamp_type: r.timestamp_type.as_str().to_string(),
            key: r.key.clone(),
            value: r.value.clone(),
            headers: r
                .headers
                .iter()
                .map(|h| PersistedHeader {
                    key: h.key.clone(),
                    value: h.value.clone(),
                })
                .collect(),
        }
    }
}

impl PersistedRecord {
    fn into_record(self) -> Record {
        Record {
            topic: self.topic,
            partition: self.partition,
            source_offset: self.offset,
            timestamp_ms: self.timestamp_ms,
            timestamp_type: TimestampType::from_wire(&self.timestamp_type)
                .unwrap_or(TimestampType::NotAvailable),
            key: self.key,
            value: self.value,
            headers: self
                .headers
                .into_iter()
                .map(|h| Header {
                    key: h.key,
                    value: h.value,
                })
                .collect(),
        }
    }
}

pub fn encode_batch(records: &[Record]) -> Result<Vec<u8>, EnvelopeError> {
    let mut out = Vec::with_capacity(records.len() * 128);
    for r in records {
        let pr = PersistedRecord::from(r);
        serde_json::to_writer(&mut out, &pr).map_err(|e| EnvelopeError::Encode(e.to_string()))?;
        out.push(b'\n');
    }
    Ok(out)
}

pub fn decode_batch(bytes: &[u8]) -> Result<Vec<Record>, EnvelopeError> {
    let mut out = Vec::new();
    for line in bytes.split(|b| *b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let pr: PersistedRecord =
            serde_json::from_slice(line).map_err(|e| EnvelopeError::Decode(e.to_string()))?;
        out.push(pr.into_record());
    }
    Ok(out)
}

mod opt_base64 {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(bytes) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
                encoded.serialize(s)
            }
            None => s.serialize_none(),
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
        let opt: Option<String> = Option::deserialize(d)?;
        match opt {
            None => Ok(None),
            Some(s) => base64::engine::general_purpose::STANDARD
                .decode(s.as_bytes())
                .map(Some)
                .map_err(serde::de::Error::custom),
        }
    }
}

// Make sure the Engine import is consumed in case opt_base64 changes.
#[allow(dead_code)]
fn _engine_check() -> impl Engine {
    base64::engine::general_purpose::STANDARD
}
