use std::collections::HashMap;

use serde::Serialize;

/// Metric type classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

/// A single data point for a metric.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct MetricSample {
    pub timestamp: i64, // milliseconds since epoch
    pub value: f64,
}

/// A registered metric with its metadata and time-series data.
pub struct MetricEntry {
    pub name: String,
    pub kind: MetricKind,
    pub labels: HashMap<String, String>,
    pub samples: crate::store::RingBuffer<MetricSample>,
}

impl MetricEntry {
    pub fn new(name: &str, kind: MetricKind, labels: HashMap<String, String>, capacity: usize) -> Self {
        Self {
            name: name.to_string(),
            kind,
            labels,
            samples: crate::store::RingBuffer::new(capacity),
        }
    }

    /// Record a new sample.
    pub fn record(&mut self, timestamp: i64, value: f64) {
        self.samples.push(MetricSample { timestamp, value });
    }

    /// Get the latest sample value, if any.
    pub fn latest_value(&self) -> Option<f64> {
        self.samples.last().map(|s| s.value)
    }

    /// Get the latest sample timestamp, if any.
    pub fn latest_timestamp(&self) -> Option<i64> {
        self.samples.last().map(|s| s.timestamp)
    }

    /// Get samples since a given timestamp (ms since epoch).
    pub fn history_since(&self, since: i64) -> Vec<MetricSample> {
        self.samples
            .to_vec()
            .into_iter()
            .filter(|s| s.timestamp >= since)
            .copied()
            .collect()
    }
}

/// Serializable snapshot of a metric entry (without full history).
#[derive(Debug, Clone, Serialize)]
pub struct MetricSnapshot {
    pub name: String,
    pub kind: MetricKind,
    pub labels: HashMap<String, String>,
    pub value: Option<f64>,
    pub timestamp: Option<i64>,
}

impl From<&MetricEntry> for MetricSnapshot {
    fn from(entry: &MetricEntry) -> Self {
        let latest = entry.samples.last();
        Self {
            name: entry.name.clone(),
            kind: entry.kind,
            labels: entry.labels.clone(),
            value: latest.map(|s| s.value),
            timestamp: latest.map(|s| s.timestamp),
        }
    }
}
