use std::collections::HashMap;
use std::sync::Mutex;

use once_cell::sync::Lazy;

use crate::log_collector::LogEntry;
use crate::metrics::{MetricEntry, MetricKind, MetricSnapshot};
use crate::store::RingBuffer;

const DEFAULT_SAMPLE_CAPACITY: usize = 300; // 5 min @ 1 sample/sec
const DEFAULT_LOG_CAPACITY: usize = 1000;

/// Global metrics registry singleton.
static GLOBAL: Lazy<Mutex<MetricsRegistry>> = Lazy::new(|| Mutex::new(MetricsRegistry::new()));

/// The central metrics registry that stores all metric entries and logs.
pub struct MetricsRegistry {
    /// Role of this node: "scheduler" or "executor".
    role: String,
    /// Node name/identifier.
    node_name: String,
    /// Unix millis when the registry was created (process start).
    started_at: i64,
    /// All registered metrics, keyed by a composite of name+labels.
    metrics: HashMap<String, MetricEntry>,
    /// Structured log entries.
    logs: RingBuffer<LogEntry>,
    /// Active processor tracking.
    processors: Vec<ProcessorInfo>,
    /// Ballista-specific config.
    concurrent_tasks: usize,
    /// Auto-increment counter for unique processor IDs.
    processor_seq: u64,
}

/// Per-stage accumulated statistics.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct StageStats {
    /// Total accumulated duration in ms.
    pub duration_ms: f64,
    /// Number of times this stage was called.
    pub calls: u64,
}

/// Information about an active .so processor.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ProcessorInfo {
    pub id: String,
    pub job_id: String,
    pub so_path: String,
    pub fn_name: String,
    pub partition: usize,
    pub key: Option<String>,
    pub stage: String, // "init", "feed", "execute", "fetch", "finish", "done"
    pub rows_in: u64,
    pub rows_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub created_at: i64,
    pub last_activity_at: i64,
    pub finished_at: Option<i64>,
    /// Accumulated statistics per stage.
    pub stage_stats: HashMap<String, StageStats>,
}

/// Overview of the node for the dashboard.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeOverview {
    pub role: String,
    pub node_name: String,
    pub started_at: i64,
    pub uptime_secs: f64,
    pub metrics: Vec<MetricSnapshot>,
    /// Number of active .so processors (not finished).
    pub processor_count: usize,
    /// Total .so processors ever created (including finished).
    pub processor_total: usize,
    pub concurrent_tasks: usize,
}

impl MetricsRegistry {
    fn new() -> Self {
        Self {
            role: "unknown".to_string(),
            node_name: "unknown".to_string(),
            started_at: now_millis(),
            metrics: HashMap::new(),
            logs: RingBuffer::new(DEFAULT_LOG_CAPACITY),
            processors: Vec::new(),
            concurrent_tasks: 0,
            processor_seq: 0,
        }
    }

    /// Access the global singleton registry.
    pub fn global() -> &'static Lazy<Mutex<MetricsRegistry>> {
        &GLOBAL
    }

    /// Initialize the registry with node identity.
    pub fn init(&mut self, role: &str, node_name: &str, concurrent_tasks: usize) {
        self.role = role.to_string();
        self.node_name = node_name.to_string();
        self.started_at = now_millis();
        self.concurrent_tasks = concurrent_tasks;
    }

    /// Record a gauge metric value.
    pub fn record_gauge(&mut self, name: &str, value: f64, labels: HashMap<String, String>) {
        self.ensure_entry(name, MetricKind::Gauge, labels.clone(), DEFAULT_SAMPLE_CAPACITY);
        let key = make_key(name, &labels);
        if let Some(entry) = self.metrics.get_mut(&key) {
            entry.record(now_millis(), value);
        }
    }

    /// Record a histogram metric value (e.g., duration).
    pub fn record_histogram(&mut self, name: &str, value: f64, labels: HashMap<String, String>) {
        self.ensure_entry(name, MetricKind::Histogram, labels.clone(), DEFAULT_SAMPLE_CAPACITY);
        let key = make_key(name, &labels);
        if let Some(entry) = self.metrics.get_mut(&key) {
            entry.record(now_millis(), value);
        }
    }

    /// Increment a counter metric.
    pub fn increment_counter(&mut self, name: &str, delta: f64, labels: HashMap<String, String>) {
        self.ensure_entry(name, MetricKind::Counter, labels.clone(), DEFAULT_SAMPLE_CAPACITY);
        let key = make_key(name, &labels);
        if let Some(entry) = self.metrics.get_mut(&key) {
            let current = entry.latest_value().unwrap_or(0.0);
            entry.record(now_millis(), current + delta);
        }
    }

    /// Get snapshot of all metrics (latest values).
    pub fn all_metrics_snapshot(&self) -> Vec<MetricSnapshot> {
        self.metrics.values().map(MetricSnapshot::from).collect()
    }

    /// Get history for a specific metric since a timestamp.
    pub fn metric_history(&self, name: &str, since: i64) -> Vec<crate::metrics::MetricSample> {
        let mut results = Vec::new();
        for (_key, entry) in &self.metrics {
            if entry.name == name {
                let mut samples = entry.history_since(since);
                results.append(&mut samples);
            }
        }
        results.sort_by_key(|s| s.timestamp);
        results
    }

    /// Add a structured log entry.
    pub fn log(&mut self, level: &str, stage: &str, message: &str, labels: HashMap<String, String>) {
        self.logs.push(LogEntry {
            timestamp: now_millis(),
            level: level.to_string(),
            stage: stage.to_string(),
            message: message.to_string(),
            labels,
        });
    }

    /// Get recent log entries.
    pub fn recent_logs(&self, limit: usize, level_filter: Option<&str>) -> Vec<&LogEntry> {
        let entries: Vec<&LogEntry> = self.logs.to_vec();
        let mut filtered: Vec<&LogEntry> = if let Some(lf) = level_filter {
            entries.into_iter().filter(|e| e.level == lf).collect()
        } else {
            entries
        };
        // Return the most recent entries
        let start = filtered.len().saturating_sub(limit);
        filtered.truncate(limit + start);
        filtered[start..].to_vec()
    }

    /// Register a new processor with init duration. Returns the unique processor ID.
    pub fn add_processor(&mut self, job_id: &str, so_path: &str, fn_name: &str, partition: usize, key: Option<&str>, init_duration_ms: f64) -> String {
        self.processor_seq += 1;
        let id = format!("{}-{}", self.processor_seq, partition);
        let now = now_millis();
        let mut stage_stats = HashMap::new();
        stage_stats.insert("init".to_string(), StageStats { duration_ms: init_duration_ms, calls: 1 });
        self.processors.push(ProcessorInfo {
            id: id.clone(),
            job_id: job_id.to_string(),
            so_path: so_path.to_string(),
            fn_name: fn_name.to_string(),
            partition,
            key: key.map(|s| s.to_string()),
            stage: "init".to_string(),
            rows_in: 0,
            rows_out: 0,
            bytes_in: 0,
            bytes_out: 0,
            created_at: now,
            last_activity_at: now,
            finished_at: None,
            stage_stats,
        });
        id
    }

    /// Update a processor's stage, counters, and duration.
    pub fn update_processor(&mut self, id: &str, stage: &str, rows_in: u64, rows_out: u64, bytes_in: u64, bytes_out: u64, duration_ms: f64) {
        if let Some(p) = self.processors.iter_mut().find(|p| p.id == id) {
            p.stage = stage.to_string();
            p.rows_in += rows_in;
            p.rows_out += rows_out;
            p.bytes_in += bytes_in;
            p.bytes_out += bytes_out;
            let stats = p.stage_stats.entry(stage.to_string()).or_default();
            stats.duration_ms += duration_ms;
            stats.calls += 1;
            p.last_activity_at = now_millis();
        }
    }

    /// Mark a processor as finished with duration.
    pub fn finish_processor(&mut self, id: &str, duration_ms: f64) {
        if let Some(p) = self.processors.iter_mut().find(|p| p.id == id) {
            p.stage = "done".to_string();
            let stats = p.stage_stats.entry("finish".to_string()).or_default();
            stats.duration_ms += duration_ms;
            stats.calls += 1;
            let now = now_millis();
            p.last_activity_at = now;
            p.finished_at = Some(now);
        }
    }

    /// Remove finished processors older than a threshold.
    pub fn cleanup_processors(&mut self, max_age_ms: i64) {
        let now = now_millis();
        self.processors.retain(|p| {
            p.stage != "done" || (now - p.last_activity_at) < max_age_ms
        });
    }

    /// Remove metric entries whose latest sample is older than a threshold.
    pub fn cleanup_metrics(&mut self, max_age_ms: i64) {
        let now = now_millis();
        self.metrics.retain(|_, entry| {
            match entry.latest_timestamp() {
                Some(ts) => (now - ts) < max_age_ms,
                None => false,
            }
        });
    }

    /// Get all processor info.
    pub fn get_processors(&self) -> &[ProcessorInfo] {
        &self.processors
    }

    /// Get node overview.
    pub fn overview(&self) -> NodeOverview {
        NodeOverview {
            role: self.role.clone(),
            node_name: self.node_name.clone(),
            started_at: self.started_at,
            uptime_secs: (now_millis() - self.started_at) as f64 / 1000.0,
            metrics: self.all_metrics_snapshot(),
            processor_count: self.processors.iter().filter(|p| p.stage != "done").count(),
            processor_total: self.processors.len(),
            concurrent_tasks: self.concurrent_tasks,
        }
    }

    fn ensure_entry(&mut self, name: &str, kind: MetricKind, labels: HashMap<String, String>, capacity: usize) {
        let key = make_key(name, &labels);
        if !self.metrics.contains_key(&key) {
            self.metrics.insert(key, MetricEntry::new(name, kind, labels, capacity));
        }
    }
}

/// Create a composite key from metric name and sorted labels.
fn make_key(name: &str, labels: &HashMap<String, String>) -> String {
    if labels.is_empty() {
        return name.to_string();
    }
    let mut parts: Vec<(&String, &String)> = labels.iter().collect();
    parts.sort_by_key(|(k, _)| *k);
    let label_str: Vec<String> = parts.iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!("{}{{{}}}", name, label_str.join(","))
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
