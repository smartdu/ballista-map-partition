use std::collections::HashMap;

use serde::Serialize;

/// A structured log entry for .so processor lifecycle events.
#[derive(Debug, Clone, Serialize)]
pub struct LogEntry {
    pub timestamp: i64,
    pub level: String,
    pub stage: String,
    pub message: String,
    pub labels: HashMap<String, String>,
}
