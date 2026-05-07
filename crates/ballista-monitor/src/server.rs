use std::net::SocketAddr;
use std::time::Duration;

use axum::extract::{Path, Query};
use axum::response::{Html, Json};
use axum::routing::get;
use axum::Router;
use serde::Deserialize;
use tower_http::cors::{Any, CorsLayer};

use crate::collector;
use crate::dashboard;
use crate::MetricsRegistry;

/// Start the monitoring HTTP server.
///
/// - `role`: "scheduler" or "executor"
/// - `node_name`: human-readable name for this node
/// - `addr`: address to bind (e.g., "0.0.0.0:8080")
/// - `concurrent_tasks`: for executor, the max concurrent task count
pub async fn start_monitor_server(
    role: &str,
    node_name: &str,
    addr: &str,
    concurrent_tasks: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize the global registry
    {
        let mut r = MetricsRegistry::global().lock().unwrap();
        r.init(role, node_name, concurrent_tasks);
    }

    // Start system metrics collector (1s interval)
    collector::start_system_collector(Duration::from_secs(1));

    // Build CORS layer
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Build router
    let app = Router::new()
        .route("/", get(dashboard_handler))
        .route("/api/overview", get(overview_handler))
        .route("/api/metrics", get(metrics_handler))
        .route("/api/metrics/{name}/history", get(metrics_history_handler))
        .route("/api/processors", get(processors_handler))
        .route("/api/logs", get(logs_handler))
        .layer(cors);

    let socket_addr: SocketAddr = addr.parse()?;
    let listener = tokio::net::TcpListener::bind(socket_addr).await?;
    eprintln!("Monitor server listening on http://{socket_addr}");

    axum::serve(listener, app).await?;
    Ok(())
}

/// GET / - Dashboard HTML
async fn dashboard_handler() -> Html<&'static str> {
    Html(dashboard::DASHBOARD_HTML)
}

/// GET /api/overview
async fn overview_handler() -> Json<crate::registry::NodeOverview> {
    let r = MetricsRegistry::global().lock().unwrap();
    Json(r.overview())
}

/// GET /api/metrics
async fn metrics_handler() -> Json<Vec<crate::metrics::MetricSnapshot>> {
    let r = MetricsRegistry::global().lock().unwrap();
    Json(r.all_metrics_snapshot())
}

/// GET /api/metrics/{name}/history?since=unix_ms
#[derive(Deserialize)]
struct HistoryQuery {
    since: Option<i64>,
}

async fn metrics_history_handler(
    Path(name): Path<String>,
    Query(query): Query<HistoryQuery>,
) -> Json<Vec<crate::metrics::MetricSample>> {
    let since = query.since.unwrap_or(0);
    let r = MetricsRegistry::global().lock().unwrap();
    Json(r.metric_history(&name, since))
}

/// GET /api/processors
async fn processors_handler() -> Json<Vec<crate::registry::ProcessorInfo>> {
    let r = MetricsRegistry::global().lock().unwrap();
    Json(r.get_processors().to_vec())
}

/// GET /api/logs?limit=100&level=info
#[derive(Deserialize)]
struct LogsQuery {
    limit: Option<usize>,
    level: Option<String>,
}

async fn logs_handler(Query(query): Query<LogsQuery>) -> Json<Vec<crate::log_collector::LogEntry>> {
    let limit = query.limit.unwrap_or(100);
    let r = MetricsRegistry::global().lock().unwrap();
    let logs: Vec<_> = r
        .recent_logs(limit, query.level.as_deref())
        .into_iter()
        .cloned()
        .collect();
    Json(logs)
}
