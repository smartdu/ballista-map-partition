use std::net::AddrParseError;
use std::sync::Arc;

use ballista_core::error::BallistaError;
use ballista_core::object_store::{
    session_config_with_s3_support, session_state_with_s3_support,
};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;
use ballista_map_partition::codec::extension::{
    ExtendedBallistaLogicalCodec, ExtendedBallistaPhysicalCodec,
};
use ballista_map_partition::physical_optimizer::EnforceDistributeBy;
use ballista_map_partition::planner::extension_planner::QueryPlannerWithExtensions;
use datafusion::execution::{SessionState, SessionStateBuilder};

/// 分布式计算调度器 — 集成 S3 对象存储 + map_partition 算子
///
/// 启动方式：
///   cargo run --example distributed_compute_scheduler
///
/// 启动方式（启用监控）：
///   cargo run --example distributed_compute_scheduler --features monitoring -- --monitor-port 8080
///
/// 参数说明：
///       --monitor-port PORT     监控 Web 服务端口 (默认 8080, 需启用 monitoring feature)

#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    let mut monitor_port: u16 = 8080;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--monitor-port" => {
                i += 1;
                monitor_port = args[i].parse().expect("invalid monitor port number");
            }
            _ => {}
        }
        i += 1;
    }

    // Start monitor server (no-op if monitoring feature is not enabled)
    #[cfg(feature = "monitoring")]
    {
        let monitor_addr = format!("0.0.0.0:{monitor_port}");
        log::info!("Starting monitor server on {monitor_addr}");
        tokio::spawn(async move {
            if let Err(e) = ballista_monitor::start_monitor_server(
                "scheduler",
                "scheduler",
                &monitor_addr,
                0,
            )
            .await
            {
                log::error!("Monitor server error: {e}");
            }
        });
    }

    let config: SchedulerConfig = SchedulerConfig {
        override_logical_codec: Some(Arc::new(ExtendedBallistaLogicalCodec::default())),
        override_physical_codec: Some(Arc::new(ExtendedBallistaPhysicalCodec::default())),
        override_config_producer: Some(Arc::new(session_config_with_s3_support)),
        override_session_builder: Some(Arc::new(combined_session_builder)),
        ..Default::default()
    };

    let address = format!("{}:{}", config.bind_host, config.bind_port);
    let address = address
        .parse()
        .map_err(|e: AddrParseError| BallistaError::Configuration(e.to_string()))?;

    let cluster = BallistaCluster::new_from_config(&config).await?;
    start_server(cluster, address, Arc::new(config)).await?;

    Ok(())
}

/// 组合 S3 支持与 MapPartition QueryPlanner + EnforceDistributeBy 优化规则的 session builder
fn combined_session_builder(
    config: datafusion::prelude::SessionConfig,
) -> datafusion::error::Result<SessionState> {
    let state = session_state_with_s3_support(config)?;
    let query_planner = Arc::new(QueryPlannerWithExtensions::default());
    Ok(SessionStateBuilder::new_from_existing(state)
        .with_query_planner(query_planner)
        .with_physical_optimizer_rule(Arc::new(EnforceDistributeBy))
        .build())
}
