use std::sync::Arc;

use ballista_core::object_store::{
    runtime_env_with_s3_support, session_config_with_s3_support,
};
use ballista_executor::executor_process::{start_executor_process, ExecutorProcessConfig};
use ballista_map_partition::codec::extension::{
    ExtendedBallistaLogicalCodec, ExtendedBallistaPhysicalCodec,
};

/// 分布式计算执行器 — 集成 S3 对象存储 + map_partition 算子
///
/// 启动方式（单 Executor）：
///   cargo run --example distributed_compute_executor
///
/// 启动方式（多 Executor，通过命令行参数指定端口和并发数）：
///   # Executor #1
///   cargo run --example distributed_compute_executor -- -p 50051 --bind-grpc-port 50052 -c 4
///   # Executor #2
///   cargo run --example distributed_compute_executor -- -p 50053 --bind-grpc-port 50054 -c 4
///
/// 启动方式（启用监控）：
///   cargo run --example distributed_compute_executor --features monitoring -- --monitor-port 8081
///
/// 参数说明：
///   -p, --port PORT             Arrow Flight 服务端口 (默认 50051)
///       --bind-grpc-port PORT   gRPC 服务端口 (默认 50052)
///   -c, --concurrent-tasks N    并发任务数 (默认 CPU 核数)
///       --monitor-port PORT     监控 Web 服务端口 (默认 8081, 需启用 monitoring feature)

#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    let mut port: u16 = 50051;
    let mut grpc_port: u16 = 50052;
    let mut concurrent_tasks: usize = std::thread::available_parallelism().unwrap().get();
    let mut monitor_port: u16 = 8081;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-p" | "--port" => {
                i += 1;
                port = args[i].parse().expect("invalid port number");
            }
            "--bind-grpc-port" => {
                i += 1;
                grpc_port = args[i].parse().expect("invalid grpc port number");
            }
            "-c" | "--concurrent-tasks" => {
                i += 1;
                concurrent_tasks = args[i].parse().expect("invalid concurrent tasks number");
            }
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
        let node_name = format!("executor-{port}");
        log::info!("Starting monitor server on {monitor_addr}");
        tokio::spawn(async move {
            if let Err(e) = ballista_monitor::start_monitor_server(
                "executor",
                &node_name,
                &monitor_addr,
                concurrent_tasks,
            )
            .await
            {
                log::error!("Monitor server error: {e}");
            }
        });
    }

    let config: ExecutorProcessConfig = ExecutorProcessConfig {
        port,
        grpc_port,
        concurrent_tasks,
        override_logical_codec: Some(Arc::new(ExtendedBallistaLogicalCodec::default())),
        override_physical_codec: Some(Arc::new(ExtendedBallistaPhysicalCodec::default())),
        override_config_producer: Some(Arc::new(session_config_with_s3_support)),
        override_runtime_producer: Some(Arc::new(runtime_env_with_s3_support)),
        ..Default::default()
    };

    start_executor_process(Arc::new(config)).await
}
