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
/// 启动方式：
///   cargo run --example distributed_compute_executor

#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    let config: ExecutorProcessConfig = ExecutorProcessConfig {
        override_logical_codec: Some(Arc::new(ExtendedBallistaLogicalCodec::default())),
        override_physical_codec: Some(Arc::new(ExtendedBallistaPhysicalCodec::default())),
        override_config_producer: Some(Arc::new(session_config_with_s3_support)),
        override_runtime_producer: Some(Arc::new(runtime_env_with_s3_support)),
        ..Default::default()
    };

    start_executor_process(Arc::new(config)).await
}
