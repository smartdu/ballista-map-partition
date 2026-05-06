use std::sync::Arc;

use ballista::extension::SessionContextExt;
use ballista::prelude::SessionConfigExt;
use ballista_core::object_store::state_with_s3_support;
use ballista_map_partition::codec::extension::{
    ExtendedBallistaLogicalCodec, ExtendedBallistaPhysicalCodec,
};
use ballista_map_partition::dataframe::map_partition::DataFrameExt;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;

/// S3 配置常量（对应 MinIO 本地测试环境）
const S3_BUCKET: &str = "ballista";
const S3_ACCESS_KEY_ID: &str = "MINIO";
const S3_SECRET_KEY: &str = "MINIOSECRET";
const S3_ENDPOINT: &str = "http://localhost:9000";

/// 分布式计算客户端 — 集成 S3 对象存储 + map_partition 算子
///
/// 启动顺序：
///   1. 启动 MinIO：
///        docker run --rm -p 9000:9000 -p 9001:9001 \
///          -e "MINIO_ACCESS_KEY=MINIO" -e "MINIO_SECRET_KEY=MINIOSECRET" \
///          quay.io/minio/minio server /data --console-address ":9001"
///   2. 启动调度器：
///        cargo run --example distributed_compute_scheduler
///   3. 启动执行器：
///        cargo run --example distributed_compute_executor
///   4. 运行客户端：
///        MAP_PARTITION_SO=/path/to/libidentity_processor.so \
///          cargo run --example distributed_compute_client

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    // 创建带 S3 支持的 SessionState
    let state = state_with_s3_support()?;

    // 在 S3 session state 基础上叠加 map_partition codec
    let config = state
        .config()
        .clone()
        .with_ballista_logical_extension_codec(Arc::new(ExtendedBallistaLogicalCodec::default()))
        .with_ballista_physical_extension_codec(Arc::new(ExtendedBallistaPhysicalCodec::default()));

    let state = SessionStateBuilder::new_from_existing(state)
        .with_config(config)
        .build();

    // 连接到 Ballista 调度器
    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

    // 配置 S3 访问参数
    ctx.sql("SET s3.allow_http = true").await?.show().await?;
    ctx.sql(&format!("SET s3.access_key_id = '{S3_ACCESS_KEY_ID}'"))
        .await?
        .show()
        .await?;
    ctx.sql(&format!("SET s3.secret_access_key = '{S3_SECRET_KEY}'"))
        .await?
        .show()
        .await?;
    ctx.sql(&format!("SET s3.endpoint = '{S3_ENDPOINT}'"))
        .await?
        .show()
        .await?;

    // 从 S3 读取 Parquet 数据
    let s3_path = format!("s3://{S3_BUCKET}/data/");
    let df = ctx.read_parquet(&s3_path, Default::default()).await?;

    // 应用 map_partition 算子处理数据
    let so_path = std::env::var("MAP_PARTITION_SO")
        .unwrap_or_else(|_| "/path/to/libidentity_processor.so".to_string());

    let output_schema = df.schema().as_arrow().clone();
    let df = df.map_partition(&so_path, "identity_processor", Arc::new(output_schema))?.build()?;

    df.show().await?;

    Ok(())
}
