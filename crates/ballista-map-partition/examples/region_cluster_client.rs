use std::sync::Arc;

use ballista::extension::SessionContextExt;
use ballista::prelude::SessionConfigExt;
use ballista_map_partition::codec::extension::{
    ExtendedBallistaLogicalCodec, ExtendedBallistaPhysicalCodec,
};
use ballista_map_partition::dataframe::map_partition::DataFrameExt;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionContext, col};

/// S3 配置常量（对应 MinIO 本地测试环境）
const S3_BUCKET: &str = "ballista";
const S3_ACCESS_KEY_ID: &str = "MINIO";
const S3_SECRET_KEY: &str = "MINIOSECRET";
const S3_ENDPOINT: &str = "http://localhost:9000";

/// DistributeBy 分区数（应 >= distinct region 数，确保每个 region 独占一个分区）
const NUM_PARTITIONS: usize = 100;

/// 按 Region 分区的聚类分布式计算客户端
///
/// 输入：人脸抓拍数据 (region, channelid, captime, recordid)
/// 输出：聚类结果 (region, dossierid, recordids)
///
/// 特性：
///   1. 输入含 region 字段
///   2. 使用 with_distribute_by 按 region 分区，确保同 region 数据在同一 processor
///   3. processor 内部按相同 channelid 聚类，生成 dossier
///   4. processor 内部检测是否出现混合 region（验证 distribute_by 正确性）
///
/// 启动顺序：
///   1. 启动 MinIO：
///        docker run --rm -p 9000:9000 -p 9001:9001 \
///          -e "MINIO_ACCESS_KEY=MINIO" -e "MINIO_SECRET_KEY=MINIOSECRET" \
///          quay.io/minio/minio server /data --console-address ":9001"
///   2. 上传测试数据到 S3 (见 README)
///   3. 启动调度器：
///        cargo run --example distributed_compute_scheduler
///   4. 启动执行器：
///        cargo run --example distributed_compute_executor
///   5. 运行客户端：
///        MAP_PARTITION_SO=target/release/libregion_cluster_processor.so \
///          cargo run --example region_cluster_client

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    // 创建带 S3 支持的 SessionState
    let s3_config = ballista_core::object_store::session_config_with_s3_support();
    let state = ballista_core::object_store::session_state_with_s3_support(s3_config)?;

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
    let ctx = SessionContext::remote_with_state("df://127.0.0.1:50050", state).await?;

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

    // 从 S3 读取含 region 字段的人脸抓拍 Parquet 数据
    let s3_path = format!("s3://{S3_BUCKET}/region_face_capture/");
    let df = ctx.read_parquet(&s3_path, Default::default()).await?;

    println!("--- Input: region face capture data ---");
    df.clone().show().await?;

    // 输出 schema: (region, dossierid, recordids)
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("dossierid", DataType::Utf8, false),
        Field::new("recordids", DataType::Utf8, false),
    ]));

    let so_path = std::env::var("MAP_PARTITION_SO")
        .unwrap_or_else(|_| "target/release/libregion_cluster_processor.so".to_string());

    // 使用 with_distribute_by 声明按 region 做 DistributeBy 分区
    // 语义：相同 region 进入同一个 processor，不同 region 进入不同 processor
    // 自定义优化规则 EnforceDistributeBy 会强制插入 RepartitionExec
    let df = df
        .map_partition(&so_path, "region_cluster_processor", output_schema)?
        .with_distribute_by(col("region"), NUM_PARTITIONS)?
        .build()?;

    println!("--- Output: region cluster result ---");
    df.clone().show().await?;

    // 将聚类结果写回 S3
    let output_path = format!("s3://{S3_BUCKET}/region_cluster_result/");
    df.write_parquet(&output_path, Default::default(), None).await?;
    println!("--- Results written to {} ---", output_path);

    Ok(())
}
