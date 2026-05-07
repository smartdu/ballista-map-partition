use std::sync::Arc;
use std::time::Instant;

use ballista::extension::SessionContextExt;
use ballista::prelude::SessionConfigExt;
use ballista_map_partition::codec::extension::{
    ExtendedBallistaLogicalCodec, ExtendedBallistaPhysicalCodec,
};
use ballista_map_partition::dataframe::map_partition::DataFrameExt;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionContext, col};
use futures::StreamExt;
use object_store::ObjectStore;

/// S3 配置常量（对应 MinIO 本地测试环境）
const S3_BUCKET: &str = "ballista";
const S3_ACCESS_KEY_ID: &str = "MINIO";
const S3_SECRET_KEY: &str = "MINIOSECRET";
const S3_ENDPOINT: &str = "http://localhost:9000";

/// 数据集参数
const NUM_CHANNELS_PER_REGION: usize = 100;
const NUM_TRAJECTORIES_PER_CHANNEL: usize = 1000;

/// 按 Region 分区的聚类分布式计算 — 并发性能验证
///
/// 数据集：
///   - N 个 region（通过 -r 参数指定，默认 1）
///   - 每个 region 100 个 channelid → 100 个档案
///   - 每个 channelid 1000 条轨迹（recordid）
///   - 每条轨迹含 json 字段（通过 -j 参数指定大小，默认 1KB）
///
/// 启动顺序：
///   1. 启动 MinIO (见 region_cluster_client 注释)
///   2. 启动调度器：cargo run --example distributed_compute_scheduler
///   3. 启动执行器：cargo run --example distributed_compute_executor
///   4. 运行基准测试：
///        MAP_PARTITION_SO=target/release/libregion_cluster_processor.so \
///          cargo run --example bench_region_cluster_client --release -- -r 50 -j 4096

struct BenchArgs {
    num_regions: usize,
    json_size: usize,
}

fn parse_args() -> BenchArgs {
    let args: Vec<String> = std::env::args().collect();
    let mut num_regions: usize = 1;
    let mut json_size: usize = 1024;
    let mut i = 1;
    while i < args.len() {
        if (args[i] == "-r" || args[i] == "--regions") && i + 1 < args.len() {
            if let Ok(n) = args[i + 1].parse::<usize>() {
                num_regions = n.max(1);
            }
            i += 2;
        } else if (args[i] == "-j" || args[i] == "--json-size") && i + 1 < args.len() {
            if let Ok(n) = args[i + 1].parse::<usize>() {
                json_size = n.max(32);
            }
            i += 2;
        } else {
            i += 1;
        }
    }
    BenchArgs { num_regions, json_size }
}

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    let args = parse_args();
    let num_regions = args.num_regions;
    let json_size = args.json_size;
    let num_partitions = num_regions.max(50);
    let total_rows = num_regions * NUM_CHANNELS_PER_REGION * NUM_TRAJECTORIES_PER_CHANNEL;

    println!("========================================");
    println!("  DistributeBy 并发性能验证");
    println!("========================================");
    println!("数据集参数：");
    println!("  总行数:       {}", total_rows);
    println!("  Region 数:    {} (-r 指定，默认 1)", num_regions);
    println!("  每 Region channelid 数: {}", NUM_CHANNELS_PER_REGION);
    println!("  每 channelid 轨迹数:    {}", NUM_TRAJECTORIES_PER_CHANNEL);
    println!("  JSON 大小:    {} 字节 (-j 指定，默认 1024)", json_size);
    println!("  分区数:       {}", num_partitions);
    println!("  预期档案数:   {} (每 region {} 个)",
             num_regions * NUM_CHANNELS_PER_REGION, NUM_CHANNELS_PER_REGION);
    println!("========================================");

    // ===== 1. 生成测试数据 =====
    let gen_start = Instant::now();

    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("channelid", DataType::Utf8, false),
        Field::new("captime", DataType::Utf8, false),
        Field::new("recordid", DataType::Utf8, false),
        Field::new("json", DataType::Utf8, false),
    ]));

    let mut regions_vec: Vec<String> = Vec::with_capacity(total_rows);
    let mut channelids_vec: Vec<String> = Vec::with_capacity(total_rows);
    let mut captimes_vec: Vec<String> = Vec::with_capacity(total_rows);
    let mut recordids_vec: Vec<String> = Vec::with_capacity(total_rows);
    let mut jsons_vec: Vec<String> = Vec::with_capacity(total_rows);

    for region_idx in 0..num_regions {
        let region_val = format!("region_{region_idx:04}");
        for channel_idx in 0..NUM_CHANNELS_PER_REGION {
            let channel_val = format!("ch_{channel_idx:03}");
            for traj_idx in 0..NUM_TRAJECTORIES_PER_CHANNEL {
                let rec_val = format!("rec_{region_idx:04}_{channel_idx:03}_{traj_idx:04}");
                // {"id":"0000_000_0000","data":"xxx..."} 固定结构占 32 字节，data 填充剩余
                let data_len = json_size.saturating_sub(32);
                let json_val = format!(
                    "{{\"id\":\"{region_idx:04}_{channel_idx:03}_{traj_idx:04}\",\"data\":\"{}\"}}",
                    "x".repeat(data_len)
                );
                regions_vec.push(region_val.clone());
                channelids_vec.push(channel_val.clone());
                captimes_vec.push(format!("2024-01-{:02}T{:02}:00:00",
                    (traj_idx % 28) + 1, (traj_idx % 24)));
                recordids_vec.push(rec_val);
                jsons_vec.push(json_val);
            }
        }
    }

    let region_array: StringArray = regions_vec.iter().map(|s| Some(s.as_str())).collect();
    let channel_array: StringArray = channelids_vec.iter().map(|s| Some(s.as_str())).collect();
    let captime_array: StringArray = captimes_vec.iter().map(|s| Some(s.as_str())).collect();
    let record_array: StringArray = recordids_vec.iter().map(|s| Some(s.as_str())).collect();
    let json_array: StringArray = jsons_vec.iter().map(|s| Some(s.as_str())).collect();

    let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(region_array),
            Arc::new(channel_array),
            Arc::new(captime_array),
            Arc::new(record_array),
            Arc::new(json_array),
        ],
    )?;

    let gen_elapsed = gen_start.elapsed();
    println!("[Timer] 数据生成耗时: {:.2}s", gen_elapsed.as_secs_f64());

    // ===== 2. 用本地 SessionContext 写入 S3 (不走 Ballista，避免序列化问题) =====
    let s3_write_start = Instant::now();
    {
        let s3_config = ballista_core::object_store::session_config_with_s3_support();
        let local_state = ballista_core::object_store::session_state_with_s3_support(s3_config)?;
        let local_ctx = SessionContext::new_with_state(local_state);

        local_ctx.sql("SET s3.allow_http = true").await?.show().await?;
        local_ctx.sql(&format!("SET s3.access_key_id = '{S3_ACCESS_KEY_ID}'"))
            .await?.show().await?;
        local_ctx.sql(&format!("SET s3.secret_access_key = '{S3_SECRET_KEY}'"))
            .await?.show().await?;
        local_ctx.sql(&format!("SET s3.endpoint = '{S3_ENDPOINT}'"))
            .await?.show().await?;

        // 清理 S3 旧数据，避免多次运行导致数据累积
        {
            let s3_url = url::Url::parse(&format!("s3://{S3_BUCKET}/bench_region_face_capture/"))
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            let store = local_ctx.runtime_env().object_store_registry.get_store(&s3_url)?;
            let prefix = object_store::path::Path::from("bench_region_face_capture");
            let mut objects = Box::pin(store.list(Some(&prefix)));
            while let Some(obj) = objects.next().await {
                if let Ok(meta) = obj {
                    let _ = store.delete(&meta.location).await;
                }
            }
        }
        println!("[Info] 已清理 S3 旧数据");

        local_ctx.register_batch("bench_data", batch)?;
        let df = local_ctx.sql("SELECT * FROM bench_data").await?;
        let url = format!("s3://{S3_BUCKET}/bench_region_face_capture/");
        df.write_parquet(&url, Default::default(), None).await?;
    }
    let s3_write_elapsed = s3_write_start.elapsed();
    println!("[Timer] S3 写入耗时: {:.2}s", s3_write_elapsed.as_secs_f64());

    // ===== 3. 创建 Ballista 远程 SessionContext =====
    let s3_config = ballista_core::object_store::session_config_with_s3_support();
    let state = ballista_core::object_store::session_state_with_s3_support(s3_config)?;

    let config = state
        .config()
        .clone()
        .with_ballista_logical_extension_codec(Arc::new(ExtendedBallistaLogicalCodec::default()))
        .with_ballista_physical_extension_codec(Arc::new(ExtendedBallistaPhysicalCodec::default()));

    let state = SessionStateBuilder::new_from_existing(state)
        .with_config(config)
        .build();

    let ctx = SessionContext::remote_with_state("df://127.0.0.1:50050", state).await?;

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

    // ===== 4. 清理 S3 旧结果 =====
    let output_path = format!("s3://{S3_BUCKET}/bench_region_cluster_result/");
    {
        let s3_url = url::Url::parse(&format!("s3://{S3_BUCKET}/bench_region_cluster_result/"))
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let store = ctx.runtime_env().object_store_registry.get_store(&s3_url)?;
        let prefix = object_store::path::Path::from("bench_region_cluster_result");
        let mut objects = Box::pin(store.list(Some(&prefix)));
        while let Some(obj) = objects.next().await {
            if let Ok(meta) = obj {
                let _ = store.delete(&meta.location).await;
            }
        }
    }

    // ===== 5. 从 S3 读取并执行 DistributeBy + MapPartition，结果直接写回 S3 =====
    let compute_start = Instant::now();

    let s3_path = format!("s3://{S3_BUCKET}/bench_region_face_capture/");
    let df = ctx.read_parquet(&s3_path, Default::default()).await?;
    println!("[Info] 输入行数: {}", df.clone().count().await?);

    let output_schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("dossierid", DataType::Utf8, false),
        Field::new("recordids", DataType::Utf8, false),
        Field::new("json1", DataType::Utf8, false),
        Field::new("json2", DataType::Utf8, false),
        Field::new("json3", DataType::Utf8, false),
        Field::new("json4", DataType::Utf8, false),
    ]));

    let so_path = std::env::var("MAP_PARTITION_SO")
        .unwrap_or_else(|_| "target/release/libregion_cluster_processor.so".to_string());

    let df = df
        .map_partition(&so_path, "region_cluster_processor", output_schema)?
        .with_distribute_by(col("region"), num_partitions)?
        .build()?;

    // Executor 端直接写 S3，不拉到客户端
    df.write_parquet(&output_path, Default::default(), None).await?;
    let compute_elapsed = compute_start.elapsed();
    println!("[Info] 聚类结果已写入 {}", output_path);

    // ===== 6. 读回结果验证正确性 =====
    let result = ctx.read_parquet(&output_path, Default::default()).await?.collect().await?;
    let total_output_rows: usize = result.iter().map(|b| b.num_rows()).sum();

    println!();
    println!("========================================");
    println!("  性能统计结果");
    println!("========================================");
    println!("  输入行数:       {}", total_rows);
    println!("  输出行数:       {} (档案数)", total_output_rows);
    println!("  预期档案数:     {}", num_regions * NUM_CHANNELS_PER_REGION);
    println!("  每 channelid 轨迹数: {}", NUM_TRAJECTORIES_PER_CHANNEL);
    println!("  JSON 大小:      {} 字节", json_size);
    println!("  数据生成耗时:   {:.2}s", gen_elapsed.as_secs_f64());
    println!("  S3 写入耗时:    {:.2}s", s3_write_elapsed.as_secs_f64());
    println!("  分布式计算耗时: {:.2}s", compute_elapsed.as_secs_f64());
    println!("  吞吐量:         {:.0} records/s", total_rows as f64 / compute_elapsed.as_secs_f64());
    println!("  分区数:         {}", num_partitions);
    println!("  每分区平均行数: {:.1}", total_rows as f64 / num_partitions as f64);
    println!("========================================");

    // 验证正确性
    if total_output_rows != num_regions * NUM_CHANNELS_PER_REGION {
        eprintln!("[ERROR] 输出档案数 {} != 预期 {}", total_output_rows, num_regions * NUM_CHANNELS_PER_REGION);
    } else {
        println!("[OK] 输出档案数正确: {}", total_output_rows);
    }

    // 检查是否有 CROSS_REGION_ERROR
    let mut cross_region_count = 0;
    for batch in &result {
        let dossier_col = batch.column(1);
        let dossier_arr = dossier_col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..dossier_arr.len() {
            if dossier_arr.value(i) == "CROSS_REGION_ERROR" {
                cross_region_count += 1;
            }
        }
    }
    if cross_region_count > 0 {
        eprintln!("[ERROR] 检测到 {} 个 CROSS_REGION_ERROR！", cross_region_count);
    } else {
        println!("[OK] 无 CROSS_REGION_ERROR，DistributeBy 语义正确");
    }

    Ok(())
}
