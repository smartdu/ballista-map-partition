#!/bin/bash
set -euo pipefail

# ============================================================
# ballista-map-partition 项目脚手架
#
# 用法:
#   ./scripts/scaffold.sh /path/to/my_project           # 同时生成 processor + client
#   ./scripts/scaffold.sh -p /path/to/my_project        # 仅生成 processor
#   ./scripts/scaffold.sh -c /path/to/my_project        # 仅生成 client
#
# 生成独立项目，通过 path 依赖引用当前仓库的框架/SDK crate。
# ============================================================

usage() {
    echo "用法: $0 [-p | -c] <project_dir>"
    echo ""
    echo "选项:"
    echo "  -p, --processor   仅生成 .so processor"
    echo "  -c, --client      仅生成 client"
    echo ""
    echo "示例:"
    echo "  $0 /workspace/my-project"
    echo "  $0 -p /workspace/my-project"
    exit 1
}

GEN_PROC=1 GEN_CLIENT=1

while [[ $# -gt 0 ]]; do
    case "$1" in
        -p|--processor) GEN_CLIENT=0; shift ;;
        -c|--client)    GEN_PROC=0;   shift ;;
        -h|--help)      usage ;;
        -*) echo "未知选项: $1"; usage ;;
        *) break ;;
    esac
done

[[ $# -ge 1 ]] || usage

PROJECT_DIR="$1"
PROJECT_NAME=$(basename "$PROJECT_DIR")
PROCESSOR_NAME="${PROJECT_NAME}_processor"
CLIENT_NAME="${PROJECT_NAME}_client"
# fn_name 必须是合法 Rust ident，- 转 _
FN_NAME="${PROJECT_NAME//-/_}_processor"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FRAMEWORK_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SDK_PATH="$FRAMEWORK_ROOT/crates/map-partition-sdk"
PLUGIN_PATH="$FRAMEWORK_ROOT/crates/ballista-map-partition"

if [[ -d "$PROJECT_DIR" ]]; then
    echo "错误: $PROJECT_DIR 已存在"
    exit 1
fi

# ---- 输出配置摘要 ----
echo "=== 生成项目: $PROJECT_NAME ==="
echo "  路径:       $PROJECT_DIR"
[[ "$GEN_PROC" -eq 1 ]]   && echo "  processor:  $PROCESSOR_NAME"
[[ "$GEN_CLIENT" -eq 1 ]] && echo "  client:     $CLIENT_NAME"
echo "  fn_name:    $FN_NAME"
echo ""

# ---- workspace Cargo.toml ----
gen_workspace_toml() {
    local members=""
    [[ "$GEN_CLIENT" -eq 1 ]] && members="$members\"client\""
    [[ "$GEN_CLIENT" -eq 1 && "$GEN_PROC" -eq 1 ]] && members="$members, "
    [[ "$GEN_PROC" -eq 1 ]]   && members="$members\"processor\""
    cat > "$PROJECT_DIR/Cargo.toml" <<EOF
[workspace]
members = [$members]
resolver = "3"
EOF
}

# ---- processor ----
gen_processor() {
    mkdir -p "$PROJECT_DIR/processor/src"

    cat > "$PROJECT_DIR/processor/Cargo.toml" <<EOF
[package]
name = "$PROCESSOR_NAME"
version = "0.1.0"
edition = "2024"

[lib]
crate-type = ["cdylib"]

[dependencies]
map-partition-sdk = { path = "$SDK_PATH" }
arrow = "54"
paste = "1"
EOF

    cat > "$PROJECT_DIR/processor/src/lib.rs" <<'PROC_LIB'
use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use map_partition_sdk::{PartitionProcessor, export_partition_processor};

/// TODO: 替换为你的业务逻辑
struct MyProcessor {
    input_schema: SchemaRef,
    partition_id: usize,
    row_count: usize,
    executed: bool,
    output_sent: bool,
}

impl PartitionProcessor for MyProcessor {
    fn new(schema: SchemaRef, partition_id: usize) -> Self {
        Self {
            input_schema: schema,
            partition_id,
            row_count: 0,
            executed: false,
            output_sent: false,
        }
    }

    fn schema(&self) -> &SchemaRef {
        &self.input_schema
    }

    fn partition_id(&self) -> usize {
        self.partition_id
    }

    fn feed(&mut self, batch: RecordBatch) {
        self.row_count += batch.num_rows();
    }

    fn execute(&mut self) {
        self.executed = true;
    }

    fn fetch(&mut self) -> Option<RecordBatch> {
        if self.output_sent {
            return None;
        }
        self.output_sent = true;

        let schema = Arc::new(Schema::new(vec![
            Field::new("partition_id", DataType::UInt64, false),
            Field::new("status", DataType::Utf8, false),
        ]));

        let ids: UInt64Array = vec![self.partition_id as u64].into_iter().map(Some).collect();
        let status: StringArray = vec![format!(
            "partition[{}] executed={} rows={}",
            self.partition_id, self.executed, self.row_count
        )]
        .into_iter()
        .map(Some)
        .collect();

        Some(
            RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(status)])
                .expect("create output batch"),
        )
    }
}

export_partition_processor!(MyProcessor, FN_PLACEHOLDER);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Utf8, false),
        ]));
        let mut p = MyProcessor::new(schema.clone(), 3);
        assert_eq!(p.partition_id(), 3);

        let col: StringArray = vec!["a", "b"].into_iter().map(Some).collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();
        p.feed(batch);
        p.execute();

        let out = p.fetch().unwrap();
        let status = out.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(status.value(0).contains("rows=2"));
        assert!(p.fetch().is_none());
    }
}
PROC_LIB

    # Replace fn_name placeholder
    local fn_macro="export_partition_processor!(MyProcessor, $FN_NAME);"
    sed -i "s/export_partition_processor!(MyProcessor, FN_PLACEHOLDER);/$fn_macro/" \
        "$PROJECT_DIR/processor/src/lib.rs"

    echo "  ✓ processor"
}

# ---- client ----
gen_client() {
    mkdir -p "$PROJECT_DIR/client/src"

    cat > "$PROJECT_DIR/client/Cargo.toml" <<EOF
[package]
name = "$CLIENT_NAME"
version = "0.1.0"
edition = "2024"

[dependencies]
ballista-map-partition = { path = "$PLUGIN_PATH" }
ballista = "52"
ballista-core = "52"
datafusion = "52"
tokio = { version = "1", features = ["full"] }
env_logger = "0.11"
log = "0.4"
futures = "0.3"
object_store = "0.12"
url = "2"
arrow = "57"
EOF

    cat > "$PROJECT_DIR/client/src/main.rs" <<CLI_MAIN
use std::sync::Arc;

use ballista::extension::SessionContextExt;
use ballista::prelude::SessionConfigExt;
use ballista_map_partition::codec::extension::{
    ExtendedBallistaLogicalCodec, ExtendedBallistaPhysicalCodec,
};
use ballista_map_partition::dataframe::map_partition::DataFrameExt;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionContext, col};

const S3_BUCKET: &str = "ballista";
const S3_ACCESS_KEY_ID: &str = "MINIO";
const S3_SECRET_KEY: &str = "MINIOSECRET";
const S3_ENDPOINT: &str = "http://localhost:9000";
const NUM_PARTITIONS: usize = 10;

const DEFAULT_SO: &str = "target/release/lib$PROCESSOR_NAME.so";
const DEFAULT_FN: &str = "$FN_NAME";

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    let so_path = std::env::var("MAP_PARTITION_SO")
        .unwrap_or_else(|_| DEFAULT_SO.to_string());
    let fn_name = std::env::var("MAP_PARTITION_FN")
        .unwrap_or_else(|_| DEFAULT_FN.to_string());

    // ---- 1. Generate demo data → S3 ----
    let s3_config = ballista_core::object_store::session_config_with_s3_support();
    let local_state = ballista_core::object_store::session_state_with_s3_support(s3_config)?;
    let local_ctx = SessionContext::new_with_state(local_state);
    local_ctx.sql("SET s3.allow_http = true").await?.show().await?;
    local_ctx.sql(&format!("SET s3.access_key_id = '{S3_ACCESS_KEY_ID}'")).await?.show().await?;
    local_ctx.sql(&format!("SET s3.secret_access_key = '{S3_SECRET_KEY}'")).await?.show().await?;
    local_ctx.sql(&format!("SET s3.endpoint = '{S3_ENDPOINT}'")).await?.show().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    // 3 regions × 5 rows = 15 rows
    let regions: StringArray = (0..3).flat_map(|r| vec![format!("region_{r:02}"); 5]).map(Some).collect();
    let values: StringArray = (0..15).map(|i| Some(format!("val_{i:03}"))).collect();
    let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
        schema, vec![Arc::new(regions), Arc::new(values)],
    )?;

    // Clean + write
    {
        let s3_url = url::Url::parse("s3://ballista/demo_data/")
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let store = local_ctx.runtime_env().object_store_registry.get_store(&s3_url)?;
        let prefix = object_store::path::Path::from("demo_data");
        use futures::StreamExt;
        let mut objects = Box::pin(store.list(Some(&prefix)));
        while let Some(obj) = objects.next().await {
            if let Ok(meta) = obj { let _ = store.delete(&meta.location).await; }
        }
    }
    local_ctx.deregister_table("demo")?;
    local_ctx.register_batch("demo", batch)?;
    let df = local_ctx.sql("SELECT * FROM demo").await?;
    let s3_path = "s3://ballista/demo_data/";
    df.write_parquet(s3_path, Default::default(), None).await?;
    println!("[OK] 写入测试数据到 {s3_path}");

    // ---- 2. Ballista remote context ----
    let s3_config = ballista_core::object_store::session_config_with_s3_support();
    let state = ballista_core::object_store::session_state_with_s3_support(s3_config)?;
    let config = state.config().clone()
        .with_ballista_logical_extension_codec(Arc::new(ExtendedBallistaLogicalCodec::default()))
        .with_ballista_physical_extension_codec(Arc::new(ExtendedBallistaPhysicalCodec::default()));
    let state = SessionStateBuilder::new_from_existing(state).with_config(config).build();
    let ctx = SessionContext::remote_with_state("df://127.0.0.1:50050", state).await?;

    ctx.sql("SET s3.allow_http = true").await?.show().await?;
    ctx.sql(&format!("SET s3.access_key_id = '{S3_ACCESS_KEY_ID}'")).await?.show().await?;
    ctx.sql(&format!("SET s3.secret_access_key = '{S3_SECRET_KEY}'")).await?.show().await?;
    ctx.sql(&format!("SET s3.endpoint = '{S3_ENDPOINT}'")).await?.show().await?;

    // ---- 3. Run processor ----
    let df = ctx.read_parquet(s3_path, Default::default()).await?;
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("partition_id", DataType::UInt64, false),
        Field::new("status", DataType::Utf8, false),
    ]));

    let df = df
        .map_partition(&so_path, &fn_name, output_schema)?
        .with_distribute_by(col("region"), NUM_PARTITIONS)?
        .build()?;

    println!("--- Output ---");
    df.show().await?;

    Ok(())
}
CLI_MAIN

    echo "  ✓ client"
}

# ---- Execute ----
mkdir -p "$PROJECT_DIR"
gen_workspace_toml
[[ "$GEN_PROC" -eq 1 ]]   && gen_processor
[[ "$GEN_CLIENT" -eq 1 ]] && gen_client

# ---- 下一步指引 ----
echo ""
echo "=== 项目已生成: $PROJECT_DIR ==="
echo ""
echo "下一步:"
echo "  cd $PROJECT_DIR"
if [[ "$GEN_PROC" -eq 1 ]]; then
    echo "  cargo test -p $PROCESSOR_NAME"
    echo "  cargo build --release -p $PROCESSOR_NAME"
fi
if [[ "$GEN_CLIENT" -eq 1 && "$GEN_PROC" -eq 1 ]]; then
    echo ""
    echo "  MAP_PARTITION_SO=target/release/lib$PROCESSOR_NAME.so \\"
    echo "  MAP_PARTITION_FN=$FN_NAME \\"
    echo "    cargo run --release -p $CLIENT_NAME"
elif [[ "$GEN_CLIENT" -eq 1 ]]; then
    echo ""
    echo "  MAP_PARTITION_SO=/path/to/your.so \\"
    echo "  MAP_PARTITION_FN=your_fn_name \\"
    echo "    cargo run --release -p $CLIENT_NAME"
fi
echo ""
echo "编辑 processor/src/lib.rs 实现你的业务逻辑。"
