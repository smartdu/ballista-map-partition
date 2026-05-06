use std::sync::Arc;

use ballista_map_partition::{
    dataframe::map_partition::DataFrameExt,
    planner::extension_planner::QueryPlannerWithExtensions,
};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;

// This example demonstrates using map_partition with DataFusion standalone
// (no Ballista cluster needed).
//
// It requires a .so file built from the identity_processor example:
//   cd crates/map-partition-sdk/examples/identity_processor
//   cargo build --release
//
// Then set SO_PATH below to the resulting .so path.

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    let query_planner = Arc::new(QueryPlannerWithExtensions::default());

    let state = SessionStateBuilder::new()
        .with_query_planner(query_planner)
        .with_default_features()
        .build();

    let ctx = SessionContext::new_with_state(state);
    let df = ctx.read_parquet("data/", Default::default()).await?;

    // Update this path to point to your built .so
    let so_path = std::env::var("MAP_PARTITION_SO")
        .unwrap_or_else(|_| "../../map-partition-sdk/examples/identity_processor/target/release/libidentity_processor.so".to_string());

    let output_schema = df.schema().as_arrow().clone();
    let df = df.map_partition(&so_path, "identity_processor", Arc::new(output_schema))?.build()?;

    df.show().await?;

    Ok(())
}
