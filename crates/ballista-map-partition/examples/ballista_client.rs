use std::sync::Arc;

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_map_partition::{
    codec::extension::{ExtendedBallistaLogicalCodec, ExtendedBallistaPhysicalCodec},
    dataframe::map_partition::DataFrameExt,
};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

/// Custom Ballista Client with map_partition support.

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    let config = SessionConfig::new_with_ballista()
        .with_ballista_logical_extension_codec(Arc::new(ExtendedBallistaLogicalCodec::default()))
        .with_ballista_physical_extension_codec(Arc::new(ExtendedBallistaPhysicalCodec::default()));

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;
    let df = ctx.read_parquet("data/", Default::default()).await?;

    let so_path = std::env::var("MAP_PARTITION_SO")
        .unwrap_or_else(|_| "/path/to/libidentity_processor.so".to_string());

    let output_schema = df.schema().as_arrow().clone();
    let df = df.map_partition(&so_path, "identity_processor", Arc::new(output_schema))?.build()?;

    df.show().await?;

    Ok(())
}
