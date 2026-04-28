use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::prelude::DataFrame;

use crate::logical::map_partition::MapPartition;

pub trait DataFrameExt {
    fn map_partition(
        self,
        so_path: &str,
        fn_name: &str,
        output_schema: SchemaRef,
    ) -> datafusion::error::Result<DataFrame>;
}

impl DataFrameExt for DataFrame {
    fn map_partition(
        self,
        so_path: &str,
        fn_name: &str,
        output_schema: SchemaRef,
    ) -> datafusion::error::Result<DataFrame> {
        if so_path.is_empty() {
            return Err(DataFusionError::Configuration(
                "so_path cannot be empty".to_string(),
            ));
        }
        if fn_name.is_empty() {
            return Err(DataFusionError::Configuration(
                "fn_name cannot be empty".to_string(),
            ));
        }

        let (state, input) = self.into_parts();

        let df_schema = DFSchema::try_from(output_schema)
            .map_err(|e| DataFusionError::Configuration(format!("invalid output schema: {e}")))?;
        let df_schema_ref = Arc::new(df_schema);

        let node = Arc::new(MapPartition::new(
            so_path.to_string(),
            fn_name.to_string(),
            df_schema_ref,
            input,
        ));
        let extension = Extension { node };
        let plan = LogicalPlan::Extension(extension);

        Ok(DataFrame::new(state, plan))
    }
}
