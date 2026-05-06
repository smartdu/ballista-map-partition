use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::prelude::{DataFrame, Expr};
use datafusion::execution::SessionState;

use crate::logical::map_partition::MapPartition;

pub trait DataFrameExt {
    fn map_partition(
        self,
        so_path: &str,
        fn_name: &str,
        output_schema: SchemaRef,
    ) -> datafusion::error::Result<MapPartitionBuilder>;
}

impl DataFrameExt for DataFrame {
    fn map_partition(
        self,
        so_path: &str,
        fn_name: &str,
        output_schema: SchemaRef,
    ) -> datafusion::error::Result<MapPartitionBuilder> {
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

        Ok(MapPartitionBuilder {
            state,
            so_path: so_path.to_string(),
            fn_name: fn_name.to_string(),
            output_schema: df_schema_ref,
            input,
            distribute_by: None,
            num_partitions: None,
            hash_partition: None,
        })
    }
}

pub struct MapPartitionBuilder {
    state: SessionState,
    so_path: String,
    fn_name: String,
    output_schema: Arc<DFSchema>,
    input: LogicalPlan,
    distribute_by: Option<Expr>,
    num_partitions: Option<usize>,
    /// Internal: derived from distribute_by for required_input_distribution / RepartitionExec
    hash_partition: Option<(Vec<Expr>, usize)>,
}

impl MapPartitionBuilder {
    /// Set the DistributeBy expression and partition count.
    ///
    /// Semantics: rows with the same value of `expr` go to the same processor;
    /// rows with different values go to different processors.
    ///
    /// `num_partitions` should be >= the number of distinct values in the `expr` column
    /// to minimize hash collisions (internal grouping provides a correctness safety net).
    pub fn with_distribute_by(mut self, expr: Expr, num_partitions: usize) -> datafusion::error::Result<Self> {
        if num_partitions == 0 {
            return Err(DataFusionError::Configuration(
                "num_partitions must be > 0".to_string(),
            ));
        }
        self.distribute_by = Some(expr.clone());
        self.num_partitions = Some(num_partitions);
        // Derive hash_partition for internal use (required_input_distribution / RepartitionExec)
        self.hash_partition = Some((vec![expr], num_partitions));
        Ok(self)
    }

    pub fn build(self) -> datafusion::error::Result<DataFrame> {
        let hash_partition = self.hash_partition;
        let node = Arc::new(MapPartition::new(
            self.so_path,
            self.fn_name,
            self.output_schema,
            self.input,
            self.distribute_by,
            self.num_partitions,
            hash_partition,
        ));
        let extension = Extension { node };
        let plan = LogicalPlan::Extension(extension);

        Ok(DataFrame::new(self.state, plan))
    }
}
