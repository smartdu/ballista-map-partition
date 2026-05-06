use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use crate::logical::map_partition::MapPartition;
use crate::physical::map_partition_exec::MapPartitionExec;

pub struct QueryPlannerWithExtensions {
    inner: DefaultPhysicalPlanner,
}

impl std::fmt::Debug for QueryPlannerWithExtensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryPlannerWithExtensions").finish()
    }
}

impl Default for QueryPlannerWithExtensions {
    fn default() -> Self {
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            CustomPlannerExtension::default(),
        )]);
        Self { inner: planner }
    }
}

#[async_trait]
impl QueryPlanner for QueryPlannerWithExtensions {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

#[derive(Debug, Clone, Default)]
pub struct CustomPlannerExtension {}

#[async_trait]
impl ExtensionPlanner for CustomPlannerExtension {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(MapPartition {
            so_path,
            fn_name,
            output_schema,
            distribute_by,
            num_partitions,
            hash_partition,
            ..
        }) = node.as_any().downcast_ref::<MapPartition>()
        {
            let input = physical_inputs
                .first()
                .ok_or(DataFusionError::Plan(
                    "MapPartition expects single input".to_string(),
                ))?
                .clone();

            // Convert DFSchema to Arrow Schema
            let arrow_schema: datafusion::arrow::datatypes::Schema =
                output_schema.as_arrow().clone();

            let input_df_schema = logical_inputs
                .first()
                .ok_or(DataFusionError::Plan(
                    "MapPartition expects single logical input".to_string(),
                ))?
                .schema();

            // Convert distribute_by logical expr to physical expr
            let distribute_by_phys = match distribute_by {
                Some(expr) => Some(create_physical_expr(
                    expr,
                    &input_df_schema,
                    session_state.execution_props(),
                )?),
                None => None,
            };

            let num_partitions = num_partitions.unwrap_or(0);

            // Derive hash_partition_phys from distribute_by_phys (they use the same expression)
            // Also check the logical hash_partition as a fallback
            let hash_partition_phys = if distribute_by_phys.is_some() && num_partitions > 0 {
                Some((vec![distribute_by_phys.clone().unwrap()], num_partitions))
            } else {
                match hash_partition {
                    Some((exprs, count)) => {
                        let phys_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = exprs
                            .iter()
                            .map(|e| {
                                create_physical_expr(
                                    e,
                                    &input_df_schema,
                                    session_state.execution_props(),
                                )
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        Some((phys_exprs, *count))
                    }
                    None => None,
                }
            };

            let exec = MapPartitionExec::new(
                so_path.clone(),
                fn_name.clone(),
                Arc::new(arrow_schema),
                input,
                distribute_by_phys,
                num_partitions,
                hash_partition_phys,
            );
            let node = Arc::new(exec);

            Ok(Some(node))
        } else {
            Ok(None)
        }
    }
}
