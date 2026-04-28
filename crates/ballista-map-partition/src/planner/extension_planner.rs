use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
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
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(MapPartition {
            so_path,
            fn_name,
            output_schema,
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

            let exec = MapPartitionExec::new(
                so_path.clone(),
                fn_name.clone(),
                Arc::new(arrow_schema),
                input,
            );
            let node = Arc::new(exec);

            Ok(Some(node))
        } else {
            Ok(None)
        }
    }
}
