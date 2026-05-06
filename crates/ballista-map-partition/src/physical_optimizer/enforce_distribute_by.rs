use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_physical_optimizer::PhysicalOptimizerRule;

use crate::physical::map_partition_exec::MapPartitionExec;

/// Custom PhysicalOptimizerRule that forces a RepartitionExec to be inserted
/// before any MapPartitionExec that has distribute_by set.
///
/// This is needed because DataFusion's built-in EnforceDistribution rule skips
/// inserting RepartitionExec for small datasets (n_rows <= batch_size) and
/// single-partition inputs. We need to guarantee that MapPartitionExec gets
/// hash-partitioned input so that same-key rows land in the same partition.
#[derive(Debug)]
pub struct EnforceDistributeBy;

impl PhysicalOptimizerRule for EnforceDistributeBy {
    fn name(&self) -> &str {
        "enforce_distribute_by"
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(&|node: Arc<dyn ExecutionPlan>| {
            if let Some(exec) = node.as_any().downcast_ref::<MapPartitionExec>() {
                if exec.distribute_by.is_some() {
                    let child = node.children()[0].clone();
                    // Check if child already satisfies the required distribution
                    if !is_satisfied(&child, exec) {
                        let hash_exprs = exec.hash_partition.as_ref()
                            .expect("hash_partition must be set when distribute_by is set")
                            .0
                            .clone();
                        let num_partitions = exec.num_partitions;
                        let partitioning = Partitioning::Hash(hash_exprs, num_partitions);
                        let repartition = RepartitionExec::try_new(child, partitioning)?;
                        let new_exec = Arc::new(MapPartitionExec::new(
                            exec.so_path.clone(),
                            exec.fn_name.clone(),
                            exec.output_schema.clone(),
                            Arc::new(repartition),
                            exec.distribute_by.clone(),
                            exec.num_partitions,
                            exec.hash_partition.clone(),
                        )) as Arc<dyn ExecutionPlan>;
                        return Ok(Transformed::yes(new_exec));
                    }
                }
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Check if the child's output partitioning already satisfies MapPartitionExec's
/// distribute_by requirements.
fn is_satisfied(
    child: &Arc<dyn ExecutionPlan>,
    exec: &MapPartitionExec,
) -> bool {
    let num_partitions = exec.num_partitions;
    let child_partitioning = child.output_partitioning();

    if let Partitioning::Hash(exprs, n) = child_partitioning {
        // Satisfied if partition count matches and hash expressions match
        if *n != num_partitions {
            return false;
        }
        let required_exprs = &exec.hash_partition.as_ref().unwrap().0;
        if exprs.len() != required_exprs.len() {
            return false;
        }
        // Compare expressions by debug string (PhysicalExpr doesn't implement Eq)
        for (a, b) in exprs.iter().zip(required_exprs.iter()) {
            if format!("{a:?}") != format!("{b:?}") {
                return false;
            }
        }
        true
    } else {
        false
    }
}
