use std::hash::Hash;

use datafusion::common::DFSchemaRef;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::Expr;

use crate::physical::map_partition_exec::encode_schema_to_ipc;

#[derive(Debug, Clone)]
pub struct MapPartition {
    pub so_path: String,
    pub fn_name: String,
    pub output_schema: DFSchemaRef,
    pub input: LogicalPlan,
    /// Business-level DistributeBy: same value → same processor, different values → different processors.
    /// When set, hash_partition is automatically derived from this.
    pub distribute_by: Option<Expr>,
    /// Number of partitions for the RepartitionExec (should be >= number of distinct values).
    pub num_partitions: Option<usize>,
    /// Internal: derived from distribute_by for required_input_distribution / RepartitionExec.
    pub hash_partition: Option<(Vec<Expr>, usize)>,
}

impl PartialEq for MapPartition {
    fn eq(&self, other: &Self) -> bool {
        self.so_path == other.so_path
            && self.fn_name == other.fn_name
            && self.output_schema.as_arrow() == other.output_schema.as_arrow()
            && self.input == other.input
            && self.distribute_by == other.distribute_by
            && self.num_partitions == other.num_partitions
            && self.hash_partition == other.hash_partition
    }
}

impl PartialOrd for MapPartition {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.so_path.partial_cmp(&other.so_path) {
            Some(std::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.fn_name.partial_cmp(&other.fn_name) {
            Some(std::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        // Compare schemas via IPC bytes for ordering
        let self_bytes = encode_schema_to_ipc(self.output_schema.as_arrow()).ok();
        let other_bytes = encode_schema_to_ipc(other.output_schema.as_arrow()).ok();
        match self_bytes.partial_cmp(&other_bytes) {
            Some(std::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.input.partial_cmp(&other.input) {
            Some(std::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        // Compare hash_partition via debug strings (Expr doesn't implement Ord)
        let self_hash = self.hash_partition.as_ref().map(|(e, n)| {
            (e.iter().map(|x| format!("{x:?}")).collect::<Vec<_>>(), n)
        });
        let other_hash = other.hash_partition.as_ref().map(|(e, n)| {
            (e.iter().map(|x| format!("{x:?}")).collect::<Vec<_>>(), n)
        });
        match self_hash.partial_cmp(&other_hash) {
            Some(std::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        // Compare distribute_by
        let self_db = self.distribute_by.as_ref().map(|e| format!("{e:?}"));
        let other_db = other.distribute_by.as_ref().map(|e| format!("{e:?}"));
        match self_db.partial_cmp(&other_db) {
            Some(std::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.num_partitions.partial_cmp(&other.num_partitions)
    }
}

impl Hash for MapPartition {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.so_path.hash(state);
        self.fn_name.hash(state);
        // Hash the Arrow schema bytes
        let arrow_schema = self.output_schema.as_arrow();
        if let Ok(bytes) = encode_schema_to_ipc(arrow_schema) {
            bytes.hash(state);
        }
        self.input.hash(state);
        // Hash distribute_by
        if let Some(expr) = &self.distribute_by {
            format!("{expr:?}").hash(state);
        }
        self.num_partitions.hash(state);
        // Hash hash_partition
        if let Some((exprs, count)) = &self.hash_partition {
            for expr in exprs {
                format!("{expr:?}").hash(state);
            }
            count.hash(state);
        }
    }
}

impl Eq for MapPartition {}

impl MapPartition {
    pub fn new(
        so_path: String,
        fn_name: String,
        output_schema: DFSchemaRef,
        input: LogicalPlan,
        distribute_by: Option<Expr>,
        num_partitions: Option<usize>,
        hash_partition: Option<(Vec<Expr>, usize)>,
    ) -> Self {
        Self {
            so_path,
            fn_name,
            output_schema,
            input,
            distribute_by,
            num_partitions,
            hash_partition,
        }
    }
}

impl UserDefinedLogicalNodeCore for MapPartition {
    fn name(&self) -> &str {
        "MapPartition"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.distribute_by
            .as_ref()
            .map(|e| vec![e.clone()])
            .unwrap_or_default()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MapPartition: so={}, fn={}",
            self.so_path, self.fn_name
        )?;
        if let Some(ref db) = self.distribute_by {
            write!(f, ", distribute_by=[{}]", db)?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        let input = inputs
            .into_iter()
            .next()
            .ok_or(DataFusionError::Plan(
                "MapPartition expects single input".to_string(),
            ))?;

        // Reconstruct distribute_by from expressions
        let distribute_by = if self.distribute_by.is_some() && !exprs.is_empty() {
            Some(exprs.into_iter().next().unwrap())
        } else {
            None
        };

        // Reconstruct hash_partition from distribute_by + num_partitions
        let hash_partition = match (&distribute_by, &self.num_partitions) {
            (Some(expr), Some(n)) => Some((vec![expr.clone()], *n)),
            _ => self.hash_partition.clone(),
        };

        Ok(Self {
            so_path: self.so_path.clone(),
            fn_name: self.fn_name.clone(),
            output_schema: self.output_schema.clone(),
            input,
            distribute_by,
            num_partitions: self.num_partitions,
            hash_partition,
        })
    }
}
