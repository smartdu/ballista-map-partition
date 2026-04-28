use std::hash::Hash;

use datafusion::common::DFSchemaRef;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};

use crate::physical::map_partition_exec::encode_schema_to_ipc;

#[derive(Debug, Clone)]
pub struct MapPartition {
    pub so_path: String,
    pub fn_name: String,
    pub output_schema: DFSchemaRef,
    pub input: LogicalPlan,
}

impl PartialEq for MapPartition {
    fn eq(&self, other: &Self) -> bool {
        self.so_path == other.so_path
            && self.fn_name == other.fn_name
            && self.output_schema.as_arrow() == other.output_schema.as_arrow()
            && self.input == other.input
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
        self.input.partial_cmp(&other.input)
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
    }
}

impl Eq for MapPartition {}

impl MapPartition {
    pub fn new(
        so_path: String,
        fn_name: String,
        output_schema: DFSchemaRef,
        input: LogicalPlan,
    ) -> Self {
        Self {
            so_path,
            fn_name,
            output_schema,
            input,
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

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MapPartition: so={}, fn={}",
            self.so_path, self.fn_name
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::prelude::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        Ok(Self {
            so_path: self.so_path.clone(),
            fn_name: self.fn_name.clone(),
            output_schema: self.output_schema.clone(),
            input: inputs
                .into_iter()
                .next()
                .ok_or(DataFusionError::Plan(
                    "MapPartition expects single input".to_string(),
                ))?,
        })
    }
}
