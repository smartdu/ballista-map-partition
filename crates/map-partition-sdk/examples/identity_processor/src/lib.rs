use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use map_partition_sdk::{PartitionProcessor, export_partition_processor};

struct IdentityProcessor {
    batches: Vec<RecordBatch>,
    output_index: usize,
}

impl PartitionProcessor for IdentityProcessor {
    fn new(_schema: SchemaRef) -> Self {
        Self {
            batches: Vec::new(),
            output_index: 0,
        }
    }

    fn feed(&mut self, batch: RecordBatch) {
        self.batches.push(batch);
    }

    fn execute(&mut self) {
        // Identity: pass through all cached batches
    }

    fn fetch(&mut self) -> Option<RecordBatch> {
        if self.output_index < self.batches.len() {
            let batch = self.batches[self.output_index].clone();
            self.output_index += 1;
            Some(batch)
        } else {
            None
        }
    }
}

export_partition_processor!(IdentityProcessor, identity_processor);
