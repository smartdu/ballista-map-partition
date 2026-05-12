use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use map_partition_sdk::{PartitionProcessor, export_partition_processor};

/// A no-op processor that discards all input and produces no output.
/// Input schema matches region_cluster_processor: (region, channelid, captime, recordid, json)
/// Output schema matches region_cluster_processor: (region, dossierid, recordids, json1..4)
struct NoopProcessor {
    input_schema: SchemaRef,
    partition_id: usize,
}

impl PartitionProcessor for NoopProcessor {
    fn new(schema: SchemaRef, partition_id: usize) -> Self {
        Self { input_schema: schema, partition_id }
    }

    fn schema(&self) -> &SchemaRef {
        &self.input_schema
    }

    fn partition_id(&self) -> usize {
        self.partition_id
    }

    fn feed(&mut self, _batch: RecordBatch) {
        // discard all input
    }

    fn execute(&mut self) {
        // nothing to compute
    }

    fn fetch(&mut self) -> Option<RecordBatch> {
        None
    }
}

export_partition_processor!(NoopProcessor, noop_processor);

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};

    fn create_input_batch() -> RecordBatch {
        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("channelid", DataType::Utf8, true),
            Field::new("captime", DataType::Utf8, false),
            Field::new("recordid", DataType::Utf8, true),
            Field::new("json", DataType::Utf8, false),
        ]));

        let region: StringArray = vec!["east", "west"].into_iter().map(Some).collect();
        let channel: StringArray = vec!["ch001", "ch002"].into_iter().map(Some).collect();
        let captime: StringArray = vec!["2024-01-01", "2024-01-02"].into_iter().map(Some).collect();
        let record: StringArray = vec!["rec1", "rec2"].into_iter().map(Some).collect();
        let json: StringArray = vec![r#"{"k":"v"}"#, r#"{"k":"v"}"#].into_iter().map(Some).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(region),
                Arc::new(channel),
                Arc::new(captime),
                Arc::new(record),
                Arc::new(json),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_feed_discards_all() {
        let schema = create_input_batch().schema();
        let mut processor = NoopProcessor::new(schema, 0);
        let batch = create_input_batch();
        processor.feed(batch);
        // no panic = success
    }

    #[test]
    fn test_execute_noop() {
        let schema = create_input_batch().schema();
        let mut processor = NoopProcessor::new(schema, 0);
        processor.execute();
        // no panic = success
    }

    #[test]
    fn test_fetch_always_none() {
        let schema = create_input_batch().schema();
        let mut processor = NoopProcessor::new(schema, 0);
        assert!(processor.fetch().is_none());
    }

    #[test]
    fn test_empty_input() {
        let schema = create_input_batch().schema();
        let mut processor = NoopProcessor::new(schema.clone(), 0);
        let batch = RecordBatch::new_empty(schema);
        processor.feed(batch);
        processor.execute();
        assert!(processor.fetch().is_none());
    }
}
