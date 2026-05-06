use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use map_partition_sdk::{PartitionProcessor, export_partition_processor};

/// Face cluster processor: groups face capture records by channelid into dossiers.
///
/// Input schema:  (channelid: Utf8, captime: Utf8, recordid: Utf8)
/// Output schema: (dossierid: Utf8, clusterids: Utf8)
///
/// Each unique channelid becomes a dossier. The `clusterids` field contains
/// a comma-separated list of all recordid values belonging to that channel.
struct FaceClusterProcessor {
    /// channelid -> list of recordids
    clusters: HashMap<String, Vec<String>>,
    /// Results after execute(), consumed by fetch()
    output_rows: Vec<(String, String)>,
    output_index: usize,
}

/// Cast any string-like column (Utf8, Utf8View, Dictionary) to StringArray.
fn to_string_array(col: &Arc<dyn Array>) -> StringArray {
    match col.data_type() {
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("downcast Utf8 failed")
            .clone(),
        _ => {
            let casted = arrow::compute::kernels::cast::cast_with_options(
                col,
                &DataType::Utf8,
                &CastOptions::default(),
            )
            .expect("cast to Utf8 failed");
            casted
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("downcast casted Utf8 failed")
                .clone()
        }
    }
}

impl PartitionProcessor for FaceClusterProcessor {
    fn new(_schema: SchemaRef) -> Self {
        Self {
            clusters: HashMap::new(),
            output_rows: Vec::new(),
            output_index: 0,
        }
    }

    fn feed(&mut self, batch: RecordBatch) {
        let schema = batch.schema();
        let ch_idx = schema.index_of("channelid").unwrap_or(0);
        let rec_idx = schema.index_of("recordid").unwrap_or(2);

        let channelids = to_string_array(batch.column(ch_idx));
        let recordids = to_string_array(batch.column(rec_idx));

        for i in 0..batch.num_rows() {
            if channelids.is_null(i) || recordids.is_null(i) {
                continue;
            }
            let ch = channelids.value(i).to_string();
            let rec = recordids.value(i).to_string();
            self.clusters.entry(ch).or_default().push(rec);
        }
    }

    fn execute(&mut self) {
        let mut rows: Vec<(String, String)> = self
            .clusters
            .iter()
            .map(|(ch, recs)| {
                let dossierid = format!("dossier_{ch}");
                let clusterids = recs.join(",");
                (dossierid, clusterids)
            })
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        self.output_rows = rows;
    }

    fn fetch(&mut self) -> Option<RecordBatch> {
        if self.output_index >= self.output_rows.len() {
            return None;
        }

        let output_schema = SchemaRef::new(Schema::new(vec![
            Field::new("dossierid", DataType::Utf8, false),
            Field::new("clusterids", DataType::Utf8, false),
        ]));

        let dossierids: StringArray = self
            .output_rows
            .iter()
            .map(|(d, _)| Some(d.as_str()))
            .collect();
        let clusterids: StringArray = self
            .output_rows
            .iter()
            .map(|(_, c)| Some(c.as_str()))
            .collect();

        self.output_index = self.output_rows.len();

        Some(
            RecordBatch::try_new(output_schema, vec![Arc::new(dossierids), Arc::new(clusterids)])
                .expect("failed to create output batch"),
        )
    }

    fn finish(&mut self) {}
}

export_partition_processor!(FaceClusterProcessor, face_cluster_processor);

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::SchemaRef;

    fn create_test_schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("channelid", DataType::Utf8, true),
            Field::new("captime", DataType::Utf8, false),
            Field::new("recordid", DataType::Utf8, true),
        ]))
    }

    fn create_record_batch(
        channelids: Vec<&str>,
        recordids: Vec<&str>,
    ) -> RecordBatch {
        let schema = create_test_schema();
        let channel_array: StringArray = channelids.into_iter().map(Some).collect();
        let captime_array: StringArray = vec!["2024-01-01"; recordids.len()]
            .into_iter()
            .map(Some)
            .collect();
        let record_array: StringArray = recordids.into_iter().map(Some).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(channel_array),
                Arc::new(captime_array),
                Arc::new(record_array),
            ],
        )
        .unwrap()
    }

    fn create_record_batch_with_nulls(
        channelids: Vec<Option<&str>>,
        recordids: Vec<Option<&str>>,
    ) -> RecordBatch {
        let schema = create_test_schema();
        let channel_array: StringArray = channelids.into_iter().collect();
        let captime_array: StringArray = vec![Some("2024-01-01"); recordids.len()]
            .into_iter()
            .collect();
        let record_array: StringArray = recordids.into_iter().collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(channel_array),
                Arc::new(captime_array),
                Arc::new(record_array),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_feed_single_record() {
        let schema = create_test_schema();
        let mut processor = FaceClusterProcessor::new(schema);
        let batch = create_record_batch(vec!["ch1"], vec!["rec1"]);
        processor.feed(batch);

        assert_eq!(processor.clusters.len(), 1);
        assert_eq!(processor.clusters.get("ch1"), Some(&vec!["rec1".to_string()]));
    }

    #[test]
    fn test_feed_multiple_records_same_channel() {
        let schema = create_test_schema();
        let mut processor = FaceClusterProcessor::new(schema);
        let batch = create_record_batch(
            vec!["ch1", "ch1", "ch1"],
            vec!["rec1", "rec2", "rec3"],
        );
        processor.feed(batch);

        assert_eq!(processor.clusters.len(), 1);
        let records = processor.clusters.get("ch1").unwrap();
        assert_eq!(records.len(), 3);
        assert!(records.contains(&"rec1".to_string()));
        assert!(records.contains(&"rec2".to_string()));
        assert!(records.contains(&"rec3".to_string()));
    }

    #[test]
    fn test_feed_multiple_channels() {
        let schema = create_test_schema();
        let mut processor = FaceClusterProcessor::new(schema);
        let batch = create_record_batch(
            vec!["ch1", "ch2", "ch3"],
            vec!["rec1", "rec2", "rec3"],
        );
        processor.feed(batch);

        assert_eq!(processor.clusters.len(), 3);
        assert_eq!(processor.clusters.get("ch1"), Some(&vec!["rec1".to_string()]));
        assert_eq!(processor.clusters.get("ch2"), Some(&vec!["rec2".to_string()]));
        assert_eq!(processor.clusters.get("ch3"), Some(&vec!["rec3".to_string()]));
    }

    #[test]
    fn test_feed_with_null_channelid() {
        let schema = create_test_schema();
        let mut processor = FaceClusterProcessor::new(schema);
        let batch = create_record_batch_with_nulls(
            vec![Some("ch1"), None, Some("ch2")],
            vec![Some("rec1"), Some("rec2"), Some("rec3")],
        );
        processor.feed(batch);

        assert_eq!(processor.clusters.len(), 2);
        assert_eq!(processor.clusters.get("ch1"), Some(&vec!["rec1".to_string()]));
        assert_eq!(processor.clusters.get("ch2"), Some(&vec!["rec3".to_string()]));
    }

    #[test]
    fn test_feed_with_null_recordid() {
        let schema = create_test_schema();
        let mut processor = FaceClusterProcessor::new(schema);
        let batch = create_record_batch_with_nulls(
            vec![Some("ch1"), Some("ch2"), Some("ch3")],
            vec![Some("rec1"), None, Some("rec3")],
        );
        processor.feed(batch);

        assert_eq!(processor.clusters.len(), 2);
        assert_eq!(processor.clusters.get("ch1"), Some(&vec!["rec1".to_string()]));
        assert_eq!(processor.clusters.get("ch3"), Some(&vec!["rec3".to_string()]));
    }

    #[test]
    fn test_feed_with_all_nulls() {
        let schema = create_test_schema();
        let mut processor = FaceClusterProcessor::new(schema);
        let batch = create_record_batch_with_nulls(
            vec![None, None, None],
            vec![None, None, None],
        );
        processor.feed(batch);

        assert_eq!(processor.clusters.len(), 0);
    }

    #[test]
    fn test_feed_empty_batch() {
        let schema = create_test_schema();
        let mut processor = FaceClusterProcessor::new(schema);
        let batch = create_record_batch(vec![], vec![]);
        processor.feed(batch);

        assert_eq!(processor.clusters.len(), 0);
    }

    #[test]
    fn test_feed_multiple_batches_accumulate() {
        let schema = create_test_schema();
        let mut processor = FaceClusterProcessor::new(schema);

        let batch1 = create_record_batch(vec!["ch1", "ch1"], vec!["rec1", "rec2"]);
        processor.feed(batch1);

        let batch2 = create_record_batch(vec!["ch1", "ch2"], vec!["rec3", "rec4"]);
        processor.feed(batch2);

        assert_eq!(processor.clusters.len(), 2);
        let ch1_records = processor.clusters.get("ch1").unwrap();
        assert_eq!(ch1_records.len(), 3);
        assert_eq!(processor.clusters.get("ch2"), Some(&vec!["rec4".to_string()]));
    }

    #[test]
    fn test_feed_mixed_channels_and_records() {
        let schema = create_test_schema();
        let mut processor = FaceClusterProcessor::new(schema);
        let batch = create_record_batch(
            vec!["ch1", "ch2", "ch1", "ch3", "ch2"],
            vec!["rec1", "rec2", "rec3", "rec4", "rec5"],
        );
        processor.feed(batch);

        assert_eq!(processor.clusters.len(), 3);
        let ch1_records = processor.clusters.get("ch1").unwrap();
        assert_eq!(ch1_records.len(), 2);
        let ch2_records = processor.clusters.get("ch2").unwrap();
        assert_eq!(ch2_records.len(), 2);
        assert_eq!(processor.clusters.get("ch3"), Some(&vec!["rec4".to_string()]));
    }
}
