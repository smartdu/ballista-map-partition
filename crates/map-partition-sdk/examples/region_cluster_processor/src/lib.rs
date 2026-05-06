use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use map_partition_sdk::{PartitionProcessor, export_partition_processor};

/// Region channel cluster processor: within a region partition, clusters records
/// by the same channelid into one dossier.
///
/// Input schema:  (region: Utf8, channelid: Utf8, captime: Utf8, recordid: Utf8)
/// Output schema: (region: Utf8, dossierid: Utf8, recordids: Utf8)
///
/// Clustering logic:
///   1. Data is partitioned by region (via with_distribute_by)
///   2. Within each region partition, records with the same channelid are
///      grouped into a single dossier
///   3. One dossier per unique channelid, recordids joined by comma
struct RegionClusterProcessor {
    /// channelid -> list of recordids
    clusters: HashMap<String, Vec<String>>,
    /// The region value seen so far (first non-null region)
    observed_region: Option<String>,
    /// Set to true if we detect rows from multiple regions in one partition
    cross_region_error: bool,
    /// Results after execute(), consumed by fetch()
    output_rows: Vec<(String, String, String)>,
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

impl PartitionProcessor for RegionClusterProcessor {
    fn new(_schema: SchemaRef) -> Self {
        Self {
            clusters: HashMap::new(),
            observed_region: None,
            cross_region_error: false,
            output_rows: Vec::new(),
            output_index: 0,
        }
    }

    fn feed(&mut self, batch: RecordBatch) {
        let schema = batch.schema();
        let region_idx = schema.index_of("region").unwrap_or(0);
        let channel_idx = schema.index_of("channelid").unwrap_or(1);
        let rec_idx = schema.index_of("recordid").unwrap_or(3);

        let regions = to_string_array(batch.column(region_idx));
        let channelids = to_string_array(batch.column(channel_idx));
        let recordids = to_string_array(batch.column(rec_idx));

        for i in 0..batch.num_rows() {
            if regions.is_null(i) || channelids.is_null(i) || recordids.is_null(i) {
                continue;
            }

            let region_val = regions.value(i).to_string();
            let channel_val = channelids.value(i).to_string();
            let rec_val = recordids.value(i).to_string();

            // Validate that all rows in this partition belong to the same region
            if let Some(ref obs) = self.observed_region {
                if obs != &region_val {
                    self.cross_region_error = true;
                }
            } else {
                self.observed_region = Some(region_val);
            }

            // Cluster by channelid: same channelid → same dossier
            self.clusters.entry(channel_val).or_default().push(rec_val);
        }
    }

    fn execute(&mut self) {
        let region = self.observed_region.clone().unwrap_or_default();

        if self.cross_region_error {
            self.output_rows.push((
                region.clone(),
                "CROSS_REGION_ERROR".to_string(),
                "Multiple regions detected in single partition — repartition failed".to_string(),
            ));
        }

        let mut rows: Vec<(String, String, String)> = self
            .clusters
            .iter()
            .map(|(channelid, recs)| {
                let dossierid = format!("dossier_{channelid}");
                let recordids = recs.join(",");
                (region.clone(), dossierid, recordids)
            })
            .collect();
        rows.sort_by(|a, b| a.1.cmp(&b.1));
        self.output_rows.extend(rows);
    }

    fn fetch(&mut self) -> Option<RecordBatch> {
        if self.output_index >= self.output_rows.len() {
            return None;
        }

        let output_schema = SchemaRef::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("dossierid", DataType::Utf8, false),
            Field::new("recordids", DataType::Utf8, false),
        ]));

        let regions: StringArray = self
            .output_rows
            .iter()
            .map(|(r, _, _)| Some(r.as_str()))
            .collect();
        let dossierids: StringArray = self
            .output_rows
            .iter()
            .map(|(_, c, _)| Some(c.as_str()))
            .collect();
        let recordids: StringArray = self
            .output_rows
            .iter()
            .map(|(_, _, r)| Some(r.as_str()))
            .collect();

        self.output_index = self.output_rows.len();

        Some(
            RecordBatch::try_new(
                output_schema,
                vec![
                    Arc::new(regions),
                    Arc::new(dossierids),
                    Arc::new(recordids),
                ],
            )
            .expect("failed to create output batch"),
        )
    }
}

export_partition_processor!(RegionClusterProcessor, region_cluster_processor);

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("channelid", DataType::Utf8, true),
            Field::new("captime", DataType::Utf8, false),
            Field::new("recordid", DataType::Utf8, true),
        ]))
    }

    fn create_record_batch(
        regions: Vec<&str>,
        channelids: Vec<&str>,
        recordids: Vec<&str>,
    ) -> RecordBatch {
        let schema = create_test_schema();
        let region_array: StringArray = regions.into_iter().map(Some).collect();
        let channel_array: StringArray = channelids.into_iter().map(Some).collect();
        let captime_array: StringArray = vec!["2024-01-01"; recordids.len()].into_iter().map(Some).collect();
        let record_array: StringArray = recordids.into_iter().map(Some).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(region_array),
                Arc::new(channel_array),
                Arc::new(captime_array),
                Arc::new(record_array),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_single_region_single_channel() {
        let schema = create_test_schema();
        let mut processor = RegionClusterProcessor::new(schema);
        let batch = create_record_batch(
            vec!["east", "east"],
            vec!["ch001", "ch001"],
            vec!["rec1", "rec2"],
        );
        processor.feed(batch);

        assert!(!processor.cross_region_error);
        assert_eq!(processor.observed_region, Some("east".to_string()));
        assert_eq!(processor.clusters.len(), 1);
        assert_eq!(processor.clusters.get("ch001").unwrap().len(), 2);
    }

    #[test]
    fn test_single_region_multiple_channels() {
        let schema = create_test_schema();
        let mut processor = RegionClusterProcessor::new(schema);
        let batch = create_record_batch(
            vec!["east", "east", "east"],
            vec!["ch001", "ch001", "ch002"],
            vec!["rec1", "rec2", "rec3"],
        );
        processor.feed(batch);

        assert!(!processor.cross_region_error);
        assert_eq!(processor.clusters.len(), 2);
        assert_eq!(processor.clusters.get("ch001").unwrap().len(), 2);
        assert_eq!(processor.clusters.get("ch002").unwrap().len(), 1);
    }

    #[test]
    fn test_cross_region_detection() {
        let schema = create_test_schema();
        let mut processor = RegionClusterProcessor::new(schema);
        let batch = create_record_batch(
            vec!["east", "west"],
            vec!["ch001", "ch002"],
            vec!["rec1", "rec2"],
        );
        processor.feed(batch);

        assert!(processor.cross_region_error);
    }

    #[test]
    fn test_execute_clusters_by_channel() {
        let schema = create_test_schema();
        let mut processor = RegionClusterProcessor::new(schema);
        let batch = create_record_batch(
            vec!["east", "east", "east"],
            vec!["ch001", "ch001", "ch002"],
            vec!["rec1", "rec2", "rec3"],
        );
        processor.feed(batch);
        processor.execute();

        assert!(!processor.output_rows.iter().any(|(_, c, _)| c == "CROSS_REGION_ERROR"));
        assert!(processor.output_rows.iter().all(|(r, _, _)| r == "east"));

        // Should have 2 clusters: dossier_ch001 and dossier_ch002
        let dossierids: Vec<&str> = processor.output_rows.iter().map(|(_, c, _)| c.as_str()).collect();
        assert!(dossierids.contains(&"dossier_ch001"));
        assert!(dossierids.contains(&"dossier_ch002"));

        // ch001 cluster should have rec1,rec2
        let ch001_row = processor.output_rows.iter().find(|(_, c, _)| c == "dossier_ch001").unwrap();
        assert_eq!(ch001_row.2, "rec1,rec2");

        // ch002 cluster should have rec3
        let ch002_row = processor.output_rows.iter().find(|(_, c, _)| c == "dossier_ch002").unwrap();
        assert_eq!(ch002_row.2, "rec3");
    }

    #[test]
    fn test_execute_cross_region_includes_error() {
        let schema = create_test_schema();
        let mut processor = RegionClusterProcessor::new(schema);
        let batch = create_record_batch(
            vec!["east", "west"],
            vec!["ch001", "ch002"],
            vec!["rec1", "rec2"],
        );
        processor.feed(batch);
        processor.execute();

        assert!(processor.output_rows.iter().any(|(_, c, _)| c == "CROSS_REGION_ERROR"));
    }

    #[test]
    fn test_fetch_returns_batch() {
        let schema = create_test_schema();
        let mut processor = RegionClusterProcessor::new(schema);
        let batch = create_record_batch(
            vec!["north", "north"],
            vec!["ch001", "ch001"],
            vec!["rec1", "rec2"],
        );
        processor.feed(batch);
        processor.execute();

        let output = processor.fetch().unwrap();
        assert_eq!(output.num_rows(), 1); // 1 cluster for ch001
        assert_eq!(output.schema().fields().len(), 3);
    }

    #[test]
    fn test_fetch_returns_none_when_exhausted() {
        let schema = create_test_schema();
        let mut processor = RegionClusterProcessor::new(schema);
        let batch = create_record_batch(
            vec!["east"],
            vec!["ch001"],
            vec!["rec1"],
        );
        processor.feed(batch);
        processor.execute();

        let _ = processor.fetch();
        assert!(processor.fetch().is_none());
    }

    #[test]
    fn test_empty_batch() {
        let schema = create_test_schema();
        let mut processor = RegionClusterProcessor::new(schema);
        let batch = create_record_batch(vec![] as Vec<&str>, vec![] as Vec<&str>, vec![] as Vec<&str>);
        processor.feed(batch);

        assert!(!processor.cross_region_error);
        assert_eq!(processor.clusters.len(), 0);
    }
}
