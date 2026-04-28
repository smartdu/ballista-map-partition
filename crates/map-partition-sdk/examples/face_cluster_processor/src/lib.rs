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
}

export_partition_processor!(FaceClusterProcessor, face_cluster_processor);
