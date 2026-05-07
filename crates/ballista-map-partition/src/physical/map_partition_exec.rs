use std::any::Any;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, Array};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    execution_plan::EmissionType,
    execution_plan::Boundedness,
};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::Distribution;
use datafusion::scalar::ScalarValue;
use futures_util::stream::StreamExt;

/// Encode an Arrow Schema to IPC bytes. Used by both logical and physical nodes.
pub fn encode_schema_to_ipc(schema: &Schema) -> Result<Vec<u8>, DataFusionError> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema)
        .map_err(|e| DataFusionError::Internal(format!("failed to create schema IPC writer: {e}")))?;
    writer
        .finish()
        .map_err(|e| DataFusionError::Internal(format!("failed to finish schema IPC writer: {e}")))?;
    Ok(buf)
}

fn encode_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>, DataFusionError> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
        .map_err(|e| DataFusionError::Internal(format!("failed to create batch IPC writer: {e}")))?;
    writer
        .write(batch)
        .map_err(|e| DataFusionError::Internal(format!("failed to write batch IPC: {e}")))?;
    writer
        .finish()
        .map_err(|e| DataFusionError::Internal(format!("failed to finish batch IPC writer: {e}")))?;
    Ok(buf)
}

fn decode_ipc_to_batch(bytes: &[u8]) -> Result<RecordBatch, DataFusionError> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| DataFusionError::Internal(format!("failed to create IPC reader: {e}")))?;
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| DataFusionError::Internal(format!("failed to read IPC batches: {e}")))?;
    batches
        .into_iter()
        .next()
        .ok_or_else(|| DataFusionError::Internal("no batch in IPC output".to_string()))
}

/// Wrapper to make raw pointers Send-safe within our spawned task.
/// Safety: we ensure these pointers are only accessed from the single spawned task.
struct SoContext {
    ctx: *mut std::ffi::c_void,
}

unsafe impl Send for SoContext {}

/// A processor instance for a single distribute_by key value.
/// Used when distribute_by is set to group rows by key within a partition.
struct GroupProcessor {
    so_ctx: SoContext,
    #[cfg(feature = "monitoring")]
    monitor_id: String,
}

/// Split a RecordBatch into sub-batches grouped by the distribute_by key.
/// Returns a Vec of (key_value, sub_batch) pairs, preserving key order.
fn split_batch_by_key(
    batch: &RecordBatch,
    key_expr: &Arc<dyn PhysicalExpr>,
) -> Result<Vec<(ScalarValue, RecordBatch)>, DataFusionError> {
    let key_value = key_expr.evaluate(batch)?;
    let key_array = key_value.into_array(batch.num_rows())?;

    // Collect unique key values in order of appearance using ScalarValue (avoids arrow version conflicts)
    let mut seen_keys: Vec<ScalarValue> = Vec::new();
    let mut key_set: std::collections::HashSet<ScalarValue> = std::collections::HashSet::new();

    for i in 0..key_array.len() {
        if key_array.is_null(i) {
            continue;
        }
        let sv = ScalarValue::try_from_array(&key_array, i)?;
        if key_set.insert(sv.clone()) {
            seen_keys.push(sv);
        }
    }

    // Build a boolean filter for each key and split the batch
    let mut result = Vec::with_capacity(seen_keys.len());
    for key in &seen_keys {
        let filter: BooleanArray = (0..key_array.len())
            .map(|i| {
                if key_array.is_null(i) {
                    false
                } else {
                    let sv = ScalarValue::try_from_array(&key_array, i).unwrap();
                    &sv == key
                }
            })
            .collect();

        let sub_batch = filter_record_batch(batch, &filter)?;
        if sub_batch.num_rows() > 0 {
            result.push((key.clone(), sub_batch));
        }
    }

    Ok(result)
}

fn compute_properties(
    output_schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
    hash_partition: &Option<(Vec<Arc<dyn PhysicalExpr>>, usize)>,
    distribute_by: &Option<Arc<dyn PhysicalExpr>>,
    num_partitions: usize,
) -> PlanProperties {
    // MapPartitionExec is a 1:1 partition mapping — output partition count
    // always equals input partition count. The hash_partition field declares
    // an INPUT distribution requirement (via required_input_distribution),
    // which causes the optimizer to insert RepartitionExec BEFORE this node.
    // It does NOT change the output partitioning of this node itself.
    let partitioning = if distribute_by.is_some() && num_partitions > 0 {
        if input.output_partitioning().partition_count() == num_partitions {
            // Propagate Hash partitioning if input matches expected partition count
            if let Some((exprs, _)) = hash_partition {
                Partitioning::Hash(exprs.clone(), num_partitions)
            } else {
                Partitioning::UnknownPartitioning(num_partitions)
            }
        } else {
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count())
        }
    } else {
        match hash_partition {
            Some((exprs, n)) => {
                if input.output_partitioning().partition_count() == *n {
                    Partitioning::Hash(exprs.clone(), *n)
                } else {
                    Partitioning::UnknownPartitioning(input.output_partitioning().partition_count())
                }
            }
            None => Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
        }
    };
    PlanProperties::new(
        EquivalenceProperties::new(output_schema),
        partitioning,
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
}

// ---- Monitoring helpers (only compiled with `monitoring` feature) ----

#[cfg(feature = "monitoring")]
fn monitor_labels(partition: usize, so_path: &str, key: Option<&str>) -> HashMap<String, String> {
    let mut labels = HashMap::new();
    labels.insert("partition".to_string(), partition.to_string());
    labels.insert("so_path".to_string(), so_path.to_string());
    if let Some(k) = key {
        labels.insert("key".to_string(), k.to_string());
    }
    labels
}

#[cfg(feature = "monitoring")]
fn monitor_record_histogram(name: &str, value: f64, labels: HashMap<String, String>) {
    let registry = ballista_monitor::MetricsRegistry::global();
    registry.lock().unwrap().record_histogram(name, value, labels);
}

#[cfg(feature = "monitoring")]
fn monitor_increment_counter(name: &str, delta: f64, labels: HashMap<String, String>) {
    let registry = ballista_monitor::MetricsRegistry::global();
    registry.lock().unwrap().increment_counter(name, delta, labels);
}

#[cfg(feature = "monitoring")]
fn monitor_log(level: &str, stage: &str, message: &str, labels: HashMap<String, String>) {
    let registry = ballista_monitor::MetricsRegistry::global();
    registry.lock().unwrap().log(level, stage, message, labels);
}

#[cfg(feature = "monitoring")]
fn monitor_add_processor(job_id: &str, so_path: &str, fn_name: &str, partition: usize, key: Option<&str>, init_duration_ms: f64) -> String {
    let registry = ballista_monitor::MetricsRegistry::global();
    registry.lock().unwrap().add_processor(job_id, so_path, fn_name, partition, key, init_duration_ms)
}

#[cfg(feature = "monitoring")]
fn monitor_update_processor(id: &str, stage: &str, rows_in: u64, rows_out: u64, bytes_in: u64, bytes_out: u64, duration_ms: f64) {
    let registry = ballista_monitor::MetricsRegistry::global();
    registry.lock().unwrap().update_processor(id, stage, rows_in, rows_out, bytes_in, bytes_out, duration_ms);
}

#[cfg(feature = "monitoring")]
fn monitor_finish_processor(id: &str, duration_ms: f64) {
    let registry = ballista_monitor::MetricsRegistry::global();
    registry.lock().unwrap().finish_processor(id, duration_ms);
}

#[derive(Debug)]
pub struct MapPartitionExec {
    pub so_path: String,
    pub fn_name: String,
    pub output_schema: SchemaRef,
    pub input: Arc<dyn ExecutionPlan>,
    /// Business-level DistributeBy expression
    pub distribute_by: Option<Arc<dyn PhysicalExpr>>,
    /// Number of partitions (from with_distribute_by)
    pub num_partitions: usize,
    /// Internal: derived hash partition info for RepartitionExec
    pub hash_partition: Option<(Vec<Arc<dyn PhysicalExpr>>, usize)>,
    cache: PlanProperties,
}

impl MapPartitionExec {
    pub fn new(
        so_path: String,
        fn_name: String,
        output_schema: SchemaRef,
        input: Arc<dyn ExecutionPlan>,
        distribute_by: Option<Arc<dyn PhysicalExpr>>,
        num_partitions: usize,
        hash_partition: Option<(Vec<Arc<dyn PhysicalExpr>>, usize)>,
    ) -> Self {
        let cache = compute_properties(output_schema.clone(), input.clone(), &hash_partition, &distribute_by, num_partitions);
        Self {
            so_path,
            fn_name,
            output_schema,
            input,
            distribute_by,
            num_partitions,
            hash_partition,
            cache,
        }
    }
}

impl DisplayAs for MapPartitionExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "MapPartitionExec: so={}, fn={}",
            self.so_path, self.fn_name
        )?;
        if let Some(ref db) = self.distribute_by {
            write!(f, ", distribute_by=[{}]", db)?;
        }
        Ok(())
    }
}

impl ExecutionPlan for MapPartitionExec {
    fn name(&self) -> &str {
        "MapPartitionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.distribute_by.is_some() {
            // When distribute_by is set, require HashPartitioned on the same expression
            if let Some((exprs, _)) = &self.hash_partition {
                vec![Distribution::HashPartitioned(exprs.clone())]
            } else {
                vec![Distribution::UnspecifiedDistribution]
            }
        } else {
            match &self.hash_partition {
                Some((exprs, _)) => vec![Distribution::HashPartitioned(exprs.clone())],
                None => vec![Distribution::UnspecifiedDistribution],
            }
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let input = children
            .into_iter()
            .next()
            .ok_or(DataFusionError::Internal(
                "MapPartitionExec expects single input".to_string(),
            ))?;
        let cache = compute_properties(self.output_schema.clone(), input.clone(), &self.hash_partition, &self.distribute_by, self.num_partitions);
        Ok(Arc::new(Self {
            so_path: self.so_path.clone(),
            fn_name: self.fn_name.clone(),
            output_schema: self.output_schema.clone(),
            input,
            distribute_by: self.distribute_by.clone(),
            num_partitions: self.num_partitions,
            hash_partition: self.hash_partition.clone(),
            cache,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        #[cfg(feature = "monitoring")]
        let job_id = context.session_id();
        let mut input_stream = self.input.execute(partition, context)?;
        let so_path = self.so_path.clone();
        let fn_name = self.fn_name.clone();
        let output_schema = self.output_schema.clone();
        let distribute_by = self.distribute_by.clone();

        let mut builder = RecordBatchReceiverStreamBuilder::new(output_schema.clone(), 4);
        let tx = builder.tx();

        builder.spawn(async move {
            #[cfg(feature = "monitoring")]
            let partition_start = std::time::Instant::now();

            // ---- Phase 1: dlopen .so ----
            let lib = unsafe {
                libloading::Library::new(&so_path).map_err(|e| {
                    DataFusionError::Internal(format!("failed to load {so_path}: {e}"))
                })?
            };

            if let Some(ref key_expr) = distribute_by {
                // ===== DistributeBy mode: group by key =====

                // Get _init symbol
                let init_name = format!("{fn_name}_init");
                let init_func: libloading::Symbol<
                    unsafe extern "C" fn(*const u8, i64) -> *mut std::ffi::c_void,
                > = unsafe {
                    lib.get(init_name.as_bytes()).map_err(|e| {
                        DataFusionError::Internal(format!("symbol {init_name} not found: {e}"))
                    })?
                };

                // Get _feed symbol
                let feed_name = format!("{fn_name}_feed");
                let feed_func: libloading::Symbol<
                    unsafe extern "C" fn(*mut std::ffi::c_void, *const u8, i64) -> i32,
                > = unsafe {
                    lib.get(feed_name.as_bytes()).map_err(|e| {
                        DataFusionError::Internal(format!("symbol {feed_name} not found: {e}"))
                    })?
                };

                // Get _execute symbol
                let exec_name = format!("{fn_name}_execute");
                let exec_func: libloading::Symbol<unsafe extern "C" fn(*mut std::ffi::c_void) -> i32> =
                    unsafe {
                        lib.get(exec_name.as_bytes()).map_err(|e| {
                            DataFusionError::Internal(format!("symbol {exec_name} not found: {e}"))
                        })?
                    };

                // Get _fetch symbol
                let fetch_name = format!("{fn_name}_fetch");
                let fetch_func: libloading::Symbol<
                    unsafe extern "C" fn(*mut std::ffi::c_void, *mut *mut u8, *mut i64) -> i32,
                > = unsafe {
                    lib.get(fetch_name.as_bytes()).map_err(|e| {
                        DataFusionError::Internal(format!("symbol {fetch_name} not found: {e}"))
                    })?
                };

                // Get _finish symbol
                let finish_name = format!("{fn_name}_finish");
                let finish_func: libloading::Symbol<unsafe extern "C" fn(*mut std::ffi::c_void) -> i32> =
                    unsafe {
                        lib.get(finish_name.as_bytes()).map_err(|e| {
                            DataFusionError::Internal(format!("symbol {finish_name} not found: {e}"))
                        })?
                    };

                let input_schema_bytes = encode_schema_to_ipc(input_stream.schema().as_ref())?;

                // ---- Phase 2: Maintain HashMap<ScalarValue, GroupProcessor> + key order ----
                let mut processors: HashMap<ScalarValue, GroupProcessor> = HashMap::new();
                let mut key_order: Vec<ScalarValue> = Vec::new();

                // ---- Phase 3: _feed (streaming input, grouped by key) ----
                while let Some(batch) = input_stream.next().await {
                    let batch = batch?;
                    let sub_batches = split_batch_by_key(&batch, key_expr)?;
                    drop(batch);

                    for (key, sub_batch) in sub_batches {
                        // Lazy init: create processor on first encounter of this key
                        if !processors.contains_key(&key) {
                            #[cfg(feature = "monitoring")]
                            let t_init = std::time::Instant::now();

                            let raw_ctx = unsafe {
                                init_func(input_schema_bytes.as_ptr(), input_schema_bytes.len() as i64)
                            };

                            #[cfg(feature = "monitoring")]
                            let key_str = format!("{key:?}");
                            #[cfg(feature = "monitoring")]
                            let init_dur = t_init.elapsed().as_secs_f64() * 1000.0;
                            #[cfg(feature = "monitoring")]
                            let monitor_id = monitor_add_processor(&job_id, &so_path, &fn_name, partition, Some(&key_str), init_dur);
                            processors.insert(key.clone(), GroupProcessor {
                                so_ctx: SoContext { ctx: raw_ctx },
                                #[cfg(feature = "monitoring")]
                                monitor_id,
                            });
                            key_order.push(key.clone());

                            #[cfg(feature = "monitoring")]
                            {
                                let labels = monitor_labels(partition, &so_path, Some(&key_str));
                                monitor_record_histogram("so_init_duration_ms", init_dur, labels.clone());
                                monitor_log("info", "init", &format!("partition={partition} key={key_str} initialized"), labels);
                            }
                        }

                        #[cfg(feature = "monitoring")]
                        let monitor_id = processors.get(&key).unwrap().monitor_id.clone();

                        let processor = processors.get_mut(&key).unwrap();
                        let input_bytes = encode_batch_to_ipc(&sub_batch)?;

                        #[cfg(feature = "monitoring")]
                        let t_feed = std::time::Instant::now();
                        #[cfg(feature = "monitoring")]
                        let feed_rows = sub_batch.num_rows() as u64;
                        #[cfg(feature = "monitoring")]
                        let feed_bytes = input_bytes.len() as u64;

                        let rc = unsafe {
                            feed_func(processor.so_ctx.ctx, input_bytes.as_ptr(), input_bytes.len() as i64)
                        };
                        if rc != 0 {
                            #[cfg(feature = "monitoring")]
                            {
                                let labels = monitor_labels(partition, &so_path, Some(&format!("{key:?}")));
                                monitor_increment_counter("so_error_count", 1.0, labels);
                                monitor_log("error", "feed", &format!("partition={partition} key={key:?} error code {rc}"), monitor_labels(partition, &so_path, Some(&format!("{key:?}"))));
                            }
                            return Err(DataFusionError::Internal(format!(
                                "{feed_name} returned error code {rc} for key {key:?}"
                            )));
                        }

                        #[cfg(feature = "monitoring")]
                        {
                            let key_str = format!("{key:?}");
                            let feed_dur = t_feed.elapsed().as_secs_f64() * 1000.0;
                            let labels = monitor_labels(partition, &so_path, Some(&key_str));
                            monitor_record_histogram("so_feed_duration_ms", feed_dur, labels.clone());
                            monitor_increment_counter("so_feed_input_rows", feed_rows as f64, labels.clone());
                            monitor_increment_counter("so_feed_input_bytes", feed_bytes as f64, labels);
                            monitor_update_processor(&monitor_id, "feed", feed_rows, 0, feed_bytes, 0, feed_dur);
                        }
                    }
                }

                // ---- Phase 4: _execute (serial for all processors, in key order) ----
                for key in &key_order {
                    #[cfg(feature = "monitoring")]
                    let t_exec = std::time::Instant::now();

                    let processor = processors.get_mut(key).unwrap();
                    let rc = unsafe { exec_func(processor.so_ctx.ctx) };
                    if rc != 0 {
                        #[cfg(feature = "monitoring")]
                        {
                            let labels = monitor_labels(partition, &so_path, Some(&format!("{key:?}")));
                            monitor_increment_counter("so_error_count", 1.0, labels);
                        }
                        return Err(DataFusionError::Internal(format!(
                            "{exec_name} returned error code {rc} for key {key:?}"
                        )));
                    }

                    #[cfg(feature = "monitoring")]
                    {
                        let key_str = format!("{key:?}");
                        let exec_dur = t_exec.elapsed().as_secs_f64() * 1000.0;
                        let labels = monitor_labels(partition, &so_path, Some(&key_str));
                        monitor_record_histogram("so_execute_duration_ms", exec_dur, labels.clone());
                        monitor_update_processor(&processors.get(key).unwrap().monitor_id, "execute", 0, 0, 0, 0, exec_dur);
                        monitor_log("info", "execute", &format!("partition={partition} key={key_str} executed"), labels);
                    }
                }

                // ---- Phase 5: _fetch (serial, ordered by key) ----
                #[cfg(feature = "monitoring")]
                let mut _total_output_rows: u64 = 0;
                #[cfg(feature = "monitoring")]
                let mut _total_output_bytes: u64 = 0;

                for key in &key_order {
                    #[cfg(feature = "monitoring")]
                    let monitor_id = processors.get(key).unwrap().monitor_id.clone();
                    let processor = processors.get_mut(key).unwrap();
                    loop {
                        #[cfg(feature = "monitoring")]
                        let t_fetch = std::time::Instant::now();
                        // Scope raw pointer usage before .await to satisfy Send
                        let (output_batch, status, out_rows, out_bytes) = {
                            let mut output_ptr: *mut u8 = std::ptr::null_mut();
                            let mut output_len: i64 = 0;

                            let status = unsafe { fetch_func(processor.so_ctx.ctx, &mut output_ptr, &mut output_len) };

                            if status < 0 {
                                return Err(DataFusionError::Internal(format!(
                                    "{fetch_name} returned error code {status} for key {key:?}"
                                )));
                            }

                            if output_ptr.is_null() || output_len == 0 {
                                (None, status, 0u64, 0u64)
                            } else {
                                let output_bytes =
                                    unsafe { std::slice::from_raw_parts(output_ptr, output_len as usize) };
                                let batch = decode_ipc_to_batch(output_bytes)?;
                                let out_rows = batch.num_rows() as u64;
                                let out_bytes = output_len as u64;
                                unsafe { libc::free(output_ptr as *mut libc::c_void) };
                                (Some(batch), status, out_rows, out_bytes)
                            }
                        };

                        #[cfg(feature = "monitoring")]
                        if output_batch.is_some() {
                            let key_str = format!("{key:?}");
                            let fetch_dur = t_fetch.elapsed().as_secs_f64() * 1000.0;
                            let labels = monitor_labels(partition, &so_path, Some(&key_str));
                            monitor_record_histogram("so_fetch_duration_ms", fetch_dur, labels.clone());
                            monitor_increment_counter("so_fetch_output_rows", out_rows as f64, labels.clone());
                            monitor_increment_counter("so_fetch_output_bytes", out_bytes as f64, labels);
                            monitor_update_processor(&monitor_id, "fetch", 0, out_rows, 0, out_bytes, fetch_dur);
                            _total_output_rows += out_rows;
                            _total_output_bytes += out_bytes;
                        }

                        if let Some(batch) = output_batch {
                            tx.send(Ok(batch)).await.unwrap();
                        } else {
                            break;
                        }

                        if status == 1 {
                            break; // last batch for this processor
                        }
                    }
                }

                // ---- Phase 6: _finish (serial for all processors, in key order) ----
                for key in &key_order {
                    #[cfg(feature = "monitoring")]
                    let t_finish = std::time::Instant::now();

                    let processor = processors.get_mut(key).unwrap();
                    let rc = unsafe { finish_func(processor.so_ctx.ctx) };
                    if rc != 0 {
                        return Err(DataFusionError::Internal(format!(
                            "{finish_name} returned error code {rc} for key {key:?}"
                        )));
                    }

                    #[cfg(feature = "monitoring")]
                    {
                        let key_str = format!("{key:?}");
                        let finish_dur = t_finish.elapsed().as_secs_f64() * 1000.0;
                        let labels = monitor_labels(partition, &so_path, Some(&key_str));
                        monitor_record_histogram("so_finish_duration_ms", finish_dur, labels.clone());
                        monitor_finish_processor(&processors.get(key).unwrap().monitor_id, finish_dur);
                        monitor_log("info", "finish", &format!("partition={partition} key={key_str} finished"), labels);
                    }
                }
            } else {
                // ===== Original mode: single processor per partition =====

                // ---- Phase 2: _init ----
                let init_name = format!("{fn_name}_init");
                let init_func: libloading::Symbol<
                    unsafe extern "C" fn(*const u8, i64) -> *mut std::ffi::c_void,
                > = unsafe {
                    lib.get(init_name.as_bytes()).map_err(|e| {
                        DataFusionError::Internal(format!("symbol {init_name} not found: {e}"))
                    })?
                };

                #[cfg(feature = "monitoring")]
                let t_init = std::time::Instant::now();

                let input_schema_bytes = encode_schema_to_ipc(input_stream.schema().as_ref())?;
                let raw_ctx = unsafe {
                    init_func(input_schema_bytes.as_ptr(), input_schema_bytes.len() as i64)
                };
                let so_ctx = SoContext { ctx: raw_ctx };

                #[cfg(feature = "monitoring")]
                {
                    let init_dur = t_init.elapsed().as_secs_f64() * 1000.0;
                    let labels = monitor_labels(partition, &so_path, None);
                    monitor_record_histogram("so_init_duration_ms", init_dur, labels.clone());
                    monitor_log("info", "init", &format!("partition={partition} initialized"), labels);
                }

                #[cfg(feature = "monitoring")]
                let monitor_id = monitor_add_processor(&job_id, &so_path, &fn_name, partition, None, t_init.elapsed().as_secs_f64() * 1000.0);

                // ---- Phase 3: _feed (streaming input) ----
                let feed_name = format!("{fn_name}_feed");
                let feed_func: libloading::Symbol<
                    unsafe extern "C" fn(*mut std::ffi::c_void, *const u8, i64) -> i32,
                > = unsafe {
                    lib.get(feed_name.as_bytes()).map_err(|e| {
                        DataFusionError::Internal(format!("symbol {feed_name} not found: {e}"))
                    })?
                };

                while let Some(batch) = input_stream.next().await {
                    let batch = batch?;
                    let input_bytes = encode_batch_to_ipc(&batch)?;

                    #[cfg(feature = "monitoring")]
                    let feed_rows = batch.num_rows() as u64;
                    #[cfg(feature = "monitoring")]
                    let feed_bytes_in = input_bytes.len() as u64;
                    #[cfg(feature = "monitoring")]
                    let t_feed = std::time::Instant::now();

                    drop(batch);

                    let rc = unsafe {
                        feed_func(so_ctx.ctx, input_bytes.as_ptr(), input_bytes.len() as i64)
                    };
                    if rc != 0 {
                        #[cfg(feature = "monitoring")]
                        {
                            let labels = monitor_labels(partition, &so_path, None);
                            monitor_increment_counter("so_error_count", 1.0, labels);
                            monitor_log("error", "feed", &format!("partition={partition} error code {rc}"), monitor_labels(partition, &so_path, None));
                        }
                        return Err(DataFusionError::Internal(format!(
                            "{feed_name} returned error code {rc}"
                        )));
                    }

                    #[cfg(feature = "monitoring")]
                    {
                        let feed_dur = t_feed.elapsed().as_secs_f64() * 1000.0;
                        let labels = monitor_labels(partition, &so_path, None);
                        monitor_record_histogram("so_feed_duration_ms", feed_dur, labels.clone());
                        monitor_increment_counter("so_feed_input_rows", feed_rows as f64, labels.clone());
                        monitor_increment_counter("so_feed_input_bytes", feed_bytes_in as f64, labels);
                        monitor_update_processor(&monitor_id, "feed", feed_rows, 0, feed_bytes_in, 0, feed_dur);
                    }
                }

                // ---- Phase 4: _execute ----
                let exec_name = format!("{fn_name}_execute");
                let exec_func: libloading::Symbol<unsafe extern "C" fn(*mut std::ffi::c_void) -> i32> =
                    unsafe {
                        lib.get(exec_name.as_bytes()).map_err(|e| {
                            DataFusionError::Internal(format!("symbol {exec_name} not found: {e}"))
                        })?
                    };

                #[cfg(feature = "monitoring")]
                let t_exec = std::time::Instant::now();

                let rc = unsafe { exec_func(so_ctx.ctx) };
                if rc != 0 {
                    #[cfg(feature = "monitoring")]
                    {
                        let labels = monitor_labels(partition, &so_path, None);
                        monitor_increment_counter("so_error_count", 1.0, labels);
                    }
                    return Err(DataFusionError::Internal(format!(
                        "{exec_name} returned error code {rc}"
                    )));
                }

                #[cfg(feature = "monitoring")]
                {
                    let exec_dur = t_exec.elapsed().as_secs_f64() * 1000.0;
                    let labels = monitor_labels(partition, &so_path, None);
                    monitor_record_histogram("so_execute_duration_ms", exec_dur, labels.clone());
                    monitor_update_processor(&monitor_id, "execute", 0, 0, 0, 0, exec_dur);
                    monitor_log("info", "execute", &format!("partition={partition} executed"), labels);
                }

                // ---- Phase 5: _fetch (streaming output) ----
                let fetch_name = format!("{fn_name}_fetch");
                let fetch_func: libloading::Symbol<
                    unsafe extern "C" fn(*mut std::ffi::c_void, *mut *mut u8, *mut i64) -> i32,
                > = unsafe {
                    lib.get(fetch_name.as_bytes()).map_err(|e| {
                        DataFusionError::Internal(format!("symbol {fetch_name} not found: {e}"))
                    })?
                };

                loop {
                    #[cfg(feature = "monitoring")]
                    let t_fetch = std::time::Instant::now();

                    let (output_batch, status) = {
                        let mut output_ptr: *mut u8 = std::ptr::null_mut();
                        let mut output_len: i64 = 0;
                        let status = unsafe { fetch_func(so_ctx.ctx, &mut output_ptr, &mut output_len) };

                        if status < 0 {
                            return Err(DataFusionError::Internal(format!(
                                "{fetch_name} returned error code {status}"
                            )));
                        }

                        if output_ptr.is_null() || output_len == 0 {
                            (None, status)
                        } else {
                            let output_bytes =
                                unsafe { std::slice::from_raw_parts(output_ptr, output_len as usize) };
                            let batch = decode_ipc_to_batch(output_bytes)?;
                            unsafe { libc::free(output_ptr as *mut libc::c_void) };
                            (Some(batch), status)
                        }
                    };

                    #[cfg(feature = "monitoring")]
                    if let Some(ref batch) = output_batch {
                        let out_rows = batch.num_rows() as u64;
                        let out_bytes = batch.get_array_memory_size() as u64;
                        let fetch_dur = t_fetch.elapsed().as_secs_f64() * 1000.0;
                        let labels = monitor_labels(partition, &so_path, None);
                        monitor_record_histogram("so_fetch_duration_ms", fetch_dur, labels.clone());
                        monitor_increment_counter("so_fetch_output_rows", out_rows as f64, labels.clone());
                        monitor_increment_counter("so_fetch_output_bytes", out_bytes as f64, labels);
                        monitor_update_processor(&monitor_id, "fetch", 0, out_rows, 0, out_bytes, fetch_dur);
                    }

                    if let Some(batch) = output_batch {
                        tx.send(Ok(batch)).await.unwrap();
                    } else {
                        break;
                    }

                    if status == 1 {
                        break; // last batch
                    }
                }

                // ---- Phase 6: _finish ----
                let finish_name = format!("{fn_name}_finish");
                let finish_func: libloading::Symbol<unsafe extern "C" fn(*mut std::ffi::c_void) -> i32> =
                    unsafe {
                        lib.get(finish_name.as_bytes()).map_err(|e| {
                            DataFusionError::Internal(format!("symbol {finish_name} not found: {e}"))
                        })?
                    };

                #[cfg(feature = "monitoring")]
                let t_finish = std::time::Instant::now();

                let rc = unsafe { finish_func(so_ctx.ctx) };
                if rc != 0 {
                    return Err(DataFusionError::Internal(format!(
                        "{finish_name} returned error code {rc}"
                    )));
                }

                #[cfg(feature = "monitoring")]
                {
                    let finish_dur = t_finish.elapsed().as_secs_f64() * 1000.0;
                    let labels = monitor_labels(partition, &so_path, None);
                    monitor_record_histogram("so_finish_duration_ms", finish_dur, labels.clone());
                    monitor_finish_processor(&monitor_id, finish_dur);
                    monitor_log("info", "finish", &format!("partition={partition} finished"), labels);
                }
            }

            #[cfg(feature = "monitoring")]
            {
                let labels = monitor_labels(partition, &so_path, None);
                monitor_record_histogram("so_partition_duration_ms", partition_start.elapsed().as_secs_f64() * 1000.0, labels);
            }

            Ok(())
        });

        Ok(builder.build())
    }
}
