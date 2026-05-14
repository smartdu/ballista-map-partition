use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, StructArray};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::ffi::{FFI_ArrowArray, from_ffi_and_data_type, to_ffi};
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

/// Deep-copy a RecordBatch so its buffers are owned by the framework's allocator,
/// not by the .so. This breaks the dependency on the .so's FFI_ArrowArray release
/// callback, which would otherwise crash if the .so is unloaded before the batch
/// is dropped by the downstream operator.
///
/// Uses IPC round-trip to force all buffers into framework-owned allocations.
/// This handles all Arrow data types generically.
fn deep_copy_batch(batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
    use datafusion::arrow::ipc::reader::StreamReader;
    use datafusion::arrow::ipc::writer::StreamWriter;

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
            .map_err(|e| DataFusionError::Internal(format!("deep copy writer: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| DataFusionError::Internal(format!("deep copy write: {e}")))?;
        writer
            .finish()
            .map_err(|e| DataFusionError::Internal(format!("deep copy finish: {e}")))?;
        // writer dropped here — flushes
    }

    let cursor = std::io::Cursor::new(&buf);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| DataFusionError::Internal(format!("deep copy reader: {e}")))?;
    let mut batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| DataFusionError::Internal(format!("deep copy read: {e}")))?;
    batches
        .pop()
        .ok_or(DataFusionError::Internal("deep copy: empty stream".to_string()))
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
}

/// Split a RecordBatch into sub-batches grouped by the distribute_by key.
/// Returns a Vec of (key_value, sub_batch) pairs, preserving key order.
///
/// Single-pass algorithm: indexes rows into HashMap<Key, Vec<usize>>,
/// then uses arrow::compute::take for vectorized sub-batch extraction.
fn split_batch_by_key(
    batch: &RecordBatch,
    key_expr: &Arc<dyn PhysicalExpr>,
) -> Result<Vec<(ScalarValue, RecordBatch)>, DataFusionError> {
    use datafusion::arrow::array::UInt64Array;
    use datafusion::arrow::compute::take;
    use std::collections::HashMap;

    let key_value = key_expr.evaluate(batch)?;
    let key_array = key_value.into_array(batch.num_rows())?;

    // Single pass: group row indices by key value
    let mut groups: HashMap<ScalarValue, Vec<usize>> = HashMap::new();
    let mut key_order: Vec<ScalarValue> = Vec::new();

    for i in 0..key_array.len() {
        if key_array.is_null(i) {
            continue;
        }
        let sv = ScalarValue::try_from_array(&key_array, i)?;
        if let std::collections::hash_map::Entry::Vacant(e) = groups.entry(sv.clone()) {
            key_order.push(sv);
            e.insert(vec![i]);
        } else {
            groups.get_mut(&sv).unwrap().push(i);
        }
    }

    // Extract sub-batches via take on each column
    let mut result = Vec::with_capacity(key_order.len());
    for key in key_order {
        let indices = groups.remove(&key).unwrap();
        let take_indices = UInt64Array::from(indices.iter().map(|&i| i as u64).collect::<Vec<_>>());
        let cols: Vec<_> = batch.columns().iter().map(|c| {
            take(c.as_ref(), &take_indices, None)
                .map_err(|e| DataFusionError::Internal(format!("take: {e}")))
        }).collect::<Result<_, _>>()?;
        let sub_batch = RecordBatch::try_new(batch.schema(), cols)
            .map_err(|e| DataFusionError::Internal(format!("split_batch_by_key: {e}")))?;
        result.push((key, sub_batch));
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
    /// Cached dlopen handle — shared across all partition tasks
    cached_lib: std::sync::OnceLock<Arc<libloading::Library>>,
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
            cached_lib: std::sync::OnceLock::new(),
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
            cached_lib: std::sync::OnceLock::new(),
            cache,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let mut input_stream = self.input.execute(partition, context)?;
        let so_path = self.so_path.clone();
        let fn_name = self.fn_name.clone();
        let output_schema = self.output_schema.clone();
        let distribute_by = self.distribute_by.clone();

        // Cache dlopen across partitions: first call loads, subsequent calls clone Arc
        let lib = self.cached_lib.get_or_init(|| {
            Arc::new(unsafe {
                libloading::Library::new(&so_path)
                    .expect("failed to dlopen map_partition .so")
            })
        }).clone();

        let mut builder = RecordBatchReceiverStreamBuilder::new(output_schema.clone(), 4);
        let tx = builder.tx();

        builder.spawn(async move {
            // lib (Arc<Library>) is used for all symbol lookups below;
            // it stays alive until this async block completes

            if let Some(ref key_expr) = distribute_by {
                // ===== DistributeBy mode: group by key =====

                // Get _init symbol
                let init_name = format!("{fn_name}_init");
                let init_func: libloading::Symbol<
                    unsafe extern "C" fn(*const u8, i64, i64) -> *mut std::ffi::c_void,
                > = unsafe {
                    lib.get(init_name.as_bytes()).map_err(|e| {
                        DataFusionError::Internal(format!("symbol {init_name} not found: {e}"))
                    })?
                };

                // Get _feed symbol
                let feed_name = format!("{fn_name}_feed");
                let feed_func: libloading::Symbol<
                    unsafe extern "C" fn(*mut std::ffi::c_void, *mut FFI_ArrowArray) -> i32,
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
                    unsafe extern "C" fn(*mut std::ffi::c_void, *mut FFI_ArrowArray) -> i32,
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
                            let raw_ctx = unsafe {
                                init_func(input_schema_bytes.as_ptr(), input_schema_bytes.len() as i64, partition as i64)
                            };

                            processors.insert(key.clone(), GroupProcessor {
                                so_ctx: SoContext { ctx: raw_ctx },
                            });
                            key_order.push(key.clone());
                        }

                        let processor = processors.get_mut(&key).unwrap();
                        // Export batch via C Data Interface (zero-copy)
                        let struct_array = StructArray::from(sub_batch);
                        let data = struct_array.into_data();
                        let (ffi_array, _) = to_ffi(&data)?;
                        let ffi_box = Box::new(ffi_array);
                        let ptr = Box::into_raw(ffi_box);

                        let rc = unsafe { feed_func(processor.so_ctx.ctx, ptr) };
                        // SDK's from_raw replaced *ptr with empty(); reclaim the Box safely
                        let _ = unsafe { Box::from_raw(ptr) };
                        if rc != 0 {
                            return Err(DataFusionError::Internal(format!(
                                "{feed_name} returned error code {rc} for key {key:?}"
                            )));
                        }
                    }
                }

                // ---- Phase 4: _execute (serial for all processors, in key order) ----
                for key in &key_order {
                    let processor = processors.get_mut(key).unwrap();
                    let rc = unsafe { exec_func(processor.so_ctx.ctx) };
                    if rc != 0 {
                        return Err(DataFusionError::Internal(format!(
                            "{exec_name} returned error code {rc} for key {key:?}"
                        )));
                    }
                }

                // ---- Phase 5: _fetch (serial, ordered by key) ----
                let output_data_type = DataType::Struct(output_schema.fields().clone());
                for key in &key_order {
                    let processor = processors.get_mut(key).unwrap();
                    loop {
                        // Scope FFI_ArrowArray before .await to satisfy Send
                        let output_batch = {
                            let mut ffi_array = FFI_ArrowArray::empty();
                            let status = unsafe {
                                fetch_func(processor.so_ctx.ctx, &mut ffi_array as *mut FFI_ArrowArray)
                            };

                            if status < 0 {
                                return Err(DataFusionError::Internal(format!(
                                    "{fetch_name} returned error code {status} for key {key:?}"
                                )));
                            }

                            if status == 1 {
                                None // no more data
                            } else {
                                // status == 0: data was written to ffi_array
                                let array_data = unsafe {
                                    from_ffi_and_data_type(ffi_array, output_data_type.clone())
                                }.map_err(|e| DataFusionError::Internal(format!("from_ffi_and_data_type: {e}")))?;
                                let struct_array = StructArray::from(array_data);
                                Some(deep_copy_batch(&RecordBatch::from(struct_array))?)
                            }
                        };

                        match output_batch {
                            Some(batch) => tx.send(Ok(batch)).await.unwrap(),
                            None => break, // status == 1: done
                        }
                    }
                }

                // ---- Phase 6: _finish (serial for all processors, in key order) ----
                for key in &key_order {
                    let processor = processors.get_mut(key).unwrap();
                    let rc = unsafe { finish_func(processor.so_ctx.ctx) };
                    if rc != 0 {
                        return Err(DataFusionError::Internal(format!(
                            "{finish_name} returned error code {rc} for key {key:?}"
                        )));
                    }
                }

            } else {
                // ===== Original mode: single processor per partition =====

                // ---- Phase 2: _init ----
                let init_name = format!("{fn_name}_init");
                let init_func: libloading::Symbol<
                    unsafe extern "C" fn(*const u8, i64, i64) -> *mut std::ffi::c_void,
                > = unsafe {
                    lib.get(init_name.as_bytes()).map_err(|e| {
                        DataFusionError::Internal(format!("symbol {init_name} not found: {e}"))
                    })?
                };

                let input_schema_bytes = encode_schema_to_ipc(input_stream.schema().as_ref())?;
                let raw_ctx = unsafe {
                    init_func(input_schema_bytes.as_ptr(), input_schema_bytes.len() as i64, partition as i64)
                };
                let so_ctx = SoContext { ctx: raw_ctx };

                // ---- Phase 3: _feed (streaming input) ----
                let feed_name = format!("{fn_name}_feed");
                let feed_func: libloading::Symbol<
                    unsafe extern "C" fn(*mut std::ffi::c_void, *mut FFI_ArrowArray) -> i32,
                > = unsafe {
                    lib.get(feed_name.as_bytes()).map_err(|e| {
                        DataFusionError::Internal(format!("symbol {feed_name} not found: {e}"))
                    })?
                };

                while let Some(batch) = input_stream.next().await {
                    let batch = batch?;
                    // Export batch via C Data Interface (zero-copy)
                    let struct_array = StructArray::from(batch);
                    let data = struct_array.into_data();
                    let (ffi_array, _) = to_ffi(&data)?;
                    let ffi_box = Box::new(ffi_array);
                    let ptr = Box::into_raw(ffi_box);

                    let rc = unsafe { feed_func(so_ctx.ctx, ptr) };
                    // SDK's from_raw replaced *ptr with empty(); reclaim the Box safely
                    let _ = unsafe { Box::from_raw(ptr) };
                    if rc != 0 {
                        return Err(DataFusionError::Internal(format!(
                            "{feed_name} returned error code {rc}"
                        )));
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

                let rc = unsafe { exec_func(so_ctx.ctx) };
                if rc != 0 {
                    return Err(DataFusionError::Internal(format!(
                        "{exec_name} returned error code {rc}"
                    )));
                }

                // ---- Phase 5: _fetch (streaming output) ----
                let fetch_name = format!("{fn_name}_fetch");
                let fetch_func: libloading::Symbol<
                    unsafe extern "C" fn(*mut std::ffi::c_void, *mut FFI_ArrowArray) -> i32,
                > = unsafe {
                    lib.get(fetch_name.as_bytes()).map_err(|e| {
                        DataFusionError::Internal(format!("symbol {fetch_name} not found: {e}"))
                    })?
                };

                let output_data_type = DataType::Struct(output_schema.fields().clone());
                loop {
                    let output_batch = {
                        let mut ffi_array = FFI_ArrowArray::empty();
                        let status = unsafe {
                            fetch_func(so_ctx.ctx, &mut ffi_array as *mut FFI_ArrowArray)
                        };

                        if status < 0 {
                            return Err(DataFusionError::Internal(format!(
                                "{fetch_name} returned error code {status}"
                            )));
                        }

                        if status == 1 {
                            None // no more data
                        } else {
                            let array_data = unsafe {
                                from_ffi_and_data_type(ffi_array, output_data_type.clone())
                            }.map_err(|e| DataFusionError::Internal(format!("from_ffi_and_data_type: {e}")))?;
                            let struct_array = StructArray::from(array_data);
                            Some(deep_copy_batch(&RecordBatch::from(struct_array))?)
                        }
                    };

                    match output_batch {
                        Some(batch) => tx.send(Ok(batch)).await.unwrap(),
                        None => break, // status == 1: done
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

                let rc = unsafe { finish_func(so_ctx.ctx) };
                if rc != 0 {
                    return Err(DataFusionError::Internal(format!(
                        "{finish_name} returned error code {rc}"
                    )));
                }

            }

            Ok(())
        });

        Ok(builder.build())
    }
}
