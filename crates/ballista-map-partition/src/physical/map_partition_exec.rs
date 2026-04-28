use std::any::Any;
use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, PlanProperties,
    execution_plan::EmissionType,
    execution_plan::Boundedness,
};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
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

fn compute_properties(output_schema: SchemaRef) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(output_schema),
        Partitioning::UnknownPartitioning(1),
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
    cache: PlanProperties,
}

impl MapPartitionExec {
    pub fn new(
        so_path: String,
        fn_name: String,
        output_schema: SchemaRef,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let cache = compute_properties(output_schema.clone());
        Self {
            so_path,
            fn_name,
            output_schema,
            input,
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
        )
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
        Ok(Arc::new(Self {
            so_path: self.so_path.clone(),
            fn_name: self.fn_name.clone(),
            output_schema: self.output_schema.clone(),
            input,
            cache: self.cache.clone(),
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

        let mut builder = RecordBatchReceiverStreamBuilder::new(output_schema.clone(), 4);
        let tx = builder.tx();

        builder.spawn(async move {
            // ---- Phase 1: dlopen .so ----
            let lib = unsafe {
                libloading::Library::new(&so_path).map_err(|e| {
                    DataFusionError::Internal(format!("failed to load {so_path}: {e}"))
                })?
            };

            // ---- Phase 2: _init ----
            let init_name = format!("{fn_name}_init");
            let init_func: libloading::Symbol<
                unsafe extern "C" fn(*const u8, i64) -> *mut std::ffi::c_void,
            > = unsafe {
                lib.get(init_name.as_bytes()).map_err(|e| {
                    DataFusionError::Internal(format!("symbol {init_name} not found: {e}"))
                })?
            };

            let input_schema_bytes = encode_schema_to_ipc(input_stream.schema().as_ref())?;
            let raw_ctx = unsafe {
                init_func(input_schema_bytes.as_ptr(), input_schema_bytes.len() as i64)
            };
            // Wrap in Send-safe struct
            let so_ctx = SoContext { ctx: raw_ctx };

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
                // Release the input batch — framework only holds one batch in memory
                drop(batch);

                let rc = unsafe {
                    feed_func(so_ctx.ctx, input_bytes.as_ptr(), input_bytes.len() as i64)
                };
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
                unsafe extern "C" fn(*mut std::ffi::c_void, *mut *mut u8, *mut i64) -> i32,
            > = unsafe {
                lib.get(fetch_name.as_bytes()).map_err(|e| {
                    DataFusionError::Internal(format!("symbol {fetch_name} not found: {e}"))
                })?
            };

            loop {
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
            let rc = unsafe { finish_func(so_ctx.ctx) };
            if rc != 0 {
                return Err(DataFusionError::Internal(format!(
                    "{finish_name} returned error code {rc}"
                )));
            }

            Ok(())
        });

        Ok(builder.build())
    }
}
