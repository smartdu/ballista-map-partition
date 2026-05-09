use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{Array, StructArray};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::ffi::{FFI_ArrowArray, from_ffi_and_data_type, to_ffi};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

/// Decode Arrow IPC bytes into a Schema.
pub fn decode_schema(bytes: &[u8]) -> Result<SchemaRef, String> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| format!("failed to create IPC reader for schema: {e}"))?;
    Ok(Arc::new((*reader.schema()).clone()))
}

/// Decode Arrow IPC bytes into a RecordBatch.
/// The IPC stream is expected to contain exactly one batch.
pub fn decode_batch(bytes: &[u8]) -> Result<RecordBatch, String> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| format!("failed to create IPC reader for batch: {e}"))?;
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("failed to read IPC batches: {e}"))?;
    batches
        .into_iter()
        .next()
        .ok_or_else(|| "no batch in IPC bytes".to_string())
}

/// Encode a RecordBatch into Arrow IPC bytes.
/// The returned Vec<u8> is meant to be handed to the framework
/// (which will free it via libc::free after reading).
pub fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
        .map_err(|e| format!("failed to create IPC writer: {e}"))?;
    writer
        .write(batch)
        .map_err(|e| format!("failed to write IPC batch: {e}"))?;
    writer
        .finish()
        .map_err(|e| format!("failed to finish IPC writer: {e}"))?;
    Ok(buf)
}

/// Encode a Schema into Arrow IPC bytes (schema-only stream).
pub fn encode_schema(schema: &Schema) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema)
        .map_err(|e| format!("failed to create IPC writer for schema: {e}"))?;
    writer
        .finish()
        .map_err(|e| format!("failed to finish IPC schema writer: {e}"))?;
    Ok(buf)
}

/// Import a RecordBatch from a raw FFI_ArrowArray pointer via the Arrow C Data Interface.
///
/// Takes ownership of the array at `array_ptr` (replacing it with an empty struct).
/// The framework must have wrapped its local FFI_ArrowArray in ManuallyDrop before
/// passing the pointer, since this function replaces the pointed-to value.
///
/// # Safety
///
/// `array_ptr` must point to a valid `FFI_ArrowArray` exported by the framework.
pub unsafe fn import_batch_from_ffi(
    array_ptr: *mut FFI_ArrowArray,
    data_type: DataType,
) -> Result<RecordBatch, String> {
    // Safety: caller guarantees `array_ptr` points to a valid FFI_ArrowArray
    // exported by the framework.
    unsafe {
        let ffi_array = FFI_ArrowArray::from_raw(array_ptr);
        let array_data = from_ffi_and_data_type(ffi_array, data_type)
            .map_err(|e| format!("from_ffi_and_data_type: {e}"))?;
        let struct_array = StructArray::from(array_data);
        Ok(RecordBatch::from(struct_array))
    }
}

/// Export a RecordBatch to a raw FFI_ArrowArray pointer via the Arrow C Data Interface.
///
/// The framework pre-allocates an empty FFI_ArrowArray and calls the .so's `_fetch`.
/// This function fills that slot with the exported array data.
///
/// # Safety
///
/// `array_ptr` must point to a valid, pre-allocated `FFI_ArrowArray::empty()` slot.
pub unsafe fn export_batch_to_ffi(
    batch: RecordBatch,
    array_ptr: *mut FFI_ArrowArray,
) -> Result<(), String> {
    let struct_array = StructArray::from(batch);
    let data = struct_array.into_data();
    let (ffi_array, _ffi_schema) =
        to_ffi(&data).map_err(|e| format!("to_ffi: {e}"))?;
    // Write into the caller's pre-allocated slot.
    // Safety: caller guarantees `array_ptr` is valid for writes and is a
    // pre-allocated FFI_ArrowArray::empty() slot. We write the exported data
    // and forget the local to transfer ownership to the framework.
    unsafe { std::ptr::write(array_ptr, ffi_array) };
    Ok(())
}
