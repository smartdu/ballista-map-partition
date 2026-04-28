use std::io::Cursor;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
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
