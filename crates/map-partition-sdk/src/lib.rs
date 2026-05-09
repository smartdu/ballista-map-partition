mod export;
mod ipc;
mod processor;

pub use processor::PartitionProcessor;

// Re-export FFI helpers (macro needs these)
pub use arrow::ffi::FFI_ArrowArray;
pub use ipc::import_batch_from_ffi;
pub use ipc::export_batch_to_ffi;

// Re-export IPC helpers (still used by _init for schema)
pub use ipc::{decode_schema, decode_batch, encode_batch, encode_schema};
