use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

/// User-implemented trait for defining partition processing logic.
///
/// The lifecycle is:
/// 1. `new(schema, partition_id)` — called once at partition start
/// 2. `feed(batch)` — called multiple times, streaming input
/// 3. `execute()` — called once after all input is fed
/// 4. `fetch()` — called multiple times, streaming output
/// 5. `finish()` — called once to release resources
///
/// The framework releases each input batch after `feed()` returns.
/// If you need to retain data, clone it or store it in your struct.
pub trait PartitionProcessor: Send + Sized + 'static {
    /// Called once at the start of each partition.
    /// Receives the input schema and partition ID for contextual awareness.
    fn new(schema: SchemaRef, partition_id: usize) -> Self;

    /// Returns the input schema. Used by the framework to decode FFI arrays.
    fn schema(&self) -> &SchemaRef;

    /// Returns the partition ID this processor is running on.
    fn partition_id(&self) -> usize;

    /// Stream input data. Called once per RecordBatch in the partition.
    /// The framework releases the batch after this call returns.
    fn feed(&mut self, batch: RecordBatch);

    /// Called after all input batches have been fed.
    /// Perform your actual business logic here.
    fn execute(&mut self);

    /// Stream output data. Called repeatedly until `None` is returned.
    /// Return `Some(batch)` for each output batch, `None` when done.
    fn fetch(&mut self) -> Option<RecordBatch>;

    /// Called once after all output has been fetched.
    /// Use this to release any external resources created in `new()`.
    /// The framework drops the processor after this returns.
    fn finish(&mut self) {}
}
