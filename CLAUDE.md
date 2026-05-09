# ballista-map-partition

## Project Overview

A Ballista extension providing `map_partition` operator backed by `.so` dynamic libraries. Users write partition processing logic as `.so` shared libraries loaded at runtime via `libloading`.

## Docs

- [README.md](./README.md) — 项目总览、快速开始、API 文档
- [DESIGN.md](./DESIGN.md) — 架构设计：S3 集成、DistributeBy、C Data Interface、SDK 架构
- [PERF.md](./PERF.md) — 性能压测流程、结果对比、内存分析

## Project Structure

```
ballista-map-partition/
├── crates/
│   ├── ballista-map-partition/    # Main crate: the Ballista operator
│   │   ├── proto/                 # Protobuf message definitions
│   │   ├── build.rs               # tonic/prost codegen
│   │   └── src/
│   │       ├── logical/           # MapPartition (UserDefinedLogicalNodeCore)
│   │       ├── physical/          # MapPartitionExec (ExecutionPlan)
│   │       ├── physical_optimizer/ # EnforceDistributeBy rule
│   │       ├── dataframe/         # DataFrameExt API
│   │       ├── planner/           # ExtensionPlanner
│   │       └── codec/             # Ballista serialization codecs
│   └── map-partition-sdk/         # SDK for writing .so processors
│       └── src/
│           ├── processor.rs       # PartitionProcessor trait (requires schema() method)
│           ├── ipc.rs             # IPC helpers (schema) + C Data Interface helpers (batch)
│           └── export.rs          # export_partition_processor! macro (generates C ABI functions)
└── data/                          # Test data
```

## Architecture

### Extension Pipeline (Ballista Standard Pattern)

1. **Proto** (`proto/extension.proto`) — `LMapPartition`/`PMapPartition` messages
2. **Logical Node** — `UserDefinedLogicalNodeCore` impl
3. **DataFrame Ext** — `DataFrameExt::map_partition()` + `with_distribute_by()` API
4. **Physical Node** — `ExecutionPlan` impl with .so C ABI lifecycle
5. **Extension Planner** — Logical→Physical conversion
6. **Codec** — Protobuf serialization for Ballista distribution
7. **Physical Optimizer** — `EnforceDistributeBy` rule for RepartitionExec insertion

### .so C ABI Lifecycle (5 phases)

Each `.so` exposes 5 C ABI functions:

| Phase | Function | Called |
|-------|----------|--------|
| init | `<fn>_init(schema_ptr, len) -> *mut c_void` | Once |
| feed | `<fn>_feed(ctx, *mut FFI_ArrowArray) -> i32` | Multiple (streaming input) |
| execute | `<fn>_execute(ctx) -> i32` | Once |
| fetch | `<fn>_fetch(ctx, *mut FFI_ArrowArray) -> i32` | Multiple (streaming output) |
| finish | `<fn>_finish(ctx) -> i32` | Once |

**Schema** uses Arrow IPC bytes (small metadata, negligible overhead).

**Data** uses the Arrow C Data Interface (`FFI_ArrowArray`) for zero-copy transfer. The framework exports a `RecordBatch` via `to_ffi` → passes the pointer to the .so → the .so imports via `from_ffi_and_data_type` → feeds the `RecordBatch` to the processor. For output, the .so exports via `to_ffi` → writes into the framework's pre-allocated `FFI_ArrowArray::empty()` slot → the framework imports via `from_ffi_and_data_type`.

SDK crate (`map-partition-sdk`) provides the `PartitionProcessor` trait and `export_partition_processor!` macro to handle FFI encode/decode and ABI export automatically.

### Arrow C Data Interface (Zero-Copy FFI)

Data crosses the framework↔.so boundary via the Arrow C Data Interface, eliminating the IPC serialization overhead:

| Direction | Framework side | .so (SDK) side |
|-----------|---------------|----------------|
| **Feed** (input) | `to_ffi(&data)` → `Box::new(ffi_array)` → pass `*mut` | `FFI_ArrowArray::from_raw(ptr)` takes ownership → `from_ffi_and_data_type(...)` imports |
| **Fetch** (output) | `FFI_ArrowArray::empty()` → pass `*mut` | `to_ffi(&data)` → `ptr::write(ptr, ffi_array)` exports |

Key ownership rules:
- **Feed**: Framework's `Box<FFI_ArrowArray>` is consumed by SDK's `from_raw`, which replaces the heap slot with `empty()` (release: None). Framework reclaims the Box (now containing empty) and drops it safely.
- **Fetch**: Framework pre-allocates `FFI_ArrowArray::empty()` on the stack. SDK writes exported data into that slot via `ptr::write`. Framework imports via `from_ffi_and_data_type`. No `libc::free` needed — the imported `ArrayData` owns all buffers.

Benefits over IPC:
- **Zero-copy**: Buffers are `Arc`-referenced, not serialized/deserialized
- **No memory peaks**: Eliminates the ~3× IPC memory duplication (original batch + serialized bytes + decoded batch)
- **No `libc::free`**: Framework doesn't need to manually free SDK-allocated memory

### DistributeBy Partitioning

`with_distribute_by(expr, num_partitions)` guarantees **same key → same processor** via two layers:

1. **Shuffle layer**: `EnforceDistributeBy` rule inserts `RepartitionExec(Hash(expr), N)` before `MapPartitionExec`
2. **Grouping layer**: `split_batch_by_key()` within each partition routes rows to per-key processor instances

Hash collisions are handled: multiple keys can land in the same partition, but each gets its own processor with isolated lifecycle.

## Processor Design Notes

When writing `.so` processors, follow these rules to avoid memory issues:

### Memory Management

1. **Do NOT cache entire input datasets in memory**. The `_feed` phase receives batches one at a time. If you need aggregation, store only the derived state (counts, groups, etc.), not the raw batches. Cloning individual fields is OK; cloning entire batches defeats the purpose of streaming.

2. **Large fields (JSON blobs, binary, etc.)**: The FFI is zero-copy, so batch data crossing the boundary is `Arc`-referenced — no serialization overhead. However, if your processor clones or caches large fields, memory multiplies per-row. Consider extracting only the fields you need in `feed()` and discarding the rest.

3. **`execute()` is where work happens**: Accumulate in `feed()`, process in `execute()`. This separation lets the framework release input batches immediately after `feed()` returns (modulo any data your processor retained via clones).

4. **Stream output via `fetch()`**: Return output in manageable batch sizes. Don't accumulate all results and return a single giant batch — this causes the framework to hold peak memory.

### PartitionProcessor Trait

The trait now requires a `schema()` method returning `&SchemaRef`. The SDK uses this to construct the `DataType::Struct(...)` needed by `from_ffi_and_data_type` for decoding incoming batches.

### Cross-Version Arrow Compatibility

The `.so` (compiled with arrow 54) and the framework (compiled with arrow 57 via DataFusion 52) exchange data through the C Data Interface. Both versions' `FFI_ArrowArray` structs have identical `#[repr(C)]` layout, so the boundary is ABI-safe. Release callbacks are function pointers — the consumer always calls the callback set by the producer, regardless of version.

## Build & Test

```bash
# Build all crates
cargo build

# Build examples
cargo build --examples

# Build .so processor example
cargo build --release -p region_cluster_processor

# Run tests
cargo test -p ballista-map-partition

# E2E tests (need MinIO + Scheduler + Executor + .so)
MAP_PARTITION_SO=target/release/libregion_cluster_processor.so \
  cargo test -p ballista-map-partition --test e2e
```

## Key Dependencies

- DataFusion 52 (uses arrow 57 internally)
- Ballista 52
- Arrow 54 (SDK/.so) / Arrow 57 (framework via DataFusion)
- Rust Edition 2024
- `arrow::ffi` feature must be enabled on both sides

## Conventions

- Logical plan nodes implement `UserDefinedLogicalNodeCore`
- Physical plans implement `ExecutionPlan` with `required_input_distribution` for partitioning requirements
- Codec follows decorator pattern: wraps `BallistaLogicalExtensionCodec`/`BallistaPhysicalExtensionCodec`
- EnforceDistributeBy intentionally duplicates `required_input_distribution` — DataFusion's built-in rule may skip RepartitionExec for small datasets
- `monitoring` feature has been removed; the ballista-monitor crate is deleted
