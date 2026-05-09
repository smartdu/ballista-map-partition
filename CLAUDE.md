# ballista-map-partition

## Project Overview

A Ballista extension providing `map_partition` operator backed by `.so` dynamic libraries. Users write partition processing logic as `.so` shared libraries loaded at runtime via `libloading`.

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
│           ├── processor.rs       # PartitionProcessor trait
│           ├── ipc.rs             # Arrow IPC helpers
│           └── export.rs          # export_partition_processor! macro
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
| feed | `<fn>_feed(ctx, data_ptr, len) -> i32` | Multiple (streaming input) |
| execute | `<fn>_execute(ctx) -> i32` | Once |
| fetch | `<fn>_fetch(ctx, out_ptr, out_len) -> i32` | Multiple (streaming output) |
| finish | `<fn>_finish(ctx) -> i32` | Once |

Data exchange uses Arrow IPC bytes. SDK crate (`map-partition-sdk`) provides the `PartitionProcessor` trait and `export_partition_processor!` macro to handle IPC encode/decode and ABI export automatically.

### DistributeBy Partitioning

`with_distribute_by(expr, num_partitions)` guarantees **same key → same processor** via two layers:

1. **Shuffle layer**: `EnforceDistributeBy` rule inserts `RepartitionExec(Hash(expr), N)` before `MapPartitionExec`
2. **Grouping layer**: `split_batch_by_key()` within each partition routes rows to per-key processor instances

Hash collisions are handled: multiple keys can land in the same partition, but each gets its own processor with isolated lifecycle.

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

- DataFusion 52
- Ballista 52
- Arrow 54
- Rust Edition 2024

## Conventions

- Logical plan nodes implement `UserDefinedLogicalNodeCore`
- Physical plans implement `ExecutionPlan` with `required_input_distribution` for partitioning requirements
- Codec follows decorator pattern: wraps `BallistaLogicalExtensionCodec`/`BallistaPhysicalExtensionCodec`
- EnforceDistributeBy intentionally duplicates `required_input_distribution` — DataFusion's built-in rule may skip RepartitionExec for small datasets
- `monitoring` feature has been removed; the ballista-monitor crate is deleted
