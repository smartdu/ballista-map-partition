# ballista-map-partition

基于 Apache Ballista 的分布式 `map_partition` 算子实现，对标 Spark 的 `mapPartition` 能力。

## 背景与动机

Spark 提供了 `mapPartition` 算子，允许用户对每个分区的数据执行自定义逻辑。Rust 生态中 DataFusion/Ballista 没有等价算子，且 Rust 不支持闭包的序列化与反序列化，无法像 Spark 那样将用户代码分发到集群节点执行。

本项目通过 **`.so` 动态库 + 五阶段流式生命周期** 的方案解决了这个问题：

- 用户将自定义分区处理逻辑编译为 `.so` 动态库
- 调用 `map_partition` 算子时传入 `.so` 路径和函数名前缀
- 框架在 Executor 节点上通过 `dlopen` 加载 `.so` 并调用对应接口
- 利用 Ballista 的分布式调度能力，将计算逻辑分发到集群各节点执行

## 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client (客户端)                           │
│  DataFrameExt::map_partition(so_path, fn_name, output_schema)  │
│  SessionConfig 注册 LogicalCodec + PhysicalCodec                │
└────────────────────────┬────────────────────────────────────────┘
                         │ LogicalPlan / PhysicalPlan (protobuf)
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Scheduler (调度器)                           │
│  SessionBuilder 注册 QueryPlannerWithExtensions                 │
│  注册 LogicalCodec + PhysicalCodec                               │
└────────────────────────┬────────────────────────────────────────┘
                         │ 物理计划分发
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Executor (执行器)                           │
│  注册 LogicalCodec + PhysicalCodec                               │
│  MapPartitionExec.execute() → dlopen .so → 五阶段调用            │
└─────────────────────────────────────────────────────────────────┘
```

## 项目结构

```
ballista-map-partition/
├── Cargo.toml                          # Workspace 根
├── crates/
│   ├── ballista-map-partition/         # Ballista 扩展 crate
│   │   ├── proto/
│   │   │   └── extension.proto         # Protobuf 消息定义
│   │   ├── build.rs                    # tonic/prost 代码生成
│   │   ├── src/
│   │   │   ├── logical/                # 逻辑算子 (MapPartition)
│   │   │   ├── physical/               # 物理算子 (MapPartitionExec)
│   │   │   ├── dataframe/              # DataFrame 扩展 (DataFrameExt)
│   │   │   ├── planner/                # 扩展查询规划器
│   │   │   └── codec/                  # Ballista 序列化编解码器
│   │   ├── examples/                   # 使用示例
│   │   └── tests/                      # E2E 测试
│   └── map-partition-sdk/              # SDK helper crate
│       ├── src/
│       │   ├── processor.rs            # PartitionProcessor trait
│       │   ├── ipc.rs                  # Arrow IPC 编解码
│       │   └── export.rs               # export_partition_processor! 宏
│       └── examples/
│           └── identity_processor/     # 示例 .so (透传处理器)
```

## 六层扩展管线

本项目遵循 Ballista 官方扩展模式，实现六层管线：

| 层次 | 文件 | 职责 |
|------|------|------|
| 1. Proto | `extension.proto` | 定义 `LMapPartition` / `PMapPartition` 消息 |
| 2. Logical Node | `logical/map_partition.rs` | `UserDefinedLogicalNodeCore` 实现 |
| 3. DataFrame Ext | `dataframe/map_partition.rs` | `DataFrameExt::map_partition()` API |
| 4. Physical Node | `physical/map_partition_exec.rs` | `ExecutionPlan` 实现，含五阶段 `.so` 调用 |
| 5. Extension Planner | `planner/extension_planner.rs` | `QueryPlannerWithExtensions` 逻辑→物理转换 |
| 6. Codec | `codec/extension.rs` | `ExtendedBallistaLogicalCodec` / `ExtendedBallistaPhysicalCodec` |

### 编解码器装饰器模式

```
ExtendedBallistaLogicalCodec ──包裹──▶ BallistaLogicalExtensionCodec
ExtendedBallistaPhysicalCodec ──包裹──▶ BallistaPhysicalExtensionCodec
```

- 编码时：识别 `MapPartition` / `MapPartitionExec` 则用自定义 protobuf 序列化，否则委托给内部 codec
- 解码时：识别自定义消息则反序列化，否则委托给内部 codec
- Physical codec 对未知节点编码为 `PMessage::Opaque(bytes)` 实现透传回退

## .so 五阶段流式生命周期

这是本项目的核心设计。每个 `.so` 需暴露 5 个 C ABI 函数，框架按序调用：

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│  _init   │───▶│  _feed   │───▶│ _execute │───▶│  _fetch  │───▶│ _finish  │
│ (一次)    │    │ (多次)    │    │ (一次)    │    │ (多次)    │    │ (一次)    │
└──────────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘
     │               │               │               │               │
  传入 schema    传入 batch      执行业务逻辑    取出 batch       释放资源
  返回 ctx       (流式输入)                     (流式输出)
```

| 阶段 | 函数签名 | 说明 |
|------|----------|------|
| init | `fn(schema_ptr, schema_len) -> *mut c_void` | 接收输入 schema，返回处理器上下文指针 |
| feed | `fn(ctx, input_ptr, input_len) -> i32` | 流式输入，每批调用一次，0=成功，负数=错误 |
| execute | `fn(ctx) -> i32` | 所有输入完成后执行业务逻辑 |
| fetch | `fn(ctx, output_ptr, output_len) -> i32` | 流式输出，0=还有数据，1=结束，负数=错误 |
| finish | `fn(ctx) -> i32` | 释放处理器资源 |

**流式设计的目的**：`feed` 逐批输入、`fetch` 逐批输出，框架同一时刻只持有一个 batch 的内存，最小化框架资源占用。

### C ABI 接口详细说明

```c
// 初始化：传入 Arrow Schema 的 IPC 二进制，返回处理器上下文
void* <fn_name>_init(const uint8_t* schema_ptr, int64_t schema_len);

// 流式输入：传入 RecordBatch 的 IPC 二进制
int32_t <fn_name>_feed(void* ctx, const uint8_t* input_ptr, int64_t input_len);

// 执行：所有输入完成后调用
int32_t <fn_name>_execute(void* ctx);

// 流式输出：框架分配 output_ptr/output_len，.so 需用 malloc 分配内存
// 框架消费完后会调用 libc::free() 释放
int32_t <fn_name>_fetch(void* ctx, uint8_t** output_ptr, int64_t* output_len);

// 结束：释放处理器上下文
int32_t <fn_name>_finish(void* ctx);
```

## Schema 处理策略

- **输入 schema**：框架自动从上游算子获取，通过 `_init` 传入 IPC 编码的 schema
- **输出 schema**：用户在 API 层显式传入 `output_schema: SchemaRef`，序列化到 protobuf 中随计划传输

```rust
// DataFrame API — 用户显式提供输出 schema
df.map_partition("/path/to/lib.so", "my_processor", Arc::new(output_schema))?
```

## SDK helper crate

用户无需手动处理 Arrow IPC 二进制编解码和 C ABI 函数导出。`map-partition-sdk` crate 提供了：

### PartitionProcessor trait

```rust
pub trait PartitionProcessor: Send + Sized + 'static {
    fn new(schema: SchemaRef) -> Self;      // 对应 _init
    fn feed(&mut self, batch: RecordBatch); // 对应 _feed
    fn execute(&mut self);                  // 对应 _execute
    fn fetch(&mut self) -> Option<RecordBatch>; // 对应 _fetch
}
```

- `_finish` 由 SDK 自动实现（drop 处理器）
- 用户只需实现 trait，无需关心底层 ABI

### export_partition_processor! 宏

```rust
struct MyProcessor { /* ... */ }
impl PartitionProcessor for MyProcessor { /* ... */ }

// 一行导出 5 个 C ABI 函数
export_partition_processor!(MyProcessor, my_processor);
```

生成的函数：`my_processor_init`, `my_processor_feed`, `my_processor_execute`, `my_processor_fetch`, `my_processor_finish`

### IPC 帮助函数

| 函数 | 说明 |
|------|------|
| `decode_schema(bytes)` | IPC bytes → SchemaRef |
| `decode_batch(bytes)` | IPC bytes → RecordBatch |
| `encode_schema(schema)` | SchemaRef → IPC bytes |
| `encode_batch(batch)` | RecordBatch → IPC bytes |

## 使用方式

### 1. 编写处理器 .so

```rust
// Cargo.toml
// [lib] crate-type = ["cdylib"]
// [dependencies] map-partition-sdk = { path = "..." }, arrow = "54", paste = "1"

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use map_partition_sdk::{PartitionProcessor, export_partition_processor};

struct MyProcessor {
    batches: Vec<RecordBatch>,
    index: usize,
}

impl PartitionProcessor for MyProcessor {
    fn new(_schema: SchemaRef) -> Self {
        Self { batches: Vec::new(), index: 0 }
    }

    fn feed(&mut self, batch: RecordBatch) {
        self.batches.push(batch);
    }

    fn execute(&mut self) {
        // 自定义业务逻辑
    }

    fn fetch(&mut self) -> Option<RecordBatch> {
        if self.index < self.batches.len() {
            let batch = self.batches[self.index].clone();
            self.index += 1;
            Some(batch)
        } else {
            None
        }
    }
}

export_partition_processor!(MyProcessor, my_processor);
```

```bash
cargo build --release
# 产出：libmy_processor.so
```

### 2. DataFusion 单机使用

```rust
use std::sync::Arc;
use ballista_map_partition::{
    dataframe::map_partition::DataFrameExt,
    planner::extension_planner::QueryPlannerWithExtensions,
};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;

let query_planner = Arc::new(QueryPlannerWithExtensions::default());
let state = SessionStateBuilder::new()
    .with_query_planner(query_planner)
    .with_default_features()
    .build();
let ctx = SessionContext::new_with_state(state);

let df = ctx.sql("SELECT * FROM my_table").await?;
let output_schema = df.schema().as_arrow().clone();
let df = df.map_partition(
    "/path/to/libmy_processor.so",
    "my_processor",
    Arc::new(output_schema),
)?;
df.show().await?;
```

### 3. Ballista 分布式使用

**Client**：注册 codecs 到 SessionConfig

```rust
use ballista_map_partition::codec::extension::{
    ExtendedBallistaLogicalCodec, ExtendedBallistaPhysicalCodec,
};
use ballista_core::serde::BallistaCodec;

let logical_codec = Arc::new(ExtendedBallistaLogicalCodec::default());
let physical_codec = Arc::new(ExtendedBallistaPhysicalCodec::default());
let codec = BallistaCodec::new(logical_codec, physical_codec);

let config = BallistaConfig::builder()
    .setting("ballista.codec.logical", "extension")
    .setting("ballista.codec.physical", "extension")
    .build()?;
```

**Scheduler**：注册 codecs + QueryPlanner

```rust
// SessionBuilder 中注册 QueryPlannerWithExtensions
// 并配置与 Client 相同的 codecs
```

**Executor**：注册 codecs

```rust
// 配置与 Client 相同的 codecs
// .so 文件需预先部署到 Executor 节点的对应路径
```

## 版本兼容

| 依赖 | 版本 |
|------|------|
| DataFusion | 52 |
| Ballista | 52 |
| Arrow | 54 |
| Rust Edition | 2024 |

## 运行测试

```bash
# 单元测试 & plan round-trip 测试
cargo test -p ballista-map-partition

# 构建示例 .so
cd crates/map-partition-sdk/examples/identity_processor
cargo build --release

# 运行 DataFusion 单机示例
MAP_PARTITION_SO=/path/to/libidentity_processor.so \
  cargo run --example datafusion
```
