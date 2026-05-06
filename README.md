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
│   │   │   ├── physical_optimizer/      # 物理优化规则 (EnforceDistributeBy)
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
│           ├── identity_processor/     # 示例 .so (透传处理器)
│           ├── face_cluster_processor/ # 示例 .so (人脸聚类处理器)
│           └── region_cluster_processor/ # 示例 .so (按 Region 分区+按 channelid 聚类处理器)
```

## 六层扩展管线

本项目遵循 Ballista 官方扩展模式，实现六层管线：

| 层次 | 文件 | 职责 |
|------|------|------|
| 1. Proto | `extension.proto` | 定义 `LMapPartition` / `PMapPartition` 消息 |
| 2. Logical Node | `logical/map_partition.rs` | `UserDefinedLogicalNodeCore` 实现 |
| 3. DataFrame Ext | `dataframe/map_partition.rs` | `DataFrameExt::map_partition()` + `with_distribute_by()` API |
| 4. Physical Node | `physical/map_partition_exec.rs` | `ExecutionPlan` 实现，含五阶段 `.so` 调用 + 内部 grouping |
| 5. Extension Planner | `planner/extension_planner.rs` | `QueryPlannerWithExtensions` 逻辑→物理转换 |
| 6. Codec | `codec/extension.rs` | `ExtendedBallistaLogicalCodec` / `ExtendedBallistaPhysicalCodec` |
| 7. Physical Optimizer | `physical_optimizer/enforce_distribute_by.rs` | `EnforceDistributeBy` 自定义优化规则，强制插入 RepartitionExec |

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

## DistributeBy 分区语义

### API

```rust
df.map_partition(&so_path, "processor", output_schema)?
  .with_distribute_by(col("region"), 100)?   // 按 region 分区，100 个分区
  .build()?;
```

`with_distribute_by(expr, num_partitions)` 的语义：**相同值进入同一个 processor，不同值进入不同 processor**。

- `expr`：按哪个列分区
- `num_partitions`：分区数（应 >= 该列的不同值数量，确保每个值大概率独占一个分区）

### 三层保障

| 层 | 机制 | 作用 |
|---|---|---|
| **1. 强制 RepartitionExec** | `EnforceDistributeBy` 自定义 PhysicalOptimizerRule | 在 scheduler 端物理优化阶段，强制在 MapPartitionExec 前插入 RepartitionExec，确保多分区并行 |
| **2. 内部 grouping** | MapPartitionExec execute() 内部按 key 分组 | 兜底——同一分区内 hash 碰撞的数据仍按值隔离，保证 100% 正确性 |
| **3. required_input_distribution** | 声明 HashPartitioned 需求 | 辅助优化器做正确决策 |

### 三级并行模型

```
级别 1：跨 executor 并行
  └─ Ballista 调度器把 N 个分区分配到多个 executor，各 executor 同时运行

级别 2：executor 内跨分区并行
  └─ 每个 executor 的线程池（concurrent_tasks）并行执行分配到的多个分区

级别 3：分区内串行
  └─ 若同分区有多个不同值（hash 碰撞），内部多个 processor 串行执行
```

| 级别 | 控制参数 | 设置方式 |
|------|---------|---------|
| 级别1：跨 executor | executor 数量 | 启动多个 executor 进程 |
| 级别2：executor 内跨分区 | `concurrent_tasks`（默认=CPU核数） | `--concurrent-tasks` 启动参数 |
| 级别3：分区内串行 | 无需配置 | 代码逻辑保证 |

### Scheduler 配置

Scheduler 需注册 `EnforceDistributeBy` 优化规则：

```rust
use ballista_map_partition::physical_optimizer::EnforceDistributeBy;

fn combined_session_builder(config: SessionConfig) -> Result<SessionState> {
    let state = session_state_with_s3_support(config)?;
    let query_planner = Arc::new(QueryPlannerWithExtensions::default());
    Ok(SessionStateBuilder::new_from_existing(state)
        .with_query_planner(query_planner)
        .with_physical_optimizer_rule(Arc::new(EnforceDistributeBy))
        .build())
}
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

### 4. S3 + MapPartition 分布式计算

项目提供了集成 **S3 对象存储** 和 **map_partition 算子** 的分布式计算示例，可从 S3 读取数据并经 `.so` 处理器处理后输出。

| 示例文件 | 说明 |
|----------|------|
| `distributed_compute_scheduler.rs` | Scheduler：S3 会话配置 + MapPartition 编解码器 + QueryPlanner |
| `distributed_compute_executor.rs` | Executor：S3 运行时 + MapPartition 编解码器 |
| `distributed_compute_client.rs` | Client：S3 数据源 + MapPartition 算子 |

**启动步骤**：

1. 启动 MinIO（本地 S3 兼容存储）：
```bash
docker run --rm -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ACCESS_KEY=MINIO" -e "MINIO_SECRET_KEY=MINIOSECRET" \
  quay.io/minio/minio server /data --console-address ":9001"
```

2. 启动 Scheduler：
```bash
cargo run --release --example distributed_compute_scheduler
```

3. 启动 Executor：
```bash
cargo run --release --example distributed_compute_executor
```

4. 运行 Client：
```bash
MAP_PARTITION_SO=/path/to/libidentity_processor.so \
  cargo run --release --example distributed_compute_client
```

**集成要点**：

- Scheduler 通过 `combined_session_builder` 同时注入 S3 会话状态和 `QueryPlannerWithExtensions`
- Executor 通过 `session_config_with_s3_support` + `runtime_env_with_s3_support` 获得 S3 访问能力
- Client 通过 `state_with_s3_support()` 创建会话，再叠加 MapPartition 编解码器，使用 `SET` 语句配置 S3 参数

详细设计文档参见 [docs/design.md](docs/design.md)。

### 5. 人脸聚类示例（S3 + MapPartition 分布式计算）

项目提供了一个人脸抓拍聚类端到端示例，演示 **输入输出 Schema 不同** 的场景：

- **输入**：人脸抓拍数据 `(channelid, captime, recordid)`
- **输出**：档案聚类结果 `(dossierid, clusterids)`，按 channelid 分组合并 recordid

| 文件 | 说明 |
|------|------|
| `face_cluster_processor/` | `.so` 处理器：按 channelid 分组，生成 dossierid → clusterids 映射 |
| `region_cluster_processor/` | `.so` 处理器：按随机值聚类，检测跨 region 分区错误 |
| `face_cluster_client.rs` | 分布式客户端：从 S3 读取人脸抓拍数据，经 map_partition 输出聚类结果并写回 S3 |
| `region_cluster_client.rs` | 分布式客户端：按 region Hash repartition 后聚类，验证不同 region 走不同分区 |
| `data/face_capture/` | 测试数据（Parquet 格式） |
| `data/region_face_capture/` | 测试数据（Parquet 格式，含 region 字段） |

**运行步骤**：

1. 构建 face_cluster_processor `.so`：

```bash
cargo build --release -p face_cluster_processor
```

2. 启动 MinIO（如已启动可跳过）：

```bash
docker run --rm -d -p 9000:9000 -p 9001:9001 \
  --name minio \
  -e "MINIO_ACCESS_KEY=MINIO" -e "MINIO_SECRET_KEY=MINIOSECRET" \
  quay.io/minio/minio server /data --console-address ":9001"
```

3. 上传人脸抓拍测试数据到 S3：

```bash
pip install minio
python3 -c "
from minio import Minio
client = Minio('localhost:9000', access_key='MINIO', secret_key='MINIOSECRET', secure=False)
if not client.bucket_exists('ballista'):
    client.make_bucket('ballista')
client.fput_object('ballista', 'face_capture/face_capture.parquet', 'data/face_capture/face_capture.parquet')
print('Uploaded to s3://ballista/face_capture/')
"
```

4. 启动 Scheduler（新终端）：

```bash
cargo run --release --example distributed_compute_scheduler
```

5. 启动 Executor（新终端）：

```bash
cargo run --release --example distributed_compute_executor
```

6. 运行人脸聚类客户端：

```bash
MAP_PARTITION_SO=target/release/libface_cluster_processor.so \
  cargo run --release --example face_cluster_client
```

预期输出：

```
--- Input: face capture data ---
+-----------+---------------------+----------+
| channelid | captime             | recordid |
+-----------+---------------------+----------+
| ch001     | 2026-01-01 08:00:00 | rec001   |
| ch001     | 2026-01-01 08:05:00 | rec002   |
| ch001     | 2026-01-01 09:00:00 | rec003   |
| ch002     | 2026-01-01 08:10:00 | rec004   |
| ch002     | 2026-01-01 08:30:00 | rec005   |
| ch003     | 2026-01-01 10:00:00 | rec006   |
+-----------+---------------------+----------+
--- Output: dossier clustering ---
+---------------+----------------------+
| dossierid     | clusterids           |
+---------------+----------------------+
| dossier_ch001 | rec001,rec002,rec003 |
| dossier_ch002 | rec004,rec005        |
| dossier_ch003 | rec006               |
+---------------+----------------------+
--- Results written to s3://ballista/face_cluster_result/ ---
```**要点**：

- 输出 Schema 由客户端定义（`dossierid + clusterids`），与输入 Schema 不同
- `face_cluster_processor` 使用 `arrow::compute::cast` 处理 DataFusion 的 `Utf8View` 列类型
- Scheduler 和 Executor 复用 `distributed_compute_scheduler` / `distributed_compute_executor`

## 版本兼容

| 依赖 | 版本 |
|------|------|
| DataFusion | 52 |
| Ballista | 52 |
| Arrow | 54 |
| Rust Edition | 2024 |

## 运行测试

### 单元测试 & 单机示例

```bash
# 单元测试 & plan round-trip 测试
cargo test -p ballista-map-partition

# 构建示例 .so
cd crates/map-partition-sdk/examples/identity_processor
cargo build --release

# 运行 DataFusion 单机示例
MAP_PARTITION_SO=/path/to/libidentity_processor.so \
  cargo run --release --example datafusion
```

### 分布式计算测试（S3 + MapPartition）

前置条件：Docker、MinIO 镜像。

#### 透传处理器（identity_processor）

**1. 构建 .so 处理器**

```bash
cargo build --release -p identity_processor
# 产出：target/release/libidentity_processor.so
```

**2. 启动 MinIO**

```bash
docker run --rm -d -p 9000:9000 -p 9001:9001 \
  --name minio \
  -e "MINIO_ACCESS_KEY=MINIO" -e "MINIO_SECRET_KEY=MINIOSECRET" \
  quay.io/minio/minio server /data --console-address ":9001"
```

**3. 上传测试数据到 S3**

```bash
pip install minio
python3 -c "
from minio import Minio
client = Minio('localhost:9000', access_key='MINIO', secret_key='MINIOSECRET', secure=False)
if not client.bucket_exists('ballista'):
    client.make_bucket('ballista')
client.fput_object('ballista', 'data/test.parquet', 'data/test.parquet')
print('Uploaded data/test.parquet to s3://ballista/data/')
"
```

**4. 启动 Scheduler**（新终端）

```bash
cargo run --release --example distributed_compute_scheduler
```

**5. 启动 Executor**（新终端）

```bash
cargo run --release --example distributed_compute_executor
```

**6. 运行 Client**

```bash
MAP_PARTITION_SO=target/release/libidentity_processor.so \
  cargo run --release --example distributed_compute_client
```

预期输出：

```
+---+---+
| a | b |
+---+---+
| 1 | x |
| 2 | y |
| 3 | z |
+---+---+
```

#### 人脸聚类处理器（face_cluster_processor）

**1. 构建 .so 处理器**

```bash
cargo build --release -p face_cluster_processor
# 产出：target/release/libface_cluster_processor.so
```

**2. 上传人脸抓拍测试数据到 S3**（MinIO 已启动）

```bash
python3 -c "
from minio import Minio
client = Minio('localhost:9000', access_key='MINIO', secret_key='MINIOSECRET', secure=False)
client.fput_object('ballista', 'face_capture/face_capture.parquet', 'data/face_capture/face_capture.parquet')
print('Uploaded to s3://ballista/face_capture/')
"
```

**3. 启动 Scheduler / Executor**（如已启动可复用）

**4. 运行人脸聚类 Client**

```bash
MAP_PARTITION_SO=target/release/libface_cluster_processor.so \
  cargo run --release --example face_cluster_client
```

聚类结果会写回 `s3://ballista/face_cluster_result/`，可通过 MinIO 控制台（http://localhost:9001）查看。

聚类结果会写回 `s3://ballista/face_cluster_result/`，可通过 MinIO 控制台（http://localhost:9001）查看。

#### Region 聚类处理器（region_cluster_processor）

演示 **DistributeBy 分区 + map_partition** 的场景：先按 `region` 字段做 DistributeBy 分区，确保相同 region 进入同一个 processor，再在分区内按相同 channelid 进行聚类。

- **输入**：含 region 的人脸抓拍数据 `(region, channelid, captime, recordid)`
- **输出**：聚类结果 `(region, dossierid, recordids)`，按 channelid 聚类
- **分区验证**：`.so` 处理器内部检测是否出现混合 region，若检测到则输出 `CROSS_REGION_ERROR`

**1. 构建 .so 处理器**

```bash
cargo build --release -p region_cluster_processor
# 产出：target/release/libregion_cluster_processor.so
```

**2. 上传含 region 的测试数据到 S3**（MinIO 已启动）

```bash
python3 -c "
from minio import Minio
client = Minio('localhost:9000', access_key='MINIO', secret_key='MINIOSECRET', secure=False)
client.fput_object('ballista', 'region_face_capture/region_face_capture.parquet', 'data/region_face_capture/region_face_capture.parquet')
print('Uploaded to s3://ballista/region_face_capture/')
"
```

**3. 启动 Scheduler / Executor**（如已启动可复用）

**4. 运行 Region 聚类 Client**

```bash
MAP_PARTITION_SO=target/release/libregion_cluster_processor.so \
  cargo run --release --example region_cluster_client
```

预期输出（无 `CROSS_REGION_ERROR` 表示分区正确，相同 channelid 的记录聚类到同一 dossier）：

```
--- Input: region face capture data ---
+--------+-----------+---------------------+----------+
| region | channelid | captime             | recordid |
+--------+-----------+---------------------+----------+
| east   | ch001     | 2026-01-01 08:00:00 | rec001   |
| east   | ch001     | 2026-01-01 08:05:00 | rec002   |
| east   | ch002     | 2026-01-01 08:10:00 | rec003   |
| west   | ch003     | 2026-01-01 09:00:00 | rec004   |
| west   | ch003     | 2026-01-01 09:30:00 | rec005   |
| north  | ch004     | 2026-01-01 10:00:00 | rec006   |
+--------+-----------+---------------------+----------+
--- Output: region cluster result ---
+--------+---------------+---------------+
| region | dossierid     | recordids     |
+--------+---------------+---------------+
| west   | dossier_ch003 | rec004,rec005 |
| east   | dossier_ch001 | rec001,rec002 |
| east   | dossier_ch002 | rec003        |
| north  | dossier_ch004 | rec006        |
+--------+---------------+---------------+
--- Results written to s3://ballista/region_cluster_result/ ---
```

**要点**：

- 使用 `with_distribute_by(col("region"), 100)` 声明按 region 做 DistributeBy 分区
- 自定义 `EnforceDistributeBy` 优化规则在 Scheduler 端强制插入 `RepartitionExec`，确保小数据集也能正确分区
- `region_cluster_processor` 在分区内按 channelid 聚类：相同 channelid 的记录归入同一个 dossier
- 若输出中出现 `CROSS_REGION_ERROR` 行，说明 DistributeBy 分区未生效

**5. 清理**

```bash
docker stop minio
```
