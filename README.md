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
│  注册 LogicalCodec + PhysicalCodec + EnforceDistributeBy        │
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
├── data/
│   └── region_face_capture/            # 测试数据（Parquet 格式，含 region 字段）
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
│   │   ├── examples/                   # 使用示例（见下文）
│   │   └── tests/                      # E2E 测试
│   └── map-partition-sdk/              # SDK helper crate
│       ├── src/
│       │   ├── processor.rs            # PartitionProcessor trait
│       │   ├── ipc.rs                  # Arrow IPC 编解码
│       │   └── export.rs               # export_partition_processor! 宏
│       └── examples/
│           └── region_cluster_processor/ # .so 处理器：按 channelid 聚类
│   └── ballista-monitor/               # 内置监控系统
│       └── src/
│           ├── lib.rs                  # 模块导出
│           ├── registry.rs             # 全局 MetricsRegistry 单例
│           ├── collector.rs            # 系统指标采集（1s 间隔）
│           ├── metrics.rs              # 指标数据结构
│           ├── store.rs                # RingBuffer 时序存储
│           ├── server.rs               # axum HTTP 服务
│           └── dashboard.rs            # 嵌入式 Web Dashboard
```

## 七层扩展管线

本项目遵循 Ballista 官方扩展模式，实现七层管线：

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
| 级别2：executor 内跨分区 | `concurrent_tasks`（默认=CPU核数） | `-c` / `--concurrent-tasks` 启动参数 |
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

## 内置监控系统

项目内置了实时监控系统（`ballista-monitor` crate），可通过 Web 页面监控 Scheduler 和 Executor 的运行状态。

### 编译

监控功能通过 `monitoring` feature flag 控制：

```bash
# 构建时启用监控
cargo build --release --examples --features monitoring
```

### 启动

Scheduler 和 Executor 各自新增 `--monitor-port` 参数：

```bash
# Scheduler（默认监控端口 8080）
cargo run --release --example distributed_compute_scheduler --features monitoring -- --monitor-port 8080

# Executor（默认监控端口 8081）
cargo run --release --example distributed_compute_executor --features monitoring -- --monitor-port 8081
```

启动后日志中会打印 `Monitor server listening on http://0.0.0.0:8080`。

### Web Dashboard

浏览器访问 `http://<executor-host>:8081` 打开监控页面。

**Overview 页面**：展示所有节点的概览卡片，包括 CPU、Memory、Disk、Uptime 等指标。Executor 额外显示 SO Processors（活跃/历史总数）和 Concurrent（并发槽位数）。

**节点详情页**：点击 Overview 卡片或顶部 Tab 进入，展示：
- **CPU / Memory 时序图表**
- **Top 10 Slowest Processors**：跨所有 Job 的耗时排行榜，按 Stages Total 降序排列
- **Processors 分组列表**：按 Job ID 分组，可折叠/展开，显示 SO 文件名、函数名、Partition、Key、Stage、创建/结束时间、I/O 统计
- **Processor 详情弹窗**：点击行弹出，显示：
  - Job ID、SO 文件名、函数名、Partition、Key、Stage
  - 创建时间、结束时间
  - Lifecycle（挂钟总耗时）、Stages Total（各阶段 CPU 耗时之和）、Wait/Overhead（等待开销）
  - Stage Breakdown 彩色条形图（init / feed / execute / fetch / finish 各阶段耗时占比）
  - 各阶段明细表（调用次数、总耗时、平均耗时、占比）
  - I/O 统计（输入/输出的行数和字节数）

**多节点聚合**：点击右上角齿轮按钮配置多个节点的 URL，页面自动轮询所有节点数据，统一展示。

### 监控架构

```
┌──────────────────────────────────────────────────────┐
│                   Browser (Dashboard)                 │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐     │
│  │  Overview   │  │ Scheduler  │  │ Executor   │     │
│  │  (全部节点) │  │  Detail    │  │  Detail    │     │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘     │
│        │               │               │             │
│        └───────────────┼───────────────┘             │
│                        │ 5s 轮询 /api/*              │
└────────────────────────┼─────────────────────────────┘
                         │
           ┌─────────────┼─────────────┐
           ▼             ▼             ▼
    ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
    │ Scheduler   │ │ Executor 1  │ │ Executor 2  │
    │ :8080       │ │ :8081       │ │ :8082       │
    │ (axum +     │ │ (axum +     │ │ (axum +     │
    │  CORS)      │ │  CORS)      │ │  CORS)      │
    └─────────────┘ └─────────────┘ └─────────────┘
```

- 每个进程内嵌 axum HTTP 服务，提供 REST API
- 启用 CORS 允许浏览器跨节点拉取数据
- 系统指标（CPU/Memory/Disk/Network）每秒采样，保留 5 分钟时序数据
- .so Processor 生命周期（init → feed → execute → fetch → finish）全程追踪，含各阶段耗时

### API 端点

| 端点 | 说明 |
|------|------|
| `GET /` | Dashboard HTML 页面 |
| `GET /api/overview` | 节点概览（角色、指标、processor 统计） |
| `GET /api/metrics` | 所有指标最新值 |
| `GET /api/metrics/{name}/history?since=unix_ms` | 指标时序数据 |
| `GET /api/processors` | 所有 processor 详情（含 stage_durations） |

## 示例与测试

### 示例总览

| 类别 | 示例 | 说明 |
|------|------|------|
| **集群组件** | `distributed_compute_scheduler` | Scheduler：S3 + MapPartition codec + EnforceDistributeBy |
| | `distributed_compute_executor` | Executor：S3 + MapPartition codec，支持命令行参数 |
| **客户端** | `region_cluster_client` | DistributeBy + region_cluster_processor 功能验证 |
| | `bench_region_cluster_client` | 10W 条数据 + 1K Region 并发性能基准测试 |

### .so 处理器

| 处理器 | 说明 |
|--------|------|
| `region_cluster_processor` | 按 channelid 聚类生成 dossier，检测 CROSS_REGION_ERROR |

---

### 1. 构建

```bash
# 构建 .so 处理器
cargo build --release -p region_cluster_processor
# 产出：target/release/libregion_cluster_processor.so

# 构建所有示例
cargo build --release --examples
```

---

### 2. Region 聚类功能验证（region_cluster_client）

演示 **DistributeBy 分区 + map_partition** 的端到端流程：

- **输入**：`(region, channelid, captime, recordid)`
- **输出**：`(region, dossierid, recordids)` — 按 channelid 聚类
- **分区验证**：处理器内部检测是否出现混合 region，若检测到则输出 `CROSS_REGION_ERROR`

**启动步骤**：

```bash
# 1. 启动 MinIO
docker run --rm -d -p 9000:9000 -p 9001:9001 \
  --name minio \
  -e "MINIO_ACCESS_KEY=MINIO" -e "MINIO_SECRET_KEY=MINIOSECRET" \
  quay.io/minio/minio server /data --console-address ":9001"

# 2. 上传测试数据到 S3
pip install minio
python3 -c "
from minio import Minio
client = Minio('localhost:9000', access_key='MINIO', secret_key='MINIOSECRET', secure=False)
if not client.bucket_exists('ballista'):
    client.make_bucket('ballista')
client.fput_object('ballista', 'region_face_capture/region_face_capture.parquet', 'data/region_face_capture/region_face_capture.parquet')
print('Uploaded to s3://ballista/region_face_capture/')
"

# 3. 启动 Scheduler（新终端）
cargo run --release --example distributed_compute_scheduler

# 4. 启动 Executor（新终端）
cargo run --release --example distributed_compute_executor

# 5. 运行 Client
MAP_PARTITION_SO=target/release/libregion_cluster_processor.so \
  cargo run --release --example region_cluster_client
```

预期输出（无 `CROSS_REGION_ERROR` 表示 DistributeBy 分区正确）：

```
--- Output: region cluster result ---
+--------+---------------+---------------+
| region | dossierid     | recordids     |
+--------+---------------+---------------+
| east   | dossier_ch001 | rec001,rec002 |
| east   | dossier_ch002 | rec003        |
| west   | dossier_ch003 | rec004,rec005 |
| north  | dossier_ch004 | rec006        |
+--------+---------------+---------------+
```

---

### 3. 并发性能基准测试（bench_region_cluster_client）

验证 DistributeBy 三级并行模型的并发性能。

**数据集参数**：100,000 条记录 / 1,000 个 Region / 每 Region 100 个 channelid / 1,000 个分区。

**启动步骤**：

```bash
# 1. 启动 MinIO / Scheduler（如已启动可复用）

# 2. 启动 Executor（支持命令行参数指定端口和并发数）

#    单 Executor（8 并发）：
cargo run --release --example distributed_compute_executor -- -p 50051 --bind-grpc-port 50052 -c 8

#    多 Executor（各用不同端口）：
cargo run --release --example distributed_compute_executor -- -p 50051 --bind-grpc-port 50052 -c 4 &
cargo run --release --example distributed_compute_executor -- -p 50053 --bind-grpc-port 50054 -c 4 &

# 3. 运行基准测试（自动生成数据并上传 S3）
MAP_PARTITION_SO=target/release/libregion_cluster_processor.so \
  cargo run --release --example bench_region_cluster_client
```

**Executor 命令行参数**：

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-p` / `--port` | Arrow Flight 服务端口 | 50051 |
| `--bind-grpc-port` | gRPC 服务端口 | 50052 |
| `-c` / `--concurrent-tasks` | 并发任务数 | CPU 核数 |

**基准测试结果对比**：

| 配置 | 总并发数 | 计算耗时 | 吞吐量 | 加速比 |
|------|---------|---------|--------|-------|
| 1 Executor × 8 concurrent | 8 | 4.72s | 21,164/s | 1x |
| 2 Executor × 4 concurrent | 8 | 2.82s | 35,445/s | **1.7x** |

同样 8 个并发任务，2 Executor 比 1 Executor 快 1.7 倍，验证了跨 Executor 并行（级别1）的扩展能力。

---

### 清理

```bash
docker stop minio
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
# 单元测试
cargo test -p ballista-map-partition

# E2E 分布式测试（需要 MinIO + Scheduler + Executor）
MAP_PARTITION_SO=target/release/libregion_cluster_processor.so \
  cargo test -p ballista-map-partition --test e2e
```
