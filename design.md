# 设计文档

## 1. S3 + MapPartition 分布式计算集成设计

### 目标

将 Ballista 的 S3 对象存储扩展与 map_partition 算子扩展合并到同一组 Scheduler/Executor/Client 示例中，实现从 S3 读取数据并经 `.so` 处理器分布式处理的能力。

### 设计思路

S3 扩展和 MapPartition 扩展分别通过 Ballista 的 `override_*` 配置钩子注入：

| 扩展能力 | 注入点 | 使用的 helper 函数 |
|----------|--------|-------------------|
| S3 | `override_config_producer` | `session_config_with_s3_support` |
| S3 | `override_runtime_producer` (Executor) | `runtime_env_with_s3_support` |
| S3 | `override_session_builder` (Scheduler) | `session_state_with_s3_support` |
| MapPartition | `override_logical_codec` | `ExtendedBallistaLogicalCodec` |
| MapPartition | `override_physical_codec` | `ExtendedBallistaPhysicalCodec` |
| MapPartition | `override_session_builder` (Scheduler) | `QueryPlannerWithExtensions` + `EnforceDistributeBy` |

关键挑战在于 Scheduler 的 `override_session_builder`：S3 的 `session_state_with_s3_support` 和 MapPartition 的 `QueryPlannerWithExtensions` + `EnforceDistributeBy` 都需要注入到 session builder，但一个配置只能设一个 builder。

### Scheduler：combined_session_builder

通过 `SessionStateBuilder::new_from_existing()` 将多个扩展的能力组合：

```rust
fn combined_session_builder(config: SessionConfig) -> Result<SessionState> {
    // 第一步：用 S3 helper 创建带 S3 支持的 SessionState
    let state = session_state_with_s3_support(config)?;
    // 第二步：在已有 state 基础上叠加 QueryPlannerWithExtensions + EnforceDistributeBy
    let query_planner = Arc::new(QueryPlannerWithExtensions::default());
    Ok(SessionStateBuilder::new_from_existing(state)
        .with_query_planner(query_planner)
        .with_physical_optimizer_rule(Arc::new(EnforceDistributeBy))
        .build())
}
```

### Executor

Executor 不需要 QueryPlanner 和 PhysicalOptimizerRule，S3 和 MapPartition 的注入点互不冲突，直接组合即可。支持通过命令行参数指定端口和并发数：

```rust
ExecutorProcessConfig {
    port: 50051,               // Arrow Flight 端口，-p 指定
    grpc_port: 50052,          // gRPC 端口，--bind-grpc-port 指定
    concurrent_tasks: 8,       // 并发任务数，-c 指定
    override_logical_codec: Some(Arc::new(ExtendedBallistaLogicalCodec::default())),
    override_physical_codec: Some(Arc::new(ExtendedBallistaPhysicalCodec::default())),
    override_config_producer: Some(Arc::new(session_config_with_s3_support)),
    override_runtime_producer: Some(Arc::new(runtime_env_with_s3_support)),
    ..Default::default()
}
```

多 Executor 启动示例：

```bash
# Executor #1
cargo run --release --example distributed_compute_executor -- -p 50051 --bind-grpc-port 50052 -c 4

# Executor #2
cargo run --release --example distributed_compute_executor -- -p 50053 --bind-grpc-port 50054 -c 4
```

### Client

Client 通过 `session_state_with_s3_support()` 获取 S3 会话状态，再叠加 Ballista codec 配置：

```rust
let state = state_with_s3_support()?;
let config = state.config().clone()
    .with_ballista_logical_extension_codec(Arc::new(ExtendedBallistaLogicalCodec::default()))
    .with_ballista_physical_extension_codec(Arc::new(ExtendedBallistaPhysicalCodec::default()));
let state = SessionStateBuilder::new_from_existing(state)
    .with_config(config)
    .build();
```

S3 的访问参数通过 SQL `SET` 语句在运行时配置：

```sql
SET s3.allow_http = true;
SET s3.access_key_id = 'MINIO';
SET s3.secret_access_key = 'MINIOSECRET';
SET s3.endpoint = 'http://localhost:9000';
```

### 依赖来源

S3 相关的 helper 函数全部来自 `ballista_core::object_store` 模块，无需额外引入 `object_store` crate：

| 函数 | 作用 |
|------|------|
| `session_config_with_s3_support` | 创建带 S3 扩展配置选项的 SessionConfig |
| `runtime_env_with_s3_support` | 创建带 S3 对象存储注册的 RuntimeEnv |
| `session_state_with_s3_support` | 创建带 S3 支持的完整 SessionState |

底层实现基于 `object_store::aws::AmazonS3Builder`，通过自定义 `ObjectStoreRegistry` 在运行时根据 SQL `SET` 语句动态创建 S3 连接。

---

## 2. DistributeBy 分区语义设计

### 目标

对外只暴露 `with_distribute_by(expr, num_partitions)` API，语义：**相同值进入同一个 processor，不同值进入不同 processor**。

### 为什么需要自定义方案

DataFusion/Ballista 没有原生的 DistributeBy 物理分区支持：

- DataFusion 52 的 `Partitioning` 枚举只有 `RoundRobinBatch`、`Hash`、`UnknownPartitioning`，没有 `DistributeBy` 变体
- DataFusion 53.1.0 同样没有
- Ballista 最新版本是 52.0.0，没有 53.x 版本
- DataFusion 的 `EnforceDistribution` 优化器对小数据集（n_rows <= batch_size）不插入 RepartitionExec，导致分区不生效

### 三层保障架构

| 层 | 机制 | 作用 |
|---|---|---|
| **1. 强制 RepartitionExec** | 自定义 PhysicalOptimizerRule `EnforceDistributeBy` | 在 scheduler 端物理优化阶段，强制在 MapPartitionExec 前插入 RepartitionExec，确保多分区并行 |
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

数据流：

```
with_distribute_by(col("region"), 100)  →  RepartitionExec: Hash([region], 100)
                                                          ↓
                    级别1: executor-A 运行分区[0..49]，executor-B 运行分区[50..99]（并行）
                                                          ↓
                    级别2: executor-A 的 concurrent_tasks=8，同时跑 8 个分区（并行）
                                                          ↓
                    级别3: 分区5 内 region=["east","west"]（hash碰撞），2个processor串行执行
```

### 内部 grouping 实现细节

当 `distribute_by` 设置时，`MapPartitionExec.execute()` 的流程：

1. **Phase 1**：dlopen .so（不变）
2. **Phase 2**：维护 `HashMap<ScalarValue, GroupProcessor>` + `key_order: Vec<ScalarValue>`
3. **Phase 3 (_feed)**：对每个输入 batch，按 distribute_by 列值拆分为子 batch，路由到对应 processor
4. **Phase 4 (_execute)**：对 HashMap 中所有 processor 串行调用 _execute
5. **Phase 5 (_fetch)**：按 key 排序，依次从每个 processor fetch 输出
6. **Phase 6 (_finish)**：对 HashMap 中所有 processor 串行调用 _finish

`split_batch_by_key()` 函数使用 `ScalarValue::try_from_array()` 提取每行的 key 值，通过 `filter_record_batch` 生成子 batch。

### EnforceDistributeBy 优化规则

自定义 `PhysicalOptimizerRule`，在物理计划中查找 `MapPartitionExec`（distribute_by 不为空），若其子节点不满足 Hash 分区要求，则强制插入 `RepartitionExec`：

```rust
plan.transform_up(&|node: Arc<dyn ExecutionPlan>| {
    if let Some(exec) = node.as_any().downcast_ref::<MapPartitionExec>() {
        if exec.distribute_by.is_some() {
            let child = node.children()[0].clone();
            if !is_satisfied(&child, exec) {
                let partitioning = Partitioning::Hash(hash_exprs, exec.num_partitions);
                let repartition = RepartitionExec::try_new(child, partitioning)?;
                let new_exec = MapPartitionExec::new(/* ... repartition as input ... */);
                return Ok(Transformed::yes(new_exec));
            }
        }
    }
    Ok(Transformed::no(node))
})
```

**为什么不用 DataFusion 内置的 EnforceDistribution**：它在两个条件下不插入 RepartitionExec：
1. 输入只有 1 个分区时 `multi_partitions=false`
2. 小数据集时 `n_rows <= batch_size`（默认 8192）

### num_partitions 如何选择

`num_partitions` 应设置为 **>= distribute_by 列的不同值数量**。

- 设 `>= 不同值数`：每个分区大概率只有 1 个值，1 个 processor 实例
- 设 `< 不同值数`：必然有 pigeonhole 碰撞，同分区多个 processor，并行度浪费
- 内部 grouping 兜底保正确性，但不应依赖它做主要分发

---

## 3. Arrow C Data Interface（零拷贝 FFI）

### 为什么替换 IPC

旧版方案使用 Arrow IPC Stream 格式在框架↔`.so` 之间序列化/反序列化 RecordBatch：

```
框架 RecordBatch → IPC bytes → .so 接收 bytes → IPC 解码 → RecordBatch
```

每个 batch 在 FFI 边界产生 **3 份内存拷贝**（原始 Arrow 数据 + IPC 字节 + 解码后的数据），导致：
- Executor 峰值内存膨胀约 20-35%
- CPU 消耗在序列化/反序列化上

### C Data Interface 方案

使用 Arrow 标准的 C Data Interface（`FFI_ArrowArray`）替代 IPC 字节流，通过 `Arc` 引用计数实现零拷贝传递：

```
框架 RecordBatch → to_ffi() → FFI_ArrowArray → .so from_ffi_and_data_type() → RecordBatch
                                    ↕
                          指针传递，Arc 引用计数，无数据拷贝
```

数据实际在共享的 Arrow Buffer 中，`Buffer::clone()` 只增加 `Arc` 引用计数，不复制数据。

### _feed 方向：框架→.so

```
┌─────────────────────────────────────┐    ┌──────────────────────────────┐
│ 框架 (arrow 57)                      │    │ .so SDK (arrow 54)           │
│                                      │    │                              │
│ RecordBatch                          │    │ FFI_ArrowArray::from_raw(ptr)│
│   → StructArray::from(batch)         │    │   // 取走所有权，*ptr ← empty│
│   → to_ffi(&data) → FFI_ArrowArray   │    │ from_ffi_and_data_type(arr)  │
│   → Box::new(ffi_array)              │    │   → ArrayData                │
│   → Box::into_raw() → *mut ptr       │    │   → StructArray → RecordBatch│
│               ↓                      │    │                              │
│        feed_func(ctx, ptr) ──────────┼───→│ processor.feed(batch)        │
│                                      │    │                              │
│ // *ptr 已被 SDK 替换为 empty()       │    │                              │
│ Box::from_raw(ptr) → drop (安全)     │    │                              │
└─────────────────────────────────────┘    └──────────────────────────────┘
```

1. 框架用 `StructArray::from(batch)` 将 RecordBatch 重新包装为 StructArray（零拷贝，只移 ownership）
2. `to_ffi(&data)` 创建 FFI_ArrowArray，内部 `private_data` 持有 `Buffer::clone()`（Arc 自增）
3. `Box::new(ffi_array)` 将 FFI_ArrowArray 移到堆上，`Box::into_raw()` 取得裸指针
4. SDK 的 `_feed` 调用 `FFI_ArrowArray::from_raw(ptr)` → `std::ptr::replace(ptr, empty())` 取走数据
5. 框架的 `ptr` 指向空结构（`release: None`），回收 Box 后安全 drop

### _fetch 方向：.so→框架

```
┌─────────────────────────────────────┐    ┌──────────────────────────────┐
│ 框架 (arrow 57)                      │    │ .so SDK (arrow 54)           │
│                                      │    │                              │
│ let mut arr = FFI_ArrowArray::empty()│    │ processor.fetch()            │
│               ↓                      │    │   → Some(batch)              │
│  fetch_func(ctx, &mut arr) ──────────┼───→│ to_ffi(&data) → FFI_ArrowArray│
│                                      │    │ ptr::write(array_ptr, arr)   │
│ // &mut arr 已被 SDK 填充             │    │ // 写入框架预分配的槽位        │
│                                      │    │                              │
│ from_ffi_and_data_type(arr, type)    │    │                              │
│   → ArrayData → RecordBatch          │    │                              │
└─────────────────────────────────────┘    └──────────────────────────────┘
```

1. 框架在栈上创建 `FFI_ArrowArray::empty()`（`release: None`）
2. SDK 的 `_fetch` 调用 `to_ffi()` 导出数据，`ptr::write(ptr, ffi_array)` 写入框架的栈槽位
3. 框架调用 `from_ffi_and_data_type(arr, data_type)` 导入 ArrayData → RecordBatch

### 跨版本 Arrow 兼容

`.so` 处理器使用 arrow 54（SDK），框架使用 arrow 57（DataFusion 52）。`FFI_ArrowArray` 是 `#[repr(C)]` 结构体，两个版本的字段布局完全相同，ABI 安全。

Release 回调是函数指针——消费者只负责调用生产者设置的函数指针，与版本无关。

### 效果

| 指标 | IPC (旧) | C Data Interface (新) |
|------|---------|---------------------|
| 框架↔.so 数据传输 | IPC 序列化字节 | `Arc` 引用计数指针 |
| 每 batch 内存额外开销 | ~2× batch 大小 (IPC 编码+解码) | 0（零拷贝） |
| Executor 内存峰值 | ~4.9 GB | ~4.3 GB |
| 计算耗时 (5M 行) | 14.33s | 11.18s |

---

## 4. SDK 架构

### PartitionProcessor trait

```rust
pub trait PartitionProcessor: Send + Sized + 'static {
    fn new(schema: SchemaRef) -> Self;
    fn schema(&self) -> &SchemaRef;  // 供 _feed 构造 DataType::Struct
    fn feed(&mut self, batch: RecordBatch);
    fn execute(&mut self);
    fn fetch(&mut self) -> Option<RecordBatch>;
    fn finish(&mut self) {}  // 默认空实现
}
```

`_finish` 由 SDK 自动实现（drop 处理器并释放 FFI 资源）；用户只需实现 trait。

### export_partition_processor! 宏

生成 5 个 `extern "C"` 函数：

| 函数 | 签名 | 数据格式 |
|------|------|---------|
| `_init` | `(schema_ptr, schema_len) -> *mut c_void` | Schema: IPC bytes |
| `_feed` | `(ctx, *mut FFI_ArrowArray) -> i32` | Batch: C Data Interface |
| `_execute` | `(ctx) -> i32` | — |
| `_fetch` | `(ctx, *mut FFI_ArrowArray) -> i32` | Batch: C Data Interface |
| `_finish` | `(ctx) -> i32` | — |

### Processor 设计约束

1. 不要在 `feed()` 中缓存整个 batch 的原始数据——只存储聚合所需的派生状态
2. 大字段（JSON、binary）如非必要，在 `feed()` 中提取关键字段后丢弃原始数据
3. `execute()` 做计算，`fetch()` 流式输出——不要在 `fetch()` 中返回单个巨大 batch
4. 跨版本编译：.so 用 arrow 54 编译，框架用 arrow 57，通过 C Data Interface 互操作
