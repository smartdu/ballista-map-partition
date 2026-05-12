# 设计文档

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

## 七层扩展管线

遵循 Ballista 官方扩展模式：

| 层次 | 文件 | 职责 |
|------|------|------|
| 1. Proto | `extension.proto` | `LMapPartition` / `PMapPartition` 消息定义 |
| 2. Logical Node | `logical/map_partition.rs` | `UserDefinedLogicalNodeCore` 实现 |
| 3. DataFrame Ext | `dataframe/map_partition.rs` | `DataFrameExt::map_partition()` + `with_distribute_by()` |
| 4. Physical Node | `physical/map_partition_exec.rs` | `ExecutionPlan`，含五阶段 .so 调用 + 内部 grouping |
| 5. Extension Planner | `planner/extension_planner.rs` | `QueryPlannerWithExtensions` 逻辑→物理转换 |
| 6. Codec | `codec/extension.rs` | 自定义 protobuf 序列化/反序列化 |
| 7. Physical Optimizer | `physical_optimizer/enforce_distribute_by.rs` | `EnforceDistributeBy` 强制插入 RepartitionExec |

### 编解码器装饰器模式

```
ExtendedBallistaLogicalCodec ──包裹──▶ BallistaLogicalExtensionCodec
ExtendedBallistaPhysicalCodec ──包裹──▶ BallistaPhysicalExtensionCodec
```

- 编码：识别 `MapPartition` / `MapPartitionExec` → 自定义 protobuf；否则委托内部 codec
- 解码：识别自定义消息 → 反序列化；否则委托内部 codec
- Physical codec 对未知节点编码为 `PMessage::Opaque(bytes)` 实现透传回退

## Schema 处理策略

- **输入 schema**：框架自动从上游算子获取，通过 `_init` 传入 IPC 编码的 schema
- **输出 schema**：用户在 API 层显式传入 `output_schema: SchemaRef`，序列化到 protobuf 随计划传输

```rust
df.map_partition("/path/to/lib.so", "my_processor", Arc::new(output_schema))?
```

## .so 五阶段流式生命周期

每个 `.so` 需暴露 5 个 C ABI 函数，框架按序调用：

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│  _init   │───▶│  _feed   │───▶│ _execute │───▶│  _fetch  │───▶│ _finish  │
│ (一次)    │    │ (多次)    │    │ (一次)    │    │ (多次)    │    │ (一次)    │
└──────────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘
     │               │               │               │               │
  传入 schema      传入 batch      执行业务逻辑    取出 batch       释放资源
  + partition_id   (流式输入)                     (流式输出)
  返回 ctx
```

| 阶段 | 签名 | 说明 |
|------|------|------|
| init | `fn(schema_ptr, len, partition_id) -> *mut c_void` | Schema: IPC 编码，传入分区 ID |
| feed | `fn(ctx, *mut FFI_ArrowArray) -> i32` | Batch: C Data Interface 零拷贝 |
| execute | `fn(ctx) -> i32` | 所有输入完成后调用 |
| fetch | `fn(ctx, *mut FFI_ArrowArray) -> i32` | 0=还有数据, 1=结束 |
| finish | `fn(ctx) -> i32` | 释放处理器资源 |

### C ABI 接口

```c
// 初始化：Schema 通过 IPC 二进制传入，partition_id 标识当前分区
void* <fn>_init(const uint8_t* schema_ptr, int64_t schema_len, int64_t partition_id);

// 流式输入：框架通过 to_ffi 导出，SDK 通过 from_ffi_and_data_type 导入
int32_t <fn>_feed(void* ctx, FFI_ArrowArray* array);

// 执行
int32_t <fn>_execute(void* ctx);

// 流式输出：框架预分配 empty()，SDK 通过 to_ffi 填充
int32_t <fn>_fetch(void* ctx, FFI_ArrowArray* array);

// 结束
int32_t <fn>_finish(void* ctx);
```

---

## 1. S3 + MapPartition 分布式计算集成

### 扩展注入点

S3 和 MapPartition 分别通过 Ballista 的 `override_*` 配置钩子注入：

| 扩展能力 | 注入点 | 使用的 helper |
|----------|--------|--------------|
| S3 | `override_config_producer` | `session_config_with_s3_support` |
| S3 | `override_runtime_producer` (Executor) | `runtime_env_with_s3_support` |
| S3 | `override_session_builder` (Scheduler) | `session_state_with_s3_support` |
| MapPartition | `override_logical_codec` | `ExtendedBallistaLogicalCodec` |
| MapPartition | `override_physical_codec` | `ExtendedBallistaPhysicalCodec` |
| MapPartition | `override_session_builder` (Scheduler) | `QueryPlannerWithExtensions` + `EnforceDistributeBy` |

核心挑战：Scheduler 的 `override_session_builder` 只能设一个 builder，但 S3 和 MapPartition 都需要注入。

### Scheduler：combined_session_builder

通过 `SessionStateBuilder::new_from_existing()` 组合多个扩展：

```rust
fn combined_session_builder(config: SessionConfig) -> Result<SessionState> {
    let state = session_state_with_s3_support(config)?;
    let query_planner = Arc::new(QueryPlannerWithExtensions::default());
    Ok(SessionStateBuilder::new_from_existing(state)
        .with_query_planner(query_planner)
        .with_physical_optimizer_rule(Arc::new(EnforceDistributeBy))
        .build())
}
```

### Executor

Executor 不需要 QueryPlanner 和 PhysicalOptimizerRule，直接组合即可：

```rust
ExecutorProcessConfig {
    port: 50051,                                    // Arrow Flight 端口，-p 指定
    grpc_port: 50052,                               // gRPC 端口，--bind-grpc-port 指定
    concurrent_tasks: 8,                            // 并发任务数，-c 指定
    override_logical_codec: Some(Arc::new(ExtendedBallistaLogicalCodec::default())),
    override_physical_codec: Some(Arc::new(ExtendedBallistaPhysicalCodec::default())),
    override_config_producer: Some(Arc::new(session_config_with_s3_support)),
    override_runtime_producer: Some(Arc::new(runtime_env_with_s3_support)),
    ..Default::default()
}
```

### Client

```rust
let state = state_with_s3_support()?;
let config = state.config().clone()
    .with_ballista_logical_extension_codec(Arc::new(ExtendedBallistaLogicalCodec::default()))
    .with_ballista_physical_extension_codec(Arc::new(ExtendedBallistaPhysicalCodec::default()));
let state = SessionStateBuilder::new_from_existing(state)
    .with_config(config)
    .build();
```

S3 参数通过 SQL `SET` 运行时配置：

```sql
SET s3.allow_http = true;
SET s3.access_key_id = 'MINIO';
SET s3.secret_access_key = 'MINIOSECRET';
SET s3.endpoint = 'http://localhost:9000';
```

---

## 2. DistributeBy 分区语义

### API

```rust
df.map_partition(&so_path, "processor", output_schema)?
  .with_distribute_by(col("region"), 100)?   // 相同 region → 同一 processor
  .build()?;
```

语义：**相同值进入同一个 processor，不同值进入不同 processor**。

### 为什么需要自定义方案

- DataFusion 52/53 的 `Partitioning` 枚举没有 `DistributeBy` 变体
- Ballista 最新版基于 DataFusion 52，没有 53.x
- DataFusion 内置 `EnforceDistribution` 对小数据集不插入 RepartitionExec

### 三层保障

| 层 | 机制 | 作用 |
|---|---|---|
| **1. 强制 RepartitionExec** | `EnforceDistributeBy` PhysicalOptimizerRule | 在物理优化阶段强制插入 RepartitionExec，确保多分区并行 |
| **2. 内部 grouping** | `split_batch_by_key()` 按 key 分组 | 兜底——同分区 hash 碰撞仍按值隔离，保证 100% 正确性 |
| **3. required_input_distribution** | 声明 HashPartitioned 需求 | 辅助优化器决策 |

### 三级并行模型

```
级别 1：跨 executor 并行  → Ballista 把 N 个分区分配到多个 executor
级别 2：executor 内并行   → concurrent_tasks 同时跑多个分区
级别 3：分区内串行        → 同分区多值（hash 碰撞）时串行执行
```

数据流示例：

```
with_distribute_by(col("region"), 100) → RepartitionExec: Hash([region], 100)
                                                       ↓
               级别1: executor-A 跑分区[0..49]，executor-B 跑分区[50..99]
                                                       ↓
               级别2: executor-A concurrent_tasks=8，同时跑 8 个分区
                                                       ↓
               级别3: 分区5 内 region=["east","west"]（碰撞），串行执行
```

### 内部 grouping 实现

DistributeBy 模式下 `MapPartitionExec.execute()` 流程：

1. dlopen .so
2. 维护 `HashMap<ScalarValue, GroupProcessor>` + `key_order`
3. **_feed**：`split_batch_by_key()` 按列值拆分子 batch → 路由到对应 processor
4. **_execute**：所有 processor 串行执行
5. **_fetch**：按 key 顺序，依次从每个 processor 取输出
6. **_finish**：所有 processor 串行释放

### EnforceDistributeBy 规则

```rust
plan.transform_up(&|node| {
    if let Some(exec) = node.as_any().downcast_ref::<MapPartitionExec>() {
        if exec.distribute_by.is_some() {
            let child = node.children()[0].clone();
            if !is_satisfied(&child, exec) {
                let repartition = RepartitionExec::try_new(
                    child,
                    Partitioning::Hash(hash_exprs, exec.num_partitions),
                )?;
                return Ok(Transformed::yes(MapPartitionExec::new(/* repartition */)));
            }
        }
    }
    Ok(Transformed::no(node))
})
```

### num_partitions 选择

- `>= 不同值数`：每个分区大概率 1 个值，1 个 processor（理想）
- `< 不同值数`：pigeonhole 碰撞，并行度浪费
- 内部 grouping 兜底保正确性，但不应依赖它做主要分发

---

## 3. Arrow C Data Interface（零拷贝 FFI）

### 问题

旧版使用 Arrow IPC Stream 在框架↔.so 之间序列化/反序列化 RecordBatch：

```
框架 RecordBatch → IPC bytes → .so 接收 bytes → IPC 解码 → RecordBatch
```

每个 batch 产生 **3 份内存拷贝**（原始数据 + IPC 字节 + 解码数据），Executor 峰值膨胀约 20-35%。

### 方案

用 Arrow C Data Interface (`FFI_ArrowArray`) 替代 IPC 字节流，通过 `Arc` 引用计数零拷贝传递：

```
框架 RecordBatch → to_ffi() → FFI_ArrowArray → .so from_ffi_and_data_type() → RecordBatch
                                    ↕
                     指针传递，Buffer::clone() = Arc 自增，无数据拷贝
```

### _feed 方向（框架→.so）

```
┌───────────────────────────────────┐    ┌──────────────────────────────┐
│ 框架 (arrow 57)                    │    │ .so SDK (arrow 54)           │
│                                    │    │                              │
│ RecordBatch                        │    │ FFI_ArrowArray::from_raw(ptr)│
│ → StructArray::from(batch)         │    │   // *ptr ← empty，取走所有权  │
│ → to_ffi(&data) → FFI_ArrowArray   │    │ from_ffi_and_data_type(arr)  │
│ → Box::new → Box::into_raw → *mut  │    │ → ArrayData → RecordBatch    │
│             ↓                      │    │                              │
│      feed_func(ctx, ptr) ──────────┼───→│ processor.feed(batch)        │
│                                    │    │                              │
│ Box::from_raw(ptr) → drop（安全）   │    │                              │
└───────────────────────────────────┘    └──────────────────────────────┘
```

### _fetch 方向（.so→框架）

```
┌───────────────────────────────────┐    ┌──────────────────────────────┐
│ 框架 (arrow 57)                    │    │ .so SDK (arrow 54)           │
│                                    │    │                              │
│ let mut arr = empty()              │    │ processor.fetch()            │
│             ↓                      │    │ → Some(batch)                │
│  fetch_func(ctx, &mut arr) ────────┼───→│ to_ffi(&data) → FFI_ArrowArray│
│                                    │    │ ptr::write(array_ptr, arr)   │
│                                    │    │                              │
│ from_ffi_and_data_type(arr, type)  │    │                              │
│ → ArrayData → RecordBatch          │    │                              │
└───────────────────────────────────┘    └──────────────────────────────┘
```

### 跨版本 Arrow 兼容

.so (arrow 54) 和框架 (arrow 57) 之间通过 C Data Interface 互操作。`FFI_ArrowArray` 是 `#[repr(C)]` 结构体，两个版本字段布局完全相同。Release 回调是函数指针——消费者只调用生产者设置的指针。

**⚠ _fetch 方向的生命周期陷阱（核心教训）**：`_fetch` 返回的 `FFI_ArrowArray` 由 .so 的 `to_ffi` 创建，release 回调指向 .so 内的 `release_array` 函数。MapPartitionExec 的 async block 结束后 `lib` 被 drop → dlclose 卸载 .so。但输出 RecordBatch 还会流向下游算子（ShuffleWriter），在后续 drop 时才调用 release 回调——此时 .so 已卸载，函数指针悬空 → **SEGFAULT**。

**修复**：fetch 路径导入 RecordBatch 后立即 `deep_copy_batch()`，将数据拷入框架侧 allocator 管理的 Buffer，断开对 .so release 回调的依赖。输出数据量通常远小于输入，拷贝代价可接受。

**教训**：C Data Interface 的 release 回调是**生产者**设置的。_feed 方向生产者是框架（安全），_fetch 方向生产者是 .so（不安全）。凡是 .so 作为生产者的 FFI_ArrowArray，必须在 .so 卸载前完成数据拷贝。

### 效果

| 指标 | IPC (旧) | C Data Interface (新) |
|------|---------|---------------------|
| 数据传输 | IPC 序列化字节 | Arc 引用计数指针 |
| 每 batch 额外开销 | ~2× batch 大小 | 0 |
| Executor 峰值 (5M 行) | ~4.9 GB | ~4.3 GB |
| 计算耗时 (5M 行) | 14.33s | 11.18s |

---

## 4. SDK 架构

### PartitionProcessor trait

```rust
pub trait PartitionProcessor: Send + Sized + 'static {
    fn new(schema: SchemaRef, partition_id: usize) -> Self;
    fn schema(&self) -> &SchemaRef;              // 供 _feed 构造 DataType
    fn partition_id(&self) -> usize;             // 当前分区 ID
    fn feed(&mut self, batch: RecordBatch);       // 流式输入
    fn execute(&mut self);                        // 计算
    fn fetch(&mut self) -> Option<RecordBatch>;   // 流式输出
    fn finish(&mut self) {}                       // 可选 cleanup
}
```

### export_partition_processor! 宏

```rust
struct MyProcessor { /* ... */ }
impl PartitionProcessor for MyProcessor { /* ... */ }
export_partition_processor!(MyProcessor, my_processor);
// 生成: my_processor_init, _feed, _execute, _fetch, _finish
```

| 函数 | 数据格式 |
|------|---------|
| `_init` | Schema: IPC bytes + partition_id |
| `_feed` | Batch: C Data Interface (FFI_ArrowArray) |
| `_execute` | — |
| `_fetch` | Batch: C Data Interface (FFI_ArrowArray) |
| `_finish` | — |

### Processor 设计约束

1. 不要在 `feed()` 中缓存整个 batch 原始数据——只存储聚合所需的派生状态
2. 大字段（JSON、binary）在 `feed()` 中提取关键字段后丢弃原始数据
3. `execute()` 做计算，`fetch()` 流式输出——不要返回单个巨大 batch
4. .so 用 arrow 54 编译，框架用 arrow 57，通过 C Data Interface 互操作
