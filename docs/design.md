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
