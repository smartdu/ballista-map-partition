# 设计文档

## S3 + MapPartition 分布式计算集成设计

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
| MapPartition | `override_session_builder` (Scheduler) | `QueryPlannerWithExtensions` |

关键挑战在于 Scheduler 的 `override_session_builder`：S3 的 `session_state_with_s3_support` 和 MapPartition 的 `QueryPlannerWithExtensions` 都需要注入到 session builder，但一个配置只能设一个 builder。

### Scheduler：combined_session_builder

通过 `SessionStateBuilder::new_from_existing()` 将两个扩展的能力组合：

```rust
fn combined_session_builder(config: SessionConfig) -> Result<SessionState> {
    // 第一步：用 S3 helper 创建带 S3 支持的 SessionState
    let state = session_state_with_s3_support(config)?;
    // 第二步：在已有 state 基础上叠加 QueryPlannerWithExtensions
    let query_planner = Arc::new(QueryPlannerWithExtensions::default());
    SessionStateBuilder::new_from_existing(state)
        .with_query_planner(query_planner)
        .build()
}
```

### Executor

Executor 不需要 QueryPlanner，S3 和 MapPartition 的注入点互不冲突，直接组合即可：

```rust
ExecutorProcessConfig {
    override_logical_codec: Some(Arc::new(ExtendedBallistaLogicalCodec::default())),
    override_physical_codec: Some(Arc::new(ExtendedBallistaPhysicalCodec::default())),
    override_config_producer: Some(Arc::new(session_config_with_s3_support)),
    override_runtime_producer: Some(Arc::new(runtime_env_with_s3_support)),
    ..Default::default()
}
```

### Client

Client 通过 `state_with_s3_support()` 获取 S3 会话状态，再叠加 Ballista codec 配置：

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
| `state_with_s3_support` | 便捷函数，等于 `session_state_with_s3_support(session_config_with_s3_support())` |

底层实现基于 `object_store::aws::AmazonS3Builder`，通过自定义 `ObjectStoreRegistry` 在运行时根据 SQL `SET` 语句动态创建 S3 连接。
