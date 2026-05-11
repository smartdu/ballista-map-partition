# ballista-map-partition

基于 Apache Ballista 的分布式 `map_partition` 算子实现，对标 Spark 的 `mapPartition` 能力。

## 是什么

Spark 的 `mapPartition` 允许对每个分区执行自定义逻辑，但 Rust 生态中 DataFusion/Ballista 没有等价算子，且 Rust 不支持闭包的序列化/反序列化，无法直接将用户代码分发到集群。

本项目通过 **`.so` 动态库 + Arrow C Data Interface 零拷贝 + 五阶段流式生命周期** 解决了这个问题：

- 用户将分区处理逻辑编译为 `.so` 动态库
- 调用 `map_partition` 时传入 `.so` 路径和函数名前缀
- Executor 通过 `dlopen` 加载 `.so`，数据通过 Arrow C Data Interface 零拷贝传输
- Ballista 负责分布式调度，框架负责生命周期管理

详细设计见 [DESIGN.md](./DESIGN.md)，性能压测见 [PERF.md](./PERF.md)。

## 项目结构

```
ballista-map-partition/
├── Cargo.toml
├── crates/
│   ├── ballista-map-partition/    # Ballista 扩展 crate
│   │   ├── proto/                 # Protobuf 消息定义
│   │   ├── src/                   # 逻辑/物理算子、codec、planner、optimizer
│   │   ├── examples/              # Scheduler / Executor / Client 示例
│   │   └── tests/                 # E2E 测试
│   └── map-partition-sdk/         # .so 处理器开发 SDK
│       ├── src/                   # PartitionProcessor trait + C ABI 宏
│       └── examples/              # .so 处理器示例
└── data/                          # 测试数据
```

## 快速开始

### 构建

```bash
# 构建 .so 处理器
cargo build --release -p region_cluster_processor
cargo build --release -p noop_processor
# 产出：target/release/libregion_cluster_processor.so 和 libnoop_processor.so

# 构建所有示例
cargo build --release --examples
```

### 示例

| 类别 | 示例 | 说明 |
|------|------|------|
| **集群组件** | `distributed_compute_scheduler` | Scheduler（S3 + MapPartition codec + EnforceDistributeBy） |
| | `distributed_compute_executor` | Executor（S3 + MapPartition codec，`-p`/`-c` 命令行参数） |
| **客户端** | `region_cluster_client` | DistributeBy + region_cluster_processor 功能验证 |
| | `bench_region_cluster_client` | 并发性能基准测试（参数化 Region 数和 JSON 大小） |

### .so 处理器

| 处理器 | 说明 |
|--------|------|
| `region_cluster_processor` | 按 channelid 聚类生成 dossier，检测 CROSS_REGION_ERROR |
| `noop_processor` | 空处理器，丢弃所有输入不产生输出 — 用于压测框架开销 |

### DistributeBy API

```rust
let so_path = std::env::var("MAP_PARTITION_SO").unwrap_or_else(|_| default_so);
let fn_name = std::env::var("MAP_PARTITION_FN").unwrap_or_else(|_| default_fn);
df.map_partition(&so_path, &fn_name, output_schema)?
  .with_distribute_by(col("region"), 100)?   // 相同 region → 同一 processor
  .build()?;
```

环境变量：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `MAP_PARTITION_SO` | `target/release/libregion_cluster_processor.so` | `.so` 动态库路径 |
| `MAP_PARTITION_FN` | `region_cluster_processor` | `.so` 中的函数名前缀 |

### 运行示例：Region 聚类

```bash
# 1. 启动 MinIO
docker run --rm -d -p 9000:9000 -p 9001:9001 --name minio \
  -e "MINIO_ACCESS_KEY=MINIO" -e "MINIO_SECRET_KEY=MINIOSECRET" \
  quay.io/minio/minio server /data --console-address ":9001"

# 2. 上传测试数据
pip install minio
python3 -c "
from minio import Minio
client = Minio('localhost:9000', access_key='MINIO', secret_key='MINIOSECRET', secure=False)
if not client.bucket_exists('ballista'):
    client.make_bucket('ballista')
client.fput_object('ballista', 'region_face_capture/region_face_capture.parquet',
                   'data/region_face_capture/region_face_capture.parquet')
"

# 3. 启动 Scheduler（新终端）
cargo run --release --example distributed_compute_scheduler

# 4. 启动 Executor（新终端）
cargo run --release --example distributed_compute_executor

# 5. 运行 Client (可选 MAP_PARTITION_FN 指定函数名前缀)
MAP_PARTITION_SO=target/release/libregion_cluster_processor.so \
  cargo run --release --example region_cluster_client

# 使用 noop 处理器压测框架开销
MAP_PARTITION_FN=noop_processor \
MAP_PARTITION_SO=target/release/libnoop_processor.so \
  cargo run --release --example bench_region_cluster_client -- -r 1
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

### 性能基准测试

详见 [PERF.md](./PERF.md)。

### 清理

```bash
docker stop minio
```

## 版本兼容

| 依赖 | 版本 |
|------|------|
| DataFusion | 52 |
| Ballista | 52 |
| Arrow (SDK) | 54 |
| Arrow (Framework) | 57 |
| Rust Edition | 2024 |

## 运行测试

```bash
# 单元测试
cargo test -p ballista-map-partition

# E2E 分布式测试（需要 MinIO + Scheduler + Executor + .so）
MAP_PARTITION_SO=target/release/libregion_cluster_processor.so \
  cargo test -p ballista-map-partition --test e2e
```
