# 性能压测报告

## 测试环境

| 项目 | 配置 |
|------|------|
| OS | Linux 6.6.98 x86_64 |
| CPU | AMD EPYC 9K85 128-Core × 64 cores |
| Memory | 246 GiB |
| Rust | 1.93.0 |
| Docker | 27.5.1 |
| 框架 | Ballista 52 + DataFusion 52 — Arrow C Data Interface |
| .so 处理器 | `libregion_cluster_processor.so` (arrow 54) |
| 部署 | 单机，所有进程共宿 |

## 数据集

| 参数 | 值 |
|------|-----|
| Region 数 | 50 |
| 每 Region channelid 数 | 100 |
| 每 channelid 轨迹数 | 1,000 |
| 每条轨迹 JSON | 4,096 字节 |
| 总行数 | 5,000,000 |
| 分区数 | 50 |

> `(region, channelid, captime, recordid, json)` → `(region, dossierid, recordids, json1-4)`

## 一键测试

```bash
# 编译
cargo build --release -p region_cluster_processor
cargo build --release --examples

# 测试
./scripts/bench.sh -e 1 -c 8   # 1 Executor × 8 并发 + 1 MinIO
./scripts/bench.sh -e 2 -c 4   # 2 Executor × 4 并发 + 2 MinIO 集群
```

脚本自动清理环境、部署 MinIO、启动 Scheduler/Executor、校验、运行时 RSS 监控、结果汇总。

## 结果

### 1 Executor × 8 并发 + 1 MinIO

| 指标 | 值 |
|------|-----|
| 计算耗时 | 10.45s |
| 吞吐量 | 478,316 rec/s |
| 输出正确性 | 5,000 档案，无 CROSS_REGION_ERROR |

| 进程 | 冷启动 | 峰值 | 回落 |
|------|--------|------|------|
| Executor | 20 MB | 5,167 MB | 879 MB |
| Scheduler | 19 MB | 63 MB | 42 MB |
| MinIO | 118 MB | 325 MB | 233 MB |

### 2 Executor × 4 并发 + 2 MinIO 集群

| 指标 | 值 |
|------|-----|
| 计算耗时 | 8.94s |
| 吞吐量 | 559,412 rec/s |
| 输出正确性 | 5,000 档案，无 CROSS_REGION_ERROR |

| 进程 | 冷启动 | 峰值 | 回落 |
|------|--------|------|------|
| Executor #1 | 19 MB | 2,155 MB | 213 MB |
| Executor #2 | 20 MB | 2,489 MB | — |
| Scheduler | 19 MB | 62 MB | 39 MB |
| MinIO | 116 MB | 333 MB | 243 MB |

2 Executor 吞吐量更高（+17%），单台峰值不到单 Executor 的一半，MinIO 集群分担 I/O 有效。

## 内存分析 (1 Executor × 8 并发)

5,167 MB 峰值，归因如下：

```
⚫ Processor 缓存 JSON String   ≈ 3.2 GB  (62%)
   每行 4KB JSON → Rust String 全量存入 HashMap
   8 并发 × 每 partition 100K 行 × 4KB ≈ 3.2 GB

⚫ 框架 / Ballista / 进程       ≈ 2.0 GB  (38%)
   Arrow Flight 流缓冲、Ballista 内部状态、
   gRPC 连接、tokio runtime、OS 页表
─────────────────────────────────────────
  合计                         ≈ 5.2 GB
```

> `to_string_array().clone()` 是 `Arc<ArrayData>` 引用计数 +1，不产生新内存。拷贝发生在 `.to_string()` 创建 Rust String 时。

RSS 时间线：**冷启动 20 MB → feed 200~740 MB → execute 峰值 5,167 MB → finish 回落 879 MB**。execute 阶段 RSS 跳升是因为 Linux 惰性缺页——`feed` 时 `malloc` 只分配虚拟地址，`execute` 遍历 HashMap 才触发物理页映射。

### 降低峰值

| 方案 | 效果 | 代价 |
|------|------|------|
| `-c 4` | 峰值 ~2.6 GB | 吞吐量相应下降 |
| `-e 2 -c 4` | 单台 ~2.5 GB，总吞吐更高 | 多 executor 管理 |
| processor 按需提取 JSON | 大幅降低缓存 | 改业务逻辑 |
