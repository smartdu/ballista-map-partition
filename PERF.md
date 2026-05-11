# 性能压测报告

## 测试环境

| 组件 | 配置 |
|------|------|
| 框架 | Ballista 52 + DataFusion 52 |
| FFI 方式 | Arrow C Data Interface (`to_ffi` / `from_ffi_and_data_type`) |
| .so 处理器 | `libregion_cluster_processor.so` (arrow 54) |
| 机器 | 单机 (所有 Executor / MinIO 共宿) |

## 测试数据集

| 参数 | 值 |
|------|-----|
| Region 数 | 50 |
| 每 Region channelid 数 | 100 |
| 每 channelid 轨迹数 | 1,000 |
| 每条轨迹 JSON 大小 | 4,096 字节 |
| 总行数 | 5,000,000 |
| 分区数 | 50 (按 region 哈希) |

**输入 Schema**：`(region, channelid, captime, recordid, json)`
**输出 Schema**：`(region, dossierid, recordids, json1, json2, json3, json4)`

## 测试方式

一键脚本，自动完成环境清理、MinIO 部署、Scheduler/Executor 启动、运行时监控和结果汇总：

```bash
# 1 Executor × 8 并发 + 1 MinIO
./scripts/bench.sh -e 1 -c 8

# 2 Executor × 4 并发 + 2 MinIO 集群
./scripts/bench.sh -e 2 -c 4
```

脚本自动采集：冷启动/峰值/回落 RSS、计算耗时、吞吐量、正确性。原始数据保存到 `/tmp/bench_<时间戳>/`。

## 测试结果

### 1. IPC vs C Data Interface

| 指标 | IPC (旧) | C Data Interface (新) | 提升 |
|------|---------|---------------------|------|
| 计算耗时 | 14.33s | 10.45s | **-27%** |
| 吞吐量 | 348,954 rec/s | 478,316 rec/s | **+37%** |
| Executor 峰值 RSS | ~4,900 MB | ~5,167 MB | — |
| Executor finish 后 RSS | ~892 MB | ~879 MB | — |

> 峰值相近是因为 processor 全量缓存 4KB JSON 占主导（~3.2 GB），框架开销已压缩至极限。

### 2. 多 Executor 扩展性

Executor 与 MinIO 1:1 配比。

| 配置 | MinIO | 耗时 | 吞吐量 | Executor 峰值 | 回落 |
|------|-------|------|--------|-------------|------|
| 1e × 8c | 1 节点 | 10.45s | 478K rec/s | 5,167 MB (单台) | 879 MB |
| 2e × 4c | 2 节点集群 | **8.94s** | **559K rec/s** | 2,155 / 2,489 MB | 213 MB |

2 Executor 方案更快（+17% 吞吐量），且单台 Executor 峰值不到一半。MinIO 分布式集群 I/O 分散 + 跨 Executor 并行均起效。

### 资源使用 (1e×8c)

| 进程 | 冷启动 | 峰值 | 回落 |
|------|--------|------|------|
| Executor | 20 MB | 5,167 MB | 879 MB |
| Scheduler | 19 MB | 63 MB | 42 MB |
| MinIO | 118 MB | 325 MB | 233 MB |

### 多轮稳定性 (3 轮连续，不重启 Scheduler/Executor)

| 指标 | Round 1 | Round 2 | Round 3 |
|------|---------|---------|---------|
| 峰值 RSS | 4,980 MB | 4,178 MB | ~3,995 MB |
| 轮间最低 RSS | 20 MB | 739 MB | 710 MB |
| 计算耗时 | 23.43s | 12.52s | 12.81s |

- 峰值递减，无内存泄漏
- 轮间回到同一基线，资源完全释放
- Round 1 偏高为冷启动（page cache / arena 预热）

## 问题分析：Executor 峰值的根因

### 内存归因 (1e×8c, 5M 行 × 4KB JSON)

```
⚫ Processor 缓存 JSON String   ≈ 3.2 GB  (74%)
   每行 4KB → Rust String × 8 并发 × 100K 行

⚫ Processor 其他缓存           ≈ 0.06 GB
   recordid / channelid / HashMap / Vec

⚫ 框架 + Ballista + 进程       ≈ 1.9 GB  (26%)
   Arrow Flight 流缓冲、Ballista 内部、gRPC、tokio
─────────────────────────────────────────
  合计                         ≈ 5.2 GB  (100%)
```

> 峰值由 processor 全量缓存 JSON 主导，非框架泄漏。`to_string_array().clone()` 是 Arc 引用计数，不产生新内存。`-c` 直接控制并发 partition 数，即控制峰值。

### RSS 时间线

| 阶段 | RSS | 说明 |
|------|-----|------|
| 冷启动 | 20 MB | 进程基准 |
| feed | 200~740 MB | 流式输入，批次处理 |
| execute 峰值 | 5,167 MB | Linux 惰性缺页，触及全部缓存页 |
| fetch | — | 输出释放 |
| finish 回落 | 879 MB | processor drop, malloc_trim 已移除 |

### 降低峰值方向

| 方案 | 预期 | 代价 |
|------|------|------|
| `-c 4` | 峰值 ~2.6 GB | 吞吐量下降 |
| `-e 2 -c 4` | 单台峰值 ~2.5 GB | 多 executor 管理 |
| 增加分区数 | 单 partition 数据更少 | 小分区调度开销 |
| processor 按需提取 | 大幅降低缓存 | 改业务逻辑 |
