# 性能压测报告

## 测试环境

| 组件 | 配置 |
|------|------|
| 框架 | Ballista 52 + DataFusion 52 |
| FFI 方式 | Arrow C Data Interface (`to_ffi` / `from_ffi_and_data_type`) |
| .so 处理器 | `libregion_cluster_processor.so` (arrow 54) |
| 部署 | 单机，所有 Executor / MinIO 共宿同一物理机 |

## 数据集

| 参数 | 值 |
|------|-----|
| Region 数 | 50 |
| 每 Region channelid 数 | 100 |
| 每 channelid 轨迹数 | 1,000 |
| 每条轨迹 JSON | 4,096 字节 |
| 总行数 | 5,000,000 |
| 分区数 | 50 |

**输入**：`(region, channelid, captime, recordid, json)`
**输出**：`(region, dossierid, recordids, json1-4)`

## 一键测试

```bash
./scripts/bench.sh -e 1 -c 8   # 1 Executor × 8 并发 + 1 MinIO
./scripts/bench.sh -e 2 -c 4   # 2 Executor × 4 并发 + 2 MinIO 集群
```

脚本自动完成环境清理、MinIO 部署、Scheduler/Executor 启动校验、运行时 RSS/CPU 监控和结果汇总。原始数据保存到 `/tmp/bench_<timestamp>/`。

## 结果

### IPC vs C Data Interface

| 指标 | IPC 序列化 | C Data Interface | 提升 |
|------|-----------|-----------------|------|
| 计算耗时 | 14.33s | 10.45s | **-27%** |
| 吞吐量 | 348,954 rec/s | 478,316 rec/s | **+37%** |

均为 1 Executor × 8 并发。

### 多 Executor 扩展性

| 配置 | MinIO | 耗时 | 吞吐量 | Exec 峰值 (单台) | 回落 |
|------|-------|------|--------|-----------------|------|
| 1e × 8c | 1 节点 | 10.45s | 478K rec/s | 5,167 MB | 879 MB |
| 2e × 4c | 2 节点集群 | **8.94s** | **559K rec/s** | 2,155 / 2,489 MB | 213 MB |

2 Executor 吞吐量更高（+17%），且单台峰值不到一半。MinIO 分布式集群分担 I/O 有效。

### 资源使用 (1e × 8c)

| 进程 | 冷启动 | 峰值 | 回落 |
|------|--------|------|------|
| Executor | 20 MB | 5,167 MB | 879 MB |
| Scheduler | 19 MB | 63 MB | 42 MB |
| MinIO | 118 MB | 325 MB | 233 MB |

### 多轮稳定性

Scheduler + Executor 不重启，连续 3 轮 bench，每轮 5M 行。

| 指标 | Round 1 | Round 2 | Round 3 |
|------|---------|---------|---------|
| 峰值 RSS | 4,980 MB | 4,178 MB | ~3,995 MB |
| 轮间回落 | — | 739 MB | 710 MB |
| 耗时 | 23.43s | 12.52s | 12.81s |

- **峰值递减**：排除泄漏。Round 1 偏高为冷启动（page cache / glibc arena 预热）
- **轮间回到同一基线**：finish 后资源完全释放
- **3 轮累计处理 1,500 万行**，RSS 不涨

## 内存分析

5,167 MB 峰值主要由 processor 全量缓存 JSON 贡献，非框架泄漏。

```
⚫ Processor 缓存 JSON String   ≈ 3.2 GB  (74%)
   每行 4KB → Rust String × 8 并发 partition
⚫ Processor 其他结构           ≈ 0.06 GB
   HashMap/Vec/recordid/channelid
⚫ 框架 + Ballista + 进程       ≈ 1.9 GB  (26%)
   Arrow Flight 流缓冲、Ballista 内部、gRPC、tokio
─────────────────────────────────────────
  合计                         ≈ 5.2 GB
```

> `to_string_array().clone()` 是 `Arc<ArrayData>` 引用计数 +1，不产生额外内存。真正的拷贝在 `.to_string()` 创建 Rust String 时发生。

### RSS 时间线

| 阶段 | RSS | 说明 |
|------|-----|------|
| 冷启动 | 20 MB | 进程基准 |
| feed | 200 ~ 740 MB | 流式输入，逐批处理 |
| execute 峰值 | **5,167 MB** | 惰性缺页：触及全部缓存页 |
| finish 回落 | 879 MB | processor drop，框架稳态 |

### 降低峰值

| 方案 | 效果 | 代价 |
|------|------|------|
| `-c 4` | 峰值 ~2.6 GB | 吞吐量按比例下降 |
| `-e 2 -c 4` | 单台 ~2.5 GB，总吞吐更高 | 多 executor 管理 |
| processor 按需提取 JSON 字段 | 大幅降低缓存 | 需改业务逻辑 |
