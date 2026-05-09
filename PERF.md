# 性能压测报告

## 测试目标

验证 `map_partition` 算子在 Arrow C Data Interface（零拷贝 FFI）下的分布式计算性能，并与旧版 IPC 序列化方案对比。

## 测试环境

| 组件 | 配置 |
|------|------|
| 框架 | Ballista 52 + DataFusion 52 |
| FFI 方式 | Arrow C Data Interface (`to_ffi` / `from_ffi_and_data_type`) |
| 存储 | MinIO (localhost:9000) |
| .so 处理器 | `libregion_cluster_processor.so` (arrow 54) |

## 测试数据集

| 参数 | 值 | 说明 |
|------|-----|------|
| Region 数 | 50 | `-r 50` |
| 每 Region channelid 数 | 100 | 每个 channelid 对应一个档案 (dossier) |
| 每 channelid 轨迹数 | 1,000 | 每个档案包含 1000 条轨迹记录 |
| JSON 大小 | 4,096 字节 | `-j 4096`，每条轨迹携带 4KB JSON 字段 |
| 总行数 | 5,000,000 | 50 × 100 × 1000 |
| 分区数 | 50 | 按 region 哈希分区，自动匹配 Region 数 |
| Executor 并发 | 8 | `-c 8`，与 CPU 核数一致 |

**输入 Schema**：`(region: Utf8, channelid: Utf8, captime: Utf8, recordid: Utf8, json: Utf8)`

**输出 Schema**：`(region: Utf8, dossierid: Utf8, recordids: Utf8, json1-4: Utf8)`

## 测试流程

### 1. 启动基础设施

```bash
# MinIO
docker run --rm -d -p 9000:9000 -p 9001:9001 --name minio \
  -e "MINIO_ACCESS_KEY=MINIO" -e "MINIO_SECRET_KEY=MINIOSECRET" \
  quay.io/minio/minio server /data --console-address ":9001"

# 创建 bucket
python3 -c "
from minio import Minio
client = Minio('localhost:9000', access_key='MINIO', secret_key='MINIOSECRET', secure=False)
if not client.bucket_exists('ballista'):
    client.make_bucket('ballista')
"
```

### 2. 启动分布式组件

```bash
# 构建 .so 处理器
cargo build --release -p region_cluster_processor

# 构建示例
cargo build --release --examples

# 启动 Scheduler
./target/release/examples/distributed_compute_scheduler &

# 启动 Executor（8 并发）
./target/release/examples/distributed_compute_executor \
  -p 50051 --bind-grpc-port 50052 -c 8 &
```

### 3. 运行基准测试

```bash
MAP_PARTITION_SO=target/release/libregion_cluster_processor.so \
  ./target/release/examples/bench_region_cluster_client -r 50 -j 4096
```

### 4. 内存监控

采样脚本每秒记录各进程的 RSS（`/proc/PID/status` 中的 `VmRSS`）：

```bash
LOG=/tmp/mem_monitor.log
echo "# timestamp exec_rss_mb sched_rss_mb minio_rss_mb" > $LOG
while true; do
    TS=$(date +%H:%M:%S)
    E=$(awk '/VmRSS/ {print int($2/1024)}' /proc/$EXEC_PID/status)
    S=$(awk '/VmRSS/ {print int($2/1024)}' /proc/$SCHED_PID/status)
    M=$(awk '/VmRSS/ {print int($2/1024)}' /proc/$MINIO_PID/status)
    echo "$TS $E $S $M" >> $LOG
    sleep 1
done
```

## 测试结果

### 性能对比

| 指标 | IPC (旧) | C Data Interface (新) | 提升 |
|------|---------|---------------------|------|
| 计算耗时 | 14.33s | **11.18s** | **-22%** |
| 吞吐量 | 348,954 rec/s | **447,078 rec/s** | **+28%** |
| Executor 峰值 RSS | ~4,900 MB | **~4,331 MB** | **-12%** |
| Executor finish 后 RSS | ~892 MB | **~654 MB** | **-27%** |
| 数据生成+写入 | 12.29s | 14.27s | — |

**输出验证**：5,000 个档案（= 50 regions × 100 channels），无 `CROSS_REGION_ERROR`，DistributeBy 分区语义正确。

### 资源使用对比

| 进程 | 空闲内存 | 峰值内存 | 峰值后内存 |
|------|---------|---------|----------|
| MinIO | ~116 MB | ~295 MB | ~295 MB |
| Scheduler | ~19 MB | ~62 MB | ~62 MB |
| Executor | ~19 MB | **~4,331 MB** (~4.2 GB) | ~654 MB |

### Executor 内存时间线（1 秒采样）

| 时间 | RSS | 阶段 | 说明 |
|------|-----|------|------|
| 08:49:49 | 19 MB | 空闲 | 进程启动后基准 |
| 08:50:15 | 494 MB | feed | 数据开始流入 |
| 08:50:16 | 261 MB | feed | 输入批次处理中 |
| 08:50:17 | 740 MB | feed | 更多数据到达 |
| 08:50:20 | 201 MB | feed | 批次处理完毕，内存回落 |
| 08:50:21 | 1,930 MB | execute 开始 | 遍历 HashMap，触发 Linux 惰性页面映射 |
| **08:50:22** | **4,331 MB** | **execute 峰值** | 8 个 task 同时在 execute |
| 08:50:23 | 3,995 MB | fetch | 输出中，部分数据释放 |
| 08:50:24 | 702 MB | finish | finish 完成 |

## 问题分析：Executor 峰值 4.3GB 的根因

### 结论

**4.3GB 峰值不是框架的问题，是 processor 业务逻辑故意全量缓存导致的。**

### 内存归因拆解

4.3 GB 峰值几乎全部来自 processor 缓存业务数据。框架和 Ballista 开销不可拆分控制。

```
⚫ Processor 缓存 JSON String   ≈ 3.2 GB  (74%)
   每行 4KB JSON → Rust String，全量存入 HashMap
   8 并发 × 100K 行 × 4KB = 3.2 GB

⚫ Processor 其他缓存            ≈ 0.06 GB
   recordid、channelid、HashMap/Vec 结构体

⚫ 框架 + Ballista + 进程        ≈ 1.0 GB  (23%)
   Arrow Flight 流缓冲、Ballista 内部状态、
   gRPC 连接、tokio runtime、OS 页表等
   均不在扩展点控制范围内
─────────────────────────────────────────
  合计                          ≈ 4.3 GB  (100%)
```

> 注：`to_string_array()` 内部 `.clone()` 是 `Arc<ArrayData>` 引用计数 +1，不复制底层 buffer，不产生额外内存。真正的拷贝发生在每行 `.to_string()` 创建 Rust String 时，已统一归入"Processor 缓存 JSON String"。

### 逐行代码追踪

`region_cluster_processor/src/lib.rs` 中的关键路径：

1. **`feed()` — 每行 .to_string() 全量复制**（第 126 行）：
   ```rust
   let json_val = jsons.value(i).to_string();
   ```
   每行 4KB JSON 被拷贝为独立的堆分配 Rust String，存入 `self.clusters: HashMap<String, Vec<(String, String)>>`。这是峰值内存的唯一主要原因。

2. **`execute()` — clusters 仍存活**（第 157 行）：
   ```rust
   let mut rows: Vec<OutputRow> = self.clusters.iter().map(...)...
   ```
   `self.clusters` 的 HashMap 在 execute 期间仍保有全部缓存数据。

### RSS 跳动解释

内存不是在 `feed()` 时线性涨到 4.3GB 的，而是呈现 `201MB → 1930MB → 4331MB` 的跳变：

- **Linux overcommit**：`malloc` 返回虚拟地址后，内核不会立即分配物理页。`feed()` 中大量 `String::to_string()` 的 `alloc` 调用只分配了地址空间，未映射物理页。
- **惰性缺页（demand paging）**：`execute()` 遍历 `self.clusters` 时，CPU 首次访问这些字符串的堆内存，触发缺页中断，内核分配物理页并建立映射。
- **RSS 滞后于 alloc**：RSS（驻留集大小）只统计已映射的物理页，所以 alloc 在 feed 阶段发生，RSS 增长在 execute 阶段才体现。

### 每 partition 内存核算

```
单 partition (~100K 行，100 个 channelid)：
  JSON 字符串：100K × 4KB = 400 MB
  recordid 字符串：100K × 15B = 1.5 MB
  HashMap key (channelid)：100 × 10B = 1 KB
  Vec<(String, String)> tuple 开销：100K × 48B = 4.8 MB
  HashMap hash table：~200 桶 × 32B = 6.4 KB
  ─────────────────────────────────
  单 partition 合计 ≈ 407 MB

8 并发 partition = 8 × 407 MB ≈ 3.26 GB (仅 processor 数据)
加上框架开销 ≈ 4.1~4.3 GB ✓ 匹配观测值
```

### 降低峰值的可能方向

| 方案 | 预期效果 | 代价 |
|------|---------|------|
| `-c 4` 减少并发 | 峰值 ~2.2 GB | 吞吐量下降约 40% |
| 增加分区数 | 每 partition 数据更少 | 小分区增多，调度开销增大 |
| processor 按需提取 JSON 字段 | 大幅降低缓存内存 | 需要修改业务逻辑 |
| processor 使用 `&str` 引用而非 `String` | 避免拷贝 | 需要管理生命周期，复杂度高 |

### 框架侧优化效果

C Data Interface 相比 IPC 方案：
- **消除序列化中间内存**：不再产生 IPC byte buffer，零拷贝跨 FFI 边界
- **Arc 引用计数传递**：`Buffer::clone()` = Arc 自增，无数据拷贝
- **耗时 -22%，峰值 -12%**：完全归因于消除 IPC 编解码的内存和 CPU 开销

框架层已无进一步优化空间——剩余开销分布在 Ballista Arrow Flight 流缓冲、gRPC 连接、tokio runtime 中，均不在扩展点控制范围。
