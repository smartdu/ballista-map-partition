#!/bin/bash
set -euo pipefail

# ============================================================
# 用法:
#   ./scripts/bench.sh -n 1 -r 50 -j 4096
#   ./scripts/bench.sh -n 4 -r 10 -j 1024
#
# 参数:
#   -n  并发数 (Ballista concurrent_tasks, 即同时跑的 partition 数)
#   -r  Region 数   (= 分区数, 总行数 = r × 100 × 1000)
#   -j  JSON 大小 (字节, 每条轨迹的 payload)
# ============================================================

N_TASKS=8
REGIONS=50
JSON=4096

usage() { echo "用法: $0 -n <concurrent_tasks> [-r regions] [-j json_bytes]"; exit 1; }
while getopts "n:r:j:h" opt; do
    case $opt in
        n) N_TASKS="$OPTARG" ;;
        r) REGIONS="$OPTARG" ;;
        j) JSON="$OPTARG" ;;
        *) usage ;;
    esac
done

[[ "$N_TASKS" -ge 1 ]] || usage

TOTAL_ROWS=$((REGIONS * 100 * 1000))
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="/tmp/bench_${TIMESTAMP}_n${N_TASKS}_r${REGIONS}_j${JSON}"
mkdir -p "$OUTDIR"

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BENCH="$PROJECT_DIR/target/release/examples/bench_region_cluster_client"
SCHED="$PROJECT_DIR/target/release/examples/distributed_compute_scheduler"
EXEC="$PROJECT_DIR/target/release/examples/distributed_compute_executor"
SO="$PROJECT_DIR/target/release/libregion_cluster_processor.so"

R='\033[0;31m'; G='\033[0;32m'; N='\033[0m'
ok()   { echo -e "${G}[$(date +%H:%M:%S)]${N} $*" | tee -a "$OUTDIR/script.log"; }
fail() { echo -e "${R}[$(date +%H:%M:%S)]${N} $*" | tee -a "$OUTDIR/script.log"; }

# ============================================================
# 检查二进制
# ============================================================
for bin in "$BENCH" "$SCHED" "$EXEC" "$SO"; do
    [[ -f "$bin" ]] || { fail "缺少: $bin"; exit 1; }
done

# ============================================================
# 清理
# ============================================================
ok "=== 清理环境 ==="
ps aux | grep distributed_compute | grep -v grep | awk '{print $2}' | xargs -r kill -9 2>/dev/null || true
sleep 2
docker ps -aq --filter "name=minio" 2>/dev/null | xargs -r docker stop 2>/dev/null || true
docker ps -aq --filter "name=minio" 2>/dev/null | xargs -r docker rm 2>/dev/null || true
docker network rm bench-net 2>/dev/null || true
rm -rf /tmp/bench-data
sleep 2

# 确认干净
LEFTOVER=$(ps aux | grep distributed_compute | grep -v grep | wc -l)
[[ "$LEFTOVER" -eq 0 ]] || { fail "残留 $LEFTOVER 个进程"; exit 1; }
ok "环境干净"

# ============================================================
# 启动 MinIO 单节点
# ============================================================
ok "=== 启动 MinIO ==="
docker network create bench-net 2>/dev/null || true
docker run -d --name minio --network bench-net \
    -p 9000:9000 -p 9001:9001 \
    -v /tmp/bench-data/minio:/data \
    -e MINIO_ROOT_USER=MINIO -e MINIO_ROOT_PASSWORD=MINIOSECRET \
    quay.io/minio/minio server /data \
    --address ":9000" --console-address ":9001" > /dev/null
sleep 5

python3 -c "
from minio import Minio
c=Minio('localhost:9000',access_key='MINIO',secret_key='MINIOSECRET',secure=False)
if not c.bucket_exists('ballista'): c.make_bucket('ballista')
" && ok "MinIO 就绪" || { fail "MinIO 失败"; exit 1; }

# ============================================================
# 启动 Scheduler + Executor
# ============================================================
ok "=== 启动 Scheduler ==="
"$SCHED" > "$OUTDIR/scheduler.log" 2>&1 &
sleep 2
ss -tlnp | grep -q ":50050 " || { fail "Scheduler 未启动"; exit 1; }
SCHED_PID=$(ss -tlnp | grep ":50050 " | sed -n 's/.*pid=\([0-9]*\).*/\1/p')
ok "Scheduler PID=$SCHED_PID"

ok "=== 启动 Executor (concurrent_tasks=$N_TASKS) ==="
"$EXEC" -p 50051 --bind-grpc-port 50052 -c "$N_TASKS" > "$OUTDIR/executor.log" 2>&1 &
sleep 5
ss -tlnp | grep -q ":50051 " || { fail "Executor 未启动"; exit 1; }
EXEC_PID=$(ss -tlnp | grep ":50051 " | sed -n 's/.*pid=\([0-9]*\).*/\1/p')
ok "Executor PID=$EXEC_PID"

# ============================================================
# 系统监控 (后台)
# ============================================================
get_rss_all() {
    local exec_rss=$(awk '/VmRSS/ {printf "%d", $2/1024}' /proc/$EXEC_PID/status 2>/dev/null || echo 0)
    local sched_rss=$(awk '/VmRSS/ {printf "%d", $2/1024}' /proc/$SCHED_PID/status 2>/dev/null || echo 0)
    local minio_pid=$(docker inspect minio --format '{{.State.Pid}}' 2>/dev/null)
    local minio_rss=$(awk '/VmRSS/ {printf "%d", $2/1024}' /proc/$minio_pid/status 2>/dev/null || echo 0)
    local cpu=$(ps -p $EXEC_PID -o %cpu --no-headers 2>/dev/null | awk '{printf "%.1f", $1}')
    echo "$exec_rss $sched_rss $minio_rss $cpu"
}

ok "=== 开始监控 ==="
(   echo "#ts exec_rss_mb sched_rss_mb minio_rss_mb exec_cpu_pct"
    while kill -0 $EXEC_PID 2>/dev/null; do
        TS=$(date +%H:%M:%S)
        echo "$TS $(get_rss_all)"
        sleep 1
    done
) > "$OUTDIR/monitor.csv" &
MON_PID=$!
sleep 1

# 冷启动基线
read -r _ baseline_exec _ baseline_sched _ baseline_minio _ < <(get_rss_all; echo)
ok "冷启动基线: Executor=${baseline_exec}MB Scheduler=${baseline_sched}MB MinIO=${baseline_minio}MB"

# ============================================================
# 压测
# ============================================================
ok "=== 运行压测 ==="
ok "  参数: -n $N_TASKS (并发)  -r $REGIONS (region)  -j $JSON (JSON字节)"
ok "  总行数: $TOTAL_ROWS  (= $REGIONS regions × 100 channels × 1000 轨迹)"
ok "  结果目录: $OUTDIR"

MAP_PARTITION_SO="$SO" "$BENCH" -r "$REGIONS" -j "$JSON" 2>&1 | tee "$OUTDIR/bench.log"
BENCH_RC=${PIPESTATUS[0]}

# 停止监控
kill $MON_PID 2>/dev/null || true; wait $MON_PID 2>/dev/null || true

# ============================================================
# 提取结果
# ============================================================
GEN_TIME=$(grep "数据生成+写入" "$OUTDIR/bench.log" | awk -F': ' '{print $2}' | tr -d 's')
COMPUTE_TIME=$(grep "分布式计算耗时" "$OUTDIR/bench.log" | awk -F': ' '{print $2}' | tr -d 's')
THROUGHPUT=$(grep "吞吐量" "$OUTDIR/bench.log" | awk -F': ' '{print $2}')
OUTPUT_ROWS=$(grep "输出行数" "$OUTDIR/bench.log" | head -1 | awk -F': ' '{print $2}')
CROSS_REGION=$(grep -c "无 CROSS_REGION_ERROR" "$OUTDIR/bench.log" || echo 0)

# 内存分析
PEAK_EXEC=$(awk 'NR>1 {if($2>max)max=$2} END{print max}' "$OUTDIR/monitor.csv")
PEAK_SCHED=$(awk 'NR>1 {if($3>max)max=$3} END{print max}' "$OUTDIR/monitor.csv")
PEAK_MINIO=$(awk 'NR>1 {if($4>max)max=$4} END{print max}' "$OUTDIR/monitor.csv")
PEAK_CPU=$(awk 'NR>1 {if($5+0>max)max=$5} END{print max}' "$OUTDIR/monitor.csv")

# 回落后的值 (最后 3 行平均)
AFTER_EXEC=$(tail -20 "$OUTDIR/monitor.csv" | awk 'NR>1 {s+=$2;c++} END{printf "%d", s/c}')
AFTER_SCHED=$(tail -20 "$OUTDIR/monitor.csv" | awk 'NR>1 {s+=$3;c++} END{printf "%d", s/c}')
AFTER_MINIO=$(tail -20 "$OUTDIR/monitor.csv" | awk 'NR>1 {s+=$4;c++} END{printf "%d", s/c}')
AFTER_CPU=$(tail -20 "$OUTDIR/monitor.csv" | awk 'NR>1 {s+=$5;c++} END{printf "%.1f", s/c}')

# ============================================================
# 输出报告
# ============================================================
ok "============================================"
ok "  压测结果"
ok "============================================"
cat <<EOF | tee "$OUTDIR/result.txt"

  配置:
    并发数:       $N_TASKS
    Region 数:    $REGIONS
    JSON 大小:    $JSON 字节
    总行数:       $TOTAL_ROWS

  耗时:
    数据生成+写入: ${GEN_TIME}s
    分布式计算:    ${COMPUTE_TIME}s
    吞吐量:        $THROUGHPUT

  内存 (RSS):
    ┌──────────┬──────────┬──────────┬──────────┐
    │ 进程     │ 冷启动   │ 峰值     │ 回落     │
    ├──────────┼──────────┼──────────┼──────────┤
    │ Executor │ ${baseline_exec} MB    │ ${PEAK_EXEC} MB     │ ${AFTER_EXEC} MB     │
    │ Scheduler│ ${baseline_sched} MB    │ ${PEAK_SCHED} MB     │ ${AFTER_SCHED} MB     │
    │ MinIO    │ ${baseline_minio} MB    │ ${PEAK_MINIO} MB     │ ${AFTER_MINIO} MB     │
    └──────────┴──────────┴──────────┴──────────┘

  Executor CPU 峰值: ${PEAK_CPU}%   回落: ${AFTER_CPU}%

  输出: ${OUTPUT_ROWS} 行  |  正确性: $([[ $CROSS_REGION -ge 1 ]] && echo "✅ 无 CROSS_REGION_ERROR" || echo "❌ 异常")
EOF

ok "完整数据: $OUTDIR"

[[ "$BENCH_RC" -eq 0 ]] || exit 1
