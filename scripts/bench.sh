#!/bin/bash
set -euo pipefail

# ============================================================
# 用法:
#   ./scripts/bench.sh                     # 默认: 1 executor * 8 并发, 50 regions, 4KB JSON
#   ./scripts/bench.sh -e 2                # 2 executor * 4 并发
#   ./scripts/bench.sh -e 1 -n 4           # 1 executor * 4 并发
#   ./scripts/bench.sh -e 2 -r 10 -j 1024 # 2 executor, 小数据量
#
# 参数:
#   -e  Executor 数量 (1-2, 默认 1)
#   -n  每 Executor 并发 task 数 (默认 8, 多 executor 时自动 / executor 数)
#   -r  Region 数 (默认 50, 总行数 = r × 100 × 1000)
#   -j  JSON 大小 字节 (默认 4096)
# ============================================================

E=1             # executor 数量
C=8             # 每 executor 并发数
REGIONS=50
JSON=4096

usage() { echo "用法: $0 [-e 1|2] [-n concurrent] [-r regions] [-j json_bytes]"; exit 1; }
while getopts "e:n:r:j:h" opt; do
    case $opt in
        e) E="$OPTARG"; [[ "$E" =~ ^[12]$ ]] || usage ;;
        n) N="$OPTARG" ;;
        r) REGIONS="$OPTARG" ;;
        j) JSON="$OPTARG" ;;
        *) usage ;;
    esac
done

[[ "$C" -ge 1 ]] || usage
[[ "$E" -eq 1 ]] && MINIO_NODES=1 || MINIO_NODES=4

TOTAL_ROWS=$((REGIONS * 100 * 1000))
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="/tmp/bench_${TIMESTAMP}_e${E}_n${N}_r${REGIONS}_j${JSON}"
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
# 二进制检查
# ============================================================
for bin in "$BENCH" "$SCHED" "$EXEC" "$SO"; do
    [[ ! -f "$bin" ]] && { fail "缺少: $bin"; exit 1; }
done

# ============================================================
# 清理
# ============================================================
clean() {
    ok "=== 清理 ==="
    ps aux | grep distributed_compute | grep -v grep | awk '{print $2}' | xargs -r kill -9 2>/dev/null || true
    sleep 2
    docker ps -aq --filter "name=minio" 2>/dev/null | xargs -r docker stop 2>/dev/null || true
    docker ps -aq --filter "name=minio" 2>/dev/null | xargs -r docker rm 2>/dev/null || true
    docker network rm bench-net 2>/dev/null || true
    rm -rf /tmp/bench-data
    sleep 2
    local n=$(ps aux | grep distributed_compute | grep -v grep | wc -l)
    [[ "$n" -eq 0 ]] || { fail "残留 $n 个进程"; exit 1; }
    ok "环境干净"
}

# ============================================================
# MinIO
# ============================================================
start_minio() {
    ok "=== 启动 MinIO (${MINIO_NODES} 节点) ==="
    docker network create bench-net 2>/dev/null || true
    mkdir -p /tmp/bench-data/minio{1,2,3,4}

    if [[ $MINIO_NODES -eq 1 ]]; then
        docker run -d --name minio --network bench-net \
            -p 9000:9000 -p 9001:9001 \
            -v /tmp/bench-data/minio1:/data \
            -e MINIO_ROOT_USER=MINIO -e MINIO_ROOT_PASSWORD=MINIOSECRET \
            quay.io/minio/minio server /data \
            --address ":9000" --console-address ":9001" > /dev/null
        sleep 5
    else
        docker run -d --name minio1 --network bench-net --hostname minio1 \
            -p 9000:9000 -v /tmp/bench-data/minio1:/data \
            -e MINIO_ROOT_USER=MINIO -e MINIO_ROOT_PASSWORD=MINIOSECRET \
            quay.io/minio/minio server http://minio{1...4}/data \
            --address ":9000" > /dev/null
        for i in 2 3 4; do
            docker run -d --name minio${i} --network bench-net --hostname minio${i} \
                -v /tmp/bench-data/minio${i}:/data \
                -e MINIO_ROOT_USER=MINIO -e MINIO_ROOT_PASSWORD=MINIOSECRET \
                quay.io/minio/minio server http://minio{1...4}/data \
                --address ":9000" > /dev/null
        done
        sleep 8
        local up=$(docker ps --filter "name=minio" --format "{{.Names}}" | wc -l)
        [[ "$up" -eq 4 ]] || { fail "MinIO 容器: 预期 4 实际 $up"; exit 1; }
    fi

    python3 -c "
from minio import Minio
c=Minio('localhost:9000',access_key='MINIO',secret_key='MINIOSECRET',secure=False)
if not c.bucket_exists('ballista'): c.make_bucket('ballista')
" && ok "  MinIO 就绪" || { fail "MinIO 失败"; exit 1; }
}

# ============================================================
# Scheduler + Executors
# ============================================================
start_cluster() {
    ok "=== 启动 Scheduler ==="
    "$SCHED" > "$OUTDIR/scheduler.log" 2>&1 &
    sleep 2
    ss -tlnp | grep -q ":50050 " || { fail "Scheduler 未启动"; exit 1; }
    SCHED_PID=$(ss -tlnp | grep ":50050 " | sed -n 's/.*pid=\([0-9]*\).*/\1/p')
    ok "  Scheduler PID=$SCHED_PID"

    ok "=== 启动 $E 个 Executor (各 $C 并发) ==="
    for i in $(seq 1 $E); do
        local flight=$((50050 + i * 2 - 1))
        local grpc=$((50050 + i * 2))
        "$EXEC" -p $flight --bind-grpc-port $grpc -c $C > "$OUTDIR/executor_${i}.log" 2>&1 &
    done
    sleep $((4 + E * 2))

    # 校验
    for i in $(seq 1 $E); do
        local flight=$((50050 + i * 2 - 1))
        ss -tlnp | grep -q ":$flight " || { fail "Executor #$i (port $flight) 未启动"; exit 1; }
        local pid=$(ss -tlnp | grep ":$flight " | sed -n 's/.*pid=\([0-9]*\).*/\1/p')
        ok "  Executor #$i PID=$pid port=$flight"
    done
    ok "  $E 个 Executor 全部就绪"
}

# ============================================================
# 监控
# ============================================================
start_monitor() {
    local exec_pids=$(for i in $(seq 1 $E); do
        local flight=$((50050 + i * 2 - 1))
        ss -tlnp | grep ":$flight " | sed -n 's/.*pid=\([0-9]*\).*/\1/p'
    done | tr '\n' ' ')
    local sched_pid=$(ss -tlnp | grep ":50050 " | sed -n 's/.*pid=\([0-9]*\).*/\1/p')
    local minio_pid=$(docker inspect minio 2>/dev/null | sed -n 's/.*"Pid":\([0-9]*\).*/\1/p' | head -1)
    [[ -z "$minio_pid" ]] && minio_pid=$(docker inspect minio1 2>/dev/null | sed -n 's/.*"Pid":\([0-9]*\).*/\1/p' | head -1)

    echo "#ts $(echo $exec_pids | sed 's/ /_rss /g')_rss sched_rss minio_rss" > "$OUTDIR/monitor.csv"

    (   while true; do
            TS=$(date +%H:%M:%S)
            local out="$TS"
            for pid in $exec_pids; do
                local rss=$(awk '/VmRSS/ {printf "%d", $2/1024}' /proc/$pid/status 2>/dev/null || echo 0)
                out="$out $rss"
            done
            local s_rss=$(awk '/VmRSS/ {printf "%d", $2/1024}' /proc/$sched_pid/status 2>/dev/null || echo 0)
            local m_rss=$(awk '/VmRSS/ {printf "%d", $2/1024}' /proc/$minio_pid/status 2>/dev/null || echo 0)
            echo "$out $s_rss $m_rss"
            sleep 1
        done
    ) > "$OUTDIR/monitor.csv" &
    MON_PID=$!
    sleep 1
    ok "=== 监控已启动 PID=$MON_PID ==="
}

# ============================================================
# 压测
# ============================================================
run_bench() {
    ok "=== 运行压测 ==="
    ok "  配置: ${E} Executor × ${N} 并发  |  ${REGIONS} regions × ${JSON}B JSON  |  ${TOTAL_ROWS} 行"

    MAP_PARTITION_SO="$SO" "$BENCH" -r "$REGIONS" -j "$JSON" 2>&1 | tee "$OUTDIR/bench.log"
}

# ============================================================
# 主流程
# ============================================================
clean
start_minio
start_cluster
start_monitor

# 冷启动基线
BASELINE=$(tail -1 "$OUTDIR/monitor.csv")
ok "  冷启动 RSS: $BASELINE"

run_bench
BENCH_RC=$?

kill $MON_PID 2>/dev/null || true; wait $MON_PID 2>/dev/null || true
sleep 2

# ============================================================
# 提取结果
# ============================================================
MONDATA="$OUTDIR/monitor.csv"
BASELINE_EXEC=$(awk 'NR==2 {print $2}' "$MONDATA")
PEAK_EXEC=$(awk 'NR>1 {for(i=2;i<=NF-2;i++) if($i+0>m[i]) m[i]=$i} END{for(i=2;i<=NF-2;i++) printf "%d ", m[i]}' "$MONDATA")
PEAK_SCHED=$(awk 'NR>1 {if($(NF-1)+0>m) m=$(NF-1)} END{printf "%d", m}' "$MONDATA")
PEAK_MINIO=$(awk 'NR>1 {if($NF+0>m) m=$NF} END{printf "%d", m}' "$MONDATA")
AFTER_EXEC=$(tail -20 "$MONDATA" | awk 'NR>1 {for(i=2;i<=NF-2;i++) s[i]+=$i} END{for(i=2;i<=NF-2;i++) printf "%d ", s[i]/NR}' "$MONDATA")
AFTER_SCHED=$(tail -20 "$MONDATA" | awk 'NR>1 {s+=$(NF-1)} END{printf "%d", s/NR}' "$MONDATA")
AFTER_MINIO=$(tail -20 "$MONDATA" | awk 'NR>1 {s+=$NF} END{printf "%d", s/NR}' "$MONDATA")

GEN_TIME=$(grep "数据生成+写入" "$OUTDIR/bench.log" | awk -F': ' '{print $2}' | tr -d 's')
COMPUTE_TIME=$(grep "分布式计算耗时" "$OUTDIR/bench.log" | awk -F': ' '{print $2}' | tr -d 's')
THROUGHPUT=$(grep "吞吐量" "$OUTDIR/bench.log" | awk -F': ' '{print $2}')
OUTPUT_ROWS=$(grep "输出行数" "$OUTDIR/bench.log" | head -1 | awk -F': ' '{print $2}')
CROSS_OK=$(grep -c "无 CROSS_REGION_ERROR" "$OUTDIR/bench.log" || true)

# ============================================================
# 报告
# ============================================================
ok "============================================"
ok "  压测结果"
ok "============================================"
cat <<EOF | tee "$OUTDIR/result.txt"

  配置:
    Executor 数:   $E
    每 Exec 并发:  $C
    Region 数:     $REGIONS × 100 channel × 1000 轨迹 = ${TOTAL_ROWS} 行
    JSON 大小:     $JSON 字节

  耗时:
    数据生成+写入: ${GEN_TIME}s
    分布式计算:    ${COMPUTE_TIME}s
    吞吐量:        $THROUGHPUT

  资源 (RSS MB):
    ┌──────────┬──────────┬──────────┬──────────┐
    │ 进程     │ 冷启动   │ 峰值     │ 回落     │
    ├──────────┼──────────┼──────────┼──────────┤
    │ Executor │ $(printf '%6s' $BASELINE_EXEC)  │ $(printf '%8s' "$PEAK_EXEC") │ $(printf '%8s' "$AFTER_EXEC") │
    │ Scheduler│ $(printf '%6s' $BASELINE_SCHED)  │ $(printf '%8s' $PEAK_SCHED) │ $(printf '%8s' $AFTER_SCHED) │
    │ MinIO    │ $(printf '%6s' $BASELINE_MINIO)  │ $(printf '%8s' $PEAK_MINIO) │ $(printf '%8s' $AFTER_MINIO) │
    └──────────┴──────────┴──────────┴──────────┘

  正确性: $([[ $CROSS_OK -ge 1 ]] && echo "✅ 无 CROSS_REGION_ERROR" || echo "❌ 异常")

  详细数据: $OUTDIR
EOF

[[ "$BENCH_RC" -eq 0 ]] || exit 1
