#!/bin/bash
set -euo pipefail

# ============================================================
# 用法:
#   ./scripts/bench.sh                          # 默认: 1e 8c 50r 4KB
#   ./scripts/bench.sh -e 2 -c 2               # 2 executor, 2 并发
#   ./scripts/bench.sh -e 1 -c 4 -r 10 -j 1024 # 小数据量
#
# 参数:
#   -e  Executor 数 (默认 1, MinIO 数 = Executor 数)
#   -c  每 Executor 并发数 (默认 8)
#   -r  Region 数 (默认 50)
#   -j  JSON 大小 (默认 4096)
# ============================================================

E=1 C=8 REGIONS=50 JSON=4096

while getopts "e:c:r:j:h" opt; do
    case $opt in
        e) E="$OPTARG"; [[ "$E" =~ ^[1-4]$ ]] || { echo "错误: -e 1-4"; exit 1; } ;;
        c) C="$OPTARG"; [[ "$C" -ge 1 ]] || { echo "错误: -c >= 1"; exit 1; } ;;
        r) REGIONS="$OPTARG" ;;
        j) JSON="$OPTARG" ;;
        *) echo "用法: $0 [-e 1|2|3|4] [-c concurrent] [-r regions] [-j json_bytes]"; exit 1 ;;
    esac
done

TOTAL_ROWS=$((REGIONS * 100 * 1000))
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="/tmp/bench_${TIMESTAMP}_e${E}_c${C}_r${REGIONS}_j${JSON}"
mkdir -p "$OUTDIR"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BENCH="$ROOT/target/release/examples/bench_region_cluster_client"
SCHED="$ROOT/target/release/examples/distributed_compute_scheduler"
EXEC="$ROOT/target/release/examples/distributed_compute_executor"
SO="$ROOT/target/release/libregion_cluster_processor.so"

R='\033[0;31m'; G='\033[0;32m'; X='\033[0m'
ok()   { echo -e "${G}[$(date +%H:%M:%S)]${X} $*" | tee -a "$OUTDIR/script.log"; }
fail() { echo -e "${R}[$(date +%H:%M:%S)]${X} $*" | tee -a "$OUTDIR/script.log"; }

for f in "$BENCH" "$SCHED" "$EXEC" "$SO"; do
    [[ -f "$f" ]] || { fail "缺少: $f"; exit 1; }
done

# ============================================================
clean() {
    ok "=== 清理 ==="
    ps aux | grep distributed_compute | grep -v grep | awk '{print $2}' | xargs -r kill -9 2>/dev/null || true
    sleep 2
    docker ps -aq --filter "name=minio" 2>/dev/null | xargs -r docker stop  2>/dev/null || true
    docker ps -aq --filter "name=minio" 2>/dev/null | xargs -r docker rm    2>/dev/null || true
    docker network rm bench-net 2>/dev/null || true
    rm -rf /tmp/bench-data
    sleep 2
    local n=$(ps aux | grep distributed_compute | grep -v grep | wc -l)
    [[ "$n" -eq 0 ]] || { fail "残留 $n 个进程"; exit 1; }
    ok "干净"
}

start_minio() {
    ok "=== MinIO (${E} 节点) ==="
    docker network create bench-net 2>/dev/null || true
    mkdir -p /tmp/bench-data/minio{1,2,3,4}

    if [[ $E -eq 1 ]]; then
        docker run -d --name minio1 --network bench-net \
            -p 9000:9000 -v /tmp/bench-data/minio1:/data \
            -e MINIO_ROOT_USER=MINIO -e MINIO_ROOT_PASSWORD=MINIOSECRET \
            quay.io/minio/minio server /data --address ":9000" > /dev/null
        sleep 5
    else
        local nodes=""
        for i in $(seq 1 $E); do nodes="$nodes http://minio${i}/data"; done
        docker run -d --name minio1 --network bench-net --hostname minio1 \
            -p 9000:9000 -v /tmp/bench-data/minio1:/data \
            -e MINIO_ROOT_USER=MINIO -e MINIO_ROOT_PASSWORD=MINIOSECRET \
            quay.io/minio/minio server $nodes --address ":9000" 2>&1 | tee -a "$OUTDIR/minio.log"
        for i in $(seq 2 $E); do
            docker run -d --name minio${i} --network bench-net --hostname minio${i} \
                -v /tmp/bench-data/minio${i}:/data \
                -e MINIO_ROOT_USER=MINIO -e MINIO_ROOT_PASSWORD=MINIOSECRET \
                quay.io/minio/minio server $nodes --address ":9000" 2>&1 | tee -a "$OUTDIR/minio.log"
        done
        sleep 8
        local up=$(docker ps --filter "name=minio" --format "{{.Names}}" | wc -l)
        if [[ "$up" -ne "$E" ]]; then
            fail "MinIO: 预期 $E 实际 $up"
            fail "--- 容器状态 ---"
            docker ps -a --filter "name=minio" --format "{{.Names}} {{.Status}}" | tee -a "$OUTDIR/minio.log"
            fail "--- minio1 日志 ---"
            docker logs minio1 2>&1 | tail -20 | tee -a "$OUTDIR/minio.log"
            exit 1
        fi
    fi

    python3 -c "
from minio import Minio
c=Minio('localhost:9000',access_key='MINIO',secret_key='MINIOSECRET',secure=False)
if not c.bucket_exists('ballista'): c.make_bucket('ballista')
" && ok "  MinIO 就绪" || { fail "MinIO 失败"; exit 1; }
}

start_cluster() {
    ok "=== Scheduler ==="
    "$SCHED" > "$OUTDIR/scheduler.log" 2>&1 &
    sleep 2
    ss -tlnp | grep -q ":50050 " || { fail "Scheduler 未启动"; exit 1; }
    ok "  Scheduler 就绪"

    ok "=== ${E} Executor (各 ${C} 并发) ==="
    for i in $(seq 1 $E); do
        local flight=$((50050 + i * 2 - 1))
        local grpc=$((50050 + i * 2))
        "$EXEC" -p $flight --bind-grpc-port $grpc -c $C > "$OUTDIR/executor_${i}.log" 2>&1 &
    done
    sleep $((4 + E * 2))

    for i in $(seq 1 $E); do
        local flight=$((50050 + i * 2 - 1))
        ss -tlnp | grep -q ":$flight " || { fail "Executor #$i 未启动"; exit 1; }
    done
    ok "  ${E} Executor 就绪"
}

# ============================================================
clean
start_minio
start_cluster

# ============================================================
# 监控
# ============================================================
MON_CSV="$OUTDIR/monitor.csv"
exec_pids=""
for i in $(seq 1 $E); do
    flight=$((50050 + i * 2 - 1))
    pid=$(ss -tlnp | grep ":$flight " | sed -n 's/.*pid=\([0-9]*\).*/\1/p')
    exec_pids="$exec_pids $pid"
done
sched_pid=$(ss -tlnp | grep ":50050 " | sed -n 's/.*pid=\([0-9]*\).*/\1/p')
minio_pid=$(docker inspect minio1 2>/dev/null | sed -n 's/.*"Pid": *\([0-9]*\).*/\1/p' | head -1)

echo "#ts $(echo $exec_pids | sed 's/ /_rss /g')_rss sched_rss minio_rss" > "$MON_CSV"
(   while true; do
        TS=$(date +%H:%M:%S); out="$TS"
        for pid in $exec_pids; do
            out="$out $(awk '/VmRSS/ {printf "%d",$2/1024}' /proc/$pid/status 2>/dev/null || echo 0)"
        done
        out="$out $(awk '/VmRSS/ {printf "%d",$2/1024}' /proc/$sched_pid/status 2>/dev/null || echo 0)"
        out="$out $(awk '/VmRSS/ {printf "%d",$2/1024}' /proc/$minio_pid/status 2>/dev/null || echo 0)"
        echo "$out"; sleep 1
    done
) > "$MON_CSV" &
MON_PID=$!
sleep 1

BASELINE=$(tail -1 "$MON_CSV" | awk '{for(i=2;i<=NF;i++) printf "%d ", $i}')
ok "  冷启动 RSS: $(echo $BASELINE)"
ok "=== 压测 ==="
ok "  配置: ${E}e × ${C}c  |  ${REGIONS}r × ${JSON}B  |  ${TOTAL_ROWS} 行"

MAP_PARTITION_SO="$SO" "$BENCH" -r "$REGIONS" -j "$JSON" 2>&1 | tee "$OUTDIR/bench.log"
RC=${PIPESTATUS[0]}

kill $MON_PID 2>/dev/null || true; wait $MON_PID 2>/dev/null || true
sleep 2

# ============================================================
# 结果
# ============================================================
GEN_T=$(grep "数据生成+写入"   "$OUTDIR/bench.log" | awk -F': ' '{print $2}' | tr -d 's')
CMP_T=$(grep "分布式计算耗时"   "$OUTDIR/bench.log" | awk -F': ' '{print $2}' | tr -d 's')
TPUT=$(grep  "吞吐量"           "$OUTDIR/bench.log" | awk -F': ' '{print $2}')
ROWS=$(grep  "输出行数"         "$OUTDIR/bench.log" | head -1 | awk -F': ' '{print $2}')
CROSS=$(grep -c "无 CROSS_REGION_ERROR" "$OUTDIR/bench.log" || true)

# Executor: 所有 executor 列的最大值/平均值
PEAK_EXEC=""
for i in $(seq 1 $E); do
    p=$(awk "NR>1 {if(\$((i+1))+0>m) m=\$((i+1))} END{printf \"%d\", m}" "$MON_CSV")
    PEAK_EXEC="$PEAK_EXEC $p"
done
PEAK_EXEC=$(echo $PEAK_EXEC | xargs)
PEAK_SCHED=$(awk "NR>1 {if(\$(NF-1)+0>m) m=\$(NF-1)} END{printf \"%d\", m}" "$MON_CSV")
PEAK_MINIO=$(awk "NR>1 {if(\$NF+0>m) m=\$NF} END{printf \"%d\", m}" "$MON_CSV")
BASE_EXEC=$(head -2 "$MON_CSV" | tail -1 | awk '{printf "%d", $2}')
BASE_SCHED=$(head -2 "$MON_CSV" | tail -1 | awk '{printf "%d", $(NF-1)}')
BASE_MINIO=$(head -2 "$MON_CSV" | tail -1 | awk '{printf "%d", $NF}')
AFT_EXEC=$(tail -20 "$MON_CSV" | awk "NR>1 {s+=\$2} END{printf \"%d\", s/(NR-1)}")
AFT_SCHED=$(tail -20 "$MON_CSV" | awk 'NR>1 {s+=$(NF-1)} END{printf "%d", s/(NR-1)}')
AFT_MINIO=$(tail -20 "$MON_CSV" | awk 'NR>1 {s+=$NF} END{printf "%d", s/(NR-1)}')

cat <<EOF | tee "$OUTDIR/result.txt"

  配置:       ${E} Executor × ${C} 并发  |  ${REGIONS} regions × ${JSON}B JSON  |  ${TOTAL_ROWS} 行

  耗时:       数据生成+写入  ${GEN_T}s  |  分布式计算  ${CMP_T}s  |  吞吐量  $TPUT

  内存 (MB):  ┌─────────┬────────┬────────┬────────┐
              │         │ 冷启动 │ 峰值   │ 回落   │
              ├─────────┼────────┼────────┼────────┤
              │ Executor│ $(printf '%6s' $BASE_EXEC) │ $(printf '%6s' "$PEAK_EXEC") │ $(printf '%6s' $AFT_EXEC) │
              │ Scheduler│ $(printf '%5s' $BASE_SCHED) │ $(printf '%6s' $PEAK_SCHED) │ $(printf '%6s' $AFT_SCHED) │
              │ MinIO   │ $(printf '%6s' $BASE_MINIO) │ $(printf '%6s' $PEAK_MINIO) │ $(printf '%6s' $AFT_MINIO) │
              └─────────┴────────┴────────┴────────┘

  正确性:     $([[ $CROSS -ge 1 ]] && echo "✅ 无 CROSS_REGION_ERROR" || echo "❌ 异常")

  数据目录:   $OUTDIR
EOF

[[ "$RC" -eq 0 ]] || exit 1
