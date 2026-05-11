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
        e) E="$OPTARG"; [[ "$E" =~ ^[1-9]$|^10$ ]] || { echo "错误: -e 1-10"; exit 1; } ;;
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

    # 按进程名杀
    ps aux | grep distributed_compute | grep -v grep | awk '{print $2}' | xargs -r kill -9 2>/dev/null || true

    # 按端口杀: 扫描所有 LISTEN 端口, 属于 distributed_compute 的一律杀掉
    ss -tlnp 2>/dev/null | grep distributed_compute | \
        sed -n 's/.*pid=\([0-9]*\).*/\1/p' | sort -u | \
        xargs -r kill -9 2>/dev/null || true
    sleep 2

    # 清理所有 minio 容器 (不管多少个)
    docker ps -aq --filter "name=minio" 2>/dev/null | xargs -r docker stop  2>/dev/null || true
    docker ps -aq --filter "name=minio" 2>/dev/null | xargs -r docker rm    2>/dev/null || true

    # 清理所有 bench-minio 卷
    docker volume ls -q --filter "name=bench-minio" 2>/dev/null | xargs -r docker volume rm -f 2>/dev/null || true
    docker network rm bench-net 2>/dev/null || true
    sleep 2

    # 最终校验: 进程和容器清干净了就行
    local n=0
    ps aux | grep distributed_compute | grep -v grep | grep -q . && n=$((n + 1))
    docker ps -aq --filter "name=minio" 2>/dev/null | grep -q . && n=$((n + 1))
    [[ "$n" -eq 0 ]] || { fail "残留 $n 类资源 (distributed_compute进程/minio容器)"; exit 1; }
    ok "干净"
}

start_minio() {
    ok "=== MinIO (${E} 节点) ==="
    docker network create bench-net 2>/dev/null || true

    # 先删同名容器 (兜底)
    for i in $(seq 1 $E); do docker rm -f minio${i} 2>/dev/null || true; done

    # Docker volumes (overlayfs bind mount 不支持 O_DIRECT)
    docker volume ls -q --filter "name=bench-minio" 2>/dev/null | xargs -r docker volume rm -f 2>/dev/null || true
    for i in $(seq 1 $E); do docker volume create bench-minio-${i} > /dev/null; done

    if [[ $E -eq 1 ]]; then
        docker run -d --name minio1 --network bench-net \
            -p 9000:9000 -v bench-minio-1:/data \
            -e MINIO_ROOT_USER=MINIO -e MINIO_ROOT_PASSWORD=MINIOSECRET \
            quay.io/minio/minio server /data --address ":9000" > /dev/null
        sleep 5
    else
        local nodes=""
        for i in $(seq 1 $E); do nodes="$nodes http://minio${i}/data"; done
        docker run -d --name minio1 --network bench-net --hostname minio1 \
            -p 9000:9000 -v bench-minio-1:/data \
            -e MINIO_ROOT_USER=MINIO -e MINIO_ROOT_PASSWORD=MINIOSECRET \
            quay.io/minio/minio server $nodes --address ":9000" 2>&1 | tee -a "$OUTDIR/minio.log"
        for i in $(seq 2 $E); do
            docker run -d --name minio${i} --network bench-net --hostname minio${i} \
                -v bench-minio-${i}:/data \
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
    SCHED_PID=$!
    sleep 3
    kill -0 $SCHED_PID 2>/dev/null || { fail "Scheduler 未启动"; cat "$OUTDIR/scheduler.log"; exit 1; }
    ok "  Scheduler PID=$SCHED_PID"

    ok "=== ${E} Executor (各 ${C} 并发) ==="
    EXEC_PIDS=()
    for i in $(seq 1 $E); do
        flight=$((50050 + i * 2 - 1))
        grpc=$((50050 + i * 2))
        "$EXEC" -p $flight --bind-grpc-port $grpc -c $C > "$OUTDIR/executor_${i}.log" 2>&1 &
        pid=$!
        EXEC_PIDS+=($pid)
        ok "  启动 Executor #$i, PID=$pid, port=$flight"
    done
    sleep 5

    for i in $(seq 0 $((${#EXEC_PIDS[@]} - 1))); do
        pid=${EXEC_PIDS[$i]}
        if kill -0 $pid 2>/dev/null; then
            ok "  Executor #$((i+1)) PID=$pid ✓ 存活"
        else
            fail "Executor #$((i+1)) PID=$pid 已退出, 日志:"
            cat "$OUTDIR/executor_$((i+1)).log"
            exit 1
        fi
    done
}

# ============================================================
clean
start_minio
start_cluster

# ============================================================
# 监控
# ============================================================
MON_CSV="$OUTDIR/monitor.csv"

# 收集所有要监控的 PID
ALL_PIDS=("${EXEC_PIDS[@]}")
ALL_PIDS+=($SCHED_PID)
for c in $(seq 1 $E); do
    pid=$(docker inspect minio${c} 2>/dev/null | sed -n 's/.*"Pid": *\([0-9]*\).*/\1/p' | head -1)
    [[ -n "$pid" ]] && ALL_PIDS+=($pid)
done
ALL_NAMES=()
for i in $(seq 1 $E); do ALL_NAMES+=("executor${i}"); done
ALL_NAMES+=("scheduler")
for i in $(seq 1 $E); do ALL_NAMES+=("minio${i}"); done

# 表头
(   echo -n "#ts"
    for n in "${ALL_NAMES[@]}"; do echo -n " ${n}_rss"; done
    echo
) > "$MON_CSV"

# 采集
(   while true; do
        out="$(date +%H:%M:%S)"
        for pid in "${ALL_PIDS[@]}"; do
            out="$out $(awk '/VmRSS/ {printf "%d",$2/1024}' /proc/$pid/status 2>/dev/null || echo 0)"
        done
        echo "$out"; sleep 1
    done
) >> "$MON_CSV" &
MON_PID=$!
sleep 1

BASELINE=$(tail -1 "$MON_CSV")
ok "  冷启动: $BASELINE"
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

# 逐个进程统计
result_lines=""
for idx in $(seq 0 $((${#ALL_PIDS[@]} - 1))); do
    name="${ALL_NAMES[$idx]}"
    col=$((idx + 2))  # CSV 第 1 列是 ts
    base=$(awk "NR==2 {printf \"%d\", \$$col}" "$MON_CSV")
    peak=$(awk "NR>1  {if(\$$col+0>m) m=\$$col} END{printf \"%d\", m}" "$MON_CSV")
    after=$(tail -20 "$MON_CSV" | awk "{s+=\$$col} END{printf \"%d\", s/(NR-1)}")
    result_lines="$result_lines${name} ${base} ${peak} ${after}\n"
done
result_lines=$(echo -e "$result_lines")

cat <<EOF | tee "$OUTDIR/result.txt"

  配置:       ${E} Executor × ${C} 并发  |  ${REGIONS} regions × ${JSON}B JSON  |  ${TOTAL_ROWS} 行
  耗时:       数据生成+写入 ${GEN_T}s  |  分布式计算 ${CMP_T}s  |  吞吐量 $TPUT

  内存 (MB):
              NAME        冷启动   峰值     回落
$(echo "$result_lines" | while read name base peak after; do
    printf "              %-10s %6s %8s %8s\n" "$name" "$base" "$peak" "$after"
done)

  正确性:     $([[ $CROSS -ge 1 ]] && echo "✅ 无 CROSS_REGION_ERROR" || echo "❌ 异常")

  原始数据:   $OUTDIR/monitor.csv
EOF

[[ "$RC" -eq 0 ]] || exit 1
