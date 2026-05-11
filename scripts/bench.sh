#!/bin/bash
set -euo pipefail

# ============================================================
# 用法:
#   ./scripts/bench.sh -n 1                 # 1 Executor
#   ./scripts/bench.sh -n 4                 # 4 Executor
#   ./scripts/bench.sh -n 1 -r 10 -j 1024  # 自定义数据量
# ============================================================

N_EXEC=1          # Executor 数量 (1-4)
REGIONS=50        # Region 数
JSON_SIZE=4096    # JSON 大小 (字节)
CONCURRENT=8      # 每 Executor 并发 (自动: 8/n)

while [[ $# -gt 0 ]]; do
    case $1 in
        -n) N_EXEC="$2"; shift 2 ;;
        -r) REGIONS="$2"; shift 2 ;;
        -j) JSON_SIZE="$2"; shift 2 ;;
        *) echo "用法: $0 -n <1|4> [-r regions] [-j json_bytes]"; exit 1 ;;
    esac
done

# 校验
if [[ ! "$N_EXEC" =~ ^(1|2|3|4)$ ]]; then
    echo "错误: -n 必须是 1-4"
    exit 1
fi

# 自动计算每 executor 并发数 (总并发 ≈ 8)
CONCURRENT=$((8 / N_EXEC))
[[ $CONCURRENT -lt 1 ]] && CONCURRENT=1

TOTAL_TASKS=$((N_EXEC * CONCURRENT))

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BENCH="$PROJECT_DIR/target/release/examples/bench_region_cluster_client"
SCHED="$PROJECT_DIR/target/release/examples/distributed_compute_scheduler"
EXEC="$PROJECT_DIR/target/release/examples/distributed_compute_executor"
SO="$PROJECT_DIR/target/release/libregion_cluster_processor.so"

R='\033[0;31m'; G='\033[0;32m'; Y='\033[1;33m'; N='\033[0m'
ok()   { echo -e "${G}[$(date +%H:%M:%S)]${N} $*"; }
warn() { echo -e "${Y}[$(date +%H:%M:%S)]${N} $*"; }
fail() { echo -e "${R}[$(date +%H:%M:%S)]${N} $*"; }

# ============================================================
# 编译检查
# ============================================================
check_binaries() {
    for bin in "$BENCH" "$SCHED" "$EXEC" "$SO"; do
        if [[ ! -f "$bin" ]]; then
            fail "缺少二进制: $bin"
            fail "请先执行: cargo build --release -p region_cluster_processor && cargo build --release --examples"
            exit 1
        fi
    done
    ok "二进制文件检查通过"
}

# ============================================================
# 环境清理
# ============================================================
clean_all() {
    ok "=== 环境清理 ==="
    local pids=$(ps aux | grep distributed_compute | grep -v grep | awk '{print $2}' || true)
    if [ -n "$pids" ]; then
        warn "残留进程: $pids"
        for pid in $pids; do kill -9 $pid 2>/dev/null || true; done
        sleep 3
    fi

    local containers=$(docker ps -aq --filter "name=minio" 2>/dev/null || true)
    if [ -n "$containers" ]; then
        warn "残留容器: $containers"
        echo "$containers" | xargs -r docker stop 2>/dev/null || true
        echo "$containers" | xargs -r docker rm 2>/dev/null || true
    fi

    docker network rm bench-net 2>/dev/null || true
    rm -rf /tmp/bench-data

    # 确认端口全部释放
    sleep 2
    for port in 50050 50051 50052 50053 50054 50055 50056 50057 50058 9000; do
        local pid=$(ss -tlnp 2>/dev/null | grep ":$port " | grep -oP 'pid=\K\d+' || true)
        if [ -n "$pid" ]; then
            warn "端口 $port 仍被 pid=$pid 占用，强制释放"
            kill -9 $pid 2>/dev/null || true
        fi
    done
    sleep 1
    ok "环境清理完成"
}

assert_clean() {
    local leftover=$(ps aux | grep distributed_compute | grep -v grep | wc -l)
    [[ "$leftover" -eq 0 ]] || { fail "仍有 $leftover 个 distributed_compute 进程残留"; exit 1; }
    for port in 50050 50051 50053 50055 50057; do
        ss -tlnp 2>/dev/null | grep -q ":$port " && { fail "端口 $port 仍被占用"; exit 1; } || true
    done
    ok "环境干净"
}

# ============================================================
# 端口校验
# ============================================================
assert_ports() {
    local desc=$1; shift; local want=$#; local got=0
    ok "  $desc (预期 $want 个)"
    for port in "$@"; do
        if ss -tlnp 2>/dev/null | grep -q ":$port "; then
            ok "    $port ✓"; got=$((got + 1))
        else
            fail "    $port ✗"
        fi
    done
    [[ "$got" -eq "$want" ]] || { fail "预期 $want 个端口，实际 $got 个"; exit 1; }
    ok "  $got/$want 就绪"
}

# ============================================================
# MinIO 启动
# ============================================================
start_minio() {
    local nodes=$1
    docker network create bench-net 2>/dev/null || true
    mkdir -p /tmp/bench-data/minio{1,2,3,4}

    if [[ "$nodes" -eq 1 ]]; then
        ok "启动 MinIO 单节点..."
        docker run -d --name minio --network bench-net \
            -p 9000:9000 -p 9001:9001 \
            -v /tmp/bench-data/minio1:/data \
            -e MINIO_ROOT_USER=MINIO -e MINIO_ROOT_PASSWORD=MINIOSECRET \
            quay.io/minio/minio server /data \
            --address ":9000" --console-address ":9001" > /dev/null
        sleep 5
    else
        ok "启动 MinIO ${nodes} 节点集群..."
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
        if [[ "$up" -ne "$nodes" ]]; then
            fail "MinIO 容器: 预期 $nodes 实际 $up"
            docker ps -a --filter "name=minio" --format "{{.Names}} {{.Status}}"
            for c in minio1 minio2 minio3 minio4; do
                echo "--- $c ---"; docker logs "$c" 2>&1 | tail -3
            done
            exit 1
        fi
    fi

    python3 -c "
from minio import Minio
c=Minio('localhost:9000',access_key='MINIO',secret_key='MINIOSECRET',secure=False)
if not c.bucket_exists('ballista'): c.make_bucket('ballista')
" && ok "  MinIO + bucket 就绪" || { fail "MinIO 启动失败"; exit 1; }
}

# ============================================================
# Executor 启动
# ============================================================
start_executors() {
    local n=$1; local label=$2; local ports=""

    # 构建端口列表
    for i in $(seq 1 $n); do
        local flight=$((50050 + i * 2 - 1))
        local grpc=$((50050 + i * 2))
        ports="$ports $flight"
    done

    ok "启动 $n 个 Executor (各 $CONCURRENT 并发, 总并发 $TOTAL_TASKS)..."
    for i in $(seq 1 $n); do
        local flight=$((50050 + i * 2 - 1))
        local grpc=$((50050 + i * 2))
        "$EXEC" -p $flight --bind-grpc-port $grpc -c $CONCURRENT > /tmp/exec_${label}_${i}.log 2>&1 &
    done
    sleep $((4 + n * 2))

    assert_ports "Executor" $ports
    echo "$ports"  # return for monitoring
}

# ============================================================
# 压测 + 运行中监控
# ============================================================
run_bench() {
    local label=$1; shift; local ports="$@"
    local logfile="/tmp/bench_${label}.log"

    ok "===== 开始压测: $label ====="
    ok "  配置: ${N_EXEC} Executor × ${CONCURRENT} 并发, ${REGIONS} regions, ${JSON_SIZE}B JSON"

    # 后台进程监控
    (
        local start=$(date +%s)
        while true; do
            sleep 2
            local dead=""; local alive=0
            for port in $ports; do
                if ss -tlnp 2>/dev/null | grep -q ":$port "; then
                    alive=$((alive + 1))
                else
                    dead="$dead $port"
                fi
            done
            if [ -n "$dead" ]; then
                local elapsed=$(($(date +%s) - start))
                fail "⚠ T+${elapsed}s Executor 端口下线:$dead (存活: $alive)"
            fi
        done
    ) &
    local mon_pid=$!

    MAP_PARTITION_SO="$SO" "$BENCH" -r $REGIONS -j $JSON_SIZE 2>&1 | tee "$logfile"
    local bench_rc=${PIPESTATUS[0]}

    kill $mon_pid 2>/dev/null || true; wait $mon_pid 2>/dev/null || true

    # 运行后状态
    ok "  运行后 Executor 状态:"
    local alive=0
    for port in $ports; do
        if ss -tlnp 2>/dev/null | grep -q ":$port "; then
            alive=$((alive + 1))
        else
            fail "    $port ✗ 已下线"
        fi
    done
    ok "  $alive/$N_EXEC 存活"

    # 结果解析
    if [[ "$bench_rc" -ne 0 ]]; then
        fail "压测进程退出码: $bench_rc"
        grep -i "error" "$logfile" | tail -5 || true
        return 1
    fi

    if grep -q "无 CROSS_REGION_ERROR" "$logfile"; then
        ok "✅ 无 CROSS_REGION_ERROR"
    else
        fail "❌ 检测到 CROSS_REGION_ERROR"
    fi

    local tm=$(grep "分布式计算耗时" "$logfile" | awk -F': ' '{print $2}' | tr -d 's')
    local tp=$(grep "吞吐量" "$logfile" | awk -F': ' '{print $2}')
    local rows=$(grep "输出行数" "$logfile" | head -1 | awk -F': ' '{print $2}')
    ok "  耗时: ${tm}s | 吞吐量: ${tp} | 输出: ${rows} 行"
    ok "  详细日志: $logfile"
    echo ""
}

# ============================================================
# 主流程
# ============================================================
main() {
    check_binaries
    clean_all
    assert_clean

    local label="${N_EXEC}x${CONCURRENT}"
    local minio_nodes=1
    [[ $N_EXEC -ge 2 ]] && minio_nodes=4

    ok "============================================================"
    ok "  ${N_EXEC} Executor × ${CONCURRENT} 并发  +  ${minio_nodes} MinIO"
    ok "  数据: ${REGIONS} regions × 4KB JSON = $((REGIONS * 100 * 1000)) 行"
    ok "============================================================"

    start_minio $minio_nodes

    ok "启动 Scheduler..."
    "$SCHED" > /tmp/sched_${label}.log 2>&1 &
    sleep 3
    assert_ports "Scheduler" 50050

    local ports=$(start_executors $N_EXEC $label)

    run_bench "$label" $ports

    # 汇总
    ok "========== 结果 =========="
    printf "  %-15s %10s %12s\n" "配置" "耗时" "吞吐量"
    local tm=$(grep "分布式计算耗时" /tmp/bench_${label}.log | awk -F': ' '{print $2}' | tr -d 's')
    local tp=$(grep "吞吐量" /tmp/bench_${label}.log | awk -F': ' '{print $2}')
    printf "  %-15s %10ss %12s\n" "$label" "$tm" "$tp"
    ok "完成。"
}

main
