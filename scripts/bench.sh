#!/bin/bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BENCH="$PROJECT_DIR/target/release/examples/bench_region_cluster_client"
SCHED="$PROJECT_DIR/target/release/examples/distributed_compute_scheduler"
EXEC="$PROJECT_DIR/target/release/examples/distributed_compute_executor"
SO="$PROJECT_DIR/target/release/libregion_cluster_processor.so"

R='\033[0;31m'; G='\033[0;32m'; Y='\033[1;33m'; N='\033[0m'
ok()   { echo -e "${G}[$(date +%H:%M:%S)]${N} $*"; }
warn() { echo -e "${Y}[$(date +%H:%M:%S)]${N} $*"; }
fail() { echo -e "${R}[$(date +%H:%M:%S)]${N} $*"; }

# ────────────────────────────────────────────────────────────
# 环境清理：杀掉一切，不留残留
# ────────────────────────────────────────────────────────────
clean_all() {
    ok "=== 环境清理 ==="

    # 杀掉所有 distrubuted_compute 进程
    local pids=$(ps aux | grep distributed_compute | grep -v grep | awk '{print $2}' || true)
    if [ -n "$pids" ]; then
        warn "发现残留进程: $pids，强制清理..."
        for pid in $pids; do kill -9 $pid 2>/dev/null || true; done
        sleep 3
    fi

    # 停掉所有 minio 容器
    local containers=$(docker ps -aq --filter "name=minio" 2>/dev/null || true)
    if [ -n "$containers" ]; then
        warn "发现残留容器: $containers，强制清理..."
        echo "$containers" | xargs -r docker stop 2>/dev/null || true
        echo "$containers" | xargs -r docker rm 2>/dev/null || true
    fi

    # 删除测试用网络 + 清理 MinIO 数据目录
    docker network rm bench-net 2>/dev/null || true
    rm -rf /tmp/bench-data

    # 确认端口释放
    sleep 2
    for port in 50050 50051 50052 50053 50054 50055 50056 50057 50058 9000; do
        local pid=$(ss -tlnp 2>/dev/null | grep ":$port " | grep -oP 'pid=\K\d+' || true)
        if [ -n "$pid" ]; then
            warn "端口 $port 仍被 pid=$pid 占用，强制释放..."
            kill -9 $pid 2>/dev/null || true
        fi
    done
    sleep 1

    ok "环境清理完成"
}

# ────────────────────────────────────────────────────────────
# 校验：进程数 / 端口数 / 无残留
# ────────────────────────────────────────────────────────────
assert_clean() {
    local leftover=$(ps aux | grep distributed_compute | grep -v grep | wc -l)
    if [ "$leftover" -ne 0 ]; then
        fail "仍有 $leftover 个 distributed_compute 进程残留！"
        ps aux | grep distributed_compute | grep -v grep
        exit 1
    fi
    for port in 50050 50051 50053 50055 50057; do
        if ss -tlnp 2>/dev/null | grep -q ":$port "; then
            fail "端口 $port 仍被占用！"
            exit 1
        fi
    done
    ok "环境干净，无残留"
}

assert_ports() {
    local desc=$1; shift
    ok "  校验 $desc..."
    local want=$#
    local got=0
    for port in "$@"; do
        if ss -tlnp 2>/dev/null | grep -q ":$port "; then
            ok "    $port ✓"
            got=$((got + 1))
        else
            fail "    $port ✗"
        fi
    done
    if [ "$got" -ne "$want" ]; then
        fail "  预期 $want 个端口，实际 $got 个"
        exit 1
    fi
    ok "  $got/$want 端口就绪"
}

# ────────────────────────────────────────────────────────────
# 测试 1：1 Executor × 8 + 1 MinIO 单节点
# ────────────────────────────────────────────────────────────
test_1x8_1minio() {
    local label="1x8_1minio"
    ok "===== 测试 1: 1 Executor × 8 并发 + 1 MinIO 单节点 ====="
    clean_all
    assert_clean

    # --- MinIO 单节点 ---
    ok "启动 MinIO 单节点..."
    docker network create bench-net 2>/dev/null || true
    docker run -d --name minio --network bench-net \
        -p 9000:9000 -p 9001:9001 \
        -v /tmp/bench-data/minio1:/data \
        -e MINIO_ROOT_USER=MINIO -e MINIO_ROOT_PASSWORD=MINIOSECRET \
        quay.io/minio/minio server /data \
        --address ":9000" --console-address ":9001" > /dev/null
    sleep 5

    python3 -c "
from minio import Minio
c=Minio('localhost:9000',access_key='MINIO',secret_key='MINIOSECRET',secure=False)
if not c.bucket_exists('ballista'): c.make_bucket('ballista')
" && ok "  MinIO + bucket 就绪" || { fail "MinIO 启动失败"; exit 1; }

    # --- Scheduler ---
    ok "启动 Scheduler..."
    "$SCHED" > /tmp/sched_${label}.log 2>&1 &
    sleep 3
    assert_ports "Scheduler" 50050

    # --- Executor ---
    ok "启动 1 Executor (8 并发)..."
    "$EXEC" -p 50051 --bind-grpc-port 50052 -c 8 > /tmp/exec_${label}.log 2>&1 &
    sleep 5
    assert_ports "Executor" 50051

    # --- 压测 ---
    run_bench "$label" "50051"
}

# ────────────────────────────────────────────────────────────
# 测试 2：4 Executors × 2 + 4 MinIO 集群
# ────────────────────────────────────────────────────────────
test_4x2_4minio() {
    local label="4x2_4minio"
    ok "===== 测试 2: 4 Executor × 2 并发 + 4 MinIO 集群 ====="
    clean_all
    assert_clean

    # --- MinIO 4 节点分布式集群 ---
    ok "启动 MinIO 4 节点集群..."
    docker network create bench-net 2>/dev/null || true
    mkdir -p /tmp/bench-data/minio{1,2,3,4}

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

    # 校验 4 个容器都在运行，如果少了打印详细信息
    local minio_up=$(docker ps --filter "name=minio" --format "{{.Names}}" | wc -l)
    if [ "$minio_up" -ne 4 ]; then
        fail "MinIO 容器: 预期 4 实际 $minio_up"
        docker ps -a --filter "name=minio" --format "{{.Names}} {{.Status}}"
        for c in minio1 minio2 minio3 minio4; do
            docker logs "$c" 2>&1 | tail -3
        done
        exit 1
    fi
    ok "  4 个 MinIO 容器全部运行"

    python3 -c "
from minio import Minio
c=Minio('localhost:9000',access_key='MINIO',secret_key='MINIOSECRET',secure=False)
if not c.bucket_exists('ballista'): c.make_bucket('ballista')
" && ok "  MinIO 集群 + bucket 就绪" || { fail "MinIO 集群失败"; exit 1; }

    # --- Scheduler ---
    ok "启动 Scheduler..."
    "$SCHED" > /tmp/sched_${label}.log 2>&1 &
    sleep 3
    assert_ports "Scheduler" 50050

    # --- Executors ---
    ok "启动 4 Executors (各 2 并发)..."
    for i in 1 2 3 4; do
        local flight=$((50050 + i * 2 - 1))
        local grpc=$((50050 + i * 2))
        "$EXEC" -p $flight --bind-grpc-port $grpc -c 2 > /tmp/exec_${label}_${i}.log 2>&1 &
    done
    sleep 8
    assert_ports "Executor" 50051 50053 50055 50057

    # --- 压测 ---
    run_bench "$label" "50051 50053 50055 50057"
}

# ────────────────────────────────────────────────────────────
# 运行压测
# ────────────────────────────────────────────────────────────
run_bench() {
    local label=$1
    local ports=$2  # space-separated list of executor ports to monitor
    local logfile="/tmp/bench_${label}.log"
    ok "===== 开始压测: $label ====="

    # 后台进程监控：每 2 秒校验 executor 端口是否存活
    (
        while true; do
            sleep 2
            local dead=""
            for port in $ports; do
                if ! ss -tlnp 2>/dev/null | grep -q ":$port "; then
                    dead="$dead $port"
                fi
            done
            if [ -n "$dead" ]; then
                fail "⚠ 运行时检测到 Executor 端口下线: $dead"
            fi
        done
    ) &
    local mon_pid=$!

    MAP_PARTITION_SO="$SO" "$BENCH" -r 50 -j 4096 2>&1 | tee "$logfile"

    # 停止监控
    kill $mon_pid 2>/dev/null || true
    wait $mon_pid 2>/dev/null || true

    # 运行后再次校验
    ok "  运行后校验 Executor 状态..."
    local alive=0
    for port in $ports; do
        if ss -tlnp 2>/dev/null | grep -q ":$port "; then
            alive=$((alive + 1))
        else
            fail "    $port ✗ 已下线"
        fi
    done
    ok "  $alive 台 Executor 存活"

    # 提取关键结果
    local ok_str=$(grep "无 CROSS_REGION_ERROR" "$logfile" | wc -l)
    if [ "$ok_str" -ge 1 ]; then
        ok "✅ 正确性: 无 CROSS_REGION_ERROR"
    else
        fail "❌ 检测到 CROSS_REGION_ERROR!"
    fi

    local time=$(grep "分布式计算耗时" "$logfile" | awk -F': ' '{print $2}' | tr -d 's')
    local tput=$(grep "吞吐量" "$logfile" | awk -F': ' '{print $2}')
    local rows=$(grep "输出行数" "$logfile" | head -1 | awk -F': ' '{print $2}')
    ok "  耗时: ${time}s | 吞吐量: ${tput} | 输出: ${rows} 行"
    ok "  详细日志: $logfile"
    echo ""
}

# ────────────────────────────────────────────────────────────
# 主流程
# ────────────────────────────────────────────────────────────
test_1x8_1minio
test_4x2_4minio

# ────────────────────────────────────────────────────────────
# 汇总
# ────────────────────────────────────────────────────────────
ok "========== 结果汇总 =========="
printf "\n  %-30s %10s %12s\n" "配置" "耗时" "吞吐量"
printf "  %-30s %10s %12s\n" "------------------------------" "----------" "------------"
for label in 1x8_1minio 4x2_4minio; do
    t=$(grep "分布式计算耗时" /tmp/bench_${label}.log | awk -F': ' '{print $2}' | tr -d 's')
    p=$(grep "吞吐量" /tmp/bench_${label}.log | awk -F': ' '{print $2}')
    printf "  %-30s %10ss %12s\n" "$label" "$t" "$p"
done
printf "\n"
ok "全部完成。"
