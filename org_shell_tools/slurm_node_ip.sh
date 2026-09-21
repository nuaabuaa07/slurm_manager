#!/bin/bash
# ============================================================
#  slurm_node_ip.sh  —  查看 Slurm partition 节点信息 + GPU 实时利用率
#
#  用法:
#    ./slurm_node_ip.sh                                    # 所有 partition，仅表格
#    ./slurm_node_ip.sh <partition>                        # 指定 partition，仅表格
#    ./slurm_node_ip.sh <partition> --gpu                  # 表格 + 实时 GPU 利用率
#    ./slurm_node_ip.sh <partition> --gpu-health           # 表格 + GPU 掉卡检测
#    ./slurm_node_ip.sh <partition> --gpu --gpu-health     # 表格 + GPU 利用率 + 掉卡检测
#    ./slurm_node_ip.sh <partition> --list                 # 表格 + 格式化列表（所有节点）
#    ./slurm_node_ip.sh <partition> --list --idle          # 表格 + 格式化列表（仅 idle 节点）
#    ./slurm_node_ip.sh <partition> --gpu --list           # 表格 + GPU + 格式化列表
#    ./slurm_node_ip.sh -h
# ============================================================

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

usage() {
    echo -e "${BOLD}用法:${RESET}"
    echo "  $0                                    # 所有 partition，仅表格"
    echo "  $0 <partition>                        # 指定 partition，仅表格"
    echo "  $0 <partition> --gpu                  # 表格 + 实时 GPU 利用率"
    echo "  $0 <partition> --gpu-health           # 表格 + GPU 掉卡检测"
    echo "  $0 <partition> --gpu --gpu-health     # 表格 + GPU 利用率 + 掉卡检测"
    echo "  $0 <partition> --list                 # 表格 + 格式化列表（所有节点）"
    echo "  $0 <partition> --list --idle          # 表格 + 格式化列表（仅 idle 节点）"
    echo "  $0 <partition> --gpu --list           # 表格 + GPU + 格式化列表"
    echo "  $0 -h"
    exit 0
}
[[ "${1:-}" == "-h" || "${1:-}" == "--help" ]] && usage

QUERY_GPU=false
CHECK_GPU_HEALTH=false
SHOW_LIST=false
IDLE_ONLY=false
TARGET_PART=""
for arg in "$@"; do
    case "$arg" in
        --gpu)        QUERY_GPU=true ;;
        --gpu-health) CHECK_GPU_HEALTH=true ;;
        --list)       SHOW_LIST=true ;;
        --idle)       IDLE_ONLY=true ;;
        -*)           ;;
        *)            TARGET_PART="$arg" ;;
    esac
done

for cmd in sinfo scontrol srun; do
    command -v "$cmd" &>/dev/null || {
        echo -e "${RED}[错误] 未找到: $cmd${RESET}" >&2; exit 1
    }
done

# ---------- 从 scontrol 抓所有节点基础信息 ----------
echo -e "${CYAN}正在收集节点信息...${RESET}" >&2

declare -A NODE_ADDR
declare -A NODE_GPU_TOTAL
declare -A NODE_GPU_ALLOC
declare -A NODE_GPU_MODEL

cur_node=""
while IFS= read -r line; do
    if [[ "$line" =~ ^NodeName=([^[:space:]]+) ]]; then
        cur_node="${BASH_REMATCH[1]}"
        continue
    fi
    [[ -z "$cur_node" ]] && continue

    if [[ "$line" =~ NodeAddr=([^[:space:]]+) ]]; then
        NODE_ADDR["$cur_node"]="${BASH_REMATCH[1]}"
    fi
    if [[ "$line" =~ CfgTRES=([^[:space:]]+) ]]; then
        tres="${BASH_REMATCH[1]}"
        if [[ "$tres" =~ gres/gpu=([0-9]+) ]]; then
            NODE_GPU_TOTAL["$cur_node"]="${BASH_REMATCH[1]}"
        else
            : "${NODE_GPU_TOTAL[$cur_node]:=0}"
        fi
    fi
    if [[ "$line" =~ AllocTRES=([^[:space:]]*) ]]; then
        tres="${BASH_REMATCH[1]}"
        if [[ "$tres" =~ gres/gpu=([0-9]+) ]]; then
            NODE_GPU_ALLOC["$cur_node"]="${BASH_REMATCH[1]}"
        else
            : "${NODE_GPU_ALLOC[$cur_node]:=0}"
        fi
    fi
    if [[ "$line" =~ ^[[:space:]]*Gres=([^[:space:]]+) ]]; then
        gres_val="${BASH_REMATCH[1]}"
        if [[ "$gres_val" =~ gpu:([A-Za-z0-9_]+):[0-9] ]]; then
            NODE_GPU_MODEL["$cur_node"]="${BASH_REMATCH[1]}"
        elif [[ "$gres_val" =~ gpu:([A-Za-z][A-Za-z0-9_]+) ]]; then
            NODE_GPU_MODEL["$cur_node"]="${BASH_REMATCH[1]}"
        else
            : "${NODE_GPU_MODEL[$cur_node]:=-}"
        fi
    fi
done < <(scontrol show nodes 2>/dev/null)

# ---------- 实时 GPU 利用率查询函数 ----------
fetch_gpu_util() {
    local node="$1"
    local part="$2"
    local raw="" attempt

    for attempt in 1 2 3; do
        raw=$(srun --overlap -p "$part" -w "$node" -N1 --quiet \
            bash -c "curl -sf --max-time 5 http://localhost:9400/metrics 2>/dev/null \
                     | grep '^DCGM_FI_DEV_GPU_UTIL{' \
                     | awk '{print \$NF}'" 2>/dev/null) || true
        [[ -n "$raw" ]] && break
        sleep 1
    done

    if [[ -z "$raw" ]]; then
        echo "无监控"
        return
    fi

    local sum=0 count=0 val utils=()
    while IFS= read -r val; do
        [[ "$val" =~ ^[0-9]+(\.[0-9]+)?$ ]] || continue
        sum=$(echo "$sum + $val" | bc)
        utils+=("$val")
        (( count++ )) || true
    done <<< "$raw"

    if (( count == 0 )); then
        echo "无监控"
        return
    fi

    local avg max_val=0
    avg=$(echo "scale=1; $sum / $count" | bc)
    for val in "${utils[@]}"; do
        (( $(echo "$val > $max_val" | bc) )) && max_val=$val || true
    done

    echo "${avg}%avg ${max_val}%max (${count}卡)"
}

# ---------- GPU 掉卡检测函数 ----------
# 通过 nvidia-smi 获取节点实际可见的 GPU 数量，与 Slurm 配置对比
# 输出格式：
#   "正常(N卡)"        —— 实际数量 == 配置数量
#   "掉卡!(实N/配M)"   —— 实际数量 < 配置数量（红色警告）
#   "无nvidia-smi"    —— 节点上没有 nvidia-smi
#   "无法连接"         —— srun 执行失败（drain/down 节点跳过此函数）
fetch_gpu_health() {
    local node="$1"
    local part="$2"
    local expected="$3"   # Slurm 配置的 GPU 总数

    # 没有配置 GPU 的节点跳过检测
    if [[ "$expected" == "0" || -z "$expected" ]]; then
        echo "无GPU"
        return
    fi

    local actual
    actual=$(srun --overlap -p "$part" -w "$node" -N1 --quiet \
        bash -c "command -v nvidia-smi &>/dev/null \
                 && nvidia-smi -L 2>/dev/null | grep -c '^GPU ' \
                 || echo '__no_smi__'" 2>/dev/null) || actual="__err__"

    actual=$(echo "$actual" | tr -d '[:space:]')

    case "$actual" in
        __no_smi__)
            echo "无nvidia-smi"
            ;;
        __err__|"")
            echo "无法连接"
            ;;
        *)
            if ! [[ "$actual" =~ ^[0-9]+$ ]]; then
                echo "解析失败"
            elif (( actual == expected )); then
                echo "正常(${actual}卡)"
            else
                # 掉卡：实际 < 配置（或异常多）
                echo "掉卡!(实${actual}/配${expected})"
            fi
            ;;
    esac
}

# ---------- 获取 partition 列表 ----------
if [[ -n "$TARGET_PART" ]]; then
    PARTITIONS="$TARGET_PART"
else
    PARTITIONS=$(sinfo -h -o "%R" 2>/dev/null | sort -u)
fi
[[ -z "$PARTITIONS" ]] && { echo -e "${RED}[错误] 未找到 partition。${RESET}" >&2; exit 1; }

# ---------- 主循环 ----------
for PART in $PARTITIONS; do
    declare -A NODE_STATE
    declare -A NODE_REASON

    while IFS= read -r ROW; do
        NODE_LIST=$(echo "$ROW" | awk '{print $1}')
        STATE=$(echo "$ROW"     | awk '{print $2}')
        REASON=$(echo "$ROW"    | awk '{$1=$2=""; print $0}' | xargs)
        [[ "$REASON" == "none" ]] && REASON=""
        while IFS= read -r NODE; do
            NODE_STATE["$NODE"]="$STATE"
            NODE_REASON["$NODE"]="${REASON:-}"
        done < <(scontrol show hostnames "$NODE_LIST" 2>/dev/null)
    done < <(sinfo -h -p "$PART" -o "%N %T %E" 2>/dev/null)

    mapfile -t SORTED_NODES < <(printf '%s\n' "${!NODE_STATE[@]}" | sort -V)

    # ── 并发查询：GPU 利用率 ───────────────────────────────
    declare -A NODE_UTIL
    if $QUERY_GPU; then
        echo -e "${CYAN}正在并发查询各节点 GPU 利用率...${RESET}" >&2
        declare -A PIDS_UTIL
        for NODE in "${SORTED_NODES[@]}"; do
            STATE="${NODE_STATE[$NODE]}"
            if [[ "$STATE" == drain* || "$STATE" == down* ]]; then
                NODE_UTIL["$NODE"]="-"
                continue
            fi
            ( fetch_gpu_util "$NODE" "$PART" > "/tmp/gpu_util_${NODE}.tmp" 2>/dev/null ) &
            PIDS_UTIL["$NODE"]=$!
        done
        for NODE in "${!PIDS_UTIL[@]}"; do
            wait "${PIDS_UTIL[$NODE]}" 2>/dev/null || true
            if [[ -f "/tmp/gpu_util_${NODE}.tmp" ]]; then
                NODE_UTIL["$NODE"]=$(cat "/tmp/gpu_util_${NODE}.tmp")
                rm -f "/tmp/gpu_util_${NODE}.tmp"
            else
                NODE_UTIL["$NODE"]="无监控"
            fi
        done
        echo "" >&2
    fi

    # ── 并发查询：GPU 掉卡检测 ────────────────────────────
    declare -A NODE_HEALTH
    if $CHECK_GPU_HEALTH; then
        echo -e "${CYAN}正在并发检测各节点 GPU 健康状态...${RESET}" >&2
        declare -A PIDS_HEALTH
        for NODE in "${SORTED_NODES[@]}"; do
            STATE="${NODE_STATE[$NODE]}"
            GPU_TOTAL="${NODE_GPU_TOTAL[$NODE]:-0}"
            # drain/down 节点标记为跳过，不发起 srun
            if [[ "$STATE" == drain* || "$STATE" == down* ]]; then
                NODE_HEALTH["$NODE"]="跳过(${STATE%%\**})"
                continue
            fi
            ( fetch_gpu_health "$NODE" "$PART" "$GPU_TOTAL" \
                > "/tmp/gpu_health_${NODE}.tmp" 2>/dev/null ) &
            PIDS_HEALTH["$NODE"]=$!
        done
        for NODE in "${!PIDS_HEALTH[@]}"; do
            wait "${PIDS_HEALTH[$NODE]}" 2>/dev/null || true
            if [[ -f "/tmp/gpu_health_${NODE}.tmp" ]]; then
                NODE_HEALTH["$NODE"]=$(cat "/tmp/gpu_health_${NODE}.tmp")
                rm -f "/tmp/gpu_health_${NODE}.tmp"
            else
                NODE_HEALTH["$NODE"]="无法连接"
            fi
        done
        echo "" >&2
    fi

    # ── 动态计算表格宽度 & 表头 ──────────────────────────
    echo ""

    # 根据启用的功能决定列组合
    # 固定列：PARTITION(22) NODE(28) IP(18) GPU用/总(12) 型号(14) STATE(16+color) REASON
    # 可选列：实时利用率(22)  GPU健康(16)

    if $QUERY_GPU && $CHECK_GPU_HEALTH; then
        SEP=$(printf '%.0s─' {1..148})
        printf "${BOLD}%-22s %-28s %-18s %-12s %-14s %-22s %-18s %-16s %s${RESET}\n" \
            "PARTITION" "NODE" "IP / NodeAddr" "GPU(用/总)" "型号" "实时利用率" "GPU健康" "STATE" "REASON"
    elif $QUERY_GPU; then
        SEP=$(printf '%.0s─' {1..130})
        printf "${BOLD}%-22s %-28s %-18s %-12s %-14s %-22s %-16s %s${RESET}\n" \
            "PARTITION" "NODE" "IP / NodeAddr" "GPU(用/总)" "型号" "实时利用率" "STATE" "REASON"
    elif $CHECK_GPU_HEALTH; then
        SEP=$(printf '%.0s─' {1..126})
        printf "${BOLD}%-22s %-28s %-18s %-12s %-14s %-18s %-16s %s${RESET}\n" \
            "PARTITION" "NODE" "IP / NodeAddr" "GPU(用/总)" "型号" "GPU健康" "STATE" "REASON"
    else
        SEP=$(printf '%.0s─' {1..108})
        printf "${BOLD}%-22s %-28s %-18s %-12s %-14s %-16s %s${RESET}\n" \
            "PARTITION" "NODE" "IP / NodeAddr" "GPU(用/总)" "型号" "STATE" "REASON"
    fi
    echo "$SEP"

    # ── 逐节点输出行 ─────────────────────────────────────
    PART_COL="$PART"
    for NODE in "${SORTED_NODES[@]}"; do
        ADDR="${NODE_ADDR[$NODE]:-N/A}"
        STATE="${NODE_STATE[$NODE]}"
        REASON="${NODE_REASON[$NODE]:-}"
        GPU_TOTAL="${NODE_GPU_TOTAL[$NODE]:-0}"
        GPU_ALLOC="${NODE_GPU_ALLOC[$NODE]:-0}"
        GPU_MODEL="${NODE_GPU_MODEL[$NODE]:--}"
        GPU_COL="${GPU_ALLOC}/${GPU_TOTAL}"

        # 节点状态着色
        case "$STATE" in
            idle*)        SC="${GREEN}${STATE}${RESET}" ;;
            alloc*|mix*)  SC="${CYAN}${STATE}${RESET}" ;;
            down*|drain*) SC="${RED}${STATE}${RESET}" ;;
            *)            SC="${YELLOW}${STATE}${RESET}" ;;
        esac

        # GPU 健康着色
        HEALTH_RAW="${NODE_HEALTH[$NODE]:-}"
        if $CHECK_GPU_HEALTH; then
            case "$HEALTH_RAW" in
                正常*)   HEALTH_COL="${GREEN}${HEALTH_RAW}${RESET}" ;;
                掉卡!*)  HEALTH_COL="${RED}${BOLD}${HEALTH_RAW}${RESET}" ;;
                跳过*)   HEALTH_COL="${YELLOW}${HEALTH_RAW}${RESET}" ;;
                无GPU)   HEALTH_COL="${HEALTH_RAW}" ;;
                *)       HEALTH_COL="${YELLOW}${HEALTH_RAW}${RESET}" ;;
            esac
        fi

        if $QUERY_GPU && $CHECK_GPU_HEALTH; then
            UTIL="${NODE_UTIL[$NODE]:-"-"}"
            printf "%-22s %-28s %-18s %-12s %-14s %-22s %-26b %-24b %s\n" \
                "$PART_COL" "$NODE" "$ADDR" "$GPU_COL" "$GPU_MODEL" \
                "$UTIL" "$HEALTH_COL" "$SC" "$REASON"
        elif $QUERY_GPU; then
            UTIL="${NODE_UTIL[$NODE]:-"-"}"
            printf "%-22s %-28s %-18s %-12s %-14s %-22s %-24b %s\n" \
                "$PART_COL" "$NODE" "$ADDR" "$GPU_COL" "$GPU_MODEL" \
                "$UTIL" "$SC" "$REASON"
        elif $CHECK_GPU_HEALTH; then
            printf "%-22s %-28s %-18s %-12s %-14s %-26b %-24b %s\n" \
                "$PART_COL" "$NODE" "$ADDR" "$GPU_COL" "$GPU_MODEL" \
                "$HEALTH_COL" "$SC" "$REASON"
        else
            printf "%-22s %-28s %-18s %-12s %-14s %-24b %s\n" \
                "$PART_COL" "$NODE" "$ADDR" "$GPU_COL" "$GPU_MODEL" \
                "$SC" "$REASON"
        fi
        PART_COL=""
    done
    echo "$SEP"

    # ── GPU 汇总 ─────────────────────────────────────────
    total_gpus=0; alloc_gpus=0; free_gpus=0; unavail_gpus=0; missing_gpus=0
    for NODE in "${SORTED_NODES[@]}"; do
        t="${NODE_GPU_TOTAL[$NODE]:-0}"
        a="${NODE_GPU_ALLOC[$NODE]:-0}"
        s="${NODE_STATE[$NODE]:-}"
        (( total_gpus += t )) || true
        (( alloc_gpus += a )) || true
        if [[ "$s" == drain* || "$s" == down* ]]; then
            unavail=$(( t - a ))
            (( unavail_gpus += unavail )) || true
        else
            (( free_gpus += t - a )) || true
        fi
        # 统计掉卡数量
        if $CHECK_GPU_HEALTH; then
            h="${NODE_HEALTH[$NODE]:-}"
            if [[ "$h" =~ ^掉卡!\(实([0-9]+)/配([0-9]+)\)$ ]]; then
                actual_n="${BASH_REMATCH[1]}"
                expect_n="${BASH_REMATCH[2]}"
                (( missing_gpus += expect_n - actual_n )) || true
            fi
        fi
    done

    printf "  ${BOLD}GPU 汇总:${RESET}  总量 %s  |  已分配 %s  |  真正空闲 %s  |  不可用(drain/down) %s" \
        "$total_gpus" "$alloc_gpus" "$free_gpus" "$unavail_gpus"
    if $CHECK_GPU_HEALTH && (( missing_gpus > 0 )); then
        printf "  |  ${RED}${BOLD}掉卡数量 %s${RESET}" "$missing_gpus"
    fi
    echo ""


    # 性能监控链接（仅拼接状态为 mixed 的节点）
    GRAFANA_URL="http://grafana-hpc.msxf.local/d/c8W2b01Zb/jie-dian-xiang-qing?orgId=1&from=now-1h&to=no    w&refresh=1m&var-job=node&var-hostname=msxf-xjhpc-172-1-ai"
    for NODE in "${SORTED_NODES[@]}"; do
        STATE="${NODE_STATE[$NODE]:-}"
        if [[ "$STATE" == mix* || "$STATE" == mixed* ]]; then
            GRAFANA_URL+="&var-node=${NODE}"
        fi
    done
    echo ""
    echo -e "  ${BOLD}性能监控链接:${RESET} ${GRAFANA_URL}"

    # ── 格式化列表（仅 --list 时显示）────────────────────
    if $SHOW_LIST; then

        if $IDLE_ONLY; then
            mapfile -t LIST_NODES < <(
                for NODE in "${SORTED_NODES[@]}"; do
                    [[ "${NODE_STATE[$NODE]}" == idle* ]] && echo "$NODE"
                done
            )
            IDLE_LABEL=" （仅 idle）"
        else
            LIST_NODES=("${SORTED_NODES[@]}")
            IDLE_LABEL=""
        fi

        echo ""
        echo -e "${BOLD}▶ 节点名列表（逗号分隔）${IDLE_LABEL}:${RESET}"
        ( IFS=,; echo "${LIST_NODES[*]}" )

        echo ""
        echo -e "${BOLD}▶ 节点名列表（每行一条）${IDLE_LABEL}:${RESET}"
        for NODE in "${LIST_NODES[@]}"; do
            echo "\"${NODE}\","
        done

        echo ""
        echo -e "${BOLD}▶ API URL 列表（端口 8000-8007，每行一条）${IDLE_LABEL}:${RESET}"
        for NODE in "${LIST_NODES[@]}"; do
            ADDR="${NODE_ADDR[$NODE]:-}"
            [[ -z "$ADDR" || "$ADDR" == "N/A" ]] && continue
            for PORT in {8000..8007}; do
                echo "\"http://${ADDR}:${PORT}/v1/chat/completions\","
            done
        done

        echo ""
        echo -e "${BOLD}▶ node:ip 列表（每行一条）${IDLE_LABEL}:${RESET}"
        for NODE in "${LIST_NODES[@]}"; do
            ADDR="${NODE_ADDR[$NODE]:-N/A}"
            echo "\"${NODE}:${ADDR}\""
        done

        echo ""
        echo -e "${BOLD}▶ Bash 关联数组格式${IDLE_LABEL}:${RESET}"
        for NODE in "${LIST_NODES[@]}"; do
            ADDR="${NODE_ADDR[$NODE]:-N/A}"
            printf "    [%s]=%s\n" "$NODE" "$ADDR"
        done
        echo ""
    fi

    unset NODE_STATE NODE_REASON NODE_UTIL NODE_HEALTH PIDS_UTIL PIDS_HEALTH
    declare -A NODE_STATE NODE_REASON
done
