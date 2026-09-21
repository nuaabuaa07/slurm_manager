#!/bin/bash
#SBATCH --job-name=vllm-qwen-tp
#SBATCH --partition=5090-eth-2
#SBATCH --nodelist=msxf-hpc-31-18-ai,msxf-hpc-31-19-ai,msxf-hpc-31-20-ai,msxf-hpc-31-21-ai,msxf-hpc-36-13-ai,msxf-hpc-37-18-ai,msxf-hpc-37-21-ai
#SBATCH --nodes=7
#SBATCH --account=aidata
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=20
#SBATCH --gres=gpu:8
#SBATCH --output=log/dj-slurm-%j-%t.log
#SBATCH --error=log/dj-slurm-%j-%t.err

set -e

echo "=== 作业开始时间: $(date) ==="
echo "分配的节点: $SLURM_JOB_NODELIST"

export MODEL_PATH="/workspace/models/Qwen3.6-27B-NVFP4"
export MODEL_NAME="Qwen3.6-27B-NVFP4"
export SIF_FILE="/share/global/datajuicer/sif/vllm-v0.23.0.sif"
# 
# 
mkdir -p log

# 清除本 job 的旧缓存和日志，并重新创建目录
JOB_CACHE_DIR="/share/global/datajuicer/.cache/vllm/job-${SLURM_JOB_ID}"
JOB_LOG_DIR="/share/global/datajuicer/logs/job-${SLURM_JOB_ID}"

rm -rf "${JOB_CACHE_DIR}"
rm -rf "${JOB_LOG_DIR}"
mkdir -p "${JOB_CACHE_DIR}"
mkdir -p "${JOB_LOG_DIR}"

export SLURM_JOB_ID

# 获取节点列表（SLURM_JOB_NODELIST 由 SLURM 自动设置）
if [ -n "$SLURM_JOB_NODELIST" ]; then
    NODE_LIST=$(echo "$SLURM_JOB_NODELIST" | tr ',' ' ')
else
    # 备用：从 scontrol 获取（正常情况下不会执行）
    NODE_LIST=$(scontrol show job $SLURM_JOB_ID | grep -oP 'NodeList=\K[^ ]+' | tr ',' ' ')
fi

echo "Available endpoints (port 8400-8403 per node):"
for node in $NODE_LIST; do
    # 节点名格式: msxf-hpc-X-Y-ai  -> 提取 X 和 Y
    X=$(echo "$node" | cut -d'-' -f3)
    Y=$(echo "$node" | cut -d'-' -f4 | sed 's/-ai//')
    IP="10.112.${X}.${Y}"
    for port in {8600..8603}; do
        # 按您要求的格式输出：带双引号和逗号
        url="\"http://${IP}:${port}/v1/chat/completions\","
        echo "$url"
    done
done

echo "[$(date)] Starting vLLM instances per node..."

srun --export=ALL bash -c '
    set -e

    NODE_ID=$SLURM_PROCID
    HOSTNAME=$(hostname)
    IP=$(hostname -I | awk "{print \$1}")

    echo "[$(date)] Node $NODE_ID ($HOSTNAME) ($IP) starting"

    # unset LD_LIBRARY_PATH
    INSTANCE_NAME="vllm-${SLURM_JOB_ID}-${NODE_ID}"

    singularity instance start \
        --cleanenv \
        --nv \
        --writable-tmpfs \
        -B /share/global/datajuicer/.cache/vllm/job-${SLURM_JOB_ID}:/workspace/.cache \
        -B /share/aidev/data:/workspace/data \
        -B /share/global/datajuicer:/workspace/datajuicer \
        -B /share/global/models:/workspace/models \
        --env TMPDIR=/workspace/.cache/tmp/node${SLURM_PROCID} \
        --env VLLM_CACHE_ROOT=/workspace/.cache/node${SLURM_PROCID} \
        --env MODEL_PATH=${MODEL_PATH} \
        --env SLURM_PROCID=${SLURM_PROCID} \
        --env SLURM_JOB_ID=${SLURM_JOB_ID} \
        --env MODEL_NAME=${MODEL_NAME} \
        ${SIF_FILE} \
        ${INSTANCE_NAME}
    # 等待实例就绪
    sleep 2

    echo "[$(date)] Starting vLLM instances inside container..."

    singularity exec instance://${INSTANCE_NAME} bash -c '"'"'
        set -e
        PYTHON_CMD=$(command -v python3 || command -v python)
        HOSTNAME=$(hostname)

        LOG_DIR="/workspace/datajuicer/logs/job-${SLURM_JOB_ID}"
        mkdir -p ${LOG_DIR}
        mkdir -p /workspace/.cache/tmp/node${SLURM_PROCID} #创建临时目录
        chmod 755 ${LOG_DIR}

        # 验证目录可写
        if [ ! -w "${LOG_DIR}" ]; then
            echo "ERROR: Cannot write to ${LOG_DIR}, using /tmp"
            LOG_DIR="/tmp/vllm-logs/job-${SLURM_JOB_ID}"
            mkdir -p ${LOG_DIR}
        fi

        echo "[$(date)] Starting instances on ${HOSTNAME}..."

        LOG_FILE="${LOG_DIR}/vllm.log"

        # unset LD_LIBRARY_PATH

        # pip install nvidia-nccl-cu12==2.27.3
        # VLLM_PORT=$((20000 + SLURM_PROCID * 8))
        ALLOCATED_PORTS=()

        for i in {0..3}; do
            PORT=$((8600 + i))
            while ss -a | grep -q ":${PORT} "; do
                echo "[$(date) ${HOSTNAME}] Port ${PORT} is in use, trying $((PORT+4))"
                PORT=$((PORT + 4))
            done
            # 【修复1】VLLM_PORT: 控制 vLLM 内部 TCPStore rendezvous 端口
            # vLLM v1 使用自己的 get_open_port() 从系统动态端口范围随机选取，
            # 多实例并发时极易冲突，必须用 VLLM_PORT 显式指定。
            # 规则: 20000 + 节点ID * 8 + GPU ID，全集群唯一不重复
            # Node0: 20000~20007, Node1: 20008~20015, Node2: 20016~20023
            # Node3: 20024~20031, Node4: 20032~20039
            # VLLM_PORT=$((20000 + SLURM_PROCID * 8 * 3 + i * 3))
            # echo "[$(date) ${HOSTNAME}] Allocated dynamic VLLM_PORT=${VLLM_PORT} for instance $i"


            # 【修复2】为每个实例分配独立的缓存目录，避免 torch.compile
            # 临时文件竞争（多实例共享缓存目录时会出现 FileNotFoundError）
            # 路径包含 Job ID，确保重新提交 Job 时不会与旧 Job 残留缓存冲突
            INSTANCE_CACHE="/workspace/.cache/node${SLURM_PROCID}/gpu${i}"
            mkdir -p "${INSTANCE_CACHE}/tmp"
            mkdir -p "${INSTANCE_CACHE}/torch"
            mkdir -p "${INSTANCE_CACHE}/triton"

            LOG_FILE="${LOG_DIR}/vllm-gpu${i}-${HOSTNAME}.log"

            # VLLM_PORT=${VLLM_PORT} \
            nohup env \
                CUDA_VISIBLE_DEVICES=$((2*i)),$((2*i+1)) \
                TMPDIR=${INSTANCE_CACHE}/tmp \
                VLLM_CACHE_ROOT=${INSTANCE_CACHE} \
                TORCH_HOME=${INSTANCE_CACHE}/torch \
                TRITON_CACHE_DIR=${INSTANCE_CACHE}/triton \
                ${PYTHON_CMD} -m vllm.entrypoints.openai.api_server \
                --model "${MODEL_PATH}" \
                --served-model-name "${MODEL_NAME}" \
                --tensor-parallel-size 2 \
                --dtype bfloat16 \
                --port ${PORT} \
                --host 0.0.0.0 \
                --gpu-memory-utilization 0.95 \
                --max-model-len 16384 \
                --max-num-seqs 128 \
                --enable-prefix-caching \
                --default-chat-template-kwargs "{\"enable_thinking\": false}"\
                > "${LOG_FILE}" 2>&1 &

            PID=$!
            PIDS="${PIDS} ${PID}"
            echo "[$(date)] Instance $i (GPU $i, Port $PORT, VLLM_PORT $VLLM_PORT, CACHE $INSTANCE_CACHE, PID $PID) started -> ${LOG_FILE}"
            sleep 120
        done

        echo "[$(date)] Waiting for all instances to be ready..."

        ALL_READY=1
        for i in {0..3}; do
            PORT=$((8600 + i))
            LOG_FILE="${LOG_DIR}/vllm-gpu${i}-${HOSTNAME}.log"
            ready=0

            for attempt in {1..600}; do
                if grep -q "Application startup complete" "${LOG_FILE}" 2>/dev/null; then
                    echo "[$(date) ${HOSTNAME}] ✅ Instance $i (port $PORT) ready"
                    ready=1
                    break
                fi

                if grep -qE "(Error|ERROR|Traceback)" "${LOG_FILE}" 2>/dev/null; then
                    echo "[$(date) ${HOSTNAME}] ❌ Instance $i crashed:"
                    tail -n 20 "${LOG_FILE}"
                    ALL_READY=0
                    break
                fi

                sleep 2
            done

            if [ $ready -eq 0 ] && [ $ALL_READY -eq 1 ]; then
                echo "[$(date)] ❌ Instance $i failed to start after 1200s"
                ALL_READY=0
            fi
        done

        if [ $ALL_READY -eq 1 ]; then
            echo "[$(date)] ✅ All 8 instances are running on ${HOSTNAME}"
        else
            echo "[$(date)] ⚠️  Some instances failed on ${HOSTNAME}, surviving instances continue running"
        fi

        # 持续等待所有后台进程，任意一个退出则打印警告
        while true; do
            wait -n 2>/dev/null || true
            echo "[$(date)] WARNING: One or more instances have exited on ${HOSTNAME}. Sleeping 30s..."
            sleep 30
        done
    '"'"'
'

# nohup ${PYTHON_CMD} -m vllm.entrypoints.openai.api_server \
#                 --model "${MODEL_PATH}" \
#                 --served-model-name "${MODEL_NAME}" \
#                 --data-parallel-size 4\
#                 --tensor-parallel-size 2 \
#                 --dtype bfloat16 \
#                 --port 8000 \
#                 --gpu-memory-utilization 0.8 \
#                 --max-model-len 16384 \
#                 --max-num-seqs 128 \
#                 --enable-prefix-caching \
#                 > "${LOG_FILE}" 2>&1 &

echo "[$(date)] Job completed"
