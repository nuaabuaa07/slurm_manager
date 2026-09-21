#!/bin/bash
#SBATCH --job-name=vllm-propella
#SBATCH --partition=5090-eth-2
#SBATCH --nodelist=msxf-hpc-37-21-ai
#SBATCH --nodes=1
#SBATCH --account=aidata
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=30
#SBATCH --gres=gpu:8
#SBATCH --time=15-00:00:00
#SBATCH --output=log/dj-slurm-%j-%t.log
#SBATCH --error=log/dj-slurm-%j-%t.err

set -e

echo "=== 作业开始时间: $(date) ==="
echo "分配的节点: $SLURM_JOB_NODELIST"

# 修改1：部署 Propella
export MODEL_PATH="/workspace/models/propella-1-1.7b"
export MODEL_NAME="propella-1-1.7b"


export SIF_FILE="/share/global/datajuicer/sif/vllm-xww.sif"

mkdir -p log

# 以 Job ID 为根目录，每次提交完全隔离
JOB_CACHE_DIR="/share/global/datajuicer/.cache/vllm/job-${SLURM_JOB_ID}"
JOB_LOG_DIR="/share/global/datajuicer/logs/job-${SLURM_JOB_ID}"

rm -rf "${JOB_CACHE_DIR}"
rm -rf "${JOB_LOG_DIR}"

mkdir -p "${JOB_CACHE_DIR}"
mkdir -p "${JOB_LOG_DIR}"

export SLURM_JOB_ID

echo "[$(date)] Starting 8 independent single-GPU vLLM instances per node..."

srun --overlap --export=ALL bash -c '
    set -e

    NODE_ID=$SLURM_PROCID
    HOSTNAME=$(hostname)
    IP=$(hostname -I | awk "{print \$1}")

    echo "[$(date)] Node $NODE_ID ($HOSTNAME, $IP) starting 8 independent instances (ports 8000-8007)..."

    singularity exec \
        --cleanenv \
        --nv \
        --writable-tmpfs \
        -B /share/global/datajuicer/.cache/vllm/job-${SLURM_JOB_ID}:/workspace/.cache \
        -B /share/aidev/data:/workspace/data \
        -B /share/global/datajuicer:/workspace/datajuicer \
        -B /share/global/models:/workspace/models \
        --env MODEL_PATH=${MODEL_PATH} \
        --env SLURM_PROCID=${SLURM_PROCID} \
        --env SLURM_JOB_ID=${SLURM_JOB_ID} \
        --env MODEL_NAME=${MODEL_NAME} \
        ${SIF_FILE} \
        bash -c '"'"'
            set -e

            PYTHON_CMD=$(command -v python3 || command -v python)
            HOSTNAME=$(hostname)

            LOG_DIR="/workspace/datajuicer/logs/job-${SLURM_JOB_ID}"

            mkdir -p "${LOG_DIR}"
            chmod 755 "${LOG_DIR}"

            # 验证目录可写
            if [ ! -w "${LOG_DIR}" ]; then
                echo "ERROR: Cannot write to ${LOG_DIR}, using /tmp"

                LOG_DIR="/tmp/vllm-logs/job-${SLURM_JOB_ID}"
                mkdir -p "${LOG_DIR}"
            fi

            echo "[$(date)] Starting 8 instances on ${HOSTNAME}..."

            # 修改2：使用数组保存每个实例的 PID
            declare -a PIDS

            for i in {0..7}; do
                PORT=$((8000 + i))

                # 每个实例使用独立的 vLLM 内部端口
                VLLM_PORT=$((20000 + SLURM_PROCID * 8 + i))

                # 每个实例使用独立缓存目录
                INSTANCE_CACHE="/workspace/.cache/node${SLURM_PROCID}/gpu${i}"

                mkdir -p "${INSTANCE_CACHE}/tmp"
                mkdir -p "${INSTANCE_CACHE}/torch"
                mkdir -p "${INSTANCE_CACHE}/triton"

                LOG_FILE="${LOG_DIR}/vllm-gpu${i}-${HOSTNAME}.log"

                nohup env \
                    CUDA_VISIBLE_DEVICES=$i \
                    VLLM_PORT=${VLLM_PORT} \
                    TMPDIR=${INSTANCE_CACHE}/tmp \
                    VLLM_CACHE_ROOT=${INSTANCE_CACHE} \
                    TORCH_HOME=${INSTANCE_CACHE}/torch \
                    TRITON_CACHE_DIR=${INSTANCE_CACHE}/triton \
                    ${PYTHON_CMD} -m vllm.entrypoints.openai.api_server \
                    --model "${MODEL_PATH}" \
                    --served-model-name "${MODEL_NAME}" \
                    --tensor-parallel-size 1 \
                    --dtype bfloat16 \
                    --hf-overrides "{\"torch_dtype\":\"bfloat16\"}" \
                    --port ${PORT} \
                    --host 0.0.0.0 \
                    --gpu-memory-utilization 0.8 \
                    --max-model-len 16384 \
                    --max-num-seqs 512 \
                    --enable-prefix-caching \
                    > "${LOG_FILE}" 2>&1 &

                PID=$!

                PIDS[$i]=$PID

                echo "[$(date)] Instance $i (GPU $i, Port $PORT, VLLM_PORT $VLLM_PORT, CACHE $INSTANCE_CACHE, PID $PID) started -> ${LOG_FILE}"

                sleep 3
            done

            echo "[$(date)] Waiting for all instances to be ready..."

            ALL_READY=1

            for i in {0..7}; do
                PORT=$((8000 + i))
                PID=${PIDS[$i]}
                LOG_FILE="${LOG_DIR}/vllm-gpu${i}-${HOSTNAME}.log"

                # ready 状态：
                #   0 = 仍在启动
                #   1 = 启动成功
                #  -1 = 进程真正退出
                ready=0

                for attempt in {1..600}; do
                    # 保留成功样例原来的启动成功标志
                    if grep -q "Application startup complete" \
                        "${LOG_FILE}" 2>/dev/null; then

                        echo "[$(date)] ✅ Instance $i (port $PORT, PID $PID) ready"

                        ready=1
                        break
                    fi

                    # 修改3：只通过 PID 判断进程是否真正退出
                    # 不再因为日志中出现 ERROR 或 Traceback 就判断崩溃
                    if ! kill -0 "${PID}" 2>/dev/null; then
                        echo "[$(date)] ❌ Instance $i really exited (port $PORT, PID $PID):"
                        tail -n 100 "${LOG_FILE}" 2>/dev/null || true

                        ready=-1
                        ALL_READY=0
                        break
                    fi

                    # 每 30 秒输出一次启动进度
                    if (( attempt % 15 == 0 )); then
                        WAIT_SECONDS=$((attempt * 2))

                        echo "[$(date)] Instance $i (port $PORT, PID $PID) is still starting, waited ${WAIT_SECONDS}s..."
                    fi

                    sleep 2
                done

                # 循环结束后仍然是 0，说明进程存在，但健康启动超时
                if [ "${ready}" -eq 0 ]; then
                    echo "[$(date)] ❌ Instance $i failed to become ready after 1200s"
                    tail -n 100 "${LOG_FILE}" 2>/dev/null || true

                    ALL_READY=0
                fi
            done

            if [ "${ALL_READY}" -eq 1 ]; then
                echo "[$(date)] ✅ All 8 instances are running on ${HOSTNAME}"
                echo "[$(date)] Service IP: ${IP}"
                echo "[$(date)] Ports: 8000-8007"
            else
                echo "[$(date)] ⚠️ Some instances failed on ${HOSTNAME}, surviving instances continue running"
            fi

            # 持续等待后台进程
            while true; do
                wait -n 2>/dev/null || true

                echo "[$(date)] WARNING: One or more instances have exited on ${HOSTNAME}. Sleeping 30s..."

                sleep 30
            done
        '"'"'
'

echo "[$(date)] Job completed"

