#!/bin/bash
#SBATCH --job-name=vllm-qwen-tp
#SBATCH --partition=5090-eth-2
#SBATCH --nodelist=msxf-hpc-31-18-ai,msxf-hpc-31-19-ai,msxf-hpc-31-20-ai,msxf-hpc-31-21-ai,msxf-hpc-36-13-ai,msxf-hpc-37-18-ai,msxf-hpc-37-21-ai
#SBATCH --nodes=7
#SBATCH --account=aidata
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=70
#SBATCH --gres=gpu:8
#SBATCH --time=15-00:00:00
#SBATCH --output=log/dj-slurm-%j-%t.log
#SBATCH --error=log/dj-slurm-%j-%t.err

set -e

echo "=== 作业开始时间: $(date) ==="
echo "分配的节点: $SLURM_JOB_NODELIST"


export MODEL_PATH="/workspace/models/Qwen2.5-14B-Instruct"
export MODEL_NAME="Qwen2.5-14B-Instruct"
export SIF_FILE="/share/global/datajuicer/sif/vllm-rtx5090_TPDP.sif"

mkdir -p log

# 清除本 job 的旧缓存和日志，并重新创建目录
JOB_CACHE_DIR="/share/global/datajuicer/.cache/vllm/job-${SLURM_JOB_ID}"
JOB_LOG_DIR="/share/global/datajuicer/logs/job-${SLURM_JOB_ID}"

rm -rf "${JOB_CACHE_DIR}"
rm -rf "${JOB_LOG_DIR}"
mkdir -p "${JOB_CACHE_DIR}"
mkdir -p "${JOB_LOG_DIR}"

export SLURM_JOB_ID

echo "[$(date)] Starting vLLM instances per node..."

srun --export=ALL bash -c '
    set -e

    NODE_ID=$SLURM_PROCID
    HOSTNAME=$(hostname)
    IP=$(hostname -I | awk "{print \$1}")

    echo "[$(date)] Node $NODE_ID ($HOSTNAME) ($IP) starting"

    # unset LD_LIBRARY_PATH

    singularity exec \
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
        bash -c '"'"'
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


            ${PYTHON_CMD} -m vllm.entrypoints.openai.api_server \
                --model "${MODEL_PATH}" \
                --served-model-name "${MODEL_NAME}" \
                --data-parallel-size 4\
                --tensor-parallel-size 2 \
                --port 8010 \
                --gpu-memory-utilization 0.95 \
                --max-model-len 14336 \
                --max-num-seqs 128 \
                --enable-prefix-caching \
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
