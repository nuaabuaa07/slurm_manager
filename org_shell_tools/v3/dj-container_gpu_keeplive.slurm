#!/bin/bash

# ========== SLURM 参数配置 ==========
#SBATCH --job-name=gpu-keeplive
#SBATCH --partition=5090-eth-2
#SBATCH --nodelist=msxf-hpc-8-31-ai,msxf-hpc-8-32-ai,msxf-hpc-8-33-ai
#SBATCH --account=aidata
#SBATCH --nodes=3
#SBATCH --cpus-per-task=5
#SBATCH --mem-per-cpu=2g
#SBATCH --ntasks-per-node=1
#SBATCH --gres=gpu:8
#SBATCH --time=15-00:00:00
#SBATCH --output=log/gpu-keeplive-%j.log
#SBATCH --error=log/gpu-keeplive-%j.err

# ========== 可调参数 ==========
# 目标 GPU 利用率(百分比)，通过 忙等/睡眠 占空比实现
export GPU_UTIL_TARGET=${GPU_UTIL_TARGET:-60}
# 每个 duty-cycle 周期长度(秒)：忙 BUSY_SEC 秒，睡 (PERIOD_SEC-BUSY_SEC) 秒
export PERIOD_SEC=${PERIOD_SEC:-1.0}
# 每张卡上开几个并发 stress 进程/流，一般 1 个矩阵乘流已足够占满利用率，仅在需要更高显存占用时增加
export STREAMS_PER_GPU=${STREAMS_PER_GPU:-1}
# 单次矩阵乘的矩阵维度，越大单次kernel耗时越长、越平滑，但显存占用也越大
export MATMUL_DIM=${MATMUL_DIM:-8192}

export SIF_PATH="/data/hpc/home/zhiliang.jin/create_dockerimage/datajuicer-no-flash/data-juicer-v1107-rtx5090-modelscopebert.sif"

# ========== 环境设置和初始化 ==========
set -e
mkdir -p log

echo "=== 作业开始时间: $(date) ==="
echo "作业 ID: $SLURM_JOB_ID"
echo "分配的节点数: $SLURM_JOB_NUM_NODES"
echo "分配的节点: $SLURM_JOB_NODELIST"
echo "目标GPU利用率: ${GPU_UTIL_TARGET}%  周期: ${PERIOD_SEC}s  每卡流数: ${STREAMS_PER_GPU}  矩阵维度: ${MATMUL_DIM}"

node_list=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
echo "节点列表:"
echo "$node_list"

export SLURM_JOB_ID

srun bash -c '
    JOB_NODE_TAG="${SLURM_JOB_ID}_${SLURM_NODEID}"

    TMP_DIR=/share/global/datajuicer/ray_tmp_keeplive_${JOB_NODE_TAG}
    rm -rf "${TMP_DIR}"
    mkdir -p "${TMP_DIR}"

    TMP_WORKSPACE=/share/global/datajuicer/ray_tmp_keeplive_workspace_${JOB_NODE_TAG}
    rm -rf "${TMP_WORKSPACE}"
    mkdir -p "${TMP_WORKSPACE}"

    OVERLAY_DIR=/share/global/datajuicer/overlay_dir_keeplive_${JOB_NODE_TAG}
    rm -rf "${OVERLAY_DIR}"
    mkdir -p "${OVERLAY_DIR}/upper"
    mkdir -p "${OVERLAY_DIR}/work"

    INSTANCE_NAME="gpu-keeplive-${JOB_NODE_TAG}"

    singularity instance start -e --nv \
        --overlay "${OVERLAY_DIR}" \
        -B /dev/pts:/dev/pts \
        -B /dev/ptmx:/dev/pts/ptmx \
        -B /share/aidev/data:/share/aidev/data \
        -B /share/airesearch/slm:/share/airesearch/slm \
        -B /share/global:/share/global \
        -B "${TMP_DIR}":/home/singularity \
        -B "${TMP_WORKSPACE}":/workspace \
        "${SIF_PATH}" \
        "${INSTANCE_NAME}"

    sleep 15

    singularity exec --nv \
        --env NODE_RANK=${SLURM_NODEID} \
        --env GPU_UTIL_TARGET=${GPU_UTIL_TARGET} \
        --env PERIOD_SEC=${PERIOD_SEC} \
        --env STREAMS_PER_GPU=${STREAMS_PER_GPU} \
        --env MATMUL_DIM=${MATMUL_DIM} \
        "instance://${INSTANCE_NAME}" \
        python3 -c "
import os, time, threading

import torch

NODE = os.environ.get(\"NODE_RANK\", \"?\")
UTIL_TARGET = float(os.environ.get(\"GPU_UTIL_TARGET\", 60))
PERIOD = float(os.environ.get(\"PERIOD_SEC\", 1.0))
STREAMS_PER_GPU = int(os.environ.get(\"STREAMS_PER_GPU\", 1))
DIM = int(os.environ.get(\"MATMUL_DIM\", 8192))

BUSY_RATIO = max(0.0, min(1.0, UTIL_TARGET / 100.0))
BUSY_SEC = PERIOD * BUSY_RATIO
SLEEP_SEC = PERIOD - BUSY_SEC

n_gpu = torch.cuda.device_count()
print(f\"[节点 {NODE}] 检测到 {n_gpu} 张GPU, 目标利用率 {UTIL_TARGET}%, 忙/睡 = {BUSY_SEC:.3f}s/{SLEEP_SEC:.3f}s\", flush=True)

def worker(gpu_id, stream_id):
    torch.cuda.set_device(gpu_id)
    device = torch.device(f\"cuda:{gpu_id}\")
    a = torch.randn(DIM, DIM, device=device, dtype=torch.float16)
    b = torch.randn(DIM, DIM, device=device, dtype=torch.float16)
    cnt = 0
    last_report = time.time()
    while True:
        t0 = time.time()
        while time.time() - t0 < BUSY_SEC:
            c = torch.matmul(a, b)
            torch.cuda.synchronize(device)
            cnt += 1
        if SLEEP_SEC > 0:
            time.sleep(SLEEP_SEC)
        if time.time() - last_report > 300:
            print(f\"[节点 {NODE}] GPU{gpu_id}-stream{stream_id} 已完成 {cnt} 次矩阵乘\", flush=True)
            last_report = time.time()

threads = []
for gpu_id in range(n_gpu):
    for s in range(STREAMS_PER_GPU):
        t = threading.Thread(target=worker, args=(gpu_id, s), daemon=True)
        t.start()
        threads.append(t)

try:
    while True:
        time.sleep(60)
except KeyboardInterrupt:
    pass
"

    echo "节点 ${SLURM_NODEID} (Job ${SLURM_JOB_ID}) 上的GPU保活任务执行完成"
'

# ========== 资源清理 ==========
echo "=== 清理资源 ==="

# singularity 实例是每个节点本地的进程，必须用 srun -w 指定到对应节点上去 stop，
# 不能在提交节点本地循环执行（多节点时本地根本连不到其他节点的实例）
node_arr=($node_list)
for nodeid in $(seq 0 $((SLURM_JOB_NUM_NODES - 1))); do
    host="${node_arr[$nodeid]}"
    srun --nodes=1 --ntasks=1 -w "$host" \
        bash -c "singularity instance stop 'gpu-keeplive-${SLURM_JOB_ID}_${nodeid}' 2>/dev/null || true"
done

# 临时目录在共享存储上，提交节点直接清理即可
for nodeid in $(seq 0 $((SLURM_JOB_NUM_NODES - 1))); do
    JOB_NODE_TAG="${SLURM_JOB_ID}_${nodeid}"
    rm -rf "/share/global/datajuicer/ray_tmp_keeplive_${JOB_NODE_TAG}"           || true
    rm -rf "/share/global/datajuicer/ray_tmp_keeplive_workspace_${JOB_NODE_TAG}" || true
    rm -rf "/share/global/datajuicer/overlay_dir_keeplive_${JOB_NODE_TAG}"      || true
done

wait

echo "=== 作业完成时间: $(date) ==="
echo "作业 $SLURM_JOB_ID 执行完毕"

