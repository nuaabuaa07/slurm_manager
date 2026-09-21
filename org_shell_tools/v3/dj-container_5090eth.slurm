#!/bin/bash

# ========== SLURM 参数配置 ==========
#SBATCH --job-name=gpu_compute
#SBATCH --partition=5090-eth-2
#SBATCH --nodelist=msxf-hpc-31-18-ai
#SBATCH --account=aidata
#SBATCH --nodes=1
#SBATCH --cpus-per-task=100
#SBATCH --mem-per-cpu=4
#SBATCH --ntasks-per-node=1
#SBATCH --gres=gpu:8
#SBATCH --time=240:00:00
#SBATCH --output=log/dj-slurm-%j.log
#SBATCH --error=log/dj-slurm-%j.err

# 计算内存分配 (40% 工作, 50% 对象存储)
TOTAL_MEM_GB=400
WORK_MEM_GB=$(echo "$TOTAL_MEM_GB * 0.4 / 1" | bc)
OBJECT_STORE_MEM_GB=$(echo "$TOTAL_MEM_GB * 0.5 / 1" | bc)
echo "内存分配: TOTAL_MEM_GB:${TOTAL_MEM_GB}, WORK_MEM_GB:${WORK_MEM_GB}, OBJECT_STORE_MEM_GB:${OBJECT_STORE_MEM_GB}"

export RAY_MEMORY=$(awk "BEGIN {print int($WORK_MEM_GB * 1024^3)}")
export RAY_OBJECT_STORE_MEMORY=$(awk "BEGIN {print int($OBJECT_STORE_MEM_GB * 1024^3)}")
echo "RAY节点内存分配: RAY_MEMORY:${RAY_MEMORY}, RAY_OBJECT_STORE_MEMORY:${RAY_OBJECT_STORE_MEMORY}"

echo "节点总内存: $(grep MemTotal /proc/meminfo | awk '{print $2/1024/1024}')" TB

# export SIF_PATH="/data/hpc/home/zhiliang.jin/create_dockerimage/datajuicer-no-flash/data-juicer-v1107-rtx5090.sif"
export SIF_PATH="/data/hpc/home/zhiliang.jin/create_dockerimage/datajuicer-no-flash/data-juicer-v1107-rtx5090-modelscopebert.sif"


# ========== 环境设置和初始化 ==========
set -e
mkdir -p log

echo "=== 作业开始时间: $(date) ==="
echo "作业 ID: $SLURM_JOB_ID"
echo "分配的节点: $SLURM_JOB_NODELIST"
echo "工作目录: $PWD"

node_list=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
echo "节点列表:"
echo "$node_list"

MASTER_HOST=$(echo "$node_list" | head -1)
MASTER_ADDR=$(srun --nodes=1 --ntasks=1 -w "$MASTER_HOST" hostname -I | awk '{print $1}')
export MASTER_ADDR

echo "主节点: $MASTER_HOST ($MASTER_ADDR)"

# ========== Singularity 实例启动 ==========
echo "=== 启动 Singularity 实例 ==="

# 将 SLURM_JOB_ID 显式导出，确保 srun 的子 shell 中可用
export SLURM_JOB_ID

srun bash -c '
    # 路径和实例名均包含 JOB_ID + NODE_ID，确保跨用户、跨任务不冲突
    JOB_NODE_TAG="${SLURM_JOB_ID}_${SLURM_NODEID}"

    TMP_DIR=/share/global/datajuicer/ray_tmp_5090_${JOB_NODE_TAG}
    rm -rf "${TMP_DIR}"
    mkdir -p "${TMP_DIR}"

    TMP_WORKSPACE=/share/global/datajuicer/ray_tmp_5090_workspace_${JOB_NODE_TAG}
    rm -rf "${TMP_WORKSPACE}"
    mkdir -p "${TMP_WORKSPACE}"

    OVERLAY_DIR=/share/global/datajuicer/overlay_dir_5090_${JOB_NODE_TAG}
    rm -rf "${OVERLAY_DIR}"
    mkdir -p "${OVERLAY_DIR}/upper"
    mkdir -p "${OVERLAY_DIR}/work"

    INSTANCE_NAME="dj-instance-${JOB_NODE_TAG}"

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

    sleep 30

    singularity exec \
        --env MASTER_ADDR=${MASTER_ADDR} \
        --env NODE_RANK=${SLURM_NODEID} \
        --env NNODES=${SLURM_JOB_NUM_NODES} \
        --env GPU_NUM=8 \
        --env CPU_NUM=${SLURM_CPUS_PER_TASK} \
        --env OBJECT_STORE_MEMORY=${RAY_OBJECT_STORE_MEMORY} \
        --env TOTAL_MEMORY=${RAY_MEMORY} \
        "instance://${INSTANCE_NAME}" \
        bash dj-ray_gpu.bash

    echo "节点 ${SLURM_NODEID} (Job ${SLURM_JOB_ID}) 上的实例任务执行完成"
'

# ========== 资源清理 ==========
echo "=== 清理资源 ==="

# 停止所有本任务的 Singularity 实例
for nodeid in $(seq 0 $((SLURM_JOB_NUM_NODES - 1))); do
    singularity instance stop "dj-instance-${SLURM_JOB_ID}_${nodeid}" 2>/dev/null || true
done

# 清理各节点临时目录
for nodeid in $(seq 0 $((SLURM_JOB_NUM_NODES - 1))); do
    JOB_NODE_TAG="${SLURM_JOB_ID}_${nodeid}"
    rm -rf "/share/global/datajuicer/ray_tmp_5090_${JOB_NODE_TAG}"          || true
    rm -rf "/share/global/datajuicer/ray_tmp_5090_workspace_${JOB_NODE_TAG}" || true
    rm -rf "/share/global/datajuicer/overlay_dir_5090_${JOB_NODE_TAG}"       || true
done

# 清理 Ray spill 目录
rm -rf "/share/global/datajuicer/ray_spill_${SLURM_JOB_ID}" || true

wait

echo "=== 作业完成时间: $(date) ==="
echo "作业 $SLURM_JOB_ID 执行完毕"
