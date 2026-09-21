#!/bin/bash

# ========== SLURM 参数配置 ==========
#SBATCH --job-name=data-juicer
#SBATCH --partition=5090-eth-2         # 使用分区资源池
#SBATCH --nodelist=msxf-hpc-31-1-ai,msxf-hpc-31-18-ai,msxf-hpc-31-19-ai,msxf-hpc-31-20-ai,msxf-hpc-31-21-ai,msxf-hpc-31-23-ai,msxf-hpc-37-16-ai
#SBATCH --account=aidata                   # 账户名
#SBATCH --nodes=7
#SBATCH --cpus-per-task=70
#SBATCH --mem-per-cpu=6g
#SBATCH --ntasks-per-node=1             # 每个节点任务数
#SBATCH --gres=gpu:0                    # 明确请求GPU资源（根据实际情况调整,gpu:1表示使用gpu,gpu:0表示不用gpu）
#SBATCH --time=15-00:00:00
#SBATCH --output=log/dj-slurm-%j.log          # 标准输出日志
#SBATCH --error=log/dj-slurm-%j.err           # 将标准错误单独输出，便于排查问题

# 计算内存分配 (60% 工作, 30% 对象存储)
TOTAL_MEM_GB=420
WORK_MEM_GB=$(echo "$TOTAL_MEM_GB * 0.4 / 1" | bc)
OBJECT_STORE_MEM_GB=$(echo "$TOTAL_MEM_GB * 0.5 / 1" | bc)
echo "内存分配: TOTAL_MEM_GB:${TOTAL_MEM_GB}, WORK_MEM_GB:${WORK_MEM_GB}, OBJECT_STORE_MEM_GB:${OBJECT_STORE_MEM_GB}"

# 设置 Ray 内存参数
export RAY_MEMORY=$(awk "BEGIN {print int($WORK_MEM_GB * 1024^3)}")
export RAY_OBJECT_STORE_MEMORY=$(awk "BEGIN {print int($OBJECT_STORE_MEM_GB * 1024^3)}")
echo "RAY节点内存分配: RAY_MEMORY:${RAY_MEMORY}, RAY_OBJECT_STORE_MEMORY:${RAY_OBJECT_STORE_MEMORY}"

# 检查节点内存
echo "节点总内存: $(grep MemTotal /proc/meminfo | awk '{print $2/1024/1024}')" TB

# ========== 环境设置和初始化 ==========
set -e  # 遇到错误立即退出
mkdir -p log  # 确保日志目录存在

echo "=== 作业开始时间: $(date) ==="
echo "分配的节点: $SLURM_JOB_NODELIST"
echo "工作目录: $PWD"

# 获取节点列表
node_list=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
echo "节点列表:"
echo "$node_list"

# 设置主节点信息
MASTER_HOST=$(echo "$node_list" | head -1)
# 获取MASTER IP地址
MASTER_ADDR=$(srun --nodes=1 --ntasks=1 -w "$MASTER_HOST" hostname -I | awk '{print $1}')
export MASTER_ADDR

echo "主节点: $MASTER_HOST ($MASTER_ADDR)"
# ========== Singularity 实例启动 ==========
echo "=== 启动 Singularity 实例 ==="


TMP_DIR=/share/global/datajuicer/ray_tmp_eth_${SLURM_JOB_ID}_${SLURM_NODEID}
mkdir -p "${TMP_DIR}"

# 为每个节点启动实例
srun   bash -c '
   singularity instance start -e --writable-tmpfs  \
        -B "${TMP_DIR}":/home/singularity \
        -B /share/aidev/data:/share/aidev/data \
        -B /share/airesearch/slm:/share/airesearch/slm \
        -B /share/global:/share/global \
        /data/hpc/home/zhiliang.jin/create_dockerimage/datajuicer-CPUclau/data-juicer-v1107-all-upgradeHF.sif \
        dj-instance-${SLURM_NODEID}

    sleep 60

    singularity exec \
        --env MASTER_ADDR=${MASTER_ADDR} \
        --env NODE_RANK=${SLURM_NODEID} \
        --env NNODES=${SLURM_JOB_NUM_NODES} \
        --env GPU_NUM=0 \
        --env CPU_NUM=${SLURM_CPUS_PER_TASK} \
        --env OBJECT_STORE_MEMORY=${RAY_OBJECT_STORE_MEMORY} \
        --env TOTAL_MEMORY=${RAY_MEMORY} \
        instance://dj-instance-${SLURM_NODEID} \
        bash dj-ray.bash
'
echo '节点 $SLURM_NODEID 上的实例任务执行完成'

# ========== 资源清理 ==========
echo "=== 清理资源 ==="

# 停止Singularity 实例
singularity instance stop dj-instance-$SLURM_NODEID 2>/dev/null || true

wait  # 等待所有实例停止完成

echo "=== 作业完成时间: $(date) ==="
echo "作业 $SLURM_JOB_ID 执行完毕"
