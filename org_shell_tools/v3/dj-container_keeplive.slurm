#!/bin/bash

# ========== SLURM 参数配置 ==========
#SBATCH --job-name=cpu-stress
#SBATCH --partition=ray-cpu
#SBATCH --nodelist=msxf-app-33-7-ai,msxf-app-33-8-ai,msxf-app-33-9-ai,msxf-app-33-10-ai,msxf-app-33-11-ai,msxf-app-33-12-ai,msxf-app-33-13-ai,msxf-app-33-14-ai
#SBATCH --account=aidev
#SBATCH --nodes=10
#SBATCH --cpus-per-task=60
#SBATCH --mem-per-cpu=8g
#SBATCH --ntasks-per-node=1
#SBATCH --gres=gpu:0
#SBATCH --time=15-00:00:00
#SBATCH --output=log/cpu-stress-%j.log
#SBATCH --error=log/cpu-stress-%j.err

set -e
mkdir -p log

echo "=== 作业开始时间: $(date) ==="
echo "分配的节点: $SLURM_JOB_NODELIST"

TMP_DIR=/share/global/datajuicer/ray_tmp_${SLURM_JOB_ID}_${SLURM_NODEID}
mkdir -p "${TMP_DIR}"

srun bash -c '
    # ===== 启动 Singularity 实例 =====
    singularity instance start -e --writable-tmpfs \
        -B "${TMP_DIR}":/home/singularity \
        -B /share/aidev/data:/share/aidev/data \
        -B /share/global:/share/global \
        /data/hpc/home/zhiliang.jin/create_dockerimage/datajuicer-CPUclau/data-juicer-v1107-all-upgradeHF.sif \
        stress-instance-${SLURM_NODEID}

    sleep 10

    # ===== 在容器内执行 Python CPU 压测 =====
    singularity exec \
        --env CPU_NUM=${SLURM_CPUS_PER_TASK} \
        --env NODE_RANK=${SLURM_NODEID} \
        instance://stress-instance-${SLURM_NODEID} \
        python3 -c "
import multiprocessing, time, os, sys

CPU_NUM  = int(os.environ.get(\"CPU_NUM\", 60))
RATIO    = 0.65
WORKERS  = max(1, int(CPU_NUM * RATIO))
NODE     = os.environ.get(\"NODE_RANK\", \"?\")

def burn():
    while True:
        x = 99999 ** 9999  # 大整数幂运算，纯 CPU 密集，无 IO

print(f\"[节点 {NODE}] 启动 {WORKERS}/{CPU_NUM} 个 worker，目标负载 ~{int(RATIO*100)}%\", flush=True)

procs = [multiprocessing.Process(target=burn) for _ in range(WORKERS)]
for p in procs:
    p.start()

# 主进程定期打印存活状态
try:
    while True:
        alive = sum(1 for p in procs if p.is_alive())
        print(f\"[节点 {NODE}] {time.strftime(\"%H:%M:%S\")} worker 存活: {alive}/{WORKERS}\", flush=True)
        time.sleep(300)
except KeyboardInterrupt:
    pass
finally:
    for p in procs:
        p.terminate()
    print(f\"[节点 {NODE}] 压测结束\", flush=True)
"

    # ===== 清理实例 =====
    singularity instance stop stress-instance-${SLURM_NODEID} 2>/dev/null || true
'

echo "=== 作业完成时间: $(date) ==="

