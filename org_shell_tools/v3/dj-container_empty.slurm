#!/bin/bash
#SBATCH --job-name=vllm-qwen-tp
#SBATCH --partition=5090-eth-2
#SBATCH --nodelist=msxf-hpc-71-13-ai
#SBATCH --nodes=1
#SBATCH --account=aidata
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --gres=gpu:8
#SBATCH --time=15-00:00:00
#SBATCH --output=log/dj-slurm-%j-%t.log
#SBATCH --error=log/dj-slurm-%j-%t.err

set -e

echo "=== 作业开始时间: $(date) ==="
echo "分配的节点: $SLURM_JOB_NODELIST"

tail -f /dev/null
