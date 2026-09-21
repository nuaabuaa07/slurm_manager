#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
PYTHON_BIN="python3"
if [[ -x .venv/bin/python ]]; then
    PYTHON_BIN=".venv/bin/python"
fi

PROCESS_NAME="${SLURM_MANAGER_PROCESS_NAME:-aihpc-slurm-manager}"
PID_DIR=".slurm_manager"
PID_FILE="${PID_DIR}/streamlit.pid"
mkdir -p "$PID_DIR" "log"

if [[ -f "$PID_FILE" ]]; then
    OLD_PID=$(tr -cd '0-9' < "$PID_FILE")
    OLD_STATE=$(ps -p "$OLD_PID" -o stat= 2>/dev/null | tr -d '[:space:]' || true)
    if [[ -n "$OLD_PID" && -n "$OLD_STATE" && "$OLD_STATE" != Z* ]]; then
        echo "${PROCESS_NAME} 已在运行，PID=${OLD_PID}" >&2
        echo "如需重启，请先执行: bash stop_slurm_manager.sh" >&2
        exit 1
    fi
    rm -f "$PID_FILE"
fi

echo "$$" > "$PID_FILE"
echo "启动 ${PROCESS_NAME}，PID=$$，PID 文件=${PID_FILE}"

exec -a "$PROCESS_NAME" "$PYTHON_BIN" -m streamlit run app.py \
    --server.address "${SLURM_MANAGER_HOST:-0.0.0.0}" \
    --server.port "${SLURM_MANAGER_PORT:-8501}" \
    --browser.gatherUsageStats false
