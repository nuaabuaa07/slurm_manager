#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PROCESS_NAME="${SLURM_MANAGER_PROCESS_NAME:-aihpc-slurm-manager}"
PID_FILE=".slurm_manager/streamlit.pid"

process_state() {
    ps -p "$1" -o stat= 2>/dev/null | tr -d '[:space:]'
}

# kill -0 returns success for a zombie until its parent shell reaps it.  A
# zombie no longer holds the Streamlit port and should be treated as stopped.
process_alive() {
    local state
    state=$(process_state "$1")
    [[ -n "$state" && "$state" != Z* ]]
}

stop_descendants() {
    local signal_name="$1"
    local child
    # Streamlit can start helper children. Stop only descendants of the
    # verified manager PID; never signal the caller's process group.
    while IFS= read -r child; do
        [[ -n "$child" ]] || continue
        stop_descendants "$signal_name" "$child"
        kill "-$signal_name" "$child" 2>/dev/null || true
    done < <(pgrep -P "$2" 2>/dev/null || true)
}

if [[ ! -f "$PID_FILE" ]]; then
    echo "未找到 PID 文件: ${PID_FILE}"
    echo "可使用以下命令查找: pgrep -af '[a]ihpc-slurm-manager'"
    exit 1
fi

PID=$(tr -cd '0-9' < "$PID_FILE")
if [[ -z "$PID" ]]; then
    echo "PID 文件内容无效: ${PID_FILE}" >&2
    exit 1
fi

if ! process_alive "$PID"; then
    echo "进程 PID=${PID} 已不存在，清理过期 PID 文件。"
    rm -f "$PID_FILE"
    exit 0
fi

COMMAND=$(ps -p "$PID" -o command= 2>/dev/null || true)
if [[ "$COMMAND" != *"${PROCESS_NAME}"* && "$COMMAND" != *"streamlit run app.py"* ]]; then
    echo "拒绝停止：PID=${PID} 当前命令与 ${PROCESS_NAME} 不匹配。" >&2
    echo "当前命令: ${COMMAND}" >&2
    exit 1
fi

echo "正在停止 ${PROCESS_NAME}，PID=${PID}..."
stop_descendants TERM "$PID"
kill -TERM "$PID" 2>/dev/null || true

for _ in {1..10}; do
    if ! process_alive "$PID"; then
        rm -f "$PID_FILE"
        echo "已停止。"
        exit 0
    fi
    sleep 0.5
done

echo "进程仍在运行，自动发送 SIGKILL..."
stop_descendants KILL "$PID"
kill -KILL "$PID" 2>/dev/null || true

for _ in {1..10}; do
    if ! process_alive "$PID"; then
        rm -f "$PID_FILE"
        echo "已强制停止。"
        exit 0
    fi
    sleep 0.2
done

echo "进程仍未停止，PID=${PID}，当前状态：$(process_state "$PID" || true)" >&2
exit 1
