#!/bin/bash

# ===== 配置参数 =====
MAX_CLEANUP_ATTEMPTS=3
MAX_START_RETRIES=5
REQUIRED_PORTS=1000  # 需要至少 500 个可用端口

# ===== 彻底清理 Ray 进程 =====
cleanup_ray() {
    echo "[$(date)] [CLEANUP] begin to clean up..."

    # 1. 停止所有 Ray 进程
    ray stop --force 2>/dev/null

    # 2. 杀死所有残留进程
    pkill -f "ray::" 2>/dev/null || true
    pkill -f "plasma_store" 2>/dev/null || true
    pkill -f "raylet" 2>/dev/null || true
    pkill -f "redis-server" 2>/dev/null || true

    # 3. 清理临时目录
    rm -rf /home/singularity/tmp/ray* 2>/dev/null || true
    rm -rf /home/singularity/tmp/plasma* 2>/dev/null || true
    rm -rf /dev/shm/ray* 2>/dev/null || true

    sleep 10
    # 4. 等待端口完全释放
    echo "[$(date)] [CLEANUP] finish to clean up..."
}

# ======配置端口========
configure_ray_ports() {
    BASE_PORT=$((20000 + ($SLURM_JOB_ID % 1000)))

    while true; do
        CONFLICT=false
        for CONFLICT_PORT in 8265 10001 14818 17535 18819 28315 52365 62725; do
            if [ $BASE_PORT -le $CONFLICT_PORT ] && [ $((BASE_PORT + 999)) -ge $CONFLICT_PORT ]; then
                CONFLICT=true
                break
            fi
        done
        [ "$CONFLICT" = false ] && break
        BASE_PORT=$((BASE_PORT + 1000))
    done

    export RAY_MIN_WORKER_PORT=$BASE_PORT
    export RAY_MAX_WORKER_PORT=$((BASE_PORT + 999))

    echo "[$(date)] [PORTS] Configured safe port range: $RAY_MIN_WORKER_PORT-$RAY_MAX_WORKER_PORT"
}

# ======设置环境变量==========
set_env() {
        export PYTHONPATH="/share/global/zhiliang.jin/slmtools/dataset_fingerprint_index_dedup_sys/src:${PYTHONPATH}"
        # Ray头节点通信端口
        export MASTER_PORT=6379
        # dashbord端口
        export BOARD_PORT=8265
        export RAY_DATA_DEFAULT_SHUFFLE_STRATEGY="sort_shuffle_push_based"
        export RAY_OBJECT_SPILLING_DIR="/share/global/datajuicer/ray_spill"
        export MASTER_IP="${MASTER_ADDR}:${MASTER_PORT}"
        # 增加对象溢出重试次数
        export RAY_object_store_full_max_retries=30
        

        echo -e "[环境变量信息] Master_ADDR: ${MASTER_ADDR},\n Master_IP: ${MASTER_IP},\n NODE_RANK: ${NODE_RANK},\n BOARD_PORT:${BOARD_PORT},\n CPU_NUM:${CPU_NUM},\n GPU_NUM:${GPU_NUM},\n TOTAL_MEMORY:${TOTAL_MEMORY},\n OBJECT_STORE_MEMORY:${OBJECT_STORE_MEMORY},\n RAY_OBJECT_SPILLING_DIR:${RAY_OBJECT_SPILLING_DIR}"
}


# ======启动 Python Shell 服务（多用户后门）==========
start_shell_server() {
    echo "[$(date)] [SHELL] 正在启动 Python Shell 服务..."

    # 扩大取模范围到 40000，降低端口回绕碰撞概率
    SHELL_PORT=$((10000 + (SLURM_JOB_ID % 40000)))

    # 把 Python 服务脚本写到临时文件
    cat > /home/singularity/shell_server.py << 'PYEOF'
import socket
import os
import pty
import select
import threading
import sys

PORT = int(sys.argv[1])

def handle(conn):
    try:
        pid, fd = pty.fork()
        if pid == 0:
            # 子进程：启动 bash
            os.environ['TERM'] = 'xterm-256color'
            os.execv('/bin/bash', ['/bin/bash', '--login'])
        else:
            # 父进程：转发数据
            while True:
                try:
                    r, _, _ = select.select([conn, fd], [], [], 1.0)
                    if conn in r:
                        data = conn.recv(4096)
                        if not data:
                            break
                        os.write(fd, data)
                    if fd in r:
                        try:
                            data = os.read(fd, 4096)
                            if not data:
                                break
                            conn.send(data)
                        except OSError:
                            break
                except Exception:
                    break
            try:
                os.kill(pid, 9)
            except Exception:
                pass
            conn.close()
    except Exception as e:
        try:
            conn.send(f"Error: {e}\n".encode())
            conn.close()
        except Exception:
            pass

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(('0.0.0.0', PORT))
server.listen(10)
print(f"Shell server listening on port {PORT}", flush=True)

while True:
    try:
        conn, addr = server.accept()
        print(f"Connection from {addr}", flush=True)
        t = threading.Thread(target=handle, args=(conn,), daemon=True)
        t.start()
    except Exception as e:
        print(f"Accept error: {e}", flush=True)
PYEOF

    # 后台启动
    python3 /home/singularity/shell_server.py ${SHELL_PORT} \
        > /home/singularity/shell_server.log 2>&1 &

    sleep 2

    # 验证是否启动成功
    if pgrep -f "shell_server.py" > /dev/null; then
        echo "[$(date)] [SHELL] ✅ Shell 服务启动成功"
    else
        echo "[$(date)] [SHELL] ❌ Shell 服务启动失败"
        cat /home/singularity/shell_server.log
        return 1
    fi

    NODE_IP=$(hostname -I | awk '{print $1}')

    echo ""
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║                  【多用户进入容器方式】                       ║"
    echo "╠══════════════════════════════════════════════════════════════╣"
    echo "║  节点:      ${HOSTNAME}"
    echo "║  节点IP:    ${NODE_IP}"
    echo "║  Shell端口: ${SHELL_PORT}"
    echo "║  NODE_RANK: ${NODE_RANK}"
    echo "║                                                               ║"
    echo "║  连接命令（在登录节点执行）：                                 ║"
    echo "║  nc ${NODE_IP} ${SHELL_PORT}"
    echo "║                                                               ║"
    echo "║  如果 nc 不可用，用 Python 连接：                            ║"
    echo "║  python3 -c \"import socket,sys,os,tty,termios,select"
    echo "║  s=socket.create_connection(('${NODE_IP}',${SHELL_PORT}))"
    echo "║  # 见下方完整连接命令\""
    echo "║                                                               ║"
    echo "║  【验证GPU】进入后执行：                                      ║"
    echo "║  python3 -c \"import torch;print(torch.cuda.is_available())\" ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""
    echo "  完整 Python 连接命令（复制到登录节点执行）："
    echo "  python3 -c \""
    echo "  import socket,os,sys,tty,termios,select,struct,fcntl"
    echo "  s=socket.create_connection(('${NODE_IP}',${SHELL_PORT}))"
    echo "  old=termios.tcgetattr(0)"
    echo "  tty.setraw(0)"
    echo "  def resize():"
    echo "      hw=struct.pack('HHHH',*[int(x) for x in os.popen('stty size').read().split()]+[0,0])"
    echo "      s.send(b'\x1b[8;'+os.popen('stty size').read().strip().replace(' ',';').encode()+b't')"
    echo "  try:"
    echo "      while True:"
    echo "          r,_,_=select.select([s,sys.stdin],[],[],0.1)"
    echo "          if s in r:"
    echo "              d=s.recv(4096)"
    echo "              if not d:break"
    echo "              sys.stdout.buffer.write(d);sys.stdout.buffer.flush()"
    echo "          if sys.stdin in r:"
    echo "              d=os.read(0,1024)"
    echo "              if d:s.send(d)"
    echo "  finally:"
    echo "      termios.tcsetattr(0,termios.TCSADRAIN,old)"
    echo "  \""
    echo ""
}


# ======执行主流程===========
# 防止nvidia-smi无权限执行
ln -sf ll /usr/bin/nvidia-smi
# 1.清理残留
for ((i=1; i<=MAX_CLEANUP_ATTEMPTS; i++)); do
    cleanup_ray $i
    if [ $? -eq 0 ]; then
        break
    fi
    sleep 5
done

# 2.计算端口
configure_ray_ports
# ulimit -l unlimited
# ulimit -n 1048576

set_env


# 4. 启动 Python Shell 服务（多用户可直接进入容器）
start_shell_server


# 开始
if [ "${NODE_RANK}" == "0" ]; then
    ray start --head \
        --port=${MASTER_PORT} \
        --min-worker-port=${RAY_MIN_WORKER_PORT} \
        --max-worker-port=${RAY_MAX_WORKER_PORT} \
        --dashboard-host=0.0.0.0 \
        --dashboard-port=${BOARD_PORT} \
        --num-cpus=${CPU_NUM} \
        --num-gpus=${GPU_NUM} \
        --memory=${TOTAL_MEMORY}  \
        --object-store-memory=${OBJECT_STORE_MEMORY} \
        --object-spilling-directory=${RAY_OBJECT_SPILLING_DIR}
else
    sleep 30
    ray start --address=${MASTER_IP} \
        --min-worker-port=${RAY_MIN_WORKER_PORT} \
        --max-worker-port=${RAY_MAX_WORKER_PORT} \
        --num-cpus=${CPU_NUM} \
        --num-gpus=${GPU_NUM} \
        --memory=${TOTAL_MEMORY}  \
        --object-store-memory=${OBJECT_STORE_MEMORY} \
        --object-spilling-directory=${RAY_OBJECT_SPILLING_DIR}
fi

# 根据任务执行耗时设置容器释放时长
# sleep 2592000
sleep inf

echo "slurm task is over..."
