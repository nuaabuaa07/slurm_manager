import socket, os, sys, tty, termios, select, time

host = sys.argv[1]
port = int(sys.argv[2])
init_cmd = sys.argv[3] if len(sys.argv) > 3 else None

s = socket.create_connection((host, port))
old = termios.tcgetattr(0)
try:
    tty.setraw(0)
    # 等 shell 初始化完成再发命令
    if init_cmd:
        time.sleep(0.5)
        s.send((init_cmd + "\n").encode())
    while True:
        r, _, _ = select.select([s, sys.stdin], [], [], 0.1)
        if s in r:
            d = s.recv(4096)
            if not d: break
            sys.stdout.buffer.write(d)
            sys.stdout.buffer.flush()
        if sys.stdin in r:
            d = os.read(0, 1024)
            if d: s.send(d)
finally:
    termios.tcsetattr(0, termios.TCSADRAIN, old)
