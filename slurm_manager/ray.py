from __future__ import annotations

import re
import shlex
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .files import resolve_allowed


PID_RE = re.compile(r"SLURM_MANAGER_PID=(\d+)")


@dataclass(frozen=True)
class RaySubmitResult:
    ok: bool
    output: str
    pid: str
    command: str


def shell_port(job_id: str, base: int = 10000, modulo: int = 40000) -> int:
    numeric = int(job_id.split("_", 1)[0])
    return base + numeric % modulo


class ShellGateway:
    """Non-interactive client for the existing TCP PTY shell inside a Ray container."""

    def execute(self, host: str, port: int, command: str, timeout: int = 10) -> str:
        marker = "__SLURM_MANAGER_DONE_" + str(time.time_ns()) + "__"
        payload = command + f"; rc=$?; printf '\\n{marker}:%s\\n' \"$rc\"; stty echo\n"
        chunks = []
        with socket.create_connection((host, port), timeout=timeout) as connection:
            connection.settimeout(1.0)
            # The existing gateway exposes a PTY. Disable terminal echo so marker
            # and status text in the command are not mistaken for command output.
            time.sleep(0.5)
            connection.sendall(b"stty -echo\n")
            time.sleep(0.1)
            connection.sendall(payload.encode("utf-8"))
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                try:
                    data = connection.recv(65536)
                except socket.timeout:
                    continue
                if not data:
                    break
                chunks.append(data)
                combined = b"".join(chunks)
                if re.search(re.escape(marker).encode() + rb":\d+", combined):
                    break
        return b"".join(chunks).decode("utf-8", errors="replace")

    def submit_script(
        self,
        host: str,
        port: int,
        script: str,
        args: str,
        log_path: str,
        allowed_roots: Iterable[Path],
        timeout: int = 10,
    ) -> RaySubmitResult:
        script_path = resolve_allowed(script, allowed_roots, must_exist=True)
        log = Path(log_path).expanduser() if log_path.strip() else Path("/dev/null")
        if str(log) != "/dev/null":
            resolve_allowed(str(log.parent), allowed_roots, must_exist=True)
        if script_path.suffix == ".py":
            launcher = "python3"
        elif script_path.suffix in {".sh", ".bash"}:
            launcher = "bash"
        else:
            launcher = "bash"
        inner = "cd " + shlex.quote(str(script_path.parent)) + " && " + launcher + " " + shlex.quote(str(script_path))
        if args.strip():
            # Arguments are intentionally interpreted by bash inside the container; the UI shows the final command.
            inner += " " + args.strip()
        prefix = "mkdir -p " + shlex.quote(str(log.parent)) + " && " if str(log) != "/dev/null" else ""
        wrapper = (
            "if command -v setsid >/dev/null 2>&1; then exec setsid bash -lc " + shlex.quote(inner)
            + "; else exec bash -lc " + shlex.quote(inner) + "; fi"
        )
        launch = "nohup bash -c " + shlex.quote(wrapper)
        command = prefix + launch + " > " + shlex.quote(str(log)) + " 2>&1 < /dev/null & echo SLURM_MANAGER_PID=$!"
        output = self.execute(host, port, command, timeout=timeout)
        match = PID_RE.search(output)
        return RaySubmitResult(bool(match), output, match.group(1) if match else "", command)

    def submit_command(self, host: str, port: int, user_command: str, timeout: int = 10) -> RaySubmitResult:
        inner = normalize_background_command(user_command)
        if not inner:
            raise ValueError("任务命令不能为空")
        pid_file = "/tmp/slurm_manager_" + str(time.time_ns()) + ".pid"
        # `$!` can be the short-lived setsid wrapper rather than the task's real
        # session leader. The child writes its own PID after setsid, so status
        # and stop operations target the persistent parent and its whole group.
        runner = "echo $$ > " + shlex.quote(pid_file) + "; exec bash -lc " + shlex.quote(inner)
        command = (
            "rm -f " + shlex.quote(pid_file) + "; "
            "if command -v setsid >/dev/null 2>&1; then "
            "nohup setsid bash -c " + shlex.quote(runner) + " > /dev/null 2>&1 < /dev/null & "
            "else nohup bash -c " + shlex.quote(runner) + " > /dev/null 2>&1 < /dev/null & fi; "
            "for i in 1 2 3 4 5 6 7 8 9 10; do [ -s " + shlex.quote(pid_file) + " ] && break; sleep 0.1; done; "
            "if [ -s " + shlex.quote(pid_file) + " ]; then printf 'SLURM_MANAGER_PID=%s\\n' \"$(cat "
            + shlex.quote(pid_file) + ")\"; fi; rm -f " + shlex.quote(pid_file)
        )
        output = self.execute(host, port, command, timeout=timeout)
        match = PID_RE.search(output)
        return RaySubmitResult(bool(match), output, match.group(1) if match else "", inner)

    def stop_process(self, host: str, port: int, pid: str, timeout: int = 10) -> str:
        if not pid.isdigit():
            raise ValueError("非法 PID")
        return self.execute(
            host,
            port,
            "kill -TERM -- -" + pid + " 2>/dev/null || kill -TERM -- " + pid + " 2>/dev/null; "
            "for i in 1 2 3 4 5 6 7 8 9 10; do "
            "if ! kill -0 -- -" + pid + " 2>/dev/null && ! kill -0 -- " + pid + " 2>/dev/null; then break; fi; sleep 0.3; done; "
            "kill -KILL -- -" + pid + " 2>/dev/null || kill -KILL -- " + pid + " 2>/dev/null || true",
            timeout=timeout,
        )

    def process_status(self, host: str, port: int, pid: str, timeout: int = 10) -> str:
        if not pid.isdigit():
            raise ValueError("非法 PID")
        output = self.execute(
            host,
            port,
            "if kill -0 " + pid + " 2>/dev/null || kill -0 -- -" + pid + " 2>/dev/null; "
            "then echo SLURM_MANAGER_STATUS=RUNNING; "
            "else echo SLURM_MANAGER_STATUS=EXITED; fi",
            timeout=timeout,
        )
        if "SLURM_MANAGER_STATUS=RUNNING" in output:
            return "RUNNING"
        if "SLURM_MANAGER_STATUS=EXITED" in output:
            return "EXITED"
        return "UNKNOWN"


def normalize_background_command(command: str) -> str:
    normalized = command.strip()
    if normalized.endswith("&"):
        normalized = normalized[:-1].rstrip()
    return normalized


def extract_log_path(command: str) -> str:
    matches = re.findall(
        r"(?<!\d)(?:>>|>)\s*(?:\"([^\"]+)\"|'([^']+)'|([^\s;&]+))(?=\s+2>&1\b|\s*(?:&|$))",
        command,
    )
    for match in reversed(matches):
        path = next((value for value in match if value), "")
        if path and path != "/dev/null":
            return path
    return ""
