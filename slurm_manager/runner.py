from __future__ import annotations

import os
import shutil
import signal
import subprocess
import time
from dataclasses import dataclass
from typing import Mapping, Optional, Sequence


@dataclass(frozen=True)
class CommandResult:
    args: Sequence[str]
    returncode: int
    stdout: str
    stderr: str
    elapsed_seconds: float
    timed_out: bool = False

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and not self.timed_out

    @property
    def command(self) -> str:
        return " ".join(str(item) for item in self.args)


class CommandRunner:
    """Run commands without invoking a local shell."""

    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    @staticmethod
    def available(binary: str) -> bool:
        return shutil.which(binary) is not None

    def run(
        self,
        args: Sequence[str],
        timeout: Optional[int] = None,
        env: Optional[Mapping[str, str]] = None,
        cwd: Optional[str] = None,
    ) -> CommandResult:
        started = time.monotonic()
        try:
            completed = subprocess.run(
                [str(item) for item in args],
                capture_output=True,
                text=True,
                timeout=timeout or self.timeout,
                env=env,
                cwd=cwd,
                check=False,
            )
            return CommandResult(
                args=args,
                returncode=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
                elapsed_seconds=time.monotonic() - started,
            )
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout.decode() if isinstance(exc.stdout, bytes) else (exc.stdout or "")
            stderr = exc.stderr.decode() if isinstance(exc.stderr, bytes) else (exc.stderr or "")
            return CommandResult(
                args=args,
                returncode=124,
                stdout=stdout,
                stderr=stderr or "命令执行超时",
                elapsed_seconds=time.monotonic() - started,
                timed_out=True,
            )
        except FileNotFoundError as exc:
            return CommandResult(
                args=args,
                returncode=127,
                stdout="",
                stderr=str(exc),
                elapsed_seconds=time.monotonic() - started,
            )

    def spawn(self, args: Sequence[str], cwd: Optional[str] = None) -> int:
        process = subprocess.Popen(
            [str(item) for item in args],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=cwd,
            start_new_session=True,
        )
        return int(process.pid)

    @staticmethod
    def process_running(pid: str) -> bool:
        if not str(pid).isdigit():
            return False
        numeric = int(pid)
        try:
            waited, _ = os.waitpid(numeric, os.WNOHANG)
            if waited == numeric:
                return False
        except ChildProcessError:
            pass
        try:
            os.kill(numeric, 0)
            state = subprocess.run(
                ["ps", "-p", str(numeric), "-o", "stat="], capture_output=True, text=True, check=False
            ).stdout.strip()
            return bool(state) and not state.startswith("Z")
        except (ProcessLookupError, PermissionError):
            return False

    @staticmethod
    def terminate_process_group(pid: str) -> None:
        if not str(pid).isdigit():
            raise ValueError("非法 PID")
        try:
            os.killpg(int(pid), signal.SIGTERM)
        except ProcessLookupError:
            return
