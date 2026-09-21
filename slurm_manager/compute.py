from __future__ import annotations

import shlex
from dataclasses import dataclass

from .ray import normalize_background_command
from .runner import CommandRunner


@dataclass(frozen=True)
class LocalTaskResult:
    pid: str
    command: str


class ComputeTaskService:
    """Launch a persistent srun step for a non-container compute-node task."""

    def __init__(self, runner: CommandRunner):
        self.runner = runner

    def submit(self, job_id: str, node: str, command: str) -> LocalTaskResult:
        normalized = normalize_background_command(command)
        if not normalized:
            raise ValueError("任务命令不能为空")
        remote = "exec bash -lc " + shlex.quote(normalized)
        args = [
            "srun", "--jobid", job_id, "--overlap", "--nodes", "1", "--ntasks", "1",
            "--nodelist", node, "bash", "-lc", remote,
        ]
        pid = self.runner.spawn(args)
        return LocalTaskResult(str(pid), normalized)

    def status(self, pid: str) -> str:
        return "RUNNING" if self.runner.process_running(pid) else "EXITED"

    def stop(self, pid: str) -> None:
        self.runner.terminate_process_group(pid)
