from __future__ import annotations

import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .runner import CommandResult, CommandRunner


ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
JOB_ID_RE = re.compile(r"^[0-9]+(?:_[0-9]+)?$")


def _rows(text: str, columns: Sequence[str]) -> List[Dict[str, str]]:
    result: List[Dict[str, str]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        values = line.split("|", len(columns) - 1)
        values += [""] * (len(columns) - len(values))
        result.append({column: values[index].strip() for index, column in enumerate(columns)})
    return result


def _key_values(line: str) -> Dict[str, str]:
    return dict(re.findall(r"(?:^|\s)([A-Za-z][A-Za-z0-9_/.-]*)=(\S*)", line))


@dataclass(frozen=True)
class SubmitRequest:
    script: Path
    chdir: Optional[Path] = None
    nodelist: str = ""
    partition: str = ""
    nodes: Optional[int] = None
    exports: Optional[Mapping[str, str]] = None


class SlurmService:
    def __init__(self, runner: CommandRunner):
        self.runner = runner

    def health(self) -> List[Dict[str, object]]:
        return [
            {"command": name, "available": self.runner.available(name)}
            for name in ("sinfo", "squeue", "scontrol", "sbatch", "scancel", "sacct", "srun")
        ]

    def partitions(self) -> Tuple[List[Dict[str, str]], CommandResult]:
        columns = ["partition", "available", "time_limit", "nodes", "state"]
        result = self.runner.run(["sinfo", "-h", "-o", "%P|%a|%l|%D|%t"])
        rows = _rows(result.stdout, columns)
        for row in rows:
            row["partition"] = row["partition"].rstrip("*")
        return rows, result

    def nodes(self, partition: str) -> Tuple[List[Dict[str, object]], CommandResult]:
        columns = ["node", "state", "cpu", "memory_mb", "gres", "reason", "partition"]
        result = self.runner.run(
            ["sinfo", "-N", "-h", "-p", partition, "-o", "%N|%T|%C|%m|%G|%E|%P"]
        )
        rows: List[Dict[str, object]] = _rows(result.stdout, columns)
        address_map = self.node_addresses()
        for row in rows:
            cpu_parts = str(row["cpu"]).split("/")
            allocated = int(cpu_parts[0]) if cpu_parts and cpu_parts[0].isdigit() else 0
            total = int(cpu_parts[3]) if len(cpu_parts) > 3 and cpu_parts[3].isdigit() else 0
            row["cpu_allocated"] = allocated
            row["cpu_total"] = total
            row["cpu_allocated_pct"] = round(100 * allocated / total, 1) if total else 0.0
            row["memory_gb"] = round(int(str(row["memory_mb"])) / 1024, 1) if str(row["memory_mb"]).isdigit() else 0
            row["address"] = address_map.get(str(row["node"]), str(row["node"]))
        return rows, result

    def node_addresses(self) -> Dict[str, str]:
        result = self.runner.run(["scontrol", "show", "nodes", "-o"])
        addresses: Dict[str, str] = {}
        if not result.ok:
            return addresses
        for line in result.stdout.splitlines():
            values = _key_values(line)
            name = values.get("NodeName", "")
            if name:
                addresses[name] = values.get("NodeAddr", "") or name
        return addresses

    def jobs(self, user: str = "", include_all: bool = False) -> Tuple[List[Dict[str, str]], CommandResult]:
        columns = ["job_id", "name", "partition", "user", "state", "elapsed", "limit", "nodes", "reason_or_nodes"]
        args = ["squeue", "-h", "-o", "%i|%j|%P|%u|%T|%M|%l|%D|%R"]
        if user and not include_all:
            args.extend(["-u", user])
        result = self.runner.run(args)
        return _rows(result.stdout, columns), result

    def job_detail(self, job_id: str) -> Tuple[Dict[str, str], CommandResult]:
        self._validate_job_id(job_id)
        result = self.runner.run(["scontrol", "show", "job", "-o", job_id])
        return _key_values(result.stdout), result

    def job_history(self, user: str, start: str = "today") -> Tuple[List[Dict[str, str]], CommandResult]:
        columns = ["job_id", "name", "partition", "state", "elapsed", "start", "end", "exit_code"]
        result = self.runner.run(
            [
                "sacct", "-X", "-n", "-P", "-S", start, "-u", user,
                "-o", "JobIDRaw,JobName,Partition,State,Elapsed,Start,End,ExitCode",
            ]
        )
        return _rows(result.stdout, columns), result

    def expand_nodelist(self, expression: str) -> Tuple[List[str], CommandResult]:
        result = self.runner.run(["scontrol", "show", "hostnames", expression])
        return [line.strip() for line in result.stdout.splitlines() if line.strip()], result

    def node_address(self, node: str) -> Tuple[str, CommandResult]:
        result = self.runner.run(["scontrol", "show", "node", "-o", node])
        return _key_values(result.stdout).get("NodeAddr", ""), result

    def job_nodes(self, job_id: str) -> Tuple[List[Dict[str, str]], CommandResult]:
        detail, detail_result = self.job_detail(job_id)
        expression = ""
        for key in ("NodeList", "ReqNodeList", "BatchHost"):
            candidate = detail.get(key, "")
            if candidate and candidate not in {"(null)", "None", "N/A"}:
                expression = candidate
                break
        if not expression:
            return [], detail_result
        nodes, expand_result = self.expand_nodelist(expression)
        output = []
        for node in nodes:
            address, _ = self.node_address(node)
            output.append({"node": node, "address": address or node})
        return output, expand_result

    def submit(self, request: SubmitRequest) -> CommandResult:
        script = request.script.resolve(strict=True)
        # Preserve the behavior of manually running sbatch from the script's
        # own directory. Many existing jobs reference sibling files such as
        # dj-ray.bash and relative log/ paths.
        workdir = (request.chdir or script.parent).resolve(strict=True)
        args: List[str] = ["sbatch", "--parsable", "--chdir=" + str(workdir)]
        if request.partition:
            args.append("--partition=" + request.partition)
        if request.nodelist:
            args.append("--nodelist=" + request.nodelist)
        if request.nodes:
            args.append("--nodes=" + str(request.nodes))
        exports = request.exports or {}
        if exports:
            clean = []
            for key, value in exports.items():
                if not ENV_NAME_RE.match(key):
                    raise ValueError("非法环境变量名: " + key)
                if "\n" in value or "\x00" in value:
                    raise ValueError("环境变量值不能包含换行或 NUL")
                clean.append(key + "=" + value)
            args.append("--export=ALL," + ",".join(clean))
        args.append(str(script))
        return self.runner.run(args)

    def cancel(self, job_id: str) -> CommandResult:
        self._validate_job_id(job_id)
        return self.runner.run(["scancel", job_id])

    @staticmethod
    def extract_job_id(result: CommandResult) -> str:
        return result.stdout.strip().split(";", 1)[0].strip()

    @staticmethod
    def _validate_job_id(job_id: str) -> None:
        if not JOB_ID_RE.match(job_id):
            raise ValueError("非法 Job ID")


def expand_slurm_log_pattern(pattern: str, job_id: str, array_task_id: str = "0") -> str:
    return pattern.replace("%j", job_id).replace("%A", job_id).replace("%a", array_task_id).replace("%t", "0")


def tail_text(path: Path, lines: int = 200, max_bytes: int = 2_000_000) -> str:
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(str(path))
    size = path.stat().st_size
    with path.open("rb") as handle:
        handle.seek(max(0, size - max_bytes))
        data = handle.read()
    return b"\n".join(data.splitlines()[-lines:]).decode("utf-8", errors="replace")
