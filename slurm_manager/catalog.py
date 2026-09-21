from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List


SBATCH_RE = re.compile(r"^#SBATCH\s+--([\w-]+)(?:=|\s+)(.*?)\s*(?:#.*)?$")


@dataclass(frozen=True)
class ScriptInfo:
    name: str
    path: str
    job_name: str = ""
    partition: str = ""
    nodes: str = ""
    nodelist: str = ""
    cpus: str = ""
    gpu: str = ""
    time_limit: str = ""
    category: str = "other"

    def as_dict(self) -> Dict[str, str]:
        return asdict(self)


def _category(path: Path, text: str) -> str:
    name = path.name.lower()
    if "vllm" in name or "api_server" in text:
        return "vllm"
    if "keeplive" in name or "stress" in name:
        return "keepalive"
    if "dj-container" in name and ("--gres=gpu:8" in text or "dj-ray_gpu" in text):
        return "ray-gpu"
    if "dj-container" in name or "dj-ray" in text:
        return "ray-cpu"
    return "other"


def parse_script(path: Path) -> ScriptInfo:
    text = path.read_text(encoding="utf-8", errors="replace")
    directives: Dict[str, str] = {}
    for line in text.splitlines():
        match = SBATCH_RE.match(line)
        if match:
            directives[match.group(1)] = match.group(2).strip()
    gres = directives.get("gres", "")
    gpu_match = re.search(r"gpu:(\d+)", gres)
    return ScriptInfo(
        name=path.name,
        path=str(path.resolve()),
        job_name=directives.get("job-name", ""),
        partition=directives.get("partition", ""),
        nodes=directives.get("nodes", ""),
        nodelist=directives.get("nodelist", ""),
        cpus=directives.get("cpus-per-task", ""),
        gpu=gpu_match.group(1) if gpu_match else "0",
        time_limit=directives.get("time", ""),
        category=_category(path, text),
    )


def discover_scripts(roots: Iterable[Path], show_backups: bool = False) -> List[ScriptInfo]:
    found: List[ScriptInfo] = []
    for root in roots:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            # Only sbatch entrypoints belong in this catalog. dj-ray*.bash are
            # container internals and must never be offered as Slurm jobs.
            if path.suffix != ".slurm" and ".slurm_" not in path.name:
                continue
            if not show_backups and ("_bak" in path.name or path.name.endswith(".bak")):
                continue
            try:
                found.append(parse_script(path))
            except OSError:
                continue
    return found
