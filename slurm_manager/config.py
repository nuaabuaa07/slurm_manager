from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List


PROJECT_ROOT = Path(__file__).resolve().parent.parent


DEFAULTS: Dict[str, Any] = {
    "default_user": "${USER}",
    "default_password": "fghjfghj",
    "auth_session_hours": 6,
    "default_partitions": ["ray-cpu", "5090-eth-2", "h200-ib-slm"],
    "script_roots": ["${PROJECT_ROOT}/org_shell_tools/v3"],
    "restart_script_roots": ["/data/hpc/home/${USER}", "/share/global/${USER}"],
    "ray_task_roots": ["/share/aidev/data", "/share/airesearch/slm", "/share/global"],
    "browse_roots": [
        "/share/aidev/data",
        "/share/airesearch/slm",
        "/share/global",
        "/data/hpc/home/${USER}",
    ],
    "editable_config_roots": [
        "/share/aidev/data",
        "/share/airesearch/slm",
        "/share/global",
        "/data/hpc/home/${USER}",
    ],
    "state_db": "${PROJECT_ROOT}/.slurm_manager/state.db",
    "job_snapshot_dir": "${PROJECT_ROOT}/.slurm_manager/job_snapshots",
    "command_timeout_seconds": 30,
    "metrics_timeout_seconds": 20,
    "metrics_parallelism": 8,
    "shell_port_base": 10000,
    "shell_port_modulo": 40000,
    "node_connect_command": "python ${PROJECT_ROOT}/org_shell_tools/v3/connect.py",
    "vllm_default_model": "qwen2.5-0.5b",
    "vllm_default_ports": "8000-8007",
    "vllm_api_token_env": "VLLM_API_TOKEN",
    "show_backup_scripts": False,
}


def _expand(value: Any) -> Any:
    if isinstance(value, str):
        value = value.replace("${PROJECT_ROOT}", str(PROJECT_ROOT))
        return os.path.expanduser(os.path.expandvars(value))
    if isinstance(value, list):
        return [_expand(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand(item) for key, item in value.items()}
    return value


@dataclass(frozen=True)
class AppConfig:
    values: Dict[str, Any]
    source: Path

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)

    def paths(self, key: str) -> List[Path]:
        return [Path(item).resolve(strict=False) for item in self.get(key, [])]


def load_config(path: str = "") -> AppConfig:
    selected = Path(path or os.environ.get("SLURM_MANAGER_CONFIG", "") or PROJECT_ROOT / "config.json")
    values = dict(DEFAULTS)
    if selected.exists():
        with selected.open("r", encoding="utf-8") as handle:
            loaded = json.load(handle)
        if not isinstance(loaded, dict):
            raise ValueError("config.json 顶层必须是 JSON object")
        values.update(loaded)
    return AppConfig(_expand(values), selected)
