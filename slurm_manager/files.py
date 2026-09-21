from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


def resolve_allowed(path: str, roots: Iterable[Path], must_exist: bool = True) -> Path:
    candidate = Path(path.strip()).expanduser().resolve(strict=False)
    allowed = [root.expanduser().resolve(strict=False) for root in roots]
    if not any(candidate == root or root in candidate.parents for root in allowed):
        raise PermissionError("路径不在允许访问的根目录中")
    if must_exist and not candidate.exists():
        raise FileNotFoundError(str(candidate))
    return candidate


def list_directory(path: Path, limit: int = 500) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for item in sorted(path.iterdir(), key=lambda value: (not value.is_dir(), value.name.lower()))[:limit]:
        try:
            stat = item.stat()
        except OSError:
            continue
        entries.append(
            {
                "name": item.name,
                "type": "directory" if item.is_dir() else item.suffix.lower().lstrip(".") or "file",
                "size": "-" if item.is_dir() else human_size(stat.st_size),
                "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                "path": str(item),
            }
        )
    return entries


def find_jsonl(path: Path, max_depth: int = 2, limit: int = 500) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    base_depth = len(path.parts)
    for current, dirs, files in os.walk(str(path)):
        current_path = Path(current)
        if len(current_path.parts) - base_depth >= max_depth:
            dirs[:] = []
        for filename in files:
            if not filename.lower().endswith((".jsonl", ".jsonlines")):
                continue
            file_path = current_path / filename
            try:
                stat = file_path.stat()
            except OSError:
                continue
            result.append(
                {"name": filename, "size": human_size(stat.st_size), "size_bytes": stat.st_size,
                 "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                 "path": str(file_path)}
            )
            if len(result) >= limit:
                return sorted(result, key=lambda item: item["modified"], reverse=True)
    return sorted(result, key=lambda item: item["modified"], reverse=True)


def preview_jsonl(path: Path, offset: int = 0, limit: int = 50, max_line_bytes: int = 1_000_000) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    records: List[Dict[str, Any]] = []
    invalid = 0
    truncated = 0
    with path.open("rb") as handle:
        for index, raw in enumerate(handle):
            if index < offset:
                continue
            if len(records) >= limit:
                break
            if len(raw) > max_line_bytes:
                raw = raw[:max_line_bytes]
                truncated += 1
            try:
                value = json.loads(raw.decode("utf-8", errors="replace"))
                if isinstance(value, dict):
                    records.append({"_line": index + 1, **value})
                else:
                    records.append({"_line": index + 1, "value": value})
            except json.JSONDecodeError as exc:
                invalid += 1
                records.append({"_line": index + 1, "_error": str(exc), "_raw": raw[:500].decode("utf-8", errors="replace")})
    return records, {"shown": len(records), "invalid": invalid, "truncated": truncated}


def human_size(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.1f} {unit}"
        value /= 1024
    return str(size)
