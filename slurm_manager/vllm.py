from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

from .files import resolve_allowed


START_MARKER = "# SLURM_MANAGER_ENDPOINTS_START"
END_MARKER = "# SLURM_MANAGER_ENDPOINTS_END"


def infer_vllm_settings(script: str) -> Dict[str, str]:
    """Best-effort extraction of the served model name and public HTTP ports."""
    text = Path(script).read_text(encoding="utf-8", errors="replace")
    active = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    model_matches = re.findall(r"(?:export\s+)?MODEL_NAME\s*=\s*['\"]?([^'\"\s]+)", active)
    model = model_matches[-1] if model_matches else ""
    if "$" in model:
        model = ""

    literal_ports = [int(value) for value in re.findall(r"--port(?:=|\s+)(\d{2,5})\b", active)]
    range_matches = re.findall(r"PORT\s*=\s*\$\(\(\s*(\d{2,5})\s*\+\s*i\s*\)\)", active)
    loop_matches = re.findall(r"for\s+i\s+in\s+\{(\d+)\.\.(\d+)\}", active)
    ports: List[int] = literal_ports
    if range_matches and loop_matches:
        base = int(range_matches[0])
        start, end = map(int, loop_matches[0])
        ports.extend(range(base + start, base + end + 1))
    ports = list(dict.fromkeys(port for port in ports if 1 <= port <= 65535))
    return {"model": model, "ports": ",".join(str(port) for port in ports)}


def find_yaml_endpoint_blocks(target: str, allowed_roots: Iterable[Path]) -> List[Dict[str, object]]:
    path = resolve_allowed(target, allowed_roots, must_exist=True)
    if path.suffix.lower() not in {".yaml", ".yml"}:
        raise ValueError("请选择 .yaml 或 .yml 文件")
    lines = path.read_text(encoding="utf-8").splitlines()
    endpoint_re = re.compile(r"https?://[^\s'\"\],]+(?::\d+)?/v1/(?:chat/completions|completions)")
    key_re = re.compile(r"^(\s*)([A-Za-z_][\w.-]*)\s*:\s*(.*)$")
    blocks: List[Dict[str, object]] = []
    index = 0
    while index < len(lines):
        match = key_re.match(lines[index])
        if not match:
            index += 1
            continue
        indent, key, tail = match.groups()
        if endpoint_re.search(tail):
            blocks.append({"id": f"{index + 1}:{index + 1}", "key": key, "start": index + 1,
                           "end": index + 1, "style": "inline", "text": lines[index]})
            index += 1
            continue
        cursor = index + 1
        endpoint_lines: List[int] = []
        nested_mapping = False
        while cursor < len(lines):
            current = lines[cursor]
            if current.strip() and len(current) - len(current.lstrip()) <= len(indent):
                break
            if current.strip() and key_re.match(current) and not endpoint_lines:
                nested_mapping = True
                break
            if endpoint_re.search(current):
                endpoint_lines.append(cursor)
            cursor += 1
        if endpoint_lines and not nested_mapping:
            blocks.append({"id": f"{index + 1}:{cursor}", "key": key, "start": index + 1,
                           "end": cursor, "style": "block", "text": "\n".join(lines[index:cursor])})
        index = max(index + 1, cursor)
    return blocks


def preview_yaml_endpoint_update(
    target: str, block: Dict[str, object], endpoints: Sequence[str], allowed_roots: Iterable[Path]
) -> str:
    path = resolve_allowed(target, allowed_roots, must_exist=True)
    lines = path.read_text(encoding="utf-8").splitlines()
    start = int(block["start"]) - 1
    end = int(block["end"])
    key_line = lines[start]
    indent = key_line[: len(key_line) - len(key_line.lstrip())]
    key = str(block["key"])
    if block["style"] == "inline":
        replacement = [f"{indent}{key}: " + json.dumps(list(endpoints), ensure_ascii=False)]
    else:
        replacement = [f"{indent}{key}:"] + [f"{indent}  - {json.dumps(item)}" for item in endpoints]
    return "\n".join(lines[:start] + replacement + lines[end:]) + "\n"


def update_yaml_endpoint_block(
    target: str, block: Dict[str, object], endpoints: Sequence[str], allowed_roots: Iterable[Path]
) -> Path:
    path = resolve_allowed(target, allowed_roots, must_exist=True)
    updated = preview_yaml_endpoint_update(target, block, endpoints, allowed_roots)
    backup = path.with_suffix(path.suffix + ".bak." + time.strftime("%Y%m%d-%H%M%S") + f"-{time.time_ns() % 1_000_000:06d}")
    shutil.copy2(str(path), str(backup))
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent), text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(updated)
        os.replace(temporary, str(path))
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return backup


def parse_ports(value: str) -> List[int]:
    ports: List[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start_text, end_text = part.split("-", 1)
            start, end = int(start_text), int(end_text)
            if start > end or end - start > 1000:
                raise ValueError("端口范围无效或过大")
            ports.extend(range(start, end + 1))
        else:
            ports.append(int(part))
    if any(port < 1 or port > 65535 for port in ports):
        raise ValueError("端口必须位于 1-65535")
    return list(dict.fromkeys(ports))


def build_endpoints(addresses: Iterable[str], ports: Iterable[int], path: str = "/v1/chat/completions") -> List[str]:
    normalized = "/" + path.lstrip("/")
    return [f"http://{address}:{port}{normalized}" for address in addresses for port in ports]


def detect_models(endpoints: Sequence[str], token: str = "", timeout: int = 8) -> List[Dict[str, object]]:
    model_urls = []
    for endpoint in endpoints:
        base = re.sub(r"/v1/(?:chat/completions|completions)/?$", "", endpoint.rstrip("/"))
        model_urls.append(base + "/v1/models")
    with ThreadPoolExecutor(max_workers=min(16, max(1, len(model_urls)))) as pool:
        futures = {pool.submit(_models_one, url, token, timeout): url for url in dict.fromkeys(model_urls)}
        return sorted((future.result() for future in as_completed(futures)), key=lambda item: str(item["url"]))


def _models_one(url: str, token: str, timeout: int) -> Dict[str, object]:
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            data = json.loads(response.read(1_000_000).decode("utf-8", errors="replace"))
        models = [str(item.get("id")) for item in data.get("data", []) if isinstance(item, dict) and item.get("id")]
        return {"url": url, "ok": True, "models": models, "error": ""}
    except Exception as exc:
        return {"url": url, "ok": False, "models": [], "error": str(exc)[:300]}


def format_endpoints(endpoints: Sequence[str], style: str = "python", variable: str = "ENDPOINTS") -> str:
    if style == "json":
        return json.dumps(list(endpoints), ensure_ascii=False, indent=2)
    if style == "lines":
        return "\n".join(endpoints)
    values = "\n".join(f"    {json.dumps(item)}," for item in endpoints)
    return f"{variable} = [\n{values}\n]"


def check_endpoints(
    endpoints: Sequence[str],
    model: str,
    token: str = "",
    timeout: int = 15,
    parallelism: int = 16,
) -> List[Dict[str, object]]:
    with ThreadPoolExecutor(max_workers=min(parallelism, max(1, len(endpoints)))) as pool:
        futures = {pool.submit(_check_one, endpoint, model, token, timeout): endpoint for endpoint in endpoints}
        results = [future.result() for future in as_completed(futures)]
    return sorted(results, key=lambda item: str(item["endpoint"]))


def _check_one(endpoint: str, model: str, token: str, timeout: int) -> Dict[str, object]:
    payload = json.dumps(
        {"model": model, "messages": [{"role": "user", "content": "hi"}], "max_tokens": 1}
    ).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = urllib.request.Request(endpoint, data=payload, headers=headers, method="POST")
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read(4096).decode("utf-8", errors="replace")
            return {"endpoint": endpoint, "ok": 200 <= response.status < 300, "status": response.status,
                    "latency_s": round(time.monotonic() - started, 2), "detail": _response_detail(body)}
    except urllib.error.HTTPError as exc:
        body = exc.read(4096).decode("utf-8", errors="replace")
        return {"endpoint": endpoint, "ok": False, "status": exc.code,
                "latency_s": round(time.monotonic() - started, 2), "detail": _response_detail(body)}
    except Exception as exc:
        return {"endpoint": endpoint, "ok": False, "status": "ERROR",
                "latency_s": round(time.monotonic() - started, 2), "detail": str(exc)[:300]}


def _response_detail(body: str) -> str:
    try:
        data = json.loads(body)
        choices = data.get("choices", []) if isinstance(data, dict) else []
        if choices:
            return str(choices[0].get("message", {}).get("content", ""))[:200]
        return str(data.get("message", data))[:200] if isinstance(data, dict) else str(data)[:200]
    except json.JSONDecodeError:
        return body.replace("\n", " ")[:200]


def update_python_endpoint_block(
    target: str,
    endpoints: Sequence[str],
    variable: str,
    allowed_roots: Iterable[Path],
) -> Path:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", variable):
        raise ValueError("Python 变量名无效")
    path = resolve_allowed(target, allowed_roots, must_exist=True)
    if path.suffix != ".py":
        raise ValueError("当前只允许更新 .py 配置文件")
    original = path.read_text(encoding="utf-8")
    if START_MARKER not in original or END_MARKER not in original:
        raise ValueError(f"目标文件必须包含 {START_MARKER} 和 {END_MARKER}")
    if original.index(START_MARKER) > original.index(END_MARKER):
        raise ValueError("endpoint 标记顺序错误")
    block = START_MARKER + "\n" + format_endpoints(endpoints, "python", variable) + "\n" + END_MARKER
    pattern = re.compile(re.escape(START_MARKER) + r".*?" + re.escape(END_MARKER), re.S)
    updated = pattern.sub(block, original, count=1)
    backup = path.with_suffix(path.suffix + ".bak." + time.strftime("%Y%m%d-%H%M%S") + f"-{time.time_ns() % 1_000_000:06d}")
    shutil.copy2(str(path), str(backup))
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent), text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(updated)
        os.replace(temporary, str(path))
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return backup
