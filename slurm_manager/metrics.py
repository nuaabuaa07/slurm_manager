from __future__ import annotations

import re
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

from .runner import CommandRunner


REMOTE_METRICS_SCRIPT = r"""
read -r _ u1 n1 s1 i1 w1 irq1 sirq1 st1 _ < /proc/stat
t1=$((u1+n1+s1+i1+w1+irq1+sirq1+st1)); idle1=$((i1+w1))
sleep 1
read -r _ u2 n2 s2 i2 w2 irq2 sirq2 st2 _ < /proc/stat
t2=$((u2+n2+s2+i2+w2+irq2+sirq2+st2)); idle2=$((i2+w2))
dt=$((t2-t1)); di=$((idle2-idle1))
awk -v dt="$dt" -v di="$di" 'BEGIN { if (dt > 0) printf "CPU_UTIL=%.1f\n", 100*(dt-di)/dt; else print "CPU_UTIL=NA" }'
awk '/MemTotal:/ {t=$2} /MemAvailable:/ {a=$2} END {if(t>0) printf "MEM_UTIL=%.1f\n",100*(t-a)/t}' /proc/meminfo
awk '{printf "LOAD1=%s\n",$1}' /proc/loadavg
if command -v nvidia-smi >/dev/null 2>&1; then
  nvidia-smi --query-gpu=index,utilization.gpu,memory.used,memory.total,temperature.gpu --format=csv,noheader,nounits 2>/dev/null | sed 's/^/GPU=/'
fi
""".strip()


class MetricsService:
    def __init__(self, runner: CommandRunner, timeout: int = 20, parallelism: int = 8):
        self.runner = runner
        self.timeout = timeout
        self.parallelism = parallelism

    def collect(
        self,
        partition: str,
        nodes: Iterable[str],
        addresses: Optional[Mapping[str, str]] = None,
        method: str = "auto",
        timeout: Optional[int] = None,
    ) -> List[Dict[str, object]]:
        node_list = list(dict.fromkeys(nodes))
        address_map = dict(addresses or {})
        effective_timeout = int(timeout or self.timeout)
        output: List[Dict[str, object]] = []
        with ThreadPoolExecutor(max_workers=min(self.parallelism, max(1, len(node_list)))) as pool:
            futures = {
                pool.submit(self._one, partition, node, address_map.get(node, node), method, effective_timeout): node
                for node in node_list
            }
            for future in as_completed(futures):
                output.append(future.result())
        return sorted(output, key=lambda item: str(item["node"]))

    def _one(self, partition: str, node: str, address: str, method: str, timeout: int) -> Dict[str, object]:
        record = _empty_record(node, address)
        direct_errors: List[str] = []
        if method in {"auto", "direct"}:
            self._direct(address, record, min(timeout, 8), direct_errors)
            if _has_metrics(record):
                record["ok"] = True
                record["source"] = "exporter"
            if direct_errors:
                record["error"] = "; ".join(direct_errors)
        if method == "srun" or (method == "auto" and not _has_metrics(record)):
            srun_record = self._srun(partition, node, timeout)
            if srun_record["ok"] or _has_metrics(srun_record):
                record = srun_record
                record["address"] = address
            elif direct_errors:
                record["error"] = "; ".join(direct_errors + [str(srun_record["error"])])
        return record

    def _direct(self, address: str, record: Dict[str, object], timeout: int, errors: List[str]) -> None:
        try:
            first = _fetch_text(f"http://{address}:9100/metrics", timeout)
            first_cpu = _cpu_totals(first)
            time.sleep(1)
            second = _fetch_text(f"http://{address}:9100/metrics", timeout)
            second_cpu = _cpu_totals(second)
            if first_cpu and second_cpu:
                delta_total = second_cpu[0] - first_cpu[0]
                delta_idle = second_cpu[1] - first_cpu[1]
                if delta_total > 0:
                    record["cpu_util_pct"] = round(100 * (delta_total - delta_idle) / delta_total, 1)
            memory = _metric_value(second, "node_memory_MemTotal_bytes")
            available = _metric_value(second, "node_memory_MemAvailable_bytes")
            if memory and available is not None:
                record["memory_util_pct"] = round(100 * (memory - available) / memory, 1)
            record["load1"] = _metric_value(second, "node_load1")
        except Exception as exc:
            errors.append("node-exporter: " + str(exc)[:160])
        try:
            dcgm = _fetch_text(f"http://{address}:9400/metrics", timeout)
            gpu_rows = _dcgm_gpu_rows(dcgm)
            record["gpu_detail"] = gpu_rows
            record["gpu_count"] = len(gpu_rows)
            utils = [float(row["util_pct"]) for row in gpu_rows if row.get("util_pct") is not None]
            if utils:
                record["gpu_avg_util_pct"] = round(sum(utils) / len(utils), 1)
                record["gpu_max_util_pct"] = max(utils)
        except Exception as exc:
            errors.append("dcgm-exporter: " + str(exc)[:160])

    def _srun(self, partition: str, node: str, timeout: int) -> Dict[str, object]:
        result = self.runner.run(
            [
                "srun", "--immediate=3", "--quiet", "--partition", partition,
                "--nodelist", node, "--nodes", "1", "--ntasks", "1",
                "bash", "-lc", REMOTE_METRICS_SCRIPT,
            ],
            timeout=timeout,
        )
        record = _empty_record(node, node)
        record["source"] = "srun"
        record["ok"] = result.ok
        record["error"] = result.stderr.strip()
        gpu_utils: List[float] = []
        for line in result.stdout.splitlines():
            if line.startswith("CPU_UTIL="):
                record["cpu_util_pct"] = _float(line.split("=", 1)[1])
            elif line.startswith("MEM_UTIL="):
                record["memory_util_pct"] = _float(line.split("=", 1)[1])
            elif line.startswith("LOAD1="):
                record["load1"] = _float(line.split("=", 1)[1])
            elif line.startswith("GPU="):
                parts = [part.strip() for part in line[4:].split(",")]
                if len(parts) >= 5:
                    util = _float(parts[1])
                    if util is not None:
                        gpu_utils.append(util)
                    record["gpu_detail"].append(
                        {"index": parts[0], "util_pct": util, "memory_used_mb": _float(parts[2]),
                         "memory_total_mb": _float(parts[3]), "temperature_c": _float(parts[4])}
                    )
        record["gpu_count"] = len(record["gpu_detail"])
        if gpu_utils:
            record["gpu_avg_util_pct"] = round(sum(gpu_utils) / len(gpu_utils), 1)
            record["gpu_max_util_pct"] = max(gpu_utils)
        return record


def _empty_record(node: str, address: str) -> Dict[str, object]:
    return {
        "node": node, "address": address, "ok": False, "source": "-",
        "cpu_util_pct": None, "memory_util_pct": None, "load1": None,
        "gpu_avg_util_pct": None, "gpu_max_util_pct": None, "gpu_count": 0,
        "gpu_detail": [], "error": "",
    }


def _has_metrics(record: Mapping[str, object]) -> bool:
    return any(record.get(key) is not None for key in ("cpu_util_pct", "memory_util_pct", "gpu_avg_util_pct"))


def _fetch_text(url: str, timeout: int) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": "slurm-manager/0.2"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read(10_000_000).decode("utf-8", errors="replace")


def _cpu_totals(text: str) -> Optional[Tuple[float, float]]:
    total = 0.0
    idle = 0.0
    found = False
    for line in text.splitlines():
        if not line.startswith("node_cpu_seconds_total{"):
            continue
        try:
            value = float(line.rsplit(" ", 1)[1])
        except (IndexError, ValueError):
            continue
        found = True
        total += value
        if 'mode="idle"' in line or 'mode="iowait"' in line:
            idle += value
    return (total, idle) if found else None


def _metric_value(text: str, name: str) -> Optional[float]:
    for line in text.splitlines():
        if line.startswith(name + " ") or line.startswith(name + "{"):
            try:
                return float(line.rsplit(" ", 1)[1])
            except (IndexError, ValueError):
                continue
    return None


def _dcgm_gpu_rows(text: str) -> List[Dict[str, object]]:
    rows: Dict[str, Dict[str, object]] = {}
    metric_map = {
        "DCGM_FI_DEV_GPU_UTIL": "util_pct",
        "DCGM_FI_DEV_FB_USED": "memory_used_mb",
        "DCGM_FI_DEV_FB_TOTAL": "memory_total_mb",
        "DCGM_FI_DEV_GPU_TEMP": "temperature_c",
    }
    for line in text.splitlines():
        metric = next((name for name in metric_map if line.startswith(name + "{")), None)
        if not metric:
            continue
        match = re.search(r'(?:gpu|device)="([^"]+)"', line)
        if not match:
            continue
        try:
            value = float(line.rsplit(" ", 1)[1])
        except (IndexError, ValueError):
            continue
        index = match.group(1)
        rows.setdefault(index, {"index": index})[metric_map[metric]] = value
    return [rows[key] for key in sorted(rows, key=lambda value: (0, int(value)) if value.isdigit() else (1, value))]


def _float(value: str):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
