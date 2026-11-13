"""Persistent, cancellable vLLM endpoint discovery and adaptive load probing."""

from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Mapping, Sequence

from .metrics import MetricsService
from .slurm import SlurmService
from .storage import StateStore
from .vllm import parse_ports


SETTING = "probe_vllm_keepalive"


def _request(url: str, payload: dict | None = None, timeout: int = 5) -> tuple[int, dict]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        url, data=data,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read(65536)
            try:
                body = json.loads(raw)
            except (ValueError, UnicodeDecodeError):
                body = {}
            return response.status, body if isinstance(body, dict) else {}
    except urllib.error.HTTPError as exc:
        return exc.code, {}


def _probe_endpoint(address: str, port: int) -> dict | None:
    base = f"http://{address}:{port}"
    try:
        status, data = _request(base + "/v1/models", timeout=3)
        model = next((str(item["id"]) for item in data.get("data", [])
                      if isinstance(item, dict) and item.get("id")), "") if status == 200 else ""
        if not model:
            # Health routes establish reachability but do not identify a model.
            health = any(_request(base + path, timeout=2)[0] < 400 for path in ("/health", "/ready"))
            if not health:
                return None
        for path, payload in (
            ("/v1/chat/completions", {"model": model, "messages": [{"role": "user", "content": "你好"}], "max_tokens": 1}),
            ("/v1/completions", {"model": model, "prompt": "你好", "max_tokens": 1}),
            ("/v1/decide", {"kind": "choice", "state": "网页正文。版权所有。", "question": "是否有模板噪音？", "options": ["无", "有"]}),
        ):
            if not model and path != "/v1/decide":
                continue
            try:
                status, _ = _request(base + path, payload, timeout=5)
                if 200 <= status < 300:
                    kind = "chat" if path == "/v1/chat/completions" else "completion" if path == "/v1/completions" else "decide"
                    return {"address": address, "port": port, "url": base + path, "model": model, "kind": kind, "enabled": True, "requests": 0, "errors": 0}
            except (OSError, TimeoutError):
                continue
    except (OSError, TimeoutError):
        return None
    return None


def _load_payload(endpoint: Mapping[str, Any]) -> dict:
    kind = endpoint.get("kind")
    model = str(endpoint.get("model", ""))
    if kind == "chat":
        return {"model": model, "messages": [{"role": "user", "content": "请简要解释大语言模型训练数据中的网页模板噪音，并给出一个例子。"}], "temperature": 0.2, "max_tokens": 128, "chat_template_kwargs": {"enable_thinking": False}}
    if kind == "completion":
        return {"model": model, "prompt": "本文介绍大语言模型的数据预处理方法。", "max_tokens": 128, "temperature": 0}
    return {"kind": "choice", "state": "首页 > 科技 > 人工智能。本文介绍大语言模型的数据预处理方法。版权所有 2026。", "question": "这段文本最明显的质量问题是什么？", "options": ["无明显问题", "主题跳变", "网页模板噪音"]}


class ProbeKeepalive:
    def __init__(self, store: StateStore, slurm: SlurmService, metrics: MetricsService):
        self.store, self.slurm, self.metrics = store, slurm, metrics
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.wake_event = threading.Event()
        self.worker: threading.Thread | None = None
        try:
            self.state: Dict[str, Any] = json.loads(store.setting(SETTING, "{}"))
        except ValueError:
            self.state = {}
        if self.state.get("phase") in {"scanning", "probing", "stressing"}:
            self._launch()

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return json.loads(json.dumps(self.state))

    def _save(self) -> None:
        self.state["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        self.store.set_setting(SETTING, json.dumps(self.state, ensure_ascii=False))

    def _launch(self) -> None:
        if self.worker and self.worker.is_alive():
            if self.stop_event.is_set():
                raise RuntimeError("上一次探针正在停止，请稍后再试")
            return
        self.stop_event.clear()
        self.worker = threading.Thread(target=self._run, name="slurm-manager-vllm-probe", daemon=True)
        self.worker.start()

    def start(self, partition: str, nodes: Sequence[str], threshold: float, ports: str,
              target_util: float, duration_seconds: int = 3600, interval_seconds: int = 60) -> None:
        if not partition or not nodes or not parse_ports(ports):
            raise ValueError("请选择 Partition、候选节点和探测端口")
        with self.lock:
            if self.worker and self.worker.is_alive():
                raise RuntimeError("请先停止当前探针")
            self.state = {
                "phase": "scanning", "partition": partition, "nodes": list(dict.fromkeys(nodes)),
                "threshold": float(threshold), "ports": ports, "target_util": float(target_util),
                "duration_seconds": int(duration_seconds), "interval_seconds": int(interval_seconds),
                "since": {}, "samples": {}, "selected_nodes": [], "endpoints": [], "error": "",
                "auto_run": True,
            }
            self._save()
            self._launch()

    def stop(self) -> None:
        self.stop_event.set()
        self.wake_event.set()
        with self.lock:
            self.state["phase"] = "stopped"
            self._save()

    def select_nodes(self, nodes: Sequence[str]) -> None:
        with self.lock:
            chosen = set(nodes)
            if not chosen or not chosen.issubset(set(self.state.get("nodes", []))):
                raise ValueError("请选择当前探针中的候选节点")
            if self.worker and self.worker.is_alive() and self.stop_event.is_set():
                raise RuntimeError("探针正在停止，请稍后再选择")
            self.stop_event.clear()
            self.state["selected_nodes"] = list(nodes)
            self.state["phase"] = "probing"
            self._save()
            self._launch()
            self.wake_event.set()

    def set_endpoints(self, endpoints: Sequence[Mapping[str, Any]]) -> None:
        if not self.state.get("selected_nodes"):
            raise ValueError("请先固化保活节点")
        clean = []
        addresses = self.slurm.node_addresses()
        nodes_by_address = {address: node for node, address in addresses.items()}
        for item in endpoints:
            url = str(item.get("url", "")).strip()
            if not url.startswith("http://") or not any(url.endswith(path) for path in
                ("/v1/chat/completions", "/v1/completions", "/v1/decide")):
                raise ValueError("接口须为 http:// 地址，且使用支持的请求路径")
            address = urllib.parse.urlsplit(url).hostname or ""
            clean.append({"url": url, "address": address,
                          "node": nodes_by_address.get(address, address),
                          "model": str(item.get("model", "")),
                          "kind": "chat" if url.endswith("/chat/completions") else "completion" if url.endswith("/completions") else "decide",
                          "enabled": bool(item.get("enabled", True)), "requests": 0, "errors": 0})
        with self.lock:
            self.state["endpoints"] = clean
            self.state["phase"] = "ready"
            self._save()
            self.wake_event.set()

    def start_pressure(self) -> None:
        with self.lock:
            if self.worker and self.worker.is_alive() and self.stop_event.is_set():
                raise RuntimeError("探针正在停止，请稍后再启动")
            self.stop_event.clear()
            if not any(item.get("enabled") for item in self.state.get("endpoints", [])):
                raise ValueError("没有已启用的接口")
            self.state["phase"] = "stressing"
            self._save()
            self._launch()
            self.wake_event.set()

    def _sample(self, nodes: Sequence[str]) -> Dict[str, dict]:
        address_map = self.slurm.node_addresses()
        records = self.metrics.collect(self.state["partition"], nodes, address_map, "direct", 8)
        return {str(item["node"]): item for item in records}

    def _scan_once(self) -> None:
        now = time.time()
        records = self._sample(self.state["nodes"])
        with self.lock:
            if self.stop_event.is_set() or self.state.get("phase") != "scanning":
                return
            since = self.state["since"]
            for node in self.state["nodes"]:
                item = records.get(node, {})
                value = item.get("gpu_max_util_pct")
                count = int(item.get("gpu_count") or 0)
                valid = count > 0 and value is not None and float(value) <= self.state["threshold"]
                self.state["samples"][node] = {"gpu_count": count, "max_util_pct": value,
                                                 "source": item.get("source", ""), "error": item.get("error", "")}
                if valid:
                    since.setdefault(node, now)
                else:
                    since.pop(node, None)
            eligible = [node for node, start in since.items() if now - start >= self.state["duration_seconds"]]
            if eligible:
                self.state["selected_nodes"] = eligible
                self.state["phase"] = "probing"
            self._save()

    def _probe(self) -> None:
        addresses = self.slurm.node_addresses()
        tasks = [(node, addresses.get(node, node), port)
                 for node in self.state["selected_nodes"] for port in parse_ports(self.state["ports"])]
        discovered = []
        with ThreadPoolExecutor(max_workers=min(16, max(1, len(tasks)))) as pool:
            futures = {pool.submit(_probe_endpoint, address, port): node for node, address, port in tasks}
            for future in as_completed(futures):
                if self.stop_event.is_set():
                    return
                item = future.result()
                if item:
                    item["node"] = futures[future]
                    discovered.append(item)
        with self.lock:
            if self.stop_event.is_set() or self.state.get("phase") != "probing":
                return
            self.state["endpoints"] = sorted(discovered, key=lambda item: (item["node"], item["port"]))
            self.state["phase"] = "stressing" if discovered and self.state.get("auto_run") else "ready"
            self._save()

    def _pressure_once(self) -> None:
        with self.lock:
            endpoints = [dict(item) for item in self.state["endpoints"] if item.get("enabled")]
            rate = float(self.state.get("rate", 1.0))
        if not endpoints:
            raise RuntimeError("没有已启用的接口")
        records = self._sample(self.state["selected_nodes"])
        values = [float(item["gpu_max_util_pct"]) for item in records.values()
                  if item.get("gpu_max_util_pct") is not None]
        if values:
            observed = max(values)
            target = float(self.state["target_util"])
            rate = min(8.0, rate + 0.25) if observed < target else max(0.1, rate * 0.7)
        else:
            rate = min(rate, 0.2)  # Avoid unbounded load when GPU feedback disappears.
        batch = max(1, min(40, round(rate * 5)))
        results = []
        with ThreadPoolExecutor(max_workers=min(8, batch)) as pool:
            futures = [pool.submit(_request, item["url"], _load_payload(item), 20)
                       for item in (endpoints * (batch // len(endpoints) + 1))[:batch]]
            for future in as_completed(futures):
                try:
                    status, _ = future.result()
                    results.append(200 <= status < 300)
                except (OSError, TimeoutError):
                    results.append(False)
        with self.lock:
            self.state["rate"] = rate
            self.state["observed_gpu_util_pct"] = max(values) if values else None
            self.state["requests"] = int(self.state.get("requests", 0)) + len(results)
            self.state["errors"] = int(self.state.get("errors", 0)) + results.count(False)
            self._save()

    def _run(self) -> None:
        while not self.stop_event.is_set():
            try:
                with self.lock:
                    phase = self.state.get("phase")
                if phase == "scanning":
                    self._scan_once()
                    with self.lock:
                        still_scanning = self.state.get("phase") == "scanning"
                        interval = int(self.state["interval_seconds"])
                    if still_scanning:
                        self.wake_event.wait(interval)
                        self.wake_event.clear()
                elif phase == "probing":
                    self._probe()
                elif phase == "stressing":
                    self._pressure_once()
                    if self.stop_event.wait(5):
                        break
                else:
                    break
            except Exception as exc:
                with self.lock:
                    self.state["error"] = str(exc)[:500]
                    self.state["failed_phase"] = phase
                    self.state["phase"] = "error"
                    self._save()
                break
