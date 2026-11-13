import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from slurm_manager.probe_keepalive import ProbeKeepalive
from slurm_manager.storage import StateStore


class FakeSlurm:
    def node_addresses(self):
        return {"node1": "127.0.0.1"}


class FakeMetrics:
    def collect(self, partition, nodes, addresses, method, timeout):
        return [{"node": node, "gpu_count": 8, "gpu_max_util_pct": 0.0,
                 "source": "exporter", "error": ""} for node in nodes]


class ProbeKeepaliveTests(unittest.TestCase):
    def test_scan_probe_and_pressure_run_in_order_and_persist(self):
        with tempfile.TemporaryDirectory() as directory:
            store = StateStore(Path(directory) / "state.db")
            endpoint = {"address": "127.0.0.1", "port": 8000,
                        "url": "http://127.0.0.1:8000/v1/chat/completions",
                        "model": "example", "kind": "chat", "enabled": True}
            with patch("slurm_manager.probe_keepalive._probe_endpoint", return_value=endpoint), \
                 patch.object(ProbeKeepalive, "_pressure_once", lambda self: None):
                probe = ProbeKeepalive(store, FakeSlurm(), FakeMetrics())
                probe.start("gpu", ["node1"], 0, "8000", 50, duration_seconds=0,
                            interval_seconds=1)
                try:
                    deadline = time.monotonic() + 3
                    while time.monotonic() < deadline and probe.snapshot().get("phase") != "stressing":
                        time.sleep(0.02)
                    state = probe.snapshot()
                    self.assertEqual(state["phase"], "stressing")
                    self.assertEqual(state["selected_nodes"], ["node1"])
                    self.assertEqual(state["endpoints"][0]["model"], "example")
                    self.assertEqual(store.setting("probe_vllm_keepalive") != "", True)
                finally:
                    probe.stop()


if __name__ == "__main__":
    unittest.main()
