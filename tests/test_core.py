import tempfile
import unittest
from pathlib import Path

from slurm_manager.catalog import parse_script
from slurm_manager.auth import auth_cookie_name, make_auth_token, valid_auth_token
from slurm_manager.files import preview_jsonl, resolve_allowed
from slurm_manager.metrics import _cpu_totals, _dcgm_gpu_rows
from slurm_manager.ray import ShellGateway, extract_log_path, normalize_background_command, shell_port
from slurm_manager.runner import CommandResult
from slurm_manager.slurm import SlurmService, SubmitRequest, expand_slurm_log_pattern
from slurm_manager.storage import StateStore
from slurm_manager.vllm import (
    END_MARKER,
    START_MARKER,
    build_endpoints,
    format_endpoints,
    parse_ports,
    update_python_endpoint_block,
    find_yaml_endpoint_blocks,
    infer_vllm_settings,
    preview_yaml_endpoint_update,
)


class FakeRunner:
    def __init__(self, stdout="", returncode=0):
        self.stdout = stdout
        self.returncode = returncode
        self.calls = []

    def run(self, args, **kwargs):
        self.calls.append(list(args))
        return CommandResult(args, self.returncode, self.stdout, "", 0.01)

    @staticmethod
    def available(_binary):
        return True


class SlurmTests(unittest.TestCase):
    def test_parse_nodes(self):
        runner = FakeRunner("node01|idle|2/14/0/16|524288|gpu:8|none|gpu\n")
        rows, result = SlurmService(runner).nodes("gpu")
        self.assertTrue(result.ok)
        self.assertEqual(rows[0]["cpu_total"], 16)
        self.assertEqual(rows[0]["cpu_allocated_pct"], 12.5)
        self.assertEqual(rows[0]["memory_gb"], 512.0)

    def test_submit_uses_argument_vector(self):
        runner = FakeRunner("12345\n")
        with tempfile.TemporaryDirectory() as directory:
            script = Path(directory) / "job.slurm"
            script.write_text("#!/bin/bash\n")
            result = SlurmService(runner).submit(
                SubmitRequest(script, nodelist="node01", partition="gpu", nodes=1, exports={"X": "1"})
            )
        self.assertTrue(result.ok)
        self.assertIn("--chdir=" + str(script.resolve().parent), runner.calls[0])
        self.assertIn("--nodelist=node01", runner.calls[0])
        self.assertIn("--export=ALL,X=1", runner.calls[0])

    def test_log_pattern(self):
        self.assertEqual(expand_slurm_log_pattern("log/%j-%t.log", "42"), "log/42-0.log")


class FileTests(unittest.TestCase):
    def test_allowed_path_and_jsonl(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "sample.jsonl"
            path.write_text('{"a": 1}\nnot-json\n', encoding="utf-8")
            self.assertEqual(resolve_allowed(str(path), [root]), path.resolve())
            self.assertEqual(resolve_allowed(f"  {path}  ", [root]), path.resolve())
            records, stats = preview_jsonl(path, limit=10)
            self.assertEqual(len(records), 2)
            self.assertEqual(stats["invalid"], 1)

    def test_reject_outside_root(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(PermissionError):
                resolve_allowed("/etc/passwd", [Path(directory)])


class UtilityTests(unittest.TestCase):
    def test_auth_token_expiry_and_password_rotation(self):
        token = make_auth_token("secret", now=100)
        self.assertTrue(valid_auth_token(token, "secret", now=100 + 21599))
        self.assertFalse(valid_auth_token(token, "secret", now=100 + 21601))
        self.assertFalse(valid_auth_token(token, "changed", now=101))
        self.assertNotEqual(auth_cookie_name("/a"), auth_cookie_name("/b"))

    def test_catalog(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "vllm.slurm"
            path.write_text(
                "#SBATCH --job-name=test\n#SBATCH --partition=gpu\n#SBATCH --nodes=2\n"
                "#SBATCH --gres=gpu:8\npython -m vllm.entrypoints.openai.api_server\n"
            )
            info = parse_script(path)
            self.assertEqual(info.category, "vllm")
            self.assertEqual(info.gpu, "8")

    def test_endpoints(self):
        ports = parse_ports("8000-8002,9000")
        endpoints = build_endpoints(["10.0.0.1"], ports)
        self.assertEqual(len(endpoints), 4)
        self.assertIn("ENDPOINTS = [", format_endpoints(endpoints))

    def test_safe_endpoint_update_creates_backup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "config.py"
            original = f"before = 1\n{START_MARKER}\nENDPOINTS = []\n{END_MARKER}\nafter = 2\n"
            target.write_text(original, encoding="utf-8")
            backup = update_python_endpoint_block(
                str(target), ["http://127.0.0.1:8000/v1/chat/completions"], "URLS", [root]
            )
            self.assertEqual(backup.read_text(encoding="utf-8"), original)
            self.assertIn("URLS = [", target.read_text(encoding="utf-8"))

    def test_vllm_script_and_yaml_endpoint_detection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = root / "vllm.slurm"
            script.write_text('export MODEL_NAME="demo"\nfor i in {0..2}; do\n PORT=$((8000 + i))\n --port ${PORT}\ndone\n')
            self.assertEqual(infer_vllm_settings(str(script)), {"model": "demo", "ports": "8000,8001,8002"})
            script.write_text('export MODEL_NAME="${MODEL_NAME}"\n--port 8000\n')
            self.assertEqual(infer_vllm_settings(str(script))["model"], "")
            yaml = root / "task.yaml"
            yaml.write_text('service:\n  endpoints:\n    - "http://a:8000/v1/chat/completions"\n    - "http://b:8001/v1/chat/completions"\n')
            blocks = find_yaml_endpoint_blocks(str(yaml), [root])
            self.assertEqual(blocks[0]["key"], "endpoints")
            preview = preview_yaml_endpoint_update(str(yaml), blocks[0], ["http://c:9000/v1/chat/completions"], [root])
            self.assertIn('  endpoints:\n    - "http://c:9000/v1/chat/completions"', preview)

    def test_shell_port_matches_existing_rule(self):
        self.assertEqual(shell_port("131974"), 21974)

    def test_background_command_normalization(self):
        command = "nohup python task.py > task.log 2>&1 &\n"
        self.assertEqual(normalize_background_command(command), "nohup python task.py > task.log 2>&1")
        self.assertEqual(extract_log_path(command), "task.log")

    def test_gateway_returns_inner_session_pid(self):
        class FakeGateway(ShellGateway):
            last_command = ""

            def execute(self, host, port, command, timeout=10):
                self.last_command = command
                return "SLURM_MANAGER_PID=1346\n"

        gateway = FakeGateway()
        result = gateway.submit_command("node", 12345, "python task.py > /tmp/task.log 2>&1")
        self.assertEqual(result.pid, "1346")
        self.assertIn("echo $$", gateway.last_command)
        self.assertIn("setsid", gateway.last_command)

    def test_prometheus_parsing(self):
        first = 'node_cpu_seconds_total{cpu="0",mode="idle"} 80\nnode_cpu_seconds_total{cpu="0",mode="user"} 20\n'
        self.assertEqual(_cpu_totals(first), (100.0, 80.0))
        dcgm = (
            'DCGM_FI_DEV_GPU_UTIL{gpu="0",UUID="x"} 75\n'
            'DCGM_FI_DEV_GPU_TEMP{gpu="0",UUID="x"} 61\n'
        )
        rows = _dcgm_gpu_rows(dcgm)
        self.assertEqual(rows[0]["util_pct"], 75.0)
        self.assertEqual(rows[0]["temperature_c"], 61.0)

    def test_preset_and_compute_task_storage(self):
        with tempfile.TemporaryDirectory() as directory:
            store = StateStore(Path(directory) / "state.db")
            preset_id = store.save_preset("metrics", "gpu nodes", {"nodes": "n1,n2"})
            self.assertEqual(store.presets("metrics")[0]["payload"]["nodes"], "n1,n2")
            store.save_preset("metrics", "gpu nodes edited", {"nodes": "n3"}, preset_id)
            self.assertEqual(store.presets("metrics")[0]["name"], "gpu nodes edited")
            task_id = store.add_compute_task("task", "normal", "42", "n1", 0, "python x.py", "123")
            resource_id = store.add_task_resource(task_id, "log", "main", "/tmp/main.log")
            self.assertEqual(store.task_resources(task_id)[0]["id"], resource_id)
            store.update_compute_task(task_id, "RUNNING")
            self.assertEqual(store.compute_task(task_id)["status"], "RUNNING")
            self.assertEqual(store.setting("quick_script", "fallback"), "fallback")
            store.set_setting("quick_script", "/tmp/job.slurm")
            self.assertEqual(store.setting("quick_script"), "/tmp/job.slurm")
            quick_id = store.save_quick_job(
                "100", "vLLM", "server", "RUNNING", "gpu", 2, "n1,n2",
                "/tmp/vllm.slurm", {"script_path": "/tmp/vllm.slurm", "nodes": 2}, "quick",
            )
            self.assertEqual(store.quick_jobs()[0]["payload"]["nodes"], 2)
            store.update_quick_job_state(quick_id, "CANCELLED")
            self.assertEqual(store.quick_jobs()[0]["state"], "CANCELLED")
            store.save_quick_job(
                "101", "vLLM", "server", "SUBMITTED", "gpu", 2, "n1,n2",
                "/tmp/vllm.slurm", {"script_path": "/tmp/vllm.slurm", "nodes": 2},
                "quick", quick_id,
            )
            self.assertEqual(store.quick_jobs()[0]["slurm_job_id"], "101")
            store.delete_quick_job(quick_id)
            self.assertEqual(store.quick_jobs(), [])


if __name__ == "__main__":
    unittest.main()
