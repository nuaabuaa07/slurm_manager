from __future__ import annotations

import hmac
import html
import json
import os
import re
import shlex
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

import streamlit as st
import streamlit.components.v1 as components

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:  # pragma: no cover
    st_autorefresh = None

from slurm_manager.catalog import ScriptInfo, discover_scripts
from slurm_manager.auth import auth_cookie_name, make_auth_token, valid_auth_token
from slurm_manager.config import PROJECT_ROOT, load_config
from slurm_manager.files import find_jsonl, list_directory, preview_jsonl, resolve_allowed
from slurm_manager.metrics import MetricsService
from slurm_manager.ray import ShellGateway, extract_log_path, shell_port
from slurm_manager.runner import CommandResult, CommandRunner
from slurm_manager.slurm import SlurmService, SubmitRequest, expand_slurm_log_pattern, tail_text
from slurm_manager.storage import StateStore
from slurm_manager.vllm import (
    build_endpoints,
    check_endpoints,
    detect_models,
    find_yaml_endpoint_blocks,
    format_endpoints,
    infer_vllm_settings,
    parse_ports,
    preview_yaml_endpoint_update,
    select_healthy_endpoints,
    update_python_endpoint_block,
    update_yaml_endpoint_block,
)


st.set_page_config(page_title="AIHPC · Slurm Manager", page_icon="◫", layout="wide")
st.markdown(
    """
    <style>
      .block-container {padding-top: 1.35rem; padding-bottom: 3rem; max-width: 1500px;}
      [data-testid="stMetric"] {background:#f7f8fa;border:1px solid #e7e9ed;padding:12px;border-radius:10px;}
      div[data-testid="stCodeBlock"] code {font-size:.82rem;}
      .stButton button {min-height:2.35rem;}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def services():
    config = load_config()
    runner = CommandRunner(int(config.get("command_timeout_seconds", 30)))
    slurm = SlurmService(runner)
    metrics = MetricsService(
        runner,
        timeout=int(config.get("metrics_timeout_seconds", 20)),
        parallelism=int(config.get("metrics_parallelism", 8)),
    )
    store = StateStore(Path(config.get("state_db")))
    return config, runner, slurm, metrics, store, ShellGateway()


CONFIG, RUNNER, SLURM, METRICS, STORE, GATEWAY = services()


def auth_query_token() -> str:
    try:
        if hasattr(st, "query_params"):
            value = st.query_params.get("_sm_auth", "")
        else:
            value = st.experimental_get_query_params().get("_sm_auth", "")
        if isinstance(value, list):
            return str(value[0]) if value else ""
        return str(value or "")
    except Exception:
        return ""


def set_auth_query_token(token: str) -> None:
    if hasattr(st, "query_params"):
        st.query_params["_sm_auth"] = token
    else:
        st.experimental_set_query_params(_sm_auth=token)


def set_auth_cookie(cookie_name: str, token: str, max_age: int) -> None:
    html = (
        "<script>document.cookie="
        + json.dumps(f"{cookie_name}={token}; Path=/; Max-Age={max_age}; SameSite=Lax")
        + ";</script>"
    )
    if hasattr(st, "iframe"):
        st.iframe(html, height=1, width=1)
    else:
        components.html(html, height=0)


def authenticate() -> None:
    expected = os.environ.get("SLURM_MANAGER_PASSWORD") or str(CONFIG.get("default_password", "fghjfghj"))
    max_age = max(300, int(float(CONFIG.get("auth_session_hours", 6)) * 3600))
    cookie_name = auth_cookie_name(str(PROJECT_ROOT))
    cookie_value = ""
    can_read_cookie = False
    try:
        cookie_value = str(st.context.cookies.get(cookie_name, ""))
        can_read_cookie = True
    except Exception:
        pass
    query_value = auth_query_token()
    if valid_auth_token(cookie_value, expected, max_age) or valid_auth_token(query_value, expected, max_age):
        st.session_state.authenticated = True
    if st.session_state.get("authenticated"):
        return
    st.title("AIHPC · Slurm Manager")
    supplied = st.text_input("访问密码", type="password")
    login_clicked = st.button("登录", type="primary")
    if login_clicked and hmac.compare_digest(supplied, expected):
        token = make_auth_token(expected)
        set_auth_cookie(cookie_name, token, max_age)
        # Streamlit versions without st.context cannot read browser cookies.
        # Keep the same signed, expiring token in the URL as a compatibility fallback.
        if not can_read_cookie:
            set_auth_query_token(token)
        st.session_state.authenticated = True
        st.success(f"登录成功；本浏览器 {max_age / 3600:g} 小时内刷新无需重新输入密码。")
        return
    if login_clicked:
        st.error("密码不正确")
    st.stop()


authenticate()


def error_from(result: CommandResult, title: str = "命令执行失败") -> None:
    st.error(f"{title}（exit={result.returncode}）")
    if result.stderr:
        st.code(result.stderr, language="text")


def split_values(text: str) -> List[str]:
    return [item.strip() for item in text.replace("\n", ",").split(",") if item.strip()]


def scroll_text(text: str, key: str, follow_bottom: bool = True, height: int = 420) -> None:
    """Render a tail-style log viewer and preserve position when follow is disabled."""
    storage_key = json.dumps("slurm-manager-log-" + key, ensure_ascii=False)
    follow_value = "true" if follow_bottom else "false"
    document = (
        "<!doctype html><html><head><style>"
        "html,body{margin:0;padding:0;background:#f6f8fa;color:#24292f;overflow:hidden;}"
        "pre{box-sizing:border-box;margin:0;width:100%;height:100vh;overflow:auto;"
        "white-space:pre-wrap;word-break:break-word;border:1px solid #dfe2e5;"
        "border-radius:6px;padding:12px;font:13px/1.45 ui-monospace,SFMono-Regular,Consolas,monospace;}"
        "</style></head><body><pre id='log'>" + html.escape(text) + "</pre>"
        "<script>"
        "const base=" + storage_key + ";const log=document.getElementById('log');"
        "const follow=" + follow_value + ";let restoring=true;"
        "function save(name,value){try{sessionStorage.setItem(base+'-'+name,String(value));}catch(e){}}"
        "function toBottom(){restoring=true;log.scrollTop=log.scrollHeight;requestAnimationFrame(()=>{restoring=false;});}"
        "requestAnimationFrame(()=>{if(follow){toBottom();}else{"
        "let position=0;try{position=Number(sessionStorage.getItem(base+'-scroll')||0);}catch(e){}"
        "log.scrollTop=position;restoring=false;}});"
        "log.addEventListener('scroll',()=>{if(!restoring&&!follow)save('scroll',log.scrollTop);});"
        "</script></body></html>"
    )
    components.html(document, height=height, scrolling=False)


def log_tail_controls(key: str, interval_ms: int = 5000) -> bool:
    """Render the two common stateful switches and return follow-bottom state."""
    refresh_column, follow_column = st.columns(2)
    auto = refresh_column.toggle(
        f"自动刷新（{interval_ms // 1000} 秒）", value=True, key="log_auto_refresh_" + key
    )
    follow = follow_column.toggle("跟随底部", value=True, key="log_follow_bottom_" + key)
    if auto:
        if st_autorefresh:
            st_autorefresh(interval=interval_ms, key="log_refresh_timer_" + key)
        else:
            st.warning("当前环境未安装 streamlit-autorefresh，无法自动刷新日志。")
    return bool(follow)


def parse_exports(text: str) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for raw in text.splitlines():
        raw = raw.strip()
        if not raw or raw.startswith("#"):
            continue
        if "=" not in raw:
            raise ValueError(f"环境变量必须使用 KEY=VALUE：{raw}")
        key, value = raw.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def script_catalog(category: str = "") -> List[ScriptInfo]:
    scripts = discover_scripts(CONFIG.paths("script_roots"), bool(CONFIG.get("show_backup_scripts", False)))
    return [item for item in scripts if not category or item.category == category]


def script_label(item: ScriptInfo) -> str:
    return f"{item.name} · {item.partition or '-'} · {item.nodes or '?'} nodes · {item.gpu} GPU/node"


def prepare_slurm_output_directories(script: Path) -> None:
    text = script.read_text(encoding="utf-8", errors="replace")
    for match in re.finditer(r"^#SBATCH\s+--(?:output|error)(?:=|\s+)(\S+)", text, re.MULTILINE):
        raw = match.group(1).strip("'\"")
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = script.parent / path
        parent = path.parent
        # Relative Slurm logs are resolved against the submit working directory.
        # They must exist before the batch script itself starts running.
        if not any(token in str(parent) for token in ("%j", "%A", "%a", "%x", "%N")):
            parent.mkdir(parents=True, exist_ok=True)


def vllm_override_script(script: Path, payload: Mapping[str, Any]) -> tuple[Path, Optional[Path]]:
    overrides = {
        "MODEL_PATH": str(payload.get("model_path", "")).strip(),
        "MODEL_NAME": str(payload.get("model_name", "")).strip(),
    }
    overrides = {key: value for key, value in overrides.items() if value}
    if not overrides:
        return script, None
    text = script.read_text(encoding="utf-8", errors="replace")
    for variable, value in overrides.items():
        pattern = re.compile(rf"^(\s*(?:export\s+)?{variable}\s*=).*$", re.MULTILINE)
        if not pattern.search(text):
            raise ValueError(f"Slurm 脚本中没有找到可覆盖的 {variable}")
        text = pattern.sub(lambda match: match.group(1) + shlex.quote(value), text, count=1)
    generated_dir = Path(CONFIG.get("state_db")).parent / "generated"
    generated_dir.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".slurm", prefix=script.stem + "-", dir=generated_dir, delete=False
    )
    with handle:
        handle.write(text)
    generated = Path(handle.name)
    return generated, generated


def validate_submit_selection(script: Path, payload: Mapping[str, Any]) -> bool:
    info = next((item for item in script_catalog() if Path(item.path) == script), None)
    partition = str(payload.get("partition", "")) or (info.partition if info else "")
    nodelist = str(payload.get("nodelist", "")) or (info.nodelist if info else "")
    node_count = int(payload.get("nodes", 1))
    if not partition or not nodelist:
        return True
    requested, expand_result = SLURM.expand_nodelist(nodelist)
    if expand_result.ok and requested and len(requested) != node_count:
        st.warning(f"当前选择了 {len(requested)} 个候选节点，提交节点数手工设置为 {node_count}。将按你的手工节点数提交。")
    partition_rows, nodes_result = SLURM.nodes(partition)
    if nodes_result.ok:
        allowed = {str(row["node"]) for row in partition_rows}
        outside = [node for node in requested if node not in allowed]
        if outside:
            st.error(f"节点与 Partition 不匹配：{', '.join(outside)} 不属于 {partition}。请更新旧预设后再提交。")
            return False
    return True


def job_snapshot_path(job_id: str, script: Path) -> Path:
    root = Path(str(CONFIG.get("job_snapshot_dir", Path(CONFIG.get("state_db")).parent / "job_snapshots")))
    directory = root / job_id
    directory.mkdir(parents=True, exist_ok=True)
    return directory / (script.name if script.name else "job.slurm")


def allowed_restart_roots() -> List[Path]:
    roots = CONFIG.paths("script_roots") + CONFIG.paths("restart_script_roots")
    roots.append(Path(str(CONFIG.get("job_snapshot_dir"))).resolve(strict=False))
    return list(dict.fromkeys(path.resolve(strict=False) for path in roots))


def validate_submit_command(command: str, cwd: str) -> tuple[List[str], Path, Path]:
    if not command.strip():
        raise ValueError("提交命令为空")
    if any(token in command for token in ("\x00", "\n", "\r")):
        raise ValueError("提交命令必须是单行 sbatch 命令")
    try:
        args = shlex.split(command, posix=True)
    except ValueError as exc:
        raise ValueError("提交命令引号不完整：" + str(exc)) from exc
    if not args or Path(args[0]).name != "sbatch":
        raise ValueError("提交命令必须以 sbatch 开头")
    if any(item in {";", "&&", "||", "|", ">", ">>", "<"} for item in args):
        raise ValueError("提交命令不能包含 shell 连接符或重定向")
    if any(item == "--wrap" or item.startswith("--wrap=") for item in args[1:]):
        raise ValueError("安全限制：不支持 sbatch --wrap，请使用 Slurm 脚本")
    if len(args) < 2 or args[-1].startswith("-"):
        raise ValueError("命令最后必须是 Slurm 脚本路径")
    workdir_text = cwd.strip() or str(PROJECT_ROOT)
    workdir = resolve_allowed(
        workdir_text,
        CONFIG.paths("browse_roots") + CONFIG.paths("restart_script_roots") + CONFIG.paths("script_roots"),
        must_exist=True,
    )
    if not workdir.is_dir():
        raise ValueError("提交工作目录不是目录")
    script_text = args[-1]
    script_candidate = Path(script_text).expanduser()
    if not script_candidate.is_absolute():
        script_candidate = workdir / script_candidate
    script = resolve_allowed(str(script_candidate), allowed_restart_roots(), must_exist=True)
    if not script.is_file():
        raise ValueError("Slurm 脚本不存在")
    syntax = RUNNER.run(["bash", "-n", str(script)])
    if not syntax.ok:
        raise ValueError("Slurm 脚本语法检查失败：" + (syntax.stderr or "bash -n failed"))
    args[-1] = str(script)
    return args, workdir, script


def reconstruct_submit_command(detail: Mapping[str, str], record: Mapping[str, Any]) -> tuple[str, str]:
    args = ["sbatch"]

    def add(option: str, value: str) -> None:
        if value and value not in {"(null)", "None", "N/A", "Unknown", "UNLIMITED"}:
            args.append(f"--{option}={value}")

    add("account", str(detail.get("Account", "")))
    add("qos", str(detail.get("QOS", "")))
    add("job-name", str(detail.get("JobName", record.get("name", ""))))
    add("partition", str(detail.get("Partition", record.get("partition", ""))))
    add("nodelist", str(detail.get("ReqNodeList", "")))
    add("nodes", str(detail.get("NumNodes", record.get("node_count", ""))))
    add("ntasks", str(detail.get("NumTasks", "")))
    add("cpus-per-task", str(detail.get("CPUs/Task", "")))
    add("time", str(detail.get("TimeLimit", "")))
    add("dependency", str(detail.get("Dependency", "")))
    req_tres = str(detail.get("ReqTRES", ""))
    memory = re.search(r"(?:^|,)mem=([^,]+)", req_tres)
    gpu = re.search(r"(?:^|,)gres/gpu=([^,]+)", req_tres)
    if memory:
        add("mem", memory.group(1))
    if gpu:
        add("gres", "gpu:" + gpu.group(1))
    if str(detail.get("Requeue", "")) == "1":
        args.append("--requeue")
    script = str(detail.get("Command", record.get("script_path", "")))
    if script:
        args.append(script)
    return shlex.join(args) if len(args) > 1 else "", str(detail.get("WorkDir", ""))


def capture_submission(record: Mapping[str, Any]) -> Dict[str, str]:
    job_id = str(record["slurm_job_id"])
    detail, detail_result = SLURM.job_detail(job_id)
    detail = detail if detail_result.ok else {}
    submit_line, submit_result = SLURM.submit_line(job_id)
    reconstructed, reconstructed_cwd = reconstruct_submit_command(detail, record)
    command = submit_line or reconstructed or str(record.get("submit_command", ""))
    cwd = str(detail.get("WorkDir", "")) or reconstructed_cwd or str(record.get("submit_cwd", ""))

    snapshot = ""
    command_script = str(detail.get("Command", "")) or str(record.get("script_path", ""))
    snapshot_name = Path(command_script).name if command_script else f"job-{job_id}.slurm"
    target = job_snapshot_path(job_id, Path(snapshot_name))
    snapshot_result = None
    if target.exists():
        snapshot = str(target)
    else:
        snapshot_result = SLURM.write_batch_script(job_id, target)
    if not snapshot and snapshot_result and snapshot_result.ok and target.exists():
        snapshot = str(target)
    elif not snapshot and command_script:
        try:
            source = resolve_allowed(command_script, allowed_restart_roots(), must_exist=True)
            shutil.copy2(source, target)
            snapshot = str(target)
        except (OSError, PermissionError, FileNotFoundError):
            pass

    if snapshot and command:
        try:
            captured_args = shlex.split(command, posix=True)
            if captured_args and Path(captured_args[0]).name == "sbatch" and len(captured_args) > 1:
                captured_args[-1] = snapshot
                command = shlex.join(captured_args)
        except ValueError:
            pass

    if submit_line and snapshot:
        status = "complete"
    elif command:
        status = "partial"
    else:
        status = "missing"
    return {
        "command": command,
        "cwd": cwd,
        "origin": "captured",
        "status": status,
        "snapshot": snapshot,
        "submit_line_error": "" if submit_result.ok else submit_result.stderr,
        "detail_error": "" if detail_result.ok else detail_result.stderr,
    }


def register_webui_submission(
    job_id: str,
    script: Path,
    submitted_script: Path,
    request: SubmitRequest,
    payload: Mapping[str, Any],
) -> None:
    args, workdir = SLURM.submit_args(request)
    snapshot = job_snapshot_path(job_id, submitted_script)
    shutil.copy2(submitted_script, snapshot)
    # Always restart from the immutable copy that was actually submitted. This
    # also protects ordinary (non-generated) scripts from later in-place edits.
    args[-1] = str(snapshot)
    info = next((item for item in script_catalog() if Path(item.path) == script), None)
    display_name = (info.job_name or script.stem) if info else script.stem
    record_id = STORE.save_quick_job(
        job_id,
        quick_job_type(info, display_name),
        display_name,
        "SUBMITTED",
        str(payload.get("partition", "")),
        int(payload.get("nodes", 1)),
        str(payload.get("nodelist", "")),
        str(script),
        dict(payload),
        "webui",
    )
    STORE.save_quick_job_command(
        record_id,
        shlex.join(args),
        str(workdir),
        "webui",
        False,
        "complete",
        str(snapshot),
        "WebUI",
    )


def submit_payload(payload: Mapping[str, Any], action: str = "SBATCH") -> Optional[str]:
    generated: Optional[Path] = None
    try:
        script = resolve_allowed(str(payload["script_path"]), CONFIG.paths("script_roots"), must_exist=True)
        if not validate_submit_selection(script, payload):
            return None
        submit_script, generated = vllm_override_script(script, payload)
        syntax = RUNNER.run(["bash", "-n", str(submit_script)])
        if not syntax.ok:
            error_from(syntax, "脚本语法检查失败")
            return None
        prepare_slurm_output_directories(script)
        request = SubmitRequest(
            submit_script,
            chdir=script.parent,
            nodelist=str(payload.get("nodelist", "")),
            partition=str(payload.get("partition", "")),
            nodes=int(payload.get("nodes", 1)),
            exports=parse_exports(str(payload.get("exports", ""))),
        )
        result = SLURM.submit(request)
        job_id = SLURM.extract_job_id(result)
        STORE.record_action(action, job_id or script.name, "OK" if result.ok else "FAILED", result.stderr)
        if result.ok:
            if job_id:
                try:
                    register_webui_submission(job_id, script, submit_script, request, payload)
                except Exception as exc:
                    STORE.record_action("WEBUI_SUBMIT_RECORD", job_id, "FAILED", str(exc))
                    st.warning("作业已经提交成功，但保存重启命令失败：" + str(exc))
                waiting = dict(st.session_state.get("quick_job_waiting", {}))
                waiting[job_id] = time.time() + 30
                st.session_state["quick_job_waiting"] = waiting
            st.success(f"提交成功，Job ID：{job_id}")
            return job_id
        error_from(result)
    except Exception as exc:
        st.error(str(exc))
    finally:
        if generated:
            try:
                generated.unlink()
            except OSError:
                pass
    return None


def confirm_action(token: str, label: str, confirm_label: str, action) -> None:
    pending_key = "pending_action"
    if st.button(label, key="ask_" + token):
        st.session_state[pending_key] = token
        st.rerun()
    if st.session_state.get(pending_key) == token:
        st.warning("请确认该操作。")
        yes, no = st.columns(2)
        if yes.button(confirm_label, type="primary", key="yes_" + token):
            action()
            st.session_state.pop(pending_key, None)
            st.rerun()
        if no.button("返回", key="no_" + token):
            st.session_state.pop(pending_key, None)
            st.rerun()


def protected_quick_job_ids() -> set[str]:
    return {
        str(job_id)
        for job_id, deadline in dict(st.session_state.get("quick_job_waiting", {})).items()
        if time.time() < float(deadline)
    }


def stopped_quick_job_records(
    records: Sequence[Mapping[str, Any]],
    live_ids: set[str],
    predicate: Optional[Callable[[Mapping[str, Any]], bool]] = None,
) -> List[Mapping[str, Any]]:
    protected = protected_quick_job_ids()
    return [
        record for record in records
        if str(record.get("slurm_job_id", "")) not in live_ids | protected
        and (predicate is None or predicate(record))
    ]


def is_vllm_quick_job(record: Mapping[str, Any]) -> bool:
    payload = record.get("payload", {})
    preset_kind = str(payload.get("_preset_kind", "")) if isinstance(payload, Mapping) else ""
    values = (
        str(record.get("job_type", "")), str(record.get("name", "")),
        str(record.get("script_path", "")), preset_kind,
    )
    return any("vllm" in value.lower() for value in values)


def delete_stopped_quick_job_records(
    users: str,
    predicate: Optional[Callable[[Mapping[str, Any]], bool]] = None,
) -> int:
    jobs, result = SLURM.jobs(users)
    if not result.ok:
        raise RuntimeError("无法确认实时 Slurm 作业状态，已取消清理：" + (result.stderr or "squeue failed"))
    live_ids = {str(job["job_id"]) for job in jobs}
    stopped = stopped_quick_job_records(STORE.quick_jobs(), live_ids, predicate)
    return STORE.delete_quick_jobs([int(record["id"]) for record in stopped])


def bulk_delete_stopped_control(
    token: str,
    count: int,
    delete_action: Callable[[], int],
    button_container: Any = None,
) -> None:
    notice_key = token + "_notice"
    if st.session_state.get(notice_key):
        st.success(str(st.session_state.pop(notice_key)))
    target = button_container or st
    if target.button(
        "一键删除已停止项",
        key="ask_" + token,
        disabled=count <= 0,
        help=f"当前可清理 {count} 项；只删除平台记录，不会停止任务。",
    ):
        st.session_state["pending_action"] = token
        st.rerun()
    if st.session_state.get("pending_action") == token:
        st.warning(f"确认删除 {count} 条已停止的平台记录？该操作不会停止任何实际任务。")
        yes, no = st.columns(2)
        if yes.button("确认批量删除", type="primary", key="yes_" + token):
            try:
                deleted = delete_action()
            except Exception as exc:
                st.error(str(exc))
            else:
                st.session_state[notice_key] = f"已删除 {deleted} 条停止记录。"
                st.session_state.pop("pending_action", None)
                st.rerun()
        if no.button("返回", key="no_" + token):
            st.session_state.pop("pending_action", None)
            st.rerun()


def preset_node_values(kind: str, payload: Mapping[str, Any]) -> List[str]:
    field = "nodes" if kind == "metrics" else "nodelist"
    return split_values(str(payload.get(field, "")))


def preset_node_count_text(kind: str, payload: Mapping[str, Any]) -> str:
    if kind == "compute":
        return "节点随所选作业"
    nodes = preset_node_values(kind, payload)
    configured_count = int(payload.get("nodes", 0) or 0) if kind != "metrics" else 0
    return f"{configured_count or len(nodes)} 个节点"


def preset_script_name(kind: str, payload: Mapping[str, Any]) -> str:
    script_path = str(payload.get("script_path", "")).strip()
    if script_path:
        return Path(script_path).name
    if kind == "compute":
        try:
            tokens = shlex.split(str(payload.get("command", "")))
        except ValueError:
            tokens = str(payload.get("command", "")).split()
        candidate = next(
            (token for token in tokens if Path(token).suffix.lower() in {".py", ".sh", ".bash"}),
            "",
        )
        return Path(candidate).name if candidate else "命令任务"
    return "采样组" if kind == "metrics" else "未指定脚本"


def preset_summary(preset: Mapping[str, Any], kind: str) -> str:
    payload = preset.get("payload", {})
    updated_at = str(preset.get("updated_at", "")).replace("T", " ") or "—"
    return (
        f"**{preset.get('name', '')}**　·　脚本：`{preset_script_name(kind, payload)}`"
        f"　·　{preset_node_count_text(kind, payload)}　·　更新：{updated_at}"
    )


def preset_details(kind: str, payload: Mapping[str, Any]) -> None:
    if kind == "compute":
        st.caption(
            "任务名称：" + str(payload.get("task_name", "—"))
            + "　·　类型：" + ("Ray 任务" if payload.get("task_type") == "ray" else "一般任务")
        )
        st.code(str(payload.get("command", "")), language="bash")
        return

    script_path = str(payload.get("script_path", "")).strip()
    if script_path:
        st.caption("脚本完整路径：" + script_path)
    partition = str(payload.get("partition", "")).strip()
    if partition:
        st.caption("Partition：" + partition)
    nodes = preset_node_values(kind, payload)
    if nodes:
        st.caption("节点列表（共 " + str(len(nodes)) + " 个）")
        st.code(",".join(nodes), language="text")
    if kind == "metrics":
        method_labels = {"auto": "自动（exporter → srun）", "direct": "仅 exporter", "srun": "仅 srun"}
        st.caption(
            "采样方式：" + method_labels.get(str(payload.get("method", "auto")), str(payload.get("method", "auto")))
            + "　·　单节点超时：" + str(payload.get("timeout", 20)) + " 秒"
        )
    if kind == "vllm":
        st.caption(
            "模型路径：" + (str(payload.get("model_path", "")).strip() or "使用脚本默认值")
            + "　·　模型名称：" + (str(payload.get("model_name", "")).strip() or "使用脚本默认值")
        )
    exports = str(payload.get("exports", "")).strip()
    if exports:
        st.caption("环境变量")
        st.code(exports, language="bash")


def preset_cards(kind: str, action_label: str, on_run) -> None:
    presets = STORE.presets(kind)
    if not presets:
        st.caption("暂无预设，可在下方创建。")
        return
    for preset in presets:
        payload = preset["payload"]
        if kind == "vllm":
            detail_col, run_col, delete_col = st.columns([8, 1.15, 1])
            edit_col = None
        else:
            detail_col, run_col, edit_col, delete_col = st.columns([7, 1.15, 1, 1])
        with detail_col.expander(preset_summary(preset, kind), expanded=False):
            preset_details(kind, payload)
        if run_col.button(action_label, key=f"run_preset_{kind}_{preset['id']}", type="primary", use_container_width=True):
            run_payload = dict(payload)
            run_payload["_preset_id"] = int(preset["id"])
            run_payload["_preset_name"] = str(preset["name"])
            run_payload["_preset_kind"] = kind
            on_run(run_payload)
        if edit_col is not None and edit_col.button("修改", key=f"edit_preset_{kind}_{preset['id']}", use_container_width=True):
            editor_key = "metrics_choice" if kind == "metrics" else f"preset_choice_{kind}"
            st.session_state[editor_key] = str(preset["name"])
            st.rerun()
        token = f"delete_preset_{kind}_{preset['id']}"
        if delete_col.button("删除", key="ask_" + token, use_container_width=True):
            st.session_state["pending_action"] = token
            st.rerun()
        if st.session_state.get("pending_action") == token:
            st.warning(f"确认删除预设“{preset['name']}”？")
            y, n, _ = st.columns([1, 1, 7])
            if y.button("确认删除", key="yes_" + token):
                STORE.delete_preset(int(preset["id"]))
                st.session_state.pop("pending_action", None)
                st.rerun()
            if n.button("返回", key="no_" + token):
                st.session_state.pop("pending_action", None)
                st.rerun()


def partition_options(preferred: str = "") -> List[str]:
    options = list(CONFIG.get("default_partitions", []))
    rows, result = SLURM.partitions()
    if result.ok:
        options = [str(row["partition"]) for row in rows] + options
    if preferred:
        options.insert(0, preferred)
    return list(dict.fromkeys(item for item in options if item))


def partition_node_picker(
    partition: str,
    key: str,
    defaults: Sequence[str] = (),
    count_default: Optional[int] = None,
    allow_count_override: bool = False,
    count_auto_default: bool = True,
    show_select_all: bool = False,
    include_unavailable: bool = False,
) -> tuple[List[str], List[Dict[str, Any]], int, bool]:
    rows, result = SLURM.nodes(partition)
    all_nodes = [str(row["node"]) for row in rows]
    available = [str(row["node"]) for row in rows if not str(row["state"]).startswith(("down", "drain"))]
    idle = [str(row["node"]) for row in rows if str(row["state"]).startswith("idle")]
    selectable = all_nodes if include_unavailable else available
    options = list(dict.fromkeys(selectable + list(defaults)))
    widget_key = f"{key}_nodes_{partition}"
    count_key = f"{key}_count_{partition}"
    auto_key = f"{key}_count_auto_{partition}"
    if widget_key not in st.session_state:
        st.session_state[widget_key] = [item for item in defaults if item in options]
    if count_key not in st.session_state:
        st.session_state[count_key] = max(1, int(count_default if count_default is not None else len(st.session_state[widget_key]) or 1))
    if auto_key not in st.session_state:
        st.session_state[auto_key] = bool(count_auto_default)

    def sync_node_count() -> None:
        if st.session_state.get(auto_key, True):
            st.session_state[count_key] = max(1, len(st.session_state.get(widget_key, [])))

    button_columns = st.columns([1, 1, 5]) if show_select_all else st.columns([1, 5])
    c1 = button_columns[0]
    if c1.button("全选 Idle", key=f"{key}_all_idle_{partition}", disabled=not idle):
        st.session_state[widget_key] = idle
        if st.session_state.get(auto_key, True):
            st.session_state[count_key] = max(1, len(idle))
    caption_column = button_columns[-1]
    if show_select_all:
        if button_columns[1].button("全选", key=f"{key}_all_{partition}", disabled=not options):
            st.session_state[widget_key] = options
            if st.session_state.get(auto_key, True):
                st.session_state[count_key] = max(1, len(options))
    caption_column.caption(f"{partition}：{len(rows)} 个节点，{len(idle)} 个 idle；可全选后取消个别节点。")
    selected = st.multiselect("节点", options, key=widget_key, on_change=sync_node_count)
    if not result.ok:
        st.warning("未能读取该 partition 的节点；仍可使用脚本或预设中已经保存的节点。")
    if allow_count_override:
        auto_count = st.toggle("节点数跟随节点选择自动更新", key=auto_key)
        if auto_count:
            st.session_state[count_key] = max(1, len(selected))
        node_count = int(st.number_input(
            "节点数",
            min_value=1,
            step=1,
            key=count_key,
            disabled=auto_count,
        ))
    else:
        auto_count = True
        node_count = len(selected)
        st.caption(f"节点数：{node_count}")
    return selected, rows, node_count, auto_count


def slurm_preset_editor(kind: str, scripts: Sequence[ScriptInfo], title: str = "新增或修改预设") -> None:
    presets = STORE.presets(kind)
    with st.expander(title):
        choices = ["＋ 新增预设"] + [str(item["name"]) for item in presets]
        choice = st.selectbox("预设名称", choices, key=f"preset_choice_{kind}")
        editing = next((item for item in presets if item["name"] == choice), None)
        payload = editing["payload"] if editing else {}
        suffix = str(editing["id"]) if editing else "new"
        name = st.text_input("名称", value=editing["name"] if editing else "", key=f"preset_name_{kind}_{suffix}")
        paths = [item.path for item in scripts]
        default_path = str(payload.get("script_path", paths[0] if paths else ""))
        index = paths.index(default_path) if default_path in paths else 0
        selected_path = st.selectbox(
            "Slurm 脚本", paths, index=index, format_func=lambda value: script_label(next(x for x in scripts if x.path == value)),
            key=f"preset_script_{kind}_{suffix}",
        ) if paths else ""
        selected = next((item for item in scripts if item.path == selected_path), None)
        preferred_partition = str(payload.get("partition", selected.partition if selected else ""))
        partitions = partition_options(preferred_partition)
        partition = st.selectbox("Partition", partitions, key=f"preset_part_{kind}_{suffix}") if partitions else ""
        defaults = split_values(str(payload.get("nodelist", selected.nodelist if selected else "")))
        default_count = int(payload.get("nodes", int(selected.nodes or len(defaults) or 1) if selected else len(defaults) or 1))
        nodes, _, node_count, nodes_auto = partition_node_picker(
            partition,
            f"preset_{kind}_{suffix}",
            defaults,
            default_count,
            True,
            bool(payload.get("nodes_auto", True)),
        ) if partition else ([], [], 1, True)
        model_path = ""
        model_name = ""
        if kind == "vllm":
            saved_model_path = str(payload.get("model_path", ""))
            saved_model_name = str(payload.get("model_name", ""))
            if "$" in saved_model_path:
                saved_model_path = ""
            if "$" in saved_model_name:
                saved_model_name = ""
            m1, m2 = st.columns(2)
            model_path = m1.text_input(
                "模型路径（可选，覆盖脚本 MODEL_PATH）",
                value=saved_model_path,
                placeholder="非必填；留空使用 Slurm 脚本中的 MODEL_PATH",
                key=f"preset_model_path_{kind}_{suffix}",
            )
            model_name = m2.text_input(
                "模型名称（可选，覆盖脚本 MODEL_NAME）",
                value=saved_model_name,
                placeholder="非必填；留空使用 Slurm 脚本中的 MODEL_NAME",
                key=f"preset_model_name_{kind}_{suffix}",
            )
        exports = st.text_area("环境变量", value=str(payload.get("exports", "")), key=f"preset_env_{kind}_{suffix}")
        if st.button("更新预设" if editing else "新增预设", type="primary", disabled=not (name and selected_path and nodes), key=f"save_preset_{kind}_{suffix}"):
            STORE.save_preset(
                kind,
                name,
                {"script_path": selected_path, "nodelist": ",".join(nodes), "partition": partition,
                 "nodes": node_count, "nodes_auto": nodes_auto, "exports": exports,
                 "model_path": model_path, "model_name": model_name,
                 "summary": f"{partition} · {node_count} nodes"},
                int(editing["id"]) if editing else None,
            )
            st.success("预设已保存。")
            st.rerun()


def render_metric_results(data: Sequence[Mapping[str, Any]], node_states: Mapping[str, str]) -> List[str]:
    display = []
    for item in data:
        row = dict(item)
        details = row.pop("gpu_detail", [])
        row["gpu_detail"] = ", ".join(
            f"GPU{x.get('index')}:{x.get('util_pct', '-')}%/{x.get('temperature_c', '-')}℃" for x in details
        )
        node = str(row.pop("node", ""))
        previous_state = str(row.pop("state", ""))
        display.append({
            "选择": False,
            "node": node,
            "state": node_states.get(node, previous_state or "unknown"),
            **row,
        })
    disabled_columns = [key for key in display[0] if key != "选择"] if display else []
    edited = st.data_editor(
        display,
        use_container_width=True,
        hide_index=True,
        disabled=disabled_columns,
        column_config={"选择": st.column_config.CheckboxColumn("选择", default=False)},
        key="metric_result_node_selection_" + str(st.session_state.get("node_metrics_version", 0)),
    )
    edited_rows = edited.to_dict("records") if hasattr(edited, "to_dict") else edited
    selected = [str(item.get("node", "")) for item in edited_rows if item.get("选择") and item.get("node")]
    st.caption(f"已选择 {len(selected)} 台服务器。下面的快捷操作共用这组选择。")
    failed = [item for item in data if not item.get("ok")]
    if failed:
        st.warning(f"{len(failed)} 个节点未获取到实时指标。可修改采样组为“仅 exporter”查看直连错误，或降低节点数量。")
    return selected


def persistent_script_select(
    label: str, scripts: Sequence[ScriptInfo], setting_key: str, widget_key: str,
) -> Optional[ScriptInfo]:
    if not scripts:
        st.selectbox(label, ["没有可用脚本"], disabled=True, key=widget_key + "_empty")
        return None
    paths = [item.path for item in scripts]
    saved = STORE.setting(setting_key)
    index = paths.index(saved) if saved in paths else 0
    selected_path = st.selectbox(
        label,
        paths,
        index=index,
        format_func=lambda value: script_label(next(item for item in scripts if item.path == value)),
        key=widget_key,
    )
    if selected_path != saved:
        STORE.set_setting(setting_key, selected_path)
    return next(item for item in scripts if item.path == selected_path)


def quick_submit_from_metrics(
    script: Optional[ScriptInfo], selected_nodes: Sequence[str], partition: str, action: str,
    job_type: str = "",
) -> Optional[str]:
    if not script or not selected_nodes:
        return None
    payload = {
        "script_path": script.path,
        "nodelist": ",".join(selected_nodes),
        "partition": partition or script.partition,
        "nodes": len(selected_nodes),
        "exports": "",
        "model_path": "",
        "model_name": "",
    }
    master_address, _ = SLURM.node_address(str(selected_nodes[0]))
    payload["_master_address"] = master_address or str(selected_nodes[0])
    job_id = submit_payload(payload, action)
    if job_id:
        STORE.save_quick_job(
            job_id,
            job_type or quick_job_type(script),
            script.job_name or Path(script.path).stem,
            "SUBMITTED",
            str(payload["partition"]),
            len(selected_nodes),
            ",".join(selected_nodes),
            script.path,
            payload,
            "quick",
        )
        waiting = dict(st.session_state.get("quick_job_waiting", {}))
        waiting[job_id] = time.time() + 30
        st.session_state["quick_job_waiting"] = waiting
    return job_id


def quick_job_type(script: Optional[ScriptInfo], job_name: str = "") -> str:
    if script:
        if script.category == "vllm":
            return "vLLM"
        if script.category == "keepalive":
            return "GPU 保活" if int(script.gpu or 0) > 0 else "CPU 保活"
        if script.category == "ray-gpu":
            return "Slurm · Ray GPU"
        if script.category == "ray-cpu":
            return "Slurm · Ray CPU"
    lowered = job_name.lower()
    if "vllm" in lowered:
        return "vLLM"
    if "keep" in lowered or "stress" in lowered:
        return "保活"
    return "Slurm"


def sync_quick_jobs() -> tuple[List[Dict[str, Any]], set[str]]:
    user = str(CONFIG.get("default_user", os.environ.get("USER", "")))
    cluster_jobs, result = SLURM.jobs(user)
    if not result.ok:
        error_from(result, "无法同步快捷作业状态")
        stored = STORE.quick_jobs()
        return stored, set()

    scripts = script_catalog()
    stored = STORE.quick_jobs()
    stored_by_job_id = {str(item["slurm_job_id"]): item for item in stored}
    live_ids: set[str] = set()
    for job in cluster_jobs:
        job_id = str(job["job_id"])
        live_ids.add(job_id)
        existing = stored_by_job_id.get(job_id)
        nodes, _ = SLURM.job_nodes(job_id)
        node_names = [str(item["node"]) for item in nodes]
        master_address = str(nodes[0].get("address", "")) if nodes else ""
        detail: Dict[str, str] = {}
        matched: Optional[ScriptInfo] = None
        payload = dict(existing.get("payload", {})) if existing else {}
        script_path = str(existing.get("script_path", "")) if existing else ""
        submitted_at = str(existing.get("submitted_at") or "") if existing else ""
        if not payload or not script_path or not submitted_at:
            detail, detail_result = SLURM.job_detail(job_id)
            if detail_result.ok and detail.get("SubmitTime") not in {"", "Unknown", "N/A"}:
                submitted_at = detail["SubmitTime"].replace("T", " ")
            command = detail.get("Command", "") if detail_result.ok else ""
            command_name = Path(command).name if command else ""
            matched = next(
                (item for item in scripts if item.path == command or Path(item.path).name == command_name),
                None,
            )
            if command:
                script_path = matched.path if matched else command
                payload = {
                    "script_path": script_path,
                    "nodelist": ",".join(node_names),
                    "partition": str(job["partition"]),
                    "nodes": int(job["nodes"]) if str(job["nodes"]).isdigit() else len(node_names),
                    "exports": "",
                    "model_path": "",
                    "model_name": "",
                }
        if master_address:
            payload["_master_address"] = master_address
        if not matched and script_path:
            matched = next((item for item in scripts if item.path == script_path), None)
        STORE.save_quick_job(
            job_id,
            str(existing["job_type"]) if existing else quick_job_type(matched, str(job["name"])),
            str(job["name"]),
            str(job["state"]),
            str(job["partition"]),
            int(job["nodes"]) if str(job["nodes"]).isdigit() else len(node_names),
            ",".join(node_names),
            script_path,
            payload,
            str(existing.get("source", "cluster")) if existing else "cluster",
            submitted_at=submitted_at,
        )

    stored = STORE.quick_jobs()
    waiting = {
        str(job_id): float(deadline)
        for job_id, deadline in dict(st.session_state.get("quick_job_waiting", {})).items()
        if time.time() < float(deadline)
    }
    for job_id in live_ids:
        waiting.pop(job_id, None)
    st.session_state["quick_job_waiting"] = waiting
    needs_history = [
        item for item in stored
        if str(item["slurm_job_id"]) not in live_ids
        and str(item["slurm_job_id"]) not in waiting
        and str(item["state"]).upper() not in {"COMPLETED", "CANCELLED", "FAILED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "NOT_IN_QUEUE"}
    ]
    if needs_history:
        history, history_result = SLURM.job_history(user, "today")
        history_states = {str(item["job_id"]): str(item["state"]) for item in history} if history_result.ok else {}
        for item in needs_history:
            state = history_states.get(str(item["slurm_job_id"]), "NOT_IN_QUEUE")
            STORE.update_quick_job_state(int(item["id"]), state)
    return STORE.quick_jobs(), live_ids


def restart_quick_job(record: Mapping[str, Any]) -> None:
    try:
        args, workdir, script = validate_submit_command(
            str(record.get("submit_command", "")), str(record.get("submit_cwd", ""))
        )
    except Exception as exc:
        st.error("提交命令无法执行：" + str(exc))
        return
    run_args = list(args)
    if not any(item == "--parsable" or item.startswith("--parsable=") for item in run_args[1:]):
        run_args.insert(1, "--parsable")
    result = RUNNER.run(run_args, cwd=str(workdir))
    job_id = SLURM.extract_job_id(result)
    STORE.record_action("QUICK_JOB_RESTART", job_id or str(record["id"]), "OK" if result.ok else "FAILED", result.stderr)
    if not result.ok or not job_id:
        error_from(result, "再次启动失败")
        return
    payload = dict(record.get("payload", {}))
    payload.update({
        "script_path": str(script),
        "partition": str(record.get("partition", payload.get("partition", ""))),
        "nodelist": str(record.get("nodelist", payload.get("nodelist", ""))),
        "nodes": int(record.get("node_count", payload.get("nodes", 1))),
    })
    STORE.save_quick_job(
        job_id,
        str(record["job_type"]),
        str(record["name"]),
        "SUBMITTED",
        str(payload.get("partition", record.get("partition", ""))),
        int(payload.get("nodes", record.get("node_count", 0))),
        str(payload.get("nodelist", record.get("nodelist", ""))),
        str(payload.get("script_path", record.get("script_path", ""))),
        payload,
        str(record.get("source", "quick")),
        int(record["id"]),
    )
    st.success(f"再次启动成功，新 Job ID：{job_id}")
    waiting = dict(st.session_state.get("quick_job_waiting", {}))
    waiting[job_id] = time.time() + 30
    st.session_state["quick_job_waiting"] = waiting


def render_node_connect_control(record: Mapping[str, Any], enabled: bool) -> None:
    prefix = str(CONFIG.get("node_connect_command", "")).strip()
    payload = dict(record.get("payload", {}))
    address = str(payload.get("_master_address", "")).strip()
    job_id = str(record.get("slurm_job_id", ""))
    if not (prefix and address and job_id):
        st.caption("尚无连接信息")
        return
    port = shell_port(
        job_id,
        int(CONFIG.get("shell_port_base", 10000)),
        int(CONFIG.get("shell_port_modulo", 40000)),
    )
    command = f'{prefix} {address} {port} "cd /workspace"'
    js_command = json.dumps(command, ensure_ascii=False).replace("</", "<\\/")
    disabled = " disabled" if not enabled else ""
    components.html(
        "<!doctype html><html><head><style>"
        "html,body{margin:0;padding:0;background:transparent;font:13px sans-serif;}"
        ".wrap{display:flex;align-items:center;gap:6px;height:36px;}"
        "code{padding:4px 6px;background:#f2f4f7;border-radius:4px;}"
        "button{border:1px solid #c8ccd2;background:white;border-radius:6px;padding:5px 8px;cursor:pointer;}"
        "button:disabled{opacity:.45;cursor:not-allowed}.ok{color:#15803d;font-size:12px}"
        "</style></head><body><div class='wrap'>"
        f'<code title="{html.escape(command, quote=True)}">{port}</code>'
        f"<button id='copy'{disabled}>复制</button><span id='status'></span></div>"
        "<script>const value=" + js_command + ";"
        "document.getElementById('copy').onclick=async()=>{"
        "let ok=false;try{if(navigator.clipboard&&window.isSecureContext){await navigator.clipboard.writeText(value);ok=true;}}catch(e){}"
        "if(!ok){const t=document.createElement('textarea');t.value=value;t.style.position='fixed';t.style.opacity='0';"
        "document.body.appendChild(t);t.focus();t.select();ok=document.execCommand('copy');t.remove();}"
        "const s=document.getElementById('status');s.textContent=ok?'已复制':'复制失败';s.className='ok';};"
        "</script></body></html>",
        height=40,
        scrolling=False,
    )


def save_captured_submission(record: Mapping[str, Any], captured: Mapping[str, str]) -> None:
    STORE.save_quick_job_command(
        int(record["id"]),
        str(captured.get("command", "")),
        str(captured.get("cwd", "")),
        "captured",
        False,
        str(captured.get("status", "missing")),
        str(captured.get("snapshot", "")),
        str(record.get("submit_method", "Terminal")),
    )


def submission_command_editor(record: Mapping[str, Any]) -> None:
    record_id = int(record["id"])
    auto_token = f"{record_id}:{record.get('slurm_job_id', '')}"
    should_auto_capture = (
        not str(record.get("submit_command", "")).strip()
        or (
            str(record.get("submit_method", "Terminal")) == "Terminal"
            and not bool(record.get("command_user_edited"))
        )
    )
    if should_auto_capture and st.session_state.get("quick_command_auto_captured") != auto_token:
        with st.spinner("正在自动捕获提交参数和批处理脚本……"):
            save_captured_submission(record, capture_submission(record))
        st.session_state["quick_command_auto_captured"] = auto_token
        st.rerun()

    status_labels = {"complete": "高完整度", "partial": "部分完整", "missing": "缺失", "validated": "人工校验"}
    st.caption(
        f"提交方式：{record.get('submit_method', 'Terminal')} · "
        f"命令来源：{record.get('command_origin') or '-'} · "
        f"捕获状态：{status_labels.get(str(record.get('capture_status', 'missing')), record.get('capture_status', 'missing'))}"
    )
    st.caption("完整度针对 Slurm 提交命令和脚本；提交 shell 中未显式写入命令的临时环境变量仍可能无法恢复。")
    command = st.text_area(
        "sbatch 命令",
        value=str(record.get("submit_command", "")),
        height=180,
        key=f"quick_command_value_{record_id}_{record.get('updated_at', '')}",
    )
    cwd = st.text_input(
        "提交工作目录",
        value=str(record.get("submit_cwd", "")),
        key=f"quick_command_cwd_{record_id}_{record.get('updated_at', '')}",
    )
    save_col, capture_col, close_col = st.columns(3)
    if save_col.button("保存修改", type="primary", key=f"save_quick_command_{record_id}"):
        try:
            validate_submit_command(command, cwd)
            STORE.save_quick_job_command(
                record_id, command.strip(), cwd.strip(), "user", True, "validated",
                str(record.get("script_snapshot", "")), str(record.get("submit_method", "Terminal")),
            )
            st.success("提交命令已验证并保存。")
            st.rerun()
        except Exception as exc:
            st.error(str(exc))
    if capture_col.button("现场捕获参数并补填", key=f"capture_quick_command_{record_id}"):
        with st.spinner("正在读取 SubmitLine、作业参数和批处理脚本……"):
            st.session_state["quick_command_capture_preview"] = {
                "record_id": record_id,
                **capture_submission(record),
            }
        st.rerun()
    if close_col.button("关闭", key=f"close_quick_command_{record_id}"):
        st.session_state.pop("quick_command_record_id", None)
        st.session_state.pop("quick_command_capture_preview", None)
        st.session_state.pop("quick_command_auto_captured", None)
        st.rerun()

    preview = st.session_state.get("quick_command_capture_preview")
    if preview and int(preview.get("record_id", -1)) == record_id:
        st.warning("现场捕获结果将覆盖当前命令。确认前请检查差异；当前版本会自动保存在历史记录中。")
        old_col, new_col = st.columns(2)
        old_col.caption("当前保存的命令")
        old_col.code(str(record.get("submit_command", "")) or "（空）", language="bash")
        new_col.caption("现场捕获的命令")
        new_col.code(str(preview.get("command", "")) or "（未捕获到）", language="bash")
        confirm = st.checkbox("确认使用现场结果覆盖当前命令", key=f"confirm_capture_{record_id}")
        if st.button(
            "确认覆盖",
            type="primary",
            disabled=not (confirm and preview.get("command")),
            key=f"apply_capture_{record_id}",
        ):
            save_captured_submission(record, preview)
            st.session_state.pop("quick_command_capture_preview", None)
            st.session_state["quick_command_auto_captured"] = auto_token
            st.success("已使用现场捕获结果覆盖，旧命令已保存到历史记录。")
            st.rerun()

    history = STORE.quick_job_command_history(record_id)
    if history:
        with st.expander("命令修改历史", expanded=False):
            for item in history:
                st.caption(f"{item['created_at']} · {item['command_origin']} · cwd={item['submit_cwd'] or '-'}")
                st.code(str(item["submit_command"]), language="bash")


def show_submission_command_editor(record: Mapping[str, Any]) -> None:
    if hasattr(st, "dialog"):
        dialog = st.dialog("提交命令")(submission_command_editor)
        dialog(record)
    else:
        st.subheader("提交命令")
        with st.container(border=True):
            submission_command_editor(record)


def render_quick_job_manager() -> None:
    title_col, refresh_col, delete_col = st.columns([5, 1, 1.7])
    title_col.subheader("快捷作业列表")
    if refresh_col.button("刷新列表", key="quick_job_refresh"):
        st.rerun()
    records, live_ids = sync_quick_jobs()
    stopped_records = stopped_quick_job_records(records, live_ids)
    bulk_delete_stopped_control(
        "bulk_delete_quick_jobs",
        len(stopped_records),
        lambda: delete_stopped_quick_job_records(str(CONFIG.get("default_user", ""))),
        delete_col,
    )
    if not records:
        st.caption("暂无 Slurm 作业记录。")
        return
    pending_ids = set(dict(st.session_state.get("quick_job_waiting", {})))
    records = sorted(
        records,
        key=lambda item: (str(item["slurm_job_id"]) not in (live_ids | pending_ids), -int(item["id"])),
    )
    submission_presets = [
        preset
        for kind in ("slurm", "vllm", "keepalive")
        for preset in STORE.presets(kind)
    ]
    widths = [1.0, 0.7, 0.75, 0.5, 1.3, 1.05, 0.8, 1.15, 0.7, 1.25, 0.8, 1.75]
    header = st.columns(widths)
    for column, label in zip(
        header,
        ("作业类型", "提交方式", "Job ID", "节点数", "任务名称", "提交预设", "Partition", "最近提交时间", "状态", "节点连接", "提交命令", "操作"),
    ):
        column.caption(label)
    for record in records:
        job_id = str(record["slurm_job_id"])
        is_live = job_id in live_ids
        is_pending = job_id in pending_ids
        columns = st.columns(widths)
        columns[0].write(str(record["job_type"]))
        columns[1].write(str(record.get("submit_method", "Terminal")))
        columns[2].code(job_id, language="text")
        tooltip = html.escape(str(record.get("nodelist", "")) or "尚未分配节点", quote=True)
        columns[3].markdown(
            f'<span title="{tooltip}" style="cursor:help;border-bottom:1px dotted #888">{int(record["node_count"])}</span>',
            unsafe_allow_html=True,
        )
        columns[4].write(str(record["name"]))
        payload = record.get("payload", {})
        preset_name = submitted_preset_name(payload, submission_presets) if isinstance(payload, dict) else ""
        columns[5].write(preset_name)
        columns[6].write(str(record["partition"]))
        columns[7].write(str(record.get("submitted_at") or "-"))
        columns[8].write(str(record["state"]))
        with columns[9]:
            render_node_connect_control(record, is_live)
        command_label = "查看" if record.get("submit_command") else "捕获"
        if columns[10].button(command_label, key=f"quick_command_{record['id']}"):
            st.session_state["quick_command_record_id"] = int(record["id"])
            st.session_state.pop("quick_command_capture_preview", None)
            st.session_state.pop("quick_command_auto_captured", None)
            st.rerun()
        with columns[11]:
            stop_col, restart_col, delete_col = st.columns(3)
            token = f"quick_cancel_{record['id']}"
            if stop_col.button("取消作业", key="ask_" + token, disabled=not is_live):
                st.session_state["pending_action"] = token
                st.rerun()
            can_restart = not (is_live or is_pending) and bool(str(record.get("submit_command", "")).strip())
            if restart_col.button("再次启动", key=f"quick_restart_{record['id']}", disabled=not can_restart):
                restart_quick_job(record)
                st.rerun()
            if delete_col.button("删除", key=f"quick_delete_{record['id']}", disabled=is_live or is_pending):
                STORE.delete_quick_job(int(record["id"]))
                st.rerun()
        if st.session_state.get("pending_action") == token:
            st.warning(f"确认取消 Slurm Job {job_id}？")
            yes, no = st.columns(2)
            if yes.button("确认取消", type="primary", key="yes_" + token):
                if cancel_job(job_id):
                    STORE.update_quick_job_state(int(record["id"]), "CANCELLING")
                    cancelling = dict(st.session_state.get("quick_job_cancelling", {}))
                    cancelling[job_id] = time.time() + 30
                    st.session_state["quick_job_cancelling"] = cancelling
                st.session_state.pop("pending_action", None)
                st.rerun()
            if no.button("返回", key="no_" + token):
                st.session_state.pop("pending_action", None)
                st.rerun()
        st.divider()
    selected_command_id = st.session_state.get("quick_command_record_id")
    if selected_command_id:
        selected_record = next((item for item in records if int(item["id"]) == int(selected_command_id)), None)
        if selected_record:
            show_submission_command_editor(selected_record)
        else:
            st.session_state.pop("quick_command_record_id", None)
    cancelling = {
        str(job_id): float(deadline)
        for job_id, deadline in dict(st.session_state.get("quick_job_cancelling", {})).items()
        if job_id in live_ids and time.time() < float(deadline)
    }
    st.session_state["quick_job_cancelling"] = cancelling
    waiting = dict(st.session_state.get("quick_job_waiting", {}))
    if cancelling or waiting:
        descriptions = []
        if waiting:
            descriptions.append("等待新 Job 进入队列：" + ", ".join(waiting))
        if cancelling:
            descriptions.append("等待取消完成：" + ", ".join(cancelling))
        st.info("；".join(descriptions) + "。")
        if st_autorefresh:
            refresh_ids = "_".join(sorted([*waiting, *cancelling]))
            st_autorefresh(interval=1500, limit=20, key="quick_job_status_refresh_" + refresh_ids)
        elif st.button("刷新快捷作业状态", key="quick_job_manual_refresh"):
            st.rerun()


def metric_quick_actions(selected_nodes: Sequence[str], partition: str) -> None:
    scripts = script_catalog()
    keepalive = [item for item in scripts if item.category == "keepalive"]
    gpu_keepalive = [item for item in keepalive if int(item.gpu or 0) > 0]
    cpu_keepalive = [item for item in keepalive if int(item.gpu or 0) == 0]
    vllm_scripts = [item for item in scripts if item.category == "vllm"]

    st.subheader("一键快捷保活")
    gpu_script_col, gpu_button_col = st.columns([5, 1])
    with gpu_script_col:
        gpu_script = persistent_script_select(
            "GPU 保活脚本", gpu_keepalive,
            "metric_quick_gpu_keepalive_script", "metric_quick_gpu_keepalive_script",
        )
    if gpu_button_col.button(
        "提交 GPU 保活", type="primary", disabled=not (selected_nodes and gpu_script),
        key="metric_quick_gpu_keepalive_submit",
    ):
        quick_submit_from_metrics(
            gpu_script, selected_nodes, partition, "METRIC_QUICK_GPU_KEEPALIVE", "GPU 保活",
        )

    cpu_script_col, cpu_button_col = st.columns([5, 1])
    with cpu_script_col:
        cpu_script = persistent_script_select(
            "CPU 保活脚本", cpu_keepalive,
            "metric_quick_cpu_keepalive_script", "metric_quick_cpu_keepalive_script",
        )
    if cpu_button_col.button(
        "提交 CPU 保活", type="primary", disabled=not (selected_nodes and cpu_script),
        key="metric_quick_cpu_keepalive_submit",
    ):
        quick_submit_from_metrics(
            cpu_script, selected_nodes, partition, "METRIC_QUICK_CPU_KEEPALIVE", "CPU 保活",
        )

    st.subheader("一键快捷提交 Slurm")
    slurm_script_col, slurm_button_col = st.columns([5, 1])
    with slurm_script_col:
        slurm_script = persistent_script_select(
            "Slurm 脚本", scripts, "metric_quick_slurm_script", "metric_quick_slurm_script",
        )
    if slurm_button_col.button(
        "提交 Slurm", type="primary", disabled=not (selected_nodes and slurm_script),
        key="metric_quick_slurm_submit",
    ):
        quick_submit_from_metrics(slurm_script, selected_nodes, partition, "METRIC_QUICK_SLURM")

    st.subheader("一键快捷提交 vLLM")
    vllm_script_col, vllm_button_col = st.columns([5, 1])
    with vllm_script_col:
        vllm_script = persistent_script_select(
            "vLLM 脚本", vllm_scripts, "metric_quick_vllm_script", "metric_quick_vllm_script",
        )
    if vllm_button_col.button(
        "提交 vLLM", type="primary", disabled=not (selected_nodes and vllm_script),
        key="metric_quick_vllm_submit",
    ):
        job_id = quick_submit_from_metrics(vllm_script, selected_nodes, partition, "METRIC_QUICK_VLLM")
        if job_id:
            st.session_state["vllm_wait_job"] = job_id
            st.session_state["vllm_recent_job"] = job_id


def run_metric_payload(payload: Mapping[str, Any]) -> None:
    nodes = split_values(str(payload.get("nodes", "")))
    if not nodes:
        st.error("采样组没有节点。")
        return
    addresses = SLURM.node_addresses()
    with st.spinner(f"正在采样 {len(nodes)} 个节点……"):
        data = METRICS.collect(
            str(payload.get("partition", "")), nodes, addresses,
            str(payload.get("method", "auto")), int(payload.get("timeout", 20)),
        )
    st.session_state["node_metrics"] = data
    st.session_state["node_metrics_name"] = str(payload.get("name", "手工采样"))
    st.session_state["node_metrics_partition"] = str(payload.get("partition", ""))
    st.session_state["node_metrics_version"] = int(st.session_state.get("node_metrics_version", 0)) + 1


def metric_presets_editor(default_partition: str, eligible: Sequence[str]) -> None:
    presets = STORE.presets("metrics")
    with st.expander("新增或修改采样组"):
        choices = ["＋ 新增采样组"] + [str(item["name"]) for item in presets]
        choice = st.selectbox("采样组名称", choices, key="metrics_choice")
        editing = next((item for item in presets if item["name"] == choice), None)
        payload = editing["payload"] if editing else {}
        suffix = str(editing["id"]) if editing else "new"
        name = st.text_input("新采样组名称" if not editing else "名称", value=editing["name"] if editing else "", key="metrics_name_" + suffix)
        preferred = str(payload.get("partition", "5090-eth-2" if not editing else default_partition))
        partitions = partition_options(preferred)
        partition = st.selectbox("Partition", partitions, key="metrics_partition_" + suffix) if partitions else preferred
        nodes_default = split_values(str(payload.get("nodes", "")))
        nodes, _, _, _ = partition_node_picker(
            partition,
            "metrics_" + suffix,
            nodes_default,
            show_select_all=True,
            include_unavailable=True,
        ) if partition else ([], [], 0, True)
        method_labels = {"自动（exporter → srun）": "auto", "仅 exporter": "direct", "仅 srun": "srun"}
        current_method = str(payload.get("method", "auto"))
        current_label = next((label for label, value in method_labels.items() if value == current_method), list(method_labels)[0])
        c1, c2 = st.columns(2)
        method_label = c1.selectbox("采样方式", list(method_labels), index=list(method_labels).index(current_label), key="metrics_method_" + suffix)
        timeout = c2.number_input("单节点超时（秒）", min_value=3, max_value=120, value=int(payload.get("timeout", 20)), key="metrics_timeout_" + suffix)
        if st.button("更新采样组" if editing else "新增采样组", type="primary", disabled=not (name and nodes), key="metrics_save_" + suffix):
            STORE.save_preset(
                "metrics", name,
                {"name": name, "partition": partition, "nodes": ",".join(nodes),
                 "method": method_labels[method_label], "timeout": int(timeout),
                 "summary": f"{partition} · {len(nodes)} nodes · {method_label}"},
                int(editing["id"]) if editing else None,
            )
            st.rerun()


def page_cluster() -> None:
    st.title("集群资源")
    known = list(CONFIG.get("default_partitions", []))
    partition_rows, partition_result = SLURM.partitions()
    if partition_result.ok:
        known = list(dict.fromkeys([row["partition"] for row in partition_rows] + known))
    if not known:
        st.error("没有发现可用 Partition。")
        return
    if st.session_state.get("cluster_partition") not in known:
        st.session_state["cluster_partition"] = known[0]
    partition = str(st.session_state["cluster_partition"])
    rows, result = SLURM.nodes(partition)
    eligible = [str(row["node"]) for row in rows if not str(row["state"]).startswith(("down", "drain"))]

    st.subheader("保存的采样组")
    preset_cards("metrics", "立即采样", run_metric_payload)
    metric_presets_editor(partition, eligible)

    st.divider()
    render_quick_job_manager()
    st.divider()

    with st.expander("临时采样", expanded=False):
        selected = st.multiselect("实时采样节点", eligible, default=eligible[: min(4, len(eligible))])
        method = st.radio("方式", ["auto", "direct", "srun"], format_func=lambda value: {"auto": "自动", "direct": "仅 exporter", "srun": "仅 srun"}[value], horizontal=True)
        if st.button("采样实时 CPU / 内存 / GPU", disabled=not selected):
            run_metric_payload({"partition": partition, "nodes": ",".join(selected), "method": method, "timeout": 20})
    if st.session_state.get("node_metrics"):
        st.subheader("采样结果 · " + st.session_state.get("node_metrics_name", ""))
        metric_partition = str(st.session_state.get("node_metrics_partition", partition))
        metric_partition_rows = rows
        if metric_partition and metric_partition != partition:
            metric_partition_rows, _ = SLURM.nodes(metric_partition)
        node_states = {
            str(item.get("node", "")): str(item.get("state", "unknown"))
            for item in metric_partition_rows
        }
        selected_metric_nodes = render_metric_results(st.session_state["node_metrics"], node_states)
        metric_quick_actions(
            selected_metric_nodes,
            metric_partition,
        )

    st.divider()
    st.subheader("Partition 节点状态")
    st.caption("这部分放在页面底部；切换 Partition 后页面会自动重新加载对应节点。")
    st.selectbox("Partition", known, key="cluster_partition")
    if not result.ok:
        error_from(result, "无法读取节点状态")
        return
    total = len(rows)
    idle = sum(str(row["state"]).startswith("idle") for row in rows)
    unavailable = sum(str(row["state"]).startswith(("down", "drain")) for row in rows)
    total_cpu = sum(int(row["cpu_total"]) for row in rows)
    allocated_cpu = sum(int(row["cpu_allocated"]) for row in rows)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("节点", total); c2.metric("Idle", idle); c3.metric("Down / Drain", unavailable)
    c4.metric("CPU 已分配", f"{allocated_cpu}/{total_cpu}")
    st.dataframe(rows, use_container_width=True, hide_index=True)


def custom_submit_form(scripts: Sequence[ScriptInfo], key: str) -> None:
    if not scripts:
        st.warning("没有发现可提交的 Slurm 脚本。")
        return
    selected_path = st.selectbox("Slurm 脚本", [item.path for item in scripts], format_func=lambda value: script_label(next(x for x in scripts if x.path == value)), key=key + "_script")
    selected = next(item for item in scripts if item.path == selected_path)
    st.caption(f"扫描目录：{', '.join(str(path) for path in CONFIG.paths('script_roots'))} · 当前发现 {len(scripts)} 个脚本。新增 `.slurm` 后刷新页面即可出现。")
    script_key = Path(selected_path).name
    partitions = partition_options(selected.partition)
    partition = st.selectbox("Partition", partitions, key=f"{key}_partition_{script_key}") if partitions else selected.partition
    node_defaults = split_values(selected.nodelist)
    selected_nodes, _, nodes, _ = partition_node_picker(
        partition,
        f"{key}_{script_key}",
        node_defaults,
        int(selected.nodes or len(node_defaults) or 1),
        True,
    ) if partition else ([], [], 1, True)
    nodelist = ",".join(selected_nodes)
    exports = st.text_area("环境变量（每行 KEY=VALUE）", key=key + "_exports")
    payload = {"script_path": selected_path, "nodelist": nodelist, "partition": partition, "nodes": nodes, "exports": exports}
    st.code(f"sbatch --parsable --chdir={Path(selected_path).parent} --partition={partition} --nodelist={nodelist} --nodes={nodes} {selected_path}", language="bash")
    confirm = st.checkbox("我已核对节点、资源数和脚本内容", key=key + "_confirm")
    if st.button("提交 Slurm 作业", type="primary", disabled=not (confirm and selected_nodes), key=key + "_submit"):
        submit_payload(payload)


def page_submit() -> None:
    st.title("提交 Slurm 作业")
    scripts = script_catalog()
    st.subheader("一键提交预设")
    preset_cards("slurm", "提交", lambda payload: submit_payload(payload, "SBATCH_PRESET"))
    slurm_preset_editor("slurm", scripts)
    st.divider()
    render_job_management()
    st.divider()
    with st.expander("自定义提交 Slurm 作业", expanded=False):
        categories = {"全部": "", "Ray CPU": "ray-cpu", "Ray GPU": "ray-gpu", "vLLM": "vllm", "任务驻留": "keepalive"}
        label = st.radio("脚本类型", list(categories), horizontal=True)
        custom_submit_form([item for item in scripts if not categories[label] or item.category == categories[label]], "custom")


def users_value(key: str = "job_users") -> str:
    default = str(CONFIG.get("default_user", os.environ.get("USER", "")))
    return st.text_input("用户（支持逗号分隔多个用户）", value=default, key=key)


def active_jobs(users: str) -> List[Dict[str, str]]:
    values = split_values(users)
    jobs, result = SLURM.jobs(",".join(values))
    if not result.ok:
        error_from(result, "无法读取 squeue")
        return []
    return jobs


def cancel_job(job_id: str) -> bool:
    result = SLURM.cancel(job_id)
    STORE.record_action("SCANCEL", job_id, "OK" if result.ok else "FAILED", result.stderr)
    if not result.ok:
        error_from(result)
    return result.ok


def job_state_badge(state: str) -> str:
    normalized = state.strip().upper() or "UNKNOWN"
    terminal = {
        "BOOT_FAIL", "CANCELLED", "COMPLETED", "COMPLETING", "DEADLINE", "FAILED",
        "NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED", "REVOKED", "SPECIAL_EXIT",
        "STAGE_OUT", "STOPPED", "TIMEOUT",
    }
    if normalized == "RUNNING":
        foreground, background, border = "#166534", "#dcfce7", "#86efac"
    elif normalized in terminal:
        foreground, background, border = "#991b1b", "#fee2e2", "#fca5a5"
    else:
        foreground, background, border = "#92400e", "#fef3c7", "#fcd34d"
    return (
        f'<span style="display:inline-block;margin-left:.45rem;padding:.12rem .48rem;'
        f'border:1px solid {border};border-radius:999px;background:{background};'
        f'color:{foreground};font-size:.75rem;font-weight:700;vertical-align:middle">'
        f'{html.escape(normalized)}</span>'
    )


def job_is_cancellable(state: str) -> bool:
    return state.strip().upper() in {
        "CONFIGURING", "PENDING", "REQUEUE_FED", "REQUEUE_HOLD", "REQUEUED",
        "RESV_DEL_HOLD", "RUNNING", "SIGNALING", "SUSPENDED",
    }


def render_slurm_detail_and_logs(job: Mapping[str, str], prefix: str, show_detail: bool = True) -> None:
    detail, result = SLURM.job_detail(job["job_id"])
    if not result.ok:
        error_from(result, "无法读取作业详情")
        return
    if show_detail:
        st.json(detail, expanded=False)
    buttons = st.columns(2)
    for column, (detail_key, label) in zip(buttons, (("StdOut", "Tail 标准输出"), ("StdErr", "Tail 标准错误"))):
        pattern = detail.get(detail_key, "")
        tail_token = f"{prefix}_{job['job_id']}_{detail_key}"
        if column.button(label, key="show_" + tail_token, disabled=not pattern):
            if st.session_state.get("slurm_tail_target") == tail_token:
                st.session_state.pop("slurm_tail_target", None)
            else:
                st.session_state["slurm_tail_target"] = tail_token
            st.rerun()
        if st.session_state.get("slurm_tail_target") == tail_token and pattern:
            path = expand_slurm_log_pattern(pattern, job["job_id"])
            if not Path(path).is_absolute() and detail.get("WorkDir"):
                path = str(Path(detail["WorkDir"]) / path)
            column.caption(path)
            try:
                with column:
                    follow_bottom = log_tail_controls(tail_token)
                    scroll_text(tail_text(Path(path), 300), tail_token, follow_bottom)
            except OSError as exc:
                column.warning(str(exc))


def job_card(
    job: Mapping[str, str], prefix: str, allow_cancel: bool = True,
    show_details: bool = True, show_logs: bool = False, preset_name: str = "",
    node_connect_record: Optional[Mapping[str, Any]] = None,
) -> None:
    with st.container(border=True):
        state = str(job.get("state", "UNKNOWN"))
        title = html.escape(f"{job['job_id']} · {job['name']}")
        st.markdown(f"<strong>{title}</strong>{job_state_badge(state)}", unsafe_allow_html=True)
        if preset_name:
            st.caption("提交预设：" + preset_name)
        st.caption(f"{job['user']} · {job['partition']} · {job['elapsed']}/{job['limit']} · {job['nodes']} nodes")
        st.caption("使用节点/排队原因：" + str(job.get("reason_or_nodes", "-")))
        if node_connect_record is not None:
            st.caption("节点连接")
            render_node_connect_control(node_connect_record, str(job.get("state", "")).upper() == "RUNNING")
        if show_details:
            render_slurm_detail_and_logs(job, prefix)
        elif show_logs:
            render_slurm_detail_and_logs(job, prefix, show_detail=False)
        token = f"cancel_{prefix}_{job['job_id']}"
        if allow_cancel and job_is_cancellable(state):
            if st.button("取消作业", key="ask_" + token):
                st.session_state["pending_action"] = token
                st.rerun()
            if st.session_state.get("pending_action") == token:
                st.warning("确认 scancel 并释放资源？")
                y, n = st.columns(2)
                if y.button("确认取消", type="primary", key="yes_" + token):
                    if cancel_job(job["job_id"]):
                        cancelling = dict(st.session_state.get("cancelling_jobs", {}))
                        cancelling[job["job_id"]] = time.time() + 20
                        st.session_state["cancelling_jobs"] = cancelling
                    st.session_state.pop("pending_action", None)
                    st.rerun()
                if n.button("返回", key="no_" + token):
                    st.session_state.pop("pending_action", None); st.rerun()
        elif st.session_state.get("pending_action") == token:
            st.session_state.pop("pending_action", None)


def submitted_preset_name(
    payload: Mapping[str, Any], presets: Sequence[Mapping[str, Any]]
) -> str:
    recorded = str(payload.get("_preset_name", "")).strip()
    if recorded:
        return recorded
    # Best-effort compatibility for jobs submitted before preset metadata was
    # introduced. Only accept an unambiguous exact match of submit parameters.
    keys = ("script_path", "partition", "nodelist", "nodes", "exports")
    matches = []
    for preset in presets:
        candidate = preset.get("payload", {})
        if isinstance(candidate, Mapping) and all(
            str(candidate.get(key, "")) == str(payload.get(key, "")) for key in keys
        ):
            matches.append(str(preset.get("name", "")))
    return matches[0] if len(matches) == 1 else ""


def job_connection_record(
    job: Mapping[str, str], stored: Mapping[str, Any]
) -> Dict[str, Any]:
    record = dict(stored)
    record["slurm_job_id"] = str(job["job_id"])
    payload = dict(record.get("payload", {}))
    if str(job.get("state", "")).upper() == "RUNNING":
        nodes, result = SLURM.job_nodes(str(job["job_id"]), limit=1)
        if result.ok and nodes:
            payload["_master_address"] = str(nodes[0].get("address") or nodes[0].get("node", ""))
    record["payload"] = payload
    return record


def job_row_label(job: Mapping[str, str]) -> str:
    return f"{job['job_id']} · {job['name']} · {job['nodes']} 节点"


def render_managed_job_row(
    job: Mapping[str, str], prefix: str, preset_name: str,
    stored: Mapping[str, Any],
) -> None:
    job_id = str(job["job_id"])
    state = str(job.get("state", "UNKNOWN"))
    detail_col, state_col, connect_col, cancel_col = st.columns([6, 1.4, 2, 1.5], vertical_alignment="top")
    with detail_col.expander(job_row_label(job), expanded=False):
        st.caption(
            f"用户：{job['user']}　·　Partition：{job['partition']}"
            f"　·　运行：{job['elapsed']}/{job['limit']}"
        )
        st.caption("使用节点/排队原因：" + str(job.get("reason_or_nodes", "-")))
        if preset_name:
            st.caption("提交预设：" + preset_name)
        render_slurm_detail_and_logs(job, prefix)
    state_col.markdown(job_state_badge(state), unsafe_allow_html=True)
    with connect_col:
        render_node_connect_control(job_connection_record(job, stored), state.upper() == "RUNNING")
    token = f"cancel_{prefix}_{job_id}"
    if job_is_cancellable(state) and cancel_col.button("取消作业", key="ask_" + token):
        st.session_state["pending_action"] = token
        st.rerun()
    if st.session_state.get("pending_action") == token:
        if not job_is_cancellable(state):
            st.session_state.pop("pending_action", None)
        else:
            st.warning(f"确认取消作业 {job_id} 并释放资源？")
            yes, no, _ = st.columns([1.2, 1, 7])
            if yes.button("确认取消", type="primary", key="yes_" + token):
                if cancel_job(job_id):
                    cancelling = dict(st.session_state.get("cancelling_jobs", {}))
                    cancelling[job_id] = time.time() + 20
                    st.session_state["cancelling_jobs"] = cancelling
                st.session_state.pop("pending_action", None)
                st.rerun()
            if no.button("返回", key="no_" + token):
                st.session_state.pop("pending_action", None)
                st.rerun()


def render_job_management() -> None:
    title_column, refresh_column, delete_column = st.columns([5, 1, 1.7])
    title_column.subheader("作业管理")
    if refresh_column.button("立即刷新", key="job_management_manual_refresh"):
        st.rerun()
    if st_autorefresh:
        st_autorefresh(interval=5000, key="job_management_status_refresh")
        st.caption("作业状态每 5 秒自动更新。")
    else:
        st.warning("当前环境未安装 streamlit-autorefresh，请使用“立即刷新”更新作业状态。")
    users = users_value()
    user_list = split_values(users)
    jobs = active_jobs(users)
    stored_records = STORE.quick_jobs()
    stored_jobs = {str(item["slurm_job_id"]): item for item in stored_records}
    live_ids = {str(job["job_id"]) for job in jobs}
    stopped_records = stopped_quick_job_records(stored_records, live_ids)
    bulk_delete_stopped_control(
        "bulk_delete_managed_slurm_jobs",
        len(stopped_records),
        lambda: delete_stopped_quick_job_records(users),
        delete_column,
    )
    slurm_presets = STORE.presets("slurm")
    columns = st.columns(max(1, len(user_list)))
    for index, user in enumerate(user_list):
        with columns[index]:
            st.subheader(user)
            owned = [job for job in jobs if job["user"] == user]
            if not owned:
                st.caption("暂无活动作业")
            for job in owned:
                stored = stored_jobs.get(str(job["job_id"]), {})
                payload = stored.get("payload", {}) if isinstance(stored, dict) else {}
                preset_name = submitted_preset_name(payload, slurm_presets) if isinstance(payload, dict) else ""
                render_managed_job_row(job, user, preset_name, stored)
    live_ids = {job["job_id"] for job in jobs}
    cancelling = dict(st.session_state.get("cancelling_jobs", {}))
    waiting = [job_id for job_id, deadline in cancelling.items() if job_id in live_ids and time.time() < float(deadline)]
    st.session_state["cancelling_jobs"] = {job_id: deadline for job_id, deadline in cancelling.items() if job_id in live_ids}
    if waiting:
        st.info("正在等待 Slurm 完成取消：" + ", ".join(waiting) + "。页面每 1.5 秒自动刷新，期间可能短暂显示 CG。")
        if st_autorefresh:
            st_autorefresh(interval=1500, limit=14, key="cancel_job_refresh_" + "_".join(waiting))
    with st.expander("最近作业与退出码（用于排查立即结束）"):
        if st.button("读取今天的 sacct", key="submit_history"):
            history: List[Dict[str, str]] = []
            for user in user_list:
                user_rows, history_result = SLURM.job_history(user, "today")
                if history_result.ok:
                    history.extend(user_rows)
                else:
                    error_from(history_result, "无法读取 sacct")
            st.session_state["submit_job_history"] = history
        if st.session_state.get("submit_job_history"):
            st.dataframe(st.session_state["submit_job_history"], use_container_width=True, hide_index=True)


def page_jobs() -> None:
    st.title("Slurm 作业")
    render_job_management()


def select_slurm_job_cards(jobs: Sequence[Mapping[str, str]], key: str, allow_cancel: bool = False) -> Optional[str]:
    if not jobs:
        st.warning("没有活动 Slurm 作业。")
        return None
    state_key = "selected_job_" + key
    visible_ids = {str(job["job_id"]) for job in jobs}
    if st.session_state.get(state_key) not in visible_ids:
        st.session_state.pop(state_key, None)
    stored_jobs = {str(item["slurm_job_id"]): item for item in STORE.quick_jobs()}
    for job in jobs:
        job_id = str(job["job_id"])
        state = str(job.get("state", "UNKNOWN"))
        selected = st.session_state.get(state_key) == job_id
        widths = [5.5, 1.35, 2, 1.2, 1.5] if allow_cancel else [6, 1.4, 2, 1.4]
        columns = st.columns(widths, vertical_alignment="top")
        with columns[0].expander(("✓ " if selected else "") + job_row_label(job), expanded=False):
            st.caption(
                f"用户：{job['user']}　·　Partition：{job['partition']}"
                f"　·　运行：{job['elapsed']}/{job['limit']}"
            )
            st.caption("使用节点/排队原因：" + str(job.get("reason_or_nodes", "-")))
            if key == "vllm_endpoint" and selected:
                st.caption("Slurm 日志")
                render_slurm_detail_and_logs(job, "vllm", show_detail=False)
        columns[1].markdown(job_state_badge(state), unsafe_allow_html=True)
        with columns[2]:
            render_node_connect_control(
                job_connection_record(job, stored_jobs.get(job_id, {})),
                state.upper() == "RUNNING",
            )
        if columns[3].button(
            "已选择" if selected else "选择", key=f"select_{key}_{job_id}",
            disabled=selected, use_container_width=True,
        ):
            st.session_state[state_key] = job_id
            st.rerun()
        if allow_cancel:
            token = f"cancel_select_{key}_{job_id}"
            if job_is_cancellable(state) and columns[4].button("取消作业", key="ask_" + token, use_container_width=True):
                st.session_state["pending_action"] = token
                st.rerun()
            if st.session_state.get("pending_action") == token:
                st.warning(f"确认取消作业 {job_id} 并释放资源？")
                yes, no, _ = st.columns([1.2, 1, 7])
                if yes.button("确认取消", type="primary", key="yes_" + token):
                    cancel_job(job_id)
                    st.session_state.pop("pending_action", None)
                    st.rerun()
                if no.button("返回", key="no_" + token):
                    st.session_state.pop("pending_action", None)
                    st.rerun()
    return st.session_state.get(state_key)


def refresh_compute_tasks(tasks: Sequence[Mapping[str, Any]]) -> None:
    for task in tasks:
        try:
            status = GATEWAY.process_status(task["host"], int(task["port"]), str(task["pid"]))
        except Exception:
            status = "UNREACHABLE"
        STORE.update_compute_task(int(task["id"]), status)


def stopped_compute_tasks(tasks: Sequence[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    return [task for task in tasks if str(task.get("status", "")).upper() in {"STOPPED", "EXITED"}]


def delete_stopped_compute_task_records() -> int:
    stopped = stopped_compute_tasks(STORE.compute_tasks())
    return STORE.delete_compute_tasks([int(task["id"]) for task in stopped])


def stop_compute_task(task: Mapping[str, Any]) -> None:
    GATEWAY.stop_process(task["host"], int(task["port"]), str(task["pid"]))
    STORE.update_compute_task(int(task["id"]), "STOPPED")
    STORE.record_action("COMPUTE_TASK_STOP", str(task["id"]), "OK", str(task["pid"]))


def result_view(path_text: str, key: str) -> None:
    try:
        path = resolve_allowed(path_text, CONFIG.paths("browse_roots"), must_exist=True)
        if path.is_file():
            files = [{"path": str(path), "name": path.name}]
        else:
            st.dataframe(list_directory(path), use_container_width=True, hide_index=True)
            files = find_jsonl(path, 2)
        if files:
            selected = st.selectbox("JSONL 预览", [item["path"] for item in files], key="json_file_" + key)
            c1, c2 = st.columns(2)
            offset = c1.number_input("起始行", min_value=0, value=0, step=50, key="offset_" + key)
            limit = c2.number_input("行数", min_value=1, max_value=500, value=50, key="limit_" + key)
            records, stats = preview_jsonl(Path(selected), int(offset), int(limit))
            st.caption(f"显示 {stats['shown']} 行；坏行 {stats['invalid']}；超长截断 {stats['truncated']}")
            st.dataframe(records, use_container_width=True, hide_index=True)
    except Exception as exc:
        st.warning(str(exc))


def task_resources(task_id: int) -> None:
    resources = STORE.task_resources(task_id)
    with st.expander("配置日志与结果跟踪", expanded=not resources):
        for resource in resources:
            c1, c2, c3, c4 = st.columns([1, 2, 5, 1])
            c1.write(resource["kind"]); c2.write(resource["label"]); c3.code(resource["path"], language="text")
            if c4.button("删除", key=f"delete_resource_{resource['id']}"):
                STORE.delete_task_resource(int(resource["id"])); st.rerun()
        c1, c2 = st.columns(2)
        kind = c1.selectbox("类型", ["log", "result"], format_func=lambda value: "日志 Tail" if value == "log" else "结果 / JSONL", key=f"resource_kind_{task_id}")
        label = c2.text_input("标签", key=f"resource_label_{task_id}")
        path = st.text_input("绝对路径", key=f"resource_path_{task_id}")
        if st.button("添加跟踪项", disabled=not (label and path), key=f"add_resource_{task_id}"):
            try:
                resolve_allowed(path, CONFIG.paths("browse_roots"), must_exist=False)
                STORE.add_task_resource(task_id, kind, label, path); st.rerun()
            except Exception as exc:
                st.error(str(exc))
    if resources:
        tabs = st.tabs([f"{'日志' if item['kind']=='log' else '结果'} · {item['label']}" for item in resources])
        for tab, resource in zip(tabs, resources):
            with tab:
                if resource["kind"] == "log":
                    lines = st.number_input("末尾行数", 20, 5000, 300, key=f"log_lines_{resource['id']}")
                    viewer_key = f"task_{task_id}_resource_{resource['id']}"
                    follow_bottom = log_tail_controls(viewer_key)
                    try:
                        path = resolve_allowed(resource["path"], CONFIG.paths("browse_roots"), must_exist=True)
                        scroll_text(tail_text(path, int(lines)), viewer_key, follow_bottom)
                    except Exception as exc:
                        st.info("等待日志文件：" + str(exc))
                else:
                    result_view(resource["path"], str(resource["id"]))


def submit_compute_payload(
    payload: Mapping[str, Any], selected_job: str, node: str, address: str, port: int
) -> Optional[int]:
    try:
        command = str(payload.get("command", ""))
        submitted = GATEWAY.submit_command(address, port, command)
        if not submitted.ok:
            raise RuntimeError("未从计算节点连接通道获得任务 PID：" + submitted.output[-500:])
        task_id = STORE.add_compute_task(
            str(payload.get("task_name", "未命名任务")), str(payload.get("task_type", "general")),
            selected_job, address, port, submitted.command, submitted.pid, status="RUNNING",
        )
        log_path = extract_log_path(command)
        if log_path:
            st.session_state[f"resource_kind_{task_id}"] = "log"
            st.session_state[f"resource_label_{task_id}"] = log_path
            st.session_state[f"resource_path_{task_id}"] = log_path
        STORE.record_action("COMPUTE_TASK_SUBMIT", str(task_id), "OK", submitted.command)
        st.success(f"提交成功，记录 #{task_id}，计算节点父进程 PID={submitted.pid}")
        return task_id
    except Exception as exc:
        st.error(str(exc))
        return None


def compute_preset_cards(selected_job: str, node: str, address: str, port: int) -> None:
    presets = STORE.presets("compute")
    if not presets:
        st.caption("暂无计算节点任务预设，可在下方创建。")
        return
    for preset in presets:
        payload = preset["payload"]
        detail_col, run_col, edit_col, delete_col = st.columns([7, 1.15, 1, 1])
        with detail_col.expander(preset_summary(preset, "compute"), expanded=False):
            preset_details("compute", payload)
        if run_col.button(
            "一键提交", type="primary", key=f"run_compute_preset_{preset['id']}",
            disabled=not (selected_job and node), use_container_width=True,
        ):
            if submit_compute_payload(payload, selected_job, node, address, port):
                st.rerun()
        if edit_col.button("修改", key=f"edit_compute_preset_{preset['id']}", use_container_width=True):
            st.session_state["compute_preset_choice"] = str(preset["name"])
            st.rerun()
        token = f"delete_compute_preset_{preset['id']}"
        if delete_col.button("删除", key="ask_" + token, use_container_width=True):
            st.session_state["pending_action"] = token
            st.rerun()
        if st.session_state.get("pending_action") == token:
            st.warning(f"确认删除任务预设“{preset['name']}”？")
            yes, no, _ = st.columns([1, 1, 7])
            if yes.button("确认删除", key="yes_" + token):
                STORE.delete_preset(int(preset["id"]))
                st.session_state.pop("pending_action", None)
                st.rerun()
            if no.button("返回", key="no_" + token):
                st.session_state.pop("pending_action", None)
                st.rerun()


def compute_preset_editor() -> None:
    presets = STORE.presets("compute")
    with st.expander("新增或修改任务预设", expanded=False):
        choices = ["＋ 新增预设"] + [str(item["name"]) for item in presets]
        choice = st.selectbox("预设", choices, key="compute_preset_choice")
        editing = next((item for item in presets if item["name"] == choice), None)
        payload = editing["payload"] if editing else {}
        suffix = str(editing["id"]) if editing else "new"
        preset_name = st.text_input("预设名称", value=editing["name"] if editing else "", key="compute_preset_name_" + suffix)
        task_name = st.text_input("任务名称", value=str(payload.get("task_name", "")), key="compute_task_name_" + suffix)
        task_type = st.radio(
            "任务类型", ["ray", "general"], horizontal=True,
            format_func=lambda value: "Ray 任务" if value == "ray" else "一般任务",
            index=0 if payload.get("task_type", "ray") == "ray" else 1,
            key="compute_preset_type_" + suffix,
        )
        command = st.text_area("完整命令", value=str(payload.get("command", "")), height=220, key="compute_preset_command_" + suffix)
        if st.button("更新预设" if editing else "新增预设", type="primary", disabled=not (preset_name and task_name and command), key="save_compute_preset_" + suffix):
            STORE.save_preset(
                "compute", preset_name,
                {"task_name": task_name, "task_type": task_type, "command": command},
                int(editing["id"]) if editing else None,
            )
            st.rerun()


def independent_log_tracking() -> None:
    st.divider(); st.subheader("独立日志追踪")
    with st.expander("新增独立日志", expanded=False):
        label = st.text_input("标签", key="independent_log_label")
        path_text = st.text_input("日志绝对路径", key="independent_log_path")
        if st.button("添加独立日志", disabled=not (label and path_text), key="add_independent_log"):
            try:
                resolve_allowed(path_text, CONFIG.paths("browse_roots"), must_exist=False)
                STORE.add_bookmark(label, path_text); st.rerun()
            except Exception as exc:
                st.error(str(exc))
    logs = STORE.bookmarks()
    for item in logs:
        c1, c2, c3 = st.columns([2, 6, 1])
        c1.write(item["name"]); c2.code(item["path"], language="text")
        if c3.button("删除", key=f"delete_independent_log_{item['id']}"):
            STORE.delete_bookmark(int(item["id"])); st.rerun()
    if logs:
        tabs = st.tabs([str(item["name"]) for item in logs])
        for tab, item in zip(tabs, logs):
            with tab:
                lines = st.number_input("末尾行数", 20, 5000, 300, key=f"independent_lines_{item['id']}")
                viewer_key = f"independent_{item['id']}"
                follow_bottom = log_tail_controls(viewer_key)
                try:
                    path = resolve_allowed(item["path"], CONFIG.paths("browse_roots"), must_exist=True)
                    scroll_text(tail_text(path, int(lines)), viewer_key, follow_bottom)
                except Exception as exc:
                    st.info("等待日志文件：" + str(exc))


def page_compute_tasks() -> None:
    st.title("计算节点任务")
    users = users_value("compute_users")
    jobs = [job for job in active_jobs(users) if job["state"] == "RUNNING"]
    st.subheader("1. 选择 Slurm Job")
    selected_job = select_slurm_job_cards(jobs, "compute")
    selected = next((job for job in jobs if job["job_id"] == selected_job), None)
    node = ""
    address = ""
    port = 0
    if selected:
        nodes, _ = SLURM.job_nodes(selected_job)
        node_labels = {item["node"]: item["address"] for item in nodes}
        driver_node = next(iter(node_labels), "")
        node = st.radio(
            "计算节点", list(node_labels),
            format_func=lambda value: f"{value} · {node_labels[value]}" + (" · Ray driver 节点" if value == driver_node else ""),
            horizontal=True, key=f"compute_node_{selected_job}",
        ) if node_labels else ""
        address = node_labels.get(node, node)
        port = int(st.number_input(
            "计算节点连接端口", 1, 65535,
            shell_port(selected_job, int(CONFIG.get("shell_port_base", 10000)), int(CONFIG.get("shell_port_modulo", 40000))),
            key=f"compute_port_{selected_job}",
        ))
        st.caption("Ray 任务和一般任务都通过 Slurm Job 提供的计算节点连接通道提交；停止时终止任务父进程组及其全部子进程。")

    st.subheader("2. 一键提交预设")
    compute_preset_cards(selected_job or "", node, address, port)
    compute_preset_editor()

    with st.expander("3. 自定义提交任务", expanded=False):
        if not (selected and node):
            st.caption("请先选择一个正在运行的 Slurm Job 和计算节点。")
        else:
            task_type = st.radio("任务类型", ["ray", "general"], format_func=lambda value: "Ray 任务" if value == "ray" else "一般任务", horizontal=True)
            name = st.text_input("任务名称", placeholder="dataset-portrait")
            command = st.text_area("完整命令", height=220, placeholder="nohup python /path/task.py --config /path/config.yaml > /path/task.log 2>&1")
            detected_log = extract_log_path(command)
            if detected_log:
                st.caption("已识别日志文件：" + detected_log + "；提交后会预填到该任务的日志跟踪表单。")
            if st.button("提交计算节点任务", type="primary", disabled=not (name and command)):
                payload = {"task_name": name, "task_type": task_type, "command": command}
                if submit_compute_payload(payload, selected_job, node, address, port):
                    st.rerun()

    st.divider()
    task_title, refresh_column, delete_column = st.columns([5, 1.4, 1.7])
    task_title.subheader("已管理的计算节点任务")
    tasks = STORE.compute_tasks()
    if refresh_column.button("刷新全部 PID 状态", disabled=not tasks):
        refresh_compute_tasks(tasks); st.rerun()
    stopped_tasks = stopped_compute_tasks(tasks)
    bulk_delete_stopped_control(
        "bulk_delete_compute_tasks",
        len(stopped_tasks),
        delete_stopped_compute_task_records,
        delete_column,
    )
    for task in tasks:
        with st.container(border=True):
            c1, c2, c3, c4, c5 = st.columns([3, 2, 2, 2, 2])
            c1.markdown(f"**#{task['id']} · {task['name']}**")
            c2.write("Ray 任务" if task["task_type"] == "ray" else "一般任务")
            c3.write(f"Job {task['slurm_job_id']}")
            c4.write(f"PID {task['pid']}")
            c5.write(task["status"])
            st.code(task["command"], language="bash")
            token = f"stop_compute_{task['id']}"
            b1, b2, _ = st.columns([1, 1, 5])
            if b1.button(
                "停止任务", key="ask_" + token,
                disabled=str(task["status"]).upper() in {"STOPPED", "EXITED"},
            ):
                st.session_state["pending_action"] = token; st.rerun()
            if b2.button("删除记录", key=f"delete_compute_{task['id']}"):
                st.session_state["pending_action"] = "delete_compute_" + str(task["id"]); st.rerun()
            if st.session_state.get("pending_action") == token:
                st.warning("确认终止整个任务进程组（包括子进程）？")
                y, n = st.columns(2)
                if y.button("确认停止", type="primary", key="yes_" + token):
                    try: stop_compute_task(task)
                    except Exception as exc: st.error(str(exc))
                    st.session_state.pop("pending_action", None); st.rerun()
                if n.button("返回", key="no_" + token):
                    st.session_state.pop("pending_action", None); st.rerun()
            delete_token = "delete_compute_" + str(task["id"])
            if st.session_state.get("pending_action") == delete_token:
                st.warning("只删除平台记录，不停止进程。确认？")
                y, n = st.columns(2)
                if y.button("确认删除记录", key="yes_" + delete_token):
                    STORE.delete_compute_task(int(task["id"])); st.session_state.pop("pending_action", None); st.rerun()
                if n.button("返回", key="no_" + delete_token):
                    st.session_state.pop("pending_action", None); st.rerun()
            task_resources(int(task["id"]))
    independent_log_tracking()


def page_vllm() -> None:
    st.title("vLLM 节点")
    scripts = script_catalog("vllm")
    st.subheader("一键启动预设")
    def start_vllm(payload: Mapping[str, Any]) -> None:
        job_id = submit_payload(payload, "VLLM_START")
        if job_id:
            st.session_state["vllm_wait_job"] = job_id
            st.session_state["vllm_recent_job"] = job_id

    preset_cards("vllm", "启动", start_vllm)
    slurm_preset_editor("vllm", scripts, "新增或修改 vLLM 预设")
    with st.expander("自定义提交 vLLM 脚本"):
        custom_submit_form(scripts, "vllm_custom")
    st.divider()
    endpoint_title, delete_column = st.columns([6, 1.7])
    endpoint_title.subheader("Endpoint")
    vllm_users = str(CONFIG.get("default_user", ""))
    all_jobs = active_jobs(vllm_users)
    live_ids = {str(job["job_id"]) for job in all_jobs}
    vllm_records = [record for record in STORE.quick_jobs() if is_vllm_quick_job(record)]
    stopped_vllm_records = stopped_quick_job_records(vllm_records, live_ids, is_vllm_quick_job)
    bulk_delete_stopped_control(
        "bulk_delete_vllm_jobs",
        len(stopped_vllm_records),
        lambda: delete_stopped_quick_job_records(vllm_users, is_vllm_quick_job),
        delete_column,
    )
    waiting_job_id = str(st.session_state.get("vllm_wait_job", ""))
    recent_job_id = str(st.session_state.get("vllm_recent_job", ""))
    waiting_job = next((job for job in all_jobs if job["job_id"] == waiting_job_id), None)
    if waiting_job_id and waiting_job and waiting_job["state"] == "RUNNING":
        st.session_state["selected_job_vllm_endpoint"] = waiting_job_id
        st.session_state.pop("vllm_wait_job", None)
        st.rerun()
    if waiting_job_id:
        state = waiting_job["state"] if waiting_job else "等待进入队列"
        st.info(f"正在等待新作业 {waiting_job_id} 就绪（{state}），Endpoint 区域会自动刷新。")
        if st_autorefresh:
            st_autorefresh(interval=2000, limit=150, key="vllm_start_refresh_" + waiting_job_id)
        elif st.button("刷新作业状态", key="vllm_start_manual_refresh"):
            st.rerun()
    jobs = [
        job for job in all_jobs
        if job["state"] == "RUNNING"
        and ("vllm" in job["name"].lower() or job["job_id"] in {waiting_job_id, recent_job_id})
    ]
    selected_job = select_slurm_job_cards(jobs, "vllm_endpoint", allow_cancel=True)
    addresses: List[str] = []
    inferred: Dict[str, str] = {}
    inferred_script = ""
    if selected_job:
        nodes, _ = SLURM.job_nodes(selected_job)
        addresses = [item["address"] for item in nodes]
        detail, detail_result = SLURM.job_detail(selected_job)
        command = detail.get("Command", "") if detail_result.ok else ""
        command_name = Path(command).name if command else ""
        matched = next((item for item in scripts if item.path == command or Path(item.path).name == command_name), None)
        if matched:
            try:
                inferred = infer_vllm_settings(matched.path)
                inferred_script = matched.path
            except OSError:
                pass
    endpoint_key = selected_job or "manual"
    addresses_text = st.text_area("节点 IP / hostname", value="\n".join(addresses), key="vllm_addresses_" + endpoint_key)
    ports_text = st.text_input("端口", value=inferred.get("ports") or str(CONFIG.get("vllm_default_ports", "8000-8007")), key="vllm_ports_" + endpoint_key)
    if inferred_script:
        st.caption(f"端口初值由活动 Job 对应脚本中的 `--port`、`PORT=$((基数+i))` 和循环范围推断：{inferred_script}。然后与每个作业节点组合生成 Endpoint；复杂的节点端口偏移仍可手工修改。")
    else:
        st.caption("未找到活动 Job 对应脚本，端口使用配置文件默认值；可手工修改。")
    try:
        endpoints = build_endpoints(split_values(addresses_text), parse_ports(ports_text))
    except Exception as exc:
        st.error(str(exc)); endpoints = []
    model_key = "vllm_model_" + endpoint_key
    if model_key not in st.session_state:
        st.session_state[model_key] = inferred.get("model") or str(CONFIG.get("vllm_default_model", ""))
    if st.button("从 Endpoint API 识别模型名", disabled=not endpoints, key="detect_vllm_model_" + endpoint_key):
        token = os.environ.get(str(CONFIG.get("vllm_api_token_env", "VLLM_API_TOKEN")), "")
        detections = detect_models(endpoints, token)
        st.session_state["vllm_model_detections"] = detections
        detected = next((str(name) for item in detections if item.get("ok") for name in item.get("models", []) if name), "")
        if detected:
            st.session_state[model_key] = detected
            st.rerun()
    model = st.text_input("模型名（可自动识别，也可人工修改）", key=model_key)
    if st.session_state.get("vllm_model_detections"):
        with st.expander("模型识别结果", expanded=False):
            st.dataframe(st.session_state["vllm_model_detections"], use_container_width=True, hide_index=True)
    style = st.radio("导出格式", ["python", "json", "lines"], horizontal=True)
    st.code(format_endpoints(endpoints, style), language="python" if style == "python" else "text")
    endpoint_check_signature = json.dumps(
        {"endpoints": endpoints, "model": model}, ensure_ascii=False, sort_keys=True
    )
    if st.button("并发检查 Endpoint", disabled=not endpoints):
        token = os.environ.get(str(CONFIG.get("vllm_api_token_env", "VLLM_API_TOKEN")), "")
        st.session_state["endpoint_checks"] = check_endpoints(endpoints, model, token)
        st.session_state["endpoint_checks_signature"] = endpoint_check_signature
    checks = (
        st.session_state.get("endpoint_checks", [])
        if st.session_state.get("endpoint_checks_signature") == endpoint_check_signature
        else []
    )
    if checks:
        st.metric("可用 Endpoint", f"{sum(bool(x['ok']) for x in checks)}/{len(checks)}")
        st.dataframe(checks, use_container_width=True, hide_index=True)
    with st.expander("更新 YAML 配置"):
        roots = CONFIG.paths("editable_config_roots")
        st.caption("允许更新的配置根目录：" + ("、".join(str(path) for path in roots) if roots else "未配置"))
        target = st.text_input(
            "YAML 文件地址",
            placeholder="请输入 .yaml / .yml 文件的绝对路径",
            key="yaml_target_path",
        ).strip()
        if st.button("识别 Endpoint 配置位置", key="yaml_scan", disabled=not target):
            try:
                if not Path(target).expanduser().is_absolute():
                    raise ValueError("请输入 YAML 文件的绝对路径")
                st.session_state["yaml_blocks"] = find_yaml_endpoint_blocks(target, roots)
                st.session_state["yaml_scanned_target"] = target
            except Exception as exc:
                st.error(str(exc))
        blocks = st.session_state.get("yaml_blocks", []) if st.session_state.get("yaml_scanned_target") == target else []
        if target and st.session_state.get("yaml_scanned_target") == target and not blocks:
            st.warning("没有识别到包含 /v1/chat/completions 或 /v1/completions 的 YAML 字段。")
        if blocks:
            block = st.selectbox(
                "匹配位置",
                blocks,
                format_func=lambda item: f"{item['key']} · 第 {item['start']}-{item['end']} 行",
                key="yaml_block",
            )
            st.caption("当前内容")
            st.code(str(block["text"]), language="yaml")
            try:
                drop_failed = st.checkbox(
                    "去掉检查 Endpoint 失败的节点",
                    value=True,
                    key="yaml_drop_failed_endpoints",
                )
                replacement_endpoints = select_healthy_endpoints(endpoints, checks, drop_failed)
                if drop_failed and not checks:
                    st.warning("当前 Endpoint 列表尚未完成检查。请先点击“并发检查 Endpoint”，或取消该选项以使用全部 Endpoint。")
                elif drop_failed:
                    st.caption(
                        f"检查通过 {len(replacement_endpoints)}/{len(endpoints)} 个；"
                        f"替换时将排除 {len(endpoints) - len(replacement_endpoints)} 个失败 Endpoint。"
                    )
                preview = preview_yaml_endpoint_update(target, block, replacement_endpoints, roots)
                preview_start = int(block["start"]) - 1
                replacement_line_count = 1 if block["style"] == "inline" else len(replacement_endpoints) + 1
                preview_lines = preview.splitlines()[preview_start:preview_start + replacement_line_count]
                st.caption("替换预览")
                st.code("\n".join(preview_lines), language="yaml")
                preview_identity = str(abs(hash((target, str(block["id"]), tuple(replacement_endpoints)))))
                confirm = st.checkbox(
                    "我已核对匹配位置和替换内容，并同意创建备份",
                    key="yaml_update_confirm_" + preview_identity,
                )
                if st.button(
                    "写入 YAML Endpoint 列表",
                    type="primary",
                    disabled=not (confirm and replacement_endpoints),
                    key="yaml_write_" + preview_identity,
                ):
                    backup = update_yaml_endpoint_block(target, block, replacement_endpoints, roots)
                    st.success(f"更新完成，原文件备份：{backup}")
                    st.session_state.pop("yaml_blocks", None)
            except Exception as exc:
                st.error(str(exc))


def page_keepalive() -> None:
    st.title("XX保活")
    scripts = script_catalog("keepalive")
    st.subheader("一键启动预设")
    preset_cards("keepalive", "启动", lambda payload: submit_payload(payload, "KEEPALIVE_START"))
    slurm_preset_editor("keepalive", scripts, "新增或修改预设")
    st.divider(); st.subheader("活跃作业")
    jobs = active_jobs(str(CONFIG.get("default_user", "")))
    keep_jobs = [job for job in jobs if any(token in job["name"].lower() for token in ("keep", "stress"))]
    if not keep_jobs: st.caption("暂无活跃作业。")
    columns = st.columns(min(3, max(1, len(keep_jobs))))
    for index, job in enumerate(keep_jobs):
        with columns[index % len(columns)]:
            job_card(job, "keepalive", show_details=False, show_logs=True)


def page_history() -> None:
    st.title("历史与诊断")
    st.dataframe(SLURM.health(), use_container_width=True, hide_index=True)
    if not os.environ.get("SLURM_MANAGER_PASSWORD"):
        st.warning("当前使用默认密码，建议设置 SLURM_MANAGER_PASSWORD。")
    st.caption(f"脚本目录：{', '.join(str(path) for path in CONFIG.paths('script_roots'))}")
    st.subheader("操作审计"); st.dataframe(STORE.recent_actions(), use_container_width=True, hide_index=True)
    user = st.text_input("sacct 用户", value=str(CONFIG.get("default_user", ""))); start = st.text_input("起始时间", value="today")
    if st.button("读取 Slurm 历史"):
        rows, result = SLURM.job_history(user, start)
        if result.ok: st.dataframe(rows, use_container_width=True, hide_index=True)
        else: error_from(result)


PAGES = {
    "集群资源": page_cluster,
    "提交 Slurm": page_submit,
    "计算节点任务": page_compute_tasks,
    "vLLM": page_vllm,
    "XX保活": page_keepalive,
    "历史与诊断": page_history,
}

with st.sidebar:
    st.title("AIHPC"); st.caption("Slurm Manager · 0.7.10")
    page = st.radio("导航", list(PAGES), label_visibility="collapsed")
    st.divider(); health = SLURM.health(); st.metric("Slurm CLI", f"{sum(bool(x['available']) for x in health)}/{len(health)}")
    st.caption(f"用户：{CONFIG.get('default_user', '')}")

PAGES[page]()
