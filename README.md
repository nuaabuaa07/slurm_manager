# AIHPC Slurm Manager

面向开发机 → Slurm → 计算集群工作流的 Streamlit 管理页面。平台通过 Slurm Job 提供的计算节点连接通道提交 Ray 与一般任务。

## 已实现

- 按 partition 查看节点和 CPU 分配；优先通过 node-exporter/DCGM 采样实时指标，失败时快速回退 `srun`。采样结果同步显示节点最新的 Slurm 状态。
- 保存、修改、删除多组节点采样配置并一键执行。
- 采样结果可勾选多台服务器，快捷提交 GPU/CPU 保活、普通 Slurm 或 vLLM 作业；每个入口会记住上次选择的脚本。
- 快捷作业列表会以 Slurm 当前状态为准同步到 SQLite，支持取消、按原参数再次启动和删除已结束记录。
- 动态扫描 `script_roots` 下的 `.slurm`，并支持 Slurm 提交预设的增删改和一键提交。
- 提交页先选 partition、再多选该 partition 节点，可一键全选 idle 节点；节点数随选择自动计算，也允许手工覆盖，并拦截节点与 partition 冲突。
- `sbatch` 会显式设置 `--chdir` 到 Slurm 脚本所在目录，保证同目录的 `dj-ray*.bash` 和相对日志路径可以正常解析。
- 作业管理合并到提交页，按多个用户分列平铺活动 Job；直接展示详情、节点和标准输出/错误 Tail，`scancel` 后短时自动刷新直到作业消失。
- 计算节点任务支持 Ray 与一般任务统一通过计算节点连接通道提交，可维护一键任务预设；保存计算节点会话真实父 PID，并按进程组停止全部子进程。
- 每个计算节点任务可挂载多个日志和结果跟踪项，命令重定向日志可自动识别和预填；另支持多个独立日志追踪。日志窗口固定高度并可滚动。
- 从模板创建 vLLM 作业，可在预设中覆盖模型路径与名称；从脚本推断端口、从 `/v1/models` 识别模型名，生成并检查 endpoint，并提供 Slurm 日志及就地取消作业。
- vLLM 和 XX保活均支持可维护的一键提交预设；vLLM 启动后自动轮询新作业，XX保活的活跃作业可直接 Tail 日志。
- SQLite 操作审计和基础环境诊断。

## 安装和启动

建议在能够执行 `sinfo`、`squeue`、`sbatch`、`srun` 的开发机上创建独立虚拟环境：

```bash
cd /path/to/slurm_manager
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp config.example.json config.json
export SLURM_MANAGER_PASSWORD='请设置一个强密码'
bash run_slurm_manager.sh
```

如果未设置 `SLURM_MANAGER_PASSWORD`，默认登录密码为 `fghjfghj`。环境变量的优先级高于 `config.json` 中的 `default_password`，正式部署时建议始终使用环境变量覆盖默认值。登录后浏览器保存带签名且限时的登录凭证，默认 6 小时内刷新页面无需重新输入密码；密码变更后旧凭证立即失效。

进程名固定为 `aihpc-slurm-manager`，PID 保存在 `.slurm_manager/streamlit.pid`。停止脚本会先发送 `SIGTERM`，5 秒后仍未退出则自动停止其子进程并发送 `SIGKILL`；僵尸进程会被视为已停止并清理 PID 文件。查看和停止服务：

```bash
pgrep -af '[a]ihpc-slurm-manager'
bash stop_slurm_manager.sh
```

默认监听 `0.0.0.0:8501`。如果办公网能够访问开发机端口，可在办公电脑打开：

```text
http://<开发机IP或主机名>:8501
```

浏览器中的 `127.0.0.1` 始终指办公电脑自身，不能用于直接访问开发机。如果公司网络未开放 8501，推荐使用 SSH 端口转发：

```bash
ssh -L 8501:127.0.0.1:8501 user@development-host
```

然后浏览器访问 `http://127.0.0.1:8501`。即使使用默认密码，也不建议把具备 `sbatch/scancel/kill` 权限的页面暴露到不可信网络。

## 配置

复制 `config.example.json` 为 `config.json` 后重点检查：

- `script_roots`：允许提交的 Slurm 脚本目录。
- `browse_roots`：日志和结果浏览白名单。
- `editable_config_roots`：允许 endpoint 更新的 YAML 配置根目录。
- `default_partitions`：常用 partition。
- `auth_session_hours`：浏览器登录保持时间，默认 6 小时。

`${PROJECT_ROOT}`、`${USER}` 和其他环境变量会自动展开。vLLM token 不写进配置文件；如接口需要认证，设置：

```bash
export VLLM_API_TOKEN='...'
```

如果直接使用服务器上现有的 Data-Juicer 脚本目录，不需要创建软链接，建议在 `config.json` 中直接配置：

```json
{
  "script_roots": [
    "/data/hpc/home/zhiliang.jin/test_datajuicer/sourcecode/data-juicer/bash/v3"
  ]
}
```

平台会用 `sbatch --chdir=<脚本目录>` 提交，因此相对日志目录会自然落到该目录的 `log/` 下。现有 `browse_roots` 已包含 `/data/hpc/home/${USER}`，所以这些日志也可直接 Tail。软链接目标位于脚本白名单之外时会触发路径安全校验，因此直接配置真实目录更简单可靠。

## Endpoint 配置安全更新

在 vLLM 页面选择允许根目录下某个目录中的 `.yaml` / `.yml` 文件。平台用正则查找包含 `/v1/chat/completions` 或 `/v1/completions` 的字段，显示字段名、行号、当前内容和替换预览；只有用户确认后才写入，并先创建同目录带时间戳的 `.bak.*`。

## 验证

核心解析和安全边界测试不依赖真实 Slurm：

```bash
python3 -m unittest discover -s tests -v
python3 -m py_compile app.py slurm_manager/*.py
```

实时指标、提交、取消、Ray PTY 和 vLLM endpoint 仍需在公司的开发机与计算集群环境中做联调。

## 建议的下一阶段

- 提交前体检：校验节点状态、镜像/模型/挂载路径、空闲 GPU、输出目录权限及端口冲突。
- 作业复制与失败重跑：从历史 Job 恢复原脚本和覆盖参数，并支持 Slurm dependency/array。
- 配额与优先级视图：整合 `sshare`、`sprio`、QoS、账户配额和预计排队原因。
- 通知：Job 启动、完成、失败、节点掉卡或 vLLM endpoint 异常时发送消息。
- 产物质量检查：JSONL schema、坏行、重复率、字段分布、输入输出条数和目录体积对比。
- 安全入口替换：将当前计算节点连接通道逐步替换为带身份认证、命令审计和最小权限的 agent。
- 垃圾清理：识别已经结束 Job 遗留的 overlay、spill、cache、实例和日志，确认后回收。
