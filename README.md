# AIHPC Slurm Manager

一个基于 Streamlit 的 Slurm 集群管理界面，面向“开发机 → Slurm → 计算节点”的工作流。在浏览器中查看集群资源、提交和管理作业、运行计算节点任务，并跟踪 vLLM 服务与日志。

> 本项目包含针对特定集群的默认路径、partition 和节点连接命令。首次使用前，请根据自己的环境调整配置。应用以启动它的系统用户权限执行命令，请勿将其暴露到不可信网络。

## 功能

- **集群资源**：按 partition 查看节点、CPU 和 GPU 分配，采样节点指标；保存采样配置并从结果快捷提交作业。
- **Slurm 作业**：扫描脚本目录、维护提交预设、选择节点并提交；查看作业状态、节点和日志，取消或重新启动作业。
- **计算节点任务**：通过计算节点连接通道运行 Ray 或一般任务，维护任务预设，跟踪多个日志或结果文件，并按进程组停止任务。
- **vLLM**：从模板提交作业，识别模型和端口、生成并检查 Endpoint；支持预览并更新指定范围内的 YAML 配置。
- **保活与诊断**：维护保活作业预设；vLLM 探针可按 60 秒间隔扫描 GPU 利用率，连续达标后依次探测接口并启动压力请求，也可手动指定节点和接口。查看历史、审计记录、环境诊断及数据库备份状态。压力目标利用率通过请求速率渐进调节，受服务响应和采样可用性影响。

## 环境要求

- 一台能够访问 Slurm 集群的开发机，且可运行 `sinfo`、`squeue`、`sbatch` 和 `srun`。
- Python 3 和 `venv`；Python 依赖见 [requirements.txt](requirements.txt)。
- 计算节点任务、Ray、实时指标和 vLLM 功能还需要对应的集群服务、脚本和访问权限。

## 快速开始

在开发机上克隆仓库并安装依赖：

```bash
git clone https://github.com/nuaabuaa07/slurm_manager.git
cd slurm_manager
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp config.example.yaml config.yaml
```

编辑 `config.yaml`，至少检查脚本目录、partition、浏览目录和计算节点连接命令（参见下方配置说明），然后设置密码并启动：

```bash
export SLURM_MANAGER_PASSWORD='替换为强密码'
bash run_slurm_manager.sh
```

默认监听 `0.0.0.0:8501`。如需仅允许本机连接，可在启动前设置：

```bash
export SLURM_MANAGER_HOST=127.0.0.1
export SLURM_MANAGER_PORT=8501
```

从本地电脑安全访问远程开发机时，可使用 SSH 端口转发：

```bash
ssh -L 8501:127.0.0.1:8501 user@development-host
```

然后打开 `http://127.0.0.1:8501`。停止服务：

```bash
bash stop_slurm_manager.sh
```

启动脚本在 `.slurm_manager/streamlit.pid` 保存进程 ID。停止脚本先发送 `SIGTERM`；等待后仍未退出时会停止子进程并发送 `SIGKILL`。

## 配置

默认先读取 `config.json`，再由 `config.yaml` 覆盖同名设置。也可以用环境变量 `SLURM_MANAGER_CONFIG` 指定单独的 JSON 或 YAML 文件。配置值支持展开 `${PROJECT_ROOT}`、`${USER}` 等环境变量。示例：

```yaml
script_roots:
  - /path/to/slurm/scripts
restart_script_roots:
  - /path/to/slurm/scripts
browse_roots:
  - /path/to/logs
editable_config_roots:
  - /path/to/configs
default_partitions:
  - my-partition
node_connect_command: python /path/to/connect.py
```

| 配置项 | 用途 |
| --- | --- |
| `script_roots` | 可提交的 Slurm 脚本目录 |
| `restart_script_roots` | 再次启动时允许引用的外部脚本目录 |
| `browse_roots` | 日志和结果浏览的允许目录 |
| `editable_config_roots` | 允许更新 Endpoint 的 YAML 配置目录 |
| `default_partitions` | 常用 partition 列表 |
| `node_connect_command` | 复制节点连接命令时使用的前缀 |
| `job_snapshot_dir` | 作业脚本快照目录 |
| `auth_session_hours` | 浏览器登录有效时间，默认 6 小时 |
| `local_state_root` | 本地活跃 SQLite 数据目录的父目录，默认 `/tmp` |
| `state_db` | 持久数据库快照，默认项目下 `.slurm_manager/state.db` |

默认路径和 partition 不一定适合你的集群；特别是默认节点连接脚本可能不存在，需要替换为实际可用的命令。提交脚本时会将 `sbatch --chdir` 指向脚本所在目录，以便解析相对路径。

如 vLLM Endpoint 需要认证，在启动应用前设置环境变量，避免将 token 写入配置文件：

```bash
export VLLM_API_TOKEN='替换为你的 token'
```

## 安全与数据

应用能以启动用户的权限提交、取消作业和停止进程。**务必设置 `SLURM_MANAGER_PASSWORD`**；未设置时程序使用内置默认密码。密码环境变量优先于配置文件中的 `default_password`。推荐仅监听本机并通过 SSH 隧道访问。

活跃 SQLite 数据库保存在本地目录，运行期间约每 60 秒备份到持久快照，正常退出时也会尝试备份。异常退出可能丢失最近一次备份后的变更；启动时保留的旧本地数据库目录可供人工恢复。`/tmp` 可能被系统清理，不应作为唯一持久副本。可在“历史与诊断”页面查看当前路径和备份状态。

vLLM 页面更新 YAML 前会显示替换预览，并在确认写入前创建带时间戳的备份文件。

## 测试

核心解析和安全边界测试无需真实 Slurm 集群：

```bash
python3 -m unittest discover -s tests -v
python3 -m py_compile app.py slurm_manager/*.py
```

实时指标、作业提交与取消、计算节点连接和 vLLM Endpoint 需要在实际环境中联调。

## 许可证

[Apache License 2.0](LICENSE)
