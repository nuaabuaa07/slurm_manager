"""Keep the active SQLite database on local storage and mirror it to NFS."""

from __future__ import annotations

import atexit
import fcntl
import hashlib
import logging
import os
import sqlite3
import tempfile
import threading
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from .storage import StateStore


LOGGER = logging.getLogger(__name__)
_MIRROR_LOCK = threading.Lock()
_MIRRORS: dict[tuple[str, str, str], "LocalStateMirror"] = {}


class LocalStateMirror:
    def __init__(
        self, snapshot_path: Path, local_root: Path, identity: str,
        interval_seconds: int = 60,
    ) -> None:
        if not local_root.expanduser().is_absolute() or not snapshot_path.expanduser().is_absolute():
            raise ValueError("数据库目录和快照路径必须是绝对路径")
        self.snapshot_path = snapshot_path.expanduser().resolve(strict=False)
        self.local_root = local_root.expanduser().resolve(strict=False)
        self.local_root.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:12]
        name = f"aihpc-slurm-manager-{os.getuid()}-{digest}"
        self.local_dir = self.local_root / name
        self.local_path = self.local_dir / "state.db"
        if self.snapshot_path == self.local_path:
            raise ValueError("本地数据库与持久快照不能是同一个文件")
        self._lock_handle = (self.local_root / f".{name}.lock").open("a+b")
        try:
            fcntl.flock(self._lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._lock_handle.close()
            raise RuntimeError(f"本地数据库目录正在被另一个服务进程使用：{self.local_dir}") from exc

        self._stop = threading.Event()
        self._backup_lock = threading.Lock()
        self._closed = False
        self.last_backup_at = ""
        self.last_backup_error = ""
        self.interval_seconds = interval_seconds
        try:
            self._restore()
            self.store = StateStore(self.local_path)
            self.backup()  # Persist any initial schema migration immediately.
            self._thread = threading.Thread(target=self._run, name="slurm-manager-db-snapshot", daemon=True)
            self._thread.start()
            atexit.register(self.close)
        except BaseException:
            fcntl.flock(self._lock_handle, fcntl.LOCK_UN)
            self._lock_handle.close()
            raise

    def _restore(self) -> None:
        if self.local_dir.exists():
            if not self.local_dir.is_dir() or self.local_dir.is_symlink():
                raise RuntimeError(f"本地数据库目录不是普通目录：{self.local_dir}")
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
            archived = self.local_root / f"{self.local_dir.name}.{timestamp}"
            self.local_dir.rename(archived)
            LOGGER.info("已保留上次的本地数据库：%s", archived)
        self.local_dir.mkdir(mode=0o700)
        if self.snapshot_path.exists():
            # SQLite backup also includes committed changes in a legacy WAL file.
            with closing(sqlite3.connect(str(self.snapshot_path))) as source:
                with closing(sqlite3.connect(str(self.local_path))) as target:
                    source.backup(target)
            LOGGER.info("已从持久快照恢复数据库：%s -> %s", self.snapshot_path, self.local_path)

    def backup(self) -> None:
        with self._backup_lock:
            self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{self.snapshot_path.name}.", suffix=".tmp", dir=self.snapshot_path.parent
            )
            os.close(descriptor)
            temporary = Path(temporary_name)
            try:
                with closing(sqlite3.connect(str(self.local_path), timeout=30)) as source:
                    with closing(sqlite3.connect(str(temporary), timeout=30)) as target:
                        source.backup(target)
                os.replace(temporary, self.snapshot_path)
                self.last_backup_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                self.last_backup_error = ""
            finally:
                temporary.unlink(missing_ok=True)

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                self.backup()
            except Exception as exc:
                self.last_backup_error = str(exc)
                LOGGER.exception("SQLite 持久快照备份失败，下次周期会重试")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        self._thread.join(timeout=5)
        try:
            self.backup()
        except Exception as exc:
            self.last_backup_error = str(exc)
            LOGGER.exception("关闭时 SQLite 持久快照备份失败")
        finally:
            fcntl.flock(self._lock_handle, fcntl.LOCK_UN)
            self._lock_handle.close()


def get_local_state_mirror(
    snapshot_path: Path, local_root: Path, identity: str,
) -> LocalStateMirror:
    """Reuse the mirror when Streamlit recompiles its cached services."""
    key = (str(snapshot_path.expanduser().resolve()), str(local_root.expanduser().resolve()), identity)
    with _MIRROR_LOCK:
        mirror = _MIRRORS.get(key)
        if mirror is None or mirror._closed:
            mirror = LocalStateMirror(snapshot_path, local_root, identity)
            _MIRRORS[key] = mirror
        return mirror
