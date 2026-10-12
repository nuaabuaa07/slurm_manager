from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional


class StateStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.path), timeout=10)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_db(self) -> None:
        with self._connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    action TEXT NOT NULL,
                    target TEXT NOT NULL,
                    status TEXT NOT NULL,
                    detail TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS bookmarks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    name TEXT NOT NULL,
                    path TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS ray_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    slurm_job_id TEXT NOT NULL,
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    command TEXT NOT NULL,
                    log_path TEXT NOT NULL,
                    remote_pid TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'SUBMITTED'
                );
                CREATE TABLE IF NOT EXISTS presets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    kind TEXT NOT NULL,
                    name TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    UNIQUE(kind, name)
                );
                CREATE TABLE IF NOT EXISTS compute_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    name TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    slurm_job_id TEXT NOT NULL,
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL DEFAULT 0,
                    command TEXT NOT NULL,
                    pid TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'SUBMITTED'
                );
                CREATE TABLE IF NOT EXISTS task_resources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    task_id INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    label TEXT NOT NULL,
                    path TEXT NOT NULL,
                    FOREIGN KEY(task_id) REFERENCES compute_tasks(id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS quick_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    submitted_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                    slurm_job_id TEXT NOT NULL UNIQUE,
                    job_type TEXT NOT NULL,
                    name TEXT NOT NULL,
                    state TEXT NOT NULL,
                    partition TEXT NOT NULL DEFAULT '',
                    node_count INTEGER NOT NULL DEFAULT 0,
                    nodelist TEXT NOT NULL DEFAULT '',
                    script_path TEXT NOT NULL DEFAULT '',
                    payload TEXT NOT NULL DEFAULT '{}',
                    source TEXT NOT NULL DEFAULT 'cluster',
                    submit_method TEXT NOT NULL DEFAULT 'Terminal',
                    submit_command TEXT NOT NULL DEFAULT '',
                    submit_cwd TEXT NOT NULL DEFAULT '',
                    command_origin TEXT NOT NULL DEFAULT '',
                    command_user_edited INTEGER NOT NULL DEFAULT 0,
                    capture_status TEXT NOT NULL DEFAULT 'missing',
                    captured_at TEXT NOT NULL DEFAULT '',
                    script_snapshot TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS quick_job_command_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    quick_job_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                    submit_command TEXT NOT NULL,
                    submit_cwd TEXT NOT NULL DEFAULT '',
                    command_origin TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY(quick_job_id) REFERENCES quick_jobs(id) ON DELETE CASCADE
                );
                """
            )
            quick_job_columns = {
                str(row["name"]) for row in db.execute("PRAGMA table_info(quick_jobs)").fetchall()
            }
            if "submitted_at" not in quick_job_columns:
                db.execute("ALTER TABLE quick_jobs ADD COLUMN submitted_at TEXT")
            quick_job_additions = {
                "submit_method": "TEXT NOT NULL DEFAULT 'Terminal'",
                "submit_command": "TEXT NOT NULL DEFAULT ''",
                "submit_cwd": "TEXT NOT NULL DEFAULT ''",
                "command_origin": "TEXT NOT NULL DEFAULT ''",
                "command_user_edited": "INTEGER NOT NULL DEFAULT 0",
                "capture_status": "TEXT NOT NULL DEFAULT 'missing'",
                "captured_at": "TEXT NOT NULL DEFAULT ''",
                "script_snapshot": "TEXT NOT NULL DEFAULT ''",
            }
            for column, declaration in quick_job_additions.items():
                if column not in quick_job_columns:
                    db.execute(f"ALTER TABLE quick_jobs ADD COLUMN {column} {declaration}")
            db.execute(
                """UPDATE quick_jobs SET submit_method='WebUI'
                   WHERE source IN ('quick','webui') AND submit_method='Terminal'"""
            )
            db.execute(
                """INSERT INTO compute_tasks(name,task_type,slurm_job_id,host,port,command,pid,status)
                   SELECT 'legacy-ray-' || r.id,'ray',r.slurm_job_id,r.host,r.port,r.command,r.remote_pid,r.status
                   FROM ray_tasks r
                   WHERE NOT EXISTS (
                     SELECT 1 FROM compute_tasks c
                     WHERE c.task_type='ray' AND c.host=r.host AND c.pid=r.remote_pid AND c.command=r.command
                   )"""
            )

    def record_action(self, action: str, target: str, status: str, detail: str = "") -> None:
        with self._connect() as db:
            db.execute(
                "INSERT INTO actions(action,target,status,detail) VALUES(?,?,?,?)",
                (action, target, status, detail[:10000]),
            )

    def recent_actions(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute("SELECT * FROM actions ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(row) for row in rows]

    def add_bookmark(self, name: str, path: str) -> None:
        with self._connect() as db:
            db.execute(
                "INSERT INTO bookmarks(name,path) VALUES(?,?) "
                "ON CONFLICT(path) DO UPDATE SET name=excluded.name",
                (name, path),
            )

    def delete_bookmark(self, bookmark_id: int) -> None:
        with self._connect() as db:
            db.execute("DELETE FROM bookmarks WHERE id=?", (bookmark_id,))

    def bookmarks(self) -> List[Dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute("SELECT * FROM bookmarks ORDER BY name,path").fetchall()
        return [dict(row) for row in rows]

    def add_ray_task(
        self,
        slurm_job_id: str,
        host: str,
        port: int,
        command: str,
        log_path: str,
        remote_pid: str = "",
    ) -> int:
        with self._connect() as db:
            cursor = db.execute(
                """INSERT INTO ray_tasks(slurm_job_id,host,port,command,log_path,remote_pid)
                   VALUES(?,?,?,?,?,?)""",
                (slurm_job_id, host, port, command, log_path, remote_pid),
            )
            return int(cursor.lastrowid)

    def ray_tasks(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute("SELECT * FROM ray_tasks ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(row) for row in rows]

    def update_ray_task(self, task_id: int, status: str) -> None:
        with self._connect() as db:
            db.execute("UPDATE ray_tasks SET status=? WHERE id=?", (status, task_id))

    def presets(self, kind: str) -> List[Dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute("SELECT * FROM presets WHERE kind=? ORDER BY name", (kind,)).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            try:
                item["payload"] = json.loads(item["payload"])
            except (TypeError, json.JSONDecodeError):
                item["payload"] = {}
            result.append(item)
        return result

    def save_preset(self, kind: str, name: str, payload: Dict[str, Any], preset_id: Optional[int] = None) -> int:
        encoded = json.dumps(payload, ensure_ascii=False)
        with self._connect() as db:
            if preset_id is not None:
                db.execute(
                    "UPDATE presets SET kind=?,name=?,payload=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (kind, name, encoded, preset_id),
                )
                return preset_id
            cursor = db.execute(
                """INSERT INTO presets(kind,name,payload) VALUES(?,?,?)
                   ON CONFLICT(kind,name) DO UPDATE SET payload=excluded.payload,updated_at=CURRENT_TIMESTAMP""",
                (kind, name, encoded),
            )
            row = db.execute("SELECT id FROM presets WHERE kind=? AND name=?", (kind, name)).fetchone()
            return int(row["id"])

    def delete_preset(self, preset_id: int) -> None:
        with self._connect() as db:
            db.execute("DELETE FROM presets WHERE id=?", (preset_id,))

    def add_compute_task(
        self,
        name: str,
        task_type: str,
        slurm_job_id: str,
        host: str,
        port: int,
        command: str,
        pid: str,
        status: str = "SUBMITTED",
    ) -> int:
        with self._connect() as db:
            cursor = db.execute(
                """INSERT INTO compute_tasks(name,task_type,slurm_job_id,host,port,command,pid,status)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (name, task_type, slurm_job_id, host, port, command, pid, status),
            )
            return int(cursor.lastrowid)

    def compute_tasks(self, limit: int = 200) -> List[Dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute("SELECT * FROM compute_tasks ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(row) for row in rows]

    def compute_task(self, task_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as db:
            row = db.execute("SELECT * FROM compute_tasks WHERE id=?", (task_id,)).fetchone()
        return dict(row) if row else None

    def update_compute_task(self, task_id: int, status: str) -> None:
        with self._connect() as db:
            db.execute(
                "UPDATE compute_tasks SET status=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (status, task_id),
            )

    def delete_compute_task(self, task_id: int) -> None:
        with self._connect() as db:
            db.execute("DELETE FROM task_resources WHERE task_id=?", (task_id,))
            db.execute("DELETE FROM compute_tasks WHERE id=?", (task_id,))

    def task_resources(self, task_id: int) -> List[Dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute(
                "SELECT * FROM task_resources WHERE task_id=? ORDER BY kind,label,id", (task_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def add_task_resource(self, task_id: int, kind: str, label: str, path: str) -> int:
        with self._connect() as db:
            cursor = db.execute(
                "INSERT INTO task_resources(task_id,kind,label,path) VALUES(?,?,?,?)",
                (task_id, kind, label, path),
            )
            return int(cursor.lastrowid)

    def delete_task_resource(self, resource_id: int) -> None:
        with self._connect() as db:
            db.execute("DELETE FROM task_resources WHERE id=?", (resource_id,))

    def setting(self, key: str, default: str = "") -> str:
        with self._connect() as db:
            row = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return str(row["value"]) if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self._connect() as db:
            db.execute(
                """INSERT INTO settings(key,value) VALUES(?,?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=CURRENT_TIMESTAMP""",
                (key, value),
            )

    def quick_jobs(self, limit: int = 200) -> List[Dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute("SELECT * FROM quick_jobs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        result: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            try:
                item["payload"] = json.loads(item["payload"])
            except (TypeError, json.JSONDecodeError):
                item["payload"] = {}
            result.append(item)
        return result

    def quick_job(self, record_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as db:
            row = db.execute("SELECT * FROM quick_jobs WHERE id=?", (record_id,)).fetchone()
        if not row:
            return None
        item = dict(row)
        try:
            item["payload"] = json.loads(item["payload"])
        except (TypeError, json.JSONDecodeError):
            item["payload"] = {}
        return item

    def save_quick_job(
        self,
        slurm_job_id: str,
        job_type: str,
        name: str,
        state: str,
        partition: str,
        node_count: int,
        nodelist: str,
        script_path: str,
        payload: Dict[str, Any],
        source: str = "cluster",
        record_id: Optional[int] = None,
        submitted_at: str = "",
    ) -> int:
        encoded = json.dumps(payload, ensure_ascii=False)
        with self._connect() as db:
            if record_id is not None:
                db.execute(
                    """UPDATE quick_jobs SET slurm_job_id=?,job_type=?,name=?,state=?,partition=?,
                       node_count=?,nodelist=?,script_path=?,payload=?,source=?,
                       submitted_at=COALESCE(?,datetime('now','localtime')),updated_at=CURRENT_TIMESTAMP
                       WHERE id=?""",
                    (slurm_job_id, job_type, name, state, partition, node_count, nodelist,
                     script_path, encoded, source, submitted_at or None, record_id),
                )
                return record_id
            db.execute(
                    """INSERT INTO quick_jobs(
                       slurm_job_id,job_type,name,state,partition,node_count,nodelist,script_path,payload,source,submitted_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,COALESCE(?,datetime('now','localtime')))
                   ON CONFLICT(slurm_job_id) DO UPDATE SET
                       job_type=excluded.job_type,name=excluded.name,state=excluded.state,
                       partition=excluded.partition,node_count=excluded.node_count,nodelist=excluded.nodelist,
                       script_path=CASE WHEN excluded.script_path='' THEN quick_jobs.script_path ELSE excluded.script_path END,
                       payload=CASE WHEN excluded.payload='{}' THEN quick_jobs.payload ELSE excluded.payload END,
                       submitted_at=CASE WHEN quick_jobs.submitted_at IS NULL OR quick_jobs.submitted_at=''
                                         THEN excluded.submitted_at ELSE quick_jobs.submitted_at END,
                       updated_at=CURRENT_TIMESTAMP""",
                (slurm_job_id, job_type, name, state, partition, node_count, nodelist,
                 script_path, encoded, source, submitted_at or None),
            )
            row = db.execute("SELECT id FROM quick_jobs WHERE slurm_job_id=?", (slurm_job_id,)).fetchone()
            return int(row["id"])

    def update_quick_job_state(self, record_id: int, state: str) -> None:
        with self._connect() as db:
            db.execute(
                "UPDATE quick_jobs SET state=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (state, record_id),
            )

    def save_quick_job_command(
        self,
        record_id: int,
        command: str,
        cwd: str,
        origin: str,
        user_edited: bool,
        capture_status: str,
        script_snapshot: str = "",
        submit_method: str = "",
    ) -> None:
        with self._connect() as db:
            current = db.execute(
                "SELECT submit_command,submit_cwd,command_origin FROM quick_jobs WHERE id=?",
                (record_id,),
            ).fetchone()
            if current and (current["submit_command"] or current["submit_cwd"]):
                db.execute(
                    """INSERT INTO quick_job_command_history(
                           quick_job_id,submit_command,submit_cwd,command_origin
                       ) VALUES(?,?,?,?)""",
                    (record_id, current["submit_command"], current["submit_cwd"], current["command_origin"]),
                )
            db.execute(
                """UPDATE quick_jobs SET submit_command=?,submit_cwd=?,command_origin=?,
                   command_user_edited=?,capture_status=?,captured_at=datetime('now','localtime'),
                   script_snapshot=CASE WHEN ?='' THEN script_snapshot ELSE ? END,
                   submit_method=CASE WHEN ?='' THEN submit_method ELSE ? END,
                   updated_at=CURRENT_TIMESTAMP WHERE id=?""",
                (command, cwd, origin, 1 if user_edited else 0, capture_status,
                 script_snapshot, script_snapshot, submit_method, submit_method, record_id),
            )

    def quick_job_command_history(self, record_id: int, limit: int = 20) -> List[Dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute(
                """SELECT * FROM quick_job_command_history
                   WHERE quick_job_id=? ORDER BY id DESC LIMIT ?""",
                (record_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def delete_quick_job(self, record_id: int) -> None:
        with self._connect() as db:
            db.execute("DELETE FROM quick_job_command_history WHERE quick_job_id=?", (record_id,))
            db.execute("DELETE FROM quick_jobs WHERE id=?", (record_id,))
