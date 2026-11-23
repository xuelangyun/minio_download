# -*- coding: utf-8 -*-
"""配置库：所有配置（MinIO / PostgreSQL 连接、运行参数、任务、目标磁盘、Web 口令）都保存在 SQLite 里。

- 单独一个文件（默认 minio_downloader_config.sqlite3），与“下载状态库”分开：
  重置下载状态不会丢配置，重置配置也不会影响下载记录。
- 下载脚本每次启动时读取一次快照；Web 控制台读写同一个文件。改动在**下一次启动下载时**生效。
- 敏感项（Secret Key、PG 密码）以明文存放在这个文件里，POSIX 上文件权限为 0600；
  Web 接口永远不会把它们读回浏览器，只会告诉前端“已设置”。
- Web 口令只保存 PBKDF2 哈希。
"""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import json
import copy
import os
import re
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

SCHEMA_VERSION = 2          # 2：任务增加 tags 列；新增 plans（任务方案）表


# ================================ 设置项定义 ================================
@dataclass(frozen=True)
class Setting:
    key: str
    default: Any
    kind: str                    # str / int / float / bool
    group: str
    label: str
    help: str = ""
    secret: bool = False
    minimum: float | None = None
    maximum: float | None = None
    placeholder: str = ""


GROUPS = {
    "minio": "MinIO 连接",
    "pg": "PostgreSQL（任务元数据）",
    "run": "运行与空间",
    "transfer": "传输与校验",
    "web": "Web 控制台",
}

SETTINGS: list[Setting] = [
    Setting("minio.endpoint", "", "str", "minio", "服务地址", "例如 http://192.168.1.10:9000 ；不写 http(s):// 时由下面的“使用 HTTPS”决定", placeholder="http://127.0.0.1:9000"),
    Setting("minio.access_key", "", "str", "minio", "Access Key"),
    Setting("minio.secret_key", "", "str", "minio", "Secret Key", "保存后不会再显示；留空表示不修改", secret=True),
    Setting("minio.secure", True, "bool", "minio", "使用 HTTPS", "仅当服务地址没有写 http:// 或 https:// 时生效"),
    Setting("minio.cert_check", True, "bool", "minio", "校验证书", "内网自签证书可关闭（不建议用于公网）"),
    Setting("minio.region", "", "str", "minio", "区域", "可选，一般留空"),
    Setting("minio.bucket", "rocp-data", "str", "minio", "Bucket"),

    Setting("pg.host", "127.0.0.1", "str", "pg", "主机"),
    Setting("pg.port", 5432, "int", "pg", "端口", minimum=1, maximum=65535),
    Setting("pg.database", "minio_event", "str", "pg", "数据库"),
    Setting("pg.user", "postgres", "str", "pg", "用户名"),
    Setting("pg.password", "", "str", "pg", "密码", "保存后不会再显示；留空表示不修改", secret=True),

    Setting("run.min_free_gb", 3.0, "float", "run", "每块盘保留空间（GiB）", "下载后至少还要剩这么多", minimum=0, maximum=1_000_000),
    Setting("run.space_safety_factor", 1.15, "float", "run", "空间安全系数", "预计占用 = 数据库记录的大小 × 该系数", minimum=1.0, maximum=5.0),
    Setting("run.max_writes_per_disk", 2, "int", "run", "每块盘并发写入数", "同一任务内同时下载的 episode 数", minimum=1, maximum=16),
    Setting("run.require_mcap", True, "bool", "run", "要求每条数据含 mcap", "缺 mcap 的数据会被跳过并记录"),
    Setting("run.include_null_quality", False, "bool", "run", "包含质量状态为空的数据", "数据库里质量状态为空（NULL）的数据默认不下载（与原脚本行为一致）；开启后把它们当作可下载"),
    Setting("run.mcap_pattern", "*.SYNC.mcap", "str", "run", "mcap 文件名规则", "fnmatch 通配符，区分大小写"),
    Setting("run.validate_workers", 16, "int", "run", "检查 MinIO 目录的并发数", minimum=1, maximum=64),
    Setting("run.default_rounds", 1, "int", "run", "默认补齐轮数", "数据缺文件被跳过时，最多改选后面的数据补几轮", minimum=1, maximum=50),

    Setting("transfer.file_workers", 4, "int", "transfer", "单条数据内并行文件数", minimum=1, maximum=16),
    Setting("transfer.file_retries", 5, "int", "transfer", "单个文件最大尝试次数", "网络中断会从断点续传", minimum=1, maximum=20),
    Setting("transfer.retry_delay", 1.0, "float", "transfer", "重试间隔基数（秒）", "实际间隔 = 基数 × 2^次数，最多 30 秒", minimum=0, maximum=60),
    Setting("transfer.connect_timeout", 10, "int", "transfer", "连接超时（秒）", minimum=1, maximum=300),
    Setting("transfer.read_timeout", 120, "int", "transfer", "读取超时（秒）", minimum=5, maximum=3600),
    Setting("transfer.fsync", True, "bool", "transfer", "改名前 fsync", "关闭可提速，但断电后不再保证数据已落盘（进程被 kill 仍然安全）"),
    Setting("transfer.verify_md5", True, "bool", "transfer", "校验 MD5", "ETag 是普通 MD5 时校验内容；启用了服务端加密的对象请关闭"),

    Setting("web.host", "127.0.0.1", "str", "web", "监听地址", "改为 0.0.0.0 对外开放前必须先设置访问口令；重启 Web 后生效"),
    Setting("web.port", 8765, "int", "web", "监听端口", "重启 Web 后生效", minimum=1, maximum=65535),
]
BY_KEY = {s.key: s for s in SETTINGS}
DATA_VERSION_KEY = "tasks.data_version"
TOKEN_KEY = "web.token_hash"
_INTERNAL_KEYS = {DATA_VERSION_KEY, TOKEN_KEY}


def coerce(setting: Setting, value: Any) -> Any:
    """把前端 / 环境变量传来的值转成设置项的类型并校验范围；不合法抛 ValueError（中文原因）。"""
    kind, label = setting.kind, setting.label
    if kind == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str) and value.strip().lower() in ("1", "true", "yes", "on", "0", "false", "no", "off"):
            return value.strip().lower() in ("1", "true", "yes", "on")
        raise ValueError(f"{label}：需要 开 / 关")
    if kind in ("int", "float"):
        try:
            number = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"{label}：需要一个数字") from None
        if kind == "int":
            if number != int(number):
                raise ValueError(f"{label}：需要整数")
            number = int(number)
        if setting.minimum is not None and number < setting.minimum:
            raise ValueError(f"{label}：不能小于 {setting.minimum:g}")
        if setting.maximum is not None and number > setting.maximum:
            raise ValueError(f"{label}：不能大于 {setting.maximum:g}")
        return number
    return "" if value is None else str(value).strip()


def schema() -> list[dict[str, Any]]:
    """给前端生成表单用：不含任何值。"""
    return [{"key": s.key, "kind": s.kind, "group": s.group, "label": s.label, "help": s.help,
             "secret": s.secret, "min": s.minimum, "max": s.maximum, "placeholder": s.placeholder,
             "default": None if s.secret else s.default} for s in SETTINGS]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ================================ 任务 JSON / 标签 / 方案（纯函数） ================================
MAX_TAGS, MAX_TAG_LEN, MAX_PLAN_NAME = 12, 24, 60


def normalize_tags(raw: Any) -> list[str]:
    """标签：列表或用逗号（, ， ; ；）分隔的字符串；去空、去重、保持顺序。"""
    if raw is None:
        return []
    if isinstance(raw, str):
        raw = re.split(r"[,，;；]", raw)
    out: list[str] = []
    for item in raw:
        tag = str(item).strip()
        if not tag:
            continue
        if len(tag) > MAX_TAG_LEN:
            raise ValueError(f"标签太长（最多 {MAX_TAG_LEN} 个字符）：{tag[:12]}…")
        if tag not in out:
            out.append(tag)
    if len(out) > MAX_TAGS:
        raise ValueError(f"一个任务最多 {MAX_TAGS} 个标签")
    return out


def tasks_to_dict(data_version: str, scene_tasks: list, tags: dict[str, list[str]] | None = None) -> dict[str, Any]:
    """(版本目录, SCENE_TASKS, 标签) → 前端 / 方案使用的 JSON 结构。"""
    tags = tags or {}
    return {"data_version": data_version, "scenes": [
        {"scene": scene, "tasks": [
            {"name": t[0], "count": t[1], "aliases": list(t[2]), "dir": (t[3] if len(t) > 3 else ""), "tags": list(tags.get(t[0], []))}
            for t in tasks]}
        for scene, tasks in scene_tasks]}


def tasks_from_dict(cfg: dict[str, Any]) -> tuple[str, list, dict[str, list[str]]]:
    """JSON 结构 → (版本目录, SCENE_TASKS, 标签)。结构或类型不对抛 ValueError / KeyError / TypeError。"""
    scene_tasks: list = []
    tags: dict[str, list[str]] = {}
    for s in cfg.get("scenes", []):
        tasks = []
        for t in s.get("tasks", []):
            name = str(t["name"]).strip()
            entry = (name, int(t["count"]), [str(a).strip() for a in t.get("aliases", []) if str(a).strip()])
            d = str(t.get("dir") or "").strip()
            tasks.append(entry + ((d,) if d else ()))
            tg = normalize_tags(t.get("tags"))
            if tg:
                tags[name] = tg
        scene_tasks.append((str(s["scene"]).strip(), tasks))
    return str(cfg.get("data_version") or "").strip(), scene_tasks, tags


def canonical_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """规范化一份任务 JSON（去多余字段、统一类型）。不合法抛 ValueError。"""
    if not isinstance(payload, dict) or not isinstance(payload.get("scenes"), list):
        raise ValueError("任务内容格式不对（需要 data_version 和 scenes）")
    try:
        return tasks_to_dict(*tasks_from_dict(payload))
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError(f"任务内容格式不对：{exc!r}") from exc


def plan_summary(payload: dict[str, Any]) -> dict[str, Any]:
    tasks = [t for s in payload.get("scenes", []) for t in s.get("tasks", [])]
    tags: list[str] = []
    for t in tasks:
        for tag in t.get("tags", []):
            if tag not in tags:
                tags.append(tag)
    return {"scene_count": len(payload.get("scenes", [])), "task_count": len(tasks),
            "total_target": sum(int(t.get("count", 0)) for t in tasks), "tags": tags}


def filter_by_tags(payload: dict[str, Any], tags: list[str]) -> dict[str, Any]:
    """只保留带有任一指定标签的任务；任务全被滤掉的场景一并去掉。tags 为空则原样返回。"""
    if not tags:
        return copy.deepcopy(payload)
    wanted = set(tags)
    scenes = []
    for s in payload.get("scenes", []):
        kept = [copy.deepcopy(t) for t in s.get("tasks", []) if wanted & set(t.get("tags", []))]
        if kept:
            scenes.append({"scene": s["scene"], "tasks": kept})
    return {**{k: v for k, v in payload.items() if k != "scenes"}, "scenes": scenes}


def merge_payloads(current: dict[str, Any], incoming: dict[str, Any],
                   validate: Any = None) -> tuple[dict[str, Any], dict[str, Any]]:
    """把 incoming 里“当前没有的任务”追加到 current（按任务名判断；场景不存在则新建）。已有的任务一律不动。

    validate(merged_payload) 在每次追加后调用，抛 ValueError 表示这个任务与现有配置冲突（别名 / 目录名等），
    该任务被跳过并记录原因，不影响其它任务。返回 (合并结果, {"added": [...], "skipped": [{"name","reason"}]})。
    """
    merged = copy.deepcopy(current)
    existing = {t["name"] for s in merged["scenes"] for t in s["tasks"]}
    added: list[str] = []
    skipped: list[dict[str, str]] = []
    for scene in incoming.get("scenes", []):
        for task in scene.get("tasks", []):
            if task["name"] in existing:
                skipped.append({"name": task["name"], "reason": "当前配置里已有同名任务"})
                continue
            target = next((s for s in merged["scenes"] if s["scene"] == scene["scene"]), None)
            created = target is None
            if created:
                target = {"scene": scene["scene"], "tasks": []}
                merged["scenes"].append(target)
            target["tasks"].append(copy.deepcopy(task))
            try:
                if validate:
                    validate(merged)
            except ValueError as exc:
                target["tasks"].pop()
                if created:
                    merged["scenes"].remove(target)
                skipped.append({"name": task["name"], "reason": str(exc.args[0]) if exc.args else str(exc)})
                continue
            existing.add(task["name"])
            added.append(task["name"])
    return merged, {"added": added, "skipped": skipped}


# ================================ 存储 ================================
SQL_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS scenes (
    id INTEGER PRIMARY KEY AUTOINCREMENT, position INTEGER NOT NULL, name TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scene_id INTEGER NOT NULL REFERENCES scenes(id) ON DELETE CASCADE,
    position INTEGER NOT NULL, name TEXT NOT NULL UNIQUE,
    target_count INTEGER NOT NULL CHECK (target_count >= 0),
    directory TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '[]');
CREATE TABLE IF NOT EXISTS task_aliases (
    task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    position INTEGER NOT NULL, alias TEXT NOT NULL, PRIMARY KEY (task_id, alias));
CREATE TABLE IF NOT EXISTS plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    note TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS disks (
    name TEXT PRIMARY KEY, position INTEGER NOT NULL, path TEXT NOT NULL, mountpoint TEXT NOT NULL);
"""


class ConfigStore:
    """每次操作使用短连接，所以 Web 的多个线程和下载脚本可以同时使用，互不影响。"""

    def __init__(self, path: str | os.PathLike):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existed = self.path.exists()
        with self._tx() as conn:
            conn.executescript(SQL_SCHEMA)
            if "tags" not in {r[1] for r in conn.execute("PRAGMA table_info(tasks)")}:     # 从 1 版配置库升级
                conn.execute("ALTER TABLE tasks ADD COLUMN tags TEXT NOT NULL DEFAULT '[]'")
            conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?)", (str(SCHEMA_VERSION),))
        if not existed and os.name != "nt":
            with contextlib.suppress(OSError):
                os.chmod(self.path, 0o600)      # 里面有密钥

    @contextlib.contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    @contextlib.contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        """只读事务：不占用写锁，WAL 模式下与写入互不阻塞；事务内多次查询看到的是同一个一致的快照。"""
        conn = sqlite3.connect(str(self.path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            conn.execute("BEGIN")
            yield conn
        finally:
            conn.close()

    # ---------------------------- 首次初始化 ----------------------------
    def is_seeded(self) -> bool:
        with self._read() as conn:
            return conn.execute("SELECT 1 FROM meta WHERE key='seeded_at'").fetchone() is not None

    def seed(self, data_version: str, scene_tasks: list, disks: list[dict[str, str]],
             settings: dict[str, Any]) -> bool:
        """只在从未初始化过时写入初始内容（来自代码默认值 / 环境变量 / 旧配置文件）。返回是否真的写入了。"""
        if self.is_seeded():
            return False
        self.set_tasks(data_version, scene_tasks)
        self.set_disks(disks)
        self.set_settings(settings, skip_invalid=True)
        with self._tx() as conn:
            conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES('seeded_at', ?)", (_now(),))
        return True

    # ---------------------------- 设置项 ----------------------------
    @staticmethod
    def _settings(conn: sqlite3.Connection) -> dict[str, Any]:
        values = {s.key: s.default for s in SETTINGS}
        for row in conn.execute("SELECT key, value FROM settings"):
            if row["key"] in BY_KEY:
                try:
                    values[row["key"]] = coerce(BY_KEY[row["key"]], json.loads(row["value"]))
                except (ValueError, TypeError):
                    pass
        return values

    def get_settings(self) -> dict[str, Any]:
        """全部设置项（含密钥），缺失的用默认值补齐。仅供下载脚本 / 后端内部使用。"""
        with self._read() as conn:
            return self._settings(conn)

    def set_settings(self, updates: dict[str, Any], skip_invalid: bool = False) -> list[str]:
        """批量保存。密钥项传 None 表示“不修改”。返回实际写入的键。不合法的值：抛 ValueError（或 skip_invalid 时忽略）。"""
        clean: dict[str, Any] = {}
        errors: list[str] = []
        for key, raw in updates.items():
            setting = BY_KEY.get(key)
            if setting is None:
                if skip_invalid:
                    continue
                errors.append(f"未知设置项：{key}")
                continue
            if setting.secret and raw is None:
                continue
            try:
                clean[key] = coerce(setting, raw)
            except ValueError as exc:
                if not skip_invalid:
                    errors.append(str(exc))
        if errors:
            raise ValueError("；".join(errors))
        with self._tx() as conn:
            for key, value in clean.items():
                conn.execute(
                    "INSERT INTO settings(key, value, updated_at) VALUES(?,?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                    (key, json.dumps(value, ensure_ascii=False), _now()))
        return list(clean)

    def public_settings(self) -> tuple[dict[str, Any], list[str]]:
        """给浏览器看的版本：密钥项一律返回空串，另外给出“哪些密钥已设置”。"""
        values = self.get_settings()
        secrets_set = [s.key for s in SETTINGS if s.secret and values.get(s.key)]
        for s in SETTINGS:
            if s.secret:
                values[s.key] = ""
        return values, secrets_set

    # ---------------------------- 任务 ----------------------------
    @staticmethod
    def _tasks(conn: sqlite3.Connection) -> tuple[str, list]:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (DATA_VERSION_KEY,)).fetchone()
        data_version = json.loads(row["value"]) if row else ""
        aliases: dict[int, list[str]] = {}
        for a in conn.execute("SELECT task_id, alias FROM task_aliases ORDER BY task_id, position"):
            aliases.setdefault(a["task_id"], []).append(a["alias"])
        tasks_by_scene: dict[int, list] = {}
        for t in conn.execute("SELECT id, scene_id, name, target_count, directory FROM tasks ORDER BY scene_id, position"):
            entry: tuple = (t["name"], t["target_count"], aliases.get(t["id"], []))
            tasks_by_scene.setdefault(t["scene_id"], []).append(entry + ((t["directory"],) if t["directory"] else ()))
        scene_tasks = [(s["name"], tasks_by_scene.get(s["id"], []))
                       for s in conn.execute("SELECT id, name FROM scenes ORDER BY position")]
        return data_version, scene_tasks

    @staticmethod
    def _tags(conn: sqlite3.Connection) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {}
        for r in conn.execute("SELECT name, tags FROM tasks"):
            try:
                tags = json.loads(r["tags"] or "[]")
            except ValueError:
                tags = []
            if tags:
                out[r["name"]] = tags
        return out

    def get_task_tags(self) -> dict[str, list[str]]:
        """任务名 → 标签。标签只是分类用的元数据，下载器不读取。"""
        with self._read() as conn:
            return self._tags(conn)

    def get_tasks_dict(self) -> dict[str, Any]:
        """当前任务配置（含标签）的 JSON 结构，一个事务内读取。"""
        with self._read() as conn:
            data_version, scene_tasks = self._tasks(conn)
            return tasks_to_dict(data_version, scene_tasks, self._tags(conn))

    @staticmethod
    def _disks(conn: sqlite3.Connection) -> list[dict[str, str]]:
        return [{"name": r["name"], "path": r["path"], "mountpoint": r["mountpoint"]}
                for r in conn.execute("SELECT name, path, mountpoint FROM disks ORDER BY position")]

    def get_tasks(self) -> tuple[str, list]:
        """返回 (版本目录, SCENE_TASKS)，格式与下载脚本里的 SCENE_TASKS 完全一致。"""
        with self._read() as conn:
            return self._tasks(conn)

    def snapshot(self) -> dict[str, Any]:
        """一次读取全部配置（同一个事务）：不会读到“任务是新的、磁盘还是旧的”这种中间状态。"""
        with self._read() as conn:
            data_version, scene_tasks = self._tasks(conn)
            return {"settings": self._settings(conn), "data_version": data_version,
                    "scene_tasks": scene_tasks, "disks": self._disks(conn), "tags": self._tags(conn)}

    def set_tasks(self, data_version: str, scene_tasks: list, tags: dict[str, list[str]] | None = None) -> None:
        """整体替换任务配置（一个事务，要么全成功要么不变）。

        tags 为 None：沿用同名任务原有的标签（下载器、恢复默认等不关心标签的调用不会把标签抹掉）；
        传字典：按它设置（没出现的任务无标签）。
        """
        try:
            with self._tx() as conn:
                if tags is None:
                    tags = self._tags(conn)
                conn.execute("DELETE FROM task_aliases")
                conn.execute("DELETE FROM tasks")
                conn.execute("DELETE FROM scenes")
                for s_pos, (scene, tasks) in enumerate(scene_tasks):
                    sid = conn.execute("INSERT INTO scenes(position, name) VALUES(?,?)", (s_pos, scene)).lastrowid
                    for t_pos, (name, count, alias_list, *extra) in enumerate(tasks):
                        tid = conn.execute(
                            "INSERT INTO tasks(scene_id, position, name, target_count, directory, tags) VALUES(?,?,?,?,?,?)",
                            (sid, t_pos, name, int(count), extra[0] if extra and extra[0] else "",
                             json.dumps(normalize_tags(tags.get(name)), ensure_ascii=False))).lastrowid
                        for a_pos, alias in enumerate(alias_list):
                            conn.execute("INSERT INTO task_aliases(task_id, position, alias) VALUES(?,?,?)", (tid, a_pos, alias))
                conn.execute(
                    "INSERT INTO settings(key, value, updated_at) VALUES(?,?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                    (DATA_VERSION_KEY, json.dumps(data_version, ensure_ascii=False), _now()))
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"任务配置有重复或非法的值：{exc}") from exc

    # ---------------------------- 任务方案 ----------------------------
    @staticmethod
    def _clean_plan_name(name: str) -> str:
        name = (name or "").strip()
        if not name:
            raise ValueError("方案名称不能为空")
        if len(name) > MAX_PLAN_NAME:
            raise ValueError(f"方案名称最长 {MAX_PLAN_NAME} 个字符")
        return name

    def list_plans(self) -> list[dict[str, Any]]:
        with self._read() as conn:
            rows = conn.execute("SELECT id, name, note, payload, created_at, updated_at FROM plans ORDER BY updated_at DESC, id DESC").fetchall()
        return [{"id": r["id"], "name": r["name"], "note": r["note"], "created_at": r["created_at"],
                 "updated_at": r["updated_at"], **plan_summary(json.loads(r["payload"]))} for r in rows]

    def get_plan(self, plan_id: int) -> dict[str, Any] | None:
        with self._read() as conn:
            r = conn.execute("SELECT id, name, note, payload, created_at, updated_at FROM plans WHERE id=?", (plan_id,)).fetchone()
        if r is None:
            return None
        payload = json.loads(r["payload"])
        return {"id": r["id"], "name": r["name"], "note": r["note"], "created_at": r["created_at"],
                "updated_at": r["updated_at"], "payload": payload, **plan_summary(payload)}

    def save_plan(self, name: str, payload: dict[str, Any], note: str = "", overwrite: bool = False) -> int:
        """新建方案；同名已存在时：overwrite=True 覆盖内容，否则抛 ValueError。返回方案 id。"""
        name = self._clean_plan_name(name)
        data = json.dumps(canonical_payload(payload), ensure_ascii=False)
        now = _now()
        with self._tx() as conn:
            row = conn.execute("SELECT id FROM plans WHERE name=?", (name,)).fetchone()
            if row and not overwrite:
                raise ValueError(f"已有同名方案：{name}")
            if row:
                conn.execute("UPDATE plans SET payload=?, note=CASE WHEN ?<>'' THEN ? ELSE note END, updated_at=? WHERE id=?",
                             (data, note.strip(), note.strip(), now, row["id"]))
                return int(row["id"])
            return int(conn.execute("INSERT INTO plans(name, note, payload, created_at, updated_at) VALUES(?,?,?,?,?)",
                                    (name, note.strip(), data, now, now)).lastrowid)

    def update_plan(self, plan_id: int, name: str | None = None, note: str | None = None,
                    payload: dict[str, Any] | None = None) -> bool:
        sets, args = [], []
        if name is not None:
            sets.append("name=?"); args.append(self._clean_plan_name(name))
        if note is not None:
            sets.append("note=?"); args.append(note.strip())
        if payload is not None:
            sets.append("payload=?"); args.append(json.dumps(canonical_payload(payload), ensure_ascii=False))
        if not sets:
            return True
        sets.append("updated_at=?"); args.append(_now())
        try:
            with self._tx() as conn:
                return conn.execute(f"UPDATE plans SET {', '.join(sets)} WHERE id=?", (*args, plan_id)).rowcount > 0
        except sqlite3.IntegrityError as exc:
            raise ValueError("已有同名方案") from exc

    def delete_plan(self, plan_id: int) -> bool:
        with self._tx() as conn:
            return conn.execute("DELETE FROM plans WHERE id=?", (plan_id,)).rowcount > 0

    # ---------------------------- 磁盘 ----------------------------
    def get_disks(self) -> list[dict[str, str]]:
        with self._read() as conn:
            return self._disks(conn)

    def set_disks(self, disks: list[dict[str, str]]) -> None:
        seen: set[str] = set()
        rows = []
        for i, d in enumerate(disks):
            name, path = str(d.get("name", "")).strip(), str(d.get("path", "")).strip()
            mount = str(d.get("mountpoint", "")).strip() or path
            if not name or not path:
                raise ValueError(f"第 {i + 1} 块磁盘：名称和路径都不能为空")
            if name in seen:
                raise ValueError(f"磁盘名称重复：{name}")
            seen.add(name)
            rows.append((name, i, path, mount))
        with self._tx() as conn:
            conn.execute("DELETE FROM disks")
            conn.executemany("INSERT INTO disks(name, position, path, mountpoint) VALUES(?,?,?,?)", rows)

    # ---------------------------- Web 口令（只存哈希） ----------------------------
    def has_web_token(self) -> bool:
        with self._read() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key=?", (TOKEN_KEY,)).fetchone()
        return bool(row and json.loads(row["value"]))

    def set_web_token(self, token: str) -> None:
        token = token.strip()
        if len(token) < 6:
            raise ValueError("访问口令至少 6 个字符")
        salt = secrets.token_hex(16)
        digest = hashlib.pbkdf2_hmac("sha256", token.encode("utf-8"), bytes.fromhex(salt), 120_000).hex()
        stored = f"pbkdf2_sha256$120000${salt}${digest}"
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO settings(key, value, updated_at) VALUES(?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                (TOKEN_KEY, json.dumps(stored), _now()))

    def clear_web_token(self) -> None:
        with self._tx() as conn:
            conn.execute("DELETE FROM settings WHERE key=?", (TOKEN_KEY,))

    def verify_web_token(self, token: str) -> bool:
        with self._read() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key=?", (TOKEN_KEY,)).fetchone()
        if not row:
            return False
        try:
            _algo, iters, salt, digest = json.loads(row["value"]).split("$")
            probe = hashlib.pbkdf2_hmac("sha256", token.encode("utf-8"), bytes.fromhex(salt), int(iters)).hex()
        except (ValueError, TypeError):
            return False
        return hmac.compare_digest(probe, digest)
