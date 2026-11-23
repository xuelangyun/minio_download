#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Download selected MinIO prefixes to multiple mounted disks, with SQLite state."""

from __future__ import annotations

import argparse
import fcntl
import fnmatch
import json
import logging
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from logging.handlers import RotatingFileHandler
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any

import psycopg2


# ============================ Configuration ============================
MC_BIN = "/opt/mc"
MC_ALIAS = "minio1"

# MinIO 已通过 /opt/mc 配置好 alias，无需在脚本中重复配置账号密码。
# 请确保运行脚本的 Linux 用户能读取自己的 mc 配置，且 minio1 alias 可用。
MINIO_BUCKET = "rocp-data"

# PostgreSQL 连接信息：直接写在脚本中。
PG_HOST = "127.0.0.1"
PG_PORT = 5432
PG_DATABASE = "minio_event"
PG_USER = "postgres"
PG_PASSWORD = "postgres@123"
# 任务配置（按时长）。本地目录结构：<磁盘>/<DATA_VERSION>/<场景>/<目录名>/<episode>
#   例：银河2.2数据/Education and Entertainment(教育娱乐)/积木搭建
# 每个场景下一行一个任务，格式 (任务名, 目标小时, [历史别名...]) 或 (任务名, 目标小时, [历史别名...], 目录名)。
#   任务名   当前使用的任务名；自上而下即下载顺序（场景块按书写顺序）。
#   目标小时 content_time 按数据库字段约定使用秒；按 create_time 升序选择完整 episode，
#            已下载的数据会先计入目标时长，只补足缺口，不会重复下载；
#            episode 不可拆分，最终所选时长可能略高于目标。目前所有任务默认 10 小时，按需逐行修改。
#   历史别名 数据库里旧记录的任务名（如 0904_xxx）。查询时会把这些旧名字的记录也查出来，
#            并改名为当前任务名：计入同一个目标时长、同一块磁盘、同一个本地目录。
#            不写别名，旧名字的记录就不会被下载。
#   目录名   可选。场景目录下的文件夹名；不写则与任务名相同。
# 规则：同一场景内，去掉任务名开头的日期/数字前缀后同名的任务视为同一个任务（旧名字写进历史别名）。
# 任务名必须全局唯一（数据库按任务名区分）；同一场景下目录名不能重复。
DATA_VERSION = "银河2.2数据"   # 最上层目录（版本）

SCENE_TASKS = [
    
    ("Household(家居)", [
        ("0930_废物装箱", 5, ["0902_废物装箱", "0904_废物装箱"], "废物装箱"),
        ("0930插花艺术", 10, ["0902_插花艺术", "0904_插花艺术"], "插花艺术"),
        ("0930_电线分类", 10, ["0902_电线分类", "0904_电线分类"], "电线分类"),
        ("0930_颜色分类", 10, ["0902_颜色分类", "0904_颜色分类"], "颜色分类"),
        ("桌面收纳", 5, ["0930桌面收纳"]),
        ("0930积木搭建", 10, [], "积木搭建"),
        ("药品检查", 5, ["0930药品检查"]),
        ("0930餐盘整理", 5, [], "餐盘整理"),
        ("叠牛仔裤", 5, []),
        ("垃圾处理", 5, []),
       
    ]),
    ("Industry(工业)", [
        ("0911电器物料抓取_左A右A", 1, ["0818电器物料抓取_左A右A", "0820电器物料抓取_左A右A", "0908电器物料抓取_左A右A"], "电器物料抓取_左A右A"),
        ("0818电器物料抓取_左A右B", 1, ["0911电器物料抓取_左A右B"], "电器物料抓取_左A右B"),
        ("0818电器物料抓取_左B右A", 0.5, [], "电器物料抓取_左B右A"),
        ("0820电器物料抓取_左B右B", 2, ["0818电器物料抓取_左B右B"], "电器物料抓取_左B右B"),
        ("0904_单臂清框_电器开关1P", 5, ["0902_单臂清框_电器开关1P"], "单臂清框_电器开关1P"),
        ("0904_单臂清框_电器开关2P", 5, ["0902_单臂清框_电器开关2P"], "单臂清框_电器开关2P"),
        ("0904_单臂清框_电器开关3P", 5, [], "单臂清框_电器开关3P"),
        ("单臂清框_零件A", 5, []),
        ("单臂清框_零件B", 5, []),
        ("单臂清框_零件C", 5, []),
        ("电器物料分拣", 1, []),
        ("线缆管理", 5, []),
  
    ]),
]


def _build_task_config(scene_tasks):
    """由 SCENE_TASKS 派生 TASK_NAMES / TASK_TARGET_HOURS / TASK_NAME_ALIASES / TASK_DIR_NAMES / TASK_SCENES，并校验配置。"""
    names: list[str] = []
    hours: dict[str, float] = {}
    aliases: dict[str, str] = {}
    dir_names: dict[str, str] = {}
    scenes: dict[str, str] = {}
    path_owner: dict[tuple[str, str], str] = {}
    for scene, tasks in scene_tasks:
        for name, target_hours, _, *extra in tasks:
            if name in hours:
                raise ValueError(f"任务名重复: {name!r}（任务名必须全局唯一）")
            if not isinstance(target_hours, (int, float)) or target_hours < 0:
                raise ValueError(f"任务 {name!r} 的目标小时必须是非负数，当前为 {target_hours!r}")
            dir_name = (extra[0] if extra and extra[0] else name).strip()
            key = (scene, dir_name)
            if key in path_owner:
                raise ValueError(
                    f"场景 {scene!r} 下目录名 {dir_name!r} 被任务 {path_owner[key]!r} 和 {name!r} 同时使用；"
                    "请把其中一个写进另一个的历史别名。"
                )
            path_owner[key] = name
            names.append(name)
            hours[name] = float(target_hours)
            dir_names[name] = dir_name
            scenes[name] = scene
    for scene, tasks in scene_tasks:
        for name, _, alias_list, *_ in tasks:
            for alias in alias_list:
                if alias in hours:
                    raise ValueError(f"别名 {alias!r}（任务 {name!r}）与某个当前任务名冲突")
                if alias in aliases:
                    raise ValueError(f"别名 {alias!r} 重复出现在任务 {aliases[alias]!r} 和 {name!r} 中")
                aliases[alias] = name
    return names, hours, aliases, dir_names, scenes


TASK_NAMES, TASK_TARGET_HOURS, TASK_NAME_ALIASES, TASK_DIR_NAMES, TASK_SCENES = _build_task_config(SCENE_TASKS)


def task_dir_name(task_name: str) -> str:
    """任务在磁盘上的相对目录：<版本>/<场景>/<目录名>（未配置的任务归入“未分类”场景）。"""
    scene = TASK_SCENES.get(task_name, "未分类")
    folder = TASK_DIR_NAMES.get(task_name, task_name)
    return "/".join(safe_component(x) for x in (DATA_VERSION, scene, folder))

# Set each path to a real mounted disk (mountpoint), or set path to a subdirectory
# and mountpoint to the actual mountpoint. Never use a mountpoint that may be absent.
DEST_DISKS = [
    #{"name": "disk1", "path": "/mnt/temdata1", "mountpoint": "/mnt/temdata1"},
    #{"name": "disk2", "path": "/mnt/temdata2", "mountpoint": "/mnt/temdata2"},
    #{"name": "disk3", "path": "/mnt/temdata3", "mountpoint": "/mnt/temdata3"},
    #{"name": "disk4", "path": "/mnt/temdata4", "mountpoint": "/mnt/temdata4"},
    {"name": "disk5", "path": "/mnt/temdata6", "mountpoint": "/mnt/temdata6"},
    # Add more disks here, for example:
    # {"name": "disk4", "path": "/mnt/temdata4", "mountpoint": "/mnt/temdata4"},
]

# 换盘 / 归档：磁盘写满后换成同路径、同 name 的新盘，旧盘数据已取走保管时，
# 数据库(downloads)/台账(downloaded_episodes)里已完成的记录即使本地目录不存在，也视为“已下载”，
# 不再报警、不再重新下载，也照常计入任务目标时长。
# 需要“目录被误删后重新下载”时，用环境变量 TRUST_COMPLETED_RECORDS=0 运行即可。
TRUST_COMPLETED_RECORDS = os.getenv("TRUST_COMPLETED_RECORDS", "1") != "0"

# Reserve this much free space after a download. Increase/decrease to suit the disks.
MIN_FREE_GB = 3.0
# file_size is the estimated source size in bytes; reserve additional headroom for variance.
SPACE_SAFETY_FACTOR = 1.15
# 每块磁盘同一时间只处理一个 TASK_NAMES 任务；该任务内部最多 2 个写入并发。
# 不同磁盘可同时处理不同任务。
MAX_WRITES_PER_DISK = 1
MAX_TASKS_PER_DISK = 1

SCRIPT_DIR = Path(__file__).resolve().parent
STATE_DB = Path(os.getenv("MINIO_DOWNLOAD_STATE_DB", str(SCRIPT_DIR / "minio_download_state.sqlite3")))
LOG_DIR = Path(os.getenv("MINIO_DOWNLOAD_LOG_DIR", str(SCRIPT_DIR / "logs")))
LOCK_FILE = SCRIPT_DIR / "minio_download.lock"

# These options deliberately omit --remove and --watch. This is a one-time download,
# not a bidirectional sync, and should not delete unrelated destination files.
MC_MIRROR_FLAGS = ["--overwrite", "--preserve", "--retry", "--summary"]

# 是否要求每条数据（episode 目录）内必须有 mcap 文件，类似必需的 task_info.json：
#   True  : 目录内没有匹配 MCAP_FILE_PATTERN 的文件则跳过该条（不标记完成，下次运行会重新检查）
#   False : 不检查
# 也可用环境变量覆盖：MINIO_REQUIRE_MCAP=0 关闭，=1 开启。
REQUIRE_MCAP = os.getenv("MINIO_REQUIRE_MCAP", "1").strip().lower() not in ("0", "false", "no", "off", "")
# 匹配 episode 目录下的文件名（fnmatch 通配符，区分大小写）。
# 例如 20260803_092916_record0.SYNC.mcap，其中 20260803_092916_record0 每条都不同。
MCAP_FILE_PATTERN = "*.SYNC.mcap"

SQL = """
SELECT
    j.episode_id,
    j.job_id,
    j.task_name,
    j.sn_code,
    j.parent_key,
    j.file_size,
    j.content_time,
    j.parsed_status,
    j.quality_status,
    j.create_time,
    j.video_file_size
FROM public.minio_galbot_scene_task_jobs AS j
WHERE j.del_flag = 0
  AND j.quality_status NOT IN ('不合格', '未解析', '未质检')
  AND j.job_id IN (
      SELECT t.job_id
      FROM public.minio_galbot_scene_task AS t
      WHERE t.task_name = ANY(%s)
  )
ORDER BY j.task_name ASC, j.create_time ASC, j.episode_id ASC
"""

NO_SPACE_MARKERS = (
    "no space left on device",
    "disk quota exceeded",
    "not enough space",
    "no space available",
    "errno 28",
    "os error 28",
    "write: no space",
    "write failed: no space",
    "no space left",
)


# ================================ Logging ================================
def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    file_handler = RotatingFileHandler(
        LOG_DIR / "minio_task_downloader.log",
        maxBytes=50 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    root.handlers.clear()
    root.addHandler(console)
    root.addHandler(file_handler)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def to_int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


# ================================ SQLite ================================
class StateDB:
    def __init__(self, path: Path, initialize: bool = True):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(path), timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=30000")
        if initialize:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS downloads (
                episode_id TEXT PRIMARY KEY,
                task_name TEXT NOT NULL,
                parent_key TEXT,
                disk_name TEXT,
                target_path TEXT,
                partial_path TEXT,
                expected_bytes INTEGER,
                status TEXT NOT NULL DEFAULT 'pending',
                error TEXT,
                started_at TEXT,
                completed_at TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS task_disk_assignments (
                task_name TEXT NOT NULL,
                disk_name TEXT NOT NULL,
                last_used_at TEXT NOT NULL,
                PRIMARY KEY (task_name, disk_name)
            );
            CREATE INDEX IF NOT EXISTS idx_download_task_status
                ON downloads(task_name, status);

            -- 已下载台账：仅在数据确认下载完成（或在本地磁盘上发现已有副本）后写入，
            -- 用于防重复下载和已下载时长统计。
            CREATE TABLE IF NOT EXISTS downloaded_episodes (
                episode_id TEXT PRIMARY KEY,
                task_name TEXT NOT NULL,
                source_task_name TEXT,
                job_id INTEGER,
                sn_code TEXT,
                parent_key TEXT,
                disk_name TEXT,
                target_path TEXT,
                file_size INTEGER,
                content_time REAL,
                create_time TEXT,
                downloaded_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_downloaded_task
                ON downloaded_episodes(task_name);
            """
        )
        self.conn.commit()

    def get_download(self, episode_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM downloads WHERE episode_id = ?", (episode_id,)
        ).fetchone()

    def set_download(self, episode_id: str, task_name: str, parent_key: str,
                     expected_bytes: int | None, status: str,
                     disk_name: str | None = None,
                     target_path: str | None = None,
                     partial_path: str | None = None,
                     error: str | None = None,
                     started_at: str | None = None,
                     completed_at: str | None = None) -> None:
        self.conn.execute(
            """
            INSERT INTO downloads (
                episode_id, task_name, parent_key, disk_name, target_path,
                partial_path, expected_bytes, status, error, started_at,
                completed_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(episode_id) DO UPDATE SET
                task_name=excluded.task_name,
                parent_key=excluded.parent_key,
                disk_name=excluded.disk_name,
                target_path=excluded.target_path,
                partial_path=excluded.partial_path,
                expected_bytes=excluded.expected_bytes,
                status=excluded.status,
                error=excluded.error,
                started_at=COALESCE(excluded.started_at, downloads.started_at),
                completed_at=excluded.completed_at,
                updated_at=excluded.updated_at
            """,
            (episode_id, task_name, parent_key, disk_name, target_path,
             partial_path, expected_bytes, status, error, started_at,
             completed_at, utc_now()),
        )
        self.conn.commit()

    def task_disk_names(self, task_name: str) -> list[str]:
        # Keep compatibility with older SQLite databases that may contain multiple
        # assignments per task. The caller detects and repairs this legacy state.
        rows = self.conn.execute(
            "SELECT disk_name FROM task_disk_assignments WHERE task_name=? ORDER BY last_used_at DESC",
            (task_name,),
        ).fetchall()
        return list(dict.fromkeys(str(r[0]) for r in rows))

    def disk_task_counts(self) -> dict[str, int]:
        rows = self.conn.execute(
            "SELECT disk_name, COUNT(*) FROM task_disk_assignments GROUP BY disk_name"
        ).fetchall()
        return {str(r[0]): int(r[1]) for r in rows}

    def assign_task_disk(self, task_name: str, disk_name: str) -> None:
        # A task must have exactly one disk assignment. Remove any legacy assignments
        # to other disks before recording the single authoritative assignment.
        self.conn.execute(
            "DELETE FROM task_disk_assignments WHERE task_name=? AND disk_name<>?",
            (task_name, disk_name),
        )
        self.conn.execute(
            """
            INSERT INTO task_disk_assignments(task_name, disk_name, last_used_at)
            VALUES (?, ?, ?)
            ON CONFLICT(task_name, disk_name) DO UPDATE SET last_used_at=excluded.last_used_at
            """,
            (task_name, disk_name, utc_now()),
        )
        self.conn.commit()

    # ---- 已下载台账 ----
    def get_downloaded(self, episode_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM downloaded_episodes WHERE episode_id = ?", (episode_id,)
        ).fetchone()

    def record_downloaded(self, row: dict[str, Any], disk_name: str | None,
                          target_path: str | Path) -> None:
        task_name = str(row.get("task_name") or "未命名任务")
        create_time = row.get("create_time")
        self.conn.execute(
            """
            INSERT INTO downloaded_episodes (
                episode_id, task_name, source_task_name, job_id, sn_code, parent_key,
                disk_name, target_path, file_size, content_time, create_time, downloaded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(episode_id) DO UPDATE SET
                task_name=excluded.task_name,
                disk_name=excluded.disk_name,
                target_path=excluded.target_path,
                file_size=excluded.file_size,
                content_time=excluded.content_time
            """,
            (
                str(row["episode_id"]), task_name,
                str(row.get("source_task_name") or task_name),
                to_int_or_none(row.get("job_id")),
                row.get("sn_code"),
                str(row.get("parent_key") or ""),
                disk_name, str(target_path),
                to_int_or_none(row.get("file_size")),
                parse_duration_seconds(row.get("content_time")),
                str(create_time) if create_time is not None else None,
                utc_now(),
            ),
        )
        self.conn.commit()

    def completed_records(self) -> list[tuple[str, str, str]]:
        """All episodes recorded as downloaded: (episode_id, task_name, target_path)."""
        rows = self.conn.execute(
            """
            SELECT episode_id, task_name, target_path FROM downloads
              WHERE status = 'completed' AND target_path IS NOT NULL
            UNION
            SELECT episode_id, task_name, target_path FROM downloaded_episodes
              WHERE target_path IS NOT NULL
            """
        ).fetchall()
        unique: dict[str, tuple[str, str, str]] = {}
        for r in rows:
            unique.setdefault(str(r["episode_id"]), (str(r["episode_id"]), str(r["task_name"]), str(r["target_path"])))
        return list(unique.values())

    def clear_partial_path(self, partial_path: str | Path) -> None:
        self.conn.execute("UPDATE downloads SET partial_path = NULL WHERE partial_path = ?", (str(partial_path),))
        self.conn.commit()

    def forget_episode(self, episode_id: str) -> None:
        """Remove every record of an episode (state table + ledger)."""
        self.conn.execute("DELETE FROM downloads WHERE episode_id = ?", (episode_id,))
        self.conn.execute("DELETE FROM downloaded_episodes WHERE episode_id = ?", (episode_id,))
        self.conn.commit()

    def downloaded_summary(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT task_name, COUNT(*) AS n,
                   COALESCE(SUM(content_time), 0) AS seconds,
                   COALESCE(SUM(file_size), 0) AS bytes
            FROM downloaded_episodes GROUP BY task_name ORDER BY task_name
            """
        ).fetchall()

    def close(self) -> None:
        self.conn.close()


# ============================ Path / Disk helpers ============================
def safe_component(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    text = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", text)
    text = text.strip(" .")
    if text in ("", ".", ".."):
        return "unnamed"
    return text


def normalize_parent_key(value: Any) -> str:
    if value is None:
        raise ValueError("parent_key 为空")
    key = str(value).strip().strip("/")
    if not key:
        raise ValueError("parent_key 为空")
    if any(part in (".", "..") for part in key.split("/")):
        raise ValueError(f"parent_key 含有不允许的路径段: {key!r}")
    return key


def build_disk_configs() -> list[dict[str, Any]]:
    if not DEST_DISKS:
        raise RuntimeError("DEST_DISKS 至少配置一个目标磁盘")
    result: list[dict[str, Any]] = []
    names: set[str] = set()
    device_ids: dict[int, str] = {}

    for item in DEST_DISKS:
        name = str(item["name"])
        path = Path(item["path"]).expanduser().absolute()
        mountpoint = Path(item["mountpoint"]).expanduser().resolve()

        if name in names:
            raise RuntimeError(f"目标磁盘 name 重复: {name}")
        names.add(name)

        if not mountpoint.exists() or not mountpoint.is_dir() or not os.path.ismount(str(mountpoint)):
            raise RuntimeError(
                f"挂载点不存在或没有真正挂载: {mountpoint}。为避免写入系统盘，已停止。"
            )

        resolved_path = path.resolve()
        try:
            common = os.path.commonpath([str(mountpoint), str(resolved_path)])
        except ValueError:
            common = ""
        if common != str(mountpoint):
            raise RuntimeError(f"目标路径 {path} 不在设置的挂载点 {mountpoint} 之下")

        path.mkdir(parents=True, exist_ok=True)
        st_dev = path.stat().st_dev
        if st_dev in device_ids:
            raise RuntimeError(
                f"{name} 和 {device_ids[st_dev]} 指向同一个文件系统，请检查磁盘配置，避免重复计算容量。"
            )
        device_ids[st_dev] = name
        result.append({"name": name, "path": path, "mountpoint": mountpoint, "st_dev": st_dev})

    return result


def directory_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    # GNU du is available on normal Linux installations and is faster than Python-walking
    # a directory containing a very large number of files.
    proc = subprocess.run(
        ["du", "-sb", "--", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if proc.returncode == 0 and proc.stdout.strip():
        try:
            return int(proc.stdout.split(None, 1)[0])
        except (ValueError, IndexError):
            pass
    logging.warning("无法读取临时目录大小 %s，将按 0 字节估算：%s", path, proc.stderr.strip())
    return 0


def required_free_bytes(expected_bytes: int, already_downloaded_bytes: int = 0) -> int:
    remaining = max(0, expected_bytes - already_downloaded_bytes)
    return math.ceil(remaining * SPACE_SAFETY_FACTOR) + int(MIN_FREE_GB * (1024 ** 3))


def disk_free_bytes(disk: dict[str, Any]) -> int:
    return shutil.disk_usage(disk["path"]).free

class DiskReservationManager:
    """Atomically reserve estimated disk space before launching concurrent mc mirror jobs."""

    def __init__(self):
        self._condition = threading.Condition(threading.Lock())
        self._reserved_bytes: dict[str, int] = {}
        self._active_by_disk: dict[str, int] = {}
        self._active_jobs = 0
        self._minimum_free_bytes = int(MIN_FREE_GB * (1024 ** 3))

    def try_reserve(self, disk: dict[str, Any], reserve_bytes: int) -> tuple[bool, int, int, int]:
        name = str(disk["name"])
        reserve_bytes = max(0, int(reserve_bytes))
        with self._condition:
            free = disk_free_bytes(disk)
            held = self._reserved_bytes.get(name, 0)
            projected = free - held
            if projected < reserve_bytes + self._minimum_free_bytes:
                return False, free, held, projected
            self._reserved_bytes[name] = held + reserve_bytes
            self._active_by_disk[name] = self._active_by_disk.get(name, 0) + 1
            self._active_jobs += 1
            return True, free, held, projected

    def release(self, disk_name: str, reserve_bytes: int) -> None:
        with self._condition:
            held = self._reserved_bytes.get(disk_name, 0)
            self._reserved_bytes[disk_name] = max(0, held - max(0, int(reserve_bytes)))
            self._active_by_disk[disk_name] = max(0, self._active_by_disk.get(disk_name, 0) - 1)
            self._active_jobs = max(0, self._active_jobs - 1)
            self._condition.notify_all()

    def active_count(self, disk_name: str) -> int:
        with self._condition:
            return self._active_by_disk.get(disk_name, 0)

    def any_active_jobs(self) -> bool:
        with self._condition:
            return self._active_jobs > 0

    def has_active_jobs(self) -> bool:
        with self._condition:
            return self._active_jobs > 0

    def wait_for_change(self, timeout: float = 2.0) -> None:
        with self._condition:
            if self._active_jobs > 0:
                self._condition.wait(timeout=timeout)


def candidate_disks(task_name: str, disks: list[dict[str, Any]], db: StateDB,
                    resume_disk_name: str | None) -> list[dict[str, Any]]:
    """Prefer resuming a partial download, otherwise fill disks in configured order."""
    by_name = {d["name"]: d for d in disks}
    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(name: str | None) -> None:
        if name and name in by_name and name not in seen:
            ordered.append(by_name[name])
            seen.add(name)

    # Resume the previous partial directory first if it is still usable.
    add(resume_disk_name)

    # Pack the first configured disk as much as possible before using later disks.
    # Tasks arrive in task_name/create_time order, so each task is kept together where practical.
    for disk in disks:
        add(disk["name"])
    return ordered


def target_paths(disk: dict[str, Any], task_name: str, episode_id: str,
                 parent_key: str) -> tuple[Path, Path]:
    task_dir = task_dir_name(task_name)
    leaf = safe_component(Path(parent_key.rstrip("/")).name or episode_id)
    # The final path component of parent_key is globally unique; do not prefix episode_id.
    folder_name = leaf
    parent_dir = disk["path"] / task_dir
    final_path = parent_dir / folder_name
    partial_path = parent_dir / f".{folder_name}.partial"
    return final_path, partial_path


# ================================ MinIO ================================
def mirror_prefix(source: str, destination: Path, episode_id: str) -> tuple[int, bool]:
    cmd = [MC_BIN, "mirror", *MC_MIRROR_FLAGS, source, str(destination)]
    log_path = LOG_DIR / f"episode_{safe_component(episode_id)}.log"
    no_space = False

    logging.info("执行命令: %s", " ".join(subprocess.list2cmdline([x]) for x in cmd))
    with log_path.open("a", encoding="utf-8", errors="replace") as log_file:
        log_file.write(f"\n[{utc_now()}] CMD: {' '.join(cmd)}\n")
        log_file.flush()
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            encoding="utf-8",
            errors="replace",
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            print(line, end="", flush=True)
            log_file.write(line)
            log_file.flush()
            lowered = line.lower()
            if any(marker in lowered for marker in NO_SPACE_MARKERS):
                no_space = True
        return_code = proc.wait()
        log_file.write(f"[{utc_now()}] EXIT_CODE: {return_code}\n")
        log_file.flush()
    return return_code, no_space


def minio_object_exists(object_path: str) -> bool:
    """Check an exact object with the existing mc alias; never downloads a prefix without task_info.json."""
    proc = subprocess.run(
        [MC_BIN, "stat", object_path],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if proc.returncode == 0:
        return True
    detail = (proc.stdout or "").strip()
    logging.info("MinIO 对象暂不可用：%s；mc stat 返回：%s", object_path, detail or f"exit={proc.returncode}")
    return False


def minio_list_matching(prefix: str, pattern: str) -> list[str] | None:
    """List one episode directory (non-recursive) and return file names matching ``pattern``.

    Returns None when the listing itself failed (treated by callers as "not available now").
    """
    proc = subprocess.run(
        [MC_BIN, "ls", "--json", prefix.rstrip("/") + "/"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        logging.info("MinIO 目录暂不可列出：%s；mc ls 返回：%s", prefix, detail or f"exit={proc.returncode}")
        return None
    matches: list[str] = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except ValueError:
            continue
        key = str(item.get("key") or "")
        if not key or key.endswith("/"):
            continue
        name = key.rsplit("/", 1)[-1]
        if fnmatch.fnmatchcase(name, pattern):
            matches.append(name)
    return matches


def verify_mc() -> None:
    if not Path(MC_BIN).is_file() or not os.access(MC_BIN, os.X_OK):
        raise RuntimeError(f"mc 不存在或不可执行: {MC_BIN}")
    proc = subprocess.run([MC_BIN, "--version"], text=True, capture_output=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"mc --version 执行失败: {proc.stderr.strip() or proc.stdout.strip()}")
    logging.info("MinIO Client: %s", (proc.stdout or proc.stderr).strip())

    # 不重新执行 mc alias set，直接使用已配置的 alias；同时验证 bucket 可访问。
    bucket_path = f"{MC_ALIAS}/{MINIO_BUCKET}/"
    alias_proc = subprocess.run(
        [MC_BIN, "ls", bucket_path], text=True, capture_output=True, check=False
    )
    if alias_proc.returncode != 0:
        detail = alias_proc.stderr.strip() or alias_proc.stdout.strip()
        raise RuntimeError(
            f"无法访问已配置的 MinIO 路径 {bucket_path}。请检查运行用户的 mc 配置及权限。详情：{detail}"
        )
    logging.info("使用现有 MinIO alias '%s'，bucket=%s，访问验证通过。", MC_ALIAS, MINIO_BUCKET)


# ================================ Download ================================
class NoSpaceAvailable(RuntimeError):
    pass


def remove_partials_for(task_name: str, episode_id: str, parent_key: str,
                        disks: list[dict[str, Any]], db: StateDB) -> None:
    """Delete every temp (.partial) directory of one episode: it cannot be resumed."""
    paths = {target_paths(d, task_name, episode_id, parent_key)[1] for d in disks}
    state = db.get_download(episode_id)
    if state and state["partial_path"]:
        paths.add(Path(state["partial_path"]))
    for path in paths:
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
            logging.info("已删除无法恢复的临时目录：episode_id=%s path=%s", episode_id, path)


def download_one(row: dict[str, Any], disks: list[dict[str, Any]], db: StateDB,
                reservations: DiskReservationManager, task_disk_name: str | None = None) -> str:
    episode_id = str(row["episode_id"])
    task_name = str(row["task_name"] or "未命名任务")

    try:
        parent_key = normalize_parent_key(row.get("parent_key"))
    except ValueError as exc:
        db.set_download(episode_id, task_name, str(row.get("parent_key") or ""), None,
                        "invalid_source", error=str(exc))
        logging.error("episode_id=%s 源路径无效，跳过：%s", episode_id, exc)
        return "invalid_source"

    try:
        expected_bytes = int(row["file_size"]) if row.get("file_size") is not None else 0
    except (TypeError, ValueError, OverflowError):
        expected_bytes = 0
    if expected_bytes <= 0:
        db.set_download(episode_id, task_name, parent_key, None, "invalid_size",
                        error="file_size 为空或不是正数字节数")
        logging.error("episode_id=%s file_size 无效，跳过。", episode_id)
        return "invalid_size"

    source = f"{MC_ALIAS}/{MINIO_BUCKET}/{parent_key}"
    required_info_object = f"{source.rstrip('/')}/task_info.json"
    previous = db.get_download(episode_id)

    # task_info.json is mandatory. If absent, skip this episode without marking it
    # completed; every future run checks MinIO again, so late-arriving data is picked up.
    if not minio_object_exists(required_info_object):
        remove_partials_for(task_name, episode_id, parent_key, disks, db)
        db.set_download(episode_id, task_name, parent_key, expected_bytes,
                        "missing_task_info", error=f"必需文件不存在或暂时无法访问：{required_info_object}")
        logging.warning(
            "跳过 episode_id=%s：缺少必需文件 %s。不会标记完成，下次运行会重新检查。",
            episode_id, required_info_object
        )
        return "missing_task_info"

    # mcap 同样是必需文件（REQUIRE_MCAP=True 时）：目录里没有匹配的 mcap 就跳过，不标记完成。
    if REQUIRE_MCAP:
        mcap_files = minio_list_matching(source, MCAP_FILE_PATTERN)
        if not mcap_files:
            remove_partials_for(task_name, episode_id, parent_key, disks, db)
            db.set_download(episode_id, task_name, parent_key, expected_bytes,
                            "missing_mcap", error=f"缺少 mcap 文件（{MCAP_FILE_PATTERN}）：{source}")
            logging.warning(
                "跳过 episode_id=%s：目录内没有匹配 %s 的 mcap 文件（%s）。不会标记完成，下次运行会重新检查。",
                episode_id, MCAP_FILE_PATTERN, source,
            )
            return "missing_mcap"

    # Search all configured disks before downloading. This also prevents duplicate copies
    # if the SQLite state was lost/recreated but an episode directory already exists.
    existing_final_dirs: list[tuple[dict[str, Any], Path]] = []
    partial_dirs: list[Path] = []
    for candidate_disk in disks:
        candidate_final, candidate_partial = target_paths(candidate_disk, task_name, episode_id, parent_key)
        if candidate_final.is_dir():
            existing_final_dirs.append((candidate_disk, candidate_final))
        if candidate_partial.is_dir():
            partial_dirs.append(candidate_partial)
    if existing_final_dirs:
        # Never silently change the task's pinned disk because an old copy exists
        # elsewhere. That would allow one configured task to spread across disks.
        if task_disk_name:
            off_disk = [(disk, path) for disk, path in existing_final_dirs if disk["name"] != task_disk_name]
            if off_disk:
                paths = [str(path) for _, path in existing_final_dirs]
                msg = (f"任务 {task_name} 固定在磁盘 {task_disk_name}，但发现已有目录位于其他磁盘；"
                       f"为防止同一任务分散到多盘，停止处理该记录。目录：{paths}")
                db.set_download(episode_id, task_name, parent_key, expected_bytes, "failed",
                                disk_name=task_disk_name, error=msg)
                logging.error(msg)
                return "failed"
        chosen_disk, chosen_final = existing_final_dirs[0]
        if len(existing_final_dirs) > 1:
            paths = [str(path) for _, path in existing_final_dirs]
            msg = f"episode_id={episode_id} 在多个磁盘存在正式目录，违反任务单盘规则：{paths}"
            db.set_download(episode_id, task_name, parent_key, expected_bytes, "failed",
                            disk_name=task_disk_name or chosen_disk["name"], error=msg)
            logging.error(msg)
            return "failed"
        for stale_partial in partial_dirs:
            shutil.rmtree(stale_partial, ignore_errors=True)
        db.set_download(episode_id, task_name, parent_key, expected_bytes, "completed",
                        disk_name=chosen_disk["name"], target_path=str(chosen_final),
                        completed_at=utc_now())
        logging.info("episode_id=%s 已在磁盘 %s 找到现有目录，跳过重复下载：%s",
                     episode_id, chosen_disk["name"], chosen_final)
        return "skipped"

    # Repair state if a previous process renamed the final directory before it exited.
    if previous and previous["target_path"]:
        saved_final = Path(previous["target_path"])
        if saved_final.is_dir():
            old_partial = Path(previous["partial_path"]) if previous["partial_path"] else None
            if old_partial and old_partial.is_dir() and old_partial != saved_final:
                shutil.rmtree(old_partial, ignore_errors=True)
            db.set_download(episode_id, task_name, parent_key, expected_bytes, "completed",
                            disk_name=previous["disk_name"], target_path=str(saved_final),
                            partial_path=previous["partial_path"], completed_at=utc_now())
            logging.info("episode_id=%s 已存在最终目录，修复状态并跳过：%s", episode_id, saved_final)
            return "skipped"

    if previous and previous["status"] == "completed":
        if TRUST_COMPLETED_RECORDS:
            logging.info("episode_id=%s 数据库标记已完成，本地目录不存在（换盘/归档），按配置信任记录，跳过。"
                         "原路径：%s", episode_id, previous["target_path"])
            return "skipped"
        logging.warning("episode_id=%s 数据库标记已完成，但本地目录不存在，重新下载。", episode_id)

    resume_disk_name = None
    if previous and previous["partial_path"]:
        saved_partial = Path(previous["partial_path"])
        if saved_partial.is_dir() and previous["disk_name"]:
            resume_disk_name = str(previous["disk_name"])

    # A task is pinned to exactly one disk. Never spill episodes of this task to another disk.
    if task_disk_name:
        pinned = [d for d in disks if d["name"] == task_disk_name]
        candidates = pinned if pinned else candidate_disks(task_name, disks, db, resume_disk_name)
    else:
        candidates = candidate_disks(task_name, disks, db, resume_disk_name)
    permanently_skip: set[str] = set()
    space_notes: dict[str, str] = {}

    # If another job is currently using capacity, wait for a reservation to be released
    # and reevaluate. This prevents concurrent workers from overbooking the same disk.
    while True:
        # Use idle disks concurrently when another disk already has active downloads.
        # Once every disk is busy (or idle), retain configured order to keep filling
        # earlier disks as the default policy.
        ordered_candidates = candidates
        if reservations.any_active_jobs():
            idle = [d for d in candidates if reservations.active_count(d["name"]) == 0]
            busy = [d for d in candidates if reservations.active_count(d["name"]) > 0]
            if idle:
                ordered_candidates = idle + busy

        for disk in ordered_candidates:
            disk_name = disk["name"]
            if disk_name in permanently_skip:
                continue

            if previous and previous["disk_name"] == disk_name and previous["target_path"] and previous["partial_path"]:
                final_path = Path(previous["target_path"])
                partial_path = Path(previous["partial_path"])
            else:
                final_path, partial_path = target_paths(disk, task_name, episode_id, parent_key)

            if final_path.is_dir():
                old_partial = Path(previous["partial_path"]) if previous and previous["partial_path"] else None
                if old_partial and old_partial.is_dir() and old_partial != final_path:
                    shutil.rmtree(old_partial, ignore_errors=True)
                db.set_download(episode_id, task_name, parent_key, expected_bytes, "completed",
                                disk_name=disk_name, target_path=str(final_path),
                                partial_path=str(partial_path), completed_at=utc_now())
                logging.info("episode_id=%s 目标目录已存在，标记完成：%s", episode_id, final_path)
                return "skipped"

            partial_size = directory_size_bytes(partial_path) if partial_path.is_dir() else 0
            remaining_estimate = max(0, expected_bytes - partial_size)
            reserve_bytes = math.ceil(remaining_estimate * SPACE_SAFETY_FACTOR)
            reserved, free, held, projected = reservations.try_reserve(disk, reserve_bytes)
            logging.info(
                "episode_id=%s 磁盘=%s 可用=%s 并发预留=%s 预计剩余下载=%s 安全预留=%s，最低保留=%s",
                episode_id, disk_name, human_bytes(free), human_bytes(held),
                human_bytes(remaining_estimate), human_bytes(reserve_bytes), human_bytes(int(MIN_FREE_GB * 1024 ** 3))
            )
            if not reserved:
                space_notes[disk_name] = (
                    f"可用={human_bytes(free)}, 并发预留={human_bytes(held)}, "
                    f"预计需={human_bytes(reserve_bytes)}, 保留={MIN_FREE_GB:.1f} GiB"
                )
                continue

            try:
                # If the old partial cannot resume on its original disk, only remove it
                # once another disk has successfully reserved enough space.
                if previous and previous["partial_path"]:
                    old_partial = Path(previous["partial_path"])
                    if old_partial.is_dir() and old_partial != partial_path:
                        shutil.rmtree(old_partial, ignore_errors=True)
                        logging.warning("切换磁盘，删除旧临时目录以释放空间：%s", old_partial)

                final_path.parent.mkdir(parents=True, exist_ok=True)
                partial_path.mkdir(parents=True, exist_ok=True)
                db.set_download(episode_id, task_name, parent_key, expected_bytes, "running",
                                disk_name=disk_name, target_path=str(final_path),
                                partial_path=str(partial_path), error=None, started_at=utc_now())
                logging.info("开始下载 episode_id=%s task=%s source=%s destination=%s disk=%s",
                             episode_id, task_name, source, partial_path, disk_name)

                return_code, no_space_error = mirror_prefix(source, partial_path, episode_id)
                if return_code == 0:
                    if final_path.exists():
                        shutil.rmtree(partial_path, ignore_errors=True)
                    else:
                        partial_path.rename(final_path)
                    db.set_download(episode_id, task_name, parent_key, expected_bytes, "completed",
                                    disk_name=disk_name, target_path=str(final_path),
                                    partial_path=str(partial_path), error=None, completed_at=utc_now())
                    logging.info("下载完成 episode_id=%s -> %s", episode_id, final_path)
                    return "completed"

                if no_space_error:
                    logging.error("mc mirror 报告磁盘空间不足，清理临时目录并尝试下一个磁盘。")
                    shutil.rmtree(partial_path, ignore_errors=True)
                    db.set_download(episode_id, task_name, parent_key, expected_bytes, "retrying_space",
                                    disk_name=None, target_path=None, partial_path=None,
                                    error=f"磁盘 {disk_name} 执行 mc mirror 时空间不足")
                    permanently_skip.add(disk_name)
                    previous = None
                    space_notes[disk_name] = "mc mirror 实际写入时遇到空间不足"
                    continue

                err = f"mc mirror 失败，退出码={return_code}；临时文件保留，可修复原因后重跑继续下载"
                db.set_download(episode_id, task_name, parent_key, expected_bytes, "failed",
                                disk_name=disk_name, target_path=str(final_path),
                                partial_path=str(partial_path), error=err)
                logging.error("%s。查看日志：%s", err, LOG_DIR / f"episode_{safe_component(episode_id)}.log")
                raise RuntimeError(f"episode_id={episode_id} 下载失败；请查看单条数据日志。")
            finally:
                reservations.release(disk_name, reserve_bytes)

        if reservations.has_active_jobs():
            reservations.wait_for_change(timeout=2.0)
            continue

        detail = "; ".join(f"{name}: {reason}" for name, reason in space_notes.items()) or "没有配置可用磁盘"
        db.set_download(
            episode_id, task_name, parent_key, expected_bytes, "waiting_space",
            disk_name=previous["disk_name"] if previous else None,
            target_path=previous["target_path"] if previous else None,
            partial_path=previous["partial_path"] if previous else None,
            error=f"所有目标磁盘暂时无法容纳此数据：{detail}",
        )
        logging.warning(
            "episode_id=%s 当前所有磁盘都无法容纳（预留 %.1f GiB 后）；继续检查后续较小数据。详情：%s",
            episode_id, MIN_FREE_GB, detail
        )
        return "waiting_space"


def download_worker(row: dict[str, Any], disks: list[dict[str, Any]], state_db_path: str,
                    reservations: DiskReservationManager, task_disk_name: str | None = None) -> str:
    """Each worker owns its own SQLite connection; reservations are shared and thread-safe."""
    worker_db = StateDB(Path(state_db_path), initialize=False)
    try:
        result = download_one(row, disks, worker_db, reservations, task_disk_name)
        if result in ("completed", "skipped"):
            state = worker_db.get_download(str(row["episode_id"]))
            if state and state["target_path"]:
                worker_db.record_downloaded(row, state["disk_name"], state["target_path"])
        return result
    finally:
        worker_db.close()


def _task_directory_has_entries(disk: dict[str, Any], task_name: str) -> bool:
    """Return True when a task directory contains entries, including resumable partials."""
    task_dir = disk["path"] / task_dir_name(task_name)
    if not task_dir.is_dir():
        return False
    try:
        return next(task_dir.iterdir(), None) is not None
    except OSError as exc:
        # Fail closed: inability to inspect a task directory must not lead to reassignment.
        raise RuntimeError(f"无法检查任务目录 {task_dir}: {exc}") from exc


def _assign_tasks_to_disks(
    ordered_tasks: list[str],
    grouped: dict[str, list[dict[str, Any]]],
    disks: list[dict[str, Any]],
    db: StateDB,
) -> tuple[dict[str, str], dict[str, str]]:
    """Assign tasks in TASK_NAMES order, balancing new tasks while honoring existing data.

    If a task already has entries on exactly one disk, that disk remains authoritative.
    If its directory contains entries on multiple disks, mark it as a conflict and do not
    download more of that task until the old copies are reconciled manually.
    """
    assignments: dict[str, str] = {}
    conflicts: dict[str, str] = {}
    disk_load = {str(d["name"]): 0 for d in disks}
    by_name = {str(d["name"]): d for d in disks}

    # Count existing task footprints first so new tasks are assigned to the least-loaded disk.
    existing_on_disk: dict[str, list[str]] = {}
    folder_disk: dict[str, str] = {}   # 共用同一目录名的任务必须在同一块磁盘
    for task_name in ordered_tasks:
        if not grouped.get(task_name):
            continue
        found = [str(d["name"]) for d in disks if _task_directory_has_entries(d, task_name)]
        existing_on_disk[task_name] = found
        if len(found) > 1:
            conflicts[task_name] = (
                f"任务目录已同时存在于多块磁盘：{found}。为保证单任务单盘，已停止该任务；"
                "请人工核实并将历史数据统一到一块磁盘后再运行。"
            )
        elif len(found) == 1:
            assignments[task_name] = found[0]
            if task_dir_name(task_name) not in folder_disk:
                folder_disk[task_dir_name(task_name)] = found[0]
                disk_load[found[0]] += 1

    # For tasks without existing on-disk data, balance assignments in TASK_NAMES order.
    # Old SQLite assignments alone are not authoritative when no data exists at that path;
    # rebalancing prevents stale assignments from pinning every task to the first disk.
    for task_name in ordered_tasks:
        if not grouped.get(task_name) or task_name in conflicts or task_name in assignments:
            continue
        folder = task_dir_name(task_name)
        if folder in folder_disk:
            name = folder_disk[folder]
        else:
            # Stable tie break follows DEST_DISKS order (disk2, then disk3, ...).
            chosen = min(
                disks,
                key=lambda d: (disk_load[str(d["name"])], disks.index(d)),
            )
            name = str(chosen["name"])
            folder_disk[folder] = name
            disk_load[name] += 1
        assignments[task_name] = name

    # Persist one authoritative assignment per task. Existing non-empty directories above
    # take priority; newly assigned tasks are distributed in TASK_NAMES order.
    for task_name, disk_name in assignments.items():
        db.assign_task_disk(task_name, disk_name)
        if not existing_on_disk.get(task_name):
            logging.info("按任务顺序分配磁盘：task=%s -> %s", task_name, disk_name)
        else:
            logging.info("沿用已有任务数据所在磁盘：task=%s -> %s", task_name, disk_name)

    return assignments, conflicts


def process_rows_parallel(rows: list[dict[str, Any]], disks: list[dict[str, Any]], db: StateDB) -> dict[str, int]:
    """Run disks in parallel, but process at most one TASK_NAMES task per disk at a time.

    Each task is pinned to one disk. Within that task, up to MAX_WRITES_PER_DISK
    episodes may download concurrently. A disk does not start its next task until
    all rows of the current task have finished.
    """
    counts = {
        "completed": 0, "skipped": 0, "invalid_source": 0, "invalid_size": 0,
        "missing_task_info": 0, "missing_mcap": 0, "waiting_space": 0, "failed": 0,
    }
    counts_lock = threading.Lock()
    reservations = DiskReservationManager()
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("task_name") or "未命名任务"), []).append(row)

    ordered_tasks = list(TASK_NAMES)
    ordered_tasks.extend(name for name in grouped if name not in TASK_NAMES)
    assignments, conflicts = _assign_tasks_to_disks(ordered_tasks, grouped, disks, db)
    total_rows = len(rows)
    processed_rows = 0
    progress_lock = threading.Lock()
    task_downloaded_seconds: dict[str, float] = {name: 0.0 for name in ordered_tasks}

    def log_task_duration_progress(task_name: str, row: dict[str, Any], result: str) -> None:
        if result not in ("completed", "skipped"):
            return
        duration = parse_duration_seconds(row.get("content_time")) or 0.0
        # Called by the main coordinator as futures are collected.
        task_downloaded_seconds[task_name] = task_downloaded_seconds.get(task_name, 0.0) + duration
        downloaded_seconds = task_downloaded_seconds[task_name]
        target_hours = float(TASK_TARGET_HOURS.get(task_name, 0.0))
        target_seconds = target_hours * 3600.0
        progress = downloaded_seconds / target_seconds * 100.0 if target_seconds > 0 else 0.0
        logging.info(
            "任务时长进度：task=%s | 本条增加=%.2f h | 累计已下载视频时长=%.2f h (%s) "
            "| 目标需要下载时长=%.2f h (%s) | 进度=%.1f%% | 本条结果=%s",
            task_name, duration / 3600.0, downloaded_seconds / 3600.0,
            format_duration_clock(downloaded_seconds), target_hours,
            format_duration_clock(target_seconds), progress, result,
        )

    logging.info(
        "调度规则：任务按 TASK_NAMES 顺序分配；不同磁盘可并行；每块磁盘同一时间只处理一个任务；"
        "每个任务固定单盘；任务内最多 %s 个并发写入；磁盘数=%s。",
        MAX_WRITES_PER_DISK, len(disks),
    )

    disk_by_name = {str(d["name"]): d for d in disks}
    # Per-disk queues preserve TASK_NAMES order. One coordinator per disk means a disk
    # cannot run two different tasks at once, while other disks remain fully independent.
    tasks_by_disk: dict[str, list[str]] = {str(d["name"]): [] for d in disks}
    for task_name in ordered_tasks:
        if not grouped.get(task_name):
            continue
        if task_name in conflicts:
            logging.error("任务 %s 因违反单盘规则而跳过：%s", task_name, conflicts[task_name])
            with counts_lock:
                counts["failed"] += len(grouped[task_name])
            with progress_lock:
                processed_rows += len(grouped[task_name])
                logging.info("处理进度：%s/%s", processed_rows, total_rows)
            continue
        disk_name = assignments.get(task_name)
        if disk_name not in tasks_by_disk:
            logging.error("任务 %s 没有有效磁盘分配（%s），跳过。", task_name, disk_name)
            with counts_lock:
                counts["failed"] += len(grouped[task_name])
            with progress_lock:
                processed_rows += len(grouped[task_name])
                logging.info("处理进度：%s/%s", processed_rows, total_rows)
            continue
        tasks_by_disk[disk_name].append(task_name)

    def process_one_disk(disk_name: str) -> None:
        nonlocal processed_rows
        # Each disk coordinator owns its SQLite connection; never share the main-thread
        # connection across threads (sqlite3 defaults to check_same_thread=True).
        coordinator_db = StateDB(STATE_DB, initialize=False)
        disk = disk_by_name[disk_name]
        try:
            for task_name in tasks_by_disk[disk_name]:
                task_rows = grouped[task_name]
                selected_seconds = sum(parse_duration_seconds(r.get("content_time")) or 0.0 for r in task_rows)
                target_hours = float(TASK_TARGET_HOURS.get(task_name, 0.0))
                logging.info(
                    "========== 开始任务：%s（%s 条），固定磁盘：%s（%s）；本盘当前仅运行此任务；"
                    "任务内最多 %s 个并发写入 | 目标时长=%.2f h (%s) | 本次选择视频时长=%.2f h (%s) ==========" ,
                    task_name, len(task_rows), disk_name, disk["path"], MAX_WRITES_PER_DISK,
                    target_hours, format_duration_clock(target_hours * 3600.0),
                    selected_seconds / 3600.0, format_duration_clock(selected_seconds),
                )
                # Verify completed rows first; a completed path on another disk must never be
                # silently accepted as satisfying the single-disk task constraint.
                pending: list[dict[str, Any]] = []
                for row in task_rows:
                    episode_id = str(row.get("episode_id"))
                    state = coordinator_db.get_download(episode_id)
                    if (state and state["status"] == "completed" and state["target_path"]
                            and (TRUST_COMPLETED_RECORDS or Path(state["target_path"]).is_dir())):
                        target = Path(state["target_path"]).resolve()
                        task_root = (disk["path"] / task_dir_name(task_name)).resolve()
                        try:
                            is_under_task_root = os.path.commonpath([str(task_root), str(target)]) == str(task_root)
                        except ValueError:
                            is_under_task_root = False
                        if is_under_task_root:
                            with counts_lock:
                                counts["skipped"] += 1
                            log_task_duration_progress(task_name, row, "skipped")
                            with progress_lock:
                                processed_rows += 1
                                logging.info("已下载过，跳过 episode_id=%s (%s)；进度：%s/%s", episode_id, task_name, processed_rows, total_rows)
                            continue
                        logging.error(
                            "episode_id=%s 的完成路径 %s 不在任务固定磁盘 %s 的目录下；拒绝跨盘写入。",
                            episode_id, target, disk_name,
                        )
                        with counts_lock:
                            counts["failed"] += 1
                        with progress_lock:
                            processed_rows += 1
                            logging.info("处理进度：%s/%s", processed_rows, total_rows)
                        continue
                    pending.append(row)
    
                # Task-level all-or-nothing space preflight: do not start any writes unless
                # the pinned disk can hold every pending episode for this task at once.
                # Existing partial downloads count toward the task's required size. Unknown
                # sizes fail closed because we cannot guarantee the whole task will fit.
                if pending:
                    task_required_bytes = 0
                    unknown_size_rows: list[dict[str, Any]] = []
                    for pending_row in pending:
                        raw_size = pending_row.get("file_size")
                        try:
                            episode_size = int(raw_size) if raw_size is not None else 0
                        except (TypeError, ValueError):
                            episode_size = 0
                        if episode_size <= 0:
                            unknown_size_rows.append(pending_row)
                            continue
                        previous_state = coordinator_db.get_download(str(pending_row.get("episode_id")))
                        partial_path = Path(previous_state["partial_path"]) if previous_state and previous_state["partial_path"] else None
                        partial_bytes = directory_size_bytes(partial_path) if partial_path and partial_path.is_dir() else 0
                        task_required_bytes += max(0, episode_size - partial_bytes)
    
                    disk_usage = shutil.disk_usage(disk["path"])
                    safety_required_bytes = math.ceil(task_required_bytes * SPACE_SAFETY_FACTOR)
                    minimum_reserve_bytes = int(MIN_FREE_GB * 1024 ** 3)
                    available_for_task = max(0, disk_usage.free - minimum_reserve_bytes)
                    if unknown_size_rows or safety_required_bytes > available_for_task:
                        reason = (
                            f"任务整体空间预检未通过：磁盘={disk_name}，可用={human_bytes(disk_usage.free)}，"
                            f"扣除最低保留后可用={human_bytes(available_for_task)}，"
                            f"任务预计剩余需求={human_bytes(task_required_bytes)}，"
                            f"含安全系数需求={human_bytes(safety_required_bytes)}，"
                            f"未知大小条数={len(unknown_size_rows)}。本任务不启动任何新写入。"
                        )
                        logging.error(reason)
                        for pending_row in pending:
                            episode_id = str(pending_row.get("episode_id"))
                            raw_size = pending_row.get("file_size")
                            try:
                                expected_size = int(raw_size) if raw_size is not None else 0
                            except (TypeError, ValueError):
                                expected_size = 0
                            old_state = coordinator_db.get_download(episode_id)
                            old_partial = old_state["partial_path"] if old_state else None
                            old_target = old_state["target_path"] if old_state else None
                            coordinator_db.set_download(
                                episode_id, task_name, str(pending_row.get("parent_key") or ""), expected_size,
                                "waiting_space", disk_name=disk_name, target_path=old_target,
                                partial_path=old_partial, error=reason,
                            )
                            with counts_lock:
                                counts["waiting_space"] += 1
                            with progress_lock:
                                processed_rows += 1
                                logging.info("任务整体空间不足，未写入 episode_id=%s；处理进度：%s/%s",
                                             episode_id, processed_rows, total_rows)
                        logging.warning("任务 %s 因整体空间不足/存在未知大小，跳过本轮写入；后续任务仍会继续。", task_name)
                        continue
                    logging.info(
                        "任务整体空间预检通过：task=%s disk=%s | 当前可用=%s | 任务预计剩余需求=%s | "
                        "安全系数后需求=%s | 最低保留=%s；允许开始写入。",
                        task_name, disk_name, human_bytes(disk_usage.free),
                        human_bytes(task_required_bytes), human_bytes(safety_required_bytes),
                        human_bytes(minimum_reserve_bytes),
                    )
    
                # This pool exists only for the current task and is fully drained before the
                # next task assigned to this disk starts. Thus task-level disk exclusivity holds.
                with ThreadPoolExecutor(
                    max_workers=max(1, MAX_WRITES_PER_DISK),
                    thread_name_prefix=f"minio-{safe_component(disk_name)}-{safe_component(task_name)}",
                ) as episode_pool:
                    active = {
                        episode_pool.submit(download_worker, row, disks, str(STATE_DB), reservations, disk_name): row
                        for row in pending
                    }
                    while active:
                        done, _ = wait(active.keys(), return_when=FIRST_COMPLETED)
                        for future in done:
                            row = active.pop(future)
                            try:
                                result = future.result()
                                with counts_lock:
                                    counts[result] = counts.get(result, 0) + 1
                                log_task_duration_progress(task_name, row, result)
                            except Exception as exc:
                                result = "failed"
                                with counts_lock:
                                    counts["failed"] += 1
                                logging.error(
                                    "下载任务失败 episode_id=%s task=%s: %s",
                                    row.get("episode_id"), task_name, exc, exc_info=True,
                                )
                            with progress_lock:
                                processed_rows += 1
                                logging.info("处理进度：%s/%s", processed_rows, total_rows)
                logging.info("========== 任务处理结束：%s；磁盘=%s；下一个任务将在本任务所有写入结束后启动 ==========" , task_name, disk_name)
        finally:
            coordinator_db.close()

    with ThreadPoolExecutor(max_workers=max(1, len(disks)), thread_name_prefix="minio-disk-coordinator") as disk_pool:
        futures = [disk_pool.submit(process_one_disk, disk_name) for disk_name in tasks_by_disk]
        for future in futures:
            try:
                future.result()
            except Exception as exc:
                logging.error("磁盘任务调度器异常：%s", exc, exc_info=True)
                with counts_lock:
                    counts["failed"] += 1

    logging.info(
        "所有任务处理结束。每块磁盘同一时间最多一个 TASK_NAMES 任务，任务内最多 %s 个并发写入。统计：%s",
        MAX_WRITES_PER_DISK, counts,
    )
    return counts


def human_bytes(value: int) -> str:
    amount = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if amount < 1024 or unit == "PiB":
            return f"{amount:.2f} {unit}"
        amount /= 1024
    return f"{amount:.2f} PiB"


def parse_duration_seconds(value: Any) -> float | None:
    """Parse content_time seconds; return None if the value is missing/invalid."""
    try:
        seconds = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return seconds


def format_duration_clock(seconds: float) -> str:
    """Render a non-negative duration as total-hours HH:MM:SS (hours may exceed 24)."""
    total = max(0, int(round(seconds)))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def find_local_copy(row: dict[str, Any], disks: list[dict[str, Any]],
                    db: StateDB) -> tuple[str, Path] | None:
    """Return (disk_name, final_dir) if this episode is already downloaded, else None.

    Order: ledger -> downloads state -> scan every disk (covers lost/recreated SQLite state).
    Only a final directory that really exists counts; deleted data will be downloaded again.
    """
    episode_id = str(row["episode_id"])
    task_name = str(row.get("task_name") or "未命名任务")

    ledger = db.get_downloaded(episode_id)
    if ledger and ledger["target_path"] and Path(ledger["target_path"]).is_dir():
        return str(ledger["disk_name"] or ""), Path(ledger["target_path"])

    state = db.get_download(episode_id)
    if (state and state["status"] == "completed" and state["target_path"]
            and Path(state["target_path"]).is_dir()):
        return str(state["disk_name"] or ""), Path(state["target_path"])

    try:
        parent_key = normalize_parent_key(row.get("parent_key"))
    except ValueError:
        parent_key = None
    if parent_key is not None:
        for disk in disks:
            final_path, _ = target_paths(disk, task_name, episode_id, parent_key)
            if final_path.is_dir():
                return str(disk["name"]), final_path

    # 换盘/归档：记录显示已下载但目录已不在当前磁盘上，信任记录（真实存在的目录优先，见上）。
    if TRUST_COMPLETED_RECORDS:
        if ledger and ledger["target_path"]:
            return str(ledger["disk_name"] or ""), Path(ledger["target_path"])
        if state and state["status"] == "completed" and state["target_path"]:
            return str(state["disk_name"] or ""), Path(state["target_path"])
    return None


def sync_downloaded_record(row: dict[str, Any], db: StateDB, disk_name: str, path: Path) -> None:
    """Make the downloads table and the ledger agree with a copy that exists on disk."""
    episode_id = str(row["episode_id"])
    state = db.get_download(episode_id)
    if not state or state["status"] != "completed" or state["target_path"] != str(path):
        db.set_download(
            episode_id, str(row.get("task_name") or "未命名任务"),
            str(row.get("parent_key") or ""), to_int_or_none(row.get("file_size")),
            "completed", disk_name=disk_name, target_path=str(path),
            partial_path=state["partial_path"] if state else None,
            completed_at=utc_now(),
        )
    ledger = db.get_downloaded(episode_id)
    if not ledger or ledger["target_path"] != str(path):
        db.record_downloaded(row, disk_name, path)


def log_downloaded_summary(db: StateDB, when: str) -> None:
    rows = db.downloaded_summary()
    if not rows:
        logging.info("已下载台账（%s）：暂无记录。", when)
        return
    logging.info("已下载台账（%s）：", when)
    for r in rows:
        logging.info("  %s | %s 条 | %.2f h | %s",
                     r["task_name"], r["n"], r["seconds"] / 3600.0, human_bytes(int(r["bytes"])))


def select_rows_for_target_hours(all_rows: list[dict[str, Any]], disks: list[dict[str, Any]],
                                 db: StateDB, exclude_ids: set[str] | None = None) -> list[dict[str, Any]]:
    """Per task: already-downloaded episodes count toward the target hours first, then the
    oldest not-yet-downloaded qualifying episodes fill the remaining gap (judged by duration).

    Already-downloaded rows stay in the result so they flow through the normal
    skip path (and the per-task progress log); they are never downloaded again.
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in all_rows:
        task_name = str(row.get("task_name") or "未命名任务")
        grouped.setdefault(task_name, []).append(row)

    selected_rows: list[dict[str, Any]] = []
    for task_name in TASK_NAMES:
        candidates = grouped.pop(task_name, [])
        target_hours = float(TASK_TARGET_HOURS[task_name])
        target_seconds = target_hours * 3600.0
        available_seconds = sum(
            duration for row in candidates
            if (duration := parse_duration_seconds(row.get("content_time"))) is not None
        )

        # 1) 已下载的数据先计入目标，并把本地已有副本补记到状态库/台账。
        keep_ids: set[str] = set()
        downloaded_seconds = 0.0
        downloaded_count = 0
        todo: list[dict[str, Any]] = []
        for row in candidates:
            found = find_local_copy(row, disks, db)
            if found:
                sync_downloaded_record(row, db, found[0], found[1])
                keep_ids.add(str(row["episode_id"]))
                downloaded_count += 1
                downloaded_seconds += parse_duration_seconds(row.get("content_time")) or 0.0
            elif not (exclude_ids and str(row["episode_id"]) in exclude_ids):
                todo.append(row)   # exclude_ids：本次运行中已确认缺少必需文件的数据，不再重试

        # 上次因缺少必需文件（task_info.json / mcap）被跳过的数据排到最后：
        # 否则它们每次都占着最早的名额，目标时长永远补不满。（稳定排序，其余保持时间顺序）
        def _was_missing(r: dict[str, Any]) -> bool:
            st = db.get_download(str(r["episode_id"]))
            return bool(st and st["status"] in ("missing_task_info", "missing_mcap"))

        todo.sort(key=_was_missing)

        # 2) 目标时长缺口由最早的、尚未下载的数据补齐（按时长判断是否补满）。
        remaining_seconds = target_seconds - downloaded_seconds
        new_seconds = 0.0
        new_count = 0
        for row in todo:
            if new_seconds >= remaining_seconds:
                break
            keep_ids.add(str(row["episode_id"]))
            new_count += 1
            duration = parse_duration_seconds(row.get("content_time"))
            if duration is not None:
                new_seconds += duration

        selected_rows.extend(r for r in candidates if str(r["episode_id"]) in keep_ids)
        logging.info(
            "任务时长计划：%s | 目标 %.2f h | SQL可用 %.2f h（%s 条）| 已下载 %.2f h（%s 条）"
            "| 本次新增 %.2f h（%s 条）",
            task_name, target_hours, available_seconds / 3600.0, len(candidates),
            downloaded_seconds / 3600.0, downloaded_count, new_seconds / 3600.0, new_count,
        )
        if available_seconds + 1e-6 < target_seconds:
            gap_hours = (target_seconds - available_seconds) / 3600.0
            logging.warning(
                "任务 %s 当前符合 SQL 条件的数据不足目标时长，缺少约 %.2f h；将下载全部可用记录。",
                task_name, gap_hours,
            )
        if not candidates:
            logging.warning("任务 %s 在当前 SQL 条件下没有查到可下载记录。", task_name)

    # 防止数据库实际返回的 task_name 与配置字符串不一致时，静默漏掉数据。
    for task_name, extra_rows in grouped.items():
        logging.warning(
            "查询返回了未配置目标时长的任务名 %r（%s 条），仍将这些记录纳入下载。请核对任务名。",
            task_name, len(extra_rows),
        )
        selected_rows.extend(extra_rows)

    return selected_rows


def fetch_rows() -> list[dict[str, Any]]:
    if not PG_PASSWORD or "替换为" in PG_PASSWORD:
        raise RuntimeError("请在脚本开头填写 PG_PASSWORD")

    conn = psycopg2.connect(
        host=PG_HOST,
        port=int(PG_PORT),
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        connect_timeout=10,
        application_name="minio_task_downloader",
    )
    try:
        with conn.cursor() as cur:
            query_task_names = list(TASK_NAMES) + list(TASK_NAME_ALIASES.keys())
            cur.execute(SQL, (query_task_names,))
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, values)) for values in cur.fetchall()]
            for row in rows:
                source_task_name = str(row.get("task_name") or "")
                canonical_task_name = TASK_NAME_ALIASES.get(source_task_name)
                if canonical_task_name:
                    row["source_task_name"] = source_task_name
                    row["task_name"] = canonical_task_name
        # Guard against a future join/query change returning duplicate episode_ids.
        deduplicated: dict[str, dict[str, Any]] = {}
        for row in rows:
            deduplicated.setdefault(str(row.get("episode_id")), row)
        if len(deduplicated) != len(rows):
            logging.warning("SQL 返回重复 episode_id，已按 episode_id 去重：%s -> %s 条。", len(rows), len(deduplicated))
        # 同一个 parent_key 对应同一份 MinIO 数据；不同 episode_id 指向同一路径时只保留最早的一条，
        # 否则会重复计入时长，并在本地指向同一个目录。
        unique_rows: list[dict[str, Any]] = []
        seen_keys: dict[str, str] = {}
        for row in deduplicated.values():
            raw_key = row.get("parent_key")
            key = str(raw_key).strip().strip("/") if raw_key is not None else ""
            if key:
                if key in seen_keys:
                    logging.warning("parent_key 重复，忽略 episode_id=%s（已保留 episode_id=%s）：%s",
                                    row.get("episode_id"), seen_keys[key], key)
                    continue
                seen_keys[key] = str(row.get("episode_id"))
            unique_rows.append(row)
        return unique_rows
    finally:
        conn.close()


def local_has_mcap(path: Path) -> bool:
    """True if the episode directory (top level) has a file matching MCAP_FILE_PATTERN.

    If the directory cannot be read, answer True: never delete on uncertainty.
    """
    try:
        with os.scandir(path) as it:
            return any(e.is_file() and fnmatch.fnmatchcase(e.name, MCAP_FILE_PATTERN) for e in it)
    except OSError as exc:
        logging.warning("无法读取目录 %s，按“有 mcap”处理（不删除）：%s", path, exc)
        return True


def cleanup_missing_mcap(disks: list[dict[str, Any]], db: StateDB, dry_run: bool) -> dict[str, int]:
    """Delete downloaded episode directories without an mcap file and forget their SQLite records.

    Only directories recorded as downloaded AND located inside a configured disk are touched.
    The affected episodes are re-selected by the normal run (filled with episodes that do have mcap).
    """
    roots = [Path(d["path"]).resolve() for d in disks]
    stats = {"checked": 0, "missing_mcap": 0, "deleted": 0, "stale_records": 0, "skipped_unsafe": 0, "failed": 0}
    per_task: dict[str, int] = {}
    for episode_id, task_name, target_path in db.completed_records():
        path = Path(target_path)
        if not path.is_dir():
            stats["stale_records"] += 1      # 记录在但目录不存在：正常流程会按需重新下载
            continue
        resolved = path.resolve()
        inside = any(root in resolved.parents and len(resolved.relative_to(root).parts) >= 2 for root in roots)
        if not inside:
            logging.warning("跳过（不在目标磁盘目录内，不会删除）：episode_id=%s path=%s", episode_id, path)
            stats["skipped_unsafe"] += 1
            continue
        stats["checked"] += 1
        if local_has_mcap(path):
            continue
        stats["missing_mcap"] += 1
        per_task[task_name] = per_task.get(task_name, 0) + 1
        if dry_run:
            logging.info("[dry-run] 缺少 mcap，将删除：episode_id=%s task=%s path=%s", episode_id, task_name, path)
            continue
        try:
            shutil.rmtree(path)
        except OSError as exc:
            logging.error("删除失败，保留记录：episode_id=%s path=%s：%s", episode_id, path, exc)
            stats["failed"] += 1
            continue
        db.forget_episode(episode_id)
        stats["deleted"] += 1
        logging.info("已删除（缺少 mcap）并清除 SQLite 记录：episode_id=%s task=%s path=%s", episode_id, task_name, path)
    for task_name, n in sorted(per_task.items()):
        logging.info("缺少 mcap：%s | %s 条", task_name, n)
    logging.info("mcap 清理%s：%s", "（dry-run，未删除任何东西）" if dry_run else "完成", stats)
    return stats


def sweep_stale_partials(rows: list[dict[str, Any]], disks: list[dict[str, Any]], db: StateDB) -> int:
    """Delete every temp ``.xxx.partial`` directory that this run will not resume.

    A partial is kept only if it belongs to an episode selected for THIS run that is not
    downloaded yet (the normal flow then resumes it). Everything else cannot be resumed:
    orphans of episodes that were not selected, partials of already-completed episodes, etc.
    """
    roots = [Path(d["path"]).resolve() for d in disks]

    keep: set[Path] = set()
    for row in rows:
        if find_local_copy(row, disks, db):
            continue    # 已下载：临时目录没用了
        episode_id = str(row["episode_id"])
        task_name = str(row.get("task_name") or "未命名任务")
        try:
            parent_key = normalize_parent_key(row.get("parent_key"))
        except ValueError:
            continue
        for disk in disks:
            keep.add(target_paths(disk, task_name, episode_id, parent_key)[1].resolve())
        state = db.get_download(episode_id)
        if state and state["partial_path"]:
            keep.add(Path(state["partial_path"]).resolve())

    found: set[Path] = set()
    task_dirs = {task_dir_name(t) for t in TASK_NAMES}
    for disk in disks:
        for rel in task_dirs:
            base = Path(disk["path"]) / rel
            try:
                with os.scandir(base) as it:
                    for e in it:
                        if e.is_dir(follow_symlinks=False) and e.name.startswith(".") and e.name.endswith(".partial"):
                            found.add(Path(e.path).resolve())
            except OSError:
                continue    # 任务目录不存在
    for r in db.conn.execute("SELECT DISTINCT partial_path FROM downloads WHERE partial_path IS NOT NULL").fetchall():
        candidate = Path(r["partial_path"])
        if candidate.is_dir():
            found.add(candidate.resolve())

    removed = 0
    freed = 0
    for path in sorted(found):
        inside = any(root in path.parents and len(path.relative_to(root).parts) >= 2 for root in roots)
        if not inside or not (path.name.startswith(".") and path.name.endswith(".partial")):
            continue    # 只删目标磁盘内、名字形如 .xxx.partial 的目录
        if path in keep:
            continue    # 本次会续传
        size = directory_size_bytes(path)
        shutil.rmtree(path, ignore_errors=True)
        if path.exists():
            logging.warning("临时目录删除失败（请检查权限）：%s", path)
            continue
        db.clear_partial_path(path)
        removed += 1
        freed += size
        logging.info("已删除无法恢复的临时目录：%s（%s）", path, human_bytes(size))
    logging.info("临时目录清扫完成：删除 %s 个，释放 %s；保留（本次将续传）%s 个。",
                 removed, human_bytes(freed), len([p for p in found if p in keep]))
    return removed


MISSING_STATUSES = ("missing_task_info", "missing_mcap")
MAX_FILL_ROUNDS = 10   # 因缺少必需文件被跳过后，最多再补几轮（每轮改选后面的数据）


def run_fill_rounds(all_rows: list[dict[str, Any]], disks: list[dict[str, Any]], db: StateDB) -> dict[str, int]:
    """Select + download; if some episodes were skipped for missing required files, select
    replacements (later episodes) and download again, until the targets are met or nothing is left."""
    total: dict[str, int] = {}
    skip_ids: set[str] = set()
    for round_no in range(1, MAX_FILL_ROUNDS + 1):
        rows = select_rows_for_target_hours(all_rows, disks, db, exclude_ids=skip_ids)
        logging.info(
            "---------- 第 %s 轮：PostgreSQL 查询到 %s 条候选；选中 %s 条记录（含已下载，已下载的会自动跳过）----------",
            round_no, len(all_rows), len(rows),
        )
        if round_no == 1:
            sweep_stale_partials(rows, disks, db)
        counts = process_rows_parallel(rows, disks, db)
        logging.info("第 %s 轮统计：%s", round_no, counts)
        for key, value in counts.items():
            total[key] = total.get(key, 0) + value
        if counts.get("failed", 0) or counts.get("waiting_space", 0):
            break    # 真实错误/空间不足不在这里反复重试
        newly_missing = 0
        for row in rows:
            state = db.get_download(str(row["episode_id"]))
            if state and state["status"] in MISSING_STATUSES and str(row["episode_id"]) not in skip_ids:
                skip_ids.add(str(row["episode_id"]))
                newly_missing += 1
        if not newly_missing:
            break
        logging.info("第 %s 轮有 %s 条因缺少必需文件被跳过，改选后面的数据补齐。", round_no, newly_missing)
    else:
        logging.warning("已达到最大补齐轮数 %s，可能仍有任务未补满；再次运行脚本会继续。", MAX_FILL_ROUNDS)
    return total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MinIO 任务数据下载（按时长）")
    parser.add_argument("--cleanup-missing-mcap", action="store_true",
                        help="先删除本地已下载但缺少 mcap 的 episode 目录并清除 SQLite 记录，再按正常流程下载补齐")
    parser.add_argument("--dry-run", action="store_true",
                        help="与 --cleanup-missing-mcap 联用：只列出将被删除的目录，不删除、不下载")
    args = parser.parse_args()
    if args.dry_run and not args.cleanup_missing_mcap:
        parser.error("--dry-run 必须与 --cleanup-missing-mcap 一起使用")
    if args.cleanup_missing_mcap and not REQUIRE_MCAP:
        parser.error("REQUIRE_MCAP 已关闭，无法清理缺少 mcap 的数据（请先开启 MINIO_REQUIRE_MCAP=1）")
    return args


def main() -> int:
    args = parse_args()
    setup_logging()
    logging.info("========== MinIO task downloader 启动 ==========")
    logging.info("任务列表：%s", ", ".join(TASK_NAMES))
    logging.info("历史任务名归并规则：%s", TASK_NAME_ALIASES)
    logging.info("SQLite 状态库：%s", STATE_DB)
    logging.info("日志目录：%s", LOG_DIR)

    lock_handle = LOCK_FILE.open("w", encoding="utf-8")
    try:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            logging.error("检测到另一个脚本实例正在运行，退出。")
            return 2

        verify_mc()
        logging.info("mcap 检查：%s（匹配规则 %s）", "开启" if REQUIRE_MCAP else "关闭", MCAP_FILE_PATTERN)
        disks = build_disk_configs()
        for disk in disks:
            usage = shutil.disk_usage(disk["path"])
            logging.info("目标磁盘 %s: path=%s free=%s total=%s mountpoint=%s",
                         disk["name"], disk["path"], human_bytes(usage.free),
                         human_bytes(usage.total), disk["mountpoint"])

        db = StateDB(STATE_DB)
        try:
            log_downloaded_summary(db, "启动时")
            if args.cleanup_missing_mcap:
                cleanup_missing_mcap(disks, db, dry_run=args.dry_run)
                if args.dry_run:
                    logging.info("dry-run 结束：未删除任何目录，也未下载。确认无误后去掉 --dry-run 再运行。")
                    return 0
                log_downloaded_summary(db, "清理后")
            all_rows = fetch_rows()
            counts = run_fill_rounds(all_rows, disks, db)
            logging.info("全部候选记录处理完成。统计：%s", counts)
            log_downloaded_summary(db, "结束时")
        finally:
            db.close()

        logging.info("========== 执行结束 ==========")
        return 1 if counts.get("failed", 0) else 0
    except NoSpaceAvailable as exc:
        logging.error("%s", exc)
        return 3
    except Exception:
        logging.exception("脚本异常退出")
        return 1
    finally:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        lock_handle.close()


if __name__ == "__main__":
    sys.exit(main())
