#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MinIO 下载器 Web 控制台（Flask 后端）。

- 下载由 minio_task_downloader_by_count.py 完成（minio SDK，不调用 mc），这里以子进程方式启动它；
- 所有配置（MinIO / PostgreSQL 连接、运行参数、任务、磁盘、访问口令）都在 SQLite 配置库里，由本页面读写；
- “暂停”= 在状态库写入 stop_requested，下载器在数据块边界安全停下并保存断点，再次“开始”即从断点续传；
- 进度读取下载器维护的状态库（只读），日志读取下载器的日志文件。

运行：
    pip install -r requirements.txt
    python web_app.py                   # 默认 127.0.0.1:8765；监听其它地址前必须先设置访问口令
    python web_app.py --set-token 口令   # 设置（或修改）访问口令后退出
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
import webbrowser
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request, send_file

APP_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(APP_DIR))
import compat                                      # noqa: E402
import config_db                                   # noqa: E402
import minio_store as store                        # noqa: E402
import minio_task_downloader_by_count as dl        # noqa: E402  （需要 psycopg2，与下载脚本同一环境）

SCRIPT = Path(dl.__file__).resolve()
try:
    APP_VERSION = (APP_DIR / "VERSION").read_text(encoding="utf-8").strip()
except OSError:
    APP_VERSION = "dev"

app = Flask(__name__)
app.json.ensure_ascii = False

run_lock = threading.Lock()
child: subprocess.Popen | None = None
child_info: dict[str, Any] = {}          # 本进程启动的那次运行：mode / pid / exit_code / stderr_tail


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ================================ 配置库 ================================
_cs_cache: dict[str, config_db.ConfigStore] = {}


def get_cs() -> config_db.ConfigStore:
    """配置库（首次使用时自动用默认值 / 旧配置 / 环境变量初始化）。ConfigStore 用短连接，线程安全。"""
    key = str(dl.CONFIG_DB)
    cs = _cs_cache.get(key)
    if cs is None:
        cs = _cs_cache[key] = dl.open_config_store()
    return cs


def snapshot() -> dict[str, Any]:
    return dl.config_snapshot(get_cs())


def stderr_file() -> Path:
    return dl.LOG_DIR / "web_last_run.stderr"


# ================================ 安全 ================================
# 目标：1) 没设口令时只有本机浏览器能用，且别的网页不能借用户的浏览器去调用本机接口（CSRF / DNS 重绑定）；
#       2) 设了口令后，口令校验有失败次数限制；3) 页面带基本的安全响应头。
_token_ok: set[str] = set()              # 已通过校验的口令的 sha256（PBKDF2 较慢，别每次请求都算）
_token_lock = threading.Lock()
_failures: dict[str, deque] = defaultdict(deque)     # 远端地址 → 最近的口令错误时间
MAX_FAILURES, FAILURE_WINDOW, LOCKOUT_SECONDS = 8, 300.0, 60.0
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


def _forget_tokens() -> None:
    with _token_lock:
        _token_ok.clear()


def _host_name() -> str:
    host = request.host or ""
    return host.rsplit(":", 1)[0].strip("[]") if not host.endswith("]") else host.strip("[]")


def _locked_out(addr: str) -> float:
    """该地址仍被锁定的剩余秒数（0 = 未锁定）。"""
    now = time.monotonic()
    q = _failures[addr]
    while q and now - q[0] > FAILURE_WINDOW:
        q.popleft()
    if len(q) >= MAX_FAILURES:
        return max(0.0, LOCKOUT_SECONDS - (now - q[-1]))
    return 0.0


@app.before_request
def guard():
    if not request.path.startswith("/api/"):
        return None
    # 1) 写操作必须是 application/json：跨站网页发不出带这个类型的“简单请求”（会先做 CORS 预检，而本服务不放行）
    if request.method in ("POST", "PUT", "DELETE", "PATCH") and request.mimetype != "application/json":
        return jsonify(error="写操作必须使用 Content-Type: application/json"), 415
    # 2) 浏览器带了 Origin 就必须与当前站点同源
    origin = request.headers.get("Origin")
    if origin and origin.split("://", 1)[-1] != request.host:
        return jsonify(error="拒绝跨站请求"), 403
    cs = get_cs()
    if not cs.has_web_token():
        # 没设口令：只允许本机直连；经过反向代理（带 X-Forwarded-For）的一律拒绝，避免意外对外开放
        if request.remote_addr not in ("127.0.0.1", "::1") or "X-Forwarded-For" in request.headers:
            return jsonify(error="尚未设置访问口令，目前只允许本机访问。请先在本机打开控制台的“参数”页设置口令。"), 403
        if _host_name() not in LOCAL_HOSTS:      # 防 DNS 重绑定：恶意域名解析到 127.0.0.1 时，Host 不会是 localhost
            return jsonify(error="没设访问口令时只能通过 localhost / 127.0.0.1 访问"), 403
        return None
    addr = request.remote_addr or "?"
    wait = _locked_out(addr)
    if wait:
        resp = jsonify(error=f"口令错误次数过多，请 {int(wait) + 1} 秒后再试")
        resp.status_code = 429
        resp.headers["Retry-After"] = str(int(wait) + 1)
        return resp
    supplied = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    digest = hashlib.sha256(supplied.encode("utf-8")).hexdigest()
    with _token_lock:
        if digest in _token_ok:
            return None
    if supplied and cs.verify_web_token(supplied):
        with _token_lock:
            _token_ok.add(digest)
        _failures.pop(addr, None)
        return None
    if supplied:                                  # 没带口令（页面刚打开、还没输入）不算一次失败
        _failures[addr].append(time.monotonic())
    return jsonify(error="口令不正确"), 401


@app.after_request
def security_headers(resp):
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("Referrer-Policy", "no-referrer")
    if request.path.startswith("/api/"):
        resp.headers["Cache-Control"] = "no-store"
    else:
        resp.headers.setdefault("Content-Security-Policy",
                                "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; "
                                "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
    return resp


# ============================ 下载器进程控制 ============================
def downloader_state() -> dict[str, Any]:
    """本 Web 启动的子进程，或命令行启动的下载器（靠 PID 文件识别，进程创建时间防止 PID 复用）。"""
    if child is not None and child.poll() is None:
        return {"running": True, "pid": child.pid, "by_web": True}
    info = compat.read_pidfile(dl.PID_FILE)
    if info and compat.process_alive(info["pid"], info.get("created")):
        return {"running": True, "pid": info["pid"], "by_web": False}
    return {"running": False, "pid": None, "by_web": False}


MODES = {
    "download": [],
    "cleanup_dry": ["--cleanup-missing-mcap", "--dry-run"],
    "cleanup": ["--cleanup-missing-mcap"],
}


def _wait_child(proc: subprocess.Popen) -> None:
    code = proc.wait()
    child_info.update(finished_at=now_iso(), exit_code=code)
    if code != 0:
        try:
            tail = stderr_file().read_text(encoding="utf-8", errors="replace")[-2000:]
        except OSError:
            tail = ""
        child_info["stderr_tail"] = tail.strip()


def setup_problems(snap: dict[str, Any]) -> list[dict[str, str]]:
    """开始下载前必须先配好的东西。"""
    st, problems = snap["settings"], []
    if not (st["minio.endpoint"] and st["minio.access_key"] and st["minio.secret_key"]):
        problems.append({"page": "connection", "text": "尚未配置 MinIO 连接（服务地址 / Access Key / Secret Key）"})
    if not snap["disks"]:
        problems.append({"page": "disks", "text": "尚未配置目标磁盘"})
    if not any(tasks for _, tasks in snap["scene_tasks"]):
        problems.append({"page": "tasks", "text": "尚未配置任何下载任务"})
    return problems


@app.post("/api/run")
def api_run():
    global child
    body = request.get_json(silent=True) or {}
    mode = body.get("mode", "download")
    if mode not in MODES:
        return jsonify(error=f"未知模式：{mode}"), 400
    snap = snapshot()
    problems = setup_problems(snap)
    if problems:
        return jsonify(error="；".join(p["text"] for p in problems), problems=problems), 400
    try:
        max_rounds = int(body.get("max_rounds") or snap["settings"]["run.default_rounds"])
    except (TypeError, ValueError):
        return jsonify(error="补齐轮数必须是整数"), 400
    if not 1 <= max_rounds <= 50:
        return jsonify(error="补齐轮数范围 1–50"), 400

    args = [*MODES[mode], "--max-rounds", str(max_rounds)]
    if body.get("recheck_missing"):
        args.append("--recheck-missing")

    with run_lock:
        state = downloader_state()
        if state["running"]:
            return jsonify(error=f"下载器已在运行（PID {state['pid']}）"), 409
        dl.LOG_DIR.mkdir(parents=True, exist_ok=True)
        db = dl.StateDB(dl.STATE_DB)
        try:
            db.clear_control("stop_requested")
        finally:
            db.close()
        env = {**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
        with stderr_file().open("w", encoding="utf-8") as err:
            child = subprocess.Popen(
                [sys.executable, str(SCRIPT), *args], cwd=str(APP_DIR), env=env,
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=err,
                **compat.detached_popen_kwargs(),      # 独立会话 / 进程组：关闭网页或终端不会连带结束下载
            )
        child_info.clear()
        child_info.update(mode=mode, args=args, started_at=now_iso(), pid=child.pid)
        threading.Thread(target=_wait_child, args=(child,), daemon=True).start()
    return jsonify(ok=True, pid=child.pid)


@app.post("/api/stop")
def api_stop():
    """默认 = 暂停：下载器在当前数据块写完后安全停下并保存断点，再次开始即续传。force=true 才直接结束进程。"""
    body = request.get_json(silent=True) or {}
    state = downloader_state()
    if not state["running"]:
        return jsonify(error="当前没有运行中的下载器"), 409
    if body.get("force"):
        try:
            compat.kill_tree(state["pid"], force=True)
        except PermissionError:
            return jsonify(error="没有权限结束该进程（它由其他用户启动）"), 403
        return jsonify(ok=True, mode="force")
    db = dl.StateDB(dl.STATE_DB)
    try:
        db.set_control("stop_requested", now_iso())
    finally:
        db.close()
    return jsonify(ok=True, mode="pause")


# ================================ 进度统计 ================================
def _open_ro() -> sqlite3.Connection | None:
    if not Path(dl.STATE_DB).is_file():
        return None
    conn = sqlite3.connect(Path(dl.STATE_DB).resolve().as_uri() + "?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


_cache: dict[str, Any] = {"at": 0.0, "key": None, "data": None}
_cache_lock = threading.Lock()
CACHE_SECONDS = 4.0          # 下载中：进度变化快
CACHE_IDLE_SECONDS = 15.0    # 空闲：统计要逐条检查本地目录是否还在，不必太频繁


def _collect(snap: dict[str, Any]) -> dict[str, Any]:
    downloaded: dict[str, dict[str, Any]] = {}
    statuses: dict[str, dict[str, int]] = {}
    assignments: dict[str, str] = {}
    stale = 0

    conn = _open_ro()
    if conn is not None:
        try:
            records = conn.execute(
                """
                SELECT episode_id, task_name, target_path, content_time, file_size
                  FROM downloaded_episodes WHERE target_path IS NOT NULL
                UNION ALL
                SELECT d.episode_id, d.task_name, d.target_path, NULL, d.expected_bytes
                  FROM downloads d
                 WHERE d.status = 'completed' AND d.target_path IS NOT NULL
                   AND d.episode_id NOT IN (SELECT episode_id FROM downloaded_episodes)
                """
            ).fetchall()
            seen: set[str] = set()
            for r in records:
                if r["episode_id"] in seen:
                    continue
                seen.add(r["episode_id"])
                if not os.path.isdir(r["target_path"]):          # 记录在、目录已被删除：不计入
                    stale += 1
                    continue
                e = downloaded.setdefault(r["task_name"], {"n": 0, "seconds": 0.0, "bytes": 0})
                e["n"] += 1
                e["seconds"] += float(r["content_time"] or 0)
                e["bytes"] += int(r["file_size"] or 0)
            for r in conn.execute("SELECT task_name, status, COUNT(*) AS n FROM downloads GROUP BY task_name, status"):
                statuses.setdefault(r["task_name"], {})[r["status"]] = r["n"]
            for r in conn.execute("SELECT task_name, disk_name FROM task_disk_assignments"):
                assignments[r["task_name"]] = r["disk_name"]
        except sqlite3.OperationalError:
            pass    # 状态库还没建表（下载器从未运行过）
        finally:
            conn.close()

    scenes = []
    tag_map = snap.get("tags", {})
    for scene, tasks in snap["scene_tasks"]:
        rows = []
        for name, target, aliases, *extra in tasks:
            d = downloaded.get(name, {"n": 0, "seconds": 0.0, "bytes": 0})
            issues = {k: v for k, v in statuses.get(name, {}).items() if k != "completed"}
            rows.append({
                "name": name, "dir": (extra[0] if extra and extra[0] else name),
                "target": target, "aliases": list(aliases),
                "downloaded": d["n"], "hours": round(d["seconds"] / 3600.0, 2), "bytes": d["bytes"],
                "issues": issues, "disk": assignments.get(name), "tags": tag_map.get(name, []),
            })
        scenes.append({"scene": scene, "tasks": rows})

    disks = []
    for d in snap["disks"]:
        item = {"name": d["name"], "path": d["path"], "mounted": os.path.ismount(d["mountpoint"]),
                "tasks": sorted(t for t, dn in assignments.items() if dn == d["name"])}
        probe = next((p for p in (d["path"], d["mountpoint"]) if os.path.exists(p)), None)
        try:
            usage = shutil.disk_usage(probe) if probe else None
        except OSError:
            usage = None
        item.update(total=usage.total if usage else 0, free=usage.free if usage else 0,
                    used=(usage.total - usage.free) if usage else 0, exists=bool(probe))
        disks.append(item)

    return {"scenes": scenes, "disks": disks, "stale_records": stale, "data_version": snap["data_version"]}


def _collect_live() -> tuple[list[dict[str, Any]], dict[str, str]]:
    """每次请求都实时读取（便宜）：正在下载的明细（含已写入字节数，前端据此算速度）与运行控制项。"""
    running: list[dict[str, Any]] = []
    control: dict[str, str] = {}
    conn = _open_ro()
    if conn is None:
        return running, control
    try:
        for r in conn.execute(
            "SELECT episode_id, task_name, disk_name, started_at, expected_bytes, partial_path FROM downloads "
            "WHERE status = 'running' ORDER BY started_at DESC LIMIT 24"):
            item = dict(r)
            partial = item.pop("partial_path", None)
            item["partial_bytes"] = dl.directory_size_bytes(Path(partial)) if partial and os.path.isdir(partial) else 0
            running.append(item)
        for r in conn.execute("SELECT key, value FROM control"):
            control[r["key"]] = r["value"]
    except sqlite3.OperationalError:
        pass
    finally:
        conn.close()
    return running, control


@app.get("/api/status")
def api_status():
    snap = snapshot()
    state = downloader_state()
    key = (snap["data_version"], str(snap["scene_tasks"]), str(snap["disks"]), str(snap.get("tags")))
    ttl = CACHE_SECONDS if state["running"] else CACHE_IDLE_SECONDS
    with _cache_lock:
        fresh = request.args.get("fresh") or key != _cache["key"] or time.monotonic() - _cache["at"] > ttl
        if fresh:
            _cache.update(data=_collect(snap), at=time.monotonic(), key=key)
        data = dict(_cache["data"])
    running, control = _collect_live()
    data["running_episodes"] = running if state["running"] else []     # 残留的 running 记录是上次被中断的，不是正在下载
    st = snap["settings"]
    try:
        endpoint = store.config_from_settings(st).describe()
    except store.StoreError:
        endpoint = ""
    data.update(
        version=APP_VERSION,
        downloader=state,
        stop_requested=bool(state["running"] and control.get("stop_requested")),
        setup_issues=setup_problems(snap),
        minio={"endpoint": endpoint, "bucket": st["minio.bucket"]},
        settings={"min_free_gb": st["run.min_free_gb"], "writes_per_disk": st["run.max_writes_per_disk"],
                  "require_mcap": st["run.require_mcap"], "mcap_pattern": st["run.mcap_pattern"],
                  "default_rounds": st["run.default_rounds"]},
        last_run=_last_run(control, state),
    )
    return jsonify(data)


def _last_run(control: dict[str, str], state: dict[str, Any]) -> dict[str, Any]:
    """上次运行结果：优先用下载器写入状态库的（命令行启动的也有）；本 Web 启动且异常退出的补充 stderr。"""
    info: dict[str, Any] = {}
    if control.get("last_run"):
        try:
            info = json.loads(control["last_run"])
        except ValueError:
            info = {}
    if child_info.get("finished_at") and child_info.get("exit_code") not in (0, None) and not state["running"]:
        if info.get("result") != "error" or not info.get("error"):
            info = {"result": "error", "at": child_info["finished_at"], "exit_code": child_info["exit_code"],
                    "error": child_info.get("stderr_tail") or f"进程异常退出，退出码 {child_info['exit_code']}"}
    return info


@app.get("/api/episodes")
def api_episodes():
    """某个任务下尚未完成的数据及原因（缺 mcap、失败、暂停、空间不足……）。"""
    task = request.args.get("task", "")
    conn = _open_ro()
    if conn is None:
        return jsonify(episodes=[])
    try:
        rows = conn.execute(
            "SELECT episode_id, status, error, parent_key, updated_at FROM downloads "
            "WHERE task_name = ? AND status <> 'completed' ORDER BY updated_at DESC LIMIT 200", (task,)
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    finally:
        conn.close()
    return jsonify(episodes=[dict(r) for r in rows])


# ================================ 日志 ================================
@app.get("/api/logs")
def api_logs():
    path = dl.LOG_DIR / "minio_task_downloader.log"
    try:
        size = path.stat().st_size
    except OSError:
        return jsonify(text="", offset=0, reset=True)
    offset = request.args.get("offset", type=int)
    initial = offset is None or offset < 0 or offset > size       # 首次加载或日志被轮转
    start = max(0, size - 96 * 1024) if initial else offset
    with path.open("rb") as f:
        f.seek(start)
        chunk = f.read(512 * 1024)
    if initial and start > 0:
        chunk = chunk.split(b"\n", 1)[-1]                          # 丢掉被截断的首行
        start = size - len(chunk)
    cut = chunk.rfind(b"\n") + 1                                   # 只返回完整的行
    chunk = chunk[:cut]
    return jsonify(text=chunk.decode("utf-8", "replace"), offset=start + len(chunk), reset=initial)


# ================================ 配置 ================================
@app.get("/api/config")
def api_config_get():
    cs = get_cs()
    values, secrets_set = cs.public_settings()
    return jsonify(
        schema=config_db.schema(), groups=config_db.GROUPS, values=values, secrets_set=secrets_set,
        tasks=cs.get_tasks_dict(), disks=cs.get_disks(), token_set=cs.has_web_token(),
        paths={"config_db": str(dl.CONFIG_DB), "state_db": str(dl.STATE_DB), "log_dir": str(dl.LOG_DIR)},
        platform="windows" if compat.IS_WINDOWS else "posix", running=downloader_state()["running"],
    )


def _bad(message: str, code: int = 400):
    return jsonify(error=message), code


def _check_tasks(payload: dict[str, Any], need_version: bool = False) -> tuple[str, list, dict[str, list[str]]]:
    """解析并校验一份任务 JSON（重名、别名冲突、目录冲突、条数非法…）。不合法抛 ValueError。"""
    try:
        data_version, scene_tasks, tags = config_db.tasks_from_dict(payload)
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError(f"任务内容格式不对：{exc!r}") from exc
    if need_version and not data_version:
        raise ValueError("版本目录不能为空")
    for scene, tasks in scene_tasks:
        if not scene:
            raise ValueError("场景名不能为空")
        for t in tasks:
            if not t[0]:
                raise ValueError(f"场景 {scene!r} 下有任务名为空")
    dl._build_task_config(scene_tasks)
    return data_version, scene_tasks, tags


@app.put("/api/config/tasks")
def api_config_tasks():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return _bad("请求内容不是有效的 JSON")
    try:
        data_version, scene_tasks, tags = _check_tasks(body, need_version=True)
        get_cs().set_tasks(data_version, scene_tasks, tags)
    except ValueError as exc:
        return _bad(str(exc.args[0]) if exc.args else str(exc))
    return jsonify(ok=True, running=downloader_state()["running"])


@app.post("/api/config/tasks/reset")
def api_config_tasks_reset():
    dv, st = dl.tasks_from_json(dl.DEFAULT_TASKS_JSON)
    get_cs().set_tasks(dv, st)                    # 不传 tags：同名任务的标签保留
    return jsonify(ok=True)


# ================================ 任务方案（可复用的任务集合） ================================
def _plan_or_404(plan_id: int):
    plan = get_cs().get_plan(plan_id)
    return plan, (None if plan else (jsonify(error="方案不存在或已被删除"), 404))


@app.get("/api/plans")
def api_plans_list():
    cs = get_cs()
    return jsonify(plans=cs.list_plans(), current=config_db.plan_summary(cs.get_tasks_dict()))


@app.get("/api/plans/<int:plan_id>")
def api_plan_get(plan_id: int):
    plan, err = _plan_or_404(plan_id)
    return err or jsonify(plan)


@app.post("/api/plans")
def api_plan_save():
    """把任务配置存成方案。body: name, note?, payload?（不给则用当前已保存的配置；给了就是编辑器里未保存的内容）, overwrite?"""
    body = request.get_json(silent=True) or {}
    cs = get_cs()
    payload = body.get("payload") if isinstance(body.get("payload"), dict) else cs.get_tasks_dict()
    try:
        _check_tasks(payload)
        plan_id = cs.save_plan(str(body.get("name", "")), payload, str(body.get("note", "")), bool(body.get("overwrite")))
    except ValueError as exc:
        msg = str(exc.args[0]) if exc.args else str(exc)
        return jsonify(error=msg, code="exists" if msg.startswith("已有同名方案") else "invalid"), (409 if msg.startswith("已有同名方案") else 400)
    return jsonify(ok=True, id=plan_id)


@app.put("/api/plans/<int:plan_id>")
def api_plan_update(plan_id: int):
    """改名 / 改备注；payload 给了就用它覆盖方案内容，from_current=true 则用当前已保存的配置覆盖。"""
    body = request.get_json(silent=True) or {}
    cs = get_cs()
    payload = body.get("payload") if isinstance(body.get("payload"), dict) else (cs.get_tasks_dict() if body.get("from_current") else None)
    try:
        if payload is not None:
            _check_tasks(payload)
        found = cs.update_plan(plan_id, name=body.get("name"), note=body.get("note"), payload=payload)
    except ValueError as exc:
        msg = str(exc.args[0]) if exc.args else str(exc)
        return jsonify(error=msg), (409 if msg.startswith("已有同名方案") else 400)
    return jsonify(ok=True) if found else (jsonify(error="方案不存在或已被删除"), 404)


@app.delete("/api/plans/<int:plan_id>")
def api_plan_delete(plan_id: int):
    return jsonify(ok=True) if get_cs().delete_plan(plan_id) else (jsonify(error="方案不存在或已被删除"), 404)


@app.post("/api/plans/<int:plan_id>/apply")
def api_plan_apply(plan_id: int):
    """载入方案。mode=replace：用方案替换当前任务配置；mode=merge：只追加当前没有的任务。tags：只取带这些标签的任务。"""
    body = request.get_json(silent=True) or {}
    mode = body.get("mode", "replace")
    if mode not in ("replace", "merge"):
        return _bad("mode 只能是 replace 或 merge")
    plan, err = _plan_or_404(plan_id)
    if err:
        return err
    try:
        tags = config_db.normalize_tags(body.get("tags"))
        incoming = config_db.filter_by_tags(plan["payload"], tags)
        if not plan_has_tasks(incoming):
            return _bad("方案里没有符合所选标签的任务" if tags else "这个方案是空的")
        cs = get_cs()
        current = cs.get_tasks_dict()
        report: dict[str, Any] = {"added": [], "skipped": []}
        if mode == "replace":
            result = {**incoming, "data_version": incoming.get("data_version") or current["data_version"]}
            data_version, scene_tasks, task_tags = _check_tasks(result, need_version=True)
            report["added"] = [t["name"] for s in incoming["scenes"] for t in s["tasks"]]
        else:
            merged, report = config_db.merge_payloads(current, incoming, lambda d: _check_tasks(d))
            data_version, scene_tasks, task_tags = _check_tasks(merged, need_version=True)
        if mode == "replace" or report["added"]:
            cs.set_tasks(data_version, scene_tasks, task_tags)
    except ValueError as exc:
        return _bad(str(exc.args[0]) if exc.args else str(exc))
    return jsonify(ok=True, mode=mode, added=len(report["added"]), skipped=report["skipped"],
                   total_tasks=sum(len(t) for _, t in scene_tasks), running=downloader_state()["running"])


def plan_has_tasks(payload: dict[str, Any]) -> bool:
    return any(s.get("tasks") for s in payload.get("scenes", []))


@app.get("/api/plans/<int:plan_id>/export")
def api_plan_export(plan_id: int):
    plan, err = _plan_or_404(plan_id)
    if err:
        return err
    doc = {"format": "minio-downloader-plan", "version": 1, "name": plan["name"], "note": plan["note"], "payload": plan["payload"]}
    resp = jsonify(doc)
    safe = "".join(c if c.isalnum() or c in "-_ " else "_" for c in plan["name"]).strip() or "plan"
    resp.headers["Content-Disposition"] = f"attachment; filename*=UTF-8''{urllib.parse.quote('方案-' + safe)}.json"
    return resp


@app.post("/api/plans/import")
def api_plan_import():
    """导入 /export 导出的方案文件（或只含 payload 的 JSON）。同名时自动加序号，不会覆盖已有方案。"""
    body = request.get_json(silent=True)
    doc = body.get("plan") if isinstance(body, dict) and isinstance(body.get("plan"), dict) else body
    if not isinstance(doc, dict) or not isinstance(doc.get("payload"), dict):
        return _bad("不是有效的方案文件（缺少 payload）")
    cs = get_cs()
    try:
        _check_tasks(doc["payload"])
        base = (str(doc.get("name") or "").strip() or "导入的方案")[:config_db.MAX_PLAN_NAME - 6]
        names = {p["name"] for p in cs.list_plans()}
        name, n = base, 1
        while name in names:
            n += 1
            name = f"{base} ({n})"
        plan_id = cs.save_plan(name, doc["payload"], str(doc.get("note") or ""))
    except ValueError as exc:
        return _bad(str(exc.args[0]) if exc.args else str(exc))
    return jsonify(ok=True, id=plan_id, name=name)


@app.put("/api/config/disks")
def api_config_disks():
    body = request.get_json(silent=True)
    disks = body.get("disks") if isinstance(body, dict) else None
    if not isinstance(disks, list):
        return _bad("请求内容不是有效的 JSON")
    try:
        get_cs().set_disks(disks)
    except ValueError as exc:
        return _bad(str(exc))
    return jsonify(ok=True, running=downloader_state()["running"])


def _check_disks(disks: list[dict[str, str]]) -> list[dict[str, Any]]:
    """不改动任何东西，只检查：路径 / 挂载点是否存在、是否真的挂载、是否可写、剩余空间、是否与其它条目是同一文件系统。"""
    results: list[dict[str, Any]] = []
    devices: dict[int, str] = {}
    for d in disks:
        name, path = d.get("name", ""), d.get("path", "")
        mount = d.get("mountpoint") or path
        notes: list[tuple[str, str]] = []
        ok = True
        if not os.path.isdir(mount):
            ok = False
            notes.append(("bad", f"挂载点不存在：{mount}"))
        elif not os.path.ismount(mount):
            ok = False
            notes.append(("bad", f"挂载点没有真正挂载：{mount}（为避免写入系统盘，下载器会拒绝启动）"))
        else:
            notes.append(("ok", "挂载点正常"))
        try:
            a, b = os.path.normcase(os.path.abspath(mount)), os.path.normcase(os.path.abspath(path))
            if os.path.commonpath([a, b]) != a:
                ok = False
                notes.append(("bad", "目标路径不在挂载点之下"))
        except ValueError:
            ok = False
            notes.append(("bad", "目标路径与挂载点不在同一个盘符"))
        probe = path
        while probe and not os.path.exists(probe) and os.path.dirname(probe) != probe:
            probe = os.path.dirname(probe)
        free = total = 0
        if probe and os.path.exists(probe):
            try:
                usage = shutil.disk_usage(probe)
                free, total = usage.free, usage.total
                dev = os.stat(probe).st_dev
                if dev and dev in devices:
                    ok = False
                    notes.append(("bad", f"与 {devices[dev]} 在同一个文件系统上，容量会被重复计算"))
                devices.setdefault(dev, name)
            except OSError as exc:
                ok = False
                notes.append(("bad", f"无法读取磁盘信息：{exc}"))
            if not os.access(probe, os.W_OK):
                ok = False
                notes.append(("bad", "没有写入权限"))
        else:
            ok = False
            notes.append(("bad", "路径不存在"))
        results.append({"name": name, "ok": ok, "notes": [{"level": lv, "text": tx} for lv, tx in notes],
                        "free": free, "total": total})
    return results


@app.post("/api/config/disks/check")
def api_disks_check():
    body = request.get_json(silent=True)
    disks = body["disks"] if isinstance(body, dict) and isinstance(body.get("disks"), list) else get_cs().get_disks()
    return jsonify(results=_check_disks(disks))


@app.put("/api/config/settings")
def api_config_settings():
    body = request.get_json(silent=True)
    values = body.get("values") if isinstance(body, dict) else None
    if not isinstance(values, dict):
        return _bad("请求内容不是有效的 JSON")
    try:
        saved = get_cs().set_settings(values)
    except ValueError as exc:
        return _bad(str(exc))
    return jsonify(ok=True, saved=saved, running=downloader_state()["running"])


@app.post("/api/config/test")
def api_config_test():
    """用（表单里尚未保存的）值测试连接；密钥留空则沿用已保存的。"""
    body = request.get_json(silent=True) or {}
    target = body.get("target")
    merged = get_cs().get_settings()
    for key, raw in (body.get("values") or {}).items():
        setting = config_db.BY_KEY.get(key)
        if setting is None or (setting.secret and raw is None):
            continue
        try:
            merged[key] = config_db.coerce(setting, raw)
        except ValueError as exc:
            return jsonify(ok=False, message=str(exc))
    try:
        if target == "minio":
            cfg = store.config_from_settings(merged)
            return jsonify(ok=True, message="连接成功：" + store.test_connection(cfg, merged["minio.bucket"]))
        if target == "pg":
            import psycopg2
            conn = psycopg2.connect(host=merged["pg.host"], port=merged["pg.port"], dbname=merged["pg.database"],
                                    user=merged["pg.user"], password=merged["pg.password"], connect_timeout=5)
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT to_regclass('public.minio_galbot_scene_task_jobs') IS NOT NULL")
                    has_table = bool(cur.fetchone()[0])
            finally:
                conn.close()
            msg = "连接成功" + ("，已找到任务数据表" if has_table else "，但没有找到表 public.minio_galbot_scene_task_jobs")
            return jsonify(ok=has_table, message=msg)
        return _bad("未知的测试目标")
    except Exception as exc:                          # 连接类错误都以 200 + ok=false 返回，前端直接显示原因
        return jsonify(ok=False, message=f"{type(exc).__name__}: {exc}".strip()[:600])


@app.put("/api/config/token")
def api_token_set():
    body = request.get_json(silent=True) or {}
    try:
        get_cs().set_web_token(str(body.get("token", "")))
    except ValueError as exc:
        return _bad(str(exc))
    _forget_tokens()
    return jsonify(ok=True)


@app.delete("/api/config/token")
def api_token_clear():
    get_cs().clear_web_token()
    _forget_tokens()
    return jsonify(ok=True)


# ================================ 页面 ================================
@app.get("/")
def index():
    return send_file(APP_DIR / "index.html")


@app.get("/healthz")
def healthz():
    """给监控 / 负载均衡用：不需要口令，不含任何配置信息。"""
    return jsonify(ok=True, version=APP_VERSION)


def main() -> int:
    parser = argparse.ArgumentParser(description="MinIO 下载器 Web 控制台")
    parser.add_argument("--host", help="监听地址（默认取配置库，初始 127.0.0.1）")
    parser.add_argument("--port", type=int, help="监听端口（默认取配置库，初始 8765）")
    parser.add_argument("--open", action="store_true", help="启动后用默认浏览器打开控制台")
    parser.add_argument("--set-token", metavar="口令", help="设置（或修改）访问口令后退出")
    args = parser.parse_args()

    cs = get_cs()
    if args.set_token:
        try:
            cs.set_web_token(args.set_token)
        except ValueError as exc:
            print(f"失败：{exc}", file=sys.stderr)
            return 2
        print("访问口令已更新。")
        return 0
    seed_token = os.getenv("MINIO_WEB_TOKEN", "")
    if seed_token and not cs.has_web_token():
        try:
            cs.set_web_token(seed_token)
            print("已用环境变量 MINIO_WEB_TOKEN 初始化访问口令（之后请在“参数”页修改）。")
        except ValueError as exc:
            print(f"忽略 MINIO_WEB_TOKEN：{exc}", file=sys.stderr)

    values = cs.get_settings()
    host = args.host or values["web.host"]
    port = args.port or values["web.port"]
    if host not in ("127.0.0.1", "localhost", "::1") and not cs.has_web_token():
        print("监听非本机地址之前必须先设置访问口令（页面能启动下载并删除本地数据）：\n"
              "    python web_app.py --set-token 你的口令", file=sys.stderr)
        return 2
    dl.LOG_DIR.mkdir(parents=True, exist_ok=True)
    url = f"http://{'127.0.0.1' if host in ('0.0.0.0', '::') else host}:{port}/"
    print(f"控制台地址：{url}")
    if args.open:
        threading.Timer(1.0, webbrowser.open, [url]).start()
    try:
        from waitress import serve
    except ImportError:
        print("提示：未安装 waitress，使用 Flask 自带的开发服务器（pip install waitress 可换成生产级服务器）。")
        app.run(host=host, port=port, threaded=True)
    else:
        serve(app, host=host, port=port, threads=8, ident="minio-downloader")
    return 0


if __name__ == "__main__":
    sys.exit(main())
