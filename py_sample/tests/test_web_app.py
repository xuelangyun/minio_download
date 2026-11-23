# -*- coding: utf-8 -*-
"""Web 接口测试：配置读写（SQLite）、密钥不外泄、口令鉴权、开始 / 暂停 / 强制结束、进度统计、日志。"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    import psycopg2  # noqa: F401
except ImportError:
    sys.modules["psycopg2"] = types.ModuleType("psycopg2")

import compat                                    # noqa: E402
import minio_store as store                      # noqa: E402
import minio_task_downloader_by_count as dl      # noqa: E402
import web_app                                   # noqa: E402


class FakeProc:
    pid = 424242

    def __init__(self, *a, **k):
        self.args, self.kwargs = a, k

    def poll(self):
        return None

    def wait(self):
        import time
        time.sleep(30)


class WebTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        for p in (mock.patch.object(dl, "CONFIG_DB", root / "cfg.sqlite3"), mock.patch.object(dl, "STATE_DB", root / "state.sqlite3"),
                  mock.patch.object(dl, "LOG_DIR", root / "logs"), mock.patch.object(dl, "PID_FILE", root / "x.pid"),
                  mock.patch.object(dl, "LOCK_FILE", root / "x.lock")):
            p.start()
            self.addCleanup(p.stop)
        web_app._cs_cache.clear()
        web_app._cache.update(at=0.0, key=None, data=None)
        web_app._forget_tokens()
        web_app.child = None
        web_app.child_info.clear()
        self.addCleanup(lambda: setattr(web_app, "child", None))
        self.c = web_app.app.test_client()
        (root / "logs").mkdir()
        self.root = root

    def configure_minimum(self):
        """让“开始下载”的前置检查通过：MinIO 连接 + 一块磁盘 + 一个任务。"""
        self.assertEqual(self.c.put("/api/config/settings", json={"values": {
            "minio.endpoint": "http://127.0.0.1:9000", "minio.access_key": "ak", "minio.secret_key": "sk"}}).status_code, 200)
        disk = self.root / "disk"
        disk.mkdir(exist_ok=True)
        self.assertEqual(self.c.put("/api/config/disks", json={"disks": [{"name": "d1", "path": str(disk), "mountpoint": str(disk)}]}).status_code, 200)


class ConfigApiTests(WebTestCase):
    def test_first_open_is_seeded_and_secrets_never_leave_the_server(self):
        cfg = self.c.get("/api/config").get_json()
        self.assertGreater(sum(len(s["tasks"]) for s in cfg["tasks"]["scenes"]), 10)
        self.assertEqual(cfg["values"]["minio.secret_key"], "")
        self.c.put("/api/config/settings", json={"values": {"minio.secret_key": "TOPSECRET", "pg.password": "PGSECRET"}})
        raw = self.c.get("/api/config").get_data(as_text=True) + self.c.get("/api/status").get_data(as_text=True)
        self.assertNotIn("TOPSECRET", raw)
        self.assertNotIn("PGSECRET", raw)
        self.assertEqual(set(self.c.get("/api/config").get_json()["secrets_set"]), {"minio.secret_key", "pg.password"})

    def test_settings_validation_and_keep_secret_on_null(self):
        r = self.c.put("/api/config/settings", json={"values": {"pg.port": 70000}})
        self.assertEqual(r.status_code, 400)
        self.assertIn("端口", r.get_json()["error"])
        self.c.put("/api/config/settings", json={"values": {"minio.secret_key": "keepme"}})
        self.c.put("/api/config/settings", json={"values": {"minio.secret_key": None, "minio.bucket": "newb"}})
        self.assertEqual(web_app.get_cs().get_settings()["minio.secret_key"], "keepme")
        self.assertEqual(self.c.get("/api/config").get_json()["values"]["minio.bucket"], "newb")

    def test_tasks_roundtrip_validation_and_reset(self):
        body = {"data_version": "V9", "scenes": [{"scene": "S", "tasks": [
            {"name": "t1", "count": 3, "aliases": ["old"], "dir": ""}, {"name": "t2", "count": 0, "aliases": [], "dir": "d2"}]}]}
        self.assertEqual(self.c.put("/api/config/tasks", json=body).status_code, 200)
        got = self.c.get("/api/config").get_json()["tasks"]
        self.assertEqual(got["data_version"], "V9")
        self.assertEqual([t["name"] for t in got["scenes"][0]["tasks"]], ["t1", "t2"])
        for bad, needle in (({**body, "data_version": ""}, "版本目录"),
                            ({"data_version": "v", "scenes": [{"scene": "S", "tasks": [{"name": "a", "count": 1, "aliases": []}, {"name": "a", "count": 1, "aliases": []}]}]}, "重复"),
                            ({"data_version": "v", "scenes": [{"scene": "S", "tasks": [{"name": "a", "count": -1, "aliases": []}]}]}, "非负整数"),
                            ({"data_version": "v", "scenes": [{"scene": "S", "tasks": [{"name": "", "count": 1, "aliases": []}]}]}, "为空")):
            r = self.c.put("/api/config/tasks", json=bad)
            self.assertEqual(r.status_code, 400, bad)
            self.assertIn(needle, r.get_json()["error"])
        self.assertEqual(self.c.get("/api/config").get_json()["tasks"]["data_version"], "V9", "校验失败不能改动已保存的配置")
        self.assertEqual(self.c.post("/api/config/tasks/reset", json={}).status_code, 200)
        self.assertGreater(len(self.c.get("/api/config").get_json()["tasks"]["scenes"]), 1)

    def test_disks_save_and_check(self):
        d = self.root / "data"
        d.mkdir()
        self.assertEqual(self.c.put("/api/config/disks", json={"disks": [{"name": "d1", "path": str(d)}]}).status_code, 200)
        self.assertEqual(self.c.put("/api/config/disks", json={"disks": [{"name": "d1", "path": "/a"}, {"name": "d1", "path": "/b"}]}).status_code, 400)
        res = self.c.post("/api/config/disks/check", json={"disks": [{"name": "d1", "path": str(d), "mountpoint": str(d)},
                                                                      {"name": "ghost", "path": "/definitely/not/here"}]}).get_json()["results"]
        self.assertFalse(res[1]["ok"])
        self.assertTrue(any("不存在" in n["text"] for n in res[1]["notes"]))
        self.assertGreater(res[0]["total"], 0)

    def test_connection_tests_report_failures_instead_of_crashing(self):
        with mock.patch.object(store, "test_connection", side_effect=store.StoreError("凭据无效")):
            r = self.c.post("/api/config/test", json={"target": "minio", "values": {
                "minio.endpoint": "http://x:9000", "minio.access_key": "a", "minio.secret_key": "b"}}).get_json()
        self.assertFalse(r["ok"])
        self.assertIn("凭据无效", r["message"])
        r = self.c.post("/api/config/test", json={"target": "minio", "values": {"pg.port": "abc"}}).get_json()
        self.assertFalse(r["ok"])
        self.assertEqual(self.c.post("/api/config/test", json={"target": "nope"}).status_code, 400)
        # 未保存的表单值参与测试；密钥为 None 时沿用已保存的
        self.c.put("/api/config/settings", json={"values": {"minio.secret_key": "saved-secret"}})
        seen = {}
        with mock.patch.object(store, "test_connection", lambda cfg, bucket: seen.update(cfg=cfg, bucket=bucket) or "ok"):
            self.c.post("/api/config/test", json={"target": "minio", "values": {
                "minio.endpoint": "http://form:9000", "minio.access_key": "formak", "minio.secret_key": None, "minio.bucket": "bb"}})
        self.assertEqual((seen["cfg"].endpoint, seen["cfg"].access_key, seen["cfg"].secret_key, seen["bucket"]),
                         ("form:9000", "formak", "saved-secret", "bb"))


class AuthTests(WebTestCase):
    def test_open_on_localhost_until_a_token_is_set(self):
        self.assertEqual(self.c.get("/api/status").status_code, 200)
        remote = {"REMOTE_ADDR": "10.1.2.3"}
        self.assertEqual(self.c.get("/api/status", environ_base=remote).status_code, 403, "没设口令时远程访问必须被拒绝")
        self.assertEqual(self.c.get("/api/status", headers={"X-Forwarded-For": "8.8.8.8"}).status_code, 403, "反向代理后面也不能默认放行")

    def test_token_flow(self):
        self.assertEqual(self.c.put("/api/config/token", json={"token": "123"}).status_code, 400)
        self.assertEqual(self.c.put("/api/config/token", json={"token": "s3cret-token"}).status_code, 200)
        self.assertEqual(self.c.get("/api/status").status_code, 401)
        self.assertEqual(self.c.get("/api/status", headers={"Authorization": "Bearer nope"}).status_code, 401)
        ok = {"Authorization": "Bearer s3cret-token"}
        self.assertEqual(self.c.get("/api/status", headers=ok).status_code, 200)
        self.assertEqual(self.c.get("/api/status", headers=ok, environ_base={"REMOTE_ADDR": "10.1.2.3"}).status_code, 200)
        self.assertEqual(self.c.put("/api/config/token", json={"token": "another-token"}, headers=ok).status_code, 200)
        self.assertEqual(self.c.get("/api/status", headers=ok).status_code, 401, "改口令后旧口令立刻失效")
        self.assertEqual(self.c.delete("/api/config/token", headers={"Authorization": "Bearer another-token"}, json={}).status_code, 200)
        self.assertEqual(self.c.get("/api/status").status_code, 200)

    def test_index_page_is_served_without_a_token(self):
        self.c.put("/api/config/token", json={"token": "s3cret-token"})
        r = self.c.get("/")
        self.assertEqual(r.status_code, 200)
        self.assertIn("下载控制台", r.get_data(as_text=True))


class HardeningTests(WebTestCase):
    def test_writes_require_json_content_type(self):
        """跨站网页发不出 application/json 的“简单请求”，所以必须拒绝其它类型（防 CSRF）。"""
        r = self.c.post("/api/run", data="{}", content_type="text/plain")
        self.assertEqual(r.status_code, 415)
        self.assertEqual(self.c.post("/api/stop", data="{}", content_type="application/x-www-form-urlencoded").status_code, 415)
        self.assertEqual(self.c.get("/api/status").status_code, 200, "读请求不受影响")

    def test_cross_origin_requests_are_refused(self):
        self.assertEqual(self.c.get("/api/status", headers={"Origin": "http://evil.example"}).status_code, 403)
        self.assertEqual(self.c.get("/api/status", headers={"Origin": "http://localhost"}).status_code, 200, "同源放行")

    def test_dns_rebinding_blocked_when_no_token(self):
        self.assertEqual(self.c.get("/api/status", headers={"Host": "attacker.example:8765"}).status_code, 403)
        self.assertEqual(self.c.get("/api/status", headers={"Host": "127.0.0.1:8765"}).status_code, 200)
        self.assertEqual(self.c.get("/api/status", headers={"Host": "[::1]:8765"}).status_code, 200)
        self.c.put("/api/config/token", json={"token": "s3cret-token"})
        ok = {"Authorization": "Bearer s3cret-token", "Host": "myserver.lan:8765"}
        self.assertEqual(self.c.get("/api/status", headers=ok).status_code, 200, "设了口令后可以用任意主机名访问")

    def test_failed_logins_are_throttled(self):
        self.c.put("/api/config/token", json={"token": "s3cret-token"})
        web_app._failures.clear()
        for _ in range(web_app.MAX_FAILURES):
            self.assertEqual(self.c.get("/api/status", headers={"Authorization": "Bearer wrong"}).status_code, 401)
        r = self.c.get("/api/status", headers={"Authorization": "Bearer wrong"})
        self.assertEqual(r.status_code, 429)
        self.assertIn("Retry-After", r.headers)
        self.assertEqual(self.c.get("/api/status", headers={"Authorization": "Bearer s3cret-token"}).status_code, 429,
                         "锁定期内连正确口令也先拒绝，防止被穷举")
        web_app._failures.clear()
        self.assertEqual(self.c.get("/api/status", headers={"Authorization": "Bearer s3cret-token"}).status_code, 200)

    def test_opening_the_page_without_a_token_is_not_a_failed_login(self):
        self.c.put("/api/config/token", json={"token": "s3cret-token"})
        web_app._failures.clear()
        for _ in range(web_app.MAX_FAILURES + 3):
            self.assertEqual(self.c.get("/api/status").status_code, 401)
        self.assertEqual(self.c.get("/api/status", headers={"Authorization": "Bearer s3cret-token"}).status_code, 200)

    def test_security_headers_and_healthz(self):
        r = self.c.get("/")
        self.assertEqual(r.headers["X-Frame-Options"], "DENY")
        self.assertEqual(r.headers["X-Content-Type-Options"], "nosniff")
        self.assertIn("frame-ancestors 'none'", r.headers["Content-Security-Policy"])
        self.assertEqual(self.c.get("/api/status").headers["Cache-Control"], "no-store")
        h = self.c.get("/healthz").get_json()
        self.assertTrue(h["ok"])
        self.assertEqual(h["version"], web_app.APP_VERSION)
        self.assertNotIn("minio", str(h).lower())

    def test_readonly_state_db_uri_handles_spaces_and_unicode(self):
        weird = self.root / "我的 数据 库"
        weird.mkdir()
        with mock.patch.object(dl, "STATE_DB", weird / "state.sqlite3"):
            db = dl.StateDB(dl.STATE_DB)
            db.set_download("e1", "t", "p/e1", 1, "failed")
            db.close()
            conn = web_app._open_ro()
            self.assertIsNotNone(conn)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM downloads").fetchone()[0], 1)
            with self.assertRaises(Exception):
                conn.execute("DELETE FROM downloads")                      # 只读
            conn.close()

    def test_status_reports_version(self):
        self.assertEqual(self.c.get("/api/status").get_json()["version"], web_app.APP_VERSION)


class RunControlTests(WebTestCase):
    def test_start_is_blocked_until_setup_is_complete(self):
        self.c.put("/api/config/disks", json={"disks": []})        # 去掉 Linux 下首次导入的默认磁盘
        r = self.c.post("/api/run", json={})
        self.assertEqual(r.status_code, 400)
        self.assertEqual({p["page"] for p in r.get_json()["problems"]}, {"connection", "disks"})

    def test_start_pause_resume_and_force(self):
        self.configure_minimum()
        popen_calls = []

        def fake_popen(*a, **k):
            popen_calls.append((a, k))
            return FakeProc(*a, **k)
        with mock.patch.object(web_app.subprocess, "Popen", fake_popen), mock.patch.object(web_app.threading, "Thread"):
            r = self.c.post("/api/run", json={"mode": "download", "max_rounds": 3, "recheck_missing": True})
            self.assertEqual(r.status_code, 200)
            argv = popen_calls[0][0][0]
            self.assertEqual(argv[2:], ["--max-rounds", "3", "--recheck-missing"])
            self.assertTrue(set(compat.detached_popen_kwargs()) <= set(popen_calls[0][1]), "必须脱离 Web 进程")
            self.assertEqual(self.c.post("/api/run", json={}).status_code, 409, "已在运行时不能重复启动")
            self.assertEqual(self.c.post("/api/run", json={"mode": "rm -rf"}).status_code, 400)

            st = self.c.get("/api/status?fresh=1").get_json()
            self.assertTrue(st["downloader"]["running"])
            self.assertFalse(st["stop_requested"])

            r = self.c.post("/api/stop", json={}).get_json()                 # 暂停 = 状态库里的请求，不杀进程
            self.assertEqual(r["mode"], "pause")
            db = dl.StateDB(dl.STATE_DB)
            self.assertTrue(db.get_control("stop_requested"))
            db.close()
            self.assertTrue(self.c.get("/api/status?fresh=1").get_json()["stop_requested"])

            with mock.patch.object(compat, "kill_tree") as kill:
                r = self.c.post("/api/stop", json={"force": True}).get_json()
                self.assertEqual(r["mode"], "force")
                kill.assert_called_once_with(FakeProc.pid, force=True)

    def test_new_run_clears_a_stale_pause_request(self):
        self.configure_minimum()
        db = dl.StateDB(dl.STATE_DB)
        db.set_control("stop_requested", "old")
        db.close()
        with mock.patch.object(web_app.subprocess, "Popen", FakeProc), mock.patch.object(web_app.threading, "Thread"):
            self.assertEqual(self.c.post("/api/run", json={}).status_code, 200)
        db = dl.StateDB(dl.STATE_DB)
        self.assertIsNone(db.get_control("stop_requested"))
        db.close()

    def test_stop_without_running_downloader_is_rejected(self):
        self.assertEqual(self.c.post("/api/stop", json={}).status_code, 409)

    def test_cli_started_downloader_is_detected_via_pid_file(self):
        compat.write_pidfile(dl.PID_FILE)                                    # 当前进程冒充
        st = self.c.get("/api/status").get_json()["downloader"]
        self.assertEqual((st["running"], st["by_web"], st["pid"]), (True, False, os.getpid()))


class StatusTests(WebTestCase):
    def seed_state(self):
        cs = web_app.get_cs()
        cs.set_tasks("V", [("S", [("任务A", 4, []), ("任务B", 2, [])])])
        done = self.root / "done_ep"
        done.mkdir()
        part = self.root / "part_ep"
        part.mkdir()
        (part / "a.bin").write_bytes(b"x" * 1234)
        db = dl.StateDB(dl.STATE_DB)
        db.set_download("e1", "任务A", "p/e1", 100, "completed", disk_name="d1", target_path=str(done))
        db.record_downloaded({"episode_id": "e1", "task_name": "任务A", "parent_key": "p/e1", "file_size": 100, "content_time": 7200}, "d1", str(done))
        db.set_download("e2", "任务A", "p/e2", 5000, "running", disk_name="d1", target_path="x", partial_path=str(part), started_at="2026-11-01T00:00:00")
        db.set_download("e3", "任务A", "p/e3", 100, "paused", disk_name="d1", partial_path=str(part))
        db.set_download("e4", "任务A", "p/e4", 100, "missing_mcap", error="缺少 mcap")
        db.set_download("e5", "任务A", "p/e5", 100, "completed", disk_name="d1", target_path=str(self.root / "deleted"))
        db.close()

    def test_progress_counts_and_issue_breakdown(self):
        self.seed_state()
        s = self.c.get("/api/status?fresh=1").get_json()
        a = s["scenes"][0]["tasks"][0]
        self.assertEqual((a["downloaded"], a["target"], a["hours"]), (1, 4, 2.0))
        self.assertEqual(a["issues"], {"running": 1, "paused": 1, "missing_mcap": 1})
        self.assertEqual(s["stale_records"], 1, "记录标记完成但目录已被删除：不计入，并单独提示")
        self.assertEqual(s["running_episodes"], [], "下载器没在运行时，残留的 running 记录不能显示成“正在下载”")

    def test_running_episode_reports_partial_bytes(self):
        self.seed_state()
        compat.write_pidfile(dl.PID_FILE)
        s = self.c.get("/api/status?fresh=1").get_json()
        run = s["running_episodes"][0]
        self.assertEqual((run["episode_id"], run["partial_bytes"], run["expected_bytes"]), ("e2", 1234, 5000))

    def test_last_run_banner_comes_from_state_db(self):
        db = dl.StateDB(dl.STATE_DB)
        db.set_control("last_run", json.dumps({"result": "paused", "at": "2026-11-01T00:00:00Z"}))
        db.close()
        self.assertEqual(self.c.get("/api/status?fresh=1").get_json()["last_run"]["result"], "paused")

    def test_logs_are_incremental_and_complete_lines_only(self):
        log = dl.LOG_DIR / "minio_task_downloader.log"
        log.write_text("a\nb\n", encoding="utf-8")
        r = self.c.get("/api/logs").get_json()
        self.assertEqual((r["text"], r["reset"]), ("a\nb\n", True))
        with log.open("a", encoding="utf-8") as f:
            f.write("c\npart")
        r2 = self.c.get(f"/api/logs?offset={r['offset']}").get_json()
        self.assertEqual((r2["text"], r2["reset"]), ("c\n", False))


if __name__ == "__main__":
    unittest.main()
