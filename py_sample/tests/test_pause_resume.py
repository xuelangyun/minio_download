# -*- coding: utf-8 -*-
"""暂停 / 续传测试：store 层、download_one 层、以及完整的 main() 流程（含 --stop、状态库里的暂停请求）。"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    import psycopg2  # noqa: F401
except ImportError:
    sys.modules["psycopg2"] = types.ModuleType("psycopg2")

import compat                                    # noqa: E402
import minio_store as store                      # noqa: E402
import minio_task_downloader_by_count as dl      # noqa: E402
from fake_minio import FakeMinio                 # noqa: E402

BIG = 600_000


def payload(n: int) -> bytes:
    return bytes((i * 13 + 7) % 251 for i in range(n))


def wait_for(predicate, timeout=10.0, interval=0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class StoreLevelPause(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dest = Path(self.tmp.name) / ".ep.partial"
        self.fake = FakeMinio()
        self.fake.chunk_delay = 0.01
        self.fake.put("b", "p/ep/task_info.json", b"{}")
        self.fake.put("b", "p/ep/big.mcap", payload(BIG))
        store.use_client(self.fake)
        store.STOP.clear()
        self.addCleanup(store.use_client, None)
        self.addCleanup(store.STOP.clear)
        for p in (mock.patch.object(store, "CHUNK_SIZE", 10_000), mock.patch.object(store, "RETRY_BASE_DELAY", 0.0),
                  mock.patch.object(store, "RESUME_BACKOFF", 20_000)):
            p.start()
            self.addCleanup(p.stop)

    def tmp_files(self):
        return [p for p in self.dest.rglob("*") if p.name.endswith(store.TMP_SUFFIX)]

    def test_pause_mid_file_keeps_progress_and_never_publishes(self):
        def pause_when_partially_written():
            wait_for(lambda: any(p.stat().st_size > 100_000 for p in self.tmp_files()))
            store.STOP.set()
        threading.Thread(target=pause_when_partially_written, daemon=True).start()

        with self.assertRaises(store.StoreStopped):
            store.download_prefix("b", "p/ep", self.dest)
        self.assertFalse((self.dest / "big.mcap").exists(), "暂停后正式文件名下不能有残缺文件")
        kept = self.tmp_files()
        self.assertEqual(len(kept), 1)
        size = kept[0].stat().st_size
        self.assertTrue(100_000 <= size < BIG)

        store.STOP.clear()                                          # 继续
        self.fake.get_calls.clear()
        self.fake.chunk_delay = 0
        store.download_prefix("b", "p/ep", self.dest)
        self.assertEqual((self.dest / "big.mcap").read_bytes(), payload(BIG))
        first = [c for c in self.fake.get_calls if c["key"] == "p/ep/big.mcap"][0]
        self.assertGreater(first["offset"], 0, "应从断点续传")
        self.assertLessEqual(first["offset"], size)
        self.assertEqual(self.tmp_files(), [])

    def test_stop_already_set_starts_nothing(self):
        store.STOP.set()
        with self.assertRaises(store.StoreStopped):
            store.download_prefix("b", "p/ep", self.dest)
        self.assertEqual(self.fake.get_calls, [])


class MainFlowBase(unittest.TestCase):
    GLOBALS = ["DATA_VERSION", "SCENE_TASKS", "TASK_NAMES", "TASK_TARGET_COUNTS", "TASK_NAME_ALIASES", "TASK_DIR_NAMES",
               "TASK_SCENES", "DEST_DISKS", "MIN_FREE_GB", "SPACE_SAFETY_FACTOR", "MAX_WRITES_PER_DISK", "REQUIRE_MCAP",
               "MCAP_FILE_PATTERN", "VALIDATE_WORKERS", "DEFAULT_FILL_ROUNDS", "MINIO_BUCKET", "PG_HOST", "PG_PORT",
               "PG_DATABASE", "PG_USER", "PG_PASSWORD", "_MINIO_CONFIG_ERROR", "RECHECK_MISSING"]
    TUNING = ["FILE_WORKERS", "FILE_RETRIES", "RETRY_BASE_DELAY", "CONNECT_TIMEOUT", "READ_TIMEOUT", "FSYNC", "VERIFY_MD5", "CHUNK_SIZE"]

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.disk = root / "disk1"
        self.disk.mkdir()

        saved = {n: getattr(dl, n) for n in self.GLOBALS}
        saved_tuning = {n: getattr(store, n) for n in self.TUNING}
        saved_handlers, saved_level = list(logging.getLogger().handlers), logging.getLogger().level
        saved_sig = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}

        def restore():
            for n, v in saved.items():
                setattr(dl, n, v)
            for n, v in saved_tuning.items():
                setattr(store, n, v)
            root_logger = logging.getLogger()
            for h in list(root_logger.handlers):
                if h not in saved_handlers:
                    h.close()
            root_logger.handlers[:] = saved_handlers
            root_logger.setLevel(saved_level)
            for s, h in saved_sig.items():
                signal.signal(s, h)
            dl.VALIDATED_EPISODES.clear()
            dl._WATCHER_DONE.set()
            store.set_config(None)
            store.use_client(None)
            store.STOP.clear()
        self.addCleanup(restore)

        for p in (mock.patch.object(dl, "CONFIG_DB", root / "cfg.sqlite3"), mock.patch.object(dl, "STATE_DB", root / "state.sqlite3"),
                  mock.patch.object(dl, "LOG_DIR", root / "logs"), mock.patch.object(dl, "LOCK_FILE", root / "x.lock"),
                  mock.patch.object(dl, "PID_FILE", root / "x.pid"), mock.patch.object(dl, "STOP_POLL_SECONDS", 0.05)):
            p.start()
            self.addCleanup(p.stop)

        cs = dl.open_config_store()
        cs.set_tasks("版本", [("场景", [("任务X", 2, [])])])
        cs.set_disks([{"name": "disk1", "path": str(self.disk), "mountpoint": str(self.disk)}])
        cs.set_settings({"run.min_free_gb": 0, "transfer.retry_delay": 0, "minio.endpoint": "http://fake:9000",
                         "minio.access_key": "a", "minio.secret_key": "b", "run.max_writes_per_disk": 2})
        self.cs = cs

        self.fake = FakeMinio()
        self.rows = []
        for i in (1, 2):
            key = f"p/ep{i}"
            self.fake.put(dl.MINIO_BUCKET, f"{key}/task_info.json", b"{}")
            self.fake.put(dl.MINIO_BUCKET, f"{key}/rec.SYNC.mcap", payload(BIG))
            self.rows.append({"episode_id": f"e{i}", "job_id": i, "task_name": "任务X", "sn_code": "s", "parent_key": key,
                              "file_size": BIG + 2, "content_time": 60, "parsed_status": "ok", "quality_status": "合格",
                              "create_time": f"2026-11-0{i}T00:00:00", "video_file_size": 0})
        store.use_client(self.fake)
        for p in (mock.patch.object(dl, "fetch_rows", lambda: list(self.rows)),
                  mock.patch.object(dl, "build_disk_configs",
                                    lambda: [{"name": "disk1", "path": self.disk, "mountpoint": self.disk, "st_dev": 1}])):
            p.start()
            self.addCleanup(p.stop)

    def run_main(self, *argv):
        with mock.patch.object(sys, "argv", ["dl", *argv]):
            return dl.main()

    def state(self):
        return dl.StateDB(dl.STATE_DB)

    def last_run(self):
        db = self.state()
        try:
            return json.loads(db.get_control("last_run"))
        finally:
            db.close()


class MainFlowTests(MainFlowBase):
    def test_full_run_completes_and_cleans_up_process_files(self):
        self.assertEqual(self.run_main(), 0)
        db = self.state()
        try:
            self.assertEqual({db.get_download(f"e{i}")["status"] for i in (1, 2)}, {"completed"})
        finally:
            db.close()
        for i in (1, 2):
            self.assertEqual((self.disk / "版本/场景/任务X" / f"ep{i}" / "rec.SYNC.mcap").read_bytes(), payload(BIG))
        self.assertEqual(self.last_run()["result"], "finished")
        self.assertFalse(dl.PID_FILE.exists(), "退出后 PID 文件应被删除")
        probe = compat.FileLock(dl.LOCK_FILE)
        self.assertTrue(probe.acquire(), "退出后锁应已释放")
        probe.release()

    def test_pause_via_state_db_flag_then_resume(self):
        self.fake.chunk_delay = 0.02
        store.CHUNK_SIZE = 10_000

        def request_pause_soon(_key, armed=[]):                # 第一次开始下载 mcap 后 0.3 秒请求暂停（和 Web / --stop 同一机制）
            if _key.endswith(".mcap") and not armed:
                armed.append(1)

                def go():
                    db = self.state()
                    db.set_control("stop_requested", "now")
                    db.close()
                threading.Timer(0.3, go).start()
        self.fake.on_get = request_pause_soon

        self.assertEqual(self.run_main(), 0)
        self.assertEqual(self.last_run()["result"], "paused")
        self.assertFalse(dl.PID_FILE.exists())
        db = self.state()
        try:
            statuses = {db.get_download(f"e{i}")["status"] if db.get_download(f"e{i}") else None for i in (1, 2)}
        finally:
            db.close()
        self.assertIn("paused", statuses)
        self.assertNotIn("completed", statuses, "暂停时不应有数据被误标记为完成")
        for i in (1, 2):
            self.assertFalse((self.disk / "版本/场景/任务X" / f"ep{i}").exists(), "暂停后不能出现不完整的正式目录")
        partials = [p for p in (self.disk / "版本/场景/任务X").glob(".*.partial")]
        self.assertTrue(partials, "暂存目录必须保留，等待续传")

        # —— 继续：再次启动，从断点续传，结果与源一致 ——
        self.fake.on_get = None
        self.fake.chunk_delay = 0
        self.fake.get_calls.clear()
        with mock.patch.object(store, "RESUME_BACKOFF", 10_000):
            self.assertEqual(self.run_main(), 0)
        self.assertEqual(self.last_run()["result"], "finished")
        for i in (1, 2):
            self.assertEqual((self.disk / "版本/场景/任务X" / f"ep{i}" / "rec.SYNC.mcap").read_bytes(), payload(BIG))
        self.assertTrue(any(c["offset"] > 0 and c["key"].endswith(".mcap") for c in self.fake.get_calls),
                        "至少有一个文件应当从断点续传，而不是从头下载")
        self.assertEqual(list((self.disk / "版本/场景/任务X").glob(".*.partial")), [])

    def test_cli_stop_requires_a_running_downloader(self):
        self.assertEqual(self.run_main("--stop"), 0)
        db = self.state()
        try:
            self.assertIsNone(db.get_control("stop_requested"), "没有运行中的下载器时不应留下暂停请求")
        finally:
            db.close()
        compat.write_pidfile(dl.PID_FILE)                        # 用当前进程冒充“运行中的下载器”
        self.assertEqual(self.run_main("--stop"), 0)
        db = self.state()
        try:
            self.assertTrue(db.get_control("stop_requested"))
        finally:
            db.close()

    def test_stale_stop_flag_from_previous_run_is_cleared_at_start(self):
        db = self.state()
        db.set_control("stop_requested", "old")
        db.close()
        self.assertEqual(self.run_main(), 0)
        self.assertEqual(self.last_run()["result"], "finished")

    def test_second_instance_is_refused(self):
        held = compat.FileLock(dl.LOCK_FILE)
        self.assertTrue(held.acquire())
        self.addCleanup(held.release)
        self.assertEqual(self.run_main(), 2)

    def test_missing_minio_config_is_a_clear_error(self):
        self.cs.set_settings({"minio.endpoint": ""})
        self.assertEqual(self.run_main(), 1)
        self.assertEqual(self.last_run()["result"], "error")
        self.assertIn("MinIO", self.last_run()["error"])

    def test_invalid_task_config_does_not_start(self):
        self.cs.set_tasks("v", [("S", [("a", 1, ["b"]), ("b", 1, [])])])     # 别名与另一个任务名冲突
        self.assertEqual(self.run_main(), 1)


class DownloadOneStopTests(MainFlowBase):
    def test_stop_set_before_start_leaves_records_untouched(self):
        dl.apply_config(dl.config_snapshot(self.cs))
        db = self.state()
        try:
            store.STOP.set()
            result = dl.download_one(dict(self.rows[0]), [{"name": "disk1", "path": self.disk, "mountpoint": self.disk, "st_dev": 1}],
                                     db, dl.DiskReservationManager(), "disk1")
            self.assertEqual(result, "stopped")
            self.assertIsNone(db.get_download("e1"))
        finally:
            db.close()

    def test_watcher_sets_stop_from_state_db_flag(self):
        dl.apply_config(dl.config_snapshot(self.cs))
        db = self.state()
        try:
            store.STOP.clear()
            dl.start_stop_watcher(dl.STATE_DB)
            self.assertFalse(store.STOP.is_set())
            db.set_control("stop_requested", "now")
            self.assertTrue(wait_for(store.STOP.is_set, timeout=3))
        finally:
            dl._WATCHER_DONE.set()
            db.close()


if __name__ == "__main__":
    unittest.main()
