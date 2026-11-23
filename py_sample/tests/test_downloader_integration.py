# -*- coding: utf-8 -*-
"""下载脚本与 minio_store 的集成测试：用内存版 MinIO + 临时目录模拟磁盘，不需要 PostgreSQL / MinIO / mc。

运行：python -m unittest discover -s tests -v
"""

from __future__ import annotations

import errno
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    import psycopg2  # noqa: F401
except ImportError:                       # 测试不连数据库，用空模块顶替
    sys.modules["psycopg2"] = types.ModuleType("psycopg2")

_TMP_ENV = tempfile.mkdtemp(prefix="dl_env_")
os.environ.setdefault("MINIO_DOWNLOAD_STATE_DB", os.path.join(_TMP_ENV, "state.sqlite3"))
os.environ.setdefault("MINIO_DOWNLOAD_LOG_DIR", os.path.join(_TMP_ENV, "logs"))
os.environ["MINIO_TASKS_CONFIG"] = os.path.join(_TMP_ENV, "absent.json")

import minio_store as store                          # noqa: E402
import minio_task_downloader_by_count as dl          # noqa: E402
from fake_minio import FakeMinio                     # noqa: E402

BUCKET = dl.MINIO_BUCKET
PARENT = "galbot/2026-11-01/ep_0001"


def payload(n: int, seed: int = 3) -> bytes:
    return bytes((i * 17 + seed) % 253 for i in range(n))


class DownloadOneTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        (root / "logs").mkdir()
        self.disk_path = root / "disk1"
        self.disk_path.mkdir()
        self.disks = [{"name": "disk1", "path": self.disk_path, "mountpoint": self.disk_path, "st_dev": 1}]
        self.db_path = root / "state.sqlite3"
        self.db = dl.StateDB(self.db_path)
        self.addCleanup(self.db.close)

        self.fake = FakeMinio()
        store.use_client(self.fake)
        self.addCleanup(store.use_client, None)
        for p in (mock.patch.object(dl, "LOG_DIR", root / "logs"),
                  mock.patch.object(dl, "MIN_FREE_GB", 0.0),
                  mock.patch.object(dl, "VALIDATED_EPISODES", set()),
                  mock.patch.object(store, "RETRY_BASE_DELAY", 0.0),
                  mock.patch.object(store, "CHUNK_SIZE", 1000)):
            p.start()
            self.addCleanup(p.stop)

        self.task = dl.TASK_NAMES[0]
        self.info = b'{"task":"demo"}'
        self.mcap = payload(9_000)
        self.fake.put(BUCKET, f"{PARENT}/task_info.json", self.info)
        self.fake.put(BUCKET, f"{PARENT}/rec0.SYNC.mcap", self.mcap)
        self.row = {"episode_id": "e1", "task_name": self.task, "parent_key": PARENT,
                    "file_size": len(self.info) + len(self.mcap), "content_time": 60}

    def run_one(self):
        return dl.download_one(self.row, self.disks, self.db, dl.DiskReservationManager(), "disk1")

    def final_dir(self) -> Path:
        return dl.target_paths(self.disks[0], self.task, "e1", PARENT)[0]

    def partial_dir(self) -> Path:
        return dl.target_paths(self.disks[0], self.task, "e1", PARENT)[1]

    def test_success_publishes_complete_directory_atomically(self):
        self.assertEqual(self.run_one(), "completed")
        final = self.final_dir()
        self.assertEqual((final / "rec0.SYNC.mcap").read_bytes(), self.mcap)
        self.assertEqual((final / "task_info.json").read_bytes(), self.info)
        self.assertFalse(self.partial_dir().exists())
        self.assertEqual([p for p in final.rglob("*") if p.name.endswith(store.TMP_SUFFIX)], [])
        state = self.db.get_download("e1")
        self.assertEqual((state["status"], state["target_path"]), ("completed", str(final)))

    def test_second_run_skips_existing(self):
        self.run_one()
        self.fake.get_calls.clear()
        self.assertEqual(self.run_one(), "skipped")
        self.assertEqual(self.fake.get_calls, [])

    def test_missing_mcap_downloads_nothing(self):
        del self.fake.store[(BUCKET, f"{PARENT}/rec0.SYNC.mcap")]
        self.assertEqual(self.run_one(), "missing_mcap")
        self.assertEqual(self.fake.get_calls, [])
        self.assertFalse(self.final_dir().exists())
        self.assertFalse(self.partial_dir().exists())
        self.assertEqual(self.db.get_download("e1")["status"], "missing_mcap")

    def test_missing_task_info_downloads_nothing(self):
        del self.fake.store[(BUCKET, f"{PARENT}/task_info.json")]
        self.assertEqual(self.run_one(), "missing_task_info")
        self.assertFalse(self.final_dir().exists())

    def test_listing_failure_is_not_recorded_as_missing(self):
        """网络故障不能被记成“缺文件”（默认会被一直跳过）。"""
        self.fake.fail_list = 10
        self.assertEqual(self.run_one(), "failed")
        state = self.db.get_download("e1")
        self.assertEqual(state["status"], "failed")
        self.assertNotIn(state["status"], dl.MISSING_STATUSES)
        self.assertFalse(self.final_dir().exists())

    def test_interrupted_download_keeps_partial_and_never_exposes_final(self):
        key = f"{PARENT}/rec0.SYNC.mcap"
        self.fake.break_plan[key] = [3_000] * 10
        with mock.patch.object(store, "FILE_RETRIES", 1):
            with self.assertRaises(RuntimeError):
                self.run_one()
        self.assertFalse(self.final_dir().exists())                  # 正式目录不存在
        self.assertTrue(self.partial_dir().is_dir())                 # 暂存目录保留，等待续传
        state = self.db.get_download("e1")
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state["partial_path"], str(self.partial_dir()))

        # 修复后重跑：从断点续传，最终结果与源完全一致
        self.fake.break_plan.clear()
        self.fake.get_calls.clear()
        with mock.patch.object(store, "RESUME_BACKOFF", 500):
            self.assertEqual(self.run_one(), "completed")
        offsets = [c["offset"] for c in self.fake.get_calls if c["key"] == key]
        self.assertEqual(offsets, [2_500])                           # 3000 - 500 回退
        self.assertEqual((self.final_dir() / "rec0.SYNC.mcap").read_bytes(), self.mcap)
        self.assertFalse(self.partial_dir().exists())

    def test_disk_full_cleans_staging_and_reports_waiting_space(self):
        def boom(f, chunk):
            raise OSError(errno.ENOSPC, "No space left on device")
        with mock.patch.object(store, "_write_chunk", boom):
            self.assertEqual(self.run_one(), "waiting_space")
        self.assertFalse(self.partial_dir().exists())
        self.assertFalse(self.final_dir().exists())

    def test_no_mc_binary_is_ever_invoked(self):
        with mock.patch("subprocess.Popen", side_effect=AssertionError("不应启动子进程")), \
             mock.patch("subprocess.run", side_effect=lambda cmd, *a, **k: self.assertEqual(cmd[0], "du") or mock.Mock(returncode=1, stdout="", stderr="")):
            self.assertEqual(self.run_one(), "completed")


class SourceHygieneTests(unittest.TestCase):
    def test_script_has_no_mc_command_references(self):
        text = (ROOT / "minio_task_downloader_by_count.py").read_text(encoding="utf-8")
        for needle in ("/opt/mc", "MC_BIN", '"mirror"', "mc mirror", "verify_mc"):
            self.assertNotIn(needle, text)


if __name__ == "__main__":
    unittest.main()
