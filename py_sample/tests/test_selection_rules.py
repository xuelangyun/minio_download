# -*- coding: utf-8 -*-
"""选条数相关的规则：历史别名按创建时间全局排序、质量状态为空的处理、PG 连接错误、本地已有数据不依赖 MinIO。"""

from __future__ import annotations

import sys
import tempfile
import types
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    import psycopg2  # noqa: F401
except ImportError:
    sys.modules["psycopg2"] = types.ModuleType("psycopg2")

import minio_store as store                      # noqa: E402
import minio_task_downloader_by_count as dl      # noqa: E402
from fake_minio import FakeMinio                 # noqa: E402


class FakePg:
    """极简的 psycopg2 替身：connect() 返回一个能执行查询并返回预置行的连接。"""
    class Error(Exception):
        pass

    def __init__(self, rows=None, fail=None):
        self.rows, self.fail, self.executed = rows or [], fail, []

    def connect(self, **kw):
        if self.fail:
            raise self.fail
        outer = self

        class Cur:
            description = [(c,) for c in ("episode_id", "job_id", "task_name", "sn_code", "parent_key", "file_size",
                                          "content_time", "parsed_status", "quality_status", "create_time", "video_file_size")]

            def __enter__(self): return self
            def __exit__(self, *a): return False
            def execute(self, sql, params): outer.executed.append((sql, params))
            def fetchall(self): return [tuple(r[c[0]] for c in self.description) for r in outer.rows]

        class Conn:
            def cursor(self): return Cur()
            def close(self): pass
        return Conn()


def row(eid, task, day, key=None):
    return {"episode_id": eid, "job_id": 1, "task_name": task, "sn_code": "s", "parent_key": key or f"p/{eid}",
            "file_size": 100, "content_time": 10, "parsed_status": "ok", "quality_status": "合格",
            "create_time": datetime(2026, 1, day), "video_file_size": 0}


class SqlRules(unittest.TestCase):
    def test_default_sql_keeps_original_behaviour(self):
        sql = dl.build_sql(False)
        self.assertIn("j.quality_status NOT IN ('不合格', '未解析', '未质检')", sql)
        self.assertNotIn("IS NULL", sql)
        self.assertEqual(dl.SQL, sql)

    def test_include_null_quality_is_explicit(self):
        sql = dl.build_sql(True)
        self.assertIn("(j.quality_status IS NULL OR j.quality_status NOT IN ('不合格', '未解析', '未质检'))", sql)

    def test_setting_is_wired_through_config(self):
        with tempfile.TemporaryDirectory() as d, mock.patch.object(dl, "CONFIG_DB", Path(d) / "c.sqlite3"):
            cs = dl.open_config_store()
            snap = cs.snapshot()
            self.assertFalse(snap["settings"]["run.include_null_quality"])
            saved = dl.INCLUDE_NULL_QUALITY
            try:
                cs.set_settings({"run.include_null_quality": True})
                dl.apply_config(cs.snapshot())
                self.assertTrue(dl.INCLUDE_NULL_QUALITY)
            finally:
                dl.INCLUDE_NULL_QUALITY = saved


class FetchRowsRules(unittest.TestCase):
    def setUp(self):
        self.saved = (dl.psycopg2, dl.TASK_NAMES, dl.TASK_NAME_ALIASES)
        self.addCleanup(lambda: (setattr(dl, "psycopg2", self.saved[0]), setattr(dl, "TASK_NAMES", self.saved[1]),
                                 setattr(dl, "TASK_NAME_ALIASES", self.saved[2])))
        dl.TASK_NAMES = ["新名"]
        dl.TASK_NAME_ALIASES = {"旧名A": "新名", "旧名B": "新名"}

    def test_alias_rows_are_ordered_by_create_time_across_aliases(self):
        """SQL 按原任务名分组排序；并入同一任务后必须全局按 create_time，才能“取最早的 N 条”。"""
        sql_order = [row("a1", "旧名A", 5), row("a2", "旧名A", 9), row("b1", "旧名B", 2), row("n1", "新名", 7), row("n0", "新名", 1)]
        dl.psycopg2 = FakePg(sql_order)
        rows = dl.fetch_rows()
        self.assertEqual([r["episode_id"] for r in rows], ["n0", "b1", "a1", "n1", "a2"])
        self.assertTrue(all(r["task_name"] == "新名" for r in rows))
        self.assertEqual(rows[1]["source_task_name"], "旧名B")

    def test_missing_create_time_sorts_last_without_crashing(self):
        late = row("x", "新名", 3)
        none = {**row("y", "新名", 1), "create_time": None}
        dl.psycopg2 = FakePg([none, late])
        self.assertEqual([r["episode_id"] for r in dl.fetch_rows()], ["x", "y"])

    def test_query_uses_configured_quality_filter(self):
        pg = FakePg([])
        dl.psycopg2 = pg
        with mock.patch.object(dl, "INCLUDE_NULL_QUALITY", True):
            dl.fetch_rows()
        self.assertIn("IS NULL", pg.executed[0][0])
        self.assertEqual(set(pg.executed[0][1][0]), {"新名", "旧名A", "旧名B"})

    def test_pg_connection_error_is_a_clear_setup_error(self):
        fake = FakePg(fail=FakePg.Error("password authentication failed for user"))
        dl.psycopg2 = fake
        with self.assertRaises(dl.SetupError) as ctx:
            dl.fetch_rows()
        self.assertIn("无法连接 PostgreSQL", str(ctx.exception))
        self.assertIn("连接", str(ctx.exception))

    def test_empty_pg_password_is_allowed_to_try(self):
        dl.psycopg2 = FakePg([])
        with mock.patch.object(dl, "PG_PASSWORD", ""):
            self.assertEqual(dl.fetch_rows(), [])


class LocalFirst(unittest.TestCase):
    def test_existing_local_copy_never_needs_minio(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            disk = {"name": "disk1", "path": root / "disk1", "mountpoint": root / "disk1", "st_dev": 1}
            disk["path"].mkdir()
            fake = FakeMinio()
            fake.fail_list = 100                                  # MinIO 完全不可用
            store.use_client(fake)
            self.addCleanup(store.use_client, None)
            task = dl.TASK_NAMES[0]
            final, _partial = dl.target_paths(disk, task, "e1", "p/ep1")
            final.mkdir(parents=True)
            (final / "x").write_text("data")
            db = dl.StateDB(root / "s.sqlite3")
            self.addCleanup(db.close)
            with mock.patch.object(dl, "LOG_DIR", root), mock.patch.object(dl, "VALIDATED_EPISODES", set()), \
                    mock.patch.object(dl, "MIN_FREE_GB", 0.0):
                result = dl.download_one({"episode_id": "e1", "task_name": task, "parent_key": "p/ep1", "file_size": 10},
                                         [disk], db, dl.DiskReservationManager(), "disk1")
            self.assertEqual(result, "skipped", "本地已有完整数据时，MinIO 故障不能让它变成 failed")
            self.assertEqual(db.get_download("e1")["status"], "completed")
            self.assertEqual(fake.get_calls, [])


if __name__ == "__main__":
    unittest.main()
