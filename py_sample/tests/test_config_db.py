# -*- coding: utf-8 -*-
"""配置库（SQLite）测试：初始化、任务 / 磁盘 / 设置项读写、校验、密钥与口令。"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config_db  # noqa: E402

SCENES = [
    ("场景A", [("任务1", 40, []), ("任务2", 10, ["旧名2"], "目录2")]),
    ("场景B", [("任务3", 5, ["x", "y"])]),
]
DISKS = [{"name": "d1", "path": "/mnt/a", "mountpoint": "/mnt/a"}, {"name": "d2", "path": "/mnt/b/data", "mountpoint": "/mnt/b"}]


class ConfigDbTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "cfg.sqlite3"
        self.cs = config_db.ConfigStore(self.path)

    def test_seed_once_and_roundtrip(self):
        self.assertFalse(self.cs.is_seeded())
        self.assertTrue(self.cs.seed("版本1", SCENES, DISKS, {"pg.port": "6543", "run.require_mcap": "0", "nope": 1}))
        self.assertTrue(self.cs.is_seeded())
        self.assertFalse(self.cs.seed("别的", [], [], {}), "第二次 seed 不能覆盖已有配置")
        dv, scenes = self.cs.get_tasks()
        self.assertEqual(dv, "版本1")
        self.assertEqual(scenes, SCENES)                       # 顺序、别名、目录名全部原样还原
        self.assertEqual(self.cs.get_disks(), DISKS)
        st = self.cs.get_settings()
        self.assertEqual((st["pg.port"], st["run.require_mcap"]), (6543, False))   # 字符串被转成正确类型

    def test_tasks_replace_is_all_or_nothing(self):
        self.cs.set_tasks("v", SCENES)
        with self.assertRaises(ValueError):
            self.cs.set_tasks("v2", [("S", [("同名", 1, [])]), ("S", [("同名2", 1, [])])])   # 场景名重复
        self.assertEqual(self.cs.get_tasks(), ("v", SCENES))                              # 失败后原配置不变
        with self.assertRaises(ValueError):
            self.cs.set_tasks("v3", [("S", [("同名", 1, []), ("同名", 2, [])])])           # 任务名重复
        self.assertEqual(self.cs.get_tasks(), ("v", SCENES))

    def test_settings_validation(self):
        with self.assertRaises(ValueError):
            self.cs.set_settings({"pg.port": 99999})
        with self.assertRaises(ValueError):
            self.cs.set_settings({"run.max_writes_per_disk": 1.5})
        with self.assertRaises(ValueError):
            self.cs.set_settings({"not.a.key": 1})
        with self.assertRaises(ValueError):
            self.cs.set_settings({"run.require_mcap": "maybe"})
        self.cs.set_settings({"run.min_free_gb": "7.5", "transfer.fsync": "off", "minio.bucket": "  b  "})
        st = self.cs.get_settings()
        self.assertEqual((st["run.min_free_gb"], st["transfer.fsync"], st["minio.bucket"]), (7.5, False, "b"))

    def test_secrets_are_kept_when_none_and_never_exposed(self):
        self.cs.set_settings({"minio.secret_key": "S3CRET", "pg.password": "pw"})
        self.cs.set_settings({"minio.secret_key": None, "minio.access_key": "AK"})     # None = 不修改
        self.assertEqual(self.cs.get_settings()["minio.secret_key"], "S3CRET")
        values, secrets_set = self.cs.public_settings()
        self.assertEqual(values["minio.secret_key"], "")
        self.assertEqual(values["pg.password"], "")
        self.assertEqual(set(secrets_set), {"minio.secret_key", "pg.password"})
        self.assertEqual(values["minio.access_key"], "AK")
        self.assertNotIn("S3CRET", str(config_db.schema()))
        self.cs.set_settings({"minio.secret_key": ""})                                 # 空串 = 清除
        self.assertEqual(self.cs.get_settings()["minio.secret_key"], "")

    def test_disks_validation(self):
        with self.assertRaises(ValueError):
            self.cs.set_disks([{"name": "a", "path": "/x"}, {"name": "a", "path": "/y"}])
        with self.assertRaises(ValueError):
            self.cs.set_disks([{"name": "", "path": "/x"}])
        self.cs.set_disks([{"name": "a", "path": "/x"}])
        self.assertEqual(self.cs.get_disks(), [{"name": "a", "path": "/x", "mountpoint": "/x"}])   # 挂载点默认同路径

    def test_web_token_is_hashed(self):
        self.assertFalse(self.cs.has_web_token())
        with self.assertRaises(ValueError):
            self.cs.set_web_token("12345")
        self.cs.set_web_token("correct horse")
        self.assertTrue(self.cs.has_web_token())
        self.assertTrue(self.cs.verify_web_token("correct horse"))
        self.assertFalse(self.cs.verify_web_token("wrong"))
        raw = sqlite3.connect(self.path).execute("SELECT value FROM settings WHERE key='web.token_hash'").fetchone()[0]
        self.assertNotIn("correct horse", raw)
        self.assertIn("pbkdf2_sha256", raw)
        self.cs.clear_web_token()
        self.assertFalse(self.cs.has_web_token())

    @unittest.skipIf(os.name == "nt", "POSIX 文件权限")
    def test_file_is_private(self):
        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o600)

    def test_snapshot_is_consistent_and_reads_do_not_wait_for_writers(self):
        self.cs.seed("v", SCENES, DISKS, {})
        import sqlite3
        writer = sqlite3.connect(self.path, timeout=1)
        writer.execute("BEGIN IMMEDIATE")                              # 另一个连接正持有写锁
        writer.execute("UPDATE settings SET value = '\"x\"' WHERE key = 'tasks.data_version'")
        try:
            snap = self.cs.snapshot()                                  # WAL 下读取不应被阻塞，也看不到未提交的修改
        finally:
            writer.rollback()
            writer.close()
        self.assertEqual(snap["data_version"], "v")
        self.assertEqual(snap["scene_tasks"], SCENES)
        self.assertEqual(snap["disks"], DISKS)
        self.assertIn("run.include_null_quality", snap["settings"])

    def test_concurrent_writers_do_not_corrupt(self):
        import threading
        errors = []

        def work(i):
            try:
                for j in range(20):
                    self.cs.set_settings({"run.default_rounds": (i + j) % 50 + 1})
                    self.cs.get_settings()
            except Exception as exc:       # pragma: no cover
                errors.append(exc)
        threads = [threading.Thread(target=work, args=(i,)) for i in range(6)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
