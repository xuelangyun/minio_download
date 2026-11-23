# -*- coding: utf-8 -*-
"""compat（跨平台进程锁 / PID 文件 / 结束进程树）与 Windows 分支的模拟测试。"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import compat  # noqa: E402
import minio_store as store  # noqa: E402


class LockAndPidTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.lock_path = Path(self.tmp.name) / "x.lock"

    def test_lock_is_exclusive_and_reusable(self):
        a, b = compat.FileLock(self.lock_path), compat.FileLock(self.lock_path)
        self.assertTrue(a.acquire())
        self.assertFalse(b.acquire(), "第二个持有者必须失败")
        a.release()
        self.assertTrue(b.acquire())
        b.release()

    def test_lock_is_released_when_holder_process_dies(self):
        code = (f"import sys; sys.path.insert(0, {str(ROOT)!r}); import compat, time; "
                f"l = compat.FileLock({str(self.lock_path)!r}); assert l.acquire(); print('held', flush=True); time.sleep(60)")
        proc = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(proc.stdout.readline().strip(), "held")
            probe = compat.FileLock(self.lock_path)
            self.assertFalse(probe.acquire(), "别的进程持有时不能获得")
            proc.kill()
            proc.wait(timeout=10)
            self.assertTrue(probe.acquire(), "持有者被 kill 后，操作系统应自动释放锁")
            probe.release()
        finally:
            proc.kill()
            proc.stdout.close()
            proc.wait()

    def test_pidfile_roundtrip_and_liveness(self):
        pidfile = Path(self.tmp.name) / "x.pid"
        compat.write_pidfile(pidfile)
        info = compat.read_pidfile(pidfile)
        self.assertEqual(info["pid"], os.getpid())
        self.assertTrue(compat.process_alive(info["pid"], info["created"]))
        if info["created"] is not None:
            self.assertFalse(compat.process_alive(info["pid"], info["created"] + 1000), "创建时间对不上 = PID 被复用")
        self.assertFalse(compat.process_alive(2 ** 22 + 12345))
        compat.remove_pidfile(pidfile)
        self.assertIsNone(compat.read_pidfile(pidfile))

    def test_kill_tree_kills_children_too(self):
        code = ("import subprocess, sys, time; "
                "c = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']); "
                "print(c.pid, flush=True); time.sleep(60)")
        parent = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True,
                                  **compat.detached_popen_kwargs())
        try:
            child_pid = int(parent.stdout.readline())
            self.assertTrue(compat.process_alive(child_pid))
            compat.kill_tree(parent.pid, force=True)
            parent.wait(timeout=10)
            deadline = time.time() + 5
            while compat.process_alive(child_pid) and time.time() < deadline:
                time.sleep(0.05)
            self.assertFalse(compat.process_alive(child_pid), "子进程应当一并结束")
        finally:
            parent.kill()
            parent.stdout.close()
            parent.wait()

    def test_detached_kwargs_per_platform(self):
        with mock.patch.object(compat, "IS_WINDOWS", True):
            self.assertIn("creationflags", compat.detached_popen_kwargs())
        with mock.patch.object(compat, "IS_WINDOWS", False):
            self.assertEqual(compat.detached_popen_kwargs(), {"start_new_session": True})


class WindowsBranchTests(unittest.TestCase):
    """在 Linux 上模拟 Windows 分支，验证逻辑（真实 Windows 行为仍需在 Windows 上实测）。"""

    def test_object_names_illegal_on_windows_are_rejected(self):
        with mock.patch.object(store, "IS_WINDOWS", True):
            for bad in ("a/b:c.txt", "x/CON", "nul.txt", "dir/trailing.", "q?.bin", "a\\b"):
                with self.assertRaises(store.StoreError, msg=bad):
                    store._safe_rel(bad)
            store._safe_rel("2026-11-01/ep_1/rec0.SYNC.mcap")          # 正常名字不受影响
        with mock.patch.object(store, "IS_WINDOWS", False):
            store._safe_rel("a/b:c.txt")                              # POSIX 允许

    def test_fs_retry_only_retries_permission_errors_on_windows(self):
        calls = []

        def flaky():
            calls.append(1)
            if len(calls) < 3:
                raise PermissionError("in use by antivirus")
            return "ok"
        with mock.patch.object(store, "IS_WINDOWS", True), mock.patch.object(store.time, "sleep"):
            self.assertEqual(store._fs_retry(flaky), "ok")
        self.assertEqual(len(calls), 3)
        calls.clear()
        with mock.patch.object(store, "IS_WINDOWS", False):
            with self.assertRaises(PermissionError):
                store._fs_retry(flaky)
        self.assertEqual(len(calls), 1)

    def test_safe_component_avoids_reserved_names_on_windows(self):
        sys.modules.setdefault("psycopg2", __import__("types").ModuleType("psycopg2"))
        import minio_task_downloader_by_count as dl
        with mock.patch.object(dl.compat, "IS_WINDOWS", True):
            self.assertEqual(dl.safe_component("CON"), "_CON")
            self.assertEqual(dl.safe_component("com1.txt"), "_com1.txt")
            self.assertEqual(dl.safe_component("积木搭建"), "积木搭建")
        with mock.patch.object(dl.compat, "IS_WINDOWS", False):
            self.assertEqual(dl.safe_component("CON"), "CON")


if __name__ == "__main__":
    unittest.main()
