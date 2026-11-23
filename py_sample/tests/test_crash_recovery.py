# -*- coding: utf-8 -*-
"""真实子进程崩溃测试：下载到一半 kill -9，验证
  1. 崩溃瞬间正式文件名下不存在任何不完整的文件；
  2. 新进程能从临时文件续传，最终内容与源逐字节一致。
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import minio_store as store          # noqa: E402
from fake_minio import FakeMinio     # noqa: E402

SIZE = 6_000_000


def source_bytes() -> bytes:
    return bytes((i * 7 + 5) % 251 for i in range(SIZE))


CHILD = textwrap.dedent("""
    import sys
    sys.path.insert(0, {root!r}); sys.path.insert(0, {tests!r})
    import minio_store as store
    from fake_minio import FakeMinio
    from pathlib import Path
    data = bytes((i * 7 + 5) % 251 for i in range({size}))
    fake = FakeMinio(); fake.chunk_delay = 0.02
    fake.put("b", "p/ep/task_info.json", b"{{}}")
    fake.put("b", "p/ep/big.mcap", data)
    store.use_client(fake)
    store.CHUNK_SIZE = 50_000
    store.download_prefix("b", "p/ep", Path({dest!r}))
""")


class CrashRecoveryTest(unittest.TestCase):
    def test_kill_minus_9_then_resume(self):
        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / ".ep.partial"
            code = CHILD.format(root=str(ROOT), tests=str(Path(__file__).resolve().parent), size=SIZE, dest=str(dest))
            child = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                deadline = time.time() + 20
                tmp = None
                while time.time() < deadline:
                    found = [p for p in dest.glob("*") if p.name.endswith(store.TMP_SUFFIX) and p.stat().st_size > 500_000] if dest.exists() else []
                    if found:
                        tmp = found[0]
                        break
                    time.sleep(0.02)
                self.assertIsNotNone(tmp, f"子进程没有开始写临时文件：{child.stderr.read1().decode() if child.poll() is not None else ''}")
                child.send_signal(signal.SIGKILL)
                child.wait(timeout=10)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait()
                child.stdout.close()
                child.stderr.close()

            # 崩溃瞬间：大文件的正式名字不存在，只有临时文件；已完成的小文件是完整的
            self.assertFalse((dest / "big.mcap").exists())
            partial_size = tmp.stat().st_size
            self.assertTrue(0 < partial_size < SIZE)

            # 新进程接手：续传并完成
            fake = FakeMinio()
            fake.put("b", "p/ep/task_info.json", b"{}")
            fake.put("b", "p/ep/big.mcap", source_bytes())
            store.use_client(fake)
            self.addCleanup(store.use_client, None)
            old = (store.RESUME_BACKOFF, store.CHUNK_SIZE)
            store.RESUME_BACKOFF, store.CHUNK_SIZE = 100_000, 50_000
            self.addCleanup(lambda: (setattr(store, "RESUME_BACKOFF", old[0]), setattr(store, "CHUNK_SIZE", old[1])))
            store.download_prefix("b", "p/ep", dest)

            self.assertEqual((dest / "big.mcap").read_bytes(), source_bytes())
            self.assertEqual((dest / "task_info.json").read_bytes(), b"{}")
            resumed_from = [c["offset"] for c in fake.get_calls if c["key"] == "p/ep/big.mcap"][0]
            self.assertGreater(resumed_from, 0, "应当从断点续传而不是从头开始")
            self.assertLessEqual(resumed_from, partial_size)
            self.assertEqual([p for p in dest.rglob("*") if p.name.endswith(store.TMP_SUFFIX)], [])


if __name__ == "__main__":
    unittest.main()
