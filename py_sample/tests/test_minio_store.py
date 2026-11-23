# -*- coding: utf-8 -*-
"""minio_store 的原子性 / 续传 / 校验测试。只用标准库 unittest，不需要真实 MinIO，也不需要安装 minio 包。

运行：python -m unittest discover -s tests -v
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import minio_store as store          # noqa: E402
from fake_minio import FakeMinio     # noqa: E402

BUCKET = "b"
PREFIX = "2026/ep1"


def payload(n: int, seed: int = 1) -> bytes:
    return bytes((i * 31 + seed) % 251 for i in range(n))


def tree(root: Path) -> dict[str, int]:
    return {str(p.relative_to(root)): p.stat().st_size for p in sorted(root.rglob("*")) if p.is_file()}


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.fake = FakeMinio()
        store.use_client(self.fake)
        patches = [
            mock.patch.object(store, "RETRY_BASE_DELAY", 0.0),
            mock.patch.object(store, "CHUNK_SIZE", 1000),
            mock.patch.object(store, "FILE_RETRIES", 4),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.addCleanup(store.use_client, None)
        self.addCleanup(self.tmp.cleanup)

    def seed_episode(self):
        self.info = b'{"task": "demo"}'
        self.big = payload(10_500)
        self.fake.put(BUCKET, f"{PREFIX}/task_info.json", self.info)
        self.fake.put(BUCKET, f"{PREFIX}/rec.SYNC.mcap", self.big, mtime=1_700_000_100.0)
        self.fake.put(BUCKET, f"{PREFIX}/video/cam0.mp4", payload(3_000, 7))
        self.fake.put(BUCKET, f"{PREFIX}-other/x.bin", b"must not be downloaded")   # 前缀相似的另一条

    def no_tmp_left(self, dest: Path):
        left = [p for p in dest.rglob("*") if p.name.endswith(store.TMP_SUFFIX)]
        self.assertEqual(left, [], f"残留临时文件：{left}")


class DownloadTests(StoreTestCase):
    def test_full_download_is_exact_and_leaves_no_temp(self):
        self.seed_episode()
        dest = self.root / ".ep1.partial"
        report = store.download_prefix(BUCKET, PREFIX, dest)
        self.assertEqual(tree(dest), {"task_info.json": len(self.info), "rec.SYNC.mcap": 10_500, "video/cam0.mp4": 3_000})
        self.assertEqual((dest / "rec.SYNC.mcap").read_bytes(), self.big)
        self.assertEqual(report.files, 3)
        self.assertEqual(report.bytes, len(self.info) + 10_500 + 3_000)
        self.no_tmp_left(dest)
        # 与 mc --preserve 一致：保留对象的修改时间
        self.assertAlmostEqual((dest / "rec.SYNC.mcap").stat().st_mtime, 1_700_000_100.0, delta=1)

    def test_prefix_does_not_leak_into_similar_prefix(self):
        self.seed_episode()
        dest = self.root / "d"
        store.download_prefix(BUCKET, PREFIX, dest)
        self.assertNotIn("x.bin", tree(dest))

    def test_resume_inside_one_run_uses_range_and_result_is_identical(self):
        self.seed_episode()
        key = f"{PREFIX}/rec.SYNC.mcap"
        self.fake.break_plan[key] = [4_000, 3_000]          # 断线两次
        dest = self.root / "d"
        store.download_prefix(BUCKET, PREFIX, dest)
        self.assertEqual((dest / "rec.SYNC.mcap").read_bytes(), self.big)
        calls = [c for c in self.fake.get_calls if c["key"] == key]
        self.assertEqual([c["offset"] for c in calls], [0, 4_000, 7_000])      # 每次从已写入处继续
        self.assertEqual(calls[1]["length"], 10_500 - 4_000)
        self.no_tmp_left(dest)

    def test_new_process_resumes_old_tmp_with_backoff(self):
        self.seed_episode()
        key = f"{PREFIX}/rec.SYNC.mcap"
        dest = self.root / "d"
        dest.mkdir()
        obj = store.stat_object(BUCKET, key)
        tmp = store._tmp_path(dest / "rec.SYNC.mcap", obj)
        tmp.write_bytes(self.big[:6_000] + b"\x00" * 100)       # 上次崩溃留下的：前 6000 字节对，尾部是脏数据
        with mock.patch.object(store, "RESUME_BACKOFF", 1_000):
            store.download_prefix(BUCKET, PREFIX, dest)
        self.assertEqual((dest / "rec.SYNC.mcap").read_bytes(), self.big)
        first = [c for c in self.fake.get_calls if c["key"] == key][0]
        self.assertEqual(first["offset"], 5_100)                 # 6100 - 回退 1000
        self.no_tmp_left(dest)

    def test_changed_object_invalidates_old_tmp(self):
        self.seed_episode()
        key = f"{PREFIX}/rec.SYNC.mcap"
        dest = self.root / "d"
        dest.mkdir()
        old = store.stat_object(BUCKET, key)
        tmp = store._tmp_path(dest / "rec.SYNC.mcap", old)
        tmp.write_bytes(self.big[:5_000])
        new = payload(10_500, seed=99)                           # 对象在 MinIO 上被替换
        self.fake.put(BUCKET, key, new, mtime=1_700_000_200.0)
        store.download_prefix(BUCKET, PREFIX, dest)
        self.assertEqual((dest / "rec.SYNC.mcap").read_bytes(), new)
        self.assertEqual([c["offset"] for c in self.fake.get_calls if c["key"] == key], [0])
        self.no_tmp_left(dest)

    def test_corrupted_stream_never_becomes_final_file(self):
        self.seed_episode()
        key = f"{PREFIX}/task_info.json"
        self.fake.corrupt_once.add(key)
        dest = self.root / "d"
        store.download_prefix(BUCKET, PREFIX, dest)            # 第一次 MD5 不符 → 丢弃 → 重下成功
        self.assertEqual((dest / "task_info.json").read_bytes(), self.info)
        self.assertEqual(len([c for c in self.fake.get_calls if c["key"] == key]), 2)

    def test_persistent_corruption_fails_and_publishes_nothing(self):
        self.seed_episode()
        key = f"{PREFIX}/task_info.json"
        dest = self.root / "d"
        with mock.patch.object(self.fake, "corrupt_once", _Always(key)):
            with self.assertRaises(store.IntegrityError):
                store.download_prefix(BUCKET, PREFIX, dest)
        self.assertFalse((dest / "task_info.json").exists())
        self.assertFalse(any(p.name == "task_info.json.minio-tmp" for p in dest.rglob("*")))

    def test_multipart_etag_skips_md5_but_checks_size(self):
        self.fake.put(BUCKET, f"{PREFIX}/task_info.json", b"{}")
        self.fake.put(BUCKET, f"{PREFIX}/big.bin", self.big if hasattr(self, "big") else payload(2_000), etag="deadbeef-3")
        dest = self.root / "d"
        store.download_prefix(BUCKET, PREFIX, dest)
        self.assertTrue((dest / "big.bin").is_file())

    def test_short_read_is_resumed(self):
        self.seed_episode()
        key = f"{PREFIX}/video/cam0.mp4"
        self.fake.break_plan[key] = [1_000]
        dest = self.root / "d"
        store.download_prefix(BUCKET, PREFIX, dest)
        self.assertEqual((dest / "video/cam0.mp4").read_bytes(), payload(3_000, 7))

    def test_object_replaced_during_download_is_never_published(self):
        key = f"{PREFIX}/task_info.json"
        self.fake.put(BUCKET, f"{PREFIX}/rec.SYNC.mcap", payload(2_000))
        # (a) 普通 ETag：新内容的 MD5 与清单中的旧 ETag 不符
        self.fake.put(BUCKET, key, b'{"task": "demo"}')
        swapped = []

        def swap_plain(k):
            if k == key and not swapped:
                swapped.append(1)
                self.fake.put(BUCKET, key, b'{"task": "v2xx"}')          # 同样 16 字节
        self.fake.on_get = swap_plain
        dest = self.root / "a"
        with self.assertRaises(store.IntegrityError):
            store.download_prefix(BUCKET, PREFIX, dest)
        self.assertFalse((dest / "task_info.json").exists())

        # (b) 分片上传的 ETag（带 "-"）无法算 MD5：靠下载后再次 stat 发现 ETag 变了
        self.fake.put(BUCKET, key, b'{"task": "demo"}', etag="aaaa-2")
        swapped.clear()

        def swap_multipart(k):
            if k == key and not swapped:
                swapped.append(1)
                self.fake.put(BUCKET, key, b'{"task": "v2xx"}', etag="bbbb-2")
        self.fake.on_get = swap_multipart
        dest = self.root / "b"
        with self.assertRaises(store.IntegrityError):
            store.download_prefix(BUCKET, PREFIX, dest)
        self.assertFalse((dest / "task_info.json").exists())

    def test_disk_full_raises_diskfull_and_keeps_no_final_file(self):
        self.seed_episode()
        dest = self.root / "d"

        def boom(f, chunk):
            raise OSError(errno.ENOSPC, "No space left on device")
        with mock.patch.object(store, "_write_chunk", boom):
            with self.assertRaises(store.DiskFull):
                store.download_prefix(BUCKET, PREFIX, dest)
        self.assertEqual([p for p in dest.rglob("*") if p.is_file() and not p.name.endswith(store.TMP_SUFFIX)], [])

    def test_rejects_path_traversal_keys(self):
        self.fake.put(BUCKET, f"{PREFIX}/../evil.txt", b"x")
        with self.assertRaises(store.StoreError):
            store.download_prefix(BUCKET, PREFIX, self.root / "d")
        self.assertFalse((self.root / "evil.txt").exists())

    def test_empty_prefix_is_an_error_not_an_empty_directory(self):
        with self.assertRaises(store.StoreError):
            store.download_prefix(BUCKET, "nothing/here", self.root / "d")

    def test_extra_and_stale_files_in_staging_are_removed(self):
        self.seed_episode()
        dest = self.root / "d"
        (dest / "old").mkdir(parents=True)
        (dest / "old/leftover.txt").write_text("x")
        (dest / ".rec.SYNC.mcap.0000.minio-tmp").write_bytes(b"stale")
        store.download_prefix(BUCKET, PREFIX, dest)
        self.assertEqual(set(tree(dest)), {"task_info.json", "rec.SYNC.mcap", "video/cam0.mp4"})
        self.assertFalse((dest / "old").exists())

    def test_completed_files_are_not_downloaded_again(self):
        self.seed_episode()
        dest = self.root / "d"
        store.download_prefix(BUCKET, PREFIX, dest)
        self.fake.get_calls.clear()
        report = store.download_prefix(BUCKET, PREFIX, dest)
        self.assertEqual(self.fake.get_calls, [])
        self.assertEqual(report.skipped_files, 3)

    def test_listing_error_is_unavailable_not_missing(self):
        self.fake.fail_list = 1
        with self.assertRaises(store.StoreUnavailable):
            store.list_names(BUCKET, PREFIX)

    def test_zero_byte_object(self):
        self.fake.put(BUCKET, f"{PREFIX}/empty.txt", b"")
        self.fake.put(BUCKET, f"{PREFIX}/task_info.json", b"{}")
        dest = self.root / "d"
        store.download_prefix(BUCKET, PREFIX, dest)
        self.assertEqual((dest / "empty.txt").read_bytes(), b"")


class _Always(set):
    """corrupt_once 的替身：每次都认为需要损坏（discard 不生效）。"""

    def __init__(self, key):
        super().__init__({key})

    def discard(self, _):
        pass


class QueryTests(StoreTestCase):
    def test_stat_and_exists(self):
        self.seed_episode()
        self.assertTrue(store.object_exists(BUCKET, f"{PREFIX}/task_info.json"))
        self.assertFalse(store.object_exists(BUCKET, f"{PREFIX}/nope.json"))
        self.assertEqual(store.stat_object(BUCKET, f"{PREFIX}/task_info.json").size, len(self.info))

    def test_list_names_is_one_level_only(self):
        self.seed_episode()
        self.assertEqual(sorted(store.list_names(BUCKET, PREFIX)), ["rec.SYNC.mcap", "task_info.json"])


class PublishTests(StoreTestCase):
    def test_publish_is_atomic_rename(self):
        staging, final = self.root / ".ep.partial", self.root / "ep"
        staging.mkdir()
        (staging / "a.txt").write_text("hi")
        store.publish_dir(staging, final)
        self.assertFalse(staging.exists())
        self.assertEqual((final / "a.txt").read_text(), "hi")

    def test_publish_refuses_to_replace_existing(self):
        staging, final = self.root / ".ep.partial", self.root / "ep"
        staging.mkdir(); final.mkdir()
        with self.assertRaises(FileExistsError):
            store.publish_dir(staging, final)
        self.assertTrue(staging.exists())


class ConfigTests(unittest.TestCase):
    def test_env_wins_and_scheme_decides_tls(self):
        cfg = store.load_config(environ={"MINIO_ENDPOINT": "http://10.0.0.5:9000", "MINIO_ACCESS_KEY": "a", "MINIO_SECRET_KEY": "s"})
        self.assertEqual((cfg.endpoint, cfg.secure), ("10.0.0.5:9000", False))
        cfg = store.load_config(environ={"MINIO_ENDPOINT": "minio.local", "MINIO_ACCESS_KEY": "a", "MINIO_SECRET_KEY": "s", "MINIO_SECURE": "0"})
        self.assertEqual((cfg.endpoint, cfg.secure), ("minio.local", False))

    def test_env_requires_both_keys(self):
        with self.assertRaises(store.StoreError):
            store.load_config(environ={"MINIO_ENDPOINT": "x:9000"})

    def test_json_file(self):
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "m.json"
            f.write_text(json.dumps({"endpoint": "https://m.example.com", "access_key": "k", "secret_key": "v"}))
            cfg = store.load_config(config_file=f, environ={})
            self.assertEqual((cfg.endpoint, cfg.secure, cfg.access_key), ("m.example.com", True, "k"))

    def test_legacy_mc_alias_is_read_without_running_mc(self):
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "config.json").write_text(json.dumps(
                {"version": "10", "aliases": {"minio1": {"url": "http://192.168.1.9:9000", "accessKey": "AK", "secretKey": "SK"}}}))
            cfg = store.load_config("minio1", config_file=Path(d) / "absent.json", environ={"MC_CONFIG_DIR": d})
            self.assertEqual((cfg.endpoint, cfg.secure, cfg.access_key, cfg.secret_key), ("192.168.1.9:9000", False, "AK", "SK"))

    def test_nothing_configured(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(store.StoreError):
                store.load_config("minio1", config_file=Path(d) / "absent.json", environ={"MC_CONFIG_DIR": d})


if __name__ == "__main__":
    unittest.main()
