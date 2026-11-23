# -*- coding: utf-8 -*-
"""测试用的内存版 MinIO 客户端：接口与 minio.Minio 的 list_objects / stat_object / get_object 对齐，
并支持注入“中途断线”“内容损坏”“对象被改动”等故障。"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import urllib3


class S3Error(Exception):
    """类名与 minio.error.S3Error 相同，minio_store 按类名 + code 识别。"""

    def __init__(self, code: str, message: str = ""):
        super().__init__(message or code)
        self.code = code


@dataclass
class FakeObject:
    object_name: str
    size: int
    etag: str
    last_modified: datetime
    is_dir: bool = False


class FakeResponse:
    def __init__(self, data: bytes, break_after: int | None = None, delay: float = 0.0):
        self._data = data
        self._break_after = break_after
        self._delay = delay
        self.closed = False

    def stream(self, amt: int = 1024 * 1024):
        sent = 0
        for i in range(0, len(self._data), max(1, amt)):
            chunk = self._data[i:i + amt]
            if self._break_after is not None and sent + len(chunk) > self._break_after:
                part = chunk[: max(0, self._break_after - sent)]
                if part:
                    yield part
                raise urllib3.exceptions.ProtocolError("Connection broken (fake)")
            sent += len(chunk)
            if self._delay:
                time.sleep(self._delay)
            yield chunk

    def close(self):
        self.closed = True

    def release_conn(self):
        pass


class FakeMinio:
    def __init__(self):
        self.store: dict[tuple[str, str], dict] = {}
        self.get_calls: list[dict] = []
        self.break_plan: dict[str, list[int]] = {}     # key -> 每次 get_object 在多少字节后断线（依次消耗）
        self.corrupt_once: set[str] = set()            # key -> 下一次 get_object 返回被改坏的内容
        self.fail_list = 0                             # 接下来 N 次 list_objects 抛网络错误
        self.on_get = None                             # 回调(key)：用于模拟下载期间对象被修改
        self.chunk_delay = 0.0                         # 每个数据块之间的延迟（秒），用于制造“下载进行中”的窗口

    # ---- 准备数据 ----
    def put(self, bucket: str, key: str, data: bytes, etag: str | None = None, mtime: float = 1_700_000_000.0):
        self.store[(bucket, key)] = {
            "data": data,
            "etag": etag if etag is not None else hashlib.md5(data).hexdigest(),
            "mtime": datetime.fromtimestamp(mtime, tz=timezone.utc),
        }

    def _obj(self, bucket: str, key: str) -> FakeObject:
        rec = self.store[(bucket, key)]
        return FakeObject(key, len(rec["data"]), rec["etag"], rec["mtime"])

    # ---- minio.Minio 同名接口 ----
    def list_objects(self, bucket: str, prefix: str | None = None, recursive: bool = False):
        if self.fail_list > 0:
            self.fail_list -= 1
            raise urllib3.exceptions.ProtocolError("list failed (fake)")
        prefix = prefix or ""
        seen_dirs: set[str] = set()
        for (b, key) in sorted(self.store):
            if b != bucket or not key.startswith(prefix):
                continue
            rest = key[len(prefix):]
            if not recursive and "/" in rest:
                d = prefix + rest.split("/", 1)[0] + "/"
                if d not in seen_dirs:
                    seen_dirs.add(d)
                    yield FakeObject(d, 0, "", datetime.fromtimestamp(0, tz=timezone.utc), True)
                continue
            yield self._obj(b, key)

    def stat_object(self, bucket: str, key: str):
        if (bucket, key) not in self.store:
            raise S3Error("NoSuchKey", f"{key} not found")
        return self._obj(bucket, key)

    def get_object(self, bucket: str, key: str, offset: int = 0, length: int = 0, **_kw):
        if (bucket, key) not in self.store:
            raise S3Error("NoSuchKey")
        self.get_calls.append({"key": key, "offset": offset, "length": length})
        if self.on_get:
            self.on_get(key)
        data = self.store[(bucket, key)]["data"]
        end = len(data) if not length else offset + length
        body = data[offset:end]
        if key in self.corrupt_once and body:
            self.corrupt_once.discard(key)
            body = bytes([body[0] ^ 0xFF]) + body[1:]
        plan = self.break_plan.get(key)
        break_after = plan.pop(0) if plan else None
        return FakeResponse(body, break_after, self.chunk_delay)
