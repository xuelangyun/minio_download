#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MinIO 访问层：使用官方 ``minio`` Python SDK，不依赖 mc 命令。

对外提供：
    load_config / get_client      连接配置与（每进程一个的）客户端
    stat_object / object_exists   查询单个对象
    list_objects / list_names     列目录
    download_prefix               把一个前缀（一条 episode）完整下载到“暂存目录”
    publish_dir                   把写完的暂存目录原子地改名为正式目录

原子性保证（数据要么完整出现，要么根本不出现）：
    1. 文件级：每个对象先写入同目录下的临时文件 ``.<名字>.<标记>.minio-tmp``，
       校验（字节数 + 可用时的 MD5/ETag）并 fsync 之后，才用 os.replace 改成正式文件名。
       读取方永远看不到写了一半的正式文件。
    2. 目录级：整条 episode 先下载到暂存目录（脚本里是 ``.<名字>.partial``），
       全部文件校验通过、清单与 MinIO 完全一致后，才用一次 rename 变成正式目录。
    3. 续传：中断后留下的临时文件按 ETag 标记识别；对象在 MinIO 上被改动则标记不同，旧临时文件作废。
       续传时会回退一段（RESUME_BACKOFF），防止断电造成的“长度对但尾部是脏数据”。
"""

from __future__ import annotations

import concurrent.futures as cf
import errno
import hashlib
import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable
from urllib.parse import urlparse

from compat import IS_WINDOWS


# ============================ 可调参数（环境变量可覆盖） ============================
def _truthy(value: Any, default: bool) -> bool:
    if value is None or value == "":
        return default
    return str(value).strip().lower() not in ("0", "false", "no", "off")


# 下面这些默认值会在启动时被配置库（SQLite）里的值覆盖，见 apply_tuning()。
CHUNK_SIZE = 1024 * 1024                              # 每次从网络读取的字节数
FILE_WORKERS = 4                                      # 一条 episode 内并行下载的文件数
FILE_RETRIES = 5                                      # 单个文件的最大尝试次数（网络错误会续传）
RETRY_BASE_DELAY = 1.0                                # 重试间隔 = 基数 × 2^次数，最多 30 秒
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 120
FSYNC = True                                          # 改名前 fsync 文件与目录（防断电丢数据）
VERIFY_MD5 = True                                     # ETag 是普通 MD5 时校验内容
FSYNC_INTERVAL = 32 * 1024 * 1024                     # 大文件每写这么多字节 fsync 一次
RESUME_BACKOFF = 64 * 1024 * 1024                     # 新进程续传时回退的字节数（> FSYNC_INTERVAL）
TMP_SUFFIX = ".minio-tmp"
DEFAULT_CONFIG_FILE = Path(__file__).resolve().parent / "minio_config.json"


# ================================ 异常 ================================
class StoreError(RuntimeError):
    """MinIO 访问或落盘失败（基类，不可重试）。"""


class StoreUnavailable(StoreError):
    """网络、超时、服务端 5xx 等暂时性问题，可以稍后重试 / 续传。"""


class IntegrityError(StoreError):
    """内容校验失败：字节数不符、MD5 与 ETag 不符、对象在下载期间被改动。"""


class DiskFull(StoreError):
    """本地磁盘空间 / 配额不足。"""


class StoreStopped(StoreError):
    """收到暂停请求：已写入的数据保留在临时文件里，下次运行从断点续传。"""


# 暂停信号：置位后，所有正在下载的文件会在下一个数据块之前安全停下（不丢已写入的数据）。
STOP = threading.Event()


def _check_abort(abort: threading.Event) -> None:
    if STOP.is_set():
        raise StoreStopped("已按请求暂停")
    if abort.is_set():
        raise StoreError("已中止（同一条数据的其他文件失败）")


def _sleep_interruptible(seconds: float, abort: threading.Event) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        _check_abort(abort)
        time.sleep(min(0.2, max(0.0, deadline - time.monotonic())))


# ================================ 连接配置 ================================
@dataclass(frozen=True)
class StoreConfig:
    endpoint: str                 # host[:port]，不含协议
    access_key: str
    secret_key: str
    secure: bool = True
    region: str | None = None
    cert_check: bool = True
    source: str = ""              # 配置来自哪里（只用于日志，不含密钥）

    def describe(self) -> str:
        return f"{'https' if self.secure else 'http'}://{self.endpoint}"


def _split_endpoint(raw: str, secure_default: bool) -> tuple[str, bool]:
    raw = raw.strip()
    if "://" in raw:
        parsed = urlparse(raw)
        return parsed.netloc, parsed.scheme.lower() == "https"
    return raw.rstrip("/"), secure_default


def load_config(legacy_alias: str = "minio1", config_file: str | os.PathLike | None = None,
                environ: dict[str, str] | None = None) -> StoreConfig:
    """按优先级读取连接配置：环境变量 > minio_config.json > ~/.mc/config.json 中的 alias（只读文件，不调用 mc）。"""
    env = os.environ if environ is None else environ

    endpoint = env.get("MINIO_ENDPOINT")
    if endpoint:
        access, secret = env.get("MINIO_ACCESS_KEY"), env.get("MINIO_SECRET_KEY")
        if not access or not secret:
            raise StoreError("已设置 MINIO_ENDPOINT，但缺少 MINIO_ACCESS_KEY / MINIO_SECRET_KEY。")
        host, secure = _split_endpoint(endpoint, _truthy(env.get("MINIO_SECURE"), True))
        return StoreConfig(host, access, secret, secure, env.get("MINIO_REGION") or None,
                           _truthy(env.get("MINIO_CERT_CHECK"), True), "环境变量 MINIO_*")

    path = Path(config_file or env.get("MINIO_CONFIG_FILE") or DEFAULT_CONFIG_FILE)
    if path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            host, secure = _split_endpoint(str(data["endpoint"]), _truthy(data.get("secure"), True))
            return StoreConfig(host, str(data["access_key"]), str(data["secret_key"]), secure,
                               data.get("region") or None, _truthy(data.get("cert_check"), True), str(path))
        except (ValueError, KeyError, TypeError) as exc:
            raise StoreError(f"MinIO 配置文件无法解析：{path}（需要 endpoint / access_key / secret_key）：{exc!r}") from exc

    mc_dir = Path(env.get("MC_CONFIG_DIR") or (Path.home() / ".mc"))
    mc_file = mc_dir / "config.json"
    if legacy_alias and mc_file.is_file():
        try:
            data = json.loads(mc_file.read_text(encoding="utf-8"))
            entry = (data.get("aliases") or data.get("hosts") or {}).get(legacy_alias)
            if entry:
                host, secure = _split_endpoint(str(entry["url"]), True)
                return StoreConfig(host, str(entry["accessKey"]), str(entry["secretKey"]), secure,
                                   None, True, f"{mc_file}（alias {legacy_alias}）")
        except (ValueError, KeyError, TypeError) as exc:
            logging.warning("读取 %s 失败：%r", mc_file, exc)

    raise StoreError(
        "未找到 MinIO 连接配置。请在 Web 控制台“连接”页填写服务地址与密钥"
        "（首次创建配置库时也会导入环境变量 MINIO_* 或旧版 minio_config.json）。"
    )


# ================================ 客户端 ================================
_client_lock = threading.Lock()
_clients: dict[int, Any] = {}
_override: Any = None
_config_override: StoreConfig | None = None
_legacy_alias = "minio1"


def set_config(cfg: StoreConfig | None) -> None:
    """指定连接配置（下载脚本启动时用配置库里的值调用）。传 None 恢复为 load_config 的查找顺序。"""
    global _config_override
    with _client_lock:
        _config_override = cfg
        _clients.clear()


def config_from_settings(values: dict[str, Any]) -> StoreConfig:
    """由配置库里的 minio.* 设置项构造连接配置。"""
    endpoint = str(values.get("minio.endpoint") or "").strip()
    access, secret = str(values.get("minio.access_key") or ""), str(values.get("minio.secret_key") or "")
    if not endpoint:
        raise StoreError("尚未配置 MinIO 服务地址（Web 控制台 →“连接”页）")
    if not access or not secret:
        raise StoreError("尚未配置 MinIO 的 Access Key / Secret Key（Web 控制台 →“连接”页）")
    host, secure = _split_endpoint(endpoint, bool(values.get("minio.secure", True)))
    return StoreConfig(host, access, secret, secure, str(values.get("minio.region") or "") or None,
                       bool(values.get("minio.cert_check", True)), "配置库")


def apply_tuning(values: dict[str, Any]) -> None:
    """把配置库里的 transfer.* 设置项应用到本模块。"""
    global FILE_WORKERS, FILE_RETRIES, RETRY_BASE_DELAY, CONNECT_TIMEOUT, READ_TIMEOUT, FSYNC, VERIFY_MD5
    FILE_WORKERS = int(values.get("transfer.file_workers", FILE_WORKERS))
    FILE_RETRIES = int(values.get("transfer.file_retries", FILE_RETRIES))
    RETRY_BASE_DELAY = float(values.get("transfer.retry_delay", RETRY_BASE_DELAY))
    CONNECT_TIMEOUT = int(values.get("transfer.connect_timeout", CONNECT_TIMEOUT))
    READ_TIMEOUT = int(values.get("transfer.read_timeout", READ_TIMEOUT))
    FSYNC = bool(values.get("transfer.fsync", FSYNC))
    VERIFY_MD5 = bool(values.get("transfer.verify_md5", VERIFY_MD5))


def use_client(client: Any) -> None:
    """注入一个现成的客户端（测试，或需要自定义 http_client 时）。传 None 取消。"""
    global _override
    _override = client


def set_legacy_alias(alias: str) -> None:
    global _legacy_alias
    _legacy_alias = alias


def _build_client(cfg: StoreConfig) -> Any:
    try:
        import urllib3
        from minio import Minio
    except ImportError as exc:
        raise StoreError("缺少依赖：请先执行 pip install minio") from exc
    http = urllib3.PoolManager(
        timeout=urllib3.Timeout(connect=CONNECT_TIMEOUT, read=READ_TIMEOUT),
        maxsize=32,
        cert_reqs="CERT_REQUIRED" if cfg.cert_check else "CERT_NONE",
        retries=urllib3.Retry(total=5, backoff_factor=0.5, status_forcelist=(500, 502, 503, 504)),
    )
    return Minio(cfg.endpoint, access_key=cfg.access_key, secret_key=cfg.secret_key,
                 secure=cfg.secure, region=cfg.region, http_client=http)


def get_client() -> Any:
    """每个进程一个客户端（线程安全；fork 之后的子进程会重新创建，不共用连接池）。"""
    if _override is not None:
        return _override
    pid = os.getpid()
    with _client_lock:
        client = _clients.get(pid)
        if client is None:
            cfg = _config_override or load_config(_legacy_alias)
            client = _clients[pid] = _build_client(cfg)
            logging.info("MinIO 客户端已创建：%s（配置来源：%s）", cfg.describe(), cfg.source)
        return client


def connection_summary() -> dict[str, Any]:
    """给 Web 界面显示用：不含密钥。"""
    if _override is not None:
        return {"endpoint": "(injected client)", "source": "test"}
    try:
        cfg = _config_override or load_config(_legacy_alias)
    except StoreError as exc:
        return {"error": str(exc)}
    return {"endpoint": cfg.describe(), "source": cfg.source}


# ================================ 错误归类 ================================
_TRANSIENT_CODES = {
    "InternalError", "ServiceUnavailable", "SlowDown", "RequestTimeout", "OperationAborted",
    "XMinioServerNotInitialized", "XMinioServerBusy", "ReadQuorum", "WriteQuorum",
}
_NOT_FOUND_CODES = {"NoSuchKey", "NoSuchObject", "NoSuchVersion", "NotFound"}
_SDK_EXC_NAMES = {"S3Error", "ServerError", "InvalidResponseError", "MinioException"}


def _is_not_found(exc: BaseException) -> bool:
    return type(exc).__name__ == "S3Error" and getattr(exc, "code", None) in _NOT_FOUND_CODES


def _as_store_error(exc: BaseException) -> StoreError | None:
    """SDK / 网络层异常 → StoreError；其它（程序错误）返回 None，让调用方原样抛出。"""
    if isinstance(exc, StoreError):
        return exc
    name, code = type(exc).__name__, getattr(exc, "code", None)
    if name == "S3Error":
        if code in _TRANSIENT_CODES:
            return StoreUnavailable(f"MinIO 暂时不可用（{code}）：{exc}")
        return StoreError(f"MinIO 拒绝了请求（{code}）：{exc}")
    is_net = name in _SDK_EXC_NAMES or isinstance(exc, OSError)
    if not is_net:
        try:
            import urllib3
            is_net = isinstance(exc, urllib3.exceptions.HTTPError)
        except ImportError:
            pass
    return StoreUnavailable(f"访问 MinIO 失败：{name}: {exc}") if is_net else None


def _reraise(exc: BaseException) -> None:
    err = _as_store_error(exc)
    if err is None:
        raise exc
    if err is exc:
        raise exc
    raise err from exc


def _local_io_error(exc: OSError) -> StoreError:
    if exc.errno in (errno.ENOSPC, errno.EDQUOT):
        return DiskFull(f"本地磁盘空间不足：{exc}")
    return StoreError(f"本地文件操作失败：{exc}")


# ================================ 查询 ================================
@dataclass(frozen=True)
class RemoteObject:
    key: str            # 桶内完整对象名
    rel: str            # 相对于所下载前缀的路径
    size: int
    etag: str
    mtime: float | None
    is_dir: bool = False


def _clean_etag(etag: Any) -> str:
    return str(etag or "").strip('"')


def _timestamp(value: Any) -> float | None:
    try:
        return float(value.timestamp()) if value is not None else None
    except (AttributeError, OSError, OverflowError, ValueError):
        return None


def _norm_prefix(prefix: str) -> str:
    prefix = prefix.strip("/")
    return prefix + "/" if prefix else ""


def stat_object(bucket: str, key: str) -> RemoteObject | None:
    """对象不存在返回 None；网络 / 服务端问题抛 StoreUnavailable。"""
    try:
        st = get_client().stat_object(bucket, key)
    except Exception as exc:
        if _is_not_found(exc):
            return None
        _reraise(exc)
    return RemoteObject(key, PurePosixPath(key).name, int(getattr(st, "size", 0) or 0),
                        _clean_etag(getattr(st, "etag", "")), _timestamp(getattr(st, "last_modified", None)))


def object_exists(bucket: str, key: str) -> bool:
    return stat_object(bucket, key) is not None


def list_objects(bucket: str, prefix: str, recursive: bool = False) -> list[RemoteObject]:
    base = _norm_prefix(prefix)
    found: list[RemoteObject] = []
    try:
        for obj in get_client().list_objects(bucket, prefix=base, recursive=recursive):
            name = str(obj.object_name)
            found.append(RemoteObject(
                name, name[len(base):], int(getattr(obj, "size", 0) or 0),
                _clean_etag(getattr(obj, "etag", "")), _timestamp(getattr(obj, "last_modified", None)),
                bool(getattr(obj, "is_dir", False)) or name.endswith("/"),
            ))
    except Exception as exc:
        _reraise(exc)
    return found


def list_names(bucket: str, prefix: str) -> list[str]:
    """前缀下直接子文件的名字（不递归、不含子目录）。"""
    return [o.rel for o in list_objects(bucket, prefix, recursive=False) if not o.is_dir and "/" not in o.rel]


def _probe(client: Any, bucket: str) -> None:
    try:
        next(iter(client.list_objects(bucket, recursive=False)), None)
    except Exception as exc:
        code = getattr(exc, "code", None)
        if code == "NoSuchBucket":
            raise StoreError(f"bucket 不存在：{bucket}") from exc
        if code in ("AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
            raise StoreError(f"MinIO 凭据无效或无权限访问 bucket {bucket}（{code}）") from exc
        _reraise(exc)


def verify_connection(bucket: str) -> str:
    """启动时检查：能连上并能读取该 bucket。返回人类可读的说明（不含密钥）。"""
    _probe(get_client(), bucket)
    summary = connection_summary()
    return f"{summary.get('endpoint')}，bucket={bucket}（配置来源：{summary.get('source')}）"


def test_connection(cfg: StoreConfig, bucket: str) -> str:
    """用给定配置临时建一个客户端测试（Web“测试连接”按钮用，不影响全局客户端）。"""
    _probe(_build_client(cfg), bucket)
    return f"{cfg.describe()}，bucket={bucket}"


# ================================ 下载 ================================
@dataclass
class DownloadReport:
    files: int = 0
    bytes: int = 0
    resumed_files: int = 0
    skipped_files: int = 0
    seconds: float = 0.0


_WIN_BAD_CHARS = re.compile(r'[<>:"|?*\\\x00-\x1f]')
_WIN_RESERVED = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}


def _safe_rel(rel: str) -> PurePosixPath:
    p = PurePosixPath(rel)
    if not rel or p.is_absolute() or any(part in ("", ".", "..") for part in p.parts):
        raise StoreError(f"对象名包含不安全的路径，拒绝写入本地：{rel!r}")
    if IS_WINDOWS:
        for part in p.parts:
            if _WIN_BAD_CHARS.search(part) or part.endswith((" ", ".")) or part.split(".")[0].upper() in _WIN_RESERVED:
                raise StoreError(f"对象名含 Windows 不支持的字符或保留名称，无法写入本地：{rel!r}")
    return p


def _fs_retry(fn: Callable[..., Any], *args: Any, attempts: int = 6) -> Any:
    """Windows 上改名 / 替换文件时，杀毒软件或索引服务可能短暂占用文件（PermissionError），稍等重试。"""
    for i in range(attempts):
        try:
            return fn(*args)
        except PermissionError:
            if not IS_WINDOWS or i == attempts - 1:
                raise
            time.sleep(0.2 * (i + 1))


def _trunc_bytes(text: str, limit: int) -> str:
    return text.encode("utf-8")[:limit].decode("utf-8", "ignore")


def _tmp_path(path: Path, obj: RemoteObject) -> Path:
    tag = hashlib.sha1(f"{obj.etag}|{obj.size}|{obj.mtime}".encode()).hexdigest()[:12]
    return path.with_name(f".{_trunc_bytes(path.name, 60)}.{tag}{TMP_SUFFIX}")


def _fsync_dir(path: Path) -> None:
    if not FSYNC:
        return
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _fsync_tree(root: Path) -> None:
    if not FSYNC:
        return
    for dirpath, _dirs, _files in os.walk(root):
        _fsync_dir(Path(dirpath))


def _write_chunk(f, chunk: bytes) -> None:       # 单独成函数，便于测试模拟磁盘写满
    f.write(chunk)


def _md5_of_prefix(path: Path, nbytes: int) -> "hashlib._Hash":
    h = hashlib.md5()
    remaining = nbytes
    with path.open("rb") as f:
        while remaining > 0:
            block = f.read(min(8 * 1024 * 1024, remaining))
            if not block:
                break
            h.update(block)
            remaining -= len(block)
    return h


def _already_complete(path: Path, obj: RemoteObject) -> bool:
    """正式文件只会由 os.replace 产生，所以大小一致（且修改时间与对象一致）就视为已完成。"""
    try:
        st = path.stat()
    except OSError:
        return False
    if not path.is_file() or st.st_size != obj.size:
        return False
    return obj.mtime is None or abs(st.st_mtime - obj.mtime) < 2.0


def _should_verify_md5(obj: RemoteObject) -> bool:
    return VERIFY_MD5 and re.fullmatch(r"[0-9a-fA-F]{32}", obj.etag or "") is not None


def _sync_quietly(f) -> None:
    try:
        f.flush()
        if FSYNC:
            os.fsync(f.fileno())
    except OSError:
        pass


def _pump(bucket: str, obj: RemoteObject, f, offset: int, md5: Any, abort: threading.Event) -> int:
    """从 MinIO 读取 [offset, size) 写入已打开的临时文件，返回写入后的总字节数。"""
    size = obj.size
    kwargs = {"offset": offset, "length": size - offset} if offset else {}
    try:
        resp = get_client().get_object(bucket, obj.key, **kwargs)
    except Exception as exc:
        _reraise(exc)
    written, since_sync = offset, 0
    try:
        chunks = resp.stream(CHUNK_SIZE)
        while True:
            _check_abort(abort)                      # 暂停 / 同批文件失败：在数据块边界安全停下
            try:
                chunk = next(chunks)
            except StopIteration:
                break
            except Exception as exc:
                _reraise(exc)
            if written + len(chunk) > size:
                raise IntegrityError(f"{obj.rel}：服务器返回的数据多于对象大小 {size}")
            _write_chunk(f, chunk)
            if md5 is not None:
                md5.update(chunk)
            written += len(chunk)
            since_sync += len(chunk)
            if FSYNC and since_sync >= FSYNC_INTERVAL:
                f.flush()
                os.fsync(f.fileno())
                since_sync = 0
    finally:
        for closer in ("close", "release_conn"):
            try:
                getattr(resp, closer)()
            except Exception:
                pass
    return written


def _stream_to_tmp(bucket: str, obj: RemoteObject, tmp: Path, abort: threading.Event,
                   first_attempt: bool, emit: Callable[[str], None]) -> int:
    """把对象写入临时文件并校验。返回本次是否续传（起始偏移，>0 表示续传）。"""
    size = obj.size
    offset = 0
    if tmp.exists():
        have = tmp.stat().st_size
        if have > size:
            have = 0
        # 新进程接手旧临时文件：回退一段，避免断电后尾部残留未落盘的脏数据。
        offset = max(0, have - RESUME_BACKOFF) if first_attempt else have
    verify = _should_verify_md5(obj)

    md5 = _md5_of_prefix(tmp, offset) if (verify and offset) else (hashlib.md5() if verify else None)
    written = offset

    with open(tmp, "r+b" if tmp.exists() else "wb") as f:
        f.seek(offset)
        f.truncate()
        if offset:
            emit(f"续传 {obj.rel}：从 {offset} / {size} 字节继续")
        try:
            if size > offset:
                written = _pump(bucket, obj, f, offset, md5, abort)
        except BaseException:
            _sync_quietly(f)          # 失败 / 暂停时也尽量把已写入的数据落盘，续传才能接着用
            raise
        f.flush()
        if FSYNC:
            os.fsync(f.fileno())

    if written != size:
        raise StoreUnavailable(f"{obj.rel}：连接提前结束，已写入 {written}/{size} 字节，将续传")
    if md5 is not None and md5.hexdigest().lower() != obj.etag.lower():
        raise IntegrityError(f"{obj.rel}：内容 MD5 与 ETag 不一致（{md5.hexdigest()} != {obj.etag}）")
    return offset


def _fetch_one(bucket: str, obj: RemoteObject, path: Path, abort: threading.Event,
               emit: Callable[[str], None]) -> tuple[str, int]:
    path.parent.mkdir(parents=True, exist_ok=True)
    if _already_complete(path, obj):
        emit(f"已存在，跳过 {obj.rel}（{obj.size} 字节）")
        return "skipped", obj.size
    tmp = _tmp_path(path, obj)
    started = time.monotonic()
    last: StoreError | None = None
    for attempt in range(1, max(1, FILE_RETRIES) + 1):
        _check_abort(abort)
        try:
            offset = _stream_to_tmp(bucket, obj, tmp, abort, attempt == 1, emit)
            current = stat_object(bucket, obj.key)          # 下载期间对象被改动 → 作废重来
            if current is None or current.etag != obj.etag or current.size != obj.size:
                raise IntegrityError(f"{obj.rel}：对象在下载期间被修改或删除")
            if obj.mtime is not None:
                os.utime(tmp, (obj.mtime, obj.mtime))
            _fs_retry(os.replace, tmp, path)                 # 原子：正式文件名只会指向完整文件
            elapsed = max(time.monotonic() - started, 1e-6)
            emit(f"完成 {obj.rel}：{obj.size} 字节，{obj.size / elapsed / 1048576:.1f} MiB/s"
                 + ("（续传）" if offset else ""))
            return ("resumed" if offset else "downloaded"), obj.size
        except (DiskFull, StoreStopped):
            raise
        except IntegrityError as exc:
            last = exc
            emit(f"校验失败，丢弃临时文件后重试（第 {attempt} 次）：{exc}")
            tmp.unlink(missing_ok=True)
        except StoreUnavailable as exc:
            last = exc
            emit(f"下载中断（第 {attempt} 次）：{exc}")
        except StoreError:
            raise
        except OSError as exc:
            raise _local_io_error(exc) from exc
        if attempt < FILE_RETRIES:
            _sleep_interruptible(min(RETRY_BASE_DELAY * 2 ** attempt, 30), abort)
    assert last is not None
    raise last


def _sweep_foreign(dest: Path, keep_files: set[Path], keep_dirs: set[Path], emit: Callable[[str], None]) -> None:
    """暂存目录里只保留本次清单内的文件与临时文件：其它一律删除，保证最终目录与 MinIO 清单一致。"""
    for dirpath, dirnames, filenames in os.walk(dest, topdown=False):
        base = Path(dirpath)
        for name in filenames + [d for d in dirnames if (base / d).is_symlink()]:
            p = base / name
            if p not in keep_files:
                p.unlink(missing_ok=True)
                emit(f"清理暂存目录中不属于本次清单的文件：{p.relative_to(dest)}")
        if base != dest and base not in keep_dirs:
            try:
                base.rmdir()
            except OSError:
                pass


def download_prefix(bucket: str, prefix: str, dest: Path, log_line: Callable[[str], None] | None = None,
                    file_workers: int | None = None) -> DownloadReport:
    """把 ``bucket/prefix/`` 下所有对象下载到 dest（必须是专用的暂存目录）。

    成功返回时：dest 里恰好是 MinIO 上的那一份文件清单，每个文件都已校验并 fsync。
    失败时抛 DiskFull / StoreUnavailable / IntegrityError / StoreError；已完成的文件与可续传的临时文件保留。
    """
    emit = log_line or (lambda msg: logging.info("%s", msg))
    started = time.monotonic()
    objects = list_objects(bucket, prefix, recursive=True)
    files = [(o, _safe_rel(o.rel)) for o in objects if not o.is_dir]
    markers = [_safe_rel(o.rel.rstrip("/")) for o in objects if o.is_dir and o.rel.strip("/")]
    if not files:
        raise StoreError(f"MinIO 前缀下没有任何文件：{bucket}/{prefix}")

    dest.mkdir(parents=True, exist_ok=True)
    keep_files: set[Path] = set()
    keep_dirs: set[Path] = set()
    targets: list[tuple[RemoteObject, Path]] = []
    for obj, rel in files:
        path = dest / Path(*rel.parts)
        targets.append((obj, path))
        keep_files.update({path, _tmp_path(path, obj)})
        keep_dirs.update(p for p in path.parents if p != dest and dest in p.parents)
    for rel in markers:
        (dest / Path(*rel.parts)).mkdir(parents=True, exist_ok=True)
        keep_dirs.add(dest / Path(*rel.parts))
    _sweep_foreign(dest, keep_files, keep_dirs, emit)

    emit(f"清单：{len(files)} 个文件，共 {sum(o.size for o, _ in files)} 字节；并行 {min(file_workers or FILE_WORKERS, len(files))} 路")
    abort = threading.Event()
    report = DownloadReport()
    errors: list[Exception] = []
    workers = max(1, min(file_workers or FILE_WORKERS, len(files)))
    with cf.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="minio-file") as pool:
        futures = [pool.submit(_fetch_one, bucket, obj, path, abort, emit) for obj, path in targets]
        try:
            for fut in cf.as_completed(futures):
                try:
                    state, nbytes = fut.result()
                except Exception as exc:
                    errors.append(exc)
                    abort.set()
                    continue
                report.files += 1
                report.bytes += nbytes
                report.skipped_files += state == "skipped"
                report.resumed_files += state == "resumed"
        except BaseException:
            abort.set()
            raise
    if errors:
        for kind in (DiskFull, StoreStopped):
            hit = next((e for e in errors if isinstance(e, kind)), None)
            if hit is not None:
                raise hit
        raise errors[0]

    # 最终核对：每个文件都在、大小与 MinIO 一致，且没有多余文件
    for obj, path in targets:
        try:
            actual = path.stat().st_size
        except OSError as exc:
            raise IntegrityError(f"{obj.rel}：下载后文件缺失：{exc}") from exc
        if actual != obj.size:
            raise IntegrityError(f"{obj.rel}：本地 {actual} 字节，MinIO 为 {obj.size} 字节")
    _sweep_foreign(dest, {p for _, p in targets}, keep_dirs, emit)
    _fsync_tree(dest)
    report.seconds = time.monotonic() - started
    return report


def publish_dir(staging: Path, final: Path) -> None:
    """把完整的暂存目录原子地改名为正式目录。

    同一文件系统内的 rename 是原子的：读取方要么看不到 final，要么看到完整的 final。
    final 已存在时抛 FileExistsError，由调用方决定如何处理。
    """
    if os.path.lexists(final):
        raise FileExistsError(str(final))
    _fsync_tree(staging)
    final.parent.mkdir(parents=True, exist_ok=True)
    _fs_retry(os.rename, staging, final)
    _fsync_dir(final.parent)


# ================================ 命令行（排查连接用） ================================
def _main(argv: list[str]) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="MinIO SDK 连接排查工具（不含任何 mc 调用；连接信息读自配置库）")
    parser.add_argument("--config-db", default=os.getenv("MINIO_DOWNLOAD_CONFIG_DB") or str(Path(__file__).resolve().parent / "minio_downloader_config.sqlite3"),
                        help="配置库 SQLite 文件（默认与脚本同目录）")
    parser.add_argument("--bucket", help="默认取配置库里的 bucket")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check", help="检查连接与 bucket 权限")
    p = sub.add_parser("ls", help="列出前缀下一层的内容"); p.add_argument("prefix", nargs="?", default="")
    p = sub.add_parser("get", help="把一个前缀下载到目录（与下载脚本使用同一套原子下载逻辑）")
    p.add_argument("prefix"); p.add_argument("dest", type=Path)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    try:
        bucket = args.bucket
        if Path(args.config_db).is_file():
            import config_db
            values = config_db.ConfigStore(args.config_db).get_settings()
            set_config(config_from_settings(values))
            apply_tuning(values)
            bucket = bucket or values.get("minio.bucket")
        if not bucket:
            raise StoreError("请用 --bucket 指定 bucket，或先在 Web 控制台里配置")
        if args.cmd == "check":
            print("OK：" + verify_connection(bucket))
        elif args.cmd == "ls":
            for o in list_objects(bucket, args.prefix):
                print(f"{'DIR ' if o.is_dir else 'FILE'} {o.size:>14}  {o.rel}")
        else:
            report = download_prefix(bucket, args.prefix, args.dest)
            print(f"完成：{report.files} 个文件，{report.bytes} 字节，{report.seconds:.1f} 秒")
    except StoreError as exc:
        print(f"失败：{exc}")
        return 1
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_main(sys.argv[1:]))
