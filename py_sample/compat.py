# -*- coding: utf-8 -*-
"""跨平台（Linux / macOS / Windows）小工具：进程锁、PID 文件、进程存活检查、结束进程树。

下载脚本和 Web 控制台共用，避免在业务代码里到处写 ``if os.name == "nt"``。
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

IS_WINDOWS = os.name == "nt"

try:                                    # psutil 让“进程是否还活着 / 结束整个进程树”在各平台一致
    import psutil
except ImportError:                     # pragma: no cover - requirements.txt 里已声明
    psutil = None


# ================================ 进程锁 ================================
class FileLock:
    """独占、非阻塞的文件锁。进程退出（包括被 kill / 崩溃）时由操作系统自动释放，不会留下“死锁文件”。

    POSIX 使用 fcntl.flock，Windows 使用 msvcrt.locking（锁定第 0 个字节）。
    锁文件本身不存放内容，PID 另存在 PID 文件里，所以 Windows 上其它进程读取不会被锁挡住。
    """

    def __init__(self, path: str | os.PathLike):
        self.path = Path(path)
        self._fd: int | None = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            if IS_WINDOWS:
                import msvcrt
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:                 # POSIX: BlockingIOError；Windows: PermissionError
            os.close(fd)
            return False
        self._fd = fd
        return True

    def release(self) -> None:
        if self._fd is None:
            return
        try:
            if IS_WINDOWS:
                import msvcrt
                os.lseek(self._fd, 0, os.SEEK_SET)
                msvcrt.locking(self._fd, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._fd, fcntl.LOCK_UN)
        except OSError:
            pass
        finally:
            os.close(self._fd)
            self._fd = None

    def __enter__(self) -> "FileLock":
        if not self.acquire():
            raise BlockingIOError(f"锁已被其他进程持有：{self.path}")
        return self

    def __exit__(self, *exc: Any) -> None:
        self.release()


# ================================ PID 文件 ================================
def _create_time(pid: int) -> float | None:
    if psutil is None:
        return None
    try:
        return psutil.Process(pid).create_time()
    except (psutil.Error, OSError):
        return None


def write_pidfile(path: str | os.PathLike, pid: int | None = None) -> None:
    pid = pid or os.getpid()
    Path(path).write_text(json.dumps({"pid": pid, "created": _create_time(pid), "written": time.time()}),
                          encoding="utf-8")


def remove_pidfile(path: str | os.PathLike) -> None:
    try:
        Path(path).unlink()
    except OSError:
        pass


def read_pidfile(path: str | os.PathLike) -> dict[str, Any] | None:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) and isinstance(data.get("pid"), int) else None
    except (OSError, ValueError):
        return None


# ================================ 进程存活 / 结束 ================================
def _windows_pid_alive(pid: int) -> bool:          # 没有 psutil 时的兜底
    import ctypes
    kernel32 = ctypes.windll.kernel32               # type: ignore[attr-defined]
    handle = kernel32.OpenProcess(0x1000, False, pid)      # PROCESS_QUERY_LIMITED_INFORMATION
    if not handle:
        return False
    try:
        code = ctypes.c_ulong()
        return bool(kernel32.GetExitCodeProcess(handle, ctypes.byref(code))) and code.value == 259   # STILL_ACTIVE
    finally:
        kernel32.CloseHandle(handle)


def process_alive(pid: int, created: float | None = None) -> bool:
    """进程是否仍在运行。给出 created（进程创建时间）时可识别“PID 被别的进程复用”。"""
    if pid <= 0:
        return False
    if psutil is not None:
        try:
            proc = psutil.Process(pid)
            if proc.status() == psutil.STATUS_ZOMBIE:
                return False
            if created is not None and abs(proc.create_time() - created) > 2.0:
                return False
            return True
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            return False
        except psutil.Error:
            return True
    if IS_WINDOWS:
        return _windows_pid_alive(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def kill_tree(pid: int, force: bool = False, timeout: float = 5.0) -> None:
    """结束进程及其所有子进程。force=False 先礼貌结束，超时再强杀。"""
    if psutil is not None:
        try:
            parent = psutil.Process(pid)
        except psutil.NoSuchProcess:
            return
        procs = parent.children(recursive=True) + [parent]
        for p in procs:
            try:
                p.kill() if force else p.terminate()
            except psutil.NoSuchProcess:
                pass
        if not force:
            _, alive = psutil.wait_procs(procs, timeout=timeout)
            for p in alive:
                try:
                    p.kill()
                except psutil.NoSuchProcess:
                    pass
        return
    if IS_WINDOWS:
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, check=False)
        return
    try:
        os.killpg(os.getpgid(pid), signal.SIGKILL if force else signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        try:
            os.kill(pid, signal.SIGKILL if force else signal.SIGTERM)
        except ProcessLookupError:
            pass


def detached_popen_kwargs() -> dict[str, Any]:
    """让子进程脱离父进程的控制台 / 进程组：关闭 Web 页面或终端不会连带结束下载。"""
    if IS_WINDOWS:
        flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(subprocess, "CREATE_NO_WINDOW", 0)
        return {"creationflags": flags}
    return {"start_new_session": True}
