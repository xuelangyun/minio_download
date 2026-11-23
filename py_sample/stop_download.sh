#!/usr/bin/env bash
# 请求正在运行的下载器安全暂停（保存断点，下次启动从断点继续）。
set -euo pipefail
cd "$(dirname "$0")"
PY="python3"; [ -x .venv/bin/python ] && PY=".venv/bin/python"
exec "$PY" minio_task_downloader_by_count.py --stop
