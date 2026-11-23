#!/usr/bin/env bash
# 命令行下载（Ctrl+C = 安全暂停，再按一次立即退出）。参数原样传给脚本，例如：
#   ./run_download.sh --max-rounds 3
#   ./run_download.sh --cleanup-missing-mcap --dry-run
set -euo pipefail
cd "$(dirname "$0")"
if [ -f .env ]; then set -a; . ./.env; set +a; fi
PY="python3"; [ -x .venv/bin/python ] && PY=".venv/bin/python"
exec "$PY" minio_task_downloader_by_count.py "$@"
