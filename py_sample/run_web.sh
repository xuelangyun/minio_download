#!/usr/bin/env bash
# 启动 Web 控制台（默认 127.0.0.1:8765）。参数原样传给 web_app.py，例如：./run_web.sh --port 9000
# 若存在 .env，会先加载——它只在“配置库第一次创建”时被导入（MINIO_* / MINIO_PG_* / MINIO_WEB_TOKEN），之后以配置库为准。
set -euo pipefail
cd "$(dirname "$0")"
if [ -f .env ]; then set -a; . ./.env; set +a; fi
PY="python3"; [ -x .venv/bin/python ] && PY=".venv/bin/python"
exec "$PY" web_app.py "$@"
