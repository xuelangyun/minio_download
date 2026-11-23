#!/usr/bin/env bash
# 创建虚拟环境并安装依赖（只需运行一次）。
set -euo pipefail
cd "$(dirname "$0")"
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt
echo "完成。下一步：./run_web.sh"
