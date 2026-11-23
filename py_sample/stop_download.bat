@echo off
rem Ask a running downloader to pause safely (it saves its progress; next start resumes from there).
setlocal
cd /d "%~dp0"
chcp 65001 >nul
set PYTHONUTF8=1
if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] Virtual environment not found. Run install.bat first.
  pause & exit /b 1
)
".venv\Scripts\python.exe" minio_task_downloader_by_count.py --stop
pause
