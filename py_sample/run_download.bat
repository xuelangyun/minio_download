@echo off
rem Run the downloader in this console (Ctrl+C = pause safely; press it again to exit at once).
rem Arguments are passed through, e.g.  run_download.bat --max-rounds 3
rem                                     run_download.bat --cleanup-missing-mcap --dry-run
setlocal
cd /d "%~dp0"
chcp 65001 >nul
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] Virtual environment not found. Run install.bat first.
  pause & exit /b 1
)
".venv\Scripts\python.exe" minio_task_downloader_by_count.py %*
echo.
echo Exit code: %errorlevel%
pause
