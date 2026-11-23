@echo off
rem Start the web console and open it in the default browser.
rem Extra arguments are passed through, e.g.  run_web.bat --port 9000   or   run_web.bat --set-token mypassword
setlocal
cd /d "%~dp0"
chcp 65001 >nul
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] Virtual environment not found. Run install.bat first.
  pause & exit /b 1
)
".venv\Scripts\python.exe" web_app.py --open %*
if errorlevel 1 pause
