@echo off
cd /d "%~dp0"
set "PYTHONPATH=%~dp0src;%~dp0scripts"
set "PY=C:\Users\ryuou\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
if not exist "%PY%" set "PY=python"
"%PY%" ".\scripts\live_thread_predict.py" --list-schedule %*
pause
