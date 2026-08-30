@echo off
cd /d "%~dp0"
set "PY=C:\Users\ryuou\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
if not exist "%PY%" set "PY=python"
"%PY%" ".\scripts\live_discord_predict.py" --from-min 5 --to-min 35
pause
