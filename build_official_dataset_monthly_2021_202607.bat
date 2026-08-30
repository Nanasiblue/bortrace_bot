@echo off
cd /d "%~dp0"
set "PYTHONPATH=%~dp0src"
set "PY=C:\Users\ryuou\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
if not exist "%PY%" set "PY=python"
"%PY%" ".\scripts\build_official_dataset_monthly.py" --start-month 202101 --end-month 202607 --out-dir ".\outputs\official_dataset_parts" --progress-every 1000
pause
