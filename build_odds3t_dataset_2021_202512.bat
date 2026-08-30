@echo off
cd /d "%~dp0"
set "PYTHONPATH=%~dp0src"
set "PY=C:\Users\ryuou\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
if not exist "%PY%" set "PY=python"
"%PY%" ".\scripts\build_odds3t_dataset.py" --start-date 20210101 --end-date 20251231 --out ".\outputs\odds3t_dataset_2021_202512.csv" --progress-every 5000
pause
