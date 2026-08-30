@echo off
cd /d "%~dp0"
set "PYTHONPATH=%~dp0src"
set "PY=C:\Users\ryuou\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
if not exist "%PY%" set "PY=python"
"%PY%" ".\scripts\build_official_dataset.py" --start-date 20210101 --end-date 20260731 --out ".\outputs\official_race_dataset.csv" --progress-every 5000
pause
