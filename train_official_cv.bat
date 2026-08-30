@echo off
cd /d "%~dp0"
set "PYTHONPATH=%~dp0src"
set "PY=C:\Users\ryuou\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
if not exist "%PY%" set "PY=python"
"%PY%" ".\scripts\train_official_cv.py" ^
  --dataset-dir ".\outputs\official_dataset_parts" ^
  --start-month 202101 ^
  --end-month 202512 ^
  --out-dir ".\outputs\official_cv" ^
  --valid-years 2023 2024 2025 ^
  --epochs 700
pause
