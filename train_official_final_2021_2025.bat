@echo off
cd /d "%~dp0"
set "PYTHONPATH=%~dp0src"
set "PY=C:\Users\ryuou\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
if not exist "%PY%" set "PY=python"
"%PY%" ".\scripts\train_official_final.py" ^
  --dataset-dir ".\outputs\official_dataset_parts" ^
  --start-month 202101 ^
  --end-month 202512 ^
  --out-dir ".\outputs\official_models_final_2021_2025" ^
  --epochs 700
pause
