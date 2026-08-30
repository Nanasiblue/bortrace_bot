@echo off
cd /d "%~dp0"
set "PYTHONPATH=%~dp0src"
set "PY=C:\Users\ryuou\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
if not exist "%PY%" set "PY=python"
"%PY%" ".\scripts\predict_official_models.py" ^
  --dataset-dir ".\outputs\official_dataset_parts" ^
  --start-month 202501 ^
  --end-month 202512 ^
  --models-dir ".\outputs\official_models" ^
  --year 2025 ^
  --out ".\outputs\official_predictions_2025.csv"
pause
