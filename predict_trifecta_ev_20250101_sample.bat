@echo off
cd /d "%~dp0"
set "PYTHONPATH=%~dp0src"
set "PY=C:\Users\ryuou\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
if not exist "%PY%" set "PY=python"
"%PY%" ".\scripts\predict_trifecta_ev.py" ^
  --dataset-dir ".\outputs\official_dataset_parts" ^
  --start-month 202501 ^
  --end-month 202501 ^
  --models-dir ".\outputs\official_models" ^
  --date 20250101 ^
  --top-n 5 ^
  --min-ev 0 ^
  --out ".\outputs\trifecta_ev_candidates_20250101.csv"
pause
