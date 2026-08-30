@echo off
cd /d "%~dp0"
set "PYTHONPATH=%~dp0src;%~dp0scripts"
set "PY=C:\Users\ryuou\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
if not exist "%PY%" set "PY=python"
"%PY%" ".\scripts\build_cv_ev_candidates.py" ^
  --dataset-dir ".\outputs\official_dataset_parts" ^
  --start-month 202401 ^
  --end-month 202412 ^
  --cv-dir ".\outputs\official_cv" ^
  --valid-years 2024 ^
  --top-candidates 10 ^
  --out ".\outputs\cv_ev_candidates_2024_top10_all.csv" ^
  --progress-every 5000 ^
  --write-every 2000
pause
