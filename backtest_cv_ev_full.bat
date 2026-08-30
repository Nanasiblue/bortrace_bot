@echo off
cd /d "%~dp0"
set "PYTHONPATH=%~dp0src;%~dp0scripts"
set "PY=C:\Users\ryuou\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
if not exist "%PY%" set "PY=python"
"%PY%" ".\scripts\backtest_cv_ev.py" ^
  --dataset-dir ".\outputs\official_dataset_parts" ^
  --start-month 202301 ^
  --end-month 202512 ^
  --cv-dir ".\outputs\official_cv" ^
  --valid-years 2023 2024 2025 ^
  --prob-scales 0.35 0.5 0.65 ^
  --min-evs 0.15 0.3 0.5 ^
  --top-ns 1 3 ^
  --kelly-scales 0.25 ^
  --unit-stake 100 ^
  --bankroll 100000 ^
  --max-kelly-fraction 0.003 ^
  --out-summary ".\outputs\cv_ev_backtest_summary.csv" ^
  --out-sample-bets ".\outputs\cv_ev_backtest_sample_bets.csv" ^
  --progress-every 5000
pause
