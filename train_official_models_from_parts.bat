@echo off
cd /d "%~dp0"
set "PYTHONPATH=%~dp0src"
set "PY=C:\Users\ryuou\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
if not exist "%PY%" set "PY=python"
"%PY%" ".\scripts\train_official_models.py" --dataset-dir ".\outputs\official_dataset_parts" --start-month 202101 --end-month 202512 --out-dir ".\outputs\official_models" --train-through 2024 --valid-year 2025 --test-year 2026 --epochs 700
pause
