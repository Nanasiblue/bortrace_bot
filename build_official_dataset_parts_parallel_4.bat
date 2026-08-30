@echo off
cd /d "%~dp0"
powershell -ExecutionPolicy Bypass -File ".\scripts\build_official_dataset_parts_parallel.ps1" -StartMonth 202101 -EndMonth 202607 -MaxParallel 4
pause
