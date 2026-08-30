@echo off
cd /d "%~dp0"
powershell -ExecutionPolicy Bypass -File ".\scripts\package_worker_results.ps1"
pause
