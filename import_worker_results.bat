@echo off
cd /d "%~dp0"
set "ZIP=%~1"
if "%ZIP%"=="" (
  set /p ZIP=Worker data zip path: 
)
powershell -ExecutionPolicy Bypass -File ".\scripts\import_worker_results.ps1" -ZipPath "%ZIP%"
pause
