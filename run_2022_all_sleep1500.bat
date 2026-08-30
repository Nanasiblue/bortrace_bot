@echo off
cd /d "%~dp0"
powershell -ExecutionPolicy Bypass -File ".\scripts\run_official_worker.ps1" -StartDate "2022-01-01" -EndDate "2022-12-31" -Pages racelist,beforeinfo,odds3t,raceresult -SleepMs 1500
pause
