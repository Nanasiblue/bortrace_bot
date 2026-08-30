@echo off
cd /d "%~dp0"
powershell -ExecutionPolicy Bypass -File ".\scripts\run_official_worker.ps1" -StartDate "2022-01-01" -EndDate "2022-06-30" -Pages racelist,beforeinfo,odds3t,raceresult -SleepMs 1000
pause
