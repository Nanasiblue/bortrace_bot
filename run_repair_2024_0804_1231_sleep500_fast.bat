@echo off
cd /d "%~dp0"
powershell -ExecutionPolicy Bypass -File ".\scripts\collect_official_pages.ps1" -StartDate "2024-08-04" -EndDate "2024-12-31" -Pages racelist,beforeinfo,odds3t,raceresult -SleepMs 500 -FastExistingCheck
pause
