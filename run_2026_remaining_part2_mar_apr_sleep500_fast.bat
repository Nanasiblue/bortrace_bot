@echo off
cd /d "%~dp0"
powershell -ExecutionPolicy Bypass -File ".\scripts\collect_official_pages.ps1" -StartDate "2026-03-01" -EndDate "2026-04-30" -Pages racelist,beforeinfo,odds3t,raceresult -SleepMs 500 -FastExistingCheck
pause
