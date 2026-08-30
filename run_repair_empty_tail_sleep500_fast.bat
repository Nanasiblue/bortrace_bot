@echo off
cd /d "%~dp0"
powershell -ExecutionPolicy Bypass -File ".\scripts\collect_official_pages.ps1" -StartDate "2021-12-31" -EndDate "2021-12-31" -Pages racelist,beforeinfo,odds3t,raceresult -SleepMs 500 -FastExistingCheck
powershell -ExecutionPolicy Bypass -File ".\scripts\collect_official_pages.ps1" -StartDate "2023-12-28" -EndDate "2023-12-31" -Pages racelist,beforeinfo,odds3t,raceresult -SleepMs 500 -FastExistingCheck
powershell -ExecutionPolicy Bypass -File ".\scripts\collect_official_pages.ps1" -StartDate "2025-12-28" -EndDate "2025-12-31" -Pages racelist,beforeinfo,odds3t,raceresult -SleepMs 500 -FastExistingCheck
pause
