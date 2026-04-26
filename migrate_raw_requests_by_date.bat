@echo off
setlocal

rem Run from the project root so config, database, and logs resolve correctly.
set "APP_ROOT=%~dp0"
cd /d "%APP_ROOT%"

rem Always use the config.json in the current project directory.
set "HKPARKING_CONFIG=%APP_ROOT%config.json"

rem Execute the one-shot raw_requests migration tool in console mode.
"%APP_ROOT%.venv\Scripts\python.exe" -m bdzc_parking.maintenance migrate-raw-requests
exit /b %ERRORLEVEL%
