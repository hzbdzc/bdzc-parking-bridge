@echo off
setlocal

rem Run from the project root so config, database, and logs resolve correctly.
set "APP_ROOT=%~dp0"
cd /d "%APP_ROOT%"

rem Always use the config.json in the current project directory.
set "HKPARKING_CONFIG=%APP_ROOT%config.json"

rem Stop any existing bdzc_parking Python process before starting a new one.
powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-CimInstance Win32_Process | Where-Object { ($_.Name -eq 'pythonw.exe' -or $_.Name -eq 'python.exe') -and $_.CommandLine -match '(^| )-m bdzc_parking($| )' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"

rem Launch the GUI app without opening a console window.
start "" "%APP_ROOT%.venv\Scripts\pythonw.exe" -m bdzc_parking
exit /b 0
