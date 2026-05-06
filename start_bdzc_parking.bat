@echo off
setlocal

rem Run from the project root so config, database, and logs resolve correctly.
set "APP_ROOT=%~dp0"
cd /d "%APP_ROOT%"

rem Always use the config.json in the current project directory.
set "HKPARKING_CONFIG=%APP_ROOT%config.json"

rem Make sure locked runtime dependencies are available without pruning local dev tools.
where uv.exe >nul 2>nul
if errorlevel 1 (
    echo [bdzc_parking] uv.exe was not found in PATH.
    echo [bdzc_parking] Install uv or run this script from a terminal where uv is available.
    pause
    exit /b 1
)

uv sync --frozen --no-dev --inexact
if errorlevel 1 (
    echo [bdzc_parking] Failed to sync runtime dependencies from uv.lock.
    pause
    exit /b 1
)

if not exist "%APP_ROOT%.venv\Scripts\pythonw.exe" (
    echo [bdzc_parking] Missing .venv\Scripts\pythonw.exe after uv sync.
    pause
    exit /b 1
)

rem Stop any existing bdzc_parking GUI process and orphaned HTTP child process before starting.
powershell -NoProfile -ExecutionPolicy Bypass -Command "& { $ErrorActionPreference='SilentlyContinue'; $appRoot=(Resolve-Path -LiteralPath $env:APP_ROOT).ProviderPath.TrimEnd('\'); $venvScripts=(Join-Path $appRoot '.venv\Scripts'); $all=@(Get-CimInstance Win32_Process); $main=@($all | Where-Object { ($_.Name -eq 'pythonw.exe' -or $_.Name -eq 'python.exe') -and ([string]$_.CommandLine).Contains($venvScripts) -and $_.CommandLine -match '(^| )-m bdzc_parking($| )' }); $children=@(); foreach ($mainProcess in $main) { $children += @($all | Where-Object { ($_.Name -eq 'pythonw.exe' -or $_.Name -eq 'python.exe') -and $_.CommandLine -match 'multiprocessing\.spawn' -and $_.CommandLine -match '--multiprocessing-fork' -and $_.CommandLine -match ('parent_pid=' + [regex]::Escape([string]$mainProcess.ProcessId)) }) }; $listenPort=1888; $configPath=Join-Path $appRoot 'config.json'; if (Test-Path -LiteralPath $configPath) { $config=Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json; if ($config.listen_port) { $listenPort=[int]$config.listen_port } }; $portPids=@(); if (Get-Command Get-NetTCPConnection -ErrorAction SilentlyContinue) { $portPids=@(Get-NetTCPConnection -State Listen -LocalPort $listenPort | Select-Object -ExpandProperty OwningProcess -Unique) }; $portChildren=@($all | Where-Object { $portPids -contains $_.ProcessId -and ($_.Name -eq 'pythonw.exe' -or $_.Name -eq 'python.exe') -and $_.CommandLine -match 'multiprocessing\.spawn' -and $_.CommandLine -match '--multiprocessing-fork' }); @($children + $portChildren + $main) | Sort-Object ProcessId -Unique | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue } }"

rem Launch the GUI app without opening a console window.
start "" "%APP_ROOT%.venv\Scripts\pythonw.exe" -m bdzc_parking
exit /b 0
