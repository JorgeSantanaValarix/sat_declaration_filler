@echo off
setlocal
cd /d "%~dp0"

echo SAT Declaration Filler - Setup
echo.

REM Check for Python (try py launcher first, then python)
set PYTHON=
where py >nul 2>&1 && set PYTHON=py -3
if not defined PYTHON where python >nul 2>&1 && set PYTHON=python
if not defined PYTHON (
    echo [ERROR] Python not found. Please install Python 3.10+ from https://www.python.org/downloads/
    echo Make sure to check "Add Python to PATH" during installation.
    pause
    exit /b 1
)

echo [OK] Python found:
%PYTHON% --version
echo.

echo Installing dependencies from requirements.txt...
%PYTHON% -m pip install -r requirements.txt
if errorlevel 1 (
    echo [ERROR] pip install failed.
    pause
    exit /b 1
)
echo.

echo Installing Playwright Chromium browser...
%PYTHON% -m playwright install chromium
if errorlevel 1 (
    echo [ERROR] playwright install failed.
    pause
    exit /b 1
)
echo.

echo [OK] Setup complete.
echo.
echo Next steps:
echo   1. Copy config.example.json to config.json and edit (DB + FIEL path).
echo   2. Run: %PYTHON% sat_declaration_filler.py -w "path\to\workpaper.xlsx" -c 1 -b 2
echo.
pause
