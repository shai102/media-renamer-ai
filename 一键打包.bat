@echo off
setlocal EnableExtensions
cd /d "%~dp0"
chcp 65001 >nul 2>&1

echo ============================================
echo      Packaging Media Renamer
echo ============================================

where pyinstaller >nul 2>&1
if errorlevel 1 (
    echo [ERROR] pyinstaller not found. Install: pip install pyinstaller
    pause
    exit /b 1
)

pyinstaller -y --onefile --windowed ^
  --name "媒体归档刮削助手" ^
  --icon "assets\app_icon.ico" ^
  --collect-all PySide6 ^
  --collect-all guessit ^
  --collect-all babelfish ^
  --collect-all Pillow ^
  --clean ^
  main.py

if errorlevel 1 (
    echo.
    echo [ERROR] PyInstaller failed. See output above.
    pause
    exit /b 1
)

echo.
echo --------------------------------------------
echo Cleaning temp files...
if exist build rd /s /q build
if exist "媒体归档刮削助手.spec" del /q "媒体归档刮削助手.spec"

echo.
echo [OK] EXE: dist\媒体归档刮削助手.exe
echo --------------------------------------------
pause
