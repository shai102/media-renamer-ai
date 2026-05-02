@echo off
chcp 65001 >nul
echo ============================================
echo      正在开始打包：媒体归档刮削助手
echo ============================================

:: 1. 执行打包
pyinstaller --noconfirm --onefile --windowed --name "媒体归档刮削助手" --icon "assets\app_icon.ico" --collect-all guessit --collect-all babelfish --collect-all Pillow --clean main.py

echo.
echo --------------------------------------------
echo 打包结束，正在清理临时文件...

:: 2. 直接清理 (如果不成功，CMD 也会继续走下去)
rd /s /q build
del /q "媒体归档刮削助手.spec"

echo.
echo [完成] 临时文件已清理。
echo [提示] 请在 dist 文件夹中查看生成的 EXE 文件。
echo --------------------------------------------
pause
