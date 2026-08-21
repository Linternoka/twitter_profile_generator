@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ============================================
echo    推特用户画像生成器 - 启动
echo ============================================
echo 若检测到旧实例在运行，可加 --force 强制结束旧实例并接管：
echo   python launcher.py --force
echo --------------------------------------------
python launcher.py %*
if errorlevel 1 (
  echo.
  echo 启动失败，请确认已安装依赖：pip install -r requirements.txt
)
pause
