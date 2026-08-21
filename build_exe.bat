@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ============================================
echo   打包「推特用户画像生成器」为独立 exe
echo ============================================
echo [1/3] 安装打包工具 PyInstaller ...
pip install pyinstaller
if errorlevel 1 goto :err
echo [2/3] 开始打包（约 1-3 分钟）...
pyinstaller --noconfirm --clean ^
  --onefile --windowed --name "推特用户画像生成器" ^
  --add-data "static;static" ^
  --collect-all twscrape ^
  --collect-all fake_useragent ^
  --hidden-import bs4 ^
  launcher.py
if errorlevel 1 goto :err
echo [3/3] 完成！
echo.
echo   独立软件已生成：dist\推特用户画像生成器.exe
echo   双击即可运行（自动打开浏览器，界面为本地网页）。
echo   隐私数据与抓取数据统一保存在用户数据目录：
echo     %%USERPROFILE%%\TwitterProfileGenerator\
echo   （可用环境变量 TPG_DATA_DIR 覆盖；界面可自定义输出目录）
pause
exit /b 0
:err
echo.
echo 打包失败，请查看上方错误信息。
pause
exit /b 1
