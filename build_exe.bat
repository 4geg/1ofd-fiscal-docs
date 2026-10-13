@echo off
setlocal
cd /d "%~dp0"

echo ==========================================
echo  Сборка 1OFD Fiscal Docs v1.7.8M
 echo ==========================================

if not exist ".venv\Scripts\python.exe" (
  echo [1/5] Создаю виртуальное окружение...
  py -3 -m venv .venv
) else (
  echo [1/5] Виртуальное окружение уже есть.
)

call .venv\Scripts\activate.bat
set "APP_VERSION="
for /f %%V in ('python -c "from version import VERSION; print(VERSION)"') do set "APP_VERSION=%%V"
if not defined APP_VERSION goto :error
set "EXE_BASENAME=1OFD_FiscalDocs_%APP_VERSION%"

echo [2/5] Обновляю pip...
python -m pip install --upgrade pip
if errorlevel 1 goto :error

echo [3/5] Устанавливаю зависимости сборки...
python -m pip install -r requirements-build.txt
if errorlevel 1 goto :error

echo [4/5] Проверяю иконку...
if exist "app.ico" (
  copy /y "app.ico" "assets\app.ico" >nul
  if not exist "assets\static\icons" mkdir "assets\static\icons"
  copy /y "app.ico" "assets\static\icons\favicon.ico" >nul
)
if not exist "assets\app.ico" (
  python tools\make_icon.py
  if errorlevel 1 goto :error
) else (
  echo Использую существующую assets\app.ico
)

echo [5/5] Собираю EXE...
python tools\pack_resources.py
if errorlevel 1 goto :error
pyinstaller --noconfirm --clean --onefile --windowed --noupx ^
  --name "%EXE_BASENAME%" ^
  --icon "assets\app.ico" ^
  --version-file "assets\version_info.txt" ^
  --manifest "assets\app.manifest" ^
  --add-data "assets\app.ico;assets" ^
  --collect-all uvicorn ^
  --collect-all fastapi ^
  --collect-all pydantic ^
  --collect-all httpx ^
  --collect-data certifi ^
  --collect-all pystray ^
  --collect-all PIL ^
  main.py
if errorlevel 1 goto :error

for /f %%H in ('powershell -NoProfile -Command "(Get-FileHash 'dist\%EXE_BASENAME%.exe' -Algorithm SHA256).Hash.ToLower()"') do echo %%H  %EXE_BASENAME%.exe>"dist\SHA256SUMS.txt"

echo.
echo ГОТОВО.
echo EXE: %CD%\dist\%EXE_BASENAME%.exe
echo SHA256: %CD%\dist\SHA256SUMS.txt
explorer "%CD%\dist"
pause
exit /b 0

:error
echo.
echo ОШИБКА СБОРКИ. Скопируй текст ошибки из этого окна.
pause
exit /b 1
