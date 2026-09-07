@echo off
setlocal
cd /d "%~dp0"

echo ==========================================
echo  Сборка 1OFD Fiscal Docs v1.7.3
 echo ==========================================

if not exist ".venv\Scripts\python.exe" (
  echo [1/5] Создаю виртуальное окружение...
  py -3 -m venv .venv
) else (
  echo [1/5] Виртуальное окружение уже есть.
)

call .venv\Scripts\activate.bat

echo [2/5] Обновляю pip...
python -m pip install --upgrade pip
if errorlevel 1 goto :error

echo [3/5] Устанавливаю зависимости сборки...
python -m pip install -r requirements-build.txt
if errorlevel 1 goto :error

echo [4/5] Проверяю иконку...
if not exist "assets\app.ico" (
  python tools\make_icon.py
  if errorlevel 1 goto :error
) else (
  echo Использую существующую assets\app.ico
)

echo [5/5] Собираю EXE...
pyinstaller --noconfirm --clean --onefile --windowed ^
  --name "1OFD_FiscalDocs" ^
  --icon "assets\app.ico" ^
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

echo.
echo ГОТОВО.
echo EXE: %CD%\dist\1OFD_FiscalDocs.exe
explorer "%CD%\dist"
pause
exit /b 0

:error
echo.
echo ОШИБКА СБОРКИ. Скопируй текст ошибки из этого окна.
pause
exit /b 1
