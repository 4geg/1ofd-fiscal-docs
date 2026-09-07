@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [1/3] Создаю виртуальное окружение...
  py -3 -m venv .venv
)

echo [2/3] Устанавливаю зависимости...
call .venv\Scripts\activate.bat
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo [3/3] Запускаю приложение...
python main.py
