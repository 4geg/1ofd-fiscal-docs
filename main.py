from __future__ import annotations

import ctypes
import logging
import os
import socket
import subprocess
import sys
import threading
import time
import webbrowser
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pystray
import uvicorn
from PIL import Image, ImageDraw, ImageFont

from config_store import LOG_DIR
from ofd_app import app
from version import APP_DISPLAY_NAME, VERSION, WEB_URL, HOST, PORT

LOGGER = logging.getLogger("ofd_app")
SERVER: uvicorn.Server | None = None
TRAY: pystray.Icon | None = None
_MUTEX_HANDLE = None


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "app.log"
    handler = RotatingFileHandler(log_file, maxBytes=1_500_000, backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)


def is_server_ready() -> bool:
    try:
        with socket.create_connection((HOST, PORT), timeout=0.4):
            return True
    except OSError:
        return False


def wait_for_server(timeout: float = 12.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if is_server_ready():
            return True
        time.sleep(0.15)
    return False


def open_web(*_args) -> None:
    if wait_for_server(2.0):
        webbrowser.open(WEB_URL)


def restart_app(icon=None, item=None) -> None:
    LOGGER.info("Перезапуск приложения")
    if icon:
        icon.stop()
    if SERVER:
        SERVER.should_exit = True
    time.sleep(0.25)

    if getattr(sys, "frozen", False):
        cmd = [sys.executable]
        cwd = str(Path(sys.executable).resolve().parent)
    else:
        cmd = [sys.executable, str(Path(__file__).resolve())]
        cwd = str(Path(__file__).resolve().parent)

    subprocess.Popen(cmd, cwd=cwd, close_fds=True)
    os._exit(0)


def exit_app(icon=None, item=None) -> None:
    LOGGER.info("Завершение приложения")
    if SERVER:
        SERVER.should_exit = True
    if icon:
        icon.stop()


def create_tray_image() -> Image.Image:
    size = 64
    image = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)
    draw.ellipse((4, 4, 60, 60), fill=(0, 119, 182, 255))
    draw.ellipse((13, 13, 51, 51), fill=(202, 240, 248, 255))
    try:
        font = ImageFont.truetype("arialbd.ttf", 26)
    except Exception:
        font = ImageFont.load_default()
    text = "1"
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    draw.text(((size - tw) / 2, (size - th) / 2 - 2), text, font=font, fill=(3, 4, 94, 255))
    return image


def start_server() -> None:
    global SERVER
    config = uvicorn.Config(app, host=HOST, port=PORT, log_level="warning", access_log=False)
    SERVER = uvicorn.Server(config)
    LOGGER.info("Запуск веб-сервера %s", WEB_URL)
    SERVER.run()


def acquire_single_instance() -> bool:
    global _MUTEX_HANDLE
    if os.name != "nt":
        return True
    kernel32 = ctypes.windll.kernel32
    _MUTEX_HANDLE = kernel32.CreateMutexW(None, False, "Local\\4geg_1OFD_FiscalDocs")
    ERROR_ALREADY_EXISTS = 183
    return kernel32.GetLastError() != ERROR_ALREADY_EXISTS


def main() -> None:
    global TRAY
    setup_logging()

    if not acquire_single_instance():
        LOGGER.info("Приложение уже запущено — открываю существующий интерфейс")
        open_web()
        return

    thread = threading.Thread(target=start_server, name="uvicorn", daemon=True)
    thread.start()

    if wait_for_server():
        open_web()
    else:
        LOGGER.error("Веб-сервер не запустился за отведённое время")

    menu = pystray.Menu(
        pystray.MenuItem("Открыть", open_web, default=True),
        pystray.Menu.SEPARATOR,
        pystray.MenuItem("Перезапустить", restart_app),
        pystray.MenuItem("Завершить", exit_app),
    )
    TRAY = pystray.Icon(
        "1ofd_fiscal_docs",
        create_tray_image(),
        f"{APP_DISPLAY_NAME} v{VERSION}",
        menu,
    )
    TRAY.run()


if __name__ == "__main__":
    main()
