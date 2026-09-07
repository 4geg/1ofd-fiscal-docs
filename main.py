from __future__ import annotations

import ctypes
import json
import logging
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import pystray
import uvicorn
from PIL import Image, ImageDraw, ImageFont

from config_store import APP_DATA_DIR, LOG_DIR
from ofd_app import app
from version import APP_DISPLAY_NAME, VERSION, HOST, PORT

LOGGER = logging.getLogger("ofd_app")
SERVER: uvicorn.Server | None = None
TRAY: pystray.Icon | None = None
_MUTEX_HANDLE = None
CURRENT_PORT = PORT
CURRENT_WEB_URL = f"http://{HOST}:{PORT}"
RUNTIME_FILE = APP_DATA_DIR / "runtime.json"
SERVER_FAILED = threading.Event()
SERVER_STOPPED = threading.Event()


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "app.log"
    handler = RotatingFileHandler(log_file, maxBytes=1_500_000, backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(threadName)s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # В onefile/перезапусках не плодим одинаковые handlers.
    if not any(isinstance(h, RotatingFileHandler) for h in root.handlers):
        root.addHandler(handler)


def _health_url(base_url: str) -> str:
    return base_url.rstrip("/") + "/api/health"


def is_server_ready(base_url: str | None = None, timeout: float = 0.6) -> bool:
    url = _health_url(base_url or CURRENT_WEB_URL)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status == 200
    except Exception:
        return False


def wait_for_server(base_url: str | None = None, timeout: float = 20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if SERVER_FAILED.is_set():
            return False
        if is_server_ready(base_url):
            return True
        time.sleep(0.18)
    return False


def _open_url_windows(url: str) -> None:
    """Open URL without blocking the tray callback thread."""
    try:
        if os.name == "nt":
            os.startfile(url)  # type: ignore[attr-defined]
            return
    except Exception:
        LOGGER.exception("os.startfile не смог открыть браузер")

    # Fallback для Windows/разработки. start "" URL корректно обрабатывает URL с & и т.п.
    try:
        if os.name == "nt":
            subprocess.Popen(
                ["cmd", "/c", "start", "", url],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        else:
            import webbrowser

            webbrowser.open(url, new=2)
    except Exception:
        LOGGER.exception("Не удалось открыть веб-интерфейс")


def _notify(title: str, message: str) -> None:
    try:
        if TRAY is not None:
            TRAY.notify(message, title)
    except Exception:
        LOGGER.exception("Не удалось показать уведомление в трее")


def _open_web_worker(url: str | None = None, startup: bool = False) -> None:
    target = url or CURRENT_WEB_URL
    timeout = 25.0 if startup else 4.0
    if wait_for_server(target, timeout=timeout):
        LOGGER.info("Открываю веб-интерфейс: %s", target)
        _open_url_windows(target)
        return

    LOGGER.error("Веб-интерфейс недоступен: %s", target)
    if startup or SERVER_FAILED.is_set():
        _notify(
            "1OFD Fiscal Docs",
            "Веб-сервер не запустился. Откройте журнал через меню значка в трее.",
        )


def open_web(*_args: Any) -> None:
    # ВАЖНО: callbacks pystray не должны ждать сеть/сервер, иначе меню трея «подвисает».
    threading.Thread(target=_open_web_worker, name="open-web", daemon=True).start()


def open_log(*_args: Any) -> None:
    log_path = LOG_DIR / "app.log"
    try:
        log_path.touch(exist_ok=True)
        if os.name == "nt":
            os.startfile(str(log_path))  # type: ignore[attr-defined]
        else:
            _open_url_windows(log_path.as_uri())
    except Exception:
        LOGGER.exception("Не удалось открыть журнал")


def open_data_folder(*_args: Any) -> None:
    try:
        APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
        if os.name == "nt":
            os.startfile(str(APP_DATA_DIR))  # type: ignore[attr-defined]
        else:
            _open_url_windows(APP_DATA_DIR.as_uri())
    except Exception:
        LOGGER.exception("Не удалось открыть папку данных")


def _wait_for_pid_to_exit(pid: int, timeout: float = 12.0) -> None:
    if os.name != "nt" or pid <= 0:
        return
    kernel32 = ctypes.windll.kernel32
    SYNCHRONIZE = 0x00100000
    handle = kernel32.OpenProcess(SYNCHRONIZE, False, pid)
    if not handle:
        return
    try:
        kernel32.WaitForSingleObject(handle, int(timeout * 1000))
    finally:
        kernel32.CloseHandle(handle)


def _release_mutex() -> None:
    global _MUTEX_HANDLE
    if os.name == "nt" and _MUTEX_HANDLE:
        try:
            ctypes.windll.kernel32.ReleaseMutex(_MUTEX_HANDLE)
        except Exception:
            pass
        try:
            ctypes.windll.kernel32.CloseHandle(_MUTEX_HANDLE)
        except Exception:
            pass
        _MUTEX_HANDLE = None


def _restart_worker() -> None:
    LOGGER.info("Перезапуск приложения")
    old_pid = os.getpid()
    if SERVER:
        SERVER.should_exit = True

    # Останавливаем UI трея раньше, а новый процесс ждёт завершения текущего PID.
    try:
        if TRAY:
            TRAY.stop()
    except Exception:
        LOGGER.exception("Ошибка остановки трея при перезапуске")

    if getattr(sys, "frozen", False):
        cmd = [sys.executable, "--wait-for-pid", str(old_pid)]
        cwd = str(Path(sys.executable).resolve().parent)
    else:
        cmd = [sys.executable, str(Path(__file__).resolve()), "--wait-for-pid", str(old_pid)]
        cwd = str(Path(__file__).resolve().parent)

    try:
        subprocess.Popen(
            cmd,
            cwd=cwd,
            close_fds=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0,
        )
    except Exception:
        LOGGER.exception("Не удалось запустить новый процесс")
        _notify("1OFD Fiscal Docs", "Не удалось перезапустить приложение. Смотрите журнал.")
        return

    _remove_runtime_file()
    _release_mutex()
    # Даём subprocess стартовать и сразу выходим. Новый процесс сам дождётся PID.
    time.sleep(0.1)
    os._exit(0)


def restart_app(*_args: Any) -> None:
    threading.Thread(target=_restart_worker, name="restart", daemon=True).start()


def exit_app(icon=None, item=None) -> None:
    LOGGER.info("Завершение приложения")
    if SERVER:
        SERVER.should_exit = True
    _remove_runtime_file()
    _release_mutex()
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


def _port_is_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((HOST, port))
            return True
        except OSError:
            return False


def choose_port() -> int:
    # 4784 остаётся предпочтительным. Если его заняла другая программа — приложение не умирает.
    for candidate in range(PORT, PORT + 20):
        if _port_is_free(candidate):
            return candidate
    raise RuntimeError(f"Не найден свободный локальный порт в диапазоне {PORT}-{PORT + 19}")


def _write_runtime_file(port: int) -> None:
    APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"pid": os.getpid(), "port": port, "url": f"http://{HOST}:{port}", "version": VERSION}
    tmp = RUNTIME_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(RUNTIME_FILE)


def _read_runtime_url() -> str | None:
    try:
        if not RUNTIME_FILE.exists():
            return None
        data = json.loads(RUNTIME_FILE.read_text(encoding="utf-8"))
        url = str(data.get("url") or "").strip()
        return url or None
    except Exception:
        return None


def _remove_runtime_file() -> None:
    try:
        RUNTIME_FILE.unlink(missing_ok=True)
    except Exception:
        pass


def start_server(port: int) -> None:
    global SERVER
    try:
        config = uvicorn.Config(
            app,
            host=HOST,
            port=port,
            log_level="warning",
            access_log=False,
            # Для frozen/tray режима используем asyncio без внешнего авто-выбора loop implementation.
            loop="asyncio",
        )
        SERVER = uvicorn.Server(config)
        LOGGER.info("Запуск веб-сервера http://%s:%s", HOST, port)
        SERVER.run()
    except BaseException:
        SERVER_FAILED.set()
        LOGGER.exception("Критическая ошибка веб-сервера")
    finally:
        SERVER_STOPPED.set()
        LOGGER.info("Поток веб-сервера завершён")


def acquire_single_instance() -> bool:
    global _MUTEX_HANDLE
    if os.name != "nt":
        return True
    kernel32 = ctypes.windll.kernel32
    _MUTEX_HANDLE = kernel32.CreateMutexW(None, False, "Local\\4geg_1OFD_FiscalDocs")
    ERROR_ALREADY_EXISTS = 183
    return kernel32.GetLastError() != ERROR_ALREADY_EXISTS


def _process_cli_wait() -> None:
    if "--wait-for-pid" not in sys.argv:
        return
    try:
        idx = sys.argv.index("--wait-for-pid")
        pid = int(sys.argv[idx + 1])
    except (ValueError, IndexError):
        return
    _wait_for_pid_to_exit(pid)


def _open_existing_instance() -> None:
    url = _read_runtime_url() or f"http://{HOST}:{PORT}"
    LOGGER.info("Приложение уже запущено — открываю %s", url)
    # Второй процесс не должен ждать 20 секунд: если runtime живой, открываем быстро.
    if is_server_ready(url, timeout=0.8):
        _open_url_windows(url)
    else:
        LOGGER.warning("Найден mutex, но существующий веб-сервер не отвечает: %s", url)


def main() -> None:
    global TRAY, CURRENT_PORT, CURRENT_WEB_URL
    setup_logging()
    LOGGER.info("==== START %s v%s pid=%s frozen=%s ====", APP_DISPLAY_NAME, VERSION, os.getpid(), getattr(sys, "frozen", False))

    _process_cli_wait()

    if not acquire_single_instance():
        _open_existing_instance()
        return

    try:
        CURRENT_PORT = choose_port()
    except Exception:
        LOGGER.exception("Не удалось выбрать локальный порт")
        _release_mutex()
        return

    CURRENT_WEB_URL = f"http://{HOST}:{CURRENT_PORT}"
    _write_runtime_file(CURRENT_PORT)

    menu = pystray.Menu(
        pystray.MenuItem("Открыть", open_web, default=True),
        pystray.Menu.SEPARATOR,
        pystray.MenuItem("Перезапустить", restart_app),
        pystray.MenuItem("Открыть журнал", open_log),
        pystray.MenuItem("Папка данных", open_data_folder),
        pystray.Menu.SEPARATOR,
        pystray.MenuItem("Завершить", exit_app),
    )
    TRAY = pystray.Icon(
        "1ofd_fiscal_docs",
        create_tray_image(),
        f"{APP_DISPLAY_NAME} v{VERSION}",
        menu,
    )

    # Сервер и автооткрытие браузера работают независимо от UI-потока трея.
    threading.Thread(target=start_server, args=(CURRENT_PORT,), name="uvicorn", daemon=True).start()
    threading.Thread(target=_open_web_worker, kwargs={"startup": True}, name="startup-open", daemon=True).start()

    try:
        TRAY.run()
    except BaseException:
        LOGGER.exception("Критическая ошибка системного трея")
    finally:
        if SERVER:
            SERVER.should_exit = True
        _remove_runtime_file()
        _release_mutex()
        LOGGER.info("==== STOP pid=%s ====", os.getpid())


if __name__ == "__main__":
    main()
