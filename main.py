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
SERVER_THREAD: threading.Thread | None = None
TRAY: pystray.Icon | None = None
_MUTEX_HANDLE = None
CURRENT_PORT = PORT
CURRENT_WEB_URL = f"http://{HOST}:{PORT}"
RUNTIME_FILE = APP_DATA_DIR / "runtime.json"
SERVER_FAILED = threading.Event()
SERVER_STOPPED = threading.Event()
RESTART_LOCK = threading.Lock()


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "app.log"
    handler = RotatingFileHandler(log_file, maxBytes=1_500_000, backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(threadName)s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if not any(isinstance(h, RotatingFileHandler) for h in root.handlers):
        root.addHandler(handler)


def _health_url(base_url: str) -> str:
    return base_url.rstrip("/") + "/api/health"


def is_server_ready(base_url: str | None = None, timeout: float = 0.7) -> bool:
    url = _health_url(base_url or CURRENT_WEB_URL)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status == 200
    except Exception:
        return False


def wait_for_server(base_url: str | None = None, timeout: float = 25.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if SERVER_FAILED.is_set():
            return False
        if is_server_ready(base_url):
            return True
        time.sleep(0.2)
    return False


def _open_url_windows(url: str) -> None:
    try:
        if os.name == "nt":
            os.startfile(url)  # type: ignore[attr-defined]
            return
    except Exception:
        LOGGER.exception("os.startfile не смог открыть браузер")

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
    timeout = 30.0 if startup else 5.0
    if wait_for_server(target, timeout=timeout):
        # Небольшая пауза после первого успешного health-check снижает шанс гонки
        # между готовностью сокета и полной инициализацией браузерного UI.
        if startup:
            time.sleep(0.35)
        LOGGER.info("Открываю веб-интерфейс: %s", target)
        _open_url_windows(target)
        return

    LOGGER.error("Веб-интерфейс недоступен: %s", target)
    _notify(
        "1OFD Fiscal Docs",
        "Веб-сервер не запустился. Откройте журнал через меню значка в трее.",
    )


def open_web(*_args: Any) -> None:
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


def _resource_path(relative: str) -> Path:
    base = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
    return base / relative


def create_tray_image() -> Image.Image:
    # Используем ту же иконку, что у EXE. Fallback оставлен для разработки,
    # если assets/app.ico отсутствует.
    icon_path = _resource_path("assets/app.ico")
    try:
        if icon_path.exists():
            return Image.open(icon_path).convert("RGBA")
    except Exception:
        LOGGER.exception("Не удалось загрузить assets/app.ico для трея")

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


def choose_port(preferred: int | None = None) -> int:
    candidates: list[int] = []
    if preferred is not None:
        candidates.append(preferred)
    candidates.extend(p for p in range(PORT, PORT + 20) if p not in candidates)
    for candidate in candidates:
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
    SERVER_FAILED.clear()
    SERVER_STOPPED.clear()
    try:
        config = uvicorn.Config(
            app,
            host=HOST,
            port=port,
            log_level="warning",
            access_log=False,
            loop="asyncio",
            # В PyInstaller --noconsole sys.stdout/sys.stderr == None.
            # Стандартный log_config Uvicorn пытается вызвать sys.stdout.isatty()
            # и падает до старта веб-сервера. Собственное логирование приложения
            # уже настроено выше, поэтому конфигурацию логов Uvicorn отключаем.
            log_config=None,
        )
        server = uvicorn.Server(config)
        SERVER = server
        LOGGER.info("Запуск веб-сервера http://%s:%s", HOST, port)
        server.run()
    except BaseException:
        SERVER_FAILED.set()
        LOGGER.exception("Критическая ошибка веб-сервера")
    finally:
        SERVER_STOPPED.set()
        LOGGER.info("Поток веб-сервера завершён")


def _launch_server(port: int) -> None:
    global SERVER_THREAD
    SERVER_THREAD = threading.Thread(target=start_server, args=(port,), name="uvicorn", daemon=True)
    SERVER_THREAD.start()


def _stop_server(timeout: float = 10.0) -> bool:
    global SERVER
    server = SERVER
    thread = SERVER_THREAD
    if server is not None:
        server.should_exit = True
    if thread and thread.is_alive():
        thread.join(timeout=timeout)
    stopped = not thread or not thread.is_alive()
    if stopped:
        SERVER = None
    return stopped


def _restart_worker() -> None:
    global CURRENT_PORT, CURRENT_WEB_URL
    if not RESTART_LOCK.acquire(blocking=False):
        return
    try:
        LOGGER.info("Перезапуск локального веб-сервера")
        _notify("1OFD Fiscal Docs", "Перезапускаю веб-сервер…")

        if not _stop_server(timeout=10.0):
            LOGGER.error("Старый веб-сервер не остановился за отведённое время")
            _notify("1OFD Fiscal Docs", "Не удалось перезапустить сервер. Откройте журнал.")
            return

        # Перезапускаем сервер в ЭТОМ ЖЕ процессе. Это принципиально для PyInstaller
        # onefile: не создаём дочерний EXE и не оставляем заблокированный _MEI-каталог.
        time.sleep(0.25)
        try:
            CURRENT_PORT = choose_port(preferred=CURRENT_PORT)
        except Exception:
            LOGGER.exception("Не удалось выбрать порт при перезапуске")
            _notify("1OFD Fiscal Docs", "Не удалось выбрать локальный порт.")
            return

        CURRENT_WEB_URL = f"http://{HOST}:{CURRENT_PORT}"
        _write_runtime_file(CURRENT_PORT)
        _launch_server(CURRENT_PORT)

        if wait_for_server(CURRENT_WEB_URL, timeout=25.0):
            LOGGER.info("Веб-сервер успешно перезапущен")
            _open_url_windows(CURRENT_WEB_URL)
        else:
            LOGGER.error("Веб-сервер не поднялся после перезапуска")
            _notify("1OFD Fiscal Docs", "Веб-сервер не запустился после перезапуска.")
    finally:
        RESTART_LOCK.release()


def restart_app(*_args: Any) -> None:
    threading.Thread(target=_restart_worker, name="restart", daemon=True).start()


def exit_app(icon=None, item=None) -> None:
    LOGGER.info("Запрошено завершение приложения")
    if SERVER:
        SERVER.should_exit = True
    if icon:
        icon.stop()


def acquire_single_instance() -> bool:
    global _MUTEX_HANDLE
    if os.name != "nt":
        return True
    kernel32 = ctypes.windll.kernel32
    _MUTEX_HANDLE = kernel32.CreateMutexW(None, False, "Local\\4geg_1OFD_FiscalDocs")
    ERROR_ALREADY_EXISTS = 183
    return kernel32.GetLastError() != ERROR_ALREADY_EXISTS


def _open_existing_instance() -> None:
    url = _read_runtime_url() or f"http://{HOST}:{PORT}"
    LOGGER.info("Приложение уже запущено — пробую открыть %s", url)
    # Если второй запуск пришёл во время первого старта, несколько секунд ждём readiness.
    if wait_for_server(url, timeout=6.0):
        _open_url_windows(url)
    else:
        LOGGER.warning("Найден mutex, но существующий веб-сервер не отвечает: %s", url)


def _tray_setup(icon: pystray.Icon) -> None:
    """Стартуем сервер только после того, как event-loop трея уже инициализирован."""
    global CURRENT_PORT, CURRENT_WEB_URL
    icon.visible = True
    LOGGER.info("Tray готов, запускаю сервер")

    try:
        CURRENT_PORT = choose_port(preferred=CURRENT_PORT)
    except Exception:
        LOGGER.exception("Не удалось выбрать локальный порт")
        _notify("1OFD Fiscal Docs", "Не удалось выбрать локальный порт. Откройте журнал.")
        return

    CURRENT_WEB_URL = f"http://{HOST}:{CURRENT_PORT}"
    _write_runtime_file(CURRENT_PORT)
    _launch_server(CURRENT_PORT)
    threading.Thread(
        target=_open_web_worker,
        kwargs={"startup": True},
        name="startup-open",
        daemon=True,
    ).start()


def main() -> None:
    global TRAY, CURRENT_PORT, CURRENT_WEB_URL
    setup_logging()
    LOGGER.info(
        "==== START %s v%s pid=%s frozen=%s ====",
        APP_DISPLAY_NAME,
        VERSION,
        os.getpid(),
        getattr(sys, "frozen", False),
    )

    if not acquire_single_instance():
        _open_existing_instance()
        return

    CURRENT_PORT = PORT
    CURRENT_WEB_URL = f"http://{HOST}:{CURRENT_PORT}"

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

    try:
        # setup вызывается после готовности event-loop трея — это устраняет гонку
        # первого запуска, из-за которой браузер раньше иногда не открывался.
        TRAY.run(setup=_tray_setup)
    except BaseException:
        LOGGER.exception("Критическая ошибка системного трея")
    finally:
        LOGGER.info("Останавливаю веб-сервер перед завершением процесса")
        if SERVER:
            SERVER.should_exit = True
        if SERVER_THREAD and SERVER_THREAD.is_alive():
            SERVER_THREAD.join(timeout=10.0)
        _remove_runtime_file()
        _release_mutex()
        LOGGER.info("==== STOP pid=%s ====", os.getpid())


if __name__ == "__main__":
    main()
