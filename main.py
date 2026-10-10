from __future__ import annotations

import ctypes
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any

import pystray
import uvicorn
from PIL import Image, ImageDraw, ImageFont

from config_store import APP_DATA_DIR, LOG_DIR, LOAD_PROFILES, RUNTIME_SETTING_CHOICES, get_load_profile, get_runtime_settings, set_runtime_setting, MANUAL_SETTING_LABELS, STORAGE_MODE_NAMES, register_settings_listener
from resource_bundle import ensure_resources
from version import APP_NAME, APP_DISPLAY_NAME, VERSION, HOST, PORT

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
SERVER_SOCKET: socket.socket | None = None
APP_INSTANCE_ID: str | None = None
app: Any = None


class SizeLimitedLogHandler(RotatingFileHandler):
    """Keep an active log and two previous logs, limited by UTF-8 byte size."""
    def __init__(self, filename: Path, limit_mb: int):
        super().__init__(filename, maxBytes=limit_mb * 1024 * 1024,
                         backupCount=2, encoding="utf-8")
        self.limit_bytes = limit_mb * 1024 * 1024
        self.set_limit(limit_mb)

    def _open(self):
        # Disable CRLF translation so the encoded byte count is exact on Windows.
        return open(self.baseFilename, self.mode, encoding=self.encoding, errors="replace", newline="")

    def _trim_tail(self, filename: Path) -> None:
        if not filename.exists() or filename.stat().st_size <= self.limit_bytes:
            return
        # A smaller tray limit takes effect on existing files too. Keep their
        # newest records; memory used here is bounded by the selected file limit.
        with filename.open("rb") as source:
            source.seek(-self.limit_bytes, os.SEEK_END)
            tail = source.read(self.limit_bytes)
        newline = tail.find(b"\n")
        tail = tail[newline + 1:] if newline >= 0 else tail.decode("utf-8", errors="ignore").encode("utf-8")
        temporary = filename.with_name(filename.name + ".limit.tmp")
        try:
            temporary.write_bytes(tail)
            os.replace(temporary, filename)
        finally:
            temporary.unlink(missing_ok=True)

    def set_limit(self, limit_mb: int) -> None:
        self.acquire()
        try:
            self.limit_bytes = limit_mb * 1024 * 1024
            self.maxBytes = self.limit_bytes
            if self.stream:
                self.stream.flush()
                self.stream.close()
                self.stream = None
            for suffix in ("", ".1", ".2"):
                self._trim_tail(Path(self.baseFilename + suffix))
        finally:
            self.release()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            # Bound even a single huge exception/API error before writing it.
            encoded = (self.format(record) + self.terminator).encode("utf-8", errors="replace")
            if len(encoded) > 16 * 1024:
                marker = b"\n[log entry truncated]\n"
                encoded = encoded[:16 * 1024 - len(marker)].decode("utf-8", errors="ignore").encode("utf-8") + marker
            if self.stream is None:
                self.stream = self._open()
            if os.fstat(self.stream.fileno()).st_size + len(encoded) > self.limit_bytes:
                self.doRollover()
            self.stream.write(encoded.decode("utf-8"))
            self.flush()
        except Exception:
            self.handleError(record)


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "app.log"
    root = logging.getLogger()
    if any(isinstance(h, SizeLimitedLogHandler) for h in root.handlers):
        return
    # Preserve the two newest archives across restarts; remove older legacy ones.
    for old in LOG_DIR.glob("app.log.*"):
        suffix = old.name.removeprefix("app.log.")
        if suffix.isdigit() and int(suffix) > 2:
            try:
                old.unlink()
            except OSError:
                pass
    handler = SizeLimitedLogHandler(log_file, get_runtime_settings()["log_limit_mb"])
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(threadName)s %(name)s: %(message)s"))
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def _setting_action(key: str, value: int | str):
    def worker(icon: pystray.Icon) -> None:
        try:
            set_runtime_setting(key, value)
            if key == "log_limit_mb":
                for handler in logging.getLogger().handlers:
                    if isinstance(handler, SizeLimitedLogHandler):
                        handler.set_limit(value)
            icon.update_menu()
            if key == "load_profile":
                profile = get_load_profile()
                message = f"{profile['name']}: до {profile['concurrency_cap']} обращений; кэш индекса {profile['sqlite_cache_mb']} МБ. Применяется к следующему поиску; просмотр — сразу."
            elif key == "log_limit_mb":
                message = f"Размер журнала на файл: {value} МБ. Сохраняются 3 файла."
            else:
                message = "Ручные настройки сохранены. Профиль помечен как изменённый вручную. Новые поиски используют эти значения."
            _notify("1OFD Fiscal Docs", message)
        except Exception:
            LOGGER.exception("Не удалось сохранить настройку трея")
            _notify("1OFD Fiscal Docs", "Не удалось сохранить настройку. Откройте журнал.")
    def action(icon: pystray.Icon, item: Any) -> None:
        threading.Thread(target=worker, args=(icon,), name="tray-setting", daemon=True).start()
    return action


def _refresh_tray_settings() -> None:
    if TRAY is not None:
        TRAY.update_menu()


register_settings_listener(_refresh_tray_settings)


def _manual_settings_action(field: str = "all"):
    def action(icon: pystray.Icon, item: Any) -> None:
        def worker() -> None:
            if wait_for_server(CURRENT_WEB_URL, timeout=5, instance_id=APP_INSTANCE_ID):
                _open_url_windows(CURRENT_WEB_URL + "/?load-settings=" + field)
        threading.Thread(target=worker, name="tray-manual-settings", daemon=True).start()
    return action


def _settings_menu() -> pystray.Menu:
    def selected(key: str, value: int | str):
        return lambda item: get_runtime_settings()[key] == value

    def choices(key: str, unit: str = "") -> pystray.Menu:
        items = [pystray.MenuItem(
            (STORAGE_MODE_NAMES[value] if key == "storage_mode" else f"{value} {unit}".strip()),
            _setting_action(key, value), radio=True, checked=selected(key, value),
        ) for value in RUNTIME_SETTING_CHOICES[key]]
        if key not in ("storage_mode", "log_limit_mb"):
            items.extend([pystray.Menu.SEPARATOR,
                          pystray.MenuItem("Ввести другое число…", _manual_settings_action(key))])
        return pystray.Menu(*items)

    def preset_caption(item: Any) -> str:
        profile = get_load_profile()
        suffix = " (изменён вручную)" if profile["manual_modified"] else ""
        return f"Профиль нагрузки: {profile['name']}{suffix}"

    def manual_caption(key: str):
        def caption(item: Any) -> str:
            value = get_runtime_settings()[key]
            if key == "storage_mode":
                value = STORAGE_MODE_NAMES[value]
            return f"{MANUAL_SETTING_LABELS[key]}: {value}"
        return caption

    descriptions = {"economy": "меньше нагрузки", "normal": "ответы на диск",
                    "performance": "часть ответов в памяти", "maximum": "больше расход памяти"}
    return pystray.Menu(
        pystray.MenuItem(lambda item: f"Размер журнала (на файл): {get_runtime_settings()['log_limit_mb']} МБ", choices("log_limit_mb", "МБ")),
        pystray.MenuItem(preset_caption, pystray.Menu(*(
            pystray.MenuItem(f"{p['name']} — {descriptions[key]}", _setting_action("load_profile", key),
                            radio=True, checked=selected("load_profile", key))
            for key, p in LOAD_PROFILES.items()
        ))),
        pystray.Menu.SEPARATOR,
        pystray.MenuItem("Ручные настройки", pystray.Menu(
            pystray.MenuItem("Открыть форму настроек…", _manual_settings_action()),
            pystray.Menu.SEPARATOR,
            *(pystray.MenuItem(manual_caption(key), choices(key)) for key in MANUAL_SETTING_LABELS),
        )),
    )


def _health_url(base_url: str) -> str:
    return base_url.rstrip("/") + "/api/health"


def is_server_ready(base_url: str | None = None, timeout: float = 0.7, instance_id: str | None = None) -> bool:
    url = _health_url(base_url or CURRENT_WEB_URL)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            payload = json.loads(response.read(8192))
            return response.status == 200 and payload.get("ok") is True and payload.get("app") == APP_NAME and (instance_id is None or payload.get("instance_id") == instance_id)
    except Exception:
        return False


def wait_for_server(base_url: str | None = None, timeout: float = 25.0, instance_id: str | None = None) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if SERVER_FAILED.is_set():
            return False
        if is_server_ready(base_url, instance_id=instance_id):
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
    if wait_for_server(target, timeout=timeout, instance_id=APP_INSTANCE_ID):
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


def _copy_text_windows(text: str) -> bool:
    """Copy Unicode text to Windows clipboard without external dependencies."""
    if os.name != "nt":
        return False

    user32 = ctypes.windll.user32
    kernel32 = ctypes.windll.kernel32
    CF_UNICODETEXT = 13
    GMEM_MOVEABLE = 0x0002

    kernel32.GlobalAlloc.argtypes = [ctypes.c_uint, ctypes.c_size_t]
    kernel32.GlobalAlloc.restype = ctypes.c_void_p
    kernel32.GlobalLock.argtypes = [ctypes.c_void_p]
    kernel32.GlobalLock.restype = ctypes.c_void_p
    kernel32.GlobalUnlock.argtypes = [ctypes.c_void_p]
    kernel32.GlobalFree.argtypes = [ctypes.c_void_p]
    user32.OpenClipboard.argtypes = [ctypes.c_void_p]
    user32.OpenClipboard.restype = ctypes.c_int
    user32.SetClipboardData.argtypes = [ctypes.c_uint, ctypes.c_void_p]
    user32.SetClipboardData.restype = ctypes.c_void_p

    data = (text + "\0").encode("utf-16-le")
    handle = kernel32.GlobalAlloc(GMEM_MOVEABLE, len(data))
    if not handle:
        return False

    ptr = kernel32.GlobalLock(handle)
    if not ptr:
        kernel32.GlobalFree(handle)
        return False

    ctypes.memmove(ptr, data, len(data))
    kernel32.GlobalUnlock(handle)

    if not user32.OpenClipboard(None):
        kernel32.GlobalFree(handle)
        return False

    success = False
    try:
        user32.EmptyClipboard()
        # On success ownership of handle passes to the system; do not free it.
        success = bool(user32.SetClipboardData(CF_UNICODETEXT, handle))
    finally:
        user32.CloseClipboard()

    if not success:
        kernel32.GlobalFree(handle)
    return success


def copy_interface_url(*_args: Any) -> None:
    try:
        if _copy_text_windows(CURRENT_WEB_URL):
            LOGGER.info("Ссылка интерфейса скопирована: %s", CURRENT_WEB_URL)
            _notify("1OFD Fiscal Docs", "Ссылка интерфейса скопирована в буфер обмена")
        else:
            raise RuntimeError("Windows clipboard API вернул ошибку")
    except Exception:
        LOGGER.exception("Не удалось скопировать ссылку интерфейса")
        _notify("1OFD Fiscal Docs", "Не удалось скопировать ссылку. Откройте журнал.")


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
    executable_dir = Path(sys.executable if getattr(sys, "frozen", False) else __file__).resolve().parent
    custom = executable_dir / "app.ico"
    icon_path = custom if custom.is_file() else APP_DATA_DIR / "resources/app.ico"
    try:
        if icon_path.exists():
            return Image.open(icon_path).convert("RGBA")
    except Exception:
        LOGGER.exception("Не удалось загрузить иконку для трея")

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


def choose_port(preferred: int | None = None) -> int:
    global SERVER_SOCKET
    candidates: list[int] = []
    if preferred is not None:
        candidates.append(preferred)
    candidates.extend(p for p in range(PORT, PORT + 20) if p not in candidates)
    for candidate in candidates:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            if os.name == "nt":
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            sock.bind((HOST, candidate))
            sock.listen(128)
            sock.setblocking(False)
            SERVER_SOCKET = sock
            return candidate
        except OSError:
            sock.close()
    raise RuntimeError(f"Не найден свободный локальный порт в диапазоне {PORT}-{PORT + 19}")


def _write_runtime_file(port: int) -> None:
    APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"pid": os.getpid(), "port": port, "url": f"http://{HOST}:{port}", "version": VERSION, "instance_id": APP_INSTANCE_ID}
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


def start_server(port: int, reserved_socket: socket.socket) -> None:
    global SERVER, SERVER_SOCKET
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
        # Keep the bound socket: no gap between a free-port check and Uvicorn.
        server.run(sockets=[reserved_socket])
    except BaseException:
        SERVER_FAILED.set()
        LOGGER.exception("Критическая ошибка веб-сервера")
    finally:
        reserved_socket.close()
        if SERVER_SOCKET is reserved_socket:
            SERVER_SOCKET = None
        SERVER_STOPPED.set()
        LOGGER.info("Поток веб-сервера завершён")


def _launch_server(port: int) -> None:
    global SERVER_THREAD
    if SERVER_SOCKET is None:
        raise RuntimeError("Порт сервера не зарезервирован")
    SERVER_THREAD = threading.Thread(target=start_server, args=(port, SERVER_SOCKET), name="uvicorn", daemon=True)
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

        if wait_for_server(CURRENT_WEB_URL, timeout=25.0, instance_id=APP_INSTANCE_ID):
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
    global TRAY, CURRENT_PORT, CURRENT_WEB_URL, app, APP_INSTANCE_ID
    # Only the owner process writes app.log; a second launch just opens its UI.
    if not acquire_single_instance():
        _open_existing_instance()
        return
    setup_logging()
    ensure_resources(APP_DATA_DIR)
    # Loading last_search.json is reserved for the owner, not each second launch.
    from ofd_app import APP_INSTANCE_ID as instance_id, app as web_app
    app, APP_INSTANCE_ID = web_app, instance_id
    LOGGER.info(
        "==== START %s v%s pid=%s frozen=%s ====",
        APP_DISPLAY_NAME,
        VERSION,
        os.getpid(),
        getattr(sys, "frozen", False),
    )

    CURRENT_PORT = PORT
    CURRENT_WEB_URL = f"http://{HOST}:{CURRENT_PORT}"

    menu = pystray.Menu(
        pystray.MenuItem("Открыть", open_web, default=True),
        pystray.MenuItem("Скопировать ссылку интерфейса", copy_interface_url),
        pystray.Menu.SEPARATOR,
        pystray.MenuItem("Перезапустить", restart_app),
        pystray.MenuItem("Настройки", _settings_menu()),
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
