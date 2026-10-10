from __future__ import annotations

import base64
import ctypes
import json
import os
import threading
import uuid
from ctypes import wintypes
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from version import APP_NAME, AUTHOR

APP_DATA_DIR = Path(os.getenv("APPDATA") or Path.home()) / AUTHOR / APP_NAME.replace(" ", "_")
APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_FILE = APP_DATA_DIR / "config.json"
CACHE_FILE = APP_DATA_DIR / "ofd_kkm_cache.json"
LOG_DIR = APP_DATA_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
RUNTIME_SETTINGS_FILE = APP_DATA_DIR / "runtime_settings.json"
LOAD_PROFILES = {
    "economy": {"name": "Минимальный", "concurrency_cap": 30, "intermediate_memory_mb": 32, "sqlite_cache_mb": 2, "storage_mode": "disk", "http_chunk_kb": 16, "page_rows": 100},
    "normal": {"name": "Средний", "concurrency_cap": 120, "intermediate_memory_mb": 64, "sqlite_cache_mb": 4, "storage_mode": "disk", "http_chunk_kb": 32, "page_rows": 200},
    "performance": {"name": "Высокий", "concurrency_cap": 160, "intermediate_memory_mb": 256, "sqlite_cache_mb": 8, "storage_mode": "mixed", "http_chunk_kb": 64, "page_rows": 500},
    "maximum": {"name": "Максимальный", "concurrency_cap": 200, "intermediate_memory_mb": 512, "sqlite_cache_mb": 16, "storage_mode": "memory", "http_chunk_kb": 128, "page_rows": 1000},
}
STORAGE_MODE_NAMES = {"disk": "на диск", "memory": "в память", "mixed": "смешанный"}
MANUAL_SETTING_LABELS = {
    "concurrency_cap": "Одновременных запросов",
    "intermediate_memory_mb": "Лимит памяти для промежуточных данных (МБ)",
    "sqlite_cache_mb": "Размер кэша базы данных (МБ)",
    "storage_mode": "Режим хранения временных данных",
    "http_chunk_kb": "Размер блока чтения ответа (КБ)",
    "page_rows": "Строк на странице, максимум",
}
MANUAL_SETTING_RANGES = {
    "concurrency_cap": (1, 200), "intermediate_memory_mb": (1, 4096),
    "sqlite_cache_mb": (1, 256), "http_chunk_kb": (4, 1024), "page_rows": (10, 1000),
}
RUNTIME_SETTING_CHOICES = {
    "log_limit_mb": (1, 5, 10, 25, 50), "load_profile": tuple(LOAD_PROFILES),
    "concurrency_cap": (10, 30, 60, 120, 160, 200),
    "intermediate_memory_mb": (16, 32, 64, 128, 256, 512, 1024),
    "sqlite_cache_mb": (1, 2, 4, 8, 16, 32, 64),
    "storage_mode": tuple(STORAGE_MODE_NAMES), "http_chunk_kb": (16, 32, 64, 128, 256),
    "page_rows": (50, 100, 200, 500, 1000),
}
_SETTINGS_LOCK = threading.RLock()
_RUNTIME_SETTINGS: dict[str, Any] | None = None
_SETTINGS_LISTENERS: list[Any] = []


def register_settings_listener(callback: Any) -> None:
    if callback not in _SETTINGS_LISTENERS:
        _SETTINGS_LISTENERS.append(callback)


def get_runtime_settings() -> dict[str, Any]:
    """Small separate file: tray writes never race with encrypted API profiles."""
    global _RUNTIME_SETTINGS
    with _SETTINGS_LOCK:
        if _RUNTIME_SETTINGS is None:
            settings = {"log_limit_mb": 5, "load_profile": "normal", "manual_modified": False,
                        **{k: v for k, v in LOAD_PROFILES["normal"].items() if k != "name"}}
            try:
                with RUNTIME_SETTINGS_FILE.open(encoding="utf-8") as stream:
                    saved = json.load(stream)
                if not isinstance(saved, dict):
                    raise ValueError("Некорректные настройки")
                profile_key = saved.get("load_profile", "normal")
                if not isinstance(profile_key, str) or profile_key not in LOAD_PROFILES:
                    profile_key = "normal"
                if "load_profile" not in saved and saved.get("concurrency_cap") == 30:
                    profile_key = "economy"
                settings.update(load_profile=profile_key, **{k: v for k, v in LOAD_PROFILES[profile_key].items() if k != "name"})
                for key in ("log_limit_mb", *MANUAL_SETTING_LABELS):
                    if key in saved:
                        try:
                            validate_runtime_setting(key, saved[key])
                            settings[key] = saved[key]
                        except ValueError:
                            pass
                if "load_profile" not in saved:
                    settings["concurrency_cap"] = min(settings["concurrency_cap"], 120)
                settings["manual_modified"] = saved.get("manual_modified") is True or any(
                    settings[key] != LOAD_PROFILES[profile_key][key] for key in MANUAL_SETTING_LABELS)
            except (OSError, ValueError, AttributeError):
                pass
            _RUNTIME_SETTINGS = settings
        return dict(_RUNTIME_SETTINGS)


def get_load_profile() -> dict[str, Any]:
    settings = get_runtime_settings()
    return {"name": LOAD_PROFILES[settings["load_profile"]]["name"], "key": settings["load_profile"],
            "manual_modified": settings["manual_modified"], "write_batch": 128,
            **{key: settings[key] for key in MANUAL_SETTING_LABELS}}


def validate_runtime_setting(key: str, value: Any) -> None:
    if key in MANUAL_SETTING_RANGES:
        low, high = MANUAL_SETTING_RANGES[key]
        if type(value) is int and low <= value <= high:
            return
        raise ValueError(f"{MANUAL_SETTING_LABELS[key]}: целое число от {low} до {high}")
    if key in RUNTIME_SETTING_CHOICES and value in RUNTIME_SETTING_CHOICES[key] and not isinstance(value, bool):
        return
    raise ValueError("Недопустимое значение настройки")


def set_manual_settings(values: dict[str, Any]) -> dict[str, Any]:
    """Validate the complete change before saving; a manual edit retains its preset."""
    if not values or any(key not in MANUAL_SETTING_LABELS for key in values):
        raise ValueError("Неизвестные параметры ручных настроек")
    for key, value in values.items():
        validate_runtime_setting(key, value)
    with _SETTINGS_LOCK:
        settings = get_runtime_settings()
        changed = any(settings[key] != value for key, value in values.items())
        settings.update(values)
        settings["manual_modified"] = settings["manual_modified"] or changed
        return _save_runtime_settings(settings)


def _save_runtime_settings(settings: dict[str, Any]) -> dict[str, Any]:
    global _RUNTIME_SETTINGS
    temp = RUNTIME_SETTINGS_FILE.with_suffix(".tmp")
    with temp.open("w", encoding="utf-8") as stream:
        json.dump(settings, stream, ensure_ascii=False)
        stream.flush()
        os.fsync(stream.fileno())
    temp.replace(RUNTIME_SETTINGS_FILE)
    _RUNTIME_SETTINGS = settings
    for callback in tuple(_SETTINGS_LISTENERS):
        try:
            callback()
        except Exception:
            pass  # Saving settings must not fail because the tray is closing.
    return dict(settings)


def set_runtime_setting(key: str, value: int | str) -> dict[str, Any]:
    validate_runtime_setting(key, value)
    if key in MANUAL_SETTING_LABELS:
        return set_manual_settings({key: value})
    with _SETTINGS_LOCK:
        settings = get_runtime_settings()
        settings[key] = value
        if key == "load_profile":
            settings.update({k: v for k, v in LOAD_PROFILES[value].items() if k != "name"})
            settings["manual_modified"] = False
        return _save_runtime_settings(settings)


class DATA_BLOB(ctypes.Structure):
    _fields_ = [
        ("cbData", wintypes.DWORD),
        ("pbData", ctypes.POINTER(ctypes.c_byte)),
    ]


def _protect_windows(value: str) -> str:
    raw = value.encode("utf-8")
    in_buffer = ctypes.create_string_buffer(raw)
    in_blob = DATA_BLOB(len(raw), ctypes.cast(in_buffer, ctypes.POINTER(ctypes.c_byte)))
    out_blob = DATA_BLOB()

    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    if not crypt32.CryptProtectData(
        ctypes.byref(in_blob),
        None,
        None,
        None,
        None,
        0,
        ctypes.byref(out_blob),
    ):
        raise ctypes.WinError()

    try:
        protected = ctypes.string_at(out_blob.pbData, out_blob.cbData)
        return "dpapi:" + base64.b64encode(protected).decode("ascii")
    finally:
        kernel32.LocalFree(out_blob.pbData)


def _unprotect_windows(value: str) -> str:
    payload = base64.b64decode(value.removeprefix("dpapi:"))
    in_buffer = ctypes.create_string_buffer(payload)
    in_blob = DATA_BLOB(len(payload), ctypes.cast(in_buffer, ctypes.POINTER(ctypes.c_byte)))
    out_blob = DATA_BLOB()

    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    if not crypt32.CryptUnprotectData(
        ctypes.byref(in_blob),
        None,
        None,
        None,
        None,
        0,
        ctypes.byref(out_blob),
    ):
        raise ctypes.WinError()

    try:
        raw = ctypes.string_at(out_blob.pbData, out_blob.cbData)
        return raw.decode("utf-8")
    finally:
        kernel32.LocalFree(out_blob.pbData)


def protect_secret(value: str) -> str:
    if os.name == "nt":
        return _protect_windows(value)
    # Только для разработки вне Windows. Релизная сборка рассчитана на Windows.
    return "dev:" + base64.b64encode(value.encode("utf-8")).decode("ascii")


def unprotect_secret(value: str) -> str:
    if value.startswith("dpapi:"):
        if os.name != "nt":
            raise RuntimeError("DPAPI-ключ можно расшифровать только в Windows-профиле, где он был сохранён")
        return _unprotect_windows(value)
    if value.startswith("dev:"):
        return base64.b64decode(value.removeprefix("dev:")).decode("utf-8")
    raise ValueError("Неизвестный формат сохранённого секрета")


def _empty_config() -> dict[str, Any]:
    return {"version": 1, "active_id": None, "profiles": []}


def load_config() -> dict[str, Any]:
    if not CONFIG_FILE.exists():
        return _empty_config()
    try:
        data = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return _empty_config()
        data.setdefault("version", 1)
        data.setdefault("active_id", None)
        data.setdefault("profiles", [])
        return data
    except Exception:
        return _empty_config()


def save_config(data: dict[str, Any]) -> None:
    APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
    temp = CONFIG_FILE.with_suffix(".tmp")
    temp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(CONFIG_FILE)


def _public_profile(profile: dict[str, Any], active_id: str | None) -> dict[str, Any]:
    return {
        "id": profile.get("id"),
        "name": profile.get("name") or "API-ключ",
        "last4": profile.get("last4") or "",
        "created_at": profile.get("created_at"),
        "active": profile.get("id") == active_id,
    }


def list_profiles() -> dict[str, Any]:
    data = load_config()
    active_id = data.get("active_id")
    profiles = [_public_profile(p, active_id) for p in data.get("profiles", [])]
    return {"active_id": active_id, "profiles": profiles}


def get_active_profile() -> dict[str, Any] | None:
    data = load_config()
    active_id = data.get("active_id")
    for profile in data.get("profiles", []):
        if profile.get("id") == active_id:
            result = dict(profile)
            result["api_key"] = unprotect_secret(str(profile["secret"]))
            return result
    return None


def add_or_select_profile(api_key: str, name: str) -> dict[str, Any]:
    api_key = api_key.strip()
    if not api_key:
        raise ValueError("API-ключ пуст")

    data = load_config()
    for profile in data.get("profiles", []):
        try:
            existing = unprotect_secret(str(profile.get("secret", "")))
        except Exception:
            continue
        if existing == api_key:
            profile["name"] = name or profile.get("name") or "API-ключ"
            profile["last4"] = api_key[-4:]
            data["active_id"] = profile["id"]
            save_config(data)
            return _public_profile(profile, profile["id"])

    profile = {
        "id": str(uuid.uuid4()),
        "name": name or "API-ключ",
        "last4": api_key[-4:],
        "secret": protect_secret(api_key),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    data.setdefault("profiles", []).append(profile)
    data["active_id"] = profile["id"]
    save_config(data)
    return _public_profile(profile, profile["id"])


def get_profile_secret(profile_id: str) -> str:
    """Return decrypted key for the local UI only. Never log the returned value."""
    data = load_config()
    for profile in data.get("profiles", []):
        if profile.get("id") == profile_id:
            return unprotect_secret(str(profile.get("secret", "")))
    raise ValueError("Сохранённый API-ключ не найден")


def select_profile(profile_id: str) -> dict[str, Any]:
    data = load_config()
    for profile in data.get("profiles", []):
        if profile.get("id") == profile_id:
            data["active_id"] = profile_id
            save_config(data)
            return _public_profile(profile, profile_id)
    raise ValueError("Сохранённый API-ключ не найден")


def delete_profile(profile_id: str) -> None:
    data = load_config()
    profiles = [p for p in data.get("profiles", []) if p.get("id") != profile_id]
    data["profiles"] = profiles
    if data.get("active_id") == profile_id:
        data["active_id"] = profiles[0]["id"] if profiles else None
    save_config(data)
