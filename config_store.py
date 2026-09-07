from __future__ import annotations

import base64
import ctypes
import json
import os
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
