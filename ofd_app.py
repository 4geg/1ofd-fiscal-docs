import asyncio
import json
import logging
import random
import time
from datetime import datetime, timedelta
from typing import Any

import certifi
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from config_store import CACHE_FILE, add_or_select_profile, delete_profile, get_active_profile, get_profile_secret, list_profiles, select_profile
from version import VERSION

BASE_URL = "https://universal-api.1-ofd.ru"
LOGGER = logging.getLogger("ofd_app.api")

# Быстрый основной проход. Если часть запросов не проходит, программа сама
# повторяет только их с меньшей параллельностью.
DEFAULT_DOCUMENT_CONCURRENCY = 120
KKT_CONCURRENCY = 32
RETRY_CONCURRENCY = 24
FINAL_RETRY_CONCURRENCY = 6
CACHE_TTL_MINUTES = 30

# Типы документов, которые Universal API официально принимает в transactionTypes.
# "registration" — отдельный синтетический тип: API возвращает fiscalReport, но
# отдельного значения transactionTypes для первоначальной регистрации нет.
DOCUMENT_TYPES: dict[str, dict[str, str | None]] = {
    "registration": {"api": None, "label": "Регистрация"},
    "reregistration": {"api": "FISCAL_REPORT_CORRECTION", "label": "Перерегистрация"},
    "close": {"api": "CLOSE_ARCHIVE", "label": "Закрытие ФН"},
    "ticket": {"api": "TICKET", "label": "Кассовый чек"},
    "open_shift": {"api": "OPEN_SHIFT", "label": "Открытие смены"},
    "close_shift": {"api": "CLOSE_SHIFT", "label": "Закрытие смены"},
    "receipt_correction": {"api": "RECEIPT_CORRECTION", "label": "Чек коррекции"},
    "bso": {"api": "BSO", "label": "БСО"},
    "bso_correction": {"api": "BSO_CORRECTION", "label": "БСО коррекции"},
}

IRKKT_STATUSES = {"OK", "WARNING", "ERROR", "WAITING"}

TOKEN_CACHE: dict[str, dict[str, Any]] = {}
SEARCH_LOCK = asyncio.Lock()
ACTIVE_SEARCH_TASK: asyncio.Task[Any] | None = None

STATE: dict[str, Any] = {
    "running": False,
    "stage": "Готово",
    "done": 0,
    "total": 0,
    "found": 0,
    "errors": 0,
    "message": "",
    "cancelled": False,
}

# Храним последний результат в памяти, чтобы можно было повторить только
# окончательно неудавшиеся запросы без повторного обхода всего парка.
LAST_SEARCH: dict[str, Any] = {
    "rows": [],
    "failed_tasks": [],
    "meta": {},
}

app = FastAPI(title="Первый ОФД — фискальные документы", version=VERSION)


class SearchRequest(BaseModel):
    date_from: str
    date_to: str
    time_from: str = "00:00"
    time_to: str = "23:59"
    refresh_kkt: bool = False
    document_types: list[str] = Field(
        default_factory=lambda: ["registration", "reregistration", "close"]
    )
    # Быстрые условия верхней панели. Несколько значений объединяются по ИЛИ:
    # удобно добавить три РНМ и запросить только эти три кассы.
    query_terms: list[str] = Field(default_factory=list)
    # Точные/расширенные фильтры по каталогу ККТ. Между полями действует И.
    rnm: str = ""
    kkt_factory_number: str = ""
    fs_number: str = ""
    internal_name: str = ""
    retail_place: str = ""
    address: str = ""
    shift_num: str = ""
    irkkt_statuses: list[str] = Field(default_factory=list)
    concurrency: int = Field(default=DEFAULT_DOCUMENT_CONCURRENCY, ge=20, le=200)


class ApiKeyAddRequest(BaseModel):
    api_key: str


class ApiKeySelectRequest(BaseModel):
    profile_id: str


def set_state(**kwargs: Any) -> None:
    STATE.update(kwargs)


def clean_text(value: Any) -> str:
    return str(value or "").strip().lower()


def contains(value: Any, needle: str) -> bool:
    if not needle:
        return True
    return needle in clean_text(value)


def safe_error_text(error: Exception) -> str:
    text = f"{type(error).__name__}: {error}"
    return text[:800]


def create_http_client(*, limits: httpx.Limits, timeout: httpx.Timeout, http2: bool = False) -> httpx.AsyncClient:
    """Create one HTTP client with an explicit bundled CA store.

    PyInstaller onefile does not always discover certifi's CA bundle reliably on its own.
    Keeping this explicit makes HTTPS behave the same in source and in the frozen EXE.
    """
    return httpx.AsyncClient(
        limits=limits,
        timeout=timeout,
        http2=http2,
        verify=certifi.where(),
    )


async def request_json(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    retries: int = 3,
    **kwargs: Any,
) -> dict[str, Any]:
    """HTTP-запрос с повторами на временных сетевых/серверных ошибках."""
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            response = await client.request(method, url, **kwargs)

            if response.status_code in (429, 500, 502, 503, 504):
                if attempt < retries:
                    retry_after = response.headers.get("Retry-After", "")
                    try:
                        delay = float(retry_after)
                    except ValueError:
                        delay = attempt * 0.9 + random.uniform(0.05, 0.35)
                    await asyncio.sleep(min(delay, 8.0))
                    continue

            response.raise_for_status()

            if response.status_code == 204 or not response.text.strip():
                return {}

            try:
                return response.json()
            except ValueError as error:
                last_error = error
                if attempt < retries:
                    await asyncio.sleep(attempt * 0.8)
                    continue
                raise

        except (
            httpx.TimeoutException,
            httpx.NetworkError,
            httpx.RemoteProtocolError,
        ) as error:
            last_error = error
            if attempt < retries:
                await asyncio.sleep(attempt * 0.8 + random.uniform(0.05, 0.3))
                continue
            raise

    if last_error:
        raise last_error

    raise RuntimeError("Неизвестная ошибка HTTP-запроса")


def api_headers(token: str) -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Authorization": token,
    }


async def authenticate_api_key(client: httpx.AsyncClient, api_key: str) -> str:
    data = await request_json(
        client,
        "POST",
        f"{BASE_URL}/api/auth",
        json={"apiKey": api_key},
        retries=3,
    )
    token = str(data["token"])
    if not token.lower().startswith("bearer "):
        token = f"Bearer {token}"
    return token


async def get_token(client: httpx.AsyncClient, *, force: bool = False) -> str:
    # Поддержка подтвердила срок токена 2 часа. Обновляем через 110 минут.
    profile = get_active_profile()
    if not profile:
        raise ValueError("API-ключ не выбран. Добавь или выбери ключ в интерфейсе.")

    profile_id = str(profile["id"])
    cache = TOKEN_CACHE.setdefault(profile_id, {"token": None, "created_at": 0.0})
    cached = cache.get("token")
    created_at = float(cache.get("created_at", 0))

    if not force and cached and time.time() - created_at < 110 * 60:
        return str(cached)

    token = await authenticate_api_key(client, str(profile["api_key"]))
    cache["token"] = token
    cache["created_at"] = time.time()
    return token


async def get_organisations(
    client: httpx.AsyncClient, token: str
) -> list[dict[str, Any]]:
    data = await request_json(
        client,
        "GET",
        f"{BASE_URL}/api/rent/v3/organisations",
        headers=api_headers(token),
        params={"page": 1, "pageSize": 100},
        retries=3,
    )
    return data.get("page", []) or []


async def get_retail_places(
    client: httpx.AsyncClient,
    token: str,
    organisation_key: str,
) -> list[dict[str, Any]]:
    data = await request_json(
        client,
        "GET",
        f"{BASE_URL}/api/rent/v2/organisations/{organisation_key}/retailPlaces",
        headers=api_headers(token),
        retries=3,
    )
    return data.get("retailPlaces", []) or []


async def get_kkms_by_retail_place(
    client: httpx.AsyncClient,
    token: str,
    organisation_key: str,
    retail_place_id: Any,
    *,
    retries: int = 3,
) -> list[dict[str, Any]]:
    data = await request_json(
        client,
        "GET",
        f"{BASE_URL}/api/rent/v2/organisations/{organisation_key}/kkms",
        headers=api_headers(token),
        params={"retailPlaceId": retail_place_id},
        retries=retries,
    )
    return data.get("kkms", []) or []


def load_cache(organisation_key: str) -> tuple[list[dict[str, Any]] | None, float | None]:
    if not CACHE_FILE.exists():
        return None, None

    try:
        data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))

        # Старые/неполные кэши намеренно не используем.
        if data.get("complete") is not True:
            return None, None
        if data.get("organisationKey") != organisation_key:
            return None, None

        saved_at = float(data.get("savedAt", 0))
        age = time.time() - saved_at
        if age > CACHE_TTL_MINUTES * 60:
            return None, None

        kkms = data.get("kkms")
        if not isinstance(kkms, list):
            return None, None

        return kkms, round(age / 60, 1)
    except Exception:
        return None, None


def save_cache(
    organisation_key: str,
    kkms: list[dict[str, Any]],
    retail_place_count: int,
) -> None:
    CACHE_FILE.write_text(
        json.dumps(
            {
                "organisationKey": organisation_key,
                "savedAt": time.time(),
                "complete": True,
                "retailPlaceCount": retail_place_count,
                "kkms": kkms,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


async def fetch_retail_place_batch(
    client: httpx.AsyncClient,
    token: str,
    organisation_key: str,
    places: list[dict[str, Any]],
    *,
    concurrency: int,
    retries: int,
    stage: str,
    unique: dict[str, dict[str, Any]],
    base_done: int,
    grand_total: int,
) -> list[dict[str, Any]]:
    semaphore = asyncio.Semaphore(max(1, concurrency))
    failed: list[dict[str, Any]] = []
    completed = 0

    set_state(
        stage=stage,
        done=base_done,
        total=grand_total,
        found=len(unique),
        errors=0,
        message="",
    )

    async def worker(place: dict[str, Any]) -> None:
        nonlocal completed
        retail_place_id = place.get("retailPlaceId")
        if retail_place_id is None:
            completed += 1
            return

        try:
            async with semaphore:
                place_kkms = await get_kkms_by_retail_place(
                    client,
                    token,
                    organisation_key,
                    retail_place_id,
                    retries=retries,
                )

            for kkm in place_kkms:
                reg_id = kkm.get("kkmRegId")
                if not reg_id:
                    continue

                # Сохраняем данные торговой точки отдельно, чтобы не смешивать
                # их с внутренним именем ККТ.
                kkm["_retailPlaceId"] = retail_place_id
                kkm["_retailPlaceTitle"] = place.get("title") or ""
                kkm["_retailPlaceAddress"] = place.get("address") or ""
                unique[str(reg_id)] = kkm
        except Exception as error:
            failed.append(
                {
                    "place": place,
                    "error": safe_error_text(error),
                }
            )
        finally:
            completed += 1
            set_state(
                done=min(grand_total, base_done + completed),
                total=grand_total,
                found=len(unique),
                errors=len(failed),
            )

    await asyncio.gather(*(worker(place) for place in places))
    return failed


async def collect_all_kkms(
    client: httpx.AsyncClient,
    token: str,
    organisation_key: str,
    retail_places: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Получение парка ККТ с автоматическим добором проблемных точек."""
    unique: dict[str, dict[str, Any]] = {}
    total = len(retail_places)

    failed = await fetch_retail_place_batch(
        client,
        token,
        organisation_key,
        retail_places,
        concurrency=KKT_CONCURRENCY,
        retries=3,
        stage="Получение ККТ — быстрый проход",
        unique=unique,
        base_done=0,
        grand_total=total,
    )

    if failed:
        retry_places = [item["place"] for item in failed]
        failed = await fetch_retail_place_batch(
            client,
            token,
            organisation_key,
            retry_places,
            concurrency=8,
            retries=3,
            stage=f"Повтор получения ККТ ({len(retry_places)} точек)",
            unique=unique,
            base_done=max(0, total - len(retry_places)),
            grand_total=total,
        )

    if failed:
        retry_places = [item["place"] for item in failed]
        failed = await fetch_retail_place_batch(
            client,
            token,
            organisation_key,
            retry_places,
            concurrency=2,
            retries=2,
            stage=f"Финальный повтор ККТ ({len(retry_places)} точек)",
            unique=unique,
            base_done=max(0, total - len(retry_places)),
            grand_total=total,
        )

    return list(unique.values()), failed


def parse_api_datetime(value: Any) -> datetime | None:
    if not value:
        return None

    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is not None:
            # Для отбора нам важна последовательность дат внутри одного API.
            # Сохраняем локальное числовое значение без tzinfo, не пересчитывая.
            dt = dt.replace(tzinfo=None)
        return dt
    except ValueError:
        return None


def format_api_datetime(value: Any) -> str:
    """Формат интерфейса: 02.09.2026 09:31, без секунд и миллисекунд."""
    dt = parse_api_datetime(value)
    if dt is None:
        return str(value or "")
    return dt.strftime("%d.%m.%Y %H:%M")


def sortable_api_datetime(value: Any) -> str:
    """ISO без timezone для корректной локальной сортировки и фильтрации."""
    dt = parse_api_datetime(value)
    if dt is None:
        return ""
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def prepare_drives(kkm: dict[str, Any]) -> list[dict[str, Any]]:
    by_number: dict[str, dict[str, Any]] = {}

    for raw in kkm.get("fiscalDrives", []) or []:
        number = raw.get("fsFactoryNumber")
        if number:
            by_number[str(number)] = dict(raw)

    current_fs = kkm.get("fsFactoryNumber")
    if current_fs and str(current_fs) not in by_number:
        by_number[str(current_fs)] = {
            "fsFactoryNumber": str(current_fs),
            "activationDate": None,
            "expireDate": None,
            "closeArchiveDate": None,
        }

    drives = list(by_number.values())
    drives.sort(
        key=lambda drive: (
            parse_api_datetime(drive.get("activationDate")) is None,
            parse_api_datetime(drive.get("activationDate")) or datetime.max,
        )
    )
    return drives


def kkm_matches_query(kkm: dict[str, Any], request: SearchRequest) -> bool:
    """Фильтр каталога ККТ до запросов документов.

    query_terms — быстрые "чипы" верхней панели. Каждый чип ищется сразу
    по РНМ, ЗН ККТ, всем ЗН ФН, внутреннему имени, торговой точке и адресу.
    Несколько чипов объединяются по ИЛИ: если добавлены три РНМ, попадут
    три соответствующие ККТ. Расширенные поля ниже объединяются по И.
    """
    rnm = clean_text(request.rnm)
    kkt_number = clean_text(request.kkt_factory_number)
    fs_number = clean_text(request.fs_number)
    internal_name = clean_text(request.internal_name)
    retail_place = clean_text(request.retail_place)
    address = clean_text(request.address)

    drive_numbers = [
        clean_text(drive.get("fsFactoryNumber"))
        for drive in prepare_drives(kkm)
        if drive.get("fsFactoryNumber")
    ]

    if rnm and not contains(kkm.get("kkmRegId"), rnm):
        return False
    if kkt_number and not contains(kkm.get("kkmFactoryNumber"), kkt_number):
        return False
    if fs_number and not any(fs_number in value for value in drive_numbers):
        return False
    if internal_name and not contains(
        kkm.get("kkmInternalName") or kkm.get("title"), internal_name
    ):
        return False
    if retail_place and not contains(kkm.get("_retailPlaceTitle"), retail_place):
        return False

    address_value = (
        kkm.get("kkmAddress")
        or kkm.get("address")
        or kkm.get("_retailPlaceAddress")
        or ""
    )
    if address and not contains(address_value, address):
        return False

    terms = [clean_text(term) for term in request.query_terms if clean_text(term)]
    if terms:
        searchable = [
            clean_text(kkm.get("kkmRegId")),
            clean_text(kkm.get("kkmFactoryNumber")),
            clean_text(kkm.get("kkmInternalName") or kkm.get("title")),
            clean_text(kkm.get("_retailPlaceTitle")),
            clean_text(address_value),
            *drive_numbers,
        ]
        # Любой из добавленных пользователем чипов может выбрать ККТ.
        if not any(
            term in value
            for term in terms
            for value in searchable
            if value
        ):
            return False

    return True

def build_candidates(
    kkms: list[dict[str, Any]],
    period_start: datetime,
    period_end: datetime,
    fs_filter: str = "",
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    int,
]:
    """
    Возвращает:
      active_pairs       — ФН, которые могли работать в периоде;
      close_pairs        — кандидаты на CLOSE_ARCHIVE;
      activation_pairs   — ФН, активированные в периоде;
      uncertain_count    — пары без достаточных дат, которые мы всё равно
                           включили, чтобы не терять документы молча.
    """
    active_pairs: dict[tuple[str, str], dict[str, Any]] = {}
    close_pairs: dict[tuple[str, str], dict[str, Any]] = {}
    activation_pairs: dict[tuple[str, str], dict[str, Any]] = {}
    uncertain_pairs: set[tuple[str, str]] = set()

    fs_filter = clean_text(fs_filter)

    for kkm in kkms:
        reg_id_raw = kkm.get("kkmRegId")
        if not reg_id_raw:
            continue
        reg_id = str(reg_id_raw)

        drives = prepare_drives(kkm)
        current_fs = str(kkm.get("fsFactoryNumber") or "")

        known_drives = [
            drive
            for drive in drives
            if parse_api_datetime(drive.get("activationDate")) is not None
        ]

        # Известные по дате ФН: строим интервалы работы через activationDate,
        # closeArchiveDate и activationDate следующего ФН.
        for index, drive in enumerate(known_drives):
            fs_number = str(drive.get("fsFactoryNumber") or "")
            if not fs_number:
                continue
            if fs_filter and fs_filter not in clean_text(fs_number):
                continue

            activation = parse_api_datetime(drive.get("activationDate"))
            close_archive = parse_api_datetime(drive.get("closeArchiveDate"))
            if activation is None:
                continue

            next_activation = None
            if index + 1 < len(known_drives):
                next_activation = parse_api_datetime(
                    known_drives[index + 1].get("activationDate")
                )

            end_candidates = [
                value for value in (close_archive, next_activation) if value is not None
            ]
            effective_end = min(end_candidates) if end_candidates else None
            key = (reg_id, fs_number)
            item = {"kkm": kkm, "drive": drive, "fs_number": fs_number}

            overlaps = activation <= period_end and (
                effective_end is None or effective_end >= period_start
            )
            if overlaps:
                active_pairs[key] = item

            if period_start <= activation <= period_end:
                activation_pairs[key] = item

            close_in_period = (
                close_archive is not None
                and period_start <= close_archive <= period_end
            )
            next_activated_in_period = (
                next_activation is not None
                and period_start <= next_activation <= period_end
            )
            if close_in_period or next_activated_in_period:
                close_pairs[key] = item

        # Неизвестные activationDate не выбрасываем молча. Это страховка
        # полноты: такие ФН попадут в проверку, даже если метаданные неполные.
        unknown_drives = [
            drive
            for drive in drives
            if parse_api_datetime(drive.get("activationDate")) is None
        ]

        for drive in unknown_drives:
            fs_number = str(drive.get("fsFactoryNumber") or "")
            if not fs_number:
                continue
            if fs_filter and fs_filter not in clean_text(fs_number):
                continue

            key = (reg_id, fs_number)
            item = {"kkm": kkm, "drive": drive, "fs_number": fs_number}
            close_archive = parse_api_datetime(drive.get("closeArchiveDate"))

            # Текущий ФН точно важен. Исторический ФН без даты — неоднозначен,
            # поэтому тоже проверяем: лучше несколько лишних запросов, чем
            # возможный пропуск документа.
            active_pairs.setdefault(key, item)
            uncertain_pairs.add(key)

            if (
                close_archive is not None
                and period_start <= close_archive <= period_end
            ) or fs_number != current_fs:
                close_pairs.setdefault(key, item)

    return (
        list(active_pairs.values()),
        list(close_pairs.values()),
        list(activation_pairs.values()),
        len(uncertain_pairs),
    )


async def get_documents(
    client: httpx.AsyncClient,
    token: str,
    organisation_key: str,
    kkm_reg_id: str,
    fs_number: str,
    date_from: str,
    date_to: str,
    transaction_types: list[str] | None,
    *,
    irkkt_statuses: list[str] | None = None,
    shift_num: str = "",
    retries: int = 3,
) -> dict[str, Any]:
    params: list[tuple[str, Any]] = [
        ("kkmRegId", kkm_reg_id),
        ("fsFactoryNumber", fs_number),
        ("fromDate", date_from),
        ("toDate", date_to),
    ]

    if shift_num.strip():
        params.append(("shiftNum", shift_num.strip()))

    if transaction_types:
        for value in transaction_types:
            params.append(("transactionTypes", value))

    if irkkt_statuses:
        for value in irkkt_statuses:
            if value in IRKKT_STATUSES:
                params.append(("irkktStatus", value))

    return await request_json(
        client,
        "GET",
        f"{BASE_URL}/api/rent/v2/organisations/{organisation_key}/documents",
        headers=api_headers(token),
        params=params,
        retries=retries,
    )

def canonical_document_key(document: dict[str, Any]) -> str:
    # Вложенные объекты — самый надёжный способ определить тип ответа.
    object_map = [
        ("fiscalReport", "registration"),
        ("fiscalReportCorrection", "reregistration"),
        ("closeArchive", "close"),
        ("ticket", "ticket"),
        ("receiptCorrection", "receipt_correction"),
        ("bsoCorrection", "bso_correction"),
        ("bso", "bso"),
        ("openShift", "open_shift"),
        ("closeShift", "close_shift"),
    ]
    for object_name, key in object_map:
        if document.get(object_name) is not None:
            return key

    raw = clean_text(document.get("transactionType"))
    raw_map = {
        "fiscal_report_correction": "reregistration",
        "close_archive": "close",
        "ticket": "ticket",
        "open_shift": "open_shift",
        "close_shift": "close_shift",
        "receipt_correction": "receipt_correction",
        "bso": "bso",
        "bso_correction": "bso_correction",
        "кассовый чек": "ticket",
        "отчёт об открытии смены": "open_shift",
        "отчет об открытии смены": "open_shift",
        "отчёт о закрытии смены": "close_shift",
        "отчет о закрытии смены": "close_shift",
        "кассовый чек коррекции": "receipt_correction",
        "бланк строгой отчетности": "bso",
        "бланк строгой отчётности": "bso",
        "бланк строгой отчетности коррекции": "bso_correction",
        "отчет об изменениях параметров регистрации": "reregistration",
        "отчёт об изменениях параметров регистрации": "reregistration",
        "закрытие архива": "close",
        "отчет о регистрации": "registration",
        "отчёт о регистрации": "registration",
    }
    return raw_map.get(raw, "unknown")


def classify_document(document: dict[str, Any]) -> str:
    key = canonical_document_key(document)
    if key in DOCUMENT_TYPES:
        return str(DOCUMENT_TYPES[key]["label"])
    return str(document.get("transactionType") or "Неизвестный тип")


def extract_amount(document: dict[str, Any]) -> Any:
    for object_name in ("ticket", "receiptCorrection", "bso", "bsoCorrection"):
        payload = document.get(object_name)
        if isinstance(payload, dict) and payload.get("totalSum") is not None:
            return payload.get("totalSum")
    return None

def normalize_result(
    document: dict[str, Any],
    response_data: dict[str, Any],
    candidate: dict[str, Any],
) -> dict[str, Any]:
    kkm = candidate["kkm"]
    drive = candidate.get("drive") or {}
    fs_number = (
        document.get("fiscalDriveNumber")
        or response_data.get("fsFactoryNumber")
        or candidate.get("fs_number")
    )

    response_meta = {
        key: value for key, value in response_data.items() if key != "documents"
    }

    transaction_date_raw = document.get("transactionDate")
    inserted_at_raw = document.get("insertedAt")
    activation_date_raw = drive.get("activationDate")
    close_archive_date_raw = drive.get("closeArchiveDate")
    expire_date_raw = drive.get("expireDate")

    return {
        "date": format_api_datetime(transaction_date_raw),
        "date_raw": transaction_date_raw,
        "date_sort": sortable_api_datetime(transaction_date_raw),
        "inserted_at": format_api_datetime(inserted_at_raw),
        "inserted_at_raw": inserted_at_raw,
        "inserted_at_sort": sortable_api_datetime(inserted_at_raw),
        "type": classify_document(document),
        "document_key": canonical_document_key(document),
        "raw_type": document.get("transactionType"),
        "amount": extract_amount(document),
        "kkm_internal_name": (
            kkm.get("kkmInternalName") or kkm.get("title") or ""
        ),
        "kkm_reg_id": document.get("kkmRegId") or kkm.get("kkmRegId"),
        "kkm_factory_number": (
            response_data.get("kkmFactoryNumber") or kkm.get("kkmFactoryNumber")
        ),
        "fs_number": fs_number,
        "fd": document.get("fiscalDocumentNumber"),
        "fpd": document.get("fiscalSign"),
        "shift": document.get("shiftNum"),
        "fns_flc_status": document.get("fnsFlcStatus"),
        "fns_status": document.get("fnsStatus"),
        "fns_description": document.get("fnsDescription"),
        "fns_confirmation": document.get("fnsConfirmation"),
        "retail_place": kkm.get("_retailPlaceTitle") or response_data.get("title") or "",
        "retail_place_id": kkm.get("_retailPlaceId"),
        "address": (
            response_data.get("kkmAddress")
            or kkm.get("kkmAddress")
            or kkm.get("address")
            or kkm.get("_retailPlaceAddress")
            or ""
        ),
        "online": kkm.get("online"),
        "activation_date": format_api_datetime(activation_date_raw),
        "activation_date_raw": activation_date_raw,
        "close_archive_date": format_api_datetime(close_archive_date_raw),
        "close_archive_date_raw": close_archive_date_raw,
        "expire_date": format_api_datetime(expire_date_raw),
        "expire_date_raw": expire_date_raw,
        "raw": {
            "document": document,
            "responseMeta": response_meta,
            "kkm": kkm,
            "fiscalDrive": drive,
        },
    }


def add_filtered_task(
    task_map: dict[tuple[str, str], dict[str, Any]],
    candidate: dict[str, Any],
    document_key: str,
    api_type: str,
    date_from: str,
    date_to: str,
    irkkt_statuses: list[str],
    shift_num: str,
) -> None:
    rnm = str(candidate["kkm"].get("kkmRegId") or "")
    fs_number = str(candidate.get("fs_number") or "")
    if not rnm or not fs_number:
        return

    key = (rnm, fs_number)
    task = task_map.setdefault(
        key,
        {
            "kind": "filtered",
            "candidate": candidate,
            "transaction_types": [],
            "wanted_keys": [],
            "date_from": date_from,
            "date_to": date_to,
            "irkkt_statuses": list(irkkt_statuses),
            "shift_num": shift_num,
        },
    )

    if api_type not in task["transaction_types"]:
        task["transaction_types"].append(api_type)
    if document_key not in task["wanted_keys"]:
        task["wanted_keys"].append(document_key)


def build_document_tasks(
    active_candidates: list[dict[str, Any]],
    close_candidates: list[dict[str, Any]],
    activation_candidates: list[dict[str, Any]],
    selected_types: set[str],
    date_from: str,
    date_to: str,
    irkkt_statuses: list[str],
    shift_num: str,
    period_start: datetime,
    period_end: datetime,
) -> list[dict[str, Any]]:
    """Формирует минимальное число запросов.

    Все официально фильтруемые типы для одной пары РНМ+ФН объединяются в
    один GET. Закрытие архива проверяется только у кандидатов на закрытие.
    Первоначальная регистрация запрашивается отдельно, потому что в
    transactionTypes у Universal API для неё нет отдельного значения.
    """
    filtered_map: dict[tuple[str, str], dict[str, Any]] = {}
    tasks: list[dict[str, Any]] = []

    # Все типы, кроме регистрации и закрытия ФН, логично искать только на ФН,
    # который мог быть активен в выбранном периоде.
    for document_key in selected_types:
        if document_key in {"registration", "close"}:
            continue
        config = DOCUMENT_TYPES.get(document_key)
        api_type = str(config.get("api")) if config and config.get("api") else ""
        if not api_type:
            continue
        for candidate in active_candidates:
            add_filtered_task(
                filtered_map,
                candidate,
                document_key,
                api_type,
                date_from,
                date_to,
                irkkt_statuses,
                shift_num,
            )

    if "close" in selected_types:
        for candidate in close_candidates:
            add_filtered_task(
                filtered_map,
                candidate,
                "close",
                "CLOSE_ARCHIVE",
                date_from,
                date_to,
                irkkt_statuses,
                shift_num,
            )

    tasks.extend(filtered_map.values())

    if "registration" in selected_types:
        for candidate in activation_candidates:
            activation = parse_api_datetime(candidate.get("drive", {}).get("activationDate"))
            if activation is None:
                continue

            # activationDate берётся из истории ФН. Чтобы не выгружать тысячи
            # чеков нового ФН, ищем fiscalReport в окне ±60 минут и обрезаем
            # окно границами выбранного пользователем периода.
            scan_from = max(period_start, activation - timedelta(minutes=60))
            scan_to = min(period_end, activation + timedelta(minutes=60))
            if scan_to < scan_from:
                continue

            tasks.append(
                {
                    "kind": "registration",
                    "candidate": candidate,
                    "transaction_types": None,
                    "wanted_keys": ["registration"],
                    "date_from": scan_from.strftime("%Y-%m-%dT%H:%M:%S"),
                    "date_to": scan_to.strftime("%Y-%m-%dT%H:%M:%S"),
                    "irkkt_statuses": list(irkkt_statuses),
                    "shift_num": shift_num,
                }
            )

    return tasks

def task_label(task: dict[str, Any]) -> dict[str, Any]:
    candidate = task["candidate"]
    return {
        "kind": task.get("kind"),
        "rnm": candidate.get("kkm", {}).get("kkmRegId"),
        "fs_number": candidate.get("fs_number"),
        "types": [
            str(DOCUMENT_TYPES.get(key, {}).get("label") or key)
            for key in (task.get("wanted_keys") or [])
        ],
        "date_from": task.get("date_from"),
        "date_to": task.get("date_to"),
        "error": task.get("error", ""),
    }


async def execute_task_batch(
    client: httpx.AsyncClient,
    token: str,
    organisation_key: str,
    tasks: list[dict[str, Any]],
    *,
    concurrency: int,
    retries: int,
    stage: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    semaphore = asyncio.Semaphore(max(1, concurrency))
    results: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    completed = 0
    succeeded = 0
    total = len(tasks)

    set_state(stage=stage, done=0, total=total, found=0, errors=0, message="")

    async def worker(task: dict[str, Any]) -> None:
        nonlocal completed, succeeded
        candidate = task["candidate"]
        kkm = candidate["kkm"]

        try:
            async with semaphore:
                data = await get_documents(
                    client,
                    token,
                    organisation_key,
                    str(kkm.get("kkmRegId")),
                    str(candidate.get("fs_number")),
                    str(task["date_from"]),
                    str(task["date_to"]),
                    task.get("transaction_types"),
                    irkkt_statuses=task.get("irkkt_statuses") or [],
                    shift_num=str(task.get("shift_num") or ""),
                    retries=retries,
                )

            wanted_keys = set(task.get("wanted_keys") or [])
            for document in data.get("documents", []) or []:
                doc_key = canonical_document_key(document)
                if doc_key in wanted_keys:
                    results.append(normalize_result(document, data, candidate))

            succeeded += 1
        except Exception as error:
            failed_task = dict(task)
            failed_task["error"] = safe_error_text(error)
            failed.append(failed_task)
        finally:
            completed += 1
            set_state(
                done=completed,
                total=total,
                found=len(results),
                errors=len(failed),
            )

    await asyncio.gather(*(worker(task) for task in tasks))
    return results, failed, succeeded


async def execute_tasks_with_recovery(
    client: httpx.AsyncClient,
    token: str,
    organisation_key: str,
    tasks: list[dict[str, Any]],
    *,
    primary_concurrency: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    """Быстрый проход -> медленный добор -> финальный добор."""
    all_results: list[dict[str, Any]] = []
    total_succeeded = 0

    rows, failed, succeeded = await execute_task_batch(
        client,
        token,
        organisation_key,
        tasks,
        concurrency=primary_concurrency,
        retries=3,
        stage=f"Запрос документов — до {primary_concurrency} одновременно",
    )
    all_results.extend(rows)
    total_succeeded += succeeded

    if failed:
        rows, failed, succeeded = await execute_task_batch(
            client,
            token,
            organisation_key,
            failed,
            concurrency=min(RETRY_CONCURRENCY, max(1, len(failed))),
            retries=3,
            stage=f"Автоповтор неудачных запросов ({len(failed)})",
        )
        all_results.extend(rows)
        total_succeeded += succeeded

    if failed:
        rows, failed, succeeded = await execute_task_batch(
            client,
            token,
            organisation_key,
            failed,
            concurrency=min(FINAL_RETRY_CONCURRENCY, max(1, len(failed))),
            retries=2,
            stage=f"Финальный медленный повтор ({len(failed)})",
        )
        all_results.extend(rows)
        total_succeeded += succeeded

    return all_results, failed, total_succeeded


def deduplicate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[tuple[Any, Any, Any, Any], dict[str, Any]] = {}
    for row in rows:
        # Тип включаем в ключ: теоретически ФД уникален внутри ФН, но так
        # безопаснее при необычных ответах API.
        key = (
            row.get("kkm_reg_id"),
            row.get("fs_number"),
            row.get("fd"),
            row.get("type"),
        )
        unique[key] = row
    return list(unique.values())


def make_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        label = str(row.get("type") or "Неизвестный тип")
        counts[label] = counts.get(label, 0) + 1
    return counts

def build_payload(
    *,
    organisation_name: str,
    date_from_user: str,
    date_to_user: str,
    used_cache: bool,
    cache_age_minutes: float | None,
    kkm_count: int,
    matched_kkm_count: int,
    active_count: int,
    close_candidate_count: int,
    new_fs_count: int,
    uncertain_count: int,
    registration_unverifiable_count: int,
    rows: list[dict[str, Any]],
    planned_queries: int,
    successful_queries: int,
    failed_tasks: list[dict[str, Any]],
    catalog_failed_places: list[dict[str, Any]],
    elapsed_seconds: float,
    query_filters: dict[str, Any],
) -> dict[str, Any]:
    failed_count = len(failed_tasks)
    completeness = (
        round(successful_queries / planned_queries * 100, 2)
        if planned_queries
        else 100.0
    )
    catalog_complete = len(catalog_failed_places) == 0
    request_complete = failed_count == 0
    registration_coverage_complete = registration_unverifiable_count == 0

    return {
        "organisation": organisation_name,
        "period": {"from": date_from_user, "to": date_to_user},
        "used_cache": used_cache,
        "cache_age_minutes": cache_age_minutes,
        "kkm_count": kkm_count,
        "matched_kkm_count": matched_kkm_count,
        "candidate_count": active_count,
        "close_candidate_count": close_candidate_count,
        "new_fs_count": new_fs_count,
        "uncertain_candidate_count": uncertain_count,
        "registration_unverifiable_count": registration_unverifiable_count,
        "counts": make_counts(rows),
        "elapsed_seconds": round(elapsed_seconds, 1),
        "rows": rows,
        "query_filters": query_filters,
        "completeness": {
            "catalog_complete": catalog_complete,
            "catalog_failed_places": len(catalog_failed_places),
            "planned_queries": planned_queries,
            "successful_queries": successful_queries,
            "failed_queries": failed_count,
            "query_completeness_percent": completeness,
            "request_complete": request_complete,
            "registration_coverage_complete": registration_coverage_complete,
            "all_planned_checks_complete": catalog_complete and request_complete,
            "full_coverage": catalog_complete and request_complete and registration_coverage_complete,
        },
        "failed_queries": [task_label(task) for task in failed_tasks[:200]],
        "catalog_failures": [
            {
                "retailPlaceId": item.get("place", {}).get("retailPlaceId"),
                "title": item.get("place", {}).get("title"),
                "error": item.get("error", ""),
            }
            for item in catalog_failed_places[:200]
        ],
    }


async def perform_search(request: SearchRequest) -> dict[str, Any]:
    started = time.perf_counter()

    try:
        start_day = datetime.strptime(request.date_from, "%Y-%m-%d")
        end_day = datetime.strptime(request.date_to, "%Y-%m-%d")
        start_time = datetime.strptime(request.time_from or "00:00", "%H:%M").time()
        end_time = datetime.strptime(request.time_to or "23:59", "%H:%M").time()
    except ValueError as error:
        raise ValueError("Проверь формат даты и времени") from error

    period_start = datetime.combine(start_day.date(), start_time)
    period_end = datetime.combine(end_day.date(), end_time)

    if period_end < period_start:
        raise ValueError("Конец периода не может быть раньше начала")
    if period_end - period_start > timedelta(days=30):
        raise ValueError("Universal API позволяет запрашивать максимум 30 дней за раз")

    selected_types = set(request.document_types) & set(DOCUMENT_TYPES)
    if not selected_types:
        raise ValueError("Выбери хотя бы один тип фискального документа")

    statuses = [value for value in request.irkkt_statuses if value in IRKKT_STATUSES]

    shift_num = request.shift_num.strip()
    if shift_num and not shift_num.isdigit():
        raise ValueError("Номер смены должен содержать только цифры")

    date_from = period_start.strftime("%Y-%m-%dT%H:%M:%S")
    date_to = period_end.strftime("%Y-%m-%dT%H:%M:%S")

    # При высокой параллельности нужен запас соединений, иначе задачи будут
    # простаивать внутри локального пула, а не реально выполняться параллельно.
    max_connections = max(240, request.concurrency + 70)
    max_keepalive = max(180, request.concurrency + 35)
    limits = httpx.Limits(
        max_connections=max_connections,
        max_keepalive_connections=max_keepalive,
        keepalive_expiry=60,
    )
    timeout = httpx.Timeout(connect=15, read=70, write=30, pool=60)

    async with create_http_client(limits=limits, timeout=timeout, http2=False) as client:
        set_state(stage="Авторизация", done=0, total=1, found=0, errors=0)
        token = await get_token(client)

        set_state(stage="Организация", done=0, total=1, found=0, errors=0)
        organisations = await get_organisations(client, token)
        if not organisations:
            raise RuntimeError("Организации не найдены")

        # Для текущего ключа фактически одна организация. Если ключ начнёт
        # возвращать несколько, добавим явный выбор в UI.
        organisation = organisations[0]
        organisation_key = str(organisation["organisationKey"])
        organisation_name = str(organisation.get("organizationName") or "")

        kkms: list[dict[str, Any]] | None = None
        cache_age: float | None = None
        used_cache = False
        catalog_failed_places: list[dict[str, Any]] = []

        if not request.refresh_kkt:
            kkms, cache_age = load_cache(organisation_key)
            used_cache = kkms is not None

        if kkms is None:
            set_state(stage="Получение торговых точек", done=0, total=1, found=0, errors=0)
            retail_places = await get_retail_places(client, token, organisation_key)
            kkms, catalog_failed_places = await collect_all_kkms(
                client, token, organisation_key, retail_places
            )

            # Неполный каталог не кэшируем: иначе следующий запуск мог бы
            # выглядеть успешным, но молча не содержать часть ККТ.
            if not catalog_failed_places:
                save_cache(organisation_key, kkms, len(retail_places))
                cache_age = 0.0
            else:
                cache_age = None

        all_kkm_count = len(kkms)

        set_state(stage="Применение условий запроса", done=0, total=1, found=0, errors=0)
        filtered_kkms = [kkm for kkm in kkms if kkm_matches_query(kkm, request)]

        active, close_candidates, activation_candidates, uncertain_count = build_candidates(
            filtered_kkms,
            period_start,
            period_end,
            request.fs_number,
        )

        tasks = build_document_tasks(
            active,
            close_candidates,
            activation_candidates,
            selected_types,
            date_from,
            date_to,
            statuses,
            shift_num,
            period_start,
            period_end,
        )

        planned_queries = len(tasks)
        rows: list[dict[str, Any]] = []
        failed_tasks: list[dict[str, Any]] = []
        successful_queries = 0

        if tasks:
            rows, failed_tasks, successful_queries = await execute_tasks_with_recovery(
                client,
                token,
                organisation_key,
                tasks,
                primary_concurrency=request.concurrency,
            )

    rows = deduplicate(rows)
    rows.sort(key=lambda item: str(item.get("date_sort") or ""), reverse=True)

    elapsed = time.perf_counter() - started
    query_filters = {
        "document_types": sorted(selected_types),
        "query_terms": [term for term in request.query_terms if clean_text(term)],
        "date_from": request.date_from,
        "date_to": request.date_to,
        "time_from": request.time_from,
        "time_to": request.time_to,
        "rnm": request.rnm,
        "kkt_factory_number": request.kkt_factory_number,
        "fs_number": request.fs_number,
        "internal_name": request.internal_name,
        "retail_place": request.retail_place,
        "address": request.address,
        "shift_num": shift_num,
        "irkkt_statuses": statuses,
        "concurrency": request.concurrency,
    }

    payload = build_payload(
        organisation_name=organisation_name,
        date_from_user=request.date_from,
        date_to_user=request.date_to,
        used_cache=used_cache,
        cache_age_minutes=cache_age,
        kkm_count=all_kkm_count,
        matched_kkm_count=len(filtered_kkms),
        active_count=len(active),
        close_candidate_count=len(close_candidates),
        new_fs_count=len(activation_candidates),
        uncertain_count=uncertain_count,
        registration_unverifiable_count=(uncertain_count if "registration" in selected_types else 0),
        rows=rows,
        planned_queries=planned_queries,
        successful_queries=successful_queries,
        failed_tasks=failed_tasks,
        catalog_failed_places=catalog_failed_places,
        elapsed_seconds=elapsed,
        query_filters=query_filters,
    )

    LAST_SEARCH["rows"] = rows
    LAST_SEARCH["failed_tasks"] = failed_tasks
    LAST_SEARCH["meta"] = {
        "organisation_key": organisation_key,
        "organisation_name": organisation_name,
        "request": request.model_dump(),
        "payload_base": {
            "used_cache": used_cache,
            "cache_age_minutes": cache_age,
            "kkm_count": all_kkm_count,
            "matched_kkm_count": len(filtered_kkms),
            "active_count": len(active),
            "close_candidate_count": len(close_candidates),
            "new_fs_count": len(activation_candidates),
            "uncertain_count": uncertain_count,
            "registration_unverifiable_count": (uncertain_count if "registration" in selected_types else 0),
            "planned_queries": planned_queries,
            "successful_queries": successful_queries,
            "catalog_failed_places": catalog_failed_places,
            "query_filters": query_filters,
            "initial_elapsed": elapsed,
        },
    }

    set_state(
        stage="Готово",
        done=successful_queries,
        total=planned_queries,
        found=len(rows),
        errors=len(failed_tasks),
        message=(
            "Все запланированные проверки выполнены"
            if payload["completeness"]["all_planned_checks_complete"]
            else "Есть непроверенные запросы"
        ),
    )

    return payload

async def retry_last_failed() -> dict[str, Any]:
    failed_tasks: list[dict[str, Any]] = LAST_SEARCH.get("failed_tasks") or []
    meta: dict[str, Any] = LAST_SEARCH.get("meta") or {}

    if not meta:
        raise ValueError("Сначала выполни обычный запрос")
    if not failed_tasks:
        raise ValueError("Неудачных запросов для повторения нет")

    organisation_key = str(meta["organisation_key"])
    started = time.perf_counter()

    limits = httpx.Limits(
        max_connections=40,
        max_keepalive_connections=30,
        keepalive_expiry=45,
    )
    timeout = httpx.Timeout(connect=20, read=90, write=30, pool=45)

    async with create_http_client(limits=limits, timeout=timeout) as client:
        token = await get_token(client)
        rows, still_failed, succeeded = await execute_task_batch(
            client,
            token,
            organisation_key,
            failed_tasks,
            concurrency=min(10, len(failed_tasks)),
            retries=4,
            stage=f"Ручной повтор ({len(failed_tasks)} запросов)",
        )

    combined_rows = deduplicate((LAST_SEARCH.get("rows") or []) + rows)
    combined_rows.sort(key=lambda item: str(item.get("date_sort") or ""), reverse=True)

    base = meta["payload_base"]
    successful_queries = int(base["successful_queries"]) + succeeded
    base["successful_queries"] = successful_queries
    LAST_SEARCH["rows"] = combined_rows
    LAST_SEARCH["failed_tasks"] = still_failed

    elapsed = float(base.get("initial_elapsed", 0)) + (time.perf_counter() - started)

    payload = build_payload(
        organisation_name=str(meta["organisation_name"]),
        date_from_user=str(meta["request"]["date_from"]),
        date_to_user=str(meta["request"]["date_to"]),
        used_cache=bool(base["used_cache"]),
        cache_age_minutes=base["cache_age_minutes"],
        kkm_count=int(base["kkm_count"]),
        matched_kkm_count=int(base["matched_kkm_count"]),
        active_count=int(base["active_count"]),
        close_candidate_count=int(base["close_candidate_count"]),
        new_fs_count=int(base["new_fs_count"]),
        uncertain_count=int(base["uncertain_count"]),
        registration_unverifiable_count=int(base.get("registration_unverifiable_count", 0)),
        rows=combined_rows,
        planned_queries=int(base["planned_queries"]),
        successful_queries=successful_queries,
        failed_tasks=still_failed,
        catalog_failed_places=base["catalog_failed_places"],
        elapsed_seconds=elapsed,
        query_filters=base["query_filters"],
    )

    set_state(
        stage="Готово",
        done=successful_queries,
        total=int(base["planned_queries"]),
        found=len(combined_rows),
        errors=len(still_failed),
        message=(
            "Все запланированные проверки выполнены"
            if payload["completeness"]["all_planned_checks_complete"]
            else "Часть запросов всё ещё не выполнена"
        ),
    )

    return payload


@app.get("/api/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "version": VERSION}


@app.get("/api/keys")
async def api_keys() -> dict[str, Any]:
    return list_profiles()


@app.post("/api/keys/add")
async def api_keys_add(request: ApiKeyAddRequest) -> dict[str, Any]:
    api_key = request.api_key.strip()
    if not api_key:
        raise HTTPException(status_code=400, detail="API-ключ не введён")

    limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)
    timeout = httpx.Timeout(connect=15, read=35, write=20, pool=20)

    # Валидность ключа определяется успешной авторизацией. Получение названия
    # организации — полезный, но НЕ обязательный второй запрос: его временный
    # сбой не должен заставлять пользователя повторно вводить рабочий ключ.
    try:
        async with create_http_client(limits=limits, timeout=timeout) as client:
            token = await authenticate_api_key(client, api_key)
            organisations: list[dict[str, Any]] = []
            try:
                organisations = await get_organisations(client, token)
            except Exception as org_error:
                LOGGER.warning("API-ключ авторизован, но организация не прочитана: %s", safe_error_text(org_error))
    except httpx.HTTPStatusError as error:
        status = error.response.status_code
        LOGGER.warning("1-ОФД отклонил API-ключ: HTTP %s", status)
        if status in (400, 401, 403):
            detail = "1-ОФД не принял API-ключ. Проверь, что ключ скопирован полностью и относится к Universal API."
        else:
            detail = f"1-ОФД вернул HTTP {status} при проверке API-ключа. Попробуй ещё раз."
        raise HTTPException(status_code=400, detail=detail) from error
    except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as error:
        LOGGER.warning("Сетевая ошибка проверки API-ключа: %s", safe_error_text(error))
        raise HTTPException(
            status_code=400,
            detail="Не удалось подключиться к Universal API 1-ОФД. Проверь интернет и повтори попытку.",
        ) from error
    except Exception as error:
        LOGGER.exception("Неожиданная ошибка проверки API-ключа")
        raise HTTPException(
            status_code=400,
            detail="Не удалось проверить API-ключ. Подробности записаны в журнал приложения.",
        ) from error

    org_name = str((organisations[0] if organisations else {}).get("organizationName") or "API-ключ 1-ОФД")
    profile = add_or_select_profile(api_key, org_name)
    TOKEN_CACHE.pop(str(profile["id"]), None)
    return {"profile": profile, **list_profiles()}


@app.post("/api/keys/select")
async def api_keys_select(request: ApiKeySelectRequest) -> dict[str, Any]:
    try:
        profile = select_profile(request.profile_id)
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    return {"profile": profile, **list_profiles()}


@app.get("/api/keys/{profile_id}/reveal")
async def api_keys_reveal(profile_id: str) -> dict[str, str]:
    try:
        api_key = get_profile_secret(profile_id)
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    # Endpoint доступен только локальному UI приложения. Значение не логировать.
    return {"api_key": api_key}


@app.delete("/api/keys/{profile_id}")
async def api_keys_delete(profile_id: str) -> dict[str, Any]:
    delete_profile(profile_id)
    return list_profiles()


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return HTML


@app.get("/api/status")
async def status() -> dict[str, Any]:
    return STATE


@app.post("/api/search")
async def search(request: SearchRequest) -> dict[str, Any]:
    global ACTIVE_SEARCH_TASK
    if SEARCH_LOCK.locked():
        raise HTTPException(status_code=409, detail="Поиск уже выполняется")

    async with SEARCH_LOCK:
        ACTIVE_SEARCH_TASK = asyncio.current_task()
        set_state(running=True, stage="Подготовка", done=0, total=0, found=0, errors=0, message="", cancelled=False)
        try:
            return await perform_search(request)
        except asyncio.CancelledError:
            set_state(running=False, stage="Остановлено", message="Запрос остановлен пользователем", cancelled=True)
            return {"cancelled": True, "message": "Запрос остановлен пользователем"}
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        except httpx.HTTPStatusError as error:
            detail = error.response.text[:1000] if error.response is not None else str(error)
            raise HTTPException(status_code=502, detail=f"Ошибка Первого ОФД: {detail}") from error
        except Exception as error:
            raise HTTPException(status_code=500, detail=str(error)) from error
        finally:
            STATE["running"] = False
            ACTIVE_SEARCH_TASK = None


@app.post("/api/stop-search")
async def stop_search() -> dict[str, Any]:
    task = ACTIVE_SEARCH_TASK
    if task is None or task.done():
        return {"stopping": False, "message": "Активного запроса нет"}
    set_state(stage="Остановка", message="Останавливаю запрос…", cancelled=True)
    task.cancel("user_stop")
    return {"stopping": True, "message": "Остановка запроса запрошена"}


@app.post("/api/retry-failed")
async def retry_failed() -> dict[str, Any]:
    global ACTIVE_SEARCH_TASK
    if SEARCH_LOCK.locked():
        raise HTTPException(status_code=409, detail="Другой запрос уже выполняется")

    async with SEARCH_LOCK:
        ACTIVE_SEARCH_TASK = asyncio.current_task()
        set_state(running=True, stage="Повтор", done=0, total=0, found=0, errors=0, message="", cancelled=False)
        try:
            return await retry_last_failed()
        except asyncio.CancelledError:
            set_state(running=False, stage="Остановлено", message="Повторный запрос остановлен пользователем", cancelled=True)
            return {"cancelled": True, "message": "Повторный запрос остановлен пользователем"}
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        except Exception as error:
            raise HTTPException(status_code=500, detail=str(error)) from error
        finally:
            STATE["running"] = False
            ACTIVE_SEARCH_TASK = None


HTML = r"""
<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Первый ОФД — фискальные документы</title>
<style>

:root{
  --bg:#eef4f8;
  --bg-soft:#f6fafc;
  --panel:#f8fbfd;
  --card:#ffffff;
  --surface:#e8f1f8;
  --surface-2:#deebf5;
  --surface-3:#d1e4f2;
  --text:#16324a;
  --muted:#6e8598;
  --blue-deep:#03045e;
  --blue:#0077b6;
  --blue-2:#00b4d8;
  --blue-soft:#90e0ef;
  --blue-pale:#caf0f8;
  --green:#2a8b63;
  --green-soft:#e9f7f0;
  --amber:#9f6b09;
  --amber-soft:#fff5df;
  --red:#ba5454;
  --red-soft:#fff1f1;
  --shadow:0 12px 30px rgba(22,50,74,.09);
  --shadow-soft:0 8px 22px rgba(22,50,74,.07);
  --radius-xl:44px;
  --radius-lg:30px;
  --radius-md:24px;
  --radius-sm:18px;
}
*{box-sizing:border-box}
html,body{min-height:100%}
body{
  margin:0;
  font-family:'Segoe UI',Arial,sans-serif;
  background:var(--bg);
  color:var(--text);
  font-size:14px;
  line-height:1.35;
}
.topbar{
  height:62px;
  background:rgba(248,251,253,.92);
  backdrop-filter:blur(8px);
  display:flex;
  align-items:center;
  padding:0 26px;
  gap:18px;
  position:sticky;
  top:0;
  z-index:30;
  box-shadow:0 3px 16px rgba(22,50,74,.05);
}
.brand{font-size:18px;font-weight:800;letter-spacing:.2px}
.brand span{color:var(--blue)}
.topnote{color:var(--muted);font-size:12px}
.page{padding:20px;max-width:1920px;margin:auto}
.panel,.results,.stat,.statusbar,.completeness,.modal-box{
  background:var(--card);
  border-radius:var(--radius-xl);
  box-shadow:var(--shadow);
}
.panel{
  margin-bottom:16px;
  overflow:visible;
  background:linear-gradient(180deg,var(--card) 0%, var(--panel) 100%);
  border-radius:48px;
}
.results,.statusbar,.completeness,.modal-box{border-radius:40px}
.panel-main{
  display:grid;
  grid-template-columns:minmax(240px,1.05fr) 180px 180px minmax(420px,1.8fr) 72px minmax(180px,.85fr);
  gap:16px;
  align-items:end;
  padding:16px 20px 18px;
}
.panel-name{
  display:flex;
  flex-direction:column;
  justify-content:center;
  align-self:center;
  gap:4px;
  min-height:64px;
  padding:0 6px 0 2px;
}
.panel-name b{display:block;font-size:19px;line-height:1.15}
.panel-name small{display:none}
.field{display:flex;flex-direction:column;justify-content:flex-start;gap:7px;min-width:0;position:relative}
.field label,.label-row label{display:block;color:var(--muted);font-size:12px;margin:0 0 0 6px;font-weight:700;line-height:1.2;min-height:22px;display:flex;align-items:flex-end}
.label-row{display:flex;align-items:flex-end;gap:8px;min-height:22px;padding-right:4px}
.action-field label{visibility:hidden}
.action-field .btn{width:100%}
.input,.select,.tokenbox,.btn,.detail,.stat,.type-pill,.failure-row,.page-size select{
  border:0;
  outline:none;
  border-radius:var(--radius-md);
  box-shadow:var(--shadow-soft);
}
.input,.select{
  width:100%;
  height:50px;
  background:var(--surface);
  color:var(--text);
  padding:0 16px;
  font:inherit;
}
.select{
  appearance:none;
  -webkit-appearance:none;
  -moz-appearance:none;
  padding-right:48px;
  background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 14 14'%3E%3Cpath d='M3.5 5.25 7 8.75l3.5-3.5' fill='none' stroke='%236b8397' stroke-width='1.8' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E");
  background-repeat:no-repeat;
  background-position:right 16px center;
  background-size:14px 14px;
}
.input::placeholder{color:#8ea3b4}
.input:focus,.select:focus,.tokenbox:focus-within{
  background:var(--surface-2);
  box-shadow:0 0 0 3px rgba(0,119,182,.12), var(--shadow-soft);
}
.date-row{display:grid;grid-template-columns:1fr 104px;gap:8px}
.btn{
  height:50px;
  background:var(--surface);
  color:var(--blue-deep);
  padding:0 18px;
  cursor:pointer;
  font-weight:700;
  white-space:nowrap;
  transition:transform .12s ease,filter .12s ease,background .12s ease;
}
.btn:hover{filter:brightness(.985);transform:translateY(-1px)}
.btn:active{transform:translateY(0)}
.btn.primary{background:var(--blue);color:#fff}
.btn.warn{background:var(--amber-soft);color:var(--amber)}
.btn.stop-search{background:var(--red-soft);color:var(--red);min-width:88px;box-shadow:none}
.btn.stop-search:hover{background:#ffe4e4}
.btn.icon{width:72px;padding:0;font-size:20px;background:var(--surface-2);color:var(--blue-deep)}
.btn.mini{height:36px;padding:0 14px;font-size:12px;font-weight:700;background:var(--surface-2)}
.btn:disabled{opacity:.58;cursor:not-allowed;transform:none}
.gear.open{background:var(--blue);color:#fff}
.tokenbox{
  min-height:50px;
  background:var(--surface);
  display:flex;
  align-items:center;
  gap:8px;
  padding:7px 9px 7px 12px;
  border-radius:28px;
}
.token-list{display:flex;gap:7px;flex-wrap:wrap;max-width:56%}
.token{
  display:inline-flex;
  align-items:center;
  gap:7px;
  background:var(--blue-pale);
  color:var(--blue-deep);
  border-radius:999px;
  padding:6px 7px 6px 12px;
  font-size:12px;
  max-width:220px;
  box-shadow:0 4px 12px rgba(0,119,182,.08);
}
.token span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.token button{
  border:0;
  background:rgba(3,4,94,.08);
  color:var(--blue-deep);
  width:22px;
  height:22px;
  border-radius:999px;
  padding:0;
  cursor:pointer;
  font-size:14px;
}
.token-input{
  border:0;
  outline:0;
  min-width:180px;
  flex:1;
  height:30px;
  font:inherit;
  background:transparent;
  color:var(--text);
}
.token-add{
  height:38px;
  min-width:38px;
  border:0;
  background:var(--blue);
  color:#fff;
  cursor:pointer;
  font-size:18px;
  border-radius:999px;
  box-shadow:0 6px 14px rgba(0,119,182,.18);
}
.inline-help{
  position:relative;
  z-index:12;
  border:2px solid rgba(0,119,182,.45);
  background:#fff;
  color:var(--blue);
  width:28px;
  height:28px;
  min-width:28px;
  border-radius:999px;
  display:inline-flex;
  align-items:center;
  justify-content:center;
  font-size:14px;
  font-weight:800;
  cursor:help;
  box-shadow:0 5px 14px rgba(0,119,182,.12);
}
.inline-help:hover,.inline-help:focus-visible{background:#f7fbff;border-color:var(--blue)}
.inline-help[data-tip]::after{
  content:attr(data-tip);
  position:absolute;
  right:-2px;
  top:calc(100% + 10px);
  width:min(360px,42vw);
  background:#16324a;
  color:#fff;
  padding:12px 14px;
  border-radius:18px;
  font-size:13px;
  line-height:1.4;
  font-weight:600;
  text-align:left;
  box-shadow:0 16px 28px rgba(22,50,74,.24);
  opacity:0;
  transform:translateY(6px);
  transition:opacity .08s ease, transform .08s ease;
  pointer-events:none;
  white-space:normal;
  z-index:9999;
}
.inline-help[data-tip]:hover::after,.inline-help[data-tip]:focus-visible::after{
  opacity:1;
  transform:translateY(0);
}
.inline-help.inside{margin-right:2px;flex:0 0 auto}
.control-with-help{position:relative}
.control-with-help .select{padding-right:86px}
.control-with-help .inline-help{
  position:absolute;
  right:42px;
  top:50%;
  transform:translateY(-50%);
  margin:0;
}
.advanced{
  display:block;
  max-height:0;
  opacity:0;
  overflow:hidden;
  background:linear-gradient(180deg,var(--panel) 0%, #f2f8fb 100%);
  padding:0 20px;
  transition:max-height .32s ease, opacity .22s ease, padding-bottom .22s ease;
}
.advanced.open{max-height:2200px;opacity:1;padding-bottom:20px}
.advanced-grid{
  display:grid;
  grid-template-columns:repeat(4,minmax(180px,1fr));
  gap:16px;
  padding-top:10px;
  align-items:start;
}
.advanced-block{
  margin-top:18px;
  padding:20px;
  background:rgba(237,245,251,.78);
  border-radius:var(--radius-lg);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.5);
}
.advanced-title{font-size:13px;font-weight:800;margin-bottom:12px;color:var(--blue-deep)}
.checks{display:flex;flex-wrap:wrap;gap:10px 12px;align-items:center}
.check{
  display:inline-flex;
  align-items:center;
  gap:8px;
  padding:10px 14px;
  border-radius:999px;
  background:rgba(255,255,255,.72);
  box-shadow:var(--shadow-soft);
}
.check input{margin:0;accent-color:var(--blue)}
.preset-row{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:10px}
.heavy-note{margin-top:10px;color:var(--amber);font-size:11px}
.advanced-footer{
  display:flex;
  justify-content:space-between;
  align-items:center;
  margin-top:14px;
  gap:12px;
  flex-wrap:wrap;
  padding:16px 18px 0;
}
.active-filters{
  font-size:12px;
  color:var(--muted);
  min-height:18px;
  padding:10px 14px;
  background:rgba(255,255,255,.72);
  border-radius:999px;
  box-shadow:var(--shadow-soft);
}
.statusbar{
  display:none;
  padding:16px 20px;
  align-items:center;
  gap:14px;
  margin-bottom:12px;
}
.statusbar.show{display:flex}
.spinner{width:18px;height:18px;border:3px solid rgba(0,119,182,.2);border-top-color:var(--blue);border-radius:50%;animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.progress{height:10px;background:var(--surface);border-radius:999px;overflow:hidden;flex:1;box-shadow:inset 0 1px 2px rgba(22,50,74,.05)}
.progress>div{height:100%;background:linear-gradient(90deg,var(--blue-2),var(--blue));width:0;border-radius:999px}
.status-text{min-width:380px;font-weight:600}
.completeness{
  display:none;
  padding:16px 20px;
  align-items:center;
  justify-content:space-between;
  gap:14px;
  margin-bottom:12px;
}
.completeness.show{display:flex}
.completeness.ok{background:var(--green-soft);color:var(--green)}
.completeness.warn{background:var(--amber-soft);color:var(--amber)}
.completeness.bad{background:var(--red-soft);color:var(--red)}
.complete-main b{display:block;margin-bottom:3px;font-size:15px}
.complete-sub{font-size:12px;opacity:.9}
.summary{display:grid;grid-template-columns:repeat(8,minmax(105px,1fr));gap:12px;margin-bottom:12px}
.stat{
  padding:16px 14px 14px;
  background:linear-gradient(180deg,#ffffff 0%, #f3f9fd 100%);
  text-align:center;
}
.stat b{display:block;font-size:20px;margin-top:5px;color:var(--blue-deep)}
.stat small{color:var(--muted);font-weight:600}
.type-summary{display:flex;gap:8px;flex-wrap:wrap;margin:0 0 12px}
.type-pill{font-size:12px;background:var(--card);border-radius:999px;padding:8px 12px;color:#53606c}
.results{overflow:hidden}
.table-meta{
  padding:12px 16px;
  color:var(--muted);
  display:flex;
  justify-content:space-between;
  gap:12px;
  align-items:center;
  background:linear-gradient(180deg,#ffffff 0%, #f5fafc 100%);
}
.table-wrap{overflow:auto;max-height:64vh}
table{border-collapse:separate;border-spacing:0 8px;width:100%;min-width:1850px;padding:0 10px 8px}
th,td{padding:12px 10px;text-align:left;white-space:nowrap}
th{
  background:transparent;
  position:sticky;
  top:0;
  z-index:2;
  font-weight:800;
  color:#476179;
  cursor:pointer;
  user-select:none;
}
th:hover{color:var(--blue-deep)}
tbody tr{cursor:pointer;transition:transform .1s ease,filter .1s ease}
tbody tr td{background:#f7fbfd}
tbody tr td:first-child{border-top-left-radius:22px;border-bottom-left-radius:22px}
tbody tr td:last-child{border-top-right-radius:22px;border-bottom-right-radius:22px}
tbody tr:hover{transform:translateY(-1px)}
tbody tr:hover td{background:#edf7fd}
.muted{color:var(--muted)}
.type{font-weight:800}
.type.reregistration{color:#9f6b09}
.type.close{color:#ba5454}
.type.registration{color:#2a8b63}
.status-ok{color:var(--green);font-weight:700}
.status-error{color:var(--red);font-weight:700}
.empty{padding:58px 20px;text-align:center;color:var(--muted)}
.pager{display:flex;align-items:center;justify-content:center;gap:10px;padding:12px 16px 16px;flex-wrap:wrap}
.pager button{border:0;background:var(--surface);color:var(--blue-deep);cursor:pointer;padding:10px 14px;border-radius:999px;box-shadow:var(--shadow-soft);font-weight:700}
.pager button:disabled{color:#b5c4d0;cursor:default;box-shadow:none;background:#eef3f6}
.page-size{margin-left:auto;display:flex;align-items:center;gap:8px;color:var(--muted);font-weight:600}
.page-size select{height:40px;background:var(--surface);padding:0 40px 0 12px;background-position:right 14px center}
.modal{display:none;position:fixed;inset:0;background:rgba(12,19,29,.36);z-index:100;align-items:center;justify-content:center;padding:20px}
.modal.show{display:flex}
.modal-box{width:min(1120px,96vw);max-height:90vh;overflow:auto}
.modal-title{
  padding:18px 20px;
  display:flex;
  align-items:center;
  justify-content:space-between;
  font-size:18px;
  font-weight:800;
  color:var(--blue-deep);
}
.close-x{border:0;background:var(--surface);width:44px;height:44px;border-radius:999px;font-size:22px;cursor:pointer;color:var(--muted);box-shadow:var(--shadow-soft)}
.modal-body{padding:0 20px 20px}
.details-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:13px}
.detail{position:relative;padding:12px 48px 12px 13px;overflow:hidden;background:var(--surface)}
.detail small{display:block;color:var(--muted);margin-bottom:4px;font-weight:600}
.detail-value{overflow-wrap:anywhere;word-break:break-word}
.detail-copy{position:absolute;right:10px;top:50%;transform:translateY(-50%);width:30px;height:30px;border:0;border-radius:999px;background:var(--surface-2);color:var(--blue);cursor:pointer;font-size:17px;font-weight:800;display:flex;align-items:center;justify-content:center}
.detail-copy:hover{background:var(--blue-pale);color:var(--blue-deep)}
.raw-title{display:flex;align-items:center;justify-content:space-between;margin:16px 0 8px;font-weight:800;color:var(--blue-deep)}
pre{margin:0;background:#112031;color:#d8e2ec;border-radius:22px;padding:16px;overflow:auto;max-height:420px;font-family:Consolas,monospace;font-size:12px;box-shadow:var(--shadow-soft)}
.failure-list{max-height:440px;overflow:auto;background:var(--bg-soft);border-radius:var(--radius-lg);padding:10px}
.failure-row{padding:11px 12px;background:#fff;margin-bottom:8px}
.failure-row:last-child{margin-bottom:0}
.failure-row code{font-family:Consolas,monospace;font-size:12px}
#failedDetailsBtn,#catalogRetryBtn,#retryBtn{margin-left:8px}

.topbar-actions{margin-left:auto;display:flex;align-items:center;gap:10px}
.version-chip{font-size:11px;color:var(--muted);background:var(--surface);padding:7px 10px;border-radius:999px;font-weight:700}
.api-key-btn{border:0;background:var(--surface-2);color:var(--blue-deep);border-radius:999px;height:38px;padding:0 14px;font-weight:800;cursor:pointer;box-shadow:var(--shadow-soft);max-width:360px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.api-key-btn.active{background:var(--blue);color:#fff}
.key-list{display:flex;flex-direction:column;gap:10px;margin:10px 0 16px}
.key-row{display:flex;align-items:center;gap:12px;background:var(--surface);padding:12px 14px;border-radius:22px}
.key-main{flex:1;min-width:0}.key-main b{display:block;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.key-main small{color:var(--muted)}
.key-secret-line{display:flex;align-items:center;gap:7px;margin-top:3px;min-height:24px}
.key-secret-value{font-family:Consolas,monospace;font-size:12px;color:var(--muted);word-break:break-all}
.key-reveal{width:28px;height:28px;min-width:28px;border:0;border-radius:999px;background:var(--surface-2);color:var(--blue);cursor:pointer;display:flex;align-items:center;justify-content:center}
.key-reveal:hover{background:var(--blue-pale)}
.key-reveal svg{width:16px;height:16px;fill:none;stroke:currentColor;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}
.key-actions{display:flex;gap:8px;align-items:center}
.key-empty{padding:16px;background:var(--surface);border-radius:22px;color:var(--muted);text-align:center}
.key-add-grid{display:grid;grid-template-columns:1fr auto;gap:10px;align-items:end}
.key-note{margin-top:10px;color:var(--muted);font-size:12px}
.key-error{display:none;margin-top:10px;background:var(--red-soft);color:var(--red);padding:10px 12px;border-radius:18px;font-weight:600}.key-error.show{display:block}
.secret-wrap{position:relative}
.secret-wrap .input{padding-right:52px}
.secret-toggle{position:absolute;right:8px;top:50%;transform:translateY(-50%);width:36px;height:36px;border:0;border-radius:999px;background:var(--surface-2);color:var(--blue);cursor:pointer;display:flex;align-items:center;justify-content:center;box-shadow:var(--shadow-soft)}
.secret-toggle:hover{background:var(--blue-pale)}
.secret-toggle svg{width:19px;height:19px;fill:none;stroke:currentColor;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}
.secret-toggle .eye-off{display:none}
.secret-toggle.visible .eye-on{display:none}
.secret-toggle.visible .eye-off{display:block}

@media(max-width:1450px){
  .panel-main{grid-template-columns:1fr 170px 170px minmax(320px,1.35fr) 72px minmax(170px,.8fr)}
  .advanced-grid{grid-template-columns:repeat(3,1fr)}
  .summary{grid-template-columns:repeat(4,1fr)}
}
@media(max-width:1040px){
  .page{padding:12px}
  .panel-main{grid-template-columns:1fr 1fr}
  .panel-name,.token-field,.main-action,.action-field{grid-column:auto}
  .token-field,.panel-name{grid-column:1/-1}
  .advanced-grid,.details-grid{grid-template-columns:1fr 1fr}
  .summary{grid-template-columns:repeat(2,1fr)}
  .status-text{min-width:0}
  .token-list{max-width:100%}
  .completeness,.statusbar,.advanced-footer,.table-meta{flex-direction:column;align-items:flex-start}
  .page-size{margin-left:0}
}
@media(max-width:640px){
  .topbar{padding:0 14px;height:auto;min-height:62px;flex-wrap:wrap;padding-top:10px;padding-bottom:10px}
  .advanced-grid,.details-grid{grid-template-columns:1fr}
  .summary{grid-template-columns:1fr}
}

</style>
</head>
<body>
<div class="topbar"><div class="brand">Первый ОФД <span>• Фискальные документы</span></div><div class="topnote">локальное веб-приложение • Universal API</div><div class="topbar-actions"><span class="version-chip">v1.7.2</span><button id="apiKeyBtn" class="api-key-btn">API-ключ</button></div></div>
<div class="page">

  <section class="panel" id="apiPanel">
    <div class="panel-main">
      <div class="panel-name"><b>Настройки запроса по API</b></div>
      <div class="field"><label>Период с</label><input id="dateFrom" type="date" class="input"></div>
      <div class="field"><label>Период по</label><input id="dateTo" type="date" class="input"></div>
      <div class="field token-field"><label>Фильтр значений при запросе по API</label><div class="tokenbox"><div id="apiTokens" class="token-list"></div><input id="apiTokenInput" class="token-input" placeholder="Введите одно или несколько значений (РНМ, ЗН, ФН, ФД, ФПД, название…)…"><button class="inline-help inside" type="button" data-tip="Можно добавить несколько значений. Например, если ввести три РНМ, программа будет искать документы только по этим трём кассам. Допускаются РНМ, ЗН ККТ, ЗН ФН, ФД, ФПД, название и адрес.">!</button><button id="apiTokenAdd" class="token-add" title="Добавить условие">＋</button></div></div>
      <div class="field action-field"><label>&nbsp;</label><button id="apiAdvancedBtn" class="btn icon gear" title="Расширенные настройки">⚙</button></div>
      <div class="field action-field main-action"><label>&nbsp;</label><button id="searchBtn" class="btn primary">Запросить</button></div>
    </div>
    <div id="apiAdvanced" class="advanced">
      <div class="advanced-grid">
        <div class="field"><label>Время с</label><input id="timeFrom" type="time" class="input" value="00:00"></div>
        <div class="field"><label>Время по</label><input id="timeTo" type="time" class="input" value="23:59"></div>
        <div class="field"><label>РНМ (точный поиск только по регистрационному номеру)</label><input id="qRnm" class="input" placeholder="целиком или часть"></div>
        <div class="field"><label>ЗН ККТ (точный поиск только по заводскому номеру)</label><input id="qKkt" class="input" placeholder="целиком или часть"></div>
        <div class="field"><label>ЗН ФН (точный поиск только по заводскому номеру ФН)</label><input id="qFn" class="input" placeholder="целиком или часть"></div>
        <div class="field"><label>Внутреннее имя ККТ (точный поиск по наименованию ККТ в ОФД)</label><input id="qInternal" class="input" placeholder="АЗС №…, касса …"></div>
        <div class="field"><label>Место установки (точный поиск по наименованию места установки из данных регистрации ККТ)</label><input id="qRetail" class="input" placeholder="название или часть"></div>
        <div class="field"><label>Адрес установки (точный поиск по адресу места установки ККТ)</label><input id="qAddress" class="input" placeholder="адрес или часть"></div>
        <div class="field"><label>Номер смены</label><input id="qShift" class="input" inputmode="numeric" placeholder="например 415"></div>
        <div class="field"><label>Количество параллельных обращений к API</label><div class="control-with-help"><select id="qConcurrency" class="select"><option>60</option><option>80</option><option>100</option><option selected>120</option><option>150</option><option>180</option><option>200</option></select><button class="inline-help" type="button" data-tip="Большие значения ускоряют поиск, но заметно повышают нагрузку на API. Обычно лучше использовать средние значения: они дают хорошую скорость и снижают риск ошибок соединения.">!</button></div></div>
      </div>
      <div class="advanced-block">
        <div class="advanced-title">Типы фискальных документов</div>
        <div class="preset-row"><button class="btn mini" id="apiCoreTypes">Для действий в ЛК ФНС</button><button class="btn mini" id="apiAllTypes">Выбрать все</button><button class="btn mini" id="apiNoTypes">Снять все</button></div>
        <div class="checks" id="apiTypeChecks">
          <label class="check"><input class="apiDocType" type="checkbox" value="registration" checked> Регистрация*</label>
          <label class="check"><input class="apiDocType" type="checkbox" value="reregistration" checked> Перерегистрация</label>
          <label class="check"><input class="apiDocType" type="checkbox" value="close" checked> Закрытие ФН</label>
          <label class="check"><input class="apiDocType" type="checkbox" value="ticket"> Кассовый чек</label>
          <label class="check"><input class="apiDocType" type="checkbox" value="open_shift"> Открытие смены</label>
          <label class="check"><input class="apiDocType" type="checkbox" value="close_shift"> Закрытие смены</label>
          <label class="check"><input class="apiDocType" type="checkbox" value="receipt_correction"> Чек коррекции</label>
          <label class="check"><input class="apiDocType" type="checkbox" value="bso"> БСО</label>
          <label class="check"><input class="apiDocType" type="checkbox" value="bso_correction"> БСО коррекции</label>
        </div>
        <div class="heavy-note">* Universal API не имеет отдельного transactionTypes для первоначальной регистрации; программа ищет fiscalReport у ФН, активированных в выбранном периоде. Массовый запрос кассовых чеков/смен по всему парку может вернуть очень большой объём данных.</div>
      </div>
      <div class="advanced-block">
        <div class="advanced-title">Статус документа в ФНС</div>
        <div class="checks">
          <label class="check"><input class="apiStatus" type="checkbox" value="OK"> OK — отправлен</label>
          <label class="check"><input class="apiStatus" type="checkbox" value="WARNING"> WARNING — мягкий карантин</label>
          <label class="check"><input class="apiStatus" type="checkbox" value="ERROR"> ERROR — жёсткий карантин</label>
          <label class="check"><input class="apiStatus" type="checkbox" value="WAITING"> WAITING — ожидается отправка</label>
        </div>
      </div>
      <div class="advanced-footer"><div id="apiFilterSummary" class="active-filters">Основные типы • весь день • 120 параллельных запросов</div><label class="check"><input id="qRefresh" type="checkbox"> обновить каталог ККТ перед запросом</label></div>
    </div>
  </section>

  <div id="statusbar" class="statusbar"><div class="spinner"></div><div class="status-text" id="statusText">Подготовка…</div><div class="progress"><div id="progressBar"></div></div><button id="stopSearchBtn" class="btn stop-search" type="button">Стоп</button></div>
  <div id="completeness" class="completeness"><div class="complete-main"><b id="completeTitle"></b><div id="completeSub" class="complete-sub"></div></div><div><button id="failedDetailsBtn" class="btn" style="display:none">Что не проверено</button> <button id="catalogRetryBtn" class="btn warn" style="display:none">Обновить ККТ и повторить</button> <button id="retryBtn" class="btn warn" style="display:none">Повторить неудачные</button></div></div>

  <div class="summary">
    <div class="stat"><small>Загружено ФД</small><b id="sLoaded">0</b></div>
    <div class="stat"><small>Показано</small><b id="sShown">0</b></div>
    <div class="stat"><small>ККТ в парке</small><b id="sKkt">—</b></div>
    <div class="stat"><small>ККТ в API-запросе</small><b id="sMatchedKkt">—</b></div>
    <div class="stat"><small>Проверено запросов</small><b id="sChecked">—</b></div>
    <div class="stat"><small>Не проверено</small><b id="sFailed">—</b></div>
    <div class="stat"><small>Неоднозначных ФН</small><b id="sUncertain">—</b></div>
    <div class="stat"><small>Время API</small><b id="sTime">—</b></div>
  </div>
  <div id="typeSummary" class="type-summary"></div>

  <section class="panel" id="resultPanel">
    <div class="panel-main">
      <div class="panel-name"><b>Поиск и фильтры по результатам запроса</b></div>
      <div class="field"><label>Период с</label><input id="rDateFrom" type="date" class="input"></div>
      <div class="field"><label>Период по</label><input id="rDateTo" type="date" class="input"></div>
      <div class="field token-field"><label>Поиск по результатам</label><div class="tokenbox"><div id="resultTokens" class="token-list"></div><input id="resultTokenInput" class="token-input" placeholder="Введите одно или несколько значений (РНМ, ЗН, ФН, ФД, ФПД, название…)…"><button class="inline-help inside" type="button" data-tip="Можно добавить несколько значений и быстро отфильтровать уже загруженные документы. Например: РНМ, ЗН, ФН, ФД, ФПД, название ККТ или адрес.">!</button><button id="resultTokenAdd" class="token-add" title="Добавить условие">＋</button></div></div>
      <div class="field action-field"><label>&nbsp;</label><button id="resultAdvancedBtn" class="btn icon gear" title="Расширенные фильтры">⚙</button></div>
      <div class="field action-field main-action"><label>&nbsp;</label><button id="applyResultBtn" class="btn primary">Применить</button></div>
    </div>
    <div id="resultAdvanced" class="advanced">
      <div class="advanced-grid">
        <div class="field"><label>Время с</label><input id="rTimeFrom" type="time" class="input" value="00:00"></div>
        <div class="field"><label>Время по</label><input id="rTimeTo" type="time" class="input" value="23:59"></div>
        <div class="field"><label>РНМ (точный поиск только по регистрационному номеру)</label><input id="rRnm" class="input" placeholder="целиком или часть"></div>
        <div class="field"><label>ЗН ККТ (точный поиск только по заводскому номеру)</label><input id="rKkt" class="input" placeholder="целиком или часть"></div>
        <div class="field"><label>ЗН ФН (точный поиск только по заводскому номеру ФН)</label><input id="rFn" class="input" placeholder="целиком или часть"></div>
        <div class="field"><label>ФД</label><input id="rFd" class="input" placeholder="номер или часть"></div>
        <div class="field"><label>ФПД</label><input id="rFpd" class="input" placeholder="значение или часть"></div>
        <div class="field"><label>Номер смены</label><input id="rShift" class="input" placeholder="например 415"></div>
        <div class="field"><label>Внутреннее имя ККТ (точный поиск по наименованию ККТ в ОФД)</label><input id="rInternal" class="input" placeholder="название или часть"></div>
        <div class="field"><label>Место установки (точный поиск по наименованию места установки из данных регистрации ККТ)</label><input id="rRetail" class="input" placeholder="название или часть"></div>
        <div class="field"><label>Адрес установки (точный поиск по адресу места установки ККТ)</label><input id="rAddress" class="input" placeholder="адрес или часть"></div>
        <div class="field"><label>Статус ФЛК / описание ФНС</label><input id="rStatus" class="input" placeholder="OK, ошибка, текст…"></div>
      </div>
      <div class="advanced-block"><div class="advanced-title">Типы документов в уже загруженной выборке</div><div class="preset-row"><button class="btn mini" id="resultAllTypes">Выбрать все</button><button class="btn mini" id="resultNoTypes">Снять все</button></div><div class="checks" id="resultTypeChecks">
        <label class="check"><input class="resultDocType" type="checkbox" value="registration" checked> Регистрация</label>
        <label class="check"><input class="resultDocType" type="checkbox" value="reregistration" checked> Перерегистрация</label>
        <label class="check"><input class="resultDocType" type="checkbox" value="close" checked> Закрытие ФН</label>
        <label class="check"><input class="resultDocType" type="checkbox" value="ticket" checked> Кассовый чек</label>
        <label class="check"><input class="resultDocType" type="checkbox" value="open_shift" checked> Открытие смены</label>
        <label class="check"><input class="resultDocType" type="checkbox" value="close_shift" checked> Закрытие смены</label>
        <label class="check"><input class="resultDocType" type="checkbox" value="receipt_correction" checked> Чек коррекции</label>
        <label class="check"><input class="resultDocType" type="checkbox" value="bso" checked> БСО</label>
        <label class="check"><input class="resultDocType" type="checkbox" value="bso_correction" checked> БСО коррекции</label>
      </div></div>
      <div class="advanced-footer"><div id="resultFilterSummary" class="active-filters">Показываются все загруженные документы</div><button id="resetResultFilters" class="btn">Сбросить локальные фильтры</button></div>
    </div>
  </section>

  <div class="results">
    <div class="table-meta"><span id="tableInfo">Документов нет</span><span><button id="csvBtn" class="btn mini">CSV ⇩</button> <button id="jsonBtn" class="btn mini">JSON ⇩</button></span></div>
    <div class="table-wrap"><table><thead><tr>
      <th data-sort="date_sort">Дата и время</th><th data-sort="kkm_internal_name">Внутреннее имя ККТ</th><th data-sort="kkm_reg_id">РНМ</th><th data-sort="type">Тип ФД</th><th data-sort="shift">Смена</th><th data-sort="fd">ФД</th><th data-sort="amount">Сумма</th><th data-sort="fpd">ФПД</th><th data-sort="fs_number">ЗН ФН</th><th data-sort="kkm_factory_number">ЗН ККТ</th><th data-sort="fns_flc_status">Статус ФНС</th><th data-sort="inserted_at_sort">Поступил в ОФД</th><th data-sort="address">Адрес</th>
    </tr></thead><tbody id="rows"></tbody></table><div id="empty" class="empty">Сначала выполни запрос в Первый ОФД.</div></div>
    <div class="pager"><button id="firstPage">«</button><button id="prevPage">‹</button><span id="pageText">Страница 0 из 0</span><button id="nextPage">›</button><button id="lastPage">»</button><div class="page-size">Показывать по <select id="pageSize" class="select" style="width:92px;height:38px"><option>20</option><option selected>50</option><option>100</option><option>200</option><option value="all">Все</option></select></div></div>
  </div>
</div>

<div id="keyModal" class="modal"><div class="modal-box" style="width:min(760px,96vw)"><div class="modal-title">API-ключ Первого ОФД <button class="close-x" data-close="keyModal">×</button></div><div class="modal-body"><div id="keyList" class="key-list"></div><div class="key-add-grid"><div class="field"><label>Добавить новый API-ключ</label><div class="secret-wrap"><input id="newApiKey" class="input" type="password" autocomplete="off" placeholder="Вставьте API-ключ"><button id="toggleApiKey" class="secret-toggle" type="button" title="Показать / скрыть API-ключ" aria-label="Показать или скрыть API-ключ"><svg class="eye-on" viewBox="0 0 24 24"><path d="M2.5 12s3.5-6 9.5-6 9.5 6 9.5 6-3.5 6-9.5 6-9.5-6-9.5-6Z"/><circle cx="12" cy="12" r="2.8"/></svg><svg class="eye-off" viewBox="0 0 24 24"><path d="m3 3 18 18"/><path d="M10.6 6.2A10.9 10.9 0 0 1 12 6c6 0 9.5 6 9.5 6a15.5 15.5 0 0 1-3.1 3.7"/><path d="M6.4 6.4C3.9 8.2 2.5 12 2.5 12s3.5 6 9.5 6c1.2 0 2.3-.2 3.3-.6"/></svg></button></div></div><button id="addApiKeyBtn" class="btn primary">Проверить и сохранить</button></div><div id="keyError" class="key-error"></div><div class="key-note">Ключ проверяется через Universal API и сохраняется только на этом компьютере. В Windows он хранится в зашифрованном виде через DPAPI.</div></div></div></div>
<div id="detailsModal" class="modal"><div class="modal-box"><div class="modal-title">Фискальный документ <button class="close-x" data-close="detailsModal">×</button></div><div class="modal-body"><div id="detailsGrid" class="details-grid"></div><div class="raw-title"><b>Полный ответ API</b><button id="copyJson" class="btn mini">Копировать JSON</button></div><pre id="rawJson"></pre></div></div></div>
<div id="failuresModal" class="modal"><div class="modal-box"><div class="modal-title">Непроверенные элементы <button class="close-x" data-close="failuresModal">×</button></div><div class="modal-body"><div id="failureSummary" style="margin-bottom:10px"></div><div id="failureList" class="failure-list"></div></div></div></div>

<script>
const $=id=>document.getElementById(id);
const TYPE_LABELS={registration:'Регистрация',reregistration:'Перерегистрация',close:'Закрытие ФН',ticket:'Кассовый чек',open_shift:'Открытие смены',close_shift:'Закрытие смены',receipt_correction:'Чек коррекции',bso:'БСО',bso_correction:'БСО коррекции'};
const CORE_TYPES=new Set(['registration','reregistration','close']);
const HEAVY_TYPES=new Set(['ticket','open_shift','close_shift','receipt_correction','bso','bso_correction']);
let apiTerms=[],resultTerms=[],allRows=[],filteredRows=[],lastResponse=null,currentPage=1,sortState={key:'date_sort',dir:'desc'},statusTimer=null,currentRaw=null,currentDetailValues=[],keyState={active_id:null,profiles:[]},revealedKeys={};
function esc(v){return String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
function lc(v){return String(v??'').trim().toLowerCase()}
function contains(v,n){return !n||lc(v).includes(lc(n))}
function renderKeys(){
  const list=$('keyList'),profiles=keyState.profiles||[];
  if(!profiles.length){list.innerHTML='<div class="key-empty">Сохранённых API-ключей пока нет.</div>'}
  else{list.innerHTML=profiles.map(p=>{const revealed=revealedKeys[p.id];const secret=revealed?esc(revealed):`••••${esc(p.last4||'')}`;return `<div class="key-row"><div class="key-main"><b>${esc(p.name||'API-ключ')}</b><div class="key-secret-line"><span class="key-secret-value">${secret} ${p.active?'• используется сейчас':''}</span><button class="key-reveal" data-id="${esc(p.id)}" title="Показать / скрыть API-ключ"><svg viewBox="0 0 24 24"><path d="M2.5 12s3.5-6 9.5-6 9.5 6 9.5 6-3.5 6-9.5 6-9.5-6-9.5-6Z"/><circle cx="12" cy="12" r="2.8"/></svg></button></div></div><div class="key-actions">${p.active?'<span class="version-chip">Активен</span>':`<button class="btn mini key-select" data-id="${esc(p.id)}">Выбрать</button>`}<button class="btn mini key-delete" data-id="${esc(p.id)}">Удалить</button></div></div>`}).join('')}
  const active=profiles.find(p=>p.active);
  $('apiKeyBtn').textContent=active?`API: ${active.name} ••••${active.last4}`:'API-ключ';
  $('apiKeyBtn').classList.toggle('active',!!active);
}
async function loadKeys(openIfMissing=false){
  try{keyState=await fetch('/api/keys').then(r=>r.json());renderKeys();if(openIfMissing&&!(keyState.profiles||[]).some(p=>p.active))$('keyModal').classList.add('show')}catch(e){}
}
async function toggleRevealKey(id){
  if(revealedKeys[id]){delete revealedKeys[id];renderKeys();return}
  try{const res=await fetch('/api/keys/'+encodeURIComponent(id)+'/reveal');const data=await res.json();if(!res.ok)throw new Error(data.detail||'Не удалось показать ключ');revealedKeys[id]=data.api_key||'';renderKeys();setTimeout(()=>{if(revealedKeys[id]){delete revealedKeys[id];renderKeys()}},30000)}
  catch(e){alert(e.message)}
}
async function addApiKey(){
  const key=$('newApiKey').value.trim(),err=$('keyError');err.classList.remove('show');if(!key){err.textContent='Вставьте API-ключ';err.classList.add('show');return}
  $('addApiKeyBtn').disabled=true;$('addApiKeyBtn').textContent='Проверяю…';
  try{const res=await fetch('/api/keys/add',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({api_key:key})});const data=await res.json();if(!res.ok)throw new Error(data.detail||'Не удалось сохранить ключ');keyState=data;$('newApiKey').value='';$('newApiKey').type='password';$('toggleApiKey').classList.remove('visible');renderKeys();$('keyModal').classList.remove('show')}
  catch(e){err.textContent=e.message;err.classList.add('show')}
  finally{$('addApiKeyBtn').disabled=false;$('addApiKeyBtn').textContent='Проверить и сохранить'}
}
async function selectKey(id){const res=await fetch('/api/keys/select',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({profile_id:id})});const data=await res.json();if(!res.ok){alert(data.detail||'Не удалось выбрать ключ');return}keyState=data;renderKeys();$('keyModal').classList.remove('show')}
async function deleteKey(id){if(!confirm('Удалить сохранённый API-ключ с этого компьютера?'))return;const res=await fetch('/api/keys/'+encodeURIComponent(id),{method:'DELETE'});keyState=await res.json();renderKeys()}
function hasActiveKey(){return (keyState.profiles||[]).some(p=>p.active)}
function selected(selector){return [...document.querySelectorAll(selector)].filter(x=>x.checked).map(x=>x.value)}
function pageSizeValue(){const raw=$('pageSize').value;return raw==='all'?Math.max(filteredRows.length,1):Number(raw)}
function setChecks(selector,mode){document.querySelectorAll(selector).forEach(x=>x.checked=mode==='all'||(mode==='core'&&CORE_TYPES.has(x.value)));updateApiSummary()}
function toggleAdvanced(panelId,btnId){const p=$(panelId),b=$(btnId);p.classList.toggle('open');b.classList.toggle('open',p.classList.contains('open'))}
function renderTokens(kind){const terms=kind==='api'?apiTerms:resultTerms,box=$(kind==='api'?'apiTokens':'resultTokens');box.innerHTML=terms.map((t,i)=>`<div class="token"><span title="${esc(t)}">${esc(t)}</span><button data-kind="${kind}" data-index="${i}" title="Удалить">×</button></div>`).join('')}
function addToken(kind){const input=$(kind==='api'?'apiTokenInput':'resultTokenInput'),value=input.value.trim();if(!value)return;const terms=kind==='api'?apiTerms:resultTerms;if(!terms.some(x=>lc(x)===lc(value)))terms.push(value);input.value='';renderTokens(kind);if(kind==='api')updateApiSummary();else applyClientFilters()}
document.addEventListener('click',e=>{const b=e.target.closest('.token button');if(!b)return;const arr=b.dataset.kind==='api'?apiTerms:resultTerms;arr.splice(Number(b.dataset.index),1);renderTokens(b.dataset.kind);if(b.dataset.kind==='api')updateApiSummary();else applyClientFilters()});
function bindTokenInput(kind){const input=$(kind==='api'?'apiTokenInput':'resultTokenInput');input.addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();addToken(kind)}});$(kind==='api'?'apiTokenAdd':'resultTokenAdd').onclick=()=>addToken(kind)}
function updateApiSummary(){const types=selected('.apiDocType'),statuses=selected('.apiStatus');const parts=[];parts.push(types.length===3&&types.every(x=>CORE_TYPES.has(x))?'основные типы':`${types.length} типов ФД`);parts.push(`${$('timeFrom').value||'00:00'}–${$('timeTo').value||'23:59'}`);parts.push(`${$('qConcurrency').value} параллельно`);if(apiTerms.length)parts.push(`быстрых условий: ${apiTerms.length}`);if($('qRnm').value.trim())parts.push('РНМ');if($('qKkt').value.trim())parts.push('ЗН ККТ');if($('qFn').value.trim())parts.push('ЗН ФН');if($('qInternal').value.trim()||$('qRetail').value.trim()||$('qAddress').value.trim())parts.push('имя / место / адрес');if($('qShift').value.trim())parts.push(`смена ${$('qShift').value.trim()}`);if(statuses.length)parts.push(`статусы ФНС: ${statuses.join(', ')}`);$('apiFilterSummary').textContent=parts.join(' • ')}
function apiPayload(){return{date_from:$('dateFrom').value,date_to:$('dateTo').value,time_from:$('timeFrom').value||'00:00',time_to:$('timeTo').value||'23:59',refresh_kkt:$('qRefresh').checked,document_types:selected('.apiDocType'),query_terms:[...apiTerms],rnm:$('qRnm').value.trim(),kkt_factory_number:$('qKkt').value.trim(),fs_number:$('qFn').value.trim(),internal_name:$('qInternal').value.trim(),retail_place:$('qRetail').value.trim(),address:$('qAddress').value.trim(),shift_num:$('qShift').value.trim(),irkkt_statuses:selected('.apiStatus'),concurrency:Number($('qConcurrency').value)}}
function apiIsTargeted(p){return p.query_terms.length||p.rnm||p.kkt_factory_number||p.fs_number||p.internal_name||p.retail_place||p.address}
async function doSearch(){if(!hasActiveKey()){$('keyModal').classList.add('show');return}const p=apiPayload();if(!p.date_from||!p.date_to){alert('Укажи обе даты');return}if(!p.document_types.length){alert('Выбери хотя бы один тип документа');return}if(p.document_types.some(x=>HEAVY_TYPES.has(x))&&!apiIsTargeted(p)){if(!confirm('Выбраны кассовые чеки/смены по всему парку. Объём данных может быть очень большим. Продолжить?'))return}$('searchBtn').disabled=true;$('retryBtn').disabled=true;$('stopSearchBtn').disabled=false;$('statusbar').classList.add('show');$('statusText').textContent='Запуск…';$('progressBar').style.width='0%';statusTimer=setInterval(pollStatus,600);try{const res=await fetch('/api/search',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(p)});const data=await res.json();if(!res.ok)throw new Error(data.detail||'Ошибка поиска');if(data.cancelled){$('statusText').textContent=data.message||'Запрос остановлен';return}acceptResponse(data,true)}catch(e){alert(e.message)}finally{clearInterval(statusTimer);await pollStatus();setTimeout(()=>$('statusbar').classList.remove('show'),900);$('searchBtn').disabled=false;$('retryBtn').disabled=false;$('stopSearchBtn').disabled=true}}
async function stopSearch(){const btn=$('stopSearchBtn');btn.disabled=true;$('statusText').textContent='Останавливаю запрос…';try{const res=await fetch('/api/stop-search',{method:'POST'});const data=await res.json();if(!res.ok)throw new Error(data.detail||'Не удалось остановить запрос');if(!data.stopping)$('statusText').textContent=data.message||'Активного запроса нет'}catch(e){btn.disabled=false;alert(e.message)}}
async function pollStatus(){try{const s=await fetch('/api/status').then(r=>r.json());const pct=s.total?Math.min(100,Math.round(s.done/s.total*100)):0;$('progressBar').style.width=pct+'%';$('statusText').textContent=`${s.stage}: ${s.done}/${s.total} • найдено ${s.found} • неудачных ${s.errors}`+(s.message?' • '+s.message:'')}catch(e){}}
function updateCompleteness(data){const c=data.completeness||{},box=$('completeness'),retry=$('retryBtn'),catalogRetry=$('catalogRetryBtn'),details=$('failedDetailsBtn');box.className='completeness show';if(c.full_coverage){box.classList.add('ok');$('completeTitle').textContent='Выборка полная по всем запланированным API-проверкам';$('completeSub').textContent=`Успешно ${c.successful_queries}/${c.planned_queries}. Каталог ККТ получен полностью. Дополнительно проверено неоднозначных ФН: ${data.uncertain_candidate_count||0}.`;retry.style.display='none';catalogRetry.style.display='none';details.style.display='none'}else if(c.all_planned_checks_complete&&!c.registration_coverage_complete){box.classList.add('warn');$('completeTitle').textContent='Все API-запросы выполнены, но поиск первичной регистрации имеет ограничение';$('completeSub').textContent=`У ${data.registration_unverifiable_count||0} ФН нет надёжной activationDate. Фильтруемые типы документов проверены, но для первичной регистрации по этим ФН нельзя гарантировать полноту без более тяжёлого полного сканирования.`;retry.style.display='none';catalogRetry.style.display='none';details.style.display='none'}else{box.classList.add(c.failed_queries>0?'bad':'warn');$('completeTitle').textContent='Результат может быть неполным';$('completeSub').textContent=`Документные запросы: ${c.successful_queries}/${c.planned_queries}; не проверено: ${c.failed_queries}. Не загружено мест установки: ${c.catalog_failed_places}.`;retry.style.display=c.failed_queries>0?'inline-block':'none';catalogRetry.style.display=c.catalog_failed_places>0?'inline-block':'none';details.style.display=(c.failed_queries>0||c.catalog_failed_places>0)?'inline-block':'none'}}
function acceptResponse(data,resetLocal=false){lastResponse=data;allRows=data.rows||[];if(resetLocal){resultTerms=[];renderTokens('result');$('rDateFrom').value=data.period?.from||$('dateFrom').value;$('rDateTo').value=data.period?.to||$('dateTo').value;$('rTimeFrom').value=$('timeFrom').value||'00:00';$('rTimeTo').value=$('timeTo').value||'23:59';['rRnm','rKkt','rFn','rFd','rFpd','rShift','rInternal','rRetail','rAddress','rStatus'].forEach(id=>$(id).value='');document.querySelectorAll('.resultDocType').forEach(x=>x.checked=true)}updateCompleteness(data);applyClientFilters()}
function rowSearchValues(r){return[r.date,r.inserted_at,r.type,r.raw_type,r.kkm_internal_name,r.kkm_reg_id,r.kkm_factory_number,r.fs_number,r.fd,r.fpd,r.shift,r.amount,r.fns_flc_status,r.fns_status,r.fns_description,r.retail_place,r.address].map(lc)}
function rowMatchesTerms(r){if(!resultTerms.length)return true;const values=rowSearchValues(r);return resultTerms.some(term=>values.some(v=>v.includes(lc(term))))}
function resultPeriodBounds(){const d1=$('rDateFrom').value,d2=$('rDateTo').value,t1=$('rTimeFrom').value||'00:00',t2=$('rTimeTo').value||'23:59';return{from:d1?`${d1}T${t1}:00`:'',to:d2?`${d2}T${t2}:59`:''}}
function applyClientFilters(){const types=new Set(selected('.resultDocType')),bounds=resultPeriodBounds(),f={rnm:$('rRnm').value.trim(),kkt:$('rKkt').value.trim(),fn:$('rFn').value.trim(),fd:$('rFd').value.trim(),fpd:$('rFpd').value.trim(),shift:$('rShift').value.trim(),internal:$('rInternal').value.trim(),retail:$('rRetail').value.trim(),address:$('rAddress').value.trim(),status:$('rStatus').value.trim()};filteredRows=allRows.filter(r=>{if(!rowMatchesTerms(r))return false;const allTypeCount=document.querySelectorAll('.resultDocType').length;if(types.size===0)return false;if(r.document_key==='unknown'){if(types.size!==allTypeCount)return false}else if(!types.has(r.document_key))return false;const d=String(r.date_sort||'');if(bounds.from&&d&&d<bounds.from)return false;if(bounds.to&&d&&d>bounds.to)return false;if(bounds.from&&!d)return false;if(!contains(r.kkm_reg_id,f.rnm)||!contains(r.kkm_factory_number,f.kkt)||!contains(r.fs_number,f.fn)||!contains(r.fd,f.fd)||!contains(r.fpd,f.fpd)||!contains(r.shift,f.shift)||!contains(r.kkm_internal_name,f.internal)||!contains(r.retail_place,f.retail)||!contains(r.address,f.address))return false;if(f.status&&!rowSearchValues({fns_flc_status:r.fns_flc_status,fns_status:r.fns_status,fns_description:r.fns_description}).some(v=>v.includes(lc(f.status))))return false;return true});filteredRows.sort((a,b)=>{const av=a[sortState.key]??'',bv=b[sortState.key]??'';const an=Number(av),bn=Number(bv);let c=(!Number.isNaN(an)&&!Number.isNaN(bn)&&String(av).trim()!==''&&String(bv).trim()!=='')?an-bn:String(av).localeCompare(String(bv),'ru',{numeric:true});return sortState.dir==='asc'?c:-c});currentPage=1;render();updateStats();updateResultSummary()}
function updateResultSummary(){const parts=[];if(resultTerms.length)parts.push(`условий поиска: ${resultTerms.length}`);const types=selected('.resultDocType');if(types.length<document.querySelectorAll('.resultDocType').length)parts.push(`типов: ${types.length}`);['rRnm','rKkt','rFn','rFd','rFpd','rShift','rInternal','rRetail','rAddress','rStatus'].forEach(id=>{if($(id).value.trim())parts.push($(id).previousElementSibling?.textContent||id)});$('resultFilterSummary').textContent=parts.length?parts.join(' • '):'Показываются все загруженные документы'}
function updateStats(){const data=lastResponse||{},c=data.completeness||{};$('sLoaded').textContent=allRows.length;$('sShown').textContent=filteredRows.length;$('sKkt').textContent=data.kkm_count??'—';$('sMatchedKkt').textContent=data.matched_kkm_count??'—';$('sChecked').textContent=`${c.successful_queries??0}/${c.planned_queries??0}`;$('sFailed').textContent=c.failed_queries??'—';$('sUncertain').textContent=data.uncertain_candidate_count??'—';$('sTime').textContent=(data.elapsed_seconds??'—')+' сек';const counts={};filteredRows.forEach(r=>counts[r.type]=(counts[r.type]||0)+1);$('typeSummary').innerHTML=Object.entries(counts).map(([k,v])=>`<span class="type-pill">${esc(k)}: <b>${v}</b></span>`).join('')}
function render(){const body=$('rows'),empty=$('empty'),size=pageSizeValue(),pages=Math.max(1,Math.ceil(filteredRows.length/size));if(currentPage>pages)currentPage=pages;const start=(currentPage-1)*size,items=filteredRows.slice(start,start+size);body.innerHTML=items.map((r,i)=>`<tr data-index="${start+i}"><td>${esc(r.date)}</td><td>${esc(r.kkm_internal_name)}</td><td>${esc(r.kkm_reg_id)}</td><td><span class="type ${esc(r.document_key)}">${esc(r.type)}</span></td><td>${esc(r.shift)}</td><td>${esc(r.fd)}</td><td>${r.amount==null?'—':esc(r.amount)}</td><td>${esc(r.fpd)}</td><td>${esc(r.fs_number)}</td><td>${esc(r.kkm_factory_number)}</td><td>${esc(r.fns_flc_status??r.fns_status)}</td><td>${esc(r.inserted_at)}</td><td>${esc(r.address)}</td></tr>`).join('');[...body.querySelectorAll('tr')].forEach(tr=>tr.onclick=()=>showDetails(filteredRows[Number(tr.dataset.index)]));empty.style.display=filteredRows.length?'none':'block';empty.textContent=allRows.length?'По текущим локальным фильтрам ничего не найдено.':'Сначала выполни запрос в Первый ОФД.';$('tableInfo').textContent=`Показано ${filteredRows.length} из ${allRows.length} документов`;$('pageText').textContent=`Страница ${filteredRows.length?currentPage:0} из ${filteredRows.length?pages:0}`;$('firstPage').disabled=$('prevPage').disabled=currentPage<=1;$('nextPage').disabled=$('lastPage').disabled=currentPage>=pages||!filteredRows.length}
function showDetails(r){currentRaw=r.raw;const pairs=[['Тип',r.type],['Дата/время ККТ',r.date],['Поступил в ОФД',r.inserted_at],['РНМ',r.kkm_reg_id],['ЗН ККТ',r.kkm_factory_number],['ЗН ФН',r.fs_number],['ФД',r.fd],['ФПД',r.fpd],['Смена',r.shift],['Сумма',r.amount],['Статус ФЛК ФНС',r.fns_flc_status],['Код ФНС',r.fns_status],['Описание ФНС',r.fns_description],['Подтверждение ФНС',typeof r.fns_confirmation==='object'?JSON.stringify(r.fns_confirmation):r.fns_confirmation],['Внутреннее имя',r.kkm_internal_name],['Место установки',r.retail_place],['Адрес',r.address],['Активация ФН',r.activation_date],['Закрытие архива',r.close_archive_date],['Срок ФН',r.expire_date]];currentDetailValues=pairs.map(([,v])=>String(v??''));$('detailsGrid').innerHTML=pairs.map(([k,v],i)=>`<div class="detail"><small>${esc(k)}</small><div class="detail-value">${esc(v)}</div><button class="detail-copy" type="button" data-copy-index="${i}" title="Скопировать значение" aria-label="Скопировать значение">⧉</button></div>`).join('');$('rawJson').textContent=JSON.stringify(r.raw,null,2);$('detailsModal').classList.add('show')}
async function copyText(text){try{await navigator.clipboard.writeText(String(text??''));return true}catch(e){try{const ta=document.createElement('textarea');ta.value=String(text??'');ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.select();const ok=document.execCommand('copy');ta.remove();return ok}catch(_){return false}}}
async function retryFailed(){if(!lastResponse)return;$('retryBtn').disabled=true;$('searchBtn').disabled=true;$('stopSearchBtn').disabled=false;$('statusbar').classList.add('show');statusTimer=setInterval(pollStatus,600);try{const res=await fetch('/api/retry-failed',{method:'POST'});const data=await res.json();if(!res.ok)throw new Error(data.detail||'Ошибка повторного запроса');if(data.cancelled){$('statusText').textContent=data.message||'Запрос остановлен';return}acceptResponse(data,false)}catch(e){alert(e.message)}finally{clearInterval(statusTimer);await pollStatus();setTimeout(()=>$('statusbar').classList.remove('show'),900);$('retryBtn').disabled=false;$('searchBtn').disabled=false;$('stopSearchBtn').disabled=true}}
function showFailures(){if(!lastResponse)return;const c=lastResponse.completeness||{},failed=lastResponse.failed_queries||[],catalog=lastResponse.catalog_failures||[];$('failureSummary').textContent=`Не проверено документных запросов: ${c.failed_queries||0}. Не загружено мест установки: ${c.catalog_failed_places||0}.`;const parts=[];failed.forEach(x=>parts.push(`<div class="failure-row"><b>${esc((x.types||[]).join(', '))}</b><br><code>РНМ ${esc(x.rnm)} • ФН ${esc(x.fs_number)}</code><br><span class="muted">${esc(x.error)}</span></div>`));catalog.forEach(x=>parts.push(`<div class="failure-row"><b>Место установки ${esc(x.title||x.retailPlaceId)}</b><br><code>ID ${esc(x.retailPlaceId)}</code><br><span class="muted">${esc(x.error)}</span></div>`));$('failureList').innerHTML=parts.join('')||'<div class="failure-row">Непроверенных элементов нет.</div>';$('failuresModal').classList.add('show')}
function downloadCSV(){if(!filteredRows.length){alert('Нет данных для выгрузки');return}const cols=[['Дата/время ККТ','date'],['Тип','type'],['Внутреннее имя ККТ','kkm_internal_name'],['Место установки','retail_place'],['РНМ','kkm_reg_id'],['ЗН ККТ','kkm_factory_number'],['ЗН ФН','fs_number'],['ФД','fd'],['ФПД','fpd'],['Смена','shift'],['Сумма','amount'],['Статус ФЛК ФНС','fns_flc_status'],['Код ФНС','fns_status'],['Описание ФНС','fns_description'],['Поступил в ОФД','inserted_at'],['Адрес','address']];const q=v=>'"'+String(v??'').replace(/"/g,'""')+'"';const lines=[cols.map(c=>q(c[0])).join(';'),...filteredRows.map(r=>cols.map(c=>q(r[c[1]])).join(';'))];const blob=new Blob(['\ufeff'+lines.join('\r\n')],{type:'text/csv;charset=utf-8'}),a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=`ofd_${$('dateFrom').value}_${$('dateTo').value}.csv`;a.click();URL.revokeObjectURL(a.href)}
function downloadJSON(){if(!filteredRows.length){alert('Нет данных для выгрузки');return}const blob=new Blob([JSON.stringify(filteredRows,null,2)],{type:'application/json;charset=utf-8'}),a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=`ofd_${$('dateFrom').value}_${$('dateTo').value}.json`;a.click();URL.revokeObjectURL(a.href)}
function resetLocal(){resultTerms=[];renderTokens('result');$('rDateFrom').value=lastResponse?.period?.from||$('dateFrom').value;$('rDateTo').value=lastResponse?.period?.to||$('dateTo').value;$('rTimeFrom').value=$('timeFrom').value||'00:00';$('rTimeTo').value=$('timeTo').value||'23:59';['rRnm','rKkt','rFn','rFd','rFpd','rShift','rInternal','rRetail','rAddress','rStatus'].forEach(id=>$(id).value='');document.querySelectorAll('.resultDocType').forEach(x=>x.checked=true);applyClientFilters()}
function initDates(){const now=new Date(),local=new Date(now.getTime()-now.getTimezoneOffset()*60000).toISOString().slice(0,10);$('dateFrom').value=local;$('dateTo').value=local;$('rDateFrom').value=local;$('rDateTo').value=local}

bindTokenInput('api');bindTokenInput('result');initDates();renderTokens('api');renderTokens('result');
$('stopSearchBtn').onclick=stopSearch;$('detailsGrid').addEventListener('click',async e=>{const b=e.target.closest('.detail-copy');if(!b)return;const i=Number(b.dataset.copyIndex);const old=b.textContent;const ok=await copyText(currentDetailValues[i]??'');b.textContent=ok?'✓':'!';setTimeout(()=>b.textContent=old,800)});$('apiAdvancedBtn').onclick=()=>toggleAdvanced('apiAdvanced','apiAdvancedBtn');$('resultAdvancedBtn').onclick=()=>toggleAdvanced('resultAdvanced','resultAdvancedBtn');$('searchBtn').onclick=doSearch;$('applyResultBtn').onclick=applyClientFilters;$('retryBtn').onclick=retryFailed;$('catalogRetryBtn').onclick=()=>{$('qRefresh').checked=true;doSearch()};$('failedDetailsBtn').onclick=showFailures;$('csvBtn').onclick=downloadCSV;$('jsonBtn').onclick=downloadJSON;$('apiCoreTypes').onclick=()=>setChecks('.apiDocType','core');$('apiAllTypes').onclick=()=>setChecks('.apiDocType','all');$('apiNoTypes').onclick=()=>setChecks('.apiDocType','none');$('resultAllTypes').onclick=()=>{document.querySelectorAll('.resultDocType').forEach(x=>x.checked=true);applyClientFilters()};$('resultNoTypes').onclick=()=>{document.querySelectorAll('.resultDocType').forEach(x=>x.checked=false);applyClientFilters()};$('resetResultFilters').onclick=resetLocal;$('pageSize').onchange=()=>{currentPage=1;render()};$('firstPage').onclick=()=>{currentPage=1;render()};$('prevPage').onclick=()=>{currentPage=Math.max(1,currentPage-1);render()};$('nextPage').onclick=()=>{currentPage++;render()};$('lastPage').onclick=()=>{currentPage=Math.max(1,Math.ceil(filteredRows.length/pageSizeValue()));render()};document.querySelectorAll('[data-close]').forEach(x=>x.onclick=()=>$(x.dataset.close).classList.remove('show'));document.querySelectorAll('.modal').forEach(m=>m.onclick=e=>{if(e.target===m)m.classList.remove('show')});$('copyJson').onclick=async()=>{await navigator.clipboard.writeText(JSON.stringify(currentRaw,null,2));$('copyJson').textContent='Скопировано';setTimeout(()=>$('copyJson').textContent='Копировать JSON',900)};document.querySelectorAll('th[data-sort]').forEach(th=>th.onclick=()=>{const key=th.dataset.sort;if(sortState.key===key)sortState.dir=sortState.dir==='asc'?'desc':'asc';else{sortState.key=key;sortState.dir='asc'}applyClientFilters()});
['timeFrom','timeTo','qRnm','qKkt','qFn','qInternal','qRetail','qAddress','qShift','qConcurrency'].forEach(id=>$(id).addEventListener('input',updateApiSummary));document.querySelectorAll('.apiDocType,.apiStatus').forEach(x=>x.addEventListener('change',updateApiSummary));['rDateFrom','rDateTo','rTimeFrom','rTimeTo'].forEach(id=>$(id).addEventListener('change',applyClientFilters));
$('toggleApiKey').onclick=()=>{const input=$('newApiKey'),btn=$('toggleApiKey'),show=input.type==='password';input.type=show?'text':'password';btn.classList.toggle('visible',show);btn.setAttribute('aria-pressed',show?'true':'false')};
$('apiKeyBtn').onclick=()=>{$('keyModal').classList.add('show');loadKeys(false)};$('addApiKeyBtn').onclick=addApiKey;$('newApiKey').addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();addApiKey()}});$('keyList').addEventListener('click',e=>{const s=e.target.closest('.key-select'),d=e.target.closest('.key-delete'),r=e.target.closest('.key-reveal');if(s)selectKey(s.dataset.id);if(d)deleteKey(d.dataset.id);if(r)toggleRevealKey(r.dataset.id)});loadKeys(true);
updateApiSummary();render();updateStats();
</script>
</body>
</html>

"""

