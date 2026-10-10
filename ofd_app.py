import asyncio
import csv
import gzip
import zlib
from contextlib import closing, contextmanager
import io
import json
import logging
import math
import os
import random
import re
import sqlite3
import sys
import tempfile
import threading
import time
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Literal

import certifi
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, Response, StreamingResponse
from pydantic import BaseModel, Field, field_validator
from urllib.parse import parse_qs

try:
    import ijson.backends.yajl2_c as json_stream
except ImportError:
    import ijson.backends.python as json_stream
from ijson.common import ObjectBuilder

from config_store import APP_DATA_DIR, CACHE_FILE, add_or_select_profile, delete_profile, get_active_profile, get_profile_secret, get_load_profile, get_runtime_settings, list_profiles, select_profile, set_manual_settings, LOAD_PROFILES, MANUAL_SETTING_LABELS, MANUAL_SETTING_RANGES, STORAGE_MODE_NAMES, set_runtime_setting
from resource_bundle import ensure_resources
from version import APP_NAME, VERSION

BASE_URL = "https://universal-api.1-ofd.ru"
LOGGER = logging.getLogger("ofd_app.api")
APP_INSTANCE_ID = uuid.uuid4().hex
SEARCH_CONCURRENCY_CAP: ContextVar[int] = ContextVar("ofd_concurrency_cap", default=200)
SEARCH_PROFILE: ContextVar[dict[str, Any] | None] = ContextVar("ofd_load_profile", default=None)
RESOURCE_DIR = ensure_resources(APP_DATA_DIR)
RESULTS_DIR = APP_DATA_DIR / "results"
RESULTS_DIR.mkdir(exist_ok=True)


def read_json_without_array(stream: Any, prefix_to_skip: str, buffer_size: int = 32768) -> dict[str, Any]:
    """Read metadata without ever materializing the documents/legacy rows array."""
    builder = ObjectBuilder()
    for prefix, event, value in json_stream.parse(stream, use_float=False, buf_size=buffer_size):
        if prefix == prefix_to_skip:
            if event == "start_array":
                builder.event("start_array", None)
                builder.event("end_array", None)
            elif event == "null":
                builder.event(event, value)
            elif event != "end_array":
                raise ValueError("Ожидался массив документов")
            continue
        if prefix.startswith(prefix_to_skip + "."):
            continue
        builder.event(event, float(value) if isinstance(value, Decimal) else value)
    return builder.value


def python_json_numbers(value: Any) -> Any:
    """Match json.load float semantics, while retaining arbitrarily large ints."""
    pending = [value]
    while pending:
        container = pending.pop()
        if isinstance(container, dict):
            items = container.items()
        elif isinstance(container, list):
            items = enumerate(container)
        else:
            continue
        for key, item in items:
            if isinstance(item, Decimal):
                container[key] = float(item)
            elif isinstance(item, (dict, list)):
                pending.append(item)
    return value


def result_db_path(store_id: str) -> Path:
    if not re.fullmatch(r"[0-9a-f]{32}", store_id):
        raise ValueError("Некорректный идентификатор результата")
    return RESULTS_DIR / (store_id + ".sqlite")


def configure_result_connection(connection: sqlite3.Connection, profile: dict[str, Any]) -> None:
    connection.execute(f"PRAGMA cache_size=-{profile['sqlite_cache_mb'] * 1024}")
    connection.execute("PRAGMA temp_store=FILE")
    connection.execute("PRAGMA busy_timeout=30000")


RESULT_READERS: dict[str, int] = {}
RESULT_READERS_LOCK = threading.RLock()


def raw_pack_path(name: str) -> Path:
    if not re.fullmatch(r"[0-9a-f]{32}\.raw", name):
        raise ValueError("Некорректное имя архива документов")
    return RESULTS_DIR / name


class ResultConnection(sqlite3.Connection):
    """A live export/card prevents removal of its snapshot and referenced packs."""
    store_id: str | None = None

    def close(self) -> None:
        super().close()
        with RESULT_READERS_LOCK:
            if self.store_id is not None:
                count = RESULT_READERS.get(self.store_id, 1) - 1
                if count:
                    RESULT_READERS[self.store_id] = count
                else:
                    RESULT_READERS.pop(self.store_id, None)
                self.store_id = None


class BoundedRawReader:
    def __init__(self, stream: Any, offset: int, size: int):
        if offset < 0 or size <= 0 or offset + size > os.fstat(stream.fileno()).st_size:
            raise ValueError("Архив документа повреждён или неполон")
        self.stream, self.remaining = stream, size
        stream.seek(offset)

    def read(self, size: int = -1) -> bytes:
        size = self.remaining if size < 0 else min(size, self.remaining)
        data = self.stream.read(size)
        self.remaining -= len(data)
        return data


@contextmanager
def open_raw_record(name: str, offset: int, size: int):
    # Each entry is an independent gzip member; never read the next receipt.
    with raw_pack_path(name).open("rb") as source:
        with gzip.GzipFile(fileobj=BoundedRawReader(source, offset, size), mode="rb") as decoded:
            yield decoded


class IntermediateMemory:
    """One byte budget shared by every HTTP response in this search."""
    def __init__(self, limit_mb: int):
        self.limit = limit_mb * 1024 * 1024
        self.used = 0
        self.lock = threading.Lock()

    def reserve(self, size: int) -> bool:
        with self.lock:
            if self.used + size > self.limit:
                return False
            self.used += size
            return True

    def release(self, size: int) -> None:
        with self.lock:
            self.used -= size


class IntermediateBuffer:
    def __init__(self, profile: dict[str, Any], budget: IntermediateMemory):
        self.budget, self.mode = budget, profile["storage_mode"]
        self.reserved = 0
        self.on_disk = self.mode == "disk"
        self.stream = self._disk_file() if self.on_disk else io.BytesIO()
        self.threshold = min(budget.limit, 8 * 1024 * 1024) if self.mode == "mixed" else budget.limit

    def _disk_file(self):
        return tempfile.TemporaryFile(mode="w+b", dir=RESULTS_DIR)

    def _spill(self) -> None:
        target = self._disk_file()
        try:
            with self.stream.getbuffer() as data:
                for start in range(0, len(data), 65536):
                    target.write(data[start:start + 65536])
        except BaseException:
            target.close()
            raise
        self.stream.close()
        self.stream = target
        self.budget.release(self.reserved)
        self.reserved = 0
        self.on_disk = True

    def write(self, data: bytes) -> None:
        if not self.on_disk:
            if self.reserved + len(data) > self.threshold:
                self._spill()
            elif self.budget.reserve(len(data)):
                self.reserved += len(data)
            else:
                # A response larger than the memory limit spills instead of being
                # dropped, or deadlocking other workers while waiting for memory.
                self._spill()
        self.stream.write(data)

    def read(self, *args: Any):
        return self.stream.read(*args)

    def readinto(self, *args: Any):
        return self.stream.readinto(*args)

    def seek(self, *args: Any):
        return self.stream.seek(*args)

    def tell(self):
        return self.stream.tell()

    def close(self) -> None:
        try:
            self.stream.close()
        finally:
            self.budget.release(self.reserved)
            self.reserved = 0


class DiskRows:
    """Small reference to a disk index. Iteration holds one compact row at a time."""
    def __init__(self, store_id: str, owner: Any = None):
        self.store_id, self.owner = store_id, owner
        self.path = result_db_path(store_id)
        self._count = None

    def connect(self) -> sqlite3.Connection:
        with RESULT_READERS_LOCK:
            # StreamingResponse may resume a sequential generator on another
            # worker thread; readers never use a connection concurrently.
            connection = sqlite3.connect(f"{self.path.as_uri()}?mode=ro", uri=True,
                                         factory=ResultConnection, check_same_thread=False)
            connection.store_id = self.store_id
            RESULT_READERS[self.store_id] = RESULT_READERS.get(self.store_id, 0) + 1
        try:
            configure_result_connection(connection, get_load_profile())
        except BaseException:
            connection.close()
            raise
        return connection

    def __len__(self) -> int:
        if self.owner is not None:
            return self.owner.count
        if self._count is None:
            with closing(self.connect()) as connection:
                self._count = connection.execute("SELECT count(*) FROM documents").fetchone()[0]
        return self._count

    def __iter__(self):
        with closing(self.connect()) as connection:
            for (compact,) in connection.execute("SELECT compact FROM documents ORDER BY date_sort DESC, rid"):
                yield json.loads(compact)

    def sort(self, **kwargs: Any) -> None:
        pass  # The date_sort index already supplies the default order.

    def counts(self) -> dict[str, int]:
        with closing(self.connect()) as connection:
            return dict(connection.execute("SELECT json_extract(compact, '$.type'), count(*) FROM documents GROUP BY json_extract(compact, '$.type')"))

    def is_legacy(self) -> bool:
        with closing(self.connect()) as connection:
            return "raw" in {r[1] for r in connection.execute("PRAGMA table_info(documents)")}

    def detail(self, *, rid: int | None = None, document_key: str | None = None) -> dict[str, Any] | None:
        with closing(self.connect()) as connection:
            legacy = "raw" in {r[1] for r in connection.execute("PRAGMA table_info(documents)")}
            fields = "compact,raw" if legacy else "compact,raw_file,raw_offset,raw_size"
            record = connection.execute("SELECT " + fields + " FROM documents WHERE " + ("rid=?" if rid is not None else "doc_key=?"), (rid if rid is not None else document_key,)).fetchone()
            if record is None:
                return None
            if legacy:
                raw = json.loads(record[1])
            else:
                with open_raw_record(*record[1:]) as source:
                    raw = json.load(source)
            return {**json.loads(record[0]), "raw": raw}


class DocumentStore:
    """SQLite contains only the index; full JSON is in independent gzip entries."""
    def __init__(self, copy_from: DiskRows | None = None, cancelled: threading.Event | None = None):
        self.store_id = uuid.uuid4().hex
        self.path = result_db_path(self.store_id)
        self.raw_name = self.store_id + ".raw"
        self.profile = dict(SEARCH_PROFILE.get() or get_load_profile())
        self.memory_budget = IntermediateMemory(self.profile["intermediate_memory_mb"])
        self.lock = threading.RLock()
        self.cancelled = cancelled if cancelled is not None else threading.Event()
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self.raw_stream = raw_pack_path(self.raw_name).open("w+b")
        self.count = self.pending = 0
        try:
            legacy = copy_from is not None and copy_from.is_legacy()
            if copy_from is not None and not legacy:
                with closing(copy_from.connect()) as source:
                    # Raw references are immutable and shared; do not duplicate
                    # gigabytes of JSON on a manual retry.
                    source.backup(self.connection, pages=256)
            self.connection.execute("PRAGMA journal_mode=DELETE")
            self.connection.execute("PRAGMA synchronous=FULL")
            configure_result_connection(self.connection, self.profile)
            self.connection.execute("CREATE TABLE IF NOT EXISTS documents (rid INTEGER PRIMARY KEY, identity TEXT UNIQUE NOT NULL, doc_key TEXT NOT NULL, date_sort TEXT, document_key TEXT, compact TEXT NOT NULL, raw_file TEXT NOT NULL, raw_offset INTEGER NOT NULL, raw_size INTEGER NOT NULL)")
            self.connection.execute("CREATE INDEX IF NOT EXISTS doc_date ON documents(date_sort DESC)")
            self.connection.execute("CREATE INDEX IF NOT EXISTS doc_link ON documents(doc_key)")
            self.connection.execute("CREATE INDEX IF NOT EXISTS doc_raw_file ON documents(raw_file)")
            self.connection.commit()
            self.count = self.connection.execute("SELECT count(*) FROM documents").fetchone()[0]
            if legacy:
                with closing(copy_from.connect()) as source:
                    for compact, raw in source.execute("SELECT compact,raw FROM documents ORDER BY rid"):
                        row = json.loads(compact)
                        row["raw"] = json.loads(raw)
                        self.add(row)
                self.flush()
                if self.count != len(copy_from):
                    raise ValueError("Перенос результата не прошёл проверку количества документов")
        except BaseException:
            self.connection.close()
            self.raw_stream.close()
            delete_result_files(self.store_id)
            raise

    def _commit(self) -> None:
        # Persist raw bytes before publishing their offsets in SQLite.
        self.raw_stream.flush()
        os.fsync(self.raw_stream.fileno())
        self.connection.commit()
        self.pending = 0

    def add(self, row: dict[str, Any]) -> None:
        if self.cancelled.is_set():
            raise RuntimeError("Запрос остановлен")
        raw = row.pop("raw", {})
        identity = json.dumps([row.get(k) for k in ("kkm_reg_id", "fs_number", "fd", "type")], ensure_ascii=False, separators=(",", ":"))
        doc_key = "~".join(str(row.get(k) if row.get(k) is not None else "") for k in ("kkm_reg_id", "fs_number", "fd", "document_key", "date_sort"))
        with self.lock:
            if self.connection is None or self.cancelled.is_set():
                raise RuntimeError("Запрос остановлен")
            previous = self.connection.execute("SELECT rid FROM documents WHERE identity=?", (identity,)).fetchone()
            rid = previous[0] if previous else self.connection.execute("SELECT coalesce(max(rid),0)+1 FROM documents").fetchone()[0]
            row["row_id"] = rid
            compact = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
            offset = self.raw_stream.tell()
            try:
                compressor = zlib.compressobj(level=1, wbits=31)
                for text in json.JSONEncoder(ensure_ascii=False, separators=(",", ":")).iterencode(raw):
                    for start in range(0, len(text), 32768):
                        self.raw_stream.write(compressor.compress(text[start:start + 32768].encode("utf-8")))
                self.raw_stream.write(compressor.flush())
                size = self.raw_stream.tell() - offset
                self.connection.execute("INSERT INTO documents(rid,identity,doc_key,date_sort,document_key,compact,raw_file,raw_offset,raw_size) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(identity) DO UPDATE SET doc_key=excluded.doc_key,date_sort=excluded.date_sort,document_key=excluded.document_key,compact=excluded.compact,raw_file=excluded.raw_file,raw_offset=excluded.raw_offset,raw_size=excluded.raw_size", (rid, identity, doc_key, row.get("date_sort") or "", row.get("document_key") or "unknown", compact, self.raw_name, offset, size))
            except BaseException:
                self.raw_stream.seek(offset)
                self.raw_stream.truncate()
                raise
            if previous is None:
                self.count += 1
            self.pending += 1
            if self.pending >= self.profile["write_batch"]:
                self._commit()

    def flush(self) -> None:
        with self.lock:
            if self.connection is not None:
                self._commit()

    def rows(self) -> DiskRows:
        return DiskRows(self.store_id, self)

    def _compact_raw_if_needed(self) -> None:
        size = self.raw_stream.tell()
        live = self.connection.execute("SELECT coalesce(sum(raw_size),0) FROM documents WHERE raw_file=?", (self.raw_name,)).fetchone()[0]
        wasted = size - live
        if wasted < 16 * 1024 * 1024 or wasted < size * .3:
            return
        new_name = uuid.uuid4().hex + ".raw"
        new_path = raw_pack_path(new_name)
        try:
            self.connection.execute("CREATE TEMP TABLE raw_repack_map (rid INTEGER PRIMARY KEY, raw_offset INTEGER NOT NULL)")
            with new_path.open("w+b") as target:
                cursor = self.connection.execute("SELECT rid,raw_offset,raw_size FROM documents WHERE raw_file=? ORDER BY rid", (self.raw_name,))
                for rid, offset, length in cursor:
                    start = target.tell()
                    self.raw_stream.seek(offset)
                    remaining = length
                    while remaining:
                        chunk = self.raw_stream.read(min(remaining, 65536))
                        if not chunk:
                            raise ValueError("Неполный архив при очистке")
                        target.write(chunk)
                        remaining -= len(chunk)
                    self.connection.execute("INSERT INTO raw_repack_map VALUES(?,?)", (rid, start))
                target.flush()
                os.fsync(target.fileno())
            self.connection.execute("UPDATE documents SET raw_file=?,raw_offset=(SELECT raw_offset FROM raw_repack_map WHERE raw_repack_map.rid=documents.rid) WHERE raw_file=?", (new_name, self.raw_name))
            self.connection.execute("DROP TABLE raw_repack_map")
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            new_path.unlink(missing_ok=True)
            LOGGER.exception("Очистка архива отложена; исходные данные сохранены")
            return
        self.raw_stream.close()
        try:
            raw_pack_path(self.raw_name).unlink(missing_ok=True)
        except OSError:
            pass  # Mark-and-sweep will reclaim the redundant old pack later.
        self.raw_name = new_name
        self.raw_stream = new_path.open("r+b")
        self.raw_stream.seek(0, os.SEEK_END)

    def close(self) -> None:
        with self.lock:
            if self.connection is not None:
                connection, raw_stream = self.connection, self.raw_stream
                try:
                    self._commit()
                    if not self.cancelled.is_set():
                        self._compact_raw_if_needed()
                finally:
                    self.connection = None
                    connection.close()
                    self.raw_stream.close()


def delete_result_files(store_id: str) -> None:
    path = result_db_path(store_id)
    path.unlink(missing_ok=True)
    for suffix in ("-journal", "-wal", "-shm"):
        path.with_name(path.name + suffix).unlink(missing_ok=True)
    raw_pack_path(store_id + ".raw").unlink(missing_ok=True)


def remove_unpublished_store(store: DocumentStore) -> None:
    delete_result_files(store.store_id)
    # A finished compaction may have given this unpublished pack a fresh name.
    raw_pack_path(store.raw_name).unlink(missing_ok=True)


SEARCH_STORE: ContextVar[DocumentStore | None] = ContextVar("ofd_document_store", default=None)


class DocumentStream:
    def __init__(self, stream: Any, buffer_size: int):
        self.stream, self.buffer_size = stream, buffer_size
        self._metadata = None

    def metadata(self) -> dict[str, Any]:
        if self._metadata is None:
            self.stream.seek(0)
            self._metadata = read_json_without_array(self.stream, "documents", self.buffer_size)
        return self._metadata

    def documents(self):
        self.stream.seek(0)
        for document in json_stream.items(self.stream, "documents.item", use_float=False, buf_size=self.buffer_size):
            yield python_json_numbers(document)

    def close(self) -> None:
        self.stream.close()

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
ACTIVE_REQUEST: dict[str, Any] | None = None
SEARCH_STATE_FILE = APP_DATA_DIR / "last_search.json"

STATE: dict[str, Any] = {
    "running": False,
    "stage": "Готово",
    "done": 0,
    "total": 0,
    "found": 0,
    "errors": 0,
    "message": "",
    "cancelled": False,
    "error": "",
}

# LAST_SEARCH содержит внутренний контекст для ручного повтора failed tasks.
# LAST_RESPONSE — готовый пользовательский результат. Он дополнительно хранится
# на диске, поэтому закрытие/обновление браузерной вкладки не стирает выборку.
LAST_SEARCH: dict[str, Any] = {
    "rows": [],
    "failed_tasks": [],
    "meta": {},
}
LAST_RESPONSE: dict[str, Any] | None = None
LEGACY_STORE_TO_MIGRATE: str | None = None


def load_saved_search_state() -> None:
    global LAST_RESPONSE, LEGACY_STORE_TO_MIGRATE
    if not SEARCH_STATE_FILE.exists():
        return
    store = None
    original_store_id = None
    try:
        with SEARCH_STATE_FILE.open("rb") as stream:
            data = read_json_without_array(stream, "result.rows")
        result, retry_context = data.get("result"), data.get("retry_context")
        migrated = False
        if isinstance(result, dict):
            store_id = result.get("store_id")
            if store_id:
                rows = DiskRows(store_id)
                if not rows.path.is_file():
                    raise ValueError("Не найден дисковый индекс последнего поиска")
                if rows.is_legacy():
                    # Do not block server startup on a multi-gigabyte conversion.
                    LEGACY_STORE_TO_MIGRATE = rows.store_id
            else:
                # One-time streaming migration of old last_search.json with raw.
                store = DocumentStore()
                with SEARCH_STATE_FILE.open("rb") as stream:
                    for row in json_stream.items(stream, "result.rows.item", use_float=False):
                        store.add(python_json_numbers(row))
                store.close()
                rows = store.rows()
                result.update(store_id=store.store_id, paged=True, total_rows=len(rows), rows=[])
                migrated = True
            LAST_RESPONSE = result
            LAST_SEARCH["rows"] = rows
        if isinstance(retry_context, dict):
            LAST_SEARCH.update(failed_tasks=retry_context.get("failed_tasks") or [], meta=retry_context.get("meta") or {})
            query_request = LAST_SEARCH["meta"].get("request") or {}
            if query_request.get("query_conditions"):
                for task in LAST_SEARCH["failed_tasks"]:
                    task["query_request"] = query_request
        if migrated and persist_search_state():
            # The old huge inline database is redundant after an atomic switch.
            if original_store_id:
                delete_result_files(original_store_id)
            cleanup_result_stores(None)
            STATE.update(stage="Готово", message="Сохранённые документы перенесены в компактный индекс")
    except Exception:
        LOGGER.exception("Не удалось восстановить последний результат поиска")
    finally:
        if store is not None:
            store.close()


def persist_search_state() -> bool:
    if LAST_RESPONSE is None:
        return False
    temp = SEARCH_STATE_FILE.with_suffix(".tmp")
    try:
        with temp.open("w", encoding="utf-8") as stream:
            stream.write('{"version":2,"saved_at":')
            json.dump(datetime.now().isoformat(timespec="seconds"), stream)
            stream.write(',"result":')
            json.dump(LAST_RESPONSE, stream, ensure_ascii=False, separators=(",", ":"))
            stream.write(',"retry_context":{"failed_tasks":[')
            for index, task in enumerate(LAST_SEARCH.get("failed_tasks") or []):
                if index:
                    stream.write(",")
                json.dump({key: value for key, value in task.items() if key != "query_request"}, stream, ensure_ascii=False, separators=(",", ":"))
            stream.write('],"meta":')
            json.dump(LAST_SEARCH.get("meta") or {}, stream, ensure_ascii=False, separators=(",", ":"))
            stream.write("}}")
            stream.flush()
            os.fsync(stream.fileno())
        temp.replace(SEARCH_STATE_FILE)
        return True
    except Exception:
        LOGGER.exception("Не удалось сохранить последний результат поиска")
        try:
            temp.unlink(missing_ok=True)
        except OSError:
            pass
        return False


def cleanup_result_stores(previous_store_id: str | None) -> None:
    # Hold the reader lock through mark-and-sweep so a new card/export cannot
    # open a snapshot between deciding it is unused and deleting its packs.
    with RESULT_READERS_LOCK:
        keep = {previous_store_id, (LAST_RESPONSE or {}).get("store_id"), *RESULT_READERS}
        for path in RESULTS_DIR.glob("*.sqlite"):
            if not re.fullmatch(r"[0-9a-f]{32}", path.stem):
                continue
            if path.stem not in keep:
                try:
                    path.unlink()
                    for suffix in ("-journal", "-wal", "-shm"):
                        path.with_name(path.name + suffix).unlink(missing_ok=True)
                except OSError:
                    keep.add(path.stem)
        referenced = set()
        try:
            for store_id in keep:
                if store_id and result_db_path(store_id).exists():
                    with closing(DiskRows(store_id).connect()) as connection:
                        columns = {r[1] for r in connection.execute("PRAGMA table_info(documents)")}
                        if "raw_file" in columns:
                            referenced.update(r[0] for r in connection.execute("SELECT DISTINCT raw_file FROM documents"))
        except (OSError, sqlite3.Error):
            LOGGER.exception("Очистка архивов отложена: не удалось проверить ссылки")
            return  # Never remove packs when their live references are uncertain.
        for path in RESULTS_DIR.glob("*.raw"):
            if not re.fullmatch(r"[0-9a-f]{32}\.raw", path.name):
                continue
            if path.name not in referenced:
                try:
                    path.unlink()
                except OSError:
                    pass


def close_search_store() -> None:
    """Release cancelled/failed scratch results; keep both published contexts."""
    store = SEARCH_STORE.get()
    if store is None:
        return
    store.cancelled.set()
    try:
        store.close()
    finally:
        saved_rows = LAST_SEARCH.get("rows")
        keep = {(LAST_RESPONSE or {}).get("store_id"), getattr(saved_rows, "store_id", None)}
        if store.store_id not in keep:
            try:
                remove_unpublished_store(store)
            except OSError:
                pass


def effective_concurrency(requested: int) -> int:
    return max(1, min(requested, SEARCH_CONCURRENCY_CAP.get()))


async def run_bounded_workers(items: list[Any], worker: Any, concurrency: int) -> None:
    """At most N coroutine tasks, not one waiting task for every KKT/FN."""
    iterator = iter(items)
    async def consume() -> None:
        for item in iterator:
            await worker(item)
    pending = [asyncio.create_task(consume()) for _ in range(min(len(items), concurrency))]
    try:
        await asyncio.gather(*pending)
    finally:
        for task in pending:
            if not task.done():
                task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


load_saved_search_state()

app = FastAPI(title="Первый ОФД — фискальные документы", version=VERSION)

async def migrate_saved_store_background() -> None:
    global LAST_RESPONSE, ACTIVE_SEARCH_TASK, LEGACY_STORE_TO_MIGRATE
    original_id = LEGACY_STORE_TO_MIGRATE
    cancelled = threading.Event()
    store = None
    worker = None
    published = False

    def convert() -> DocumentStore:
        converted = DocumentStore(copy_from=DiskRows(original_id), cancelled=cancelled)
        try:
            converted.close()
        except BaseException:
            remove_unpublished_store(converted)
            raise
        return converted

    try:
        async with SEARCH_LOCK:
            STATE.update(running=True, cancelled=False, error="", stage="Перенос сохранённых документов",
                         message="Перенос большого результата может занять время; прежние документы доступны", done=0, total=0)
            SEARCH_PROFILE.set(get_load_profile())
            worker = asyncio.create_task(asyncio.to_thread(convert))
            store = await asyncio.shield(worker)
            old_response = LAST_RESPONSE
            old_rows = LAST_SEARCH["rows"]
            new_response = {**old_response, "store_id": store.store_id, "total_rows": store.count, "paged": True, "rows": []}
            LAST_RESPONSE = new_response
            LAST_SEARCH["rows"] = store.rows()
            # This tiny metadata write is atomic; do not await between switching
            # RAM references and persisting, so cancellation cannot split them.
            if persist_search_state():
                published = True
                LEGACY_STORE_TO_MIGRATE = None
                await asyncio.to_thread(cleanup_result_stores, None)
                STATE.update(stage="Готово", message="Сохранённые документы перенесены в компактный индекс")
            else:
                LAST_RESPONSE, LAST_SEARCH["rows"] = old_response, old_rows
                remove_unpublished_store(store)
                STATE.update(stage="Ошибка переноса", message="Старый результат сохранён; не удалось записать новое состояние")
    except asyncio.CancelledError:
        cancelled.set()
        if worker is not None:
            try:
                completed = await worker
                if not published:
                    remove_unpublished_store(completed)
            except (Exception, asyncio.CancelledError):
                pass
        if not published:
            STATE.update(stage="Перенос остановлен", message="Прежний результат сохранён", cancelled=True)
    except Exception:
        LOGGER.exception("Перенос результата отложен; прежний результат сохранён")
        STATE.update(stage="Ошибка переноса", message="Прежний результат сохранён. Подробности в журнале.")
    finally:
        STATE["running"] = False
        if ACTIVE_SEARCH_TASK is asyncio.current_task():
            ACTIVE_SEARCH_TASK = None


@app.on_event("startup")
async def start_saved_store_migration() -> None:
    global ACTIVE_SEARCH_TASK
    if LEGACY_STORE_TO_MIGRATE:
        STATE["running"] = True
        ACTIVE_SEARCH_TASK = asyncio.create_task(migrate_saved_store_background(), name="migrate-result-index")



class QueryCondition(BaseModel):
    # CSV creates typed chips. Keep identifiers as strings (including leading zeroes).
    type: Literal["rnm", "kkt", "fn", "fd", "fpd", "internal", "retail", "address", "shift"]
    completeness: Literal["full", "partial"]
    data: str

    @field_validator("data")
    @classmethod
    def nonempty_data(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Значение условия не должно быть пустым")
        return value


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
    query_conditions: list[QueryCondition] = Field(default_factory=list)
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


class LocalFilterRequest(BaseModel):
    store_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    terms: list[str] = Field(default_factory=list, max_length=10000)
    conditions: list[QueryCondition] = Field(default_factory=list, max_length=10000)
    document_types: list[str] = Field(default_factory=lambda: list(DOCUMENT_TYPES), max_length=32)
    date_from: str = ""
    date_to: str = ""
    advanced: dict[str, str] = Field(default_factory=dict)
    sort_key: Literal["date_sort", "inserted_at_sort", "type", "kkm_internal_name", "kkm_reg_id", "kkm_factory_number", "fs_number", "fd", "fpd", "shift", "amount", "fns_flc_status", "fns_status", "inserted_at", "address"] = "date_sort"
    sort_dir: Literal["asc", "desc"] = "desc"
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=1000)


LOCAL_FIELD_KEYS = {"rnm": "kkm_reg_id", "kkt": "kkm_factory_number", "fn": "fs_number", "fd": "fd", "fpd": "fpd", "shift": "shift", "internal": "kkm_internal_name", "retail": "retail_place", "address": "address"}
LOCAL_SEARCH_KEYS = ("date", "inserted_at", "type", "raw_type", "kkm_internal_name", "kkm_reg_id", "kkm_factory_number", "fs_number", "fd", "fpd", "shift", "amount", "fns_flc_status", "fns_status", "fns_description", "retail_place", "address")


def local_cell_text(value: Any) -> str:
    return str(value if value is not None else "").strip()


def local_predicate(request: LocalFilterRequest):
    types = set(request.document_types)
    all_types = set(DOCUMENT_TYPES).issubset(types)
    terms = [clean_text(term).lower() for term in request.terms if clean_text(term)]
    full, fragments = {}, {}
    for condition in request.conditions:
        value = condition.data.strip() if condition.type == "fpd" else condition.data.strip().lower()
        if condition.completeness == "full":
            full.setdefault(condition.type, set()).add(value)
        else:
            fragments.setdefault(condition.type, []).append(value)
    advanced = [(LOCAL_FIELD_KEYS[k], v.strip().lower()) for k, v in request.advanced.items() if k in LOCAL_FIELD_KEYS and v.strip()]
    status = request.advanced.get("status", "").strip().lower()
    if all_types and not terms and not request.conditions and not advanced and not status and not request.date_from and not request.date_to:
        return lambda compact: True

    def matches(compact: str) -> bool:
        row = json.loads(compact)
        kind = row.get("document_key", "unknown")
        if not types or (kind == "unknown" and not all_types) or (kind != "unknown" and kind not in types):
            return False
        date = str(row.get("date_sort") or "")
        if request.date_from and (not date or date < request.date_from):
            return False
        if request.date_to and date and date > request.date_to:
            return False
        if not terms and not request.conditions and not advanced and not status:
            return True
        values = {k: local_cell_text(row.get(k)).lower() for k in LOCAL_SEARCH_KEYS}
        if any(value not in values.get(key, "") for key, value in advanced):
            return False
        if status and not any(status in values.get(k, "") for k in ("fns_flc_status", "fns_status", "fns_description")):
            return False
        if not terms and not request.conditions:
            return True
        if any(term in value for term in terms for value in values.values()):
            return True
        for kind, wanted in full.items():
            key = LOCAL_FIELD_KEYS.get(kind)
            actual = local_cell_text(row.get(key)) if kind == "fpd" else values.get(key, "")
            if actual in wanted:
                return True
        for kind, wanted in fragments.items():
            key = LOCAL_FIELD_KEYS.get(kind)
            actual = local_cell_text(row.get(key)) if kind == "fpd" else values.get(key, "")
            if any(value in actual for value in wanted):
                return True
        return False
    return matches


def natural_compare(left: str, right: str) -> int:
    try:
        a, b = float(left), float(right)
        if math.isfinite(a) and math.isfinite(b):
            return (a > b) - (a < b)
    except ValueError:
        pass
    split = lambda text: [(0, int(part)) if part.isdigit() else (1, part.lower().replace("ё", "е")) for part in re.split(r"(\d+)", text)]
    a, b = split(left), split(right)
    return (a > b) - (a < b)


def local_connection(request: LocalFilterRequest) -> sqlite3.Connection:
    rows = DiskRows(request.store_id)
    if not rows.path.is_file():
        raise HTTPException(status_code=404, detail="Этот результат уже заменён. Восстанови последний поиск.")
    connection = rows.connect()
    connection.create_function("matches", 1, local_predicate(request), deterministic=True)
    connection.create_function("sort_value", 1, lambda compact: local_cell_text(json.loads(compact).get(request.sort_key)), deterministic=True)
    connection.create_collation("NATURAL_RU", natural_compare)
    return connection


def query_local_page(request: LocalFilterRequest, cancelled: threading.Event | None = None) -> dict[str, Any]:
    page_size = min(request.page_size, get_load_profile()["page_rows"])
    with closing(local_connection(request)) as connection:
        if cancelled is not None:
            connection.set_progress_handler(lambda: int(cancelled.is_set()), 1000)
        counts = dict(connection.execute("SELECT document_key,count(*) FROM documents WHERE matches(compact) GROUP BY document_key"))
        total = sum(counts.values())
        page = min(request.page, max(1, (total + page_size - 1) // page_size))
        order = "ASC" if request.sort_dir == "asc" else "DESC"
        sort_expression = "date_sort" if request.sort_key == "date_sort" else "sort_value(compact) COLLATE NATURAL_RU"
        records = connection.execute(f"SELECT compact FROM documents WHERE matches(compact) ORDER BY {sort_expression} {order},rid LIMIT ? OFFSET ?", (page_size, (page - 1) * page_size))
        rows = [json.loads(record[0]) for record in records]
    return {"rows": rows, "filtered_count": total, "type_counts": counts, "page": page, "page_size": page_size}


EXPORT_COLUMNS = [("Дата/время ФД", "date"), ("Тип", "type"), ("Внутреннее имя в ОФД", "kkm_internal_name"), ("Место установки", "retail_place"), ("РНМ", "kkm_reg_id"), ("ЗН ККТ", "kkm_factory_number"), ("ЗН ФН", "fs_number"), ("ФД", "fd"), ("ФПД", "fpd"), ("Смена", "shift"), ("Сумма", "amount"), ("Статус ФЛК ФНС", "fns_flc_status"), ("Код ФНС", "fns_status"), ("Описание ФНС", "fns_description"), ("Поступил в ОФД", "inserted_at"), ("Адрес", "address")]


def stream_export(request: LocalFilterRequest, format_name: str):
    with closing(local_connection(request)) as connection:
        order = "ASC" if request.sort_dir == "asc" else "DESC"
        legacy = "raw" in {r[1] for r in connection.execute("PRAGMA table_info(documents)")}
        columns = ("compact,raw" if legacy else "compact,raw_file,raw_offset,raw_size") if format_name == "json" else "compact"
        sort_expression = "date_sort" if request.sort_key == "date_sort" else "sort_value(compact) COLLATE NATURAL_RU"
        records = connection.execute(f"SELECT {columns} FROM documents WHERE matches(compact) ORDER BY {sort_expression} {order},rid")
        if format_name == "csv":
            buffer = io.StringIO(newline="")
            writer = csv.writer(buffer, delimiter=";", quoting=csv.QUOTE_ALL, lineterminator="\r\n")
            writer.writerow([label for label, key in EXPORT_COLUMNS])
            yield "\ufeff" + buffer.getvalue()
            for (compact,) in records:
                row = json.loads(compact)
                buffer.seek(0); buffer.truncate(0)
                writer.writerow([row.get(key) if row.get(key) is not None else "" for label, key in EXPORT_COLUMNS])
                yield buffer.getvalue()
        else:
            yield "["
            for index, record in enumerate(records):
                row = json.loads(record[0]); row.pop("row_id", None)
                if index:
                    yield ","
                yield json.dumps(row, ensure_ascii=False, separators=(",", ":"))[:-1] + ',"raw":'
                if legacy:
                    raw = record[1]
                    for start in range(0, len(raw), 65536):
                        yield raw[start:start + 65536]
                else:
                    with open_raw_record(*record[1:]) as source:
                        for chunk in iter(lambda: source.read(65536), b""):
                            yield chunk
                yield "}"
            yield "]"


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

            if response.status_code == 204 or not response.content.strip():
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
    now = time.time()
    for key in list(TOKEN_CACHE):
        if now - float(TOKEN_CACHE[key].get("created_at", 0)) >= 110 * 60:
            TOKEN_CACHE.pop(key, None)
    if profile_id not in TOKEN_CACHE and len(TOKEN_CACHE) >= 8:
        oldest = min(TOKEN_CACHE, key=lambda key: float(TOKEN_CACHE[key].get("created_at", 0)))
        TOKEN_CACHE.pop(oldest, None)
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
        with CACHE_FILE.open(encoding="utf-8") as stream:
            data = json.load(stream)

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
    temp = CACHE_FILE.with_suffix(".tmp")
    with temp.open("w", encoding="utf-8") as stream:
        json.dump({
                "organisationKey": organisation_key,
                "savedAt": time.time(),
                "complete": True,
                "retailPlaceCount": retail_place_count,
                "kkms": kkms,
            }, stream, ensure_ascii=False, separators=(",", ":"))
    temp.replace(CACHE_FILE)


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
    concurrency = effective_concurrency(concurrency)
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

    await run_bounded_workers(places, worker, concurrency)
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


DOCUMENT_QUERY_TYPES = {"fd", "fpd", "shift"}


def condition_matches(value: Any, condition: QueryCondition) -> bool:
    if value is None:
        return False
    actual, wanted = str(value).strip(), condition.data
    # ФПД может быть буквенно-цифровым; регистр является частью идентификатора.
    if condition.type != "fpd":
        actual, wanted = actual.casefold(), wanted.casefold()
    return actual == wanted if condition.completeness == "full" else wanted in actual


def catalog_query_values(kkm: dict[str, Any], fs_number: str | None = None) -> dict[str, list[Any]]:
    return {
        "rnm": [kkm.get("kkmRegId")],
        "kkt": [kkm.get("kkmFactoryNumber")],
        "fn": ([fs_number] if fs_number is not None else
               [drive.get("fsFactoryNumber") for drive in prepare_drives(kkm)]),
        "internal": [kkm.get("kkmInternalName") or kkm.get("title")],
        "retail": [kkm.get("_retailPlaceTitle")],
        "address": [kkm.get("kkmAddress") or kkm.get("address") or kkm.get("_retailPlaceAddress")],
    }


def matches_query_chips(kkm: dict[str, Any], request: SearchRequest, *,
                        fs_number: str | None = None, row: dict[str, Any] | None = None) -> bool:
    """OR across manual/catalog chips and typed CSV chips; advanced fields stay AND.

    FD/FPD/shift have no safe catalog prefilter. Do not silently exclude KKT:
    request their selected documents and apply those conditions to actual rows.
    """
    terms = [clean_text(term) for term in request.query_terms if clean_text(term)]
    if not terms and not request.query_conditions:
        return True
    values = catalog_query_values(kkm, fs_number)
    # Manual chips retain their existing catalog semantics, including historical FN.
    manual_values = catalog_query_values(kkm)
    if any(contains(value, term) for term in terms for group in manual_values.values() for value in group):
        return True
    if row is not None:
        values.update({"fd": [row.get("fd")], "fpd": [row.get("fpd")], "shift": [row.get("shift")]})
    for condition in request.query_conditions:
        if row is None and condition.type in DOCUMENT_QUERY_TYPES:
            return True
        if any(condition_matches(value, condition) for value in values.get(condition.type, [])):
            return True
    return False


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

    return matches_query_chips(kkm, request)

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
) -> DocumentStream:
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

    profile = SEARCH_PROFILE.get() or get_load_profile()
    chunk_size = profile["http_chunk_kb"] * 1024
    headers = api_headers(token)
    headers["Accept-Encoding"] = "identity"
    url = f"{BASE_URL}/api/rent/v2/organisations/{organisation_key}/documents"
    for attempt in range(1, retries + 1):
        store = SEARCH_STORE.get()
        budget = store.memory_budget if store is not None else IntermediateMemory(profile["intermediate_memory_mb"])
        stream = IntermediateBuffer(profile, budget)
        try:
            async with client.stream("GET", url, headers=headers, params=params) as response:
                if response.status_code in (429, 500, 502, 503, 504) and attempt < retries:
                    try:
                        delay = float(response.headers.get("Retry-After", ""))
                    except ValueError:
                        delay = attempt * 0.9 + random.uniform(0.05, 0.35)
                    stream.close()
                    await asyncio.sleep(min(delay, 8.0))
                    continue
                response.raise_for_status()
                if response.status_code == 204:
                    stream.write(b"{}")
                else:
                    async for chunk in response.aiter_bytes(chunk_size=chunk_size):
                        stream.write(chunk)
            if stream.tell() == 0:
                stream.write(b"{}")
            data = DocumentStream(stream, chunk_size)
            await asyncio.to_thread(data.metadata)
            return data
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError, ValueError):
            stream.close()
            if attempt >= retries:
                raise
            await asyncio.sleep(attempt * 0.8 + random.uniform(0.05, 0.3))
        except BaseException:
            stream.close()
            raise
    raise RuntimeError("Не удалось получить документы")


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
) -> tuple[DiskRows, list[dict[str, Any]], int]:
    concurrency = effective_concurrency(concurrency)
    semaphore = asyncio.Semaphore(max(1, concurrency))
    store = SEARCH_STORE.get()
    if store is None:
        raise RuntimeError("Не открыт дисковый индекс результата")
    results = store.rows()
    failed: list[dict[str, Any]] = []
    completed = 0
    succeeded = 0
    total = len(tasks)
    # Old persisted retry tasks have no typed conditions and remain compatible.
    task_requests: dict[int, SearchRequest] = {}
    request_models: dict[int, SearchRequest] = {}
    for task in tasks:
        if task.get("query_request"):
            key = id(task["query_request"])
            if key not in request_models:
                request_models[key] = SearchRequest.model_validate(task["query_request"])
            task_requests[id(task)] = request_models[key]

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

            def save_documents() -> None:
                try:
                    wanted_keys = set(task.get("wanted_keys") or [])
                    metadata = data.metadata()
                    query_request = task_requests.get(id(task))
                    for document in data.documents():
                        if store.cancelled.is_set():
                            raise RuntimeError("Запрос остановлен")
                        if canonical_document_key(document) in wanted_keys:
                            row = normalize_result(document, metadata, candidate)
                            if query_request is None or matches_query_chips(kkm, query_request, fs_number=str(row.get("fs_number") or ""), row=row):
                                store.add(row)
                    store.flush()
                finally:
                    data.close()
            await asyncio.to_thread(save_documents)

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

    await run_bounded_workers(tasks, worker, concurrency)
    return results, failed, succeeded


async def execute_tasks_with_recovery(
    client: httpx.AsyncClient,
    token: str,
    organisation_key: str,
    tasks: list[dict[str, Any]],
    *,
    primary_concurrency: int,
) -> tuple[DiskRows, list[dict[str, Any]], int]:
    """Быстрый проход -> медленный добор -> финальный добор."""
    primary_concurrency = effective_concurrency(primary_concurrency)
    all_results = SEARCH_STORE.get().rows()
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
        total_succeeded += succeeded

    return all_results, failed, total_succeeded


def deduplicate(rows: Any) -> Any:
    if isinstance(rows, DiskRows):
        return rows
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


def make_counts(rows: Any) -> dict[str, int]:
    if isinstance(rows, DiskRows):
        return rows.counts()
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
    rows: list[dict[str, Any]] | DiskRows,
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
        "rows": [] if isinstance(rows, DiskRows) else rows,
        "store_id": rows.store_id if isinstance(rows, DiskRows) else None,
        "total_rows": len(rows),
        "paged": isinstance(rows, DiskRows),
        "large_selection": len(rows) >= 100000,
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
    profile = get_load_profile()
    SEARCH_PROFILE.set(profile)
    SEARCH_CONCURRENCY_CAP.set(profile["concurrency_cap"])
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
    # Never allocate hundreds of idle sockets for a reduced tray limit.
    connection_budget = max(effective_concurrency(request.concurrency), effective_concurrency(KKT_CONCURRENCY))
    max_connections = connection_budget
    max_keepalive = min(connection_budget, 32)
    limits = httpx.Limits(
        max_connections=max_connections,
        max_keepalive_connections=max_keepalive,
        keepalive_expiry=60,
    )
    timeout = httpx.Timeout(connect=15, read=70, write=30, pool=60)

    store = DocumentStore()
    SEARCH_STORE.set(store)
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

        # In particular a full FN chip must not request all historical FN of its KKT.
        def selected_candidate(candidate: dict[str, Any]) -> bool:
            return matches_query_chips(candidate["kkm"], request, fs_number=candidate["fs_number"])

        active = [candidate for candidate in active if selected_candidate(candidate)]
        close_candidates = [candidate for candidate in close_candidates if selected_candidate(candidate)]
        activation_candidates = [candidate for candidate in activation_candidates if selected_candidate(candidate)]
        # Recalculate uncertainty after the typed FN prefilter, not for excluded FN.
        uncertain_count = sum(parse_api_datetime(candidate["drive"].get("activationDate")) is None
                              for candidate in active)

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

        if request.query_conditions:
            query_request = request.model_dump()
            for task in tasks:
                task["query_request"] = query_request

        planned_queries = len(tasks)
        rows = store.rows()
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

    await asyncio.to_thread(store.close)
    rows = deduplicate(rows)
    rows.sort(key=lambda item: str(item.get("date_sort") or ""), reverse=True)

    elapsed = time.perf_counter() - started
    query_filters = {
        "document_types": sorted(selected_types),
        "query_terms": [term for term in request.query_terms if clean_text(term)],
        "query_conditions": [condition.model_dump() for condition in request.query_conditions],
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
    profile = get_load_profile()
    SEARCH_PROFILE.set(profile)
    SEARCH_CONCURRENCY_CAP.set(profile["concurrency_cap"])
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

    store = await asyncio.to_thread(DocumentStore, copy_from=LAST_SEARCH["rows"])
    SEARCH_STORE.set(store)
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

    await asyncio.to_thread(store.close)
    combined_rows = rows
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
    return {"ok": True, "app": APP_NAME, "version": VERSION, "instance_id": APP_INSTANCE_ID}


@app.get("/static/{relative_path:path}")
async def static_resource(relative_path: str) -> Response:
    part = Path(relative_path)
    allowed = relative_path in {"docs/csv_filter_guide.pdf", "docs/csv_filter_template.csv", "fonts/NotoSans-Variable.ttf", "fonts/OFL.txt"} or (
        len(part.parts) == 2 and part.parts[0] == "icons" and part.suffix.lower() in {".svg", ".png", ".ico"}
    )
    candidate = (RESOURCE_DIR / part).resolve()
    if not allowed or not candidate.is_relative_to(RESOURCE_DIR.resolve()) or not candidate.is_file():
        raise HTTPException(status_code=404, detail="Файл не найден")
    media = "font/ttf" if part.suffix.lower() == ".ttf" else None
    return FileResponse(candidate, media_type=media, headers={"X-Content-Type-Options": "nosniff"})


@app.get("/favicon.ico")
async def favicon() -> Response:
    executable_dir = Path(sys.executable if getattr(sys, "frozen", False) else __file__).resolve().parent
    custom = executable_dir / "app.ico"
    path = custom if custom.is_file() else RESOURCE_DIR / "icons/favicon.ico"
    return FileResponse(path, media_type="image/x-icon")


UPDATE_CACHE_FILE = APP_DATA_DIR / "update_check.json"
UPDATE_INTERVAL = 6 * 60 * 60
UPDATE_TASK: asyncio.Task[Any] | None = None
try:
    with UPDATE_CACHE_FILE.open(encoding="utf-8") as stream:
        UPDATE_CACHE = json.load(stream)
    if not isinstance(UPDATE_CACHE, dict):
        UPDATE_CACHE = {}
except (OSError, ValueError):
    UPDATE_CACHE = {}


def semantic_version(value: str):
    match = re.fullmatch(r"v?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-([0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?", value.strip())
    if not match:
        return None
    return tuple(int(match[i]) for i in (1, 2, 3)), match[4].split(".") if match[4] else []


def version_is_newer(remote: str, current: str = VERSION) -> bool:
    left, right = semantic_version(remote), semantic_version(current)
    if left is None or right is None:
        return False
    if left[0] != right[0]:
        return left[0] > right[0]
    a, b = left[1], right[1]
    if not a or not b:
        return bool(b) and not a
    for x, y in zip(a, b):
        if x == y:
            continue
        if x.isdigit() and y.isdigit():
            return int(x) > int(y)
        if x.isdigit() != y.isdigit():
            return not x.isdigit()
        return x > y
    return len(a) > len(b)


async def refresh_update_cache() -> None:
    global UPDATE_CACHE
    updated = dict(UPDATE_CACHE)
    updated["attempt_at"] = time.time()
    try:
        async with httpx.AsyncClient(timeout=6, verify=certifi.where()) as client:
            async with client.stream("GET", "https://api.github.com/repos/4geg/1ofd-fiscal-docs/releases/latest", headers={"Accept": "application/vnd.github+json", "User-Agent": "1OFD-Fiscal-Docs/" + VERSION}) as response:
                response.raise_for_status()
                body = bytearray()
                async for chunk in response.aiter_bytes(16384):
                    if len(body) + len(chunk) > 2 * 1024 * 1024:
                        raise ValueError("Слишком большой ответ проверки версии")
                    body.extend(chunk)
        release = json.loads(body)
        if not isinstance(release, dict):
            raise ValueError("Некорректный ответ GitHub")
        tag = str(release.get("tag_name") or "")
        if not release.get("draft") and not release.get("prerelease") and semantic_version(tag):
            updated.update(latest_version=tag, checked_at=time.time())
    except (httpx.HTTPError, ValueError, TypeError):
        # A network failure never becomes a red error badge or an alert.
        pass
    UPDATE_CACHE = updated
    def save() -> None:
        try:
            temporary = UPDATE_CACHE_FILE.with_suffix(".tmp")
            with temporary.open("w", encoding="utf-8") as stream:
                json.dump(updated, stream, ensure_ascii=False)
            temporary.replace(UPDATE_CACHE_FILE)
        except OSError:
            pass
    await asyncio.to_thread(save)


@app.get("/api/update-check")
async def update_check() -> dict[str, Any]:
    global UPDATE_TASK
    if (UPDATE_TASK is None or UPDATE_TASK.done()) and time.time() - float(UPDATE_CACHE.get("attempt_at") or 0) >= UPDATE_INTERVAL:
        UPDATE_TASK = asyncio.create_task(refresh_update_cache(), name="github-release-check")
    tag = str(UPDATE_CACHE.get("latest_version") or "")
    return {"latest_version": tag, "available": version_is_newer(tag), "checking": UPDATE_TASK is not None and not UPDATE_TASK.done()}


LOCAL_PAGE_LOCK = asyncio.Semaphore(1)


@app.post("/api/result-page")
async def result_page(filters: LocalFilterRequest, request: Request) -> dict[str, Any]:
    # Repeated typing must not leave multiple full scans running in parallel.
    async with LOCAL_PAGE_LOCK:
        if await request.is_disconnected():
            raise HTTPException(status_code=499, detail="Запрос страницы отменён")
        cancelled = threading.Event()
        task = asyncio.create_task(asyncio.to_thread(query_local_page, filters, cancelled))
        try:
            while not task.done():
                await asyncio.wait({task}, timeout=0.25)
                if await request.is_disconnected():
                    cancelled.set()
            return await task
        except sqlite3.OperationalError as error:
            if cancelled.is_set():
                raise HTTPException(status_code=499, detail="Запрос страницы отменён") from error
            raise
        finally:
            cancelled.set()


@app.get("/api/document")
async def document_detail(store_id: str, row_id: int | None = None, doc: str | None = None) -> dict[str, Any]:
    try:
        rows = DiskRows(store_id)
        if not rows.path.is_file():
            raise HTTPException(status_code=404, detail="Результат уже заменён")
        row = await asyncio.to_thread(rows.detail, rid=row_id, document_key=doc)
        if row is None:
            raise HTTPException(status_code=404, detail="Фискальный документ не найден")
        return row
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/api/export.{format_name}")
async def export_result(format_name: Literal["csv", "json"], request: Request) -> StreamingResponse:
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > 16 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="Слишком большой набор фильтров")
        body.extend(chunk)
    try:
        filters = parse_qs(body.decode("utf-8"))["filters"][0]
        query = LocalFilterRequest.model_validate_json(filters)
        # Validate before the download headers are sent.
        with closing(local_connection(query)):
            pass
    except (ValueError, KeyError) as error:
        raise HTTPException(status_code=400, detail="Не удалось прочитать фильтры выгрузки") from error
    period = (LAST_RESPONSE or {}).get("period") or {}
    dates = [str(period.get(key) or "") for key in ("from", "to")]
    filename = "ofd_" + ("_".join(dates) if all(re.fullmatch(r"\d{4}-\d{2}-\d{2}", date) for date in dates) else "last_result") + "." + format_name
    return StreamingResponse(stream_export(query, format_name), media_type="text/csv" if format_name == "csv" else "application/json", headers={"Content-Disposition": 'attachment; filename="' + filename + '"', "X-Content-Type-Options": "nosniff"})


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
    TOKEN_CACHE.pop(profile_id, None)
    return list_profiles()


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return HTML


@app.get("/api/status")
async def status() -> dict[str, Any]:
    return dict(STATE)


class LoadSettingsRequest(BaseModel):
    profile: str | None = None
    values: dict[str, Any] | None = None
    model_config = {"extra": "forbid"}


def load_settings_payload() -> dict[str, Any]:
    return {"profile": get_load_profile(), "profiles": LOAD_PROFILES,
            "labels": MANUAL_SETTING_LABELS, "ranges": MANUAL_SETTING_RANGES,
            "storage_modes": STORAGE_MODE_NAMES}


@app.get("/api/load-settings")
def load_settings() -> dict[str, Any]:
    return load_settings_payload()


@app.post("/api/load-settings")
def save_load_settings(request: LoadSettingsRequest) -> dict[str, Any]:
    try:
        if request.profile is not None and request.values is None:
            set_runtime_setting("load_profile", request.profile)
        elif request.profile is None and request.values is not None:
            set_manual_settings(request.values)
        else:
            raise ValueError("Выбери пресет или сохрани ручные значения")
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    return load_settings_payload()


@app.get("/api/session")
async def session() -> dict[str, Any]:
    """Состояние UI, независимое от жизненного цикла вкладки браузера."""
    return {
        "status": dict(STATE),
        "request": ACTIVE_REQUEST,
        "result": LAST_RESPONSE,
        "load_profile": get_load_profile(),
    }


async def _run_search_background(request: SearchRequest) -> None:
    global ACTIVE_SEARCH_TASK, ACTIVE_REQUEST, LAST_RESPONSE
    try:
        async with SEARCH_LOCK:
            set_state(
                running=True,
                stage="Подготовка",
                done=0,
                total=0,
                found=0,
                errors=0,
                message="",
                cancelled=False,
                error="",
            )
            try:
                previous_store_id = (LAST_RESPONSE or {}).get("store_id")
                payload = await perform_search(request)
                LAST_RESPONSE = payload
                if await asyncio.to_thread(persist_search_state):
                    await asyncio.to_thread(cleanup_result_stores, previous_store_id)
            except asyncio.CancelledError:
                set_state(
                    stage="Остановлено",
                    message="Запрос остановлен пользователем",
                    cancelled=True,
                    error="",
                )
            except ValueError as error:
                set_state(stage="Ошибка", message=str(error), error=str(error))
            except httpx.HTTPStatusError as error:
                detail = error.response.text[:1000] if error.response is not None else str(error)
                message = f"Ошибка Первого ОФД: {detail}"
                set_state(stage="Ошибка", message=message, error=message)
                LOGGER.exception("Ошибка HTTP при поиске документов")
            except Exception as error:
                message = safe_error_text(error)
                set_state(stage="Ошибка", message=message, error=message)
                LOGGER.exception("Ошибка фонового поиска документов")
            finally:
                try:
                    await asyncio.to_thread(close_search_store)
                finally:
                    STATE["running"] = False
    finally:
        ACTIVE_REQUEST = None
        ACTIVE_SEARCH_TASK = None


@app.post("/api/search")
async def search(request: SearchRequest) -> dict[str, Any]:
    global ACTIVE_SEARCH_TASK, ACTIVE_REQUEST
    if ACTIVE_SEARCH_TASK is not None and not ACTIVE_SEARCH_TASK.done():
        raise HTTPException(status_code=409, detail="Поиск уже выполняется")
    if SEARCH_LOCK.locked():
        raise HTTPException(status_code=409, detail="Поиск уже выполняется")

    ACTIVE_REQUEST = request.model_dump()
    set_state(
        running=True,
        stage="Запуск",
        done=0,
        total=0,
        found=0,
        errors=0,
        message="",
        cancelled=False,
        error="",
    )
    ACTIVE_SEARCH_TASK = asyncio.create_task(
        _run_search_background(request),
        name="ofd-search",
    )
    return {"started": True, "message": "Запрос запущен"}


@app.post("/api/stop-search")
async def stop_search() -> dict[str, Any]:
    task = ACTIVE_SEARCH_TASK
    if task is None or task.done():
        return {"stopping": False, "message": "Активного запроса нет"}
    set_state(stage="Остановка", message="Останавливаю запрос…", cancelled=True)
    task.cancel("user_stop")
    return {"stopping": True, "message": "Остановка запроса запрошена"}


async def _run_retry_background() -> None:
    global ACTIVE_SEARCH_TASK, ACTIVE_REQUEST, LAST_RESPONSE
    try:
        async with SEARCH_LOCK:
            set_state(
                running=True,
                stage="Повтор",
                done=0,
                total=0,
                found=0,
                errors=0,
                message="",
                cancelled=False,
                error="",
            )
            try:
                previous_store_id = (LAST_RESPONSE or {}).get("store_id")
                payload = await retry_last_failed()
                LAST_RESPONSE = payload
                if await asyncio.to_thread(persist_search_state):
                    await asyncio.to_thread(cleanup_result_stores, previous_store_id)
            except asyncio.CancelledError:
                set_state(
                    stage="Остановлено",
                    message="Повторный запрос остановлен пользователем",
                    cancelled=True,
                    error="",
                )
            except ValueError as error:
                set_state(stage="Ошибка", message=str(error), error=str(error))
            except Exception as error:
                message = safe_error_text(error)
                set_state(stage="Ошибка", message=message, error=message)
                LOGGER.exception("Ошибка фонового повтора запросов")
            finally:
                try:
                    await asyncio.to_thread(close_search_store)
                finally:
                    STATE["running"] = False
    finally:
        ACTIVE_REQUEST = None
        ACTIVE_SEARCH_TASK = None


@app.post("/api/retry-failed")
async def retry_failed() -> dict[str, Any]:
    global ACTIVE_SEARCH_TASK, ACTIVE_REQUEST
    if ACTIVE_SEARCH_TASK is not None and not ACTIVE_SEARCH_TASK.done():
        raise HTTPException(status_code=409, detail="Другой запрос уже выполняется")
    if SEARCH_LOCK.locked():
        raise HTTPException(status_code=409, detail="Другой запрос уже выполняется")
    if not (LAST_SEARCH.get("meta") or {}):
        raise HTTPException(status_code=400, detail="Сначала выполни обычный запрос")
    if not (LAST_SEARCH.get("failed_tasks") or []):
        raise HTTPException(status_code=400, detail="Неудачных запросов для повторения нет")

    ACTIVE_REQUEST = dict((LAST_SEARCH.get("meta") or {}).get("request") or {})
    set_state(
        running=True,
        stage="Повтор",
        done=0,
        total=0,
        found=0,
        errors=0,
        message="",
        cancelled=False,
        error="",
    )
    ACTIVE_SEARCH_TASK = asyncio.create_task(
        _run_retry_background(),
        name="ofd-retry",
    )
    return {"started": True, "message": "Повтор запущен"}


HTML = r"""
<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Первый ОФД — фискальные документы · 1.7.6</title>
<link rel="icon" href="/favicon.ico">
<style>
#loadManualFields{display:grid;grid-template-columns:repeat(auto-fit,minmax(235px,1fr));gap:16px;margin-top:18px}
#loadSettingsModal .field label{display:block;margin-bottom:8px}
#loadSettingsModal input{width:100%;box-sizing:border-box}


@font-face{font-family:'Noto Sans OFD';src:url('/static/fonts/NotoSans-Variable.ttf') format('truetype');font-weight:100 900;font-style:normal;font-display:swap}
:root{
  --font-ui:'Noto Sans OFD','Segoe UI',Arial,sans-serif;
  --font-heading:var(--font-ui);
  --font-mono:Consolas,'Courier New',monospace;
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
  font-family:var(--font-ui);
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
.brand{font-size:18px;font-weight:600;letter-spacing:.2px}
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
  grid-template-columns:240px 180px minmax(180px,1fr) 72px 180px;
  gap:12px 16px;
  align-items:end;
  padding:16px 20px 18px;
}
.panel-main>.panel-name{grid-column:1;grid-row:1}
.panel-main>.field:nth-child(2){grid-column:2;grid-row:1}
.panel-main>.field:nth-child(3){grid-column:3;grid-row:1;max-width:180px}
.panel-main>.token-field{grid-column:1/4;grid-row:2}
.panel-main>.action-field:not(.main-action){grid-column:4;grid-row:2}
.panel-main>.main-action{grid-column:5;grid-row:2}
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
.field label,.label-row label{display:block;color:var(--muted);font-size:12px;margin:0 0 0 6px;font-weight:600;line-height:1.2;min-height:22px;display:flex;align-items:flex-end}
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
  background-color:var(--surface-2);
  box-shadow:0 0 0 3px rgba(0,119,182,.12), var(--shadow-soft);
}
.date-row{display:grid;grid-template-columns:1fr 104px;gap:8px}
.btn{
  height:50px;
  background:var(--surface);
  color:var(--blue-deep);
  padding:0 18px;
  cursor:pointer;
  font-weight:600;
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
.btn.mini{height:36px;padding:0 14px;font-size:12px;font-weight:600;background:var(--surface-2)}
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
  min-width:0;
  width:100%;
  height:100%;
  min-height:30px;
  font:inherit;
  background:transparent;
  color:var(--text);
}
.token-entry{display:grid;grid-template-columns:minmax(0,1fr);align-items:center;flex:1;min-width:0}
.token-entry>.token-input,.token-entry>.token-hint{grid-area:1/1}
.token-entry>.token-input::placeholder{color:transparent}
.token-hint{padding:5px 2px;color:#72889a;font-size:14px;line-height:1.45;white-space:normal;overflow-wrap:anywhere;pointer-events:none}
.token-input:not(:placeholder-shown)+.token-hint{visibility:hidden}
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
  font-weight:600;
  cursor:help;
  box-shadow:0 5px 14px rgba(0,119,182,.12);
}
.inline-help:hover,.inline-help:focus-visible{background:#f7fbff;border-color:var(--blue)}
.inline-help[data-tip]::after,.ui-tooltip[data-tip]::after{
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
.inline-help[data-tip]:hover::after,.inline-help[data-tip]:focus-visible::after,.ui-tooltip[data-tip]:hover::after,.ui-tooltip[data-tip]:focus-visible::after{
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
.advanced-title{font-size:13px;font-weight:600;margin-bottom:12px;color:var(--blue-deep)}
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
.export-actions{display:flex;align-items:center;gap:8px}
.export-btn{height:38px;border:0;border-radius:999px;background:var(--surface-2);color:var(--blue-deep);padding:0 13px 0 15px;display:inline-flex;align-items:center;gap:8px;font:inherit;font-size:12px;font-weight:600;cursor:pointer;box-shadow:var(--shadow-soft);transition:background .12s ease,transform .12s ease,color .12s ease}
.export-btn:hover{background:var(--blue-pale);color:var(--blue);transform:translateY(-1px)}
.export-btn:active{transform:translateY(0)}
.export-btn svg{width:16px;height:16px;fill:none;stroke:currentColor;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}
.table-wrap{overflow:auto;max-height:64vh}
table{border-collapse:separate;border-spacing:0 8px;width:100%;min-width:1850px;padding:0 10px 8px}
th,td{padding:12px 10px;text-align:left;white-space:nowrap}
th{
  background:#edf6fb;
  position:sticky;
  top:0;
  z-index:4;
  font-weight:600;
  color:#476179;
  cursor:pointer;
  user-select:none;
  box-shadow:inset 0 -1px 0 rgba(71,97,121,.10),0 5px 10px rgba(22,50,74,.04);
}
thead th:first-child{border-top-left-radius:18px;border-bottom-left-radius:18px}
thead th:last-child{border-top-right-radius:18px;border-bottom-right-radius:18px}
th:hover{color:var(--blue-deep)}
tbody tr{cursor:pointer;transition:transform .1s ease,filter .1s ease}
tbody tr td{background:#f7fbfd}
tbody tr td:first-child{border-top-left-radius:22px;border-bottom-left-radius:22px}
tbody tr td:last-child{border-top-right-radius:22px;border-bottom-right-radius:22px}
tbody tr:hover{transform:translateY(-1px)}
tbody tr:hover td{background:#edf7fd}
.muted{color:var(--muted)}
.type{font-weight:600}
.type,.type-pill,.detail-type-chip{color:var(--type-color);background-color:var(--type-tint)}
.type{display:inline-block;padding:5px 9px;border-radius:999px}
.check.document-type{color:var(--type-color)}
.check.document-type input{accent-color:var(--type-color)}
.status-ok{color:var(--green);font-weight:600}
.status-error{color:var(--red);font-weight:600}
.empty{padding:58px 20px;text-align:center;color:var(--muted)}
.pager{display:flex;align-items:center;justify-content:center;gap:10px;padding:12px 16px 16px;flex-wrap:wrap}
.pager button{border:0;background:var(--surface);color:var(--blue-deep);cursor:pointer;padding:10px 14px;border-radius:999px;box-shadow:var(--shadow-soft);font-weight:600}
.pager button:disabled{color:#b5c4d0;cursor:default;box-shadow:none;background:#eef3f6}
.page-size{margin-left:auto;display:flex;align-items:center;gap:8px;color:var(--muted);font-weight:600}
.page-size select{height:40px;background-color:var(--surface);padding:0 40px 0 12px;background-position:right 14px center}
.modal{display:none;position:fixed;inset:0;background:rgba(12,19,29,.36);z-index:100;align-items:center;justify-content:center;padding:20px}
.modal.show{display:flex}
.modal-box{width:min(1240px,97vw);max-height:92vh;overflow:hidden;background:#fff;border-radius:30px;box-shadow:0 26px 70px rgba(22,57,85,.22);display:flex;flex-direction:column}
.modal-title{
  padding:20px 24px 8px;
  display:flex;
  align-items:center;
  justify-content:space-between;
  font-size:18px;
  font-weight:600;
  color:var(--blue-deep);
  flex:0 0 auto;
}
.detail-title-wrap{display:flex;align-items:center;gap:12px;flex-wrap:wrap}
.detail-title-actions{display:flex;align-items:center;gap:8px;margin-left:auto}
.detail-open-tab{display:inline-flex;align-items:center;gap:7px;height:40px;padding:0 13px;background:var(--surface-2)}
.detail-open-tab svg{width:16px;height:16px;fill:none;stroke:currentColor;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}
.detail-type-chip{display:inline-flex;align-items:center;gap:7px;padding:7px 12px;border-radius:999px;font-size:12px;font-weight:600}
.detail-type-chip:before{content:'';width:8px;height:8px;border-radius:50%;background:var(--type-color)}
.close-x{border:0;background:var(--surface);width:44px;height:44px;border-radius:999px;font-size:22px;cursor:pointer;color:var(--muted);box-shadow:var(--shadow-soft)}
.modal-body{padding:8px 14px 20px 20px;margin-right:6px;overflow-y:auto;overflow-x:hidden;max-height:calc(92vh - 72px);scrollbar-gutter:stable;overscroll-behavior:contain}
.modal-body::-webkit-scrollbar{width:10px}
.modal-body::-webkit-scrollbar-track{background:transparent}
.modal-body::-webkit-scrollbar-thumb{background:#c6d9e6;border:2px solid #fff;border-radius:999px}
.modal-body::-webkit-scrollbar-thumb:hover{background:#a9c7da}
.details-grid{display:flex;flex-direction:column;gap:12px;margin-bottom:13px}
.detail-section{background:linear-gradient(180deg,#eef8fe 0%,#e5f3fb 100%);padding:12px 14px 14px;border-radius:26px}
.detail-section-title{display:flex;align-items:center;gap:8px;color:var(--blue-deep);font-size:14px;font-weight:600;margin:0 0 10px 2px}
.detail-section-title .section-dot{width:26px;height:26px;border-radius:999px;background:rgba(255,255,255,.75);display:flex;align-items:center;justify-content:center;color:var(--blue)}
.detail-section-grid{display:grid;grid-template-columns:repeat(12,minmax(0,1fr));gap:10px}
.detail{position:relative;grid-column:span 3;padding:11px 13px;min-height:64px;overflow:hidden;background:rgba(255,255,255,.76);border-radius:19px;transition:background .12s ease,transform .12s ease,box-shadow .12s ease}
.detail.wide{grid-column:span 4}
.detail.xwide{grid-column:span 6}
.detail.full{grid-column:1/-1}
.detail small{display:block;color:var(--muted);margin-bottom:5px;font-weight:600}
.detail-value{overflow-wrap:anywhere;word-break:break-word;line-height:1.35}
.detail.copyable{cursor:pointer;user-select:none}
.detail.copyable:hover,.detail-subfield.copyable:hover{background:#fff;box-shadow:0 7px 18px rgba(30,95,140,.09)}
.detail.copyable:active,.detail-subfield.copyable:active{transform:scale(.992)}
.detail.copied,.detail-subfield.copied{background:var(--green-soft)!important;box-shadow:none!important}
.detail.copied:after,.detail-subfield.copied:after{content:'Скопировано';position:absolute;right:10px;top:8px;font-size:10px;font-weight:600;color:var(--green);background:#fff;padding:3px 7px;border-radius:999px}
.detail-copy{position:absolute;right:10px;top:50%;transform:translateY(-50%);width:32px;height:32px;border:0;border-radius:999px;background:var(--surface-2);color:var(--blue);cursor:pointer;display:flex;align-items:center;justify-content:center;transition:background .12s ease,color .12s ease,transform .12s ease}
.detail-copy:hover{background:var(--blue-pale);color:var(--blue-deep);transform:translateY(-50%) scale(1.04)}
.detail-copy svg{width:16px;height:16px;fill:none;stroke:currentColor;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}
.detail-copy.copied{background:var(--green-soft);color:var(--green)}
.detail-datetime{grid-column:span 4;padding:11px 12px;overflow:visible}
.detail-datetime-title{display:block;color:var(--muted);margin-bottom:7px;font-weight:600;font-size:11px}
.detail-datetime-fields{display:grid;grid-template-columns:1.35fr .8fr;gap:7px}
.detail-subfield{position:relative;min-height:46px;padding:7px 10px;background:rgba(255,255,255,.66);border-radius:15px;display:flex;flex-direction:column;justify-content:center;transition:background .12s ease,transform .12s ease,box-shadow .12s ease}
.detail-subfield small{margin:0 0 2px;color:var(--muted);font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:.04em}
.detail-address{padding-right:52px}
.raw-details{margin-top:13px;background:var(--surface);border-radius:22px;overflow:hidden;box-shadow:var(--shadow-soft)}
.extra-details .details-grid{padding:0 12px 12px;margin:0}
.extra-details .detail-section{box-shadow:none}
.raw-details summary{list-style:none;cursor:pointer;padding:14px 16px;display:flex;align-items:center;justify-content:space-between;font-weight:600;color:var(--blue-deep);user-select:none}
.raw-details summary::-webkit-details-marker{display:none}
.raw-details summary:after{content:'⌄';font-size:18px;transition:transform .15s ease}
.raw-details[open] summary:after{transform:rotate(180deg)}
.raw-content{padding:0 12px 12px}
.raw-actions{display:flex;justify-content:flex-end;margin:0 0 8px}
pre{margin:0;background:#112031;color:#d8e2ec;border-radius:18px;padding:16px;overflow:auto;max-height:420px;font-family:var(--font-mono);font-size:12px}
button,input,select,textarea{font-family:inherit}
b,strong{font-weight:600}
body{font-weight:400;font-synthesis:none}
.brand,.panel-name b,th,.advanced-title,.modal-title,.receipt-heading,.receipt-section-title{font-family:var(--font-heading);font-weight:600}
#detailsModal .modal-box{width:min(880px,97vw);border:2px solid var(--type-color)}
.receipt{max-width:700px;width:100%;margin:12px auto 0;padding:20px 24px;color:#243242;font-family:var(--font-ui);font-size:17px;line-height:1.5}
.receipt-heading{margin:0 0 20px;text-align:center;font-size:24px;line-height:1.3;font-weight:600;text-transform:uppercase;color:var(--type-color);overflow-wrap:anywhere}
.receipt-section{padding:12px 0;border-top:1px dashed #c3cdd5}
.receipt-section-title{text-align:center;margin:0 0 7px;font-size:16px;font-weight:600;color:#556675}
.receipt-row{position:relative;display:grid;grid-template-columns:minmax(0,44%) minmax(0,1fr);gap:12px;padding:7px 2px;font-size:17px;line-height:1.5;border-radius:4px;overflow-wrap:anywhere}
.receipt-label{color:#526271}.receipt-value{text-align:right;min-width:0;font-size:21px;font-variant-numeric:tabular-nums}
.receipt-row.copyable{cursor:pointer}.receipt-row.copyable:hover{background:#eef4f8}
.receipt-row.copied{background:var(--green-soft)}.receipt-total,.receipt-total .receipt-value{font-size:25px;font-weight:600}
.receipt-address,.receipt-address *{user-select:text;cursor:text}
.receipt-extra{max-width:700px;width:100%;margin:auto;padding:0 12px 12px}
.csv-note{font-size:13px;color:var(--muted);margin:12px 0;line-height:1.5}
.csv-report{max-height:260px;overflow:auto;white-space:pre-wrap;background:var(--surface);padding:14px;border-radius:18px;font-size:13px;overflow-wrap:anywhere}
.csv-controls{display:flex;gap:12px;flex-wrap:wrap;margin:14px 0;align-items:center}
@media(max-width:620px){
  .receipt{padding:12px 4px}.receipt-heading{font-size:22px}
  .receipt-row{grid-template-columns:minmax(0,40%) minmax(0,1fr);gap:8px;font-size:15px}
  .receipt-value{font-size:17px}.receipt-total,.receipt-total .receipt-value{font-size:21px}
  #detailsModal .modal-title{flex-wrap:wrap;gap:8px;padding:16px 18px 8px}
  #detailsModal .detail-title-wrap{flex:1 1 100%;min-width:0}
  #detailsModal .detail-title-actions{margin-left:0;width:100%;justify-content:space-between}
  #detailsModal .close-x{flex-shrink:0}
}
.copy-toast{position:fixed;left:50%;bottom:28px;transform:translate(-50%,16px);z-index:250;background:#10283d;color:#fff;padding:9px 15px;border-radius:999px;font-size:12px;font-weight:600;opacity:0;pointer-events:none;transition:opacity .14s ease,transform .14s ease;box-shadow:0 10px 30px rgba(0,0,0,.18)}
.copy-toast.show{opacity:1;transform:translate(-50%,0)}
@media(max-width:900px){.detail,.detail.wide,.detail.xwide,.detail-datetime{grid-column:span 6}.detail-section-grid{grid-template-columns:repeat(12,minmax(0,1fr))}}
@media(max-width:620px){.detail,.detail.wide,.detail.xwide,.detail-datetime{grid-column:1/-1}}
.failure-list{max-height:440px;overflow:auto;background:var(--bg-soft);border-radius:var(--radius-lg);padding:10px}
.failure-row{padding:11px 12px;background:#fff;margin-bottom:8px}
.failure-row:last-child{margin-bottom:0}
.failure-row code{font-family:var(--font-mono);font-size:12px}
#failedDetailsBtn,#catalogRetryBtn,#retryBtn{margin-left:8px}

.topbar-actions{margin-left:auto;display:flex;align-items:center;gap:10px}
.version-button{display:inline-flex;align-items:center;gap:6px;text-decoration:none}.version-chip.version-button.update-available{background:var(--red);color:#fff}.version-alert{font-size:14px;line-height:1;font-weight:800}
.version-chip{font-size:11px;color:var(--muted);background:var(--surface);padding:7px 10px;border-radius:999px;font-weight:600}
.reset-group{display:inline-flex;align-items:center;gap:8px}.reset-caption{color:var(--muted);font-size:12px;font-weight:600}
.ui-tooltip{position:relative;z-index:12}.reset-arrow{font-size:18px;font-weight:800;font-synthesis:weight;line-height:1}
.reset-control{display:inline-flex;flex-shrink:0;border:1px solid var(--blue-pale);border-radius:999px;background:var(--surface-2)}
.reset-control button{display:inline-flex;align-items:center;gap:5px;border:0;background:transparent;color:var(--blue-deep);padding:9px 12px;font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap}
.reset-control button+button{border-left:1px solid var(--blue-pale)}
.reset-control button:first-child{border-radius:999px 0 0 999px}.reset-control button:last-child{border-radius:0 999px 999px 0}
.reset-control button:hover,.reset-control button:focus-visible{background:var(--blue-pale)}
.reset-control #resetEverythingBtn{background:var(--blue-pale)}
.filter-csv-actions{display:flex;align-items:center;flex-wrap:wrap;gap:8px;margin-top:16px}
#emptyText{display:block}
.empty .restore-result{margin:12px auto 0}
.api-key-btn{border:0;background:var(--surface-2);color:var(--blue-deep);border-radius:999px;height:38px;padding:0 14px;font-weight:600;cursor:pointer;box-shadow:var(--shadow-soft);max-width:360px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.api-key-btn.active{background:var(--blue);color:#fff}
.key-list{display:flex;flex-direction:column;gap:10px;margin:10px 0 16px}
.key-row{display:flex;align-items:center;gap:12px;background:var(--surface);padding:12px 14px;border-radius:22px}
.key-main{flex:1;min-width:0}.key-main b{display:block;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.key-main small{color:var(--muted)}
.key-secret-line{display:flex;align-items:center;gap:7px;margin-top:3px;min-height:24px}
.key-secret-value{font-family:var(--font-mono);font-size:12px;color:var(--muted);word-break:break-all}
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
  .panel-main{grid-template-columns:230px 160px minmax(160px,1fr) 64px 160px;column-gap:12px}
  .panel-main>.field:nth-child(3){max-width:160px}
  .panel-main>.action-field .btn.icon{width:100%}
  .advanced-grid{grid-template-columns:repeat(3,1fr)}
  .summary{grid-template-columns:repeat(4,1fr)}
}
@media(max-width:1040px){
  .page{padding:12px}
  .panel-main{grid-template-columns:1fr 1fr}
  .panel-main>.panel-name{grid-column:1/-1;grid-row:auto}
  .panel-main>.field:nth-child(2){grid-column:1;grid-row:auto}
  .panel-main>.field:nth-child(3){grid-column:2;grid-row:auto;max-width:none}
  .panel-main>.token-field{grid-column:1/-1;grid-row:auto}
  .panel-main>.action-field:not(.main-action){grid-column:1;grid-row:auto}
  .panel-main>.main-action{grid-column:2;grid-row:auto}
  .advanced-grid,.details-grid{grid-template-columns:1fr 1fr}
  .summary{grid-template-columns:repeat(2,1fr)}
  .status-text{min-width:0}
  .token-list{max-width:100%}
  .completeness,.statusbar,.advanced-footer,.table-meta{flex-direction:column;align-items:flex-start}
  .page-size{margin-left:0}
}
@media(max-width:640px){
  .detail-datetime{grid-column:span 1}
  .detail-datetime-fields{grid-template-columns:1fr}
  .topbar{padding:0 14px;height:auto;min-height:62px;flex-wrap:wrap;padding-top:10px;padding-bottom:10px}
  .advanced-grid,.details-grid{grid-template-columns:1fr}
  .summary{grid-template-columns:1fr}
}


/* Icon ink follows the original blue palette, with heavier glyphs/strokes only. */
.btn.icon,.token-add,.token button,.inline-help,.close-x,.pager button{font-weight:800;font-synthesis:weight}
.btn.icon,.token button,.close-x{color:var(--blue-deep)}
.btn.icon.open{color:#fff}
.inline-help{color:var(--blue)}
.token-add{color:#fff}
.pager button:not(:disabled){color:var(--blue-deep)}
.raw-details summary:after{color:var(--blue-deep);font-weight:800;font-synthesis:weight}
.export-btn svg,.detail-open-tab svg,.key-reveal svg,.secret-toggle svg{stroke-width:2.2}
.btn.mini[href]{text-decoration:none;display:inline-flex;align-items:center;justify-content:center}
.topbar{height:auto;min-height:62px;padding-top:8px;padding-bottom:8px;flex-wrap:wrap}.topbar-actions{flex-wrap:wrap}
@media(min-width:1850px){
  .panel-main{grid-template-columns:220px 160px 160px minmax(0,1fr) 64px 160px;column-gap:12px}
  .panel-main>.token-field{grid-column:4;grid-row:1}
  .panel-main>.action-field:not(.main-action){grid-column:5;grid-row:1}
  .panel-main>.main-action{grid-column:6;grid-row:1}
  .panel-main>.action-field .btn.icon{width:100%}
}

</style>
</head>
<body>
<div class="topbar"><div class="brand">Первый ОФД <span>• Фискальные документы</span></div><div class="topnote">локальное веб-приложение • Universal API</div><div class="topbar-actions"><a id="versionBtn" class="version-chip version-button ui-tooltip" href="https://github.com/4geg/1ofd-fiscal-docs" target="_blank" rel="noopener" data-tip="Открыть GitHub">v1.7.7<span id="versionAlert" class="version-alert" style="display:none">!</span></a><div class="reset-group"><span class="reset-caption">Сброс</span><div class="reset-control" role="group" aria-label="Сброс фильтров и результата"><button id="resetAllFiltersBtn" class="ui-tooltip" type="button" data-tip="Сбросить фильтры" aria-label="Сбросить фильтры"><span class="reset-arrow" aria-hidden="true">↻</span>Фильтры</button><button id="resetEverythingBtn" class="ui-tooltip" type="button" data-tip="Сбросить фильтры и результат" aria-label="Сбросить фильтры и результат"><span class="reset-arrow" aria-hidden="true">↻</span>Всё</button><button id="resetLoadedResultBtn" class="ui-tooltip" type="button" data-tip="Сбросить результат" aria-label="Сбросить результат"><span class="reset-arrow" aria-hidden="true">↻</span>Результат</button></div></div><button id="apiKeyBtn" class="api-key-btn">API-ключ</button></div></div>
<div class="page">

  <section class="panel" id="apiPanel">
    <div class="panel-main">
      <div class="panel-name"><b>Настройки запроса по API</b></div>
      <div class="field"><label>Период с</label><input id="dateFrom" type="date" class="input"></div>
      <div class="field"><label>Период по</label><input id="dateTo" type="date" class="input"></div>
      <div class="field token-field"><label>Фильтр значений при запросе по API</label><div class="tokenbox"><div id="apiTokens" class="token-list"></div><div class="token-entry"><input id="apiTokenInput" class="token-input" aria-label="Фильтр запроса к API" placeholder="РНМ, ЗН ККТ/ФН, название или адрес · несколько значений через «;» · Enter — добавить" title="РНМ, ЗН ККТ/ФН, название или адрес · несколько значений через «;» · Enter — добавить"><span class="token-hint" aria-hidden="true">РНМ, ЗН ККТ/ФН, название или адрес · несколько значений через «;» · Enter — добавить</span></div><button class="inline-help inside" type="button" data-tip="Можно добавить несколько значений. Например, если ввести три РНМ, программа будет искать документы только по этим трём кассам. Допускаются РНМ, ЗН ККТ, ЗН ФН, название и адрес; для ФД, ФПД и точного типа идентификатора используй Умный CSV-фильтр. Вводи значения через ; и нажимай Enter: условия работают по ИЛИ, запятые сохраняются. Верхняя панель задаёт условия нового запроса к ОФД.">!</button><button id="apiTokenAdd" class="token-add" title="Добавить условие">+</button></div></div>
      <div class="field action-field"><label>&nbsp;</label><button id="apiAdvancedBtn" class="btn icon gear" title="Расширенные настройки">⚙&#xfe0e;</button></div>
      <div class="field action-field main-action"><label>&nbsp;</label><button id="searchBtn" class="btn primary">Запросить</button></div>
    </div>
    <div id="apiAdvanced" class="advanced">
      <div class="advanced-grid">
        <div class="field"><label>Время с</label><input id="timeFrom" type="time" class="input" value="00:00"></div>
        <div class="field"><label>Время по</label><input id="timeTo" type="time" class="input" value="23:59"></div>
        <div class="field"><label>РНМ (точный поиск только по регистрационному номеру)</label><input id="qRnm" class="input" placeholder="целиком или часть"></div>
        <div class="field"><label>ЗН ККТ (точный поиск только по заводскому номеру)</label><input id="qKkt" class="input" placeholder="целиком или часть"></div>
        <div class="field"><label>ЗН ФН (точный поиск только по заводскому номеру ФН)</label><input id="qFn" class="input" placeholder="целиком или часть"></div>
        <div class="field"><label>Внутреннее имя в ОФД (точный поиск по наименованию ККТ в ОФД)</label><input id="qInternal" class="input" placeholder="АЗС №…, касса …"></div>
        <div class="field"><label>Место установки (точный поиск по наименованию места установки из данных регистрации ККТ)</label><input id="qRetail" class="input" placeholder="название или часть"></div>
        <div class="field"><label>Адрес установки (точный поиск по адресу места установки ККТ)</label><input id="qAddress" class="input" placeholder="адрес или часть"></div>
        <div class="field"><label>Номер смены</label><input id="qShift" class="input" inputmode="numeric" placeholder="например 415"></div>
        <div class="field"><label>Количество параллельных обращений к API</label><div class="control-with-help"><select id="qConcurrency" class="select"><option>60</option><option>80</option><option>100</option><option selected>120</option><option>150</option><option>180</option><option>200</option></select><button class="inline-help" type="button" data-tip="Большие значения ускоряют поиск, но заметно повышают нагрузку на API. Обычно лучше использовать средние значения: они дают хорошую скорость и снижают риск ошибок соединения. Дополнительный предел задаётся в трее: Настройки → Профиль нагрузки / Ручные настройки. Применяется меньшее из двух значений.">!</button></div></div>
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
      <div class="filter-csv-actions"><button class="btn mini" type="button" data-csv-target="api">Умный CSV-фильтр</button><a class="btn mini" href="/static/docs/csv_filter_guide.pdf" target="_blank" rel="noopener">Инструкция</a><a class="btn mini" href="/static/docs/csv_filter_template.csv" download="csv_filter_template.csv">Шаблон CSV</a></div>
      <div class="advanced-footer"><div id="apiFilterSummary" class="active-filters">Основные типы • весь день • 120 параллельных запросов</div><label class="check"><input id="qRefresh" type="checkbox"> обновить каталог ККТ перед запросом</label></div>
    </div>
  </section>

  <div id="statusbar" class="statusbar"><div class="spinner"></div><div class="status-text" id="statusText">Подготовка…</div><div class="progress"><div id="progressBar"></div></div><button id="stopSearchBtn" class="btn stop-search" type="button">Стоп</button></div>
  <div id="memoryWarning" class="heavy-note" style="display:none;margin:10px 12px">Большая выборка, возможна высокая нагрузка на ПК. Документы сохраняются на диске и показываются страницами.</div>
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
      <div class="field token-field"><label>Поиск по результатам</label><div class="tokenbox"><div id="resultTokens" class="token-list"></div><div class="token-entry"><input id="resultTokenInput" class="token-input" aria-label="Фильтр загруженного результата" placeholder="РНМ, ЗН ККТ/ФН, ФД/ФПД, название или адрес · несколько значений через «;» · Enter — добавить" title="РНМ, ЗН ККТ/ФН, ФД/ФПД, название или адрес · несколько значений через «;» · Enter — добавить"><span class="token-hint" aria-hidden="true">РНМ, ЗН ККТ/ФН, ФД/ФПД, название или адрес · несколько значений через «;» · Enter — добавить</span></div><button class="inline-help inside" type="button" data-tip="Можно добавить несколько значений. Например, если ввести три РНМ, программа покажет загруженные документы только по этим трём кассам. Допускаются РНМ, ЗН ККТ, ЗН ФН, ФД, ФПД, название и адрес; для точного типа идентификатора используй Умный CSV-фильтр. Вводи значения через ; и нажимай Enter: условия работают по ИЛИ, запятые сохраняются. Нижняя панель фильтрует уже загруженные документы без нового запроса к ОФД.">!</button><button id="resultTokenAdd" class="token-add" title="Добавить условие">+</button></div></div>
      <div class="field action-field"><label>&nbsp;</label><button id="resultAdvancedBtn" class="btn icon gear" title="Расширенные фильтры">⚙&#xfe0e;</button></div>
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
        <div class="field"><label>Внутреннее имя в ОФД (точный поиск по наименованию ККТ в ОФД)</label><input id="rInternal" class="input" placeholder="название или часть"></div>
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
      <div class="filter-csv-actions"><button class="btn mini" type="button" data-csv-target="result">Умный CSV-фильтр</button><a class="btn mini" href="/static/docs/csv_filter_guide.pdf" target="_blank" rel="noopener">Инструкция</a><a class="btn mini" href="/static/docs/csv_filter_template.csv" download="csv_filter_template.csv">Шаблон CSV</a></div>
      <div class="advanced-footer"><div id="resultFilterSummary" class="active-filters">Показываются все загруженные документы</div><button id="resetResultFilters" class="btn">Сбросить локальные фильтры</button></div>
    </div>
  </section>

  <div class="results">
    <div class="table-meta"><span id="tableInfo">Документов нет</span><div class="export-actions"><button id="csvBtn" class="export-btn" type="button"><span>CSV</span><svg viewBox="0 0 20 20" aria-hidden="true"><path d="M10 2.5v9"></path><path d="m6.5 8.5 3.5 3.5 3.5-3.5"></path><path d="M4 15.5h12"></path></svg></button><button id="jsonBtn" class="export-btn" type="button"><span>JSON</span><svg viewBox="0 0 20 20" aria-hidden="true"><path d="M10 2.5v9"></path><path d="m6.5 8.5 3.5 3.5 3.5-3.5"></path><path d="M4 15.5h12"></path></svg></button></div></div>
    <div class="table-wrap"><table><thead><tr>
      <th data-sort="date_sort">Дата и время ФД</th><th data-sort="kkm_internal_name">Внутреннее имя в ОФД</th><th data-sort="kkm_reg_id">РНМ</th><th data-sort="type">Тип ФД</th><th data-sort="shift">Смена</th><th data-sort="fd">ФД</th><th data-sort="amount">Сумма</th><th data-sort="fpd">ФПД</th><th data-sort="fs_number">ЗН ФН</th><th data-sort="kkm_factory_number">ЗН ККТ</th><th data-sort="fns_flc_status">Статус ФНС</th><th data-sort="inserted_at_sort">Поступил в ОФД</th><th data-sort="address">Адрес</th>
    </tr></thead><tbody id="rows"></tbody></table><div id="empty" class="empty"><span id="emptyText">Сначала выполни запрос в Первый ОФД.</span><button id="restoreLastResultBtn" class="btn mini restore-result" type="button" style="display:none">Восстановить последний результат</button></div></div>
    <div class="pager"><button id="firstPage" title="Первая страница" aria-label="Первая страница">«</button><button id="prevPage" title="Предыдущая страница" aria-label="Предыдущая страница">‹</button><span id="pageText">Страница 0 из 0</span><button id="nextPage" title="Следующая страница" aria-label="Следующая страница">›</button><button id="lastPage" title="Последняя страница" aria-label="Последняя страница">»</button><div class="page-size">Показывать по <select id="pageSize" class="select" style="width:92px;height:38px"><option>20</option><option selected>50</option><option>100</option><option>200</option><option value="all">1000</option></select></div></div>
  </div>
</div>

<div id="keyModal" class="modal"><div class="modal-box" style="width:min(760px,96vw)"><div class="modal-title">API-ключ Первого ОФД <button class="close-x" title="Закрыть" aria-label="Закрыть" data-close="keyModal">×</button></div><div class="modal-body"><div id="keyList" class="key-list"></div><div class="key-add-grid"><div class="field"><label>Добавить новый API-ключ</label><div class="secret-wrap"><input id="newApiKey" class="input" type="password" autocomplete="off" placeholder="Вставьте API-ключ"><button id="toggleApiKey" class="secret-toggle" type="button" title="Показать / скрыть API-ключ" aria-label="Показать или скрыть API-ключ"><svg class="eye-on" viewBox="0 0 24 24"><path d="M2.5 12s3.5-6 9.5-6 9.5 6 9.5 6-3.5 6-9.5 6-9.5-6-9.5-6Z"/><circle cx="12" cy="12" r="2.8"/></svg><svg class="eye-off" viewBox="0 0 24 24"><path d="m3 3 18 18"/><path d="M10.6 6.2A10.9 10.9 0 0 1 12 6c6 0 9.5 6 9.5 6a15.5 15.5 0 0 1-3.1 3.7"/><path d="M6.4 6.4C3.9 8.2 2.5 12 2.5 12s3.5 6 9.5 6c1.2 0 2.3-.2 3.3-.6"/></svg></button></div></div><button id="addApiKeyBtn" class="btn primary">Проверить и сохранить</button></div><div id="keyError" class="key-error"></div><div class="key-note">Ключ проверяется через Universal API и сохраняется только на этом компьютере. В Windows он хранится в зашифрованном виде через DPAPI.</div></div></div></div>
<div id="detailsModal" class="modal"><div class="modal-box"><div class="modal-title"><div class="detail-title-wrap"><span>Фискальный документ</span><span id="detailTypeChip" class="detail-type-chip">ФД</span></div><div class="detail-title-actions"><button id="openDetailTab" class="btn mini detail-open-tab" type="button" title="Открыть этот ФД в отдельной вкладке"><svg viewBox="0 0 20 20" aria-hidden="true"><path d="M11 3h6v6"></path><path d="M17 3 9.5 10.5"></path><path d="M15 11.5V16a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1V6a1 1 0 0 1 1-1h4.5"></path></svg><span>В новой вкладке</span></button><button class="close-x" title="Закрыть" aria-label="Закрыть" data-close="detailsModal">×</button></div></div><div class="modal-body"><div id="detailsGrid" class="details-grid"></div><details id="extraDetails" class="raw-details extra-details"><summary>Дополнительные данные ФД</summary><div id="extraDetailsGrid" class="details-grid"></div></details><details class="raw-details"><summary>Полный ответ API</summary><div class="raw-content"><div class="raw-actions"><button id="copyJson" class="btn mini">Копировать JSON</button></div><pre id="rawJson"></pre></div></details></div></div></div><div id="copyToast" class="copy-toast">Скопировано</div>
<div id="failuresModal" class="modal"><div class="modal-box"><div class="modal-title">Непроверенные элементы <button class="close-x" title="Закрыть" aria-label="Закрыть" data-close="failuresModal">×</button></div><div class="modal-body"><div id="failureSummary" style="margin-bottom:10px"></div><div id="failureList" class="failure-list"></div></div></div></div>

<div class="modal" id="loadSettingsModal">
<div class="modal-box" style="width:min(760px,97vw)">
<div class="modal-title"><b>Профиль нагрузки и ручные настройки</b><button class="close-x" data-close="loadSettingsModal" aria-label="Закрыть">×</button></div>
<div class="modal-body">
<p id="loadProfileName" class="csv-note"></p>
<div class="field"><label for="loadPreset">Профиль</label><select class="select" id="loadPreset"></select></div>
<button type="button" id="loadPresetApply" class="btn mini">Применить пресет</button>
<p class="csv-note">«Максимальный» может заметно увеличить расход памяти. Пресет заменяет ручные значения.</p>
<form id="loadSettingsForm"><div id="loadManualFields" class="advanced-grid"></div>
<p class="csv-note">Режим хранения относится к промежуточным ответам ОФД. Сохранённые документы всегда на диске. Когда лимит промежуточной памяти исчерпан, ответы продолжают записываться на диск. Лимит не ограничивает общую память программы.</p>
<p class="csv-note">Одновременных запросов — верхний предел. Значение в фильтре API может дополнительно уменьшить его. Активный поиск сохраняет прежние настройки; новые значения действуют со следующего поиска или добора.</p>
<p id="loadSettingsMessage" role="status" class="csv-note"></p>
<button class="btn" type="submit">Сохранить ручные настройки</button></form>
</div></div></div>
<div id="csvImportModal" class="modal"><div class="modal-box" style="width:min(760px,96vw)"><div class="modal-title">Умный CSV-фильтр <button class="close-x" title="Закрыть" aria-label="Закрыть" data-close="csvImportModal">×</button></div><div class="modal-body">
<p id="csvDestination" class="csv-note"></p>
<p class="csv-note">Три колонки: type, completeness, data. Типы: РНМ, ЗН ККТ, ЗН ФН, ФД, ФПД, Смена, Название, Место установки, Адрес. Полнота: full / полное или partial / частичное. Разделитель колонок — ; или запятая; значения с разделителем заключай в кавычки. Ведущие нули сохраняются, значения не преобразуются в числа.</p>
<div class="csv-controls"><input id="csvFile" type="file" accept=".csv,text/csv"><button id="csvApplyBtn" class="btn primary" type="button" disabled>Добавить условия</button></div>
<div id="csvReport" class="csv-report" role="status">Выбери CSV-файл. Импорт не запускает запрос к ОФД.</div>
</div></div></div>
<script>
const $=id=>document.getElementById(id);
const TYPE_LABELS={registration:'Регистрация',reregistration:'Перерегистрация',close:'Закрытие ФН',ticket:'Кассовый чек',open_shift:'Открытие смены',close_shift:'Закрытие смены',receipt_correction:'Чек коррекции',bso:'БСО',bso_correction:'БСО коррекции'};
const CORE_TYPES=new Set(['registration','reregistration','close']);
const HEAVY_TYPES=new Set(['ticket','open_shift','close_shift','receipt_correction','bso','bso_correction']);
let apiTerms=[],resultTerms=[],allRows=[],filteredRows=[],lastResponse=null,currentPage=1,sortState={key:'date_sort',dir:'desc'},statusTimer=null,currentRaw=null,currentDetailRow=null,currentDetailValues=[],requestedDetailOpened=false,keyState={active_id:null,profiles:[]},revealedKeys={};
let resultResetEpoch=0,filterResetEpoch=0,resultHidden=false;
let totalRows=0,filteredCount=0,filteredTypeCounts={},pageRowLimit=1000,effectivePageSize=50,localPageGeneration=0,localPageController=null,detailLoadGeneration=0,detailController=null,currentDetailStoreId=null;
try{resultHidden=sessionStorage.getItem('ofdResultHidden')==='1'}catch(e){}
function setResultHidden(hidden){resultHidden=hidden;try{if(hidden)sessionStorage.setItem('ofdResultHidden','1');else sessionStorage.removeItem('ofdResultHidden')}catch(e){}}
function keyEyeIcon(visible){return visible?'<svg aria-hidden="true" viewBox="0 0 24 24"><path d="m3 3 18 18"/><path d="M10.6 6.2A10.9 10.9 0 0 1 12 6c6 0 9.5 6 9.5 6a15.5 15.5 0 0 1-3.1 3.7"/><path d="M6.4 6.4C3.9 8.2 2.5 12 2.5 12s3.5 6 9.5 6c1.2 0 2.3-.2 3.3-.6"/></svg>':'<svg aria-hidden="true" viewBox="0 0 24 24"><path d="M2.5 12s3.5-6 9.5-6 9.5 6 9.5 6-3.5 6-9.5 6-9.5-6-9.5-6Z"/><circle cx="12" cy="12" r="2.8"/></svg>'}
function esc(v){return String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
function lc(v){return String(v??'').trim().toLowerCase()}
function contains(v,n){return !n||lc(v).includes(lc(n))}

// One shared palette for table, counts, checkbox labels and receipt.
const TYPE_COLORS={
  registration:'#1D4ED8',reregistration:'#7E22CE',close:'#B91C1C',
  ticket:'#15803D',open_shift:'#0E7490',close_shift:'#334155',
  receipt_correction:'#C2410C',bso:'#8A6500',bso_correction:'#BE185D',unknown:'#6B7280'
};
const CONDITION_LABELS={rnm:'РНМ',kkt:'ЗН ККТ',fn:'ЗН ФН',fd:'ФД',fpd:'ФПД',shift:'Смена',internal:'Название',retail:'Место установки',address:'Адрес'};
const DOCUMENT_QUERY_TYPES=new Set(['fd','fpd','shift']);
const CONDITION_ROW_FIELDS={rnm:'kkm_reg_id',kkt:'kkm_factory_number',fn:'fs_number',fd:'fd',fpd:'fpd',shift:'shift',internal:'kkm_internal_name',retail:'retail_place',address:'address'};
let apiConditions=[],resultConditions=[],csvPreview=null,csvReadGeneration=0,csvTargetPanel='';
function typeStyle(key){const color=TYPE_COLORS[key]||TYPE_COLORS.unknown;return '--type-color:'+color+';--type-tint:'+color+'12'}
function conditionIdentity(c){return [c.type,c.completeness,c.type==='fpd'?c.data:lc(c.data)].join('\u0000')}
function typedConditionMatches(value,c){
  if(value==null)return false;
  const actual=c.type==='fpd'?String(value).trim():lc(value),wanted=c.type==='fpd'?c.data:lc(c.data);
  return c.completeness==='full'?actual===wanted:actual.includes(wanted);
}
function splitTerms(value){return String(value).split(';').map(x=>x.trim()).filter(Boolean)}
function normalizedCsvWord(value){return lc(value).replace(/[ _-]+/g,' ')}
function csvCondition(type,completeness,data){
  const types={'рнм':'rnm','rnm':'rnm','зн ккт':'kkt','kkt':'kkt','зн фн':'fn','fn':'fn','фд':'fd','fd':'fd','фпд':'fpd','fpd':'fpd','смена':'shift','shift':'shift','название':'internal','наименование':'internal','внутреннее имя':'internal','внутреннее имя в офд':'internal','internal':'internal','место установки':'retail','retail':'retail','адрес':'address','address':'address'};
  const modes={'полное':'full','полное значение':'full','полный':'full','full':'full','exact':'full','частичное':'partial','частичное значение':'partial','частичный':'partial','partial':'partial','fragment':'partial'};
  const typeKey=normalizedCsvWord(type),modeKey=normalizedCsvWord(completeness);
  const kind=Object.hasOwn(types,typeKey)?types[typeKey]:null,mode=Object.hasOwn(modes,modeKey)?modes[modeKey]:null,value=String(data??'').trim();
  if(!kind)return {error:'type — неизвестный тип идентификатора: '+String(type)};
  if(!mode)return {error:'completeness — укажи полное/full или частичное/partial'};
  if(!value)return {error:'data — пустое значение'};
  return {condition:{type:kind,completeness:mode,data:value}};
}
function parseCsvRows(text,delimiter){
  // Quoted separators, escaped quotes and multiline values; line = physical CSV line.
  const rows=[];let cells=[],field='',quoted=false,closed=false,line=1,start=1,error='';
  const pushField=()=>{cells.push(field);field='';closed=false};
  const pushRow=()=>{pushField();if(cells.length>1||cells.some(x=>x.trim())||error)rows.push({line:start,cells,error});cells=[];error='';start=line+1};
  for(let i=0;i<text.length;i++){
    const ch=text[i];
    if(quoted){
      if(ch==='"'){if(text[i+1]==='"'){field+='"';i++}else{quoted=false;closed=true}}
      else{field+=ch;if(ch==='\n')line++;else if(ch==='\r'&&text[i+1]!=='\n')line++}
      continue;
    }
    if(ch===delimiter){pushField();continue}
    if(ch==='\r'||ch==='\n'){pushRow();if(ch==='\r'&&text[i+1]==='\n')i++;line++;continue}
    if(ch==='"'){
      if(!field.trim()&&!closed){field='';quoted=true}
      else{error='кавычка внутри незаключённого в кавычки значения';field+=ch}
      continue;
    }
    if(closed){if(ch.trim())error='текст после закрывающей кавычки';else continue}
    field+=ch;
  }
  if(quoted)error='незакрытая кавычка';
  if(field||cells.length||error)pushRow();
  return rows;
}
function parseConditionsCsv(input){
  const text=String(input).replace(/^\uFEFF/,'');
  const attempts=[';',','].map(d=>parseCsvRows(text,d));
  const validHeader=rows=>rows.length&&!rows[0].error&&rows[0].cells.length===3&&['type','completeness','data'].every(k=>rows[0].cells.map(lc).includes(k));
  const rows=attempts.find(validHeader);
  if(!rows)return {conditions:[],errors:['Строка 1: ожидается заголовок type;completeness;data (или колонки через запятую).'],duplicates:0,total:0};
  const header=rows[0].cells.map(lc),conditions=[],errors=[],seen=new Set();let duplicates=0;
  if(rows.length>10001)return {conditions:[],errors:['Файл содержит более 10 000 строк. Раздели его на несколько файлов.'],duplicates:0,total:rows.length-1};
  for(const row of rows.slice(1)){
    if(row.error||row.cells.length!==3){errors.push('Строка '+row.line+': '+(row.error||'ожидаются ровно три колонки'));continue}
    const get=k=>row.cells[header.indexOf(k)],parsed=csvCondition(get('type'),get('completeness'),get('data'));
    if(parsed.error){errors.push('Строка '+row.line+': '+parsed.error);continue}
    const id=conditionIdentity(parsed.condition);
    if(seen.has(id)){duplicates++;continue}
    seen.add(id);conditions.push(parsed.condition);
  }
  return {conditions,errors,duplicates,total:rows.length-1};
}
function showCsvReport(message){$('csvReport').textContent=message}
let loadSettingsData=null;
function applyLoadSettingsView(data){
  loadSettingsData=data;const p=data.profile;
  $('loadProfileName').textContent=p.name+(p.manual_modified?' — изменён вручную':' — значения пресета');
  $('loadPreset').innerHTML=Object.entries(data.profiles).map(([key,value])=>`<option value="${esc(key)}">${esc(value.name)}</option>`).join('');$('loadPreset').value=p.key;
  $('loadManualFields').innerHTML=Object.entries(data.labels).map(([key,label])=>{
    const id='load_'+key;
    const input=key==='storage_mode'?`<select class="select" id="${id}" name="${key}">${Object.entries(data.storage_modes).map(([value,title])=>`<option value="${value}" ${p[key]===value?'selected':''}>${esc(title)}</option>`).join('')}</select>`:`<input class="input" id="${id}" name="${key}" type="number" required min="${data.ranges[key][0]}" max="${data.ranges[key][1]}" step="1" value="${p[key]}">`;
    return `<div class="field"><label for="${id}">${esc(label)}</label>${input}</div>`;
  }).join('');
  pageRowLimit=p.page_rows;configurePageSizeOptions();
}
async function openLoadSettings(field='all'){
  $('loadSettingsModal').classList.add('show');$('loadSettingsMessage').textContent='Загрузка…';
  try{const response=await fetch('/api/load-settings');if(!response.ok)throw new Error('Не удалось загрузить настройки');applyLoadSettingsView(await response.json());$('loadSettingsMessage').textContent='';if(field!=='all')document.getElementById('load_'+field)?.focus()}catch(e){$('loadSettingsMessage').textContent=e.message}
}
async function saveLoadSettings(payload){
  $('loadSettingsMessage').textContent='Сохранение…';
  try{const response=await fetch('/api/load-settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});const data=await response.json();if(!response.ok)throw new Error(typeof data.detail==='string'?data.detail:'Проверь введённые значения');applyLoadSettingsView(data);$('loadSettingsMessage').textContent='Сохранено. Активный поиск продолжает работу с прежними настройками.'}catch(e){$('loadSettingsMessage').textContent=e.message}
}
$('loadSettingsForm').onsubmit=e=>{e.preventDefault();const values={};for(const key of Object.keys(loadSettingsData.labels)){const value=$('load_'+key).value;values[key]=key==='storage_mode'?value:Number(value)}saveLoadSettings({values})};
$('loadPresetApply').onclick=()=>saveLoadSettings({profile:$('loadPreset').value});
function refreshCsvApply(){$('csvApplyBtn').disabled=!csvPreview?.conditions.length||!csvTargetPanel}
async function readCsvFile(){
  const generation=++csvReadGeneration,file=$('csvFile').files?.[0];csvPreview=null;refreshCsvApply();
  if(!file)return;
  if(file.size>2*1024*1024){showCsvReport('Файл больше 2 МБ. Раздели его на несколько файлов.');return}
  showCsvReport('Читаю файл…');
  try{
    const bytes=await file.arrayBuffer();let text,encoding='UTF-8';
    try{text=new TextDecoder('utf-8',{fatal:true}).decode(bytes)}catch(e){text=new TextDecoder('windows-1251',{fatal:true}).decode(bytes);encoding='Windows-1251'}
    if(generation!==csvReadGeneration)return;
    csvPreview=parseConditionsCsv(text);
    showCsvReport('Кодировка: '+encoding+'. Строк: '+csvPreview.total+'. Допустимых условий: '+csvPreview.conditions.length+'. Дублей: '+csvPreview.duplicates+'. Пропущено с ошибками: '+csvPreview.errors.length+'.\n'+csvPreview.errors.join('\n'));
    refreshCsvApply();
  }catch(e){if(generation===csvReadGeneration)showCsvReport('Не удалось прочитать файл: '+e.message)}
}
function importCsvConditions(){
  const target=csvTargetPanel;
  if(!['api','result'].includes(target)||!csvPreview?.conditions.length){showCsvReport('Открой CSV-фильтр нужной панели и выбери корректный файл.');return}
  const destination=target==='api'?apiConditions:resultConditions,seen=new Set(destination.map(conditionIdentity));let added=0,duplicates=csvPreview.duplicates;
  for(const c of csvPreview.conditions){const id=conditionIdentity(c);if(seen.has(id)){duplicates++;continue}destination.push({...c});seen.add(id);added++}
  renderTokens(target);
  // Import never calls 1-OFD. The API panel runs only on Запросить; result filtering uses localhost.
  if(target==='api')updateApiSummary();else applyClientFilters();
  showCsvReport('Назначение: '+(target==='api'?'верхняя панель (запрос ещё не запущен)':'нижняя панель (без обращения к ОФД)')+'.\nДобавлено: '+added+'. Дублей: '+duplicates+'. Пропущено с ошибками: '+csvPreview.errors.length+'.\n'+csvPreview.errors.join('\n'));
  csvPreview=null;refreshCsvApply();
}

function compactOrganizationName(value){
  const name=String(value??'').trim();
  const forms=[
    ['федеральное государственное унитарное предприятие','ФГУП'],
    ['общество с ограниченной ответственностью','ООО'],
    ['общество с дополнительной ответственностью','ОДО'],
    ['публичное акционерное общество','ПАО'],
    ['непубличное акционерное общество','НАО'],
    ['открытое акционерное общество','ОАО'],
    ['закрытое акционерное общество','ЗАО'],
    ['акционерное общество','АО'],
    ['индивидуальный предприниматель','ИП'],
    ['государственное унитарное предприятие','ГУП'],
    ['муниципальное унитарное предприятие','МУП'],
    ['автономная некоммерческая организация','АНО']
  ];
  for(const [full,short] of forms){
    // Match only a whole legal form at the beginning, including multiple spaces.
    const pattern=new RegExp('^'+full.split(' ').join('\\s+')+'(?=$|[\\s«"\'])','i');
    const match=name.match(pattern);
    if(match)return short+(name.slice(match[0].length).trim()?' '+name.slice(match[0].length).trim():'');
  }
  return name;
}
function renderKeys(){
  const list=$('keyList'),profiles=keyState.profiles||[];
  if(!profiles.length){list.innerHTML='<div class="key-empty">Сохранённых API-ключей пока нет.</div>'}
  else{list.innerHTML=profiles.map(p=>{const revealed=revealedKeys[p.id];const secret=revealed?esc(revealed):`••••${esc(p.last4||'')}`;return `<div class="key-row"><div class="key-main"><b>${esc(p.name||'API-ключ')}</b><div class="key-secret-line"><span class="key-secret-value">${secret} ${p.active?'• используется сейчас':''}</span><button class="key-reveal" data-id="${esc(p.id)}" title="Показать / скрыть API-ключ">${keyEyeIcon(!!revealed)}</button></div></div><div class="key-actions">${p.active?'<span class="version-chip">Активен</span>':`<button class="btn mini key-select" data-id="${esc(p.id)}">Выбрать</button>`}<button class="btn mini key-delete" data-id="${esc(p.id)}">Удалить</button></div></div>`}).join('')}
  const active=profiles.find(p=>p.active);
  $('apiKeyBtn').textContent=active?`API: ${compactOrganizationName(active.name)} ••••${active.last4}`:'API-ключ';
  $('apiKeyBtn').title=active?String(active.name||'API-ключ'):'API-ключ';
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
  try{const res=await fetch('/api/keys/add',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({api_key:key})});const data=await res.json();if(!res.ok)throw new Error(data.detail||'Не удалось сохранить ключ');keyState=data;$('newApiKey').value='';$('newApiKey').type='password';$('toggleApiKey').classList.remove('visible');renderKeys();closeModal('keyModal')}
  catch(e){err.textContent=e.message;err.classList.add('show')}
  finally{$('addApiKeyBtn').disabled=false;$('addApiKeyBtn').textContent='Проверить и сохранить'}
}
async function selectKey(id){const res=await fetch('/api/keys/select',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({profile_id:id})});const data=await res.json();if(!res.ok){alert(data.detail||'Не удалось выбрать ключ');return}keyState=data;renderKeys();closeModal('keyModal')}
async function deleteKey(id){if(!confirm('Удалить сохранённый API-ключ с этого компьютера?'))return;const res=await fetch('/api/keys/'+encodeURIComponent(id),{method:'DELETE'});keyState=await res.json();renderKeys()}
function hasActiveKey(){return (keyState.profiles||[]).some(p=>p.active)}
function selected(selector){return [...document.querySelectorAll(selector)].filter(x=>x.checked).map(x=>x.value)}
function configurePageSizeOptions(){
  for(const option of $('pageSize').options){option.disabled=option.value!=='all'&&Number(option.value)>pageRowLimit;if(option.value==='all')option.textContent=String(pageRowLimit)}
  if($('pageSize').value!=='all'&&Number($('pageSize').value)>pageRowLimit)$('pageSize').value='all';
}
function pageSizeValue(){const raw=$('pageSize').value;return Math.min(raw==='all'?pageRowLimit:Number(raw),pageRowLimit)}
function setChecks(selector,mode){document.querySelectorAll(selector).forEach(x=>x.checked=mode==='all'||(mode==='core'&&CORE_TYPES.has(x.value)));updateApiSummary()}
function toggleAdvanced(panelId,btnId){const p=$(panelId),b=$(btnId);p.classList.toggle('open');b.classList.toggle('open',p.classList.contains('open'))}
function renderTokens(kind){
  const terms=kind==='api'?apiTerms:resultTerms,conditions=kind==='api'?apiConditions:resultConditions,box=$(kind==='api'?'apiTokens':'resultTokens');
  const chip=(text,index,typed)=>'<div class="token"><span title="'+esc(text)+'">'+esc(text)+'</span><button data-kind="'+kind+'" data-typed="'+typed+'" data-index="'+index+'" title="Удалить">×</button></div>';
  box.innerHTML=terms.map((t,i)=>chip(t,i,'no')).join('')+conditions.map((c,i)=>chip(CONDITION_LABELS[c.type]+' '+(c.completeness==='full'?'=':'содержит')+' '+c.data,i,'yes')).join('');
}
function addToken(kind){
  const input=$(kind==='api'?'apiTokenInput':'resultTokenInput'),terms=kind==='api'?apiTerms:resultTerms,seen=new Set(terms.map(lc));
  for(const value of splitTerms(input.value)){if(!seen.has(lc(value))){terms.push(value);seen.add(lc(value))}}
  input.value='';renderTokens(kind);if(kind==='api')updateApiSummary();else applyClientFilters();
}
document.addEventListener('click',e=>{const b=e.target.closest('.token button');if(!b)return;const typed=b.dataset.typed==='yes',arr=b.dataset.kind==='api'?(typed?apiConditions:apiTerms):(typed?resultConditions:resultTerms);arr.splice(Number(b.dataset.index),1);renderTokens(b.dataset.kind);if(b.dataset.kind==='api')updateApiSummary();else applyClientFilters()});
function bindTokenInput(kind){const input=$(kind==='api'?'apiTokenInput':'resultTokenInput');input.addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();addToken(kind)}});$(kind==='api'?'apiTokenAdd':'resultTokenAdd').onclick=()=>addToken(kind)}
function updateApiSummary(){const types=selected('.apiDocType'),statuses=selected('.apiStatus');const parts=[];parts.push(types.length===3&&types.every(x=>CORE_TYPES.has(x))?'основные типы':`${types.length} типов ФД`);parts.push(`${$('timeFrom').value||'00:00'}–${$('timeTo').value||'23:59'}`);parts.push(`${$('qConcurrency').value} параллельно`);if(apiTerms.length+apiConditions.length)parts.push(`быстрых условий: ${apiTerms.length+apiConditions.length}`);if($('qRnm').value.trim())parts.push('РНМ');if($('qKkt').value.trim())parts.push('ЗН ККТ');if($('qFn').value.trim())parts.push('ЗН ФН');if($('qInternal').value.trim()||$('qRetail').value.trim()||$('qAddress').value.trim())parts.push('имя / место / адрес');if($('qShift').value.trim())parts.push(`смена ${$('qShift').value.trim()}`);if(statuses.length)parts.push(`статусы ФНС: ${statuses.join(', ')}`);$('apiFilterSummary').textContent=parts.join(' • ')}
function apiPayload(){return{date_from:$('dateFrom').value,date_to:$('dateTo').value,time_from:$('timeFrom').value||'00:00',time_to:$('timeTo').value||'23:59',refresh_kkt:$('qRefresh').checked,document_types:selected('.apiDocType'),query_terms:[...apiTerms],query_conditions:apiConditions.map(c=>({...c})),rnm:$('qRnm').value.trim(),kkt_factory_number:$('qKkt').value.trim(),fs_number:$('qFn').value.trim(),internal_name:$('qInternal').value.trim(),retail_place:$('qRetail').value.trim(),address:$('qAddress').value.trim(),shift_num:$('qShift').value.trim(),irkkt_statuses:selected('.apiStatus'),concurrency:Number($('qConcurrency').value)}}
function apiIsTargeted(p){return p.rnm||p.kkt_factory_number||p.fs_number||p.internal_name||p.retail_place||p.address||(!(p.query_conditions||[]).some(c=>DOCUMENT_QUERY_TYPES.has(c.type))&&(p.query_terms.length||(p.query_conditions||[]).length))}
function setBusyUi(busy){$('searchBtn').disabled=busy;$('retryBtn').disabled=busy;$('stopSearchBtn').disabled=!busy;if(busy)$('statusbar').classList.add('show')}
async function fetchSession(){
  const res=await fetch('/api/session',{cache:'no-store'});if(!res.ok)throw new Error('Не удалось получить состояние запроса');
  const data=await res.json();pageRowLimit=data.load_profile?.page_rows||1000;
  configurePageSizeOptions();
  return data;
}
function applyRequestToUi(p){if(!p)return;if(p.date_from)$('dateFrom').value=p.date_from;if(p.date_to)$('dateTo').value=p.date_to;$('timeFrom').value=p.time_from||'00:00';$('timeTo').value=p.time_to||'23:59';apiTerms=Array.isArray(p.query_terms)?[...p.query_terms]:[];apiConditions=Array.isArray(p.query_conditions)?p.query_conditions.map(c=>({...c})):[];renderTokens('api');$('qRnm').value=p.rnm||'';$('qKkt').value=p.kkt_factory_number||'';$('qFn').value=p.fs_number||'';$('qInternal').value=p.internal_name||'';$('qRetail').value=p.retail_place||'';$('qAddress').value=p.address||'';$('qShift').value=p.shift_num||'';$('qConcurrency').value=String(p.concurrency||120);$('qRefresh').checked=!!p.refresh_kkt;const types=new Set(p.document_types||[]);document.querySelectorAll('.apiDocType').forEach(x=>x.checked=types.has(x.value));const statuses=new Set(p.irkkt_statuses||[]);document.querySelectorAll('.apiStatus').forEach(x=>x.checked=statuses.has(x.value));updateApiSummary()}
async function pollStatus(){try{const s=await fetch('/api/status',{cache:'no-store'}).then(r=>r.json());if(s.running&&s.found>=100000&&!resultHidden)$('memoryWarning').style.display='block';const pct=s.total?Math.min(100,Math.round(s.done/s.total*100)):0;$('progressBar').style.width=pct+'%';$('statusText').textContent=`${s.stage}: ${s.done}/${s.total} • найдено ${s.found} • неудачных ${s.errors}`+(s.message?' • '+s.message:'');return s}catch(e){return null}}
async function watchSearch(resetLocalOnFinish=true,{resultEpoch=resultResetEpoch,filtersEpoch=filterResetEpoch,reveal=false}={}){
  if(statusTimer)clearInterval(statusTimer);setBusyUi(true);let finishing=false,polling=false;
  const tick=async()=>{
    if(finishing||polling)return;polling=true;
    try{
      const s=await pollStatus();if(!s||s.running)return;
      finishing=true;if(statusTimer){clearInterval(statusTimer);statusTimer=null}
      try{
        const session=await fetchSession();
        if(session.result&&resultEpoch===resultResetEpoch){
          // Only a newly requested, completed search may reveal a hidden result.
          if(reveal&&!s.error&&!s.cancelled&&s.stage==='Готово')setResultHidden(false);
          acceptResponse(session.result,resetLocalOnFinish&&filtersEpoch===filterResetEpoch);
        }
        if(s.error)alert(s.message||s.error);
      }catch(e){console.error(e)}finally{setBusyUi(false);setTimeout(()=>$('statusbar').classList.remove('show'),1200)}
    }finally{polling=false}
  };
  statusTimer=setInterval(tick,650);await tick();
}
async function doSearch(){if(!hasActiveKey()){$('keyModal').classList.add('show');return}const p=apiPayload();if(!p.date_from||!p.date_to){alert('Укажи обе даты');return}if(!p.document_types.length){alert('Выбери хотя бы один тип документа');return}if(p.document_types.some(x=>HEAVY_TYPES.has(x))&&!apiIsTargeted(p)){if(!confirm('Выбраны кассовые чеки/смены по всему парку. Объём данных может быть очень большим. Продолжить?'))return}if(p.query_conditions.some(c=>DOCUMENT_QUERY_TYPES.has(c.type))&&!confirm('Условия ФД, ФПД или смены проверяются после загрузки документов: Universal API не позволяет найти их по каталогу ККТ. Без расширенного отбора ККТ запрос охватит весь парк и может быть объёмным. Продолжить?'))return;const watchOptions={resultEpoch:resultResetEpoch,filtersEpoch:filterResetEpoch,reveal:true};setBusyUi(true);$('statusText').textContent='Запуск…';$('progressBar').style.width='0%';try{const res=await fetch('/api/search',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(p)});const data=await res.json();if(!res.ok)throw new Error(data.detail||'Ошибка поиска');await watchSearch(true,watchOptions)}catch(e){setBusyUi(false);$('statusbar').classList.remove('show');alert(e.message)}}
async function stopSearch(){const btn=$('stopSearchBtn');btn.disabled=true;$('statusText').textContent='Останавливаю запрос…';try{const res=await fetch('/api/stop-search',{method:'POST'});const data=await res.json();if(!res.ok)throw new Error(data.detail||'Не удалось остановить запрос');if(!data.stopping)$('statusText').textContent=data.message||'Активного запроса нет'}catch(e){btn.disabled=false;alert(e.message)}}
async function restoreSession(){
  const resultEpoch=resultResetEpoch,filtersEpoch=filterResetEpoch;
  try{
    const session=await fetchSession();
    if(session.result&&resultEpoch===resultResetEpoch)acceptResponse(session.result,filtersEpoch===filterResetEpoch);
    if(session.request&&filtersEpoch===filterResetEpoch)applyRequestToUi(session.request);
    const s=session.status||{};if(s.running){$('statusText').textContent='Восстановлен активный запрос…';await watchSearch(true,{resultEpoch,filtersEpoch})}
  }catch(e){console.error('Не удалось восстановить состояние',e)}
}
async function restoreLastResult(){
  const resultEpoch=resultResetEpoch,button=$('restoreLastResultBtn');button.disabled=true;
  try{
    const session=await fetchSession();if(resultEpoch!==resultResetEpoch)return;
    if(!session.result){alert('Сохранённого результата пока нет');return}
    setResultHidden(false);acceptResponse(session.result,false);
  }catch(e){alert(e.message)}finally{button.disabled=false}
}
function updateCompleteness(data){const c=data.completeness||{},box=$('completeness'),retry=$('retryBtn'),catalogRetry=$('catalogRetryBtn'),details=$('failedDetailsBtn');box.className='completeness show';if(c.full_coverage){box.classList.add('ok');$('completeTitle').textContent='Выборка полная по всем запланированным API-проверкам';$('completeSub').textContent=`Успешно ${c.successful_queries}/${c.planned_queries}. Каталог ККТ получен полностью. Дополнительно проверено неоднозначных ФН: ${data.uncertain_candidate_count||0}.`;retry.style.display='none';catalogRetry.style.display='none';details.style.display='none'}else if(c.all_planned_checks_complete&&!c.registration_coverage_complete){box.classList.add('warn');$('completeTitle').textContent='Все API-запросы выполнены, но поиск первичной регистрации имеет ограничение';$('completeSub').textContent=`У ${data.registration_unverifiable_count||0} ФН нет надёжной activationDate. Фильтруемые типы документов проверены, но для первичной регистрации по этим ФН нельзя гарантировать полноту без более тяжёлого полного сканирования.`;retry.style.display='none';catalogRetry.style.display='none';details.style.display='none'}else{box.classList.add(c.failed_queries>0?'bad':'warn');$('completeTitle').textContent='Результат может быть неполным';$('completeSub').textContent=`Документные запросы: ${c.successful_queries}/${c.planned_queries}; не проверено: ${c.failed_queries}. Не загружено мест установки: ${c.catalog_failed_places}.`;retry.style.display=c.failed_queries>0?'inline-block':'none';catalogRetry.style.display=c.catalog_failed_places>0?'inline-block':'none';details.style.display=(c.failed_queries>0||c.catalog_failed_places>0)?'inline-block':'none'}}
function acceptResponse(data,resetLocal=false){if(resultHidden)return;lastResponse=data;totalRows=data.total_rows??data.rows?.length??0;allRows=[];filteredRows=[];filteredCount=0;filteredTypeCounts={};$('memoryWarning').style.display=data.large_selection?'block':'none';if(resetLocal){if(data.query_filters)applyRequestToUi(data.query_filters);resultTerms=[];resultConditions=[];renderTokens('result');$('rDateFrom').value=data.period?.from||$('dateFrom').value;$('rDateTo').value=data.period?.to||$('dateTo').value;$('rTimeFrom').value=$('timeFrom').value||'00:00';$('rTimeTo').value=$('timeTo').value||'23:59';['rRnm','rKkt','rFn','rFd','rFpd','rShift','rInternal','rRetail','rAddress','rStatus'].forEach(id=>$(id).value='');document.querySelectorAll('.resultDocType').forEach(x=>x.checked=true)}updateCompleteness(data);applyClientFilters();openRequestedDetail()}
function rowSearchValues(r){return[r.date,r.inserted_at,r.type,r.raw_type,r.kkm_internal_name,r.kkm_reg_id,r.kkm_factory_number,r.fs_number,r.fd,r.fpd,r.shift,r.amount,r.fns_flc_status,r.fns_status,r.fns_description,r.retail_place,r.address].map(lc)}
function rowMatchesTerms(r){if(!resultTerms.length&&!resultConditions.length)return true;const values=rowSearchValues(r);return resultTerms.some(term=>values.some(v=>v.includes(lc(term))))||resultConditions.some(c=>typedConditionMatches(r[CONDITION_ROW_FIELDS[c.type]],c))}
function resultPeriodBounds(){const d1=$('rDateFrom').value,d2=$('rDateTo').value,t1=$('rTimeFrom').value||'00:00',t2=$('rTimeTo').value||'23:59';return{from:d1?`${d1}T${t1}:00`:'',to:d2?`${d2}T${t2}:59`:''}}
function localFilterPayload(){
  const bounds=resultPeriodBounds();
  return {store_id:lastResponse.store_id,terms:[...resultTerms],conditions:resultConditions.map(c=>({...c})),document_types:selected('.resultDocType'),date_from:bounds.from,date_to:bounds.to,
    advanced:{rnm:$('rRnm').value.trim(),kkt:$('rKkt').value.trim(),fn:$('rFn').value.trim(),fd:$('rFd').value.trim(),fpd:$('rFpd').value.trim(),shift:$('rShift').value.trim(),internal:$('rInternal').value.trim(),retail:$('rRetail').value.trim(),address:$('rAddress').value.trim(),status:$('rStatus').value.trim()},
    sort_key:sortState.key,sort_dir:sortState.dir,page:currentPage,page_size:pageSizeValue()};
}
function applyClientFilters(){currentPage=1;updateResultSummary();return loadLocalPage()}
async function loadLocalPage(){
  const generation=++localPageGeneration;localPageController?.abort();
  if(resultHidden||!lastResponse?.store_id){allRows=[];filteredRows=[];filteredCount=0;filteredTypeCounts={};render();updateStats();$('applyResultBtn').disabled=false;return}
  const storeId=lastResponse.store_id,controller=new AbortController();localPageController=controller;
  $('applyResultBtn').disabled=true;$('resultFilterSummary').textContent='Обработка локальных фильтров…';
  try{
    const res=await fetch('/api/result-page',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(localFilterPayload()),signal:controller.signal});
    const data=await res.json();if(!res.ok)throw new Error(data.detail||'Не удалось применить локальные фильтры');
    if(generation!==localPageGeneration||resultHidden||lastResponse?.store_id!==storeId)return;
    allRows=filteredRows=data.rows||[];filteredCount=data.filtered_count||0;filteredTypeCounts=data.type_counts||{};currentPage=data.page;effectivePageSize=data.page_size;
    render();updateStats();updateResultSummary();
  }catch(e){if(e.name!=='AbortError'&&generation===localPageGeneration)alert(e.message)}
  finally{if(generation===localPageGeneration){$('applyResultBtn').disabled=false;localPageController=null}}
}
function updateResultSummary(){const parts=[];if(resultTerms.length+resultConditions.length)parts.push(`условий поиска: ${resultTerms.length+resultConditions.length}`);const types=selected('.resultDocType');if(types.length<document.querySelectorAll('.resultDocType').length)parts.push(`типов: ${types.length}`);['rRnm','rKkt','rFn','rFd','rFpd','rShift','rInternal','rRetail','rAddress','rStatus'].forEach(id=>{if($(id).value.trim())parts.push($(id).previousElementSibling?.textContent||id)});$('resultFilterSummary').textContent=parts.length?parts.join(' • '):'Показываются все загруженные документы'}
function updateStats(){const data=lastResponse||{},c=data.completeness||{};$('sLoaded').textContent=totalRows;$('sShown').textContent=filteredCount;$('sKkt').textContent=data.kkm_count??'—';$('sMatchedKkt').textContent=data.matched_kkm_count??'—';$('sChecked').textContent=`${c.successful_queries??0}/${c.planned_queries??0}`;$('sFailed').textContent=c.failed_queries??'—';$('sUncertain').textContent=data.uncertain_candidate_count??'—';$('sTime').textContent=(data.elapsed_seconds??'—')+' сек';$('typeSummary').innerHTML=Object.entries(filteredTypeCounts).map(([k,v])=>`<span class="type-pill" style="${typeStyle(k)}">${esc(TYPE_LABELS[k]||'Неизвестный тип')}: <b>${v}</b></span>`).join('')}
function render(){const body=$('rows'),empty=$('empty'),size=effectivePageSize,pages=Math.max(1,Math.ceil(filteredCount/size));if(currentPage>pages)currentPage=pages;const items=filteredRows;body.innerHTML=items.map((r,i)=>`<tr data-index="${i}"><td>${esc(r.date)}</td><td>${esc(r.kkm_internal_name)}</td><td>${esc(r.kkm_reg_id)}</td><td><span class="type ${esc(r.document_key)}" style="${typeStyle(r.document_key)}">${esc(r.type)}</span></td><td>${esc(r.shift)}</td><td>${esc(r.fd)}</td><td>${r.amount==null?'—':esc(r.amount)}</td><td>${esc(r.fpd)}</td><td>${esc(r.fs_number)}</td><td>${esc(r.kkm_factory_number)}</td><td>${esc(r.fns_flc_status??r.fns_status)}</td><td>${esc(r.inserted_at)}</td><td>${esc(r.address)}</td></tr>`).join('');[...body.querySelectorAll('tr')].forEach(tr=>tr.onclick=()=>showDetails(filteredRows[Number(tr.dataset.index)]));empty.style.display=filteredCount?'none':'block';$('emptyText').textContent=totalRows?'По текущим локальным фильтрам ничего не найдено.':resultHidden?'Результат сброшен. Сохранённый поиск можно восстановить.':'Сначала выполни запрос в Первый ОФД.';$('restoreLastResultBtn').style.display=resultHidden&&!totalRows?'inline-flex':'none';$('tableInfo').textContent=`Показано ${filteredCount} из ${totalRows} документов`;$('pageText').textContent=`Страница ${filteredCount?currentPage:0} из ${filteredCount?pages:0}`;$('firstPage').disabled=$('prevPage').disabled=currentPage<=1;$('nextPage').disabled=$('lastPage').disabled=currentPage>=pages||!filteredCount}
function splitDisplayDateTime(value){const s=String(value??'').trim();const m=s.match(/^(\d{2}\.\d{2}\.\d{4})(?:\s+(\d{2}:\d{2}))?$/);return m?{date:m[1],time:m[2]||''}:null}
function hasDetailValue(value){return value!=null&&(typeof value!=='string'||value.trim()!=='')&&(!Array.isArray(value)||value.length>0)&&(typeof value!=='object'||Object.keys(value).length>0)}
function displayDetailValue(value){return typeof value==='boolean'?(value?'Да':'Нет'):typeof value==='object'?JSON.stringify(value):String(value)}
function pushCopyValue(value){return currentDetailValues.push(String(value))-1}
const RECEIPT_EQUALS_FIELDS=new Set([
  'kkmRegId','registrationNumber','kkmFactoryNumber','kktNumber','fiscalDriveNumber','fsFactoryNumber',
  'fiscalDocumentNumber','fiscalSign','shiftNum','correctionNumber','number','receiptCode',
  'userInn','operatorInn','cashierInn','inn','ofdInn',
  'ffdVersion','fiscalFormatVersion','fiscalDocumentVersion',
  'totalSum','cashSum','cashTotalSum','electronicSum','ecashTotalSum','prepaidSum','creditSum','provisionSum','price','sum','ndsSum',
  'quantity','count','counter','receiptCount','itemsCount','id','retailPlaceId','_retailPlaceId'
]);
function receiptField(label,value,{address=false,total=false,fieldKey=''}={}){
  if(!hasDetailValue(value))return '';
  const text=displayDetailValue(value),caption=String(label).trim().replace(/:+$/,'')+':';
  const prefix=!address&&RECEIPT_EQUALS_FIELDS.has(fieldKey)?'= ':'';
  // No copy index anywhere on the address row: normal selection and Ctrl+C only.
  const attrs=address?'':' data-copy-index="'+pushCopyValue(text)+'" tabindex="0" role="button" title="Нажми, чтобы скопировать"';
  return '<div class="receipt-row '+(address?'receipt-address':'copyable')+(total?' receipt-total':'')+'"'+attrs+'><span class="receipt-label">'+esc(caption)+'</span><span class="receipt-value">'+prefix+esc(text)+'</span></div>';
}
function copyableField(label,value,fieldKey=''){return receiptField(label,value,{fieldKey})}
function addressField(label,value){return receiptField(label,value,{address:true})}
function datetimeField(title,value,dateLabel='Дата',timeLabel='Время'){
  if(!hasDetailValue(value))return '';
  const dt=splitDisplayDateTime(value);
  return dt?copyableField(dateLabel,dt.date,'date')+copyableField(timeLabel,dt.time,'time'):copyableField(title,value,'dateTime');
}
function detailSection(title,items){
  const content=items.filter(Boolean).join('');
  return content?'<section class="receipt-section">'+(title?'<h3 class="receipt-section-title">'+esc(title)+'</h3>':'')+content+'</section>':'';
}
const DETAIL_FIELD_LABELS={
  user:'Пользователь',userName:'Пользователь',userInn:'ИНН пользователя',inn:'ИНН',
  operator:'Оператор',operatorInn:'ИНН оператора',cashier:'Кассир',cashierInn:'ИНН кассира',
  taxationType:'Система налогообложения',taxationSystem:'Система налогообложения',
  operationType:'Признак расчёта',calculationType:'Тип расчёта',paymentType:'Тип оплаты',
  totalSum:'Итого',cashSum:'Наличными',cashTotalSum:'Наличными',electronicSum:'Безналичными',
  ecashTotalSum:'Безналичными',prepaidSum:'Предоплата',creditSum:'Кредит',provisionSum:'Встречное предоставление',
  receiptCode:'Код чека',kktNumber:'Номер ККТ',kktModel:'Модель ККТ',fiscalDriveModel:'Модель ФН',
  ffdVersion:'Версия ФФД',fiscalFormatVersion:'Версия ФФД',fiscalDocumentVersion:'Версия ФФД',
  address:'Адрес',paymentAddress:'Адрес расчётов',retailAddress:'Адрес места установки',
  retailPlace:'Место расчётов',paymentPlace:'Место расчётов',ofdInn:'ИНН ОФД',ofdName:'ОФД',
  name:'Наименование',quantity:'Количество',price:'Цена',sum:'Сумма',items:'Позиции',
  taxes:'Налоги',nds:'НДС',ndsType:'Ставка НДС',ndsSum:'Сумма НДС',
  correctionType:'Тип коррекции',correctionReason:'Основание коррекции',
  correctionDescription:'Описание коррекции',correctionDate:'Дата основания коррекции',
  correctionNumber:'Номер основания коррекции',reason:'Основание',date:'Дата',number:'Номер',
  registrationNumber:'Регистрационный номер',fiscalDriveNumber:'ЗН ФН',fsFactoryNumber:'ЗН ФН',
  fiscalDocumentNumber:'ФД',fiscalSign:'ФПД',transactionDate:'Дата и время ФД',shiftNum:'Смена',
  insertedAt:'Поступление в ОФД',fnsFlcStatus:'Статус ФЛК ФНС',fnsStatus:'Код ФНС',
  fnsDescription:'Описание ФНС',fnsConfirmation:'Подтверждение ФНС',
  fiscalReport:'Данные регистрации',fiscalReportCorrection:'Данные перерегистрации',
  closeArchive:'Данные закрытия ФН',ticket:'Данные чека',receiptCorrection:'Данные коррекции',
  bso:'Данные БСО',bsoCorrection:'Данные коррекции БСО',openShift:'Данные открытия смены',
  closeShift:'Данные закрытия смены'
};
const RECEIPT_PAYLOAD_KEYS=new Set(['fiscalReport','fiscalReportCorrection','closeArchive','ticket','receiptCorrection','bso','bsoCorrection','openShift','closeShift']);
function isAddressKey(key){return /address|адрес/i.test(key)}
function isTechnicalKey(key){return /^(insertedAt|irkktStatus|transactionType|online)$|^fns|(^id$|Id$|_id$|^internal)/i.test(key)}
function documentReceiptRows(document,usedFields){
  const main=[],extra=[];
  const walk=(value,label,technical=false,depth=0,fieldKey='')=>{
    if(!hasDetailValue(value))return;
    if(typeof value==='object'&&depth<8){
      if(Array.isArray(value))value.forEach((v,i)=>walk(v,label+' '+(i+1),technical,depth+1,fieldKey));
      else for(const [key,v] of Object.entries(value)){
        if(depth===0&&usedFields.has(key)&&!technical)continue;
        const fieldLabel=DETAIL_FIELD_LABELS[key]||key;
        walk(v,label?label+' / '+fieldLabel:fieldLabel,technical||isTechnicalKey(key),depth+1,key);
      }
      return;
    }
    (technical?extra:main).push(receiptField(label,value,{address:isAddressKey(label),fieldKey}));
  };
  for(const [key,value] of Object.entries(document||{})){
    if(usedFields.has(key))continue;
    const label=RECEIPT_PAYLOAD_KEYS.has(key)?'':(DETAIL_FIELD_LABELS[key]||key);
    walk(value,label,isTechnicalKey(key),0,key);
  }
  return {main,extra};
}
function detailKey(r){return [r?.kkm_reg_id??'',r?.fs_number??'',r?.fd??'',r?.document_key??'',r?.date_sort??''].join('~')}
function openCurrentDetailInNewTab(){if(!currentDetailRow)return;const url=new URL(window.location.origin+window.location.pathname);url.searchParams.set('doc',detailKey(currentDetailRow));if(currentDetailStoreId)url.searchParams.set('result',currentDetailStoreId);window.open(url.toString(),'_blank','noopener')}
function openRequestedDetail(){
  if(requestedDetailOpened||!lastResponse?.store_id)return;const params=new URLSearchParams(window.location.search),key=params.get('doc');if(!key)return;
  requestedDetailOpened=true;showDetails({_docKey:key,_storeId:params.get('result')||lastResponse.store_id}).then(opened=>{if(!opened)requestedDetailOpened=false});
}
async function showDetails(row){
  const generation=++detailLoadGeneration;detailController?.abort();const controller=new AbortController();detailController=controller;
  const storeId=row._storeId||lastResponse?.store_id,epoch=resultResetEpoch;
  try{
    if(!row.raw){
      const params=new URLSearchParams({store_id:storeId});if(row._docKey)params.set('doc',row._docKey);else params.set('row_id',String(row.row_id));
      const res=await fetch('/api/document?'+params,{cache:'no-store',signal:controller.signal});const data=await res.json();if(!res.ok)throw new Error(data.detail||'Не удалось открыть ФД');row=data;
    }
    if(generation!==detailLoadGeneration||epoch!==resultResetEpoch||resultHidden)return;
    currentDetailStoreId=storeId;renderDetails(row);return true;
  }catch(e){if(e.name!=='AbortError'&&generation===detailLoadGeneration)alert(e.message);return false}
  finally{if(generation===detailLoadGeneration)detailController=null}
}
function renderDetails(r){
  currentRaw=r.raw;currentDetailRow=r;currentDetailValues=[];
  const color=TYPE_COLORS[r.document_key]||TYPE_COLORS.unknown;
  const modal=$('detailsModal');modal.style.setProperty('--type-color',color);modal.style.setProperty('--type-tint',color+'12');
  $('detailTypeChip').textContent=r.type||TYPE_LABELS[r.document_key]||'Фискальный документ';
  const used=new Set();
  const sourceValues={
    transactionDate:r.date,fiscalDocumentNumber:r.fd,fiscalSign:r.fpd,shiftNum:r.shift,
    kkmRegId:r.kkm_reg_id,fiscalDriveNumber:r.fs_number,fsFactoryNumber:r.fs_number,
    kkmFactoryNumber:r.kkm_factory_number,totalSum:r.amount,
    insertedAt:r.inserted_at,fnsFlcStatus:r.fns_flc_status,fnsStatus:r.fns_status,
    fnsDescription:r.fns_description,fnsConfirmation:r.fns_confirmation,
    kkmInternalName:r.kkm_internal_name
  };
  for(const [key,value] of Object.entries(sourceValues))if(hasDetailValue(value))used.add(key);
  const dynamic=documentReceiptRows(r.raw?.document||{},used);
  const sections=[
    detailSection('',[
      datetimeField('Дата и время ФД',r.date,'Дата ФД','Время ФД'),
      receiptField('Сумма',r.amount,{total:true,fieldKey:'totalSum'})
    ]),
    detailSection('Реквизиты',[
      copyableField('РНМ',r.kkm_reg_id,'kkmRegId'),copyableField('ЗН ККТ',r.kkm_factory_number,'kkmFactoryNumber'),
      copyableField('ЗН ФН',r.fs_number,'fiscalDriveNumber'),copyableField('ФД',r.fd,'fiscalDocumentNumber'),
      copyableField('ФПД',r.fpd,'fiscalSign'),copyableField('Смена',r.shift,'shiftNum')
    ]),
    detailSection('Адрес и место расчетов',[copyableField('Место установки',r.retail_place),addressField('Адрес',r.address)]),
    detailSection('Данные документа',dynamic.main),
    detailSection('Жизненный цикл ФН',[
      datetimeField('Активация ФН',r.activation_date,'Дата активации ФН','Время активации ФН'),
      datetimeField('Срок ФН',r.expire_date,'Дата окончания срока ФН','Время окончания срока ФН'),
      datetimeField('Закрытие архива',r.close_archive_date,'Дата закрытия архива','Время закрытия архива')
    ])
  ].filter(Boolean);
  $('detailsGrid').className='receipt';
  const title=r.type||TYPE_LABELS[r.document_key]||'Фискальный документ';
  $('detailsGrid').innerHTML='<h2 class="receipt-heading">'+esc(title)+'</h2>'+sections.join('');
  const extra=[
    datetimeField('Поступление в ОФД',r.inserted_at,'Дата поступления в ОФД','Время поступления в ОФД'),
    copyableField('Статус ФЛК ФНС',r.fns_flc_status),copyableField('Код ФНС',r.fns_status),
    copyableField('Внутреннее имя в ОФД',r.kkm_internal_name),
    copyableField('Подтверждение ФНС',r.fns_confirmation),copyableField('Описание ФНС',r.fns_description),
    copyableField('ID места установки',r.retail_place_id,'retailPlaceId'),...dynamic.extra
  ];
  // Internal IDs of catalog/response metadata are technical, never fiscal receipt rows.
  for(const [prefix,obj] of [['ККТ',r.raw?.kkm],['Ответ ОФД',r.raw?.responseMeta]]){
    for(const [key,value] of Object.entries(obj||{})){
      if(isTechnicalKey(key)&&key!=='_retailPlaceId')extra.push(copyableField(prefix+' / '+key,value,key));
    }
  }
  $('extraDetailsGrid').className='receipt-extra';
  $('extraDetailsGrid').innerHTML=detailSection('',extra);
  $('rawJson').textContent='';
  $('extraDetails').open=false;
  const rawDetails=document.querySelector('#detailsModal .raw-details:not(.extra-details)');if(rawDetails)rawDetails.open=false;
  modal.classList.add('show');
}
function closeModal(id){
  $(id).classList.remove('show');
  if(id==='detailsModal'){detailLoadGeneration++;detailController?.abort();detailController=null;currentDetailStoreId=null;currentRaw=null;currentDetailRow=null;currentDetailValues=[];$('rawJson').textContent='';$('detailsGrid').innerHTML='';$('extraDetailsGrid').innerHTML=''}
  if(id==='csvImportModal'){csvReadGeneration++;csvPreview=null;csvTargetPanel='';$('csvFile').value='';$('csvReport').textContent=''}
  if(id==='keyModal'){revealedKeys={};$('newApiKey').value='';$('newApiKey').type='password';$('toggleApiKey').classList.remove('visible');renderKeys()}
}
function showCopyToast(text='Скопировано'){const t=$('copyToast');t.textContent=text;t.classList.add('show');clearTimeout(showCopyToast.timer);showCopyToast.timer=setTimeout(()=>t.classList.remove('show'),900)}
async function copyText(text){try{await navigator.clipboard.writeText(String(text??''));return true}catch(e){try{const ta=document.createElement('textarea');ta.value=String(text??'');ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.select();const ok=document.execCommand('copy');ta.remove();return ok}catch(_){return false}}}
async function retryFailed(){if(!lastResponse)return;const watchOptions={resultEpoch:resultResetEpoch,filtersEpoch:filterResetEpoch,reveal:true};setBusyUi(true);$('statusText').textContent='Запускаю повтор…';try{const res=await fetch('/api/retry-failed',{method:'POST'});const data=await res.json();if(!res.ok)throw new Error(data.detail||'Ошибка повторного запроса');await watchSearch(false,watchOptions)}catch(e){setBusyUi(false);$('statusbar').classList.remove('show');alert(e.message)}}
function showFailures(){if(!lastResponse)return;const c=lastResponse.completeness||{},failed=lastResponse.failed_queries||[],catalog=lastResponse.catalog_failures||[];$('failureSummary').textContent=`Не проверено документных запросов: ${c.failed_queries||0}. Не загружено мест установки: ${c.catalog_failed_places||0}.`;const parts=[];failed.forEach(x=>parts.push(`<div class="failure-row"><b>${esc((x.types||[]).join(', '))}</b><br><code>РНМ ${esc(x.rnm)} • ФН ${esc(x.fs_number)}</code><br><span class="muted">${esc(x.error)}</span></div>`));catalog.forEach(x=>parts.push(`<div class="failure-row"><b>Место установки ${esc(x.title||x.retailPlaceId)}</b><br><code>ID ${esc(x.retailPlaceId)}</code><br><span class="muted">${esc(x.error)}</span></div>`));$('failureList').innerHTML=parts.join('')||'<div class="failure-row">Непроверенных элементов нет.</div>';$('failuresModal').classList.add('show')}
function downloadResult(format){
  if(!filteredCount||!lastResponse?.store_id){alert('Нет данных для выгрузки');return}
  let frame=$('exportFrame');if(!frame){frame=document.createElement('iframe');frame.id='exportFrame';frame.name='ofdExport';frame.style.display='none';document.body.appendChild(frame);
    frame.onload=()=>{try{const text=frame.contentDocument?.body?.textContent||'';if(text.startsWith('{')){const error=JSON.parse(text);if(error.detail)alert(error.detail)}}catch(e){}};
  }
  const form=document.createElement('form');form.method='POST';form.action='/api/export.'+format;form.target='ofdExport';form.acceptCharset='UTF-8';form.style.display='none';
  const input=document.createElement('input');input.type='hidden';input.name='filters';input.value=JSON.stringify(localFilterPayload());form.appendChild(input);document.body.appendChild(form);form.submit();form.remove();
}
function downloadCSV(){downloadResult('csv')}
function downloadJSON(){downloadResult('json')}
function resetLocal(){resultTerms=[];resultConditions=[];renderTokens('result');$('rDateFrom').value=lastResponse?.period?.from||$('dateFrom').value;$('rDateTo').value=lastResponse?.period?.to||$('dateTo').value;$('rTimeFrom').value=$('timeFrom').value||'00:00';$('rTimeTo').value=$('timeTo').value||'23:59';['rRnm','rKkt','rFn','rFd','rFpd','rShift','rInternal','rRetail','rAddress','rStatus'].forEach(id=>$(id).value='');document.querySelectorAll('.resultDocType').forEach(x=>x.checked=true);applyClientFilters()}
function resetAllFilters(){
  filterResetEpoch++;apiTerms=[];resultTerms=[];apiConditions=[];resultConditions=[];
  renderTokens('api');renderTokens('result');
  document.querySelectorAll('#apiPanel input,#resultPanel input').forEach(input=>{
    if(input.type==='checkbox')input.checked=input.defaultChecked;else input.value='';
  });
  $('qConcurrency').value='120';
  closeModal('csvImportModal');refreshCsvApply();
  updateApiSummary();applyClientFilters();
}
function resetLoadedResult(){
  resultResetEpoch++;setResultHidden(true);
  localPageGeneration++;localPageController?.abort();localPageController=null;lastResponse=null;allRows=[];filteredRows=[];totalRows=0;filteredCount=0;filteredTypeCounts={};currentPage=1;$('applyResultBtn').disabled=false;$('memoryWarning').style.display='none';
  closeModal('detailsModal');closeModal('failuresModal');
  $('failureList').innerHTML='';$('failureSummary').textContent='';
  $('completeness').className='completeness';$('completeTitle').textContent='';$('completeSub').textContent='';
  ['failedDetailsBtn','catalogRetryBtn','retryBtn'].forEach(id=>$(id).style.display='none');
  render();updateStats();updateResultSummary();
}
function resetEverything(){if(!confirm('Сбросить фильтры и результат?'))return;resetAllFilters();resetLoadedResult()}
function openCsvFilter(target){
  if(!['api','result'].includes(target))return;
  csvReadGeneration++;csvPreview=null;csvTargetPanel=target;$('csvFile').value='';
  $('csvDestination').textContent=target==='api'?'Условия будут добавлены в фильтр запроса к API.':'Условия будут добавлены в фильтр по найденному.';
  showCsvReport('Выбери CSV-файл. Импорт не запускает запрос к ОФД.');
  refreshCsvApply();$('csvImportModal').classList.add('show');
}
function initDates(){const now=new Date(),localNow=new Date(now.getTime()-now.getTimezoneOffset()*60000),today=localNow.toISOString().slice(0,10),first=`${today.slice(0,8)}01`;$('dateFrom').value=first;$('dateTo').value=today;$('rDateFrom').value=first;$('rDateTo').value=today}

async function checkVersion(attempt=0){
  try{
    const res=await fetch('/api/update-check',{cache:'no-store'});if(!res.ok)return;const data=await res.json(),button=$('versionBtn');
    button.classList.toggle('update-available',!!data.available);$('versionAlert').style.display=data.available?'inline':'none';button.dataset.tip=data.available?'Доступна новая версия':'Открыть GitHub';
    if(data.checking&&attempt<10)setTimeout(()=>checkVersion(attempt+1),1000);
  }catch(e){}
}
bindTokenInput('api');bindTokenInput('result');initDates();renderTokens('api');renderTokens('result');
document.querySelectorAll('.apiDocType,.resultDocType').forEach(input=>{const label=input.closest('label');label.classList.add('document-type');label.style.cssText=typeStyle(input.value)});
document.querySelector('#detailsModal .raw-details:not(.extra-details)').addEventListener('toggle',function(){ $('rawJson').textContent=this.open&&currentRaw?JSON.stringify(currentRaw,null,2):'' });
document.querySelectorAll('[data-csv-target]').forEach(button=>button.onclick=()=>openCsvFilter(button.dataset.csvTarget));
$('resetAllFiltersBtn').onclick=resetAllFilters;$('resetEverythingBtn').onclick=resetEverything;$('resetLoadedResultBtn').onclick=resetLoadedResult;$('restoreLastResultBtn').onclick=restoreLastResult;
$('csvFile').onchange=readCsvFile;$('csvApplyBtn').onclick=importCsvConditions;
$('detailsModal').addEventListener('keydown',e=>{const target=e.target.closest('[data-copy-index]');if(target&&(e.key==='Enter'||e.key===' ')){e.preventDefault();target.click()}});
$('stopSearchBtn').onclick=stopSearch;$('detailsModal').addEventListener('click',async e=>{const target=e.target.closest('[data-copy-index]');if(!target)return;const i=Number(target.dataset.copyIndex),ok=await copyText(currentDetailValues[i]??'');if(ok){target.classList.add('copied');showCopyToast();setTimeout(()=>target.classList.remove('copied'),850)}else{showCopyToast('Не удалось скопировать')}});$('openDetailTab').onclick=openCurrentDetailInNewTab;$('apiAdvancedBtn').onclick=()=>toggleAdvanced('apiAdvanced','apiAdvancedBtn');$('resultAdvancedBtn').onclick=()=>toggleAdvanced('resultAdvanced','resultAdvancedBtn');$('searchBtn').onclick=doSearch;$('applyResultBtn').onclick=applyClientFilters;$('retryBtn').onclick=retryFailed;$('catalogRetryBtn').onclick=()=>{$('qRefresh').checked=true;doSearch()};$('failedDetailsBtn').onclick=showFailures;$('csvBtn').onclick=downloadCSV;$('jsonBtn').onclick=downloadJSON;$('apiCoreTypes').onclick=()=>setChecks('.apiDocType','core');$('apiAllTypes').onclick=()=>setChecks('.apiDocType','all');$('apiNoTypes').onclick=()=>setChecks('.apiDocType','none');$('resultAllTypes').onclick=()=>{document.querySelectorAll('.resultDocType').forEach(x=>x.checked=true);applyClientFilters()};$('resultNoTypes').onclick=()=>{document.querySelectorAll('.resultDocType').forEach(x=>x.checked=false);applyClientFilters()};$('resetResultFilters').onclick=resetLocal;$('pageSize').onchange=()=>{currentPage=1;loadLocalPage()};$('firstPage').onclick=()=>{currentPage=1;loadLocalPage()};$('prevPage').onclick=()=>{currentPage=Math.max(1,currentPage-1);loadLocalPage()};$('nextPage').onclick=()=>{currentPage++;loadLocalPage()};$('lastPage').onclick=()=>{currentPage=Math.max(1,Math.ceil(filteredCount/effectivePageSize));loadLocalPage()};document.querySelectorAll('[data-close]').forEach(x=>x.onclick=()=>closeModal(x.dataset.close));document.querySelectorAll('.modal').forEach(m=>m.onclick=e=>{if(e.target===m)closeModal(m.id)});$('copyJson').onclick=async()=>{await navigator.clipboard.writeText(JSON.stringify(currentRaw,null,2));$('copyJson').textContent='Скопировано';setTimeout(()=>$('copyJson').textContent='Копировать JSON',900)};document.querySelectorAll('th[data-sort]').forEach(th=>th.onclick=()=>{const key=th.dataset.sort;if(sortState.key===key)sortState.dir=sortState.dir==='asc'?'desc':'asc';else{sortState.key=key;sortState.dir='asc'}applyClientFilters()});
['timeFrom','timeTo','qRnm','qKkt','qFn','qInternal','qRetail','qAddress','qShift','qConcurrency'].forEach(id=>$(id).addEventListener('input',updateApiSummary));document.querySelectorAll('.apiDocType,.apiStatus').forEach(x=>x.addEventListener('change',updateApiSummary));['rDateFrom','rDateTo','rTimeFrom','rTimeTo'].forEach(id=>$(id).addEventListener('change',applyClientFilters));
$('toggleApiKey').onclick=()=>{const input=$('newApiKey'),btn=$('toggleApiKey'),show=input.type==='password';input.type=show?'text':'password';btn.classList.toggle('visible',show);btn.setAttribute('aria-pressed',show?'true':'false')};
$('apiKeyBtn').onclick=()=>{$('keyModal').classList.add('show');loadKeys(false)};$('addApiKeyBtn').onclick=addApiKey;$('newApiKey').addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();addApiKey()}});$('keyList').addEventListener('click',e=>{const s=e.target.closest('.key-select'),d=e.target.closest('.key-delete'),r=e.target.closest('.key-reveal');if(s)selectKey(s.dataset.id);if(d)deleteKey(d.dataset.id);if(r)toggleRevealKey(r.dataset.id)});loadKeys(true);
updateApiSummary();render();updateStats();restoreSession();checkVersion();
const requestedLoadSettings=new URLSearchParams(location.search).get("load-settings");if(requestedLoadSettings!==null)openLoadSettings(requestedLoadSettings);
</script>
</body>
</html>

"""
