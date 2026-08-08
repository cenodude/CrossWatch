# cw_platform/local_db/last_sync.py
# CrossWatch - SQLite-backed last sync storage
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .db import get_conn

_LOCK = threading.RLock()


def _now() -> int:
    return int(time.time_ns())


def _scalar_parts(value: Any) -> tuple[str | None, int | None, float | None, str | None]:
    if isinstance(value, bool):
        return None, 1 if value else 0, None, "bool"
    if isinstance(value, int):
        return None, int(value), None, "int"
    if isinstance(value, float):
        return None, None, float(value), "real"
    if value in (None, ""):
        return None, None, None, None
    return str(value), None, None, "text"


def _scalar_from_values(value_text: Any, value_int: Any, value_real: Any, value_type: Any) -> Any:
    typ = str(value_type or "")
    if typ == "bool":
        return bool(value_int)
    if typ == "int":
        return value_int
    if typ == "real":
        return value_real
    if typ == "text":
        return value_text
    return None


def _set_scalar_row(conn: Any, table: str, key_col: str, key: str, value: Any, ts: int) -> None:
    text, iv, rv, typ = _scalar_parts(value)
    if typ is None:
        conn.execute(f"DELETE FROM {table} WHERE {key_col}=?", (key,))
        return
    conn.execute(
        f"INSERT INTO {table}({key_col},value_text,value_int,value_real,value_type,updated_at) VALUES(?,?,?,?,?,?) "
        f"ON CONFLICT({key_col}) DO UPDATE SET value_text=excluded.value_text,value_int=excluded.value_int,"
        "value_real=excluded.value_real,value_type=excluded.value_type,updated_at=excluded.updated_at",
        (key, text, iv, rv, typ, ts),
    )


def load_last_sync(base_path: str | Path) -> dict[str, Any]:
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return {}
        row = conn.execute("SELECT started_at,finished_at FROM last_sync_summary WHERE id=1").fetchone()
        out: dict[str, Any] = {}
        if row is not None:
            if row["started_at"] is not None:
                out["started_at"] = row["started_at"]
            if row["finished_at"] is not None:
                out["finished_at"] = row["finished_at"]
        fields = conn.execute("SELECT * FROM last_sync_fields ORDER BY key").fetchall()
        for item in fields:
            value = _scalar_from_values(item["value_text"], item["value_int"], item["value_real"], item["value_type"])
            if value is not None:
                out[str(item["key"])] = value
        result_rows = conn.execute("SELECT * FROM last_sync_result_metrics ORDER BY key").fetchall()
        if result_rows:
            result: dict[str, Any] = {}
            for item in result_rows:
                value = _scalar_from_values(item["value_text"], item["value_int"], item["value_real"], item["value_type"])
                if value is not None:
                    result[str(item["key"])] = value
            out["result"] = result
        timeline_rows = conn.execute("SELECT flag,value FROM last_sync_timeline ORDER BY flag").fetchall()
        if timeline_rows:
            out["timeline"] = {str(item["flag"]): bool(item["value"]) for item in timeline_rows}
        return out


def save_last_sync(base_path: str | Path, data: Mapping[str, Any]) -> None:
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return
        ts = _now()
        raw_result = data.get("result")
        result: Mapping[str, Any] = raw_result if isinstance(raw_result, Mapping) else {}
        raw_timeline = data.get("timeline")
        timeline: Mapping[str, Any] = raw_timeline if isinstance(raw_timeline, Mapping) else {}
        started_at = data.get("started_at")
        finished_at = data.get("finished_at")
        try:
            started_i = int(started_at) if started_at not in (None, "") else None
        except Exception:
            started_i = None
        try:
            finished_i = int(finished_at) if finished_at not in (None, "") else None
        except Exception:
            finished_i = None
        with conn:
            conn.execute(
                "INSERT INTO last_sync_summary(id,started_at,finished_at,updated_at) VALUES(1,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET started_at=excluded.started_at,finished_at=excluded.finished_at,updated_at=excluded.updated_at",
                (started_i, finished_i, ts),
            )
            conn.execute("DELETE FROM last_sync_fields")
            conn.execute("DELETE FROM last_sync_result_metrics")
            conn.execute("DELETE FROM last_sync_timeline")
            for key, value in data.items():
                k = str(key or "")
                if k in {"started_at", "finished_at", "result", "timeline"}:
                    continue
                _set_scalar_row(conn, "last_sync_fields", "key", k, value, ts)
            for key, value in result.items():
                _set_scalar_row(conn, "last_sync_result_metrics", "key", str(key or ""), value, ts)
            for key, value in timeline.items():
                conn.execute(
                    "INSERT INTO last_sync_timeline(flag,value,updated_at) VALUES(?,?,?)",
                    (str(key or ""), 1 if bool(value) else 0, ts),
                )


def clear_last_sync(base_path: str | Path) -> None:
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return
        with conn:
            conn.execute("DELETE FROM last_sync_summary")
            conn.execute("DELETE FROM last_sync_fields")
            conn.execute("DELETE FROM last_sync_result_metrics")
            conn.execute("DELETE FROM last_sync_timeline")
