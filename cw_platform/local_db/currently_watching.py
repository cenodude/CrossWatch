# cw_platform/local_db/currently_watching.py
# CrossWatch - SQLite-backed currently watching storage
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .db import get_conn

STATE_VERSION = 2

_STREAM_COLUMNS = (
    "stream_key",
    "source",
    "provider_instance",
    "media_type",
    "title",
    "year",
    "season",
    "episode",
    "progress",
    "duration_ms",
    "cover",
    "state",
    "updated",
    "started",
    "account",
    "server_uuid",
    "session_key",
    "stored_at",
)


def _now_ns() -> int:
    return int(time.time_ns())


def _s(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _i(value: Any) -> int | None:
    try:
        if value is None or isinstance(value, bool):
            return None
        text = str(value).strip()
        if not text:
            return None
        return int(float(text))
    except Exception:
        return None


def _ids(value: Any) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    out: dict[str, str] = {}
    for key, raw in value.items():
        name = str(key or "").strip()
        if not name or raw in (None, ""):
            continue
        text = str(raw).strip()
        if text:
            out[name] = text
    return out


def _payload_row(stream_key: str, payload: Mapping[str, Any], stored_at: int) -> tuple[Any, ...]:
    return (
        stream_key,
        _s(payload.get("source")) or "unknown",
        _s(payload.get("provider_instance")),
        _s(payload.get("media_type")),
        _s(payload.get("title")),
        _i(payload.get("year")),
        _i(payload.get("season")),
        _i(payload.get("episode")),
        _i(payload.get("progress")),
        _i(payload.get("duration_ms")),
        _s(payload.get("cover")),
        _s(payload.get("state")),
        _i(payload.get("updated")),
        _i(payload.get("started")),
        _s(payload.get("account")),
        _s(payload.get("server_uuid")),
        _s(payload.get("session_key")),
        stored_at,
    )


def _row_payload(row: Any, ids: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source": row["source"],
        "provider_instance": row["provider_instance"] or "",
        "media_type": row["media_type"],
        "title": row["title"] or "",
        "year": row["year"],
        "season": row["season"],
        "episode": row["episode"],
        "progress": row["progress"] or 0,
        "duration_ms": row["duration_ms"],
        "cover": row["cover"],
        "state": row["state"],
        "updated": row["updated"] or 0,
        "started": row["started"] or 0,
        "ids": dict(ids),
        "account": row["account"],
        "server_uuid": row["server_uuid"],
        "session_key": row["session_key"],
    }


def load_streams(base_path: str | Path | None = None) -> dict[str, dict[str, Any]]:
    conn = get_conn(base_path)
    if conn is None:
        return {}
    rows = conn.execute(
        "SELECT stream_key,source,provider_instance,media_type,title,year,season,episode,progress,"
        "duration_ms,cover,state,updated,started,account,server_uuid,session_key "
        "FROM currently_watching_streams ORDER BY updated DESC, stream_key"
    ).fetchall()
    id_rows = conn.execute(
        "SELECT stream_key,id_type,id_value FROM currently_watching_stream_ids ORDER BY stream_key,id_type"
    ).fetchall()
    ids_by_stream: dict[str, dict[str, str]] = {}
    for row in id_rows:
        stream_key = str(row["stream_key"] or "")
        id_type = str(row["id_type"] or "")
        id_value = str(row["id_value"] or "")
        if stream_key and id_type and id_value:
            ids_by_stream.setdefault(stream_key, {})[id_type] = id_value
    streams: dict[str, dict[str, Any]] = {}
    for row in rows:
        stream_key = str(row["stream_key"] or "")
        if stream_key:
            streams[stream_key] = _row_payload(row, ids_by_stream.get(stream_key) or {})
    return streams


def load_state(base_path: str | Path | None = None) -> dict[str, Any]:
    return {"v": STATE_VERSION, "streams": load_streams(base_path)}


def replace_streams(streams: Mapping[str, Any], base_path: str | Path | None = None) -> bool:
    conn = get_conn(base_path)
    if conn is None:
        return False
    stored_at = _now_ns()
    rows: list[tuple[Any, ...]] = []
    id_rows: list[tuple[str, str, str]] = []
    for raw_key, raw_payload in (streams or {}).items():
        stream_key = str(raw_key or "").strip()
        if not stream_key or not isinstance(raw_payload, Mapping):
            continue
        rows.append(_payload_row(stream_key, raw_payload, stored_at))
        for id_type, id_value in _ids(raw_payload.get("ids")).items():
            id_rows.append((stream_key, id_type, id_value))
    placeholders = ",".join("?" for _ in _STREAM_COLUMNS)
    with conn:
        conn.execute("DELETE FROM currently_watching_streams")
        if rows:
            conn.executemany(
                f"INSERT INTO currently_watching_streams({','.join(_STREAM_COLUMNS)}) VALUES({placeholders})",
                rows,
            )
        if id_rows:
            conn.executemany(
                "INSERT INTO currently_watching_stream_ids(stream_key,id_type,id_value) VALUES(?,?,?)",
                id_rows,
            )
    return True


def replace_state(payload: Mapping[str, Any] | None, base_path: str | Path | None = None) -> bool:
    streams = payload.get("streams") if isinstance(payload, Mapping) else {}
    return replace_streams(streams if isinstance(streams, Mapping) else {}, base_path)


def clear_streams(base_path: str | Path | None = None) -> int:
    conn = get_conn(base_path)
    if conn is None:
        return 0
    before = conn.execute("SELECT COUNT(*) AS c FROM currently_watching_streams").fetchone()
    count = int(before["c"] or 0) if before is not None else 0
    with conn:
        conn.execute("DELETE FROM currently_watching_streams")
    return count


def stream_count(base_path: str | Path | None = None, *, active_only: bool = False) -> int:
    conn = get_conn(base_path)
    if conn is None:
        return 0
    if active_only:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM currently_watching_streams WHERE lower(COALESCE(state,'')) IN ('playing','paused','buffering')"
        ).fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) AS c FROM currently_watching_streams").fetchone()
    return int(row["c"] or 0) if row is not None else 0
