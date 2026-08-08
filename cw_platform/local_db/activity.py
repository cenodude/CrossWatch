# cw_platform/local_db/activity.py
# CrossWatch - SQLite-backed recent activity storage
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .db import get_conn


def _now() -> int:
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
        if isinstance(raw, (str, int, float, bool)):
            out[name] = str(raw)
    return out


def _row_item(row: Any, ids: Mapping[str, dict[str, str]]) -> dict[str, Any]:
    item = {
        "id": row["event_id"],
        "kind": row["kind"],
        "method": row["method"],
        "event": row["event"],
        "status": row["status"],
        "source": row["source"],
        "source_instance": row["source_instance"],
        "target": row["target"],
        "target_instance": row["target_instance"],
        "media_type": row["media_type"],
        "title": row["title"],
        "year": row["year"],
        "season": row["season"],
        "episode": row["episode"],
        "progress": row["progress"],
        "account": row["account"],
        "watched_at": row["watched_at"],
        "captured_at": row["captured_at"],
        "ids": ids.get(str(row["event_id"] or "")) or {},
    }
    return {key: value for key, value in item.items() if value not in (None, "")}


def save_event(base_path: str | Path | None, item: Mapping[str, Any], *, limit: int = 1000) -> None:
    conn = get_conn(base_path)
    if conn is None:
        return
    event_id = str(item.get("id") or "").strip()
    if not event_id:
        return
    cap = max(1, int(limit or 1000))
    ts = _now()
    id_rows = [(event_id, key, value) for key, value in _ids(item.get("ids")).items()]
    with conn:
        conn.execute(
            "INSERT INTO activity_events(event_id,kind,method,event,status,source,source_instance,target,target_instance,"
            "media_type,title,year,season,episode,progress,account,watched_at,captured_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(event_id) DO UPDATE SET kind=excluded.kind,method=excluded.method,event=excluded.event,"
            "status=excluded.status,source=excluded.source,source_instance=excluded.source_instance,target=excluded.target,"
            "target_instance=excluded.target_instance,media_type=excluded.media_type,title=excluded.title,year=excluded.year,"
            "season=excluded.season,episode=excluded.episode,progress=excluded.progress,account=excluded.account,"
            "watched_at=excluded.watched_at,captured_at=excluded.captured_at,updated_at=excluded.updated_at",
            (
                event_id,
                _s(item.get("kind")) or "activity",
                _s(item.get("method")),
                _s(item.get("event")),
                _s(item.get("status")) or "ok",
                _s(item.get("source")),
                _s(item.get("source_instance")) or "default",
                _s(item.get("target")),
                _s(item.get("target_instance")) or "default",
                _s(item.get("media_type")),
                _s(item.get("title")),
                _i(item.get("year")),
                _i(item.get("season")),
                _i(item.get("episode")),
                _i(item.get("progress")),
                _s(item.get("account")),
                _i(item.get("watched_at")),
                _i(item.get("captured_at")),
                ts,
            ),
        )
        conn.execute("DELETE FROM activity_event_ids WHERE event_id=?", (event_id,))
        if id_rows:
            conn.executemany(
                "INSERT INTO activity_event_ids(event_id,id_type,id_value) VALUES(?,?,?)",
                id_rows,
            )
        conn.execute(
            "DELETE FROM activity_events WHERE event_id NOT IN ("
            "SELECT event_id FROM activity_events ORDER BY captured_at DESC, watched_at DESC, updated_at DESC LIMIT ?"
            ")",
            (cap,),
        )


def list_events(
    base_path: str | Path | None,
    *,
    media_type: str = "all",
    status: str = "all",
    kind: str = "all",
    query: str = "",
    since: int | None = None,
) -> list[dict[str, Any]]:
    conn = get_conn(base_path)
    if conn is None:
        return []
    clauses: list[str] = []
    params: list[Any] = []
    mt = str(media_type or "all").strip().lower()
    st = str(status or "all").strip().lower()
    kd = str(kind or "all").strip().lower()
    q = str(query or "").strip().lower()
    if mt in {"movie", "episode"}:
        clauses.append("media_type=?")
        params.append(mt)
    if st in {"ok", "failed", "error"}:
        clauses.append("status=?")
        params.append("failed" if st == "error" else st)
    if kd != "all":
        clauses.append("kind=?")
        params.append(kd)
    if since is not None:
        clauses.append("COALESCE(captured_at, watched_at, 0)>=?")
        params.append(int(since))
    if q:
        like = f"%{q}%"
        clauses.append(
            "(LOWER(COALESCE(title,'')) LIKE ? OR LOWER(COALESCE(source,'')) LIKE ? OR "
            "LOWER(COALESCE(target,'')) LIKE ? OR LOWER(COALESCE(account,'')) LIKE ? OR "
            "LOWER(COALESCE(media_type,'')) LIKE ? OR LOWER(COALESCE(event,'')) LIKE ? OR "
            "LOWER(COALESCE(method,'')) LIKE ? OR LOWER(COALESCE(source_instance,'')) LIKE ? OR "
            "LOWER(COALESCE(target_instance,'')) LIKE ?)"
        )
        params.extend([like] * 9)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = conn.execute(
        f"SELECT * FROM activity_events{where} ORDER BY captured_at DESC, watched_at DESC, updated_at DESC",
        params,
    ).fetchall()
    event_ids = [str(row["event_id"] or "") for row in rows if row["event_id"]]
    ids_by_event: dict[str, dict[str, str]] = {}
    if event_ids:
        for i in range(0, len(event_ids), 400):
            chunk = event_ids[i:i + 400]
            placeholders = ",".join("?" for _ in chunk)
            id_rows = conn.execute(
                f"SELECT event_id,id_type,id_value FROM activity_event_ids WHERE event_id IN ({placeholders})",
                chunk,
            ).fetchall()
            for id_row in id_rows:
                ids_by_event.setdefault(str(id_row["event_id"] or ""), {})[str(id_row["id_type"] or "")] = str(id_row["id_value"] or "")
    return [_row_item(row, ids_by_event) for row in rows]


def clear_events(base_path: str | Path | None, *, kind: str | None = None) -> dict[str, int | bool]:
    conn = get_conn(base_path)
    if conn is None:
        return {"existed": False, "removed": 0, "remaining": 0}
    wanted = str(kind or "").strip().lower()
    before = int(conn.execute("SELECT COUNT(*) FROM activity_events").fetchone()[0] or 0)
    with conn:
        if wanted:
            cur = conn.execute("DELETE FROM activity_events WHERE kind=?", (wanted,))
        else:
            cur = conn.execute("DELETE FROM activity_events")
    removed = int(cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0)
    remaining = int(conn.execute("SELECT COUNT(*) FROM activity_events").fetchone()[0] or 0)
    return {"existed": before > 0, "removed": removed, "remaining": remaining}


def event_count(base_path: str | Path | None, *, kind: str | None = None) -> int:
    conn = get_conn(base_path)
    if conn is None:
        return 0
    wanted = str(kind or "").strip().lower()
    if wanted:
        row = conn.execute("SELECT COUNT(*) FROM activity_events WHERE kind=?", (wanted,)).fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) FROM activity_events").fetchone()
    return int(row[0] or 0) if row is not None else 0
