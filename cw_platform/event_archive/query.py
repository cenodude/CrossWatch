# cw_platform/event_archive/query.py
# CrossWatch - Bounded, paginated read queries
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any

from .db import get_conn, events_db_path

_LOG = logging.getLogger("crosswatch.event_archive")

_MAX_LIMIT = 500
_DEFAULT_LIMIT = 50

_COLUMNS = (
    "id", "event_hash", "created_at", "run_id", "event_type", "severity",
    "feature", "operation", "pair_key", "direction",
    "source_provider", "source_instance", "destination_provider", "destination_instance",
    "origin_provider", "origin_instance", "origin_confidence",
    "item_key", "title", "year", "media_type", "season", "episode",
    "old_value", "new_value", "value_type", "reason_code", "reason",
    "match_basis", "source_kind", "source_file", "source_mtime", "detail",
    "acknowledged_at", "acknowledged_by",
)


def _bound_limit(limit: Any) -> int:
    try:
        n = int(limit)
    except Exception:
        return _DEFAULT_LIMIT
    if n <= 0:
        return _DEFAULT_LIMIT
    return min(n, _MAX_LIMIT)


def _bound_offset(offset: Any) -> int:
    try:
        n = int(offset)
    except Exception:
        return 0
    return max(0, n)


def _vis_clause(visibility: str | None) -> str | None:
    v = str(visibility or "open").strip().lower()
    if v == "all":
        return None
    if v == "acknowledged":
        return "(acknowledged_at IS NOT NULL OR EXISTS (SELECT 1 FROM event_groups g WHERE g.id=events.group_id AND g.acknowledged_at IS NOT NULL))"
    return "acknowledged_at IS NULL AND NOT EXISTS (SELECT 1 FROM event_groups g WHERE g.id=events.group_id AND g.acknowledged_at IS NOT NULL)"


def _order_dir(order: str | None) -> str:
    return "ASC" if str(order or "").strip().lower() in ("oldest", "asc") else "DESC"


def _run(
    conn: sqlite3.Connection | None,
    clauses: list[str],
    params: list[Any],
    limit: int,
    offset: int,
    visibility: str | None,
    order: str | None = "newest",
) -> dict[str, Any]:
    c = conn or get_conn()
    if c is None:
        return {"items": [], "total": 0, "limit": limit, "offset": offset}
    all_clauses = list(clauses)
    vc = _vis_clause(visibility)
    if vc:
        all_clauses.append(vc)
    where = (" WHERE " + " AND ".join(all_clauses)) if all_clauses else ""
    lim = _bound_limit(limit)
    off = _bound_offset(offset)
    dir_ = _order_dir(order)
    try:
        total = int(c.execute(f"SELECT COUNT(*) FROM events{where}", params).fetchone()[0])
        rows = c.execute(
            f"SELECT {','.join(_COLUMNS)} FROM events{where} ORDER BY created_at {dir_}, id {dir_} LIMIT ? OFFSET ?",
            [*params, lim, off],
        ).fetchall()
    except Exception:
        return {"items": [], "total": 0, "limit": lim, "offset": off}
    return {"items": [dict(r) for r in rows], "total": total, "limit": lim, "offset": off}


def recent(*, limit: int = _DEFAULT_LIMIT, offset: int = 0, visibility: str | None = "open", order: str | None = "newest", conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    return _run(conn, [], [], limit, offset, visibility, order)


def search(
    *,
    q: str | None = None,
    event_type: str | None = None,
    provider: str | None = None,
    origin_provider: str | None = None,
    destination_provider: str | None = None,
    source_provider: str | None = None,
    feature: str | None = None,
    pair_key: str | None = None,
    run_id: str | None = None,
    reason_code: str | None = None,
    since: int | None = None,
    until: int | None = None,
    visibility: str | None = "open",
    order: str | None = "newest",
    limit: int = _DEFAULT_LIMIT,
    offset: int = 0,
    conn: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    clauses: list[str] = []
    params: list[Any] = []

    def eq(col: str, val: Any) -> None:
        if val not in (None, ""):
            clauses.append(f"{col}=?")
            params.append(val)

    if q:
        like = f"%{q}%"
        clauses.append("(title LIKE ? OR item_key LIKE ? OR reason LIKE ? OR reason_code LIKE ?)")
        params.extend([like, like, like, like])

    eq("event_type", event_type)
    eq("feature", feature)
    eq("pair_key", pair_key)
    eq("run_id", run_id)
    eq("reason_code", reason_code)
    eq("origin_provider", (origin_provider or "").upper() or None)
    eq("destination_provider", (destination_provider or "").upper() or None)
    eq("source_provider", (source_provider or "").upper() or None)

    if provider:
        p = provider.upper()
        clauses.append("(source_provider=? OR destination_provider=? OR origin_provider=?)")
        params.extend([p, p, p])

    if since not in (None, ""):
        try:
            clauses.append("created_at>=?")
            params.append(int(since))
        except Exception:
            pass
    if until not in (None, ""):
        try:
            clauses.append("created_at<=?")
            params.append(int(until))
        except Exception:
            pass

    return _run(conn, clauses, params, limit, offset, visibility, order)


def by_item(item_key: str, *, limit: int = _MAX_LIMIT, offset: int = 0, visibility: str | None = "all", conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    return _run(conn, ["item_key=?"], [item_key], limit, offset, visibility)


def by_run(run_id: str, *, limit: int = _MAX_LIMIT, offset: int = 0, visibility: str | None = "all", conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    return _run(conn, ["run_id=?"], [run_id], limit, offset, visibility)


def acknowledge(event_id: Any, *, by: str | None = None, conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    c = conn or get_conn()
    if c is None:
        return {"ok": False, "available": False}
    try:
        eid = int(event_id)
    except Exception:
        return {"ok": False, "error": "bad_id"}
    ts = int(time.time())
    try:
        with c:
            c.execute(
                "UPDATE events SET acknowledged_at=?, acknowledged_by=? WHERE id=? AND acknowledged_at IS NULL",
                (ts, by, eid),
            )
        row = c.execute("SELECT acknowledged_at, acknowledged_by FROM events WHERE id=?", (eid,)).fetchone()
    except Exception:
        _LOG.exception("acknowledge failed")
        return {"ok": False, "error": "internal_error"}
    if row is None:
        return {"ok": False, "id": eid, "found": False}
    return {"ok": True, "id": eid, "acknowledged_at": int(row[0]) if row[0] is not None else ts, "acknowledged_by": row[1]}


def unacknowledge(event_id: Any, *, conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    c = conn or get_conn()
    if c is None:
        return {"ok": False, "available": False}
    try:
        eid = int(event_id)
    except Exception:
        return {"ok": False, "error": "bad_id"}
    try:
        with c:
            c.execute("UPDATE events SET acknowledged_at=NULL, acknowledged_by=NULL WHERE id=?", (eid,))
        row = c.execute("SELECT id FROM events WHERE id=?", (eid,)).fetchone()
    except Exception:
        _LOG.exception("unacknowledge failed")
        return {"ok": False, "error": "internal_error"}
    if row is None:
        return {"ok": False, "id": eid, "found": False}
    return {"ok": True, "id": eid, "acknowledged_at": None}


def status(*, conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    c = conn or get_conn()
    path = str(events_db_path())
    if c is None:
        return {"ok": False, "available": False, "path": path, "events": 0, "runs": 0}
    out: dict[str, Any] = {"ok": True, "available": True, "path": path}
    try:
        out["events"] = int(c.execute("SELECT COUNT(*) FROM events").fetchone()[0])
        out["acknowledged"] = int(c.execute("SELECT COUNT(*) FROM events WHERE acknowledged_at IS NOT NULL").fetchone()[0])
        out["runs"] = int(c.execute("SELECT COUNT(*) FROM sync_runs").fetchone()[0])
        row = c.execute("SELECT MIN(created_at), MAX(created_at) FROM events").fetchone()
        out["first_event"] = int(row[0]) if row and row[0] is not None else None
        out["last_event"] = int(row[1]) if row and row[1] is not None else None
        imp = c.execute("SELECT MAX(last_imported_at) FROM event_imports").fetchone()
        out["last_import"] = int(imp[0]) if imp and imp[0] is not None else None
        by_type = c.execute("SELECT event_type, COUNT(*) FROM events GROUP BY event_type ORDER BY 2 DESC").fetchall()
        out["by_type"] = {str(r[0] or ""): int(r[1]) for r in by_type}
    except Exception:
        out["available"] = False
    return out
