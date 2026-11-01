# cw_platform/event_archive/maintenance.py
# CrossWatch - Health, optimize and rebuild operations
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path
from typing import Any

from .db import events_db_path, get_conn

_LOG = logging.getLogger("crosswatch.event_archive")
from .schema import SCHEMA_VERSION
from .importer import import_all
from .query import status


_PURGE_TABLES = ("events", "event_groups", "sync_runs", "run_pairs", "event_imports")


def _table_count(conn: sqlite3.Connection, table: str) -> int:
    try:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] or 0)
    except Exception:
        return 0


def _file_size(p: Path) -> int:
    try:
        return p.stat().st_size if p.exists() else 0
    except Exception:
        return 0


def _db_size(p: Path) -> int:
    return sum(_file_size(Path(str(p) + s)) for s in ("", "-wal", "-shm"))


def health(*, conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    path = events_db_path()
    p = Path(str(path))
    exists = p.exists()
    c = conn or get_conn()
    if c is None:
        return {"ok": False, "available": False, "healthy": False, "path": str(path), "exists": exists}
    out: dict[str, Any] = {"ok": True, "available": True, "path": str(path), "exists": exists}
    try:
        integrity = str(c.execute("PRAGMA integrity_check").fetchone()[0])
        quick = str(c.execute("PRAGMA quick_check").fetchone()[0])
        fk = c.execute("PRAGMA foreign_key_check").fetchall()
        ver = int(c.execute("PRAGMA user_version").fetchone()[0] or 0)
        jm = str(c.execute("PRAGMA journal_mode").fetchone()[0])
        st = status(conn=c)
        out.update({
            "healthy": exists and integrity == "ok" and quick == "ok" and not fk and ver == SCHEMA_VERSION,
            "integrity": integrity,
            "quick_check": quick,
            "foreign_key_errors": len(fk),
            "schema_version": ver,
            "expected_schema_version": SCHEMA_VERSION,
            "journal_mode": jm,
            "events": int(st.get("events") or 0),
            "acknowledged": int(st.get("acknowledged") or 0),
            "runs": int(st.get("runs") or 0),
            "first_event": st.get("first_event"),
            "last_event": st.get("last_event"),
            "last_import": st.get("last_import"),
            "size_bytes": _db_size(p),
            "wal_size_bytes": _file_size(Path(str(p) + "-wal")),
        })
        if not exists:
            out["error"] = "database_file_missing"
    except Exception:
        _LOG.exception("events health check failed")
        out.update({"healthy": False, "error": "internal_error"})
    return out


def _boot_result(status: str, ok: bool, path: Any, h: dict[str, Any], message: str) -> dict[str, Any]:
    return {
        "ok": ok, "status": status, "message": message, "path": str(path),
        "schema_version": h.get("schema_version"), "events": int(h.get("events") or 0),
        "size_bytes": int(h.get("size_bytes") or 0),
    }


def boot_check(*, auto_repair: bool = True, state_dir: str | Path | None = None, reports_dir: str | Path | None = None) -> dict[str, Any]:
    path = events_db_path()
    existed = Path(path).exists()
    c = get_conn()
    if c is None:
        return {"ok": False, "status": "error", "message": "Unavailable; run database diagnostics", "path": str(path)}
    started = time.monotonic()
    try:
        ver = int(c.execute("PRAGMA user_version").fetchone()[0])
        events = int(c.execute("SELECT COUNT(*) FROM events").fetchone()[0])
        h = {"schema_version": ver, "events": events, "size_bytes": _db_size(Path(path))}
        healthy = ver == SCHEMA_VERSION
        label = ("ready" if existed else "created") if healthy else "unhealthy"
        return _boot_result(label, healthy, path, h, f"{label.capitalize()} - event schema v{ver} - {events:,} events - shared runtime database")
    except Exception:
        _LOG.exception("Event archive startup check failed")
        return {"ok": False, "status": "error", "message": "Startup check failed; run database diagnostics", "path": str(path)}
    finally:
        _LOG.info("Event archive startup check finished in %s ms", int((time.monotonic() - started) * 1000))


def optimize(*, conn: sqlite3.Connection | None = None, progress=None) -> dict[str, Any]:
    path = Path(events_db_path())
    before = _db_size(path)
    c = conn or get_conn()
    if c is None:
        return {"ok": False, "available": False, "path": str(path)}
    c.commit()
    started = time.monotonic()
    steps: list[str] = []
    timings: dict[str, int] = {}
    checkpoints: dict[str, list[int]] = {}
    for step, stmt in (
        ("checkpoint_before", "PRAGMA wal_checkpoint(TRUNCATE)"),
        ("vacuum", "VACUUM"),
        ("analyze", "ANALYZE"),
        ("optimize", "PRAGMA optimize"),
        ("checkpoint_after", "PRAGMA wal_checkpoint(TRUNCATE)"),
    ):
        if progress:
            progress(step)
        _LOG.info("Database optimization starting %s", step)
        t0 = time.monotonic()
        try:
            cur = c.execute(stmt)
            rows = cur.fetchall()
            cur.close()
            if step.startswith("checkpoint"):
                checkpoints[step] = list(rows[0])
                if rows[0][0] and step == "checkpoint_before":
                    return {"ok": False, "error": "checkpoint_busy", "step": step, "checkpoints": checkpoints}
            steps.append(step)
            timings[step] = int((time.monotonic() - t0) * 1000)
            _LOG.info("Database optimization finished %s in %s ms", step, timings[step])
        except Exception:
            _LOG.exception("Database optimization failed: %s", step)
            return {"ok": False, "error": "optimize_failed", "step": step, "steps": steps, "timings_ms": timings}
    after = _db_size(path)
    return {
        "ok": True, "path": str(path), "steps": steps, "timings_ms": timings,
        "checkpoints": checkpoints, "checkpoint_pending": bool(checkpoints["checkpoint_after"][0]),
        "before_bytes": before, "after_bytes": after, "reclaimed_bytes": max(0, before - after),
        "database_bytes": _file_size(path), "wal_bytes": _file_size(Path(str(path) + "-wal")),
        "duration_ms": int((time.monotonic() - started) * 1000),
    }


def rebuild(
    *,
    state_dir: str | Path | None = None,
    reports_dir: str | Path | None = None,
    reimport: bool = True,
) -> dict[str, Any]:
    path = events_db_path()
    cleared = purge()
    if not cleared.get("ok"):
        return cleared
    removed = cleared.get("removed", {})
    c = get_conn()
    if c is None:
        return {"ok": False, "available": False, "path": str(path), "removed": removed}
    imported = 0
    if reimport:
        try:
            res = import_all(state_dir=state_dir, reports_dir=reports_dir, conn=c)
            imported = int(res.get("imported") or 0)
        except Exception:
            imported = 0
    return {"ok": True, "path": str(path), "removed": removed, "imported": imported, "events": int(status(conn=c).get("events") or 0)}


def purge(domain: str | None = None) -> dict[str, Any]:
    """Delete archived events without touching the rest of the local database.

    `domain` limits the wipe to one event domain; None or "all" clears every
    domain plus the run tables the sync activity calendar reads.
    """
    c = get_conn()
    path = events_db_path()
    if c is None:
        return {"ok": False, "available": False, "path": str(path)}

    scope = str(domain or "").strip().lower()
    before = {t: _table_count(c, t) for t in _PURGE_TABLES}
    try:
        with c:
            if scope in ("", "all"):
                for table in _PURGE_TABLES:
                    c.execute(f"DELETE FROM {table}")
            else:
                c.execute("DELETE FROM events WHERE domain=?", (scope,))
                c.execute("DELETE FROM event_groups WHERE domain=?", (scope,))
                if scope == "sync":
                    for table in ("sync_runs", "run_pairs", "event_imports"):
                        c.execute(f"DELETE FROM {table}")
    except Exception:
        _LOG.exception("events purge failed")
        return {"ok": False, "path": str(path), "error": "purge_failed", "domain": scope or "all"}

    try:
        from .activity import invalidate

        invalidate()
    except Exception:
        pass

    after = {t: _table_count(c, t) for t in _PURGE_TABLES}
    return {
        "ok": True,
        "cleared": True,
        "path": str(path),
        "domain": scope or "all",
        "removed": {t: max(0, before[t] - after[t]) for t in _PURGE_TABLES},
        "remaining": after,
    }
