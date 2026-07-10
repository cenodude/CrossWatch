# cw_platform/event_archive/maintenance.py
# CrossWatch - Health, optimize and rebuild operations
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path
from typing import Any

from .db import events_db_path, get_conn, close_conn

_LOG = logging.getLogger("crosswatch.event_archive")
from .schema import SCHEMA_VERSION
from .importer import import_all
from .query import status


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


def optimize(*, conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    path = events_db_path()
    p = Path(str(path))
    before = _db_size(p)
    c = conn or get_conn()
    if c is None:
        return {"ok": False, "available": False, "path": str(path)}
    try:
        c.commit()
    except Exception:
        pass
    t0 = time.time()
    steps: list[str] = []
    for stmt in ("PRAGMA wal_checkpoint(TRUNCATE)", "VACUUM", "ANALYZE", "PRAGMA optimize"):
        step = stmt.split("(")[0].replace("PRAGMA ", "").strip()
        try:
            c.execute(stmt)
            steps.append(step)
        except Exception:
            _LOG.exception("events optimize step failed: %s", step)
            return {"ok": False, "path": str(path), "error": "optimize_failed", "step": step, "before_bytes": before, "steps": steps}
    after = _db_size(p)
    return {
        "ok": True, "path": str(path), "steps": steps,
        "before_bytes": before, "after_bytes": after, "reclaimed_bytes": max(0, before - after),
        "duration_ms": int((time.time() - t0) * 1000),
    }


def rebuild(
    *,
    state_dir: str | Path | None = None,
    reports_dir: str | Path | None = None,
    reimport: bool = True,
) -> dict[str, Any]:
    path = events_db_path()
    close_conn()
    removed: list[str] = []
    for suffix in ("", "-wal", "-shm"):
        fp = Path(str(path) + suffix)
        try:
            if fp.exists():
                fp.unlink()
                removed.append(fp.name)
        except Exception:
            _LOG.exception("events rebuild could not remove %s", fp.name)
            return {"ok": False, "path": str(path), "error": "remove_failed", "file": fp.name}
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
