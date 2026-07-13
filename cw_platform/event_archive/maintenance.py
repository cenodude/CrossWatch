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


def _fmt_size(n: Any) -> str:
    size = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{int(size)} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return "0 B"


def _health_issue(h: dict[str, Any]) -> str:
    if not h.get("available"):
        return "database unavailable"
    if not h.get("exists"):
        return "database file missing"
    if h.get("integrity") not in (None, "ok"):
        return f"integrity: {h.get('integrity')}"
    if h.get("quick_check") not in (None, "ok"):
        return f"quick check: {h.get('quick_check')}"
    if h.get("foreign_key_errors"):
        return f"{h.get('foreign_key_errors')} foreign key errors"
    if h.get("schema_version") != h.get("expected_schema_version"):
        return f"schema v{h.get('schema_version')} != v{h.get('expected_schema_version')}"
    return h.get("error") or "unknown issue"


def _boot_result(status: str, ok: bool, path: Any, h: dict[str, Any], message: str) -> dict[str, Any]:
    return {
        "ok": ok, "status": status, "message": message, "path": str(path),
        "schema_version": h.get("schema_version"), "events": int(h.get("events") or 0),
        "size_bytes": int(h.get("size_bytes") or 0),
    }


def boot_check(*, auto_repair: bool = True, state_dir: str | Path | None = None, reports_dir: str | Path | None = None) -> dict[str, Any]:
    path = events_db_path()
    existed = Path(str(path)).exists()
    c = get_conn()
    rebuilt = False
    if c is None and auto_repair:
        rebuild(state_dir=state_dir, reports_dir=reports_dir, reimport=True)
        c = get_conn()
        rebuilt = True
    if c is None:
        return {"ok": False, "status": "error", "message": "Unavailable — cannot open database", "path": str(path)}

    h = health(conn=c)
    if h.get("healthy"):
        ver, events, size = h.get("schema_version"), int(h.get("events") or 0), _fmt_size(h.get("size_bytes"))
        if rebuilt:
            return _boot_result("rebuilt", True, path, h, f"Rebuilt · corruption detected, archive recreated · schema v{ver} · {events:,} events")
        if existed:
            return _boot_result("ready", True, path, h, f"Ready · schema v{ver} · {events:,} events · {size}")
        return _boot_result("created", True, path, h, f"Created · schema v{ver}")

    if not auto_repair:
        return _boot_result("unhealthy", False, path, h, f"Unhealthy — {_health_issue(h)}")

    corrupt = (h.get("integrity") not in (None, "ok")) or (h.get("quick_check") not in (None, "ok")) or bool(h.get("foreign_key_errors"))
    if corrupt:
        rebuild(state_dir=state_dir, reports_dir=reports_dir, reimport=True)
        label, note = "rebuilt", "corruption detected, archive recreated"
    else:
        close_conn()
        label, note = "repaired", "schema updated"

    c2 = get_conn()
    h2 = health(conn=c2) if c2 is not None else {"healthy": False}
    if h2.get("healthy"):
        ver, events = h2.get("schema_version"), int(h2.get("events") or 0)
        return _boot_result(label, True, path, h2, f"{label.capitalize()} · {note} · schema v{ver} · {events:,} events")
    return _boot_result("error", False, path, h2, f"Repair failed — {_health_issue(h2)}")


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
