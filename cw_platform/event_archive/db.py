# cw_platform/event_archive/db.py
# CrossWatch - Event archive database path and connection management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import os
import sqlite3
import threading
from pathlib import Path

_LOG = logging.getLogger("crosswatch.event_archive")

_LOCK = threading.RLock()
_CONN: sqlite3.Connection | None = None
_CONN_PATH: str | None = None


class EventArchiveError(Exception):
    pass


def _config_base() -> Path:
    try:
        from ..config_base import CONFIG_BASE
        return CONFIG_BASE()
    except Exception:
        if Path("/app").exists():
            return Path("/config")
        return Path(__file__).resolve().parents[2]


def events_db_path() -> Path:
    env = (os.getenv("CROSSWATCH_EVENTS_DB") or "").strip()
    if env:
        return Path(env)
    return _config_base() / ".cw_databases" / "events.sqlite3"


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    for pragma in (
        "PRAGMA journal_mode=WAL",
        "PRAGMA synchronous=NORMAL",
        "PRAGMA busy_timeout=5000",
        "PRAGMA foreign_keys=ON",
    ):
        try:
            cur.execute(pragma)
        except Exception:
            pass
    cur.close()


def connect(path: str | os.PathLike[str] | None = None) -> sqlite3.Connection:
    p = Path(path) if path is not None else events_db_path()
    if str(p) != ":memory:":
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            raise EventArchiveError(f"cannot create db dir {p.parent}: {exc}") from exc
    conn = sqlite3.connect(str(p), timeout=5.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    from .schema import apply_schema
    apply_schema(conn)
    return conn


def get_conn() -> sqlite3.Connection | None:
    global _CONN, _CONN_PATH
    with _LOCK:
        want = str(events_db_path())
        if _CONN is not None and _CONN_PATH == want:
            return _CONN
        if _CONN is not None:
            try:
                _CONN.close()
            except Exception:
                pass
            _CONN = None
        try:
            _CONN = connect(want)
            _CONN_PATH = want
            return _CONN
        except Exception as exc:
            _LOG.warning("event archive unavailable: %s", exc)
            _CONN = None
            _CONN_PATH = None
            return None


def close_conn() -> None:
    global _CONN, _CONN_PATH
    with _LOCK:
        if _CONN is not None:
            try:
                _CONN.close()
            except Exception:
                pass
        _CONN = None
        _CONN_PATH = None
