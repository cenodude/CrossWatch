# cw_platform/event_archive/db.py
# CrossWatch - Event archive database access
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from ..local_db.db import LocalDatabaseError, close_conn, connect as _connect, crosswatch_db_path, get_conn as _get_conn

EventArchiveError = LocalDatabaseError


def events_db_path() -> Path:
    return crosswatch_db_path()


def _apply_event_schema(conn: sqlite3.Connection) -> sqlite3.Connection:
    from .schema import apply_schema

    apply_schema(conn)
    return conn


def connect(path: str | os.PathLike[str] | None = None) -> sqlite3.Connection:
    return _apply_event_schema(_connect(path))


def get_conn() -> sqlite3.Connection | None:
    conn = _get_conn()
    if conn is None:
        return None
    try:
        return _apply_event_schema(conn)
    except Exception:
        return None
