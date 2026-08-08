# cw_platform/local_db/db.py
# CrossWatch - Local database connection management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import os
import sqlite3
import threading
from pathlib import Path

_LOG = logging.getLogger("crosswatch.local_db")

_LOCK = threading.RLock()
_CONN: sqlite3.Connection | None = None
_CONN_PATH: str | None = None


class LocalDatabaseError(Exception):
    pass


def _config_base() -> Path:
    try:
        from ..config_base import CONFIG_BASE

        return Path(CONFIG_BASE())
    except Exception:
        if Path("/app").exists():
            return Path("/config")
        return Path(__file__).resolve().parents[2]


def crosswatch_db_path(base_path: str | os.PathLike[str] | None = None) -> Path:
    env = (os.getenv("CROSSWATCH_DB") or "").strip()
    if env:
        return Path(env)
    base = Path(base_path) if base_path is not None else _config_base()
    return base / ".cw_databases" / "crosswatch.sqlite3"


def _legacy_events_path(base_path: str | os.PathLike[str] | None = None) -> Path:
    base = Path(base_path) if base_path is not None else _config_base()
    return base / ".cw_databases" / "events.sqlite3"


def _move_legacy_events_db(target: Path, base_path: str | os.PathLike[str] | None = None) -> None:
    if (os.getenv("CROSSWATCH_DB") or "").strip():
        return
    legacy = _legacy_events_path(base_path)
    if target.exists() or not legacy.exists():
        return
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        for suffix in ("", "-wal", "-shm"):
            src = Path(str(legacy) + suffix)
            if src.exists():
                src.replace(Path(str(target) + suffix))
    except Exception as exc:
        _LOG.warning("could not move legacy events database: %s", exc)


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


def connect(
    path: str | os.PathLike[str] | None = None,
    *,
    base_path: str | os.PathLike[str] | None = None,
) -> sqlite3.Connection:
    p = Path(path) if path is not None else crosswatch_db_path(base_path)
    if str(p) != ":memory:":
        try:
            from .legacy_files import move_legacy_artifacts

            move_legacy_artifacts(Path(base_path) if base_path is not None else _config_base())
            _move_legacy_events_db(p, base_path)
            p.parent.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            raise LocalDatabaseError(f"cannot create db dir {p.parent}: {exc}") from exc
    conn = sqlite3.connect(str(p), timeout=5.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        _apply_pragmas(conn)
        from .schema import apply_schema

        apply_schema(conn)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        raise
    return conn


def get_conn(base_path: str | os.PathLike[str] | None = None) -> sqlite3.Connection | None:
    global _CONN, _CONN_PATH
    with _LOCK:
        want = str(crosswatch_db_path(base_path))
        if _CONN is not None and _CONN_PATH == want:
            if want == ":memory:" or Path(want).exists():
                return _CONN
            try:
                _CONN.close()
            except Exception:
                pass
            _CONN = None
            _CONN_PATH = None
            _LOG.warning("local database file missing; recreating %s", want)
        if _CONN is not None:
            try:
                _CONN.close()
            except Exception:
                pass
            _CONN = None
        try:
            _CONN = connect(want, base_path=base_path)
            _CONN_PATH = want
            return _CONN
        except Exception as exc:
            _LOG.warning("local database unavailable: %s", exc)
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
