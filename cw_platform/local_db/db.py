# cw_platform/local_db/db.py
# CrossWatch - Local database connection management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import os
import sqlite3
import sys
import threading
import time
from pathlib import Path
from typing import Any

_LOG = logging.getLogger("crosswatch.local_db")

_LOCK = threading.RLock()
_WARN_INTERVAL = 60.0
_LAST_WARNING: dict[str, float] = {}
_SUPPRESSED: dict[str, int] = {}
_WRITERS: dict[int, tuple[str, str, float, float]] = {}
_STORAGE_CHECKED: set[str] = set()
# A connection owns its transaction state. Sharing it between workers allows
# one worker's commit/rollback to affect another worker's writes.
_CONNECTIONS: dict[threading.Thread, tuple[str, sqlite3.Connection, tuple[int, int] | None]] = {}


class LocalDatabaseError(Exception):
    pass


def _warn(key: str, message: str) -> None:
    now = time.monotonic()
    with _LOCK:
        last = _LAST_WARNING.get(key)
        if last is not None and now - last < _WARN_INTERVAL:
            _SUPPRESSED[key] = _SUPPRESSED.get(key, 0) + 1
            return
        _LAST_WARNING[key] = now
        suppressed = _SUPPRESSED.pop(key, 0)
    if suppressed:
        _LOG.warning("%s (%d similar warnings suppressed)", message, suppressed)
    else:
        _LOG.warning("%s", message)


def _caller(depth: int) -> str:
    frame = sys._getframe(depth)
    module = str(frame.f_globals.get("__name__") or "?").rsplit(".", 1)[-1]
    return f"{module}.{frame.f_code.co_name}"


def active_writers() -> list[dict[str, Any]]:
    now = time.monotonic()
    with _LOCK:
        rows = list(_WRITERS.values())
    return sorted(
        (
            {"operation": op, "thread": thread, "seconds": round(now - started, 1), "since": int(wall)}
            for op, thread, started, wall in rows
        ),
        key=lambda row: row["seconds"],
        reverse=True,
    )


def _holder_note(exclude: int | None = None) -> str:
    now = time.monotonic()
    with _LOCK:
        rows = [row for key, row in _WRITERS.items() if key != exclude]
    if not rows:
        return ""
    op, thread, started, _ = min(rows, key=lambda row: row[2])
    return f" (held by {op} on {thread} for {now - started:.0f}s)"


def _report_locked(conn: sqlite3.Connection) -> None:
    with _LOCK:
        mine = _WRITERS.get(id(conn))
    note = _holder_note(exclude=id(conn)) or " (no open transaction in this process; another process or the storage holds the lock)"
    _warn("locked", f"database is locked in {mine[0] if mine else 'unknown'}{note}")


class _TrackedConnection(sqlite3.Connection):
    _cw_depth = 0

    def __enter__(self) -> sqlite3.Connection:
        if self._cw_depth == 0:
            with _LOCK:
                _WRITERS[id(self)] = (_caller(2), threading.current_thread().name, time.monotonic(), time.time())
        self._cw_depth += 1
        return super().__enter__()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        failure = exc if isinstance(exc, sqlite3.OperationalError) else None
        try:
            return super().__exit__(exc_type, exc, tb)
        except sqlite3.OperationalError as err:
            failure = err
            raise
        finally:
            if failure is not None and "locked" in str(failure):
                _report_locked(self)
            self._cw_depth -= 1
            if self._cw_depth == 0:
                with _LOCK:
                    _WRITERS.pop(id(self), None)


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
    conn = sqlite3.connect(str(p), timeout=5.0, check_same_thread=False, factory=_TrackedConnection)
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
    if str(p) != ":memory:":
        _check_storage(p)
    return conn


def _check_storage(path: Path) -> None:
    key = str(path)
    with _LOCK:
        if key in _STORAGE_CHECKED:
            return
        _STORAGE_CHECKED.add(key)
    from .storage import storage_info

    info = storage_info(path)
    if info["network"]:
        _LOG.warning(
            "local database %s is on a network filesystem (%s); SQLite locking is unreliable there. "
            "Move /config to local storage or set CROSSWATCH_DB to a local path",
            key,
            info["filesystem"],
        )


def _file_id(path: str) -> tuple[int, int] | None:
    if path == ":memory:":
        return (0, 0)
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_dev, st.st_ino)


def get_conn(base_path: str | os.PathLike[str] | None = None) -> sqlite3.Connection | None:
    with _LOCK:
        worker = threading.current_thread()
        # Reap short-lived workers without relying on recycled thread IDs.
        for owner in list(_CONNECTIONS):
            if owner is not worker and not owner.is_alive():
                _, stale, _ = _CONNECTIONS.pop(owner)
                try:
                    stale.close()
                except Exception:
                    pass
        want = str(crosswatch_db_path(base_path))
        cached = _CONNECTIONS.get(worker)
        if cached is not None:
            path, conn, fid = cached
            current = _file_id(want) if path == want else None
            if current is not None and current == fid:
                return conn
            try:
                conn.close()
            except Exception:
                pass
            del _CONNECTIONS[worker]
            if path == want and current is None:
                _LOG.warning("local database file missing; recreating %s", want)
    # Schema setup can wait for another worker's transaction. Do not hold the
    # registry lock while waiting: that worker may need get_conn() to finish.
    try:
        conn = connect(want, base_path=base_path)
        with _LOCK:
            _CONNECTIONS[worker] = (want, conn, _file_id(want))
        return conn
    except Exception as exc:
        note = _holder_note() if "locked" in str(exc) else ""
        _warn(want, f"local database unavailable: {exc}{note}")
        return None


def close_conn() -> None:
    with _LOCK:
        for _, conn, _ in _CONNECTIONS.values():
            try:
                conn.close()
            except Exception:
                pass
        _CONNECTIONS.clear()


def replace_database(base_path: str | os.PathLike[str] | None = None, *, stamp: str | None = None) -> dict[str, Any]:
    path = crosswatch_db_path(base_path)
    backup_dir = path.parent / "backups"
    label = stamp or time.strftime("%Y%m%d_%H%M%S")
    target = backup_dir / f"{path.stem}-{label}{path.suffix}"
    moved: list[str] = []
    with _LOCK:
        busy = active_writers()
        if busy:
            return {"ok": False, "error": "database_busy", "operation": busy[0]["operation"]}
        close_conn()
        backup_dir.mkdir(parents=True, exist_ok=True)
        for suffix in ("", "-wal", "-shm"):
            src = Path(str(path) + suffix)
            if src.exists():
                dst = Path(str(target) + suffix)
                src.replace(dst)
                moved.append(str(dst))
    return {"ok": True, "path": str(path), "backup": str(target) if moved else None, "moved": moved}
