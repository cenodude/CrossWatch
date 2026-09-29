# cw_platform/event_archive/retention.py
# CrossWatch - Event archive and sync report retention
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import sqlite3
import threading
import time
from typing import Any

from .db import get_conn

_LOG = logging.getLogger("crosswatch.event_archive")

DAY = 86400
THREAD_DAYS = {"sync": 365, "scrobble": 365, "audit": 90}
KEEP_OPEN_DOMAINS = ("sync", "scrobble")
OPEN_STATUSES = ("failed", "unresolved", "blackboxed")
REPORT_KEEP = 500
INTERVAL = DAY
STARTUP_DELAY = 600

_CHUNK = 500
_worker: threading.Thread | None = None
_worker_lock = threading.Lock()
_stop = threading.Event()


def _chunks(ids: list[Any]) -> list[list[Any]]:
    return [ids[i:i + _CHUNK] for i in range(0, len(ids), _CHUNK)]


def _prune_threads(c: sqlite3.Connection, domain: str, cutoff: int) -> tuple[int, int]:
    sql = "SELECT id FROM event_groups WHERE COALESCE(domain,'sync')=? AND COALESCE(last_event_at, created_at, 0)<?"
    params: list[Any] = [domain, cutoff]
    if domain in KEEP_OPEN_DOMAINS:
        sql += f" AND NOT (acknowledged_at IS NULL AND status IN ({','.join('?' for _ in OPEN_STATUSES)}))"
        params.extend(OPEN_STATUSES)
    gids = [int(r[0]) for r in c.execute(sql, params).fetchall()]
    events = 0
    for part in _chunks(gids):
        qm = ",".join("?" for _ in part)
        with c:
            events += int(c.execute(f"DELETE FROM events WHERE group_id IN ({qm})", part).rowcount or 0)
            c.execute(f"DELETE FROM event_groups WHERE id IN ({qm})", part)
    return len(gids), events


def _prune_runs(c: sqlite3.Connection, cutoff: int) -> int:
    run_ids = [str(r[0]) for r in c.execute(
        "SELECT run_id FROM sync_runs WHERE COALESCE(finished_at, started_at, 0)<? "
        "AND NOT EXISTS (SELECT 1 FROM events e WHERE e.run_id=sync_runs.run_id)",
        (cutoff,),
    ).fetchall()]
    for part in _chunks(run_ids):
        qm = ",".join("?" for _ in part)
        with c:
            c.execute(f"DELETE FROM run_pairs WHERE run_id IN ({qm})", part)
            c.execute(f"DELETE FROM sync_runs WHERE run_id IN ({qm})", part)
    return len(run_ids)


def apply_retention(*, conn: sqlite3.Connection | None = None, now: int | None = None) -> dict[str, Any]:
    c = conn or get_conn()
    if c is None:
        return {"ok": False, "available": False}
    ts = int(now or time.time())
    out: dict[str, Any] = {"ok": True, "threads": {}, "events": 0, "runs": 0, "reports": 0}
    report_rows = 0
    from .groups import _GROUP_LOCK, correlate

    try:
        correlate(conn=c)
        with _GROUP_LOCK:
            for domain, days in THREAD_DAYS.items():
                groups, events = _prune_threads(c, domain, ts - days * DAY)
                out["threads"][domain] = groups
                out["events"] += events
            out["runs"] = _prune_runs(c, ts - THREAD_DAYS["sync"] * DAY)
    except Exception as exc:
        _LOG.warning("event retention failed: %s", exc, exc_info=True)
        return {"ok": False, "error": "internal_error"}
    try:
        from ..local_db.sync_reports import prune_reports

        out["reports"], report_rows = prune_reports(REPORT_KEEP, conn=c)
    except Exception as exc:
        _LOG.warning("sync report retention failed: %s", exc, exc_info=True)
    removed = out["events"] + out["runs"] + report_rows
    if removed:
        _LOG.info(
            "retention removed events=%s threads=%s runs=%s reports=%s",
            out["events"], out["threads"], out["runs"], out["reports"],
        )
    return out


def _loop() -> None:
    try:
        from .groups import refresh_run_summaries

        refresh_run_summaries()
    except Exception as exc:
        _LOG.warning("run summary refresh failed: %s", exc, exc_info=True)
    if _stop.wait(STARTUP_DELAY):
        return
    try:
        from .scrobble_recorder import prune_superseded_backfill

        prune_superseded_backfill()
    except Exception as exc:
        _LOG.warning("scrobble prune backfill failed: %s", exc, exc_info=True)
    while not _stop.is_set():
        apply_retention()
        _stop.wait(INTERVAL)


def start_worker() -> bool:
    global _worker
    with _worker_lock:
        if _worker is not None and _worker.is_alive():
            return False
        _stop.clear()
        _worker = threading.Thread(target=_loop, name="event-retention", daemon=True)
        _worker.start()
        return True


def stop_worker() -> None:
    _stop.set()
