# services/tracker_storage_maintenance.py
# CrossWatch - Background tracker snapshot compression and idle compaction
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from cw_platform.local_db.operation_lock import exclusive_operation
from cw_platform.tracker_store import SnapshotMaintenanceDeferred, TrackerStore, _snapshot_failure_reason, tracker_roots
from services import tracker_migration

_LOG = logging.getLogger("crosswatch.tracker_storage_maintenance")
_LOCK = threading.RLock()
_JOBS: dict[str, dict[str, Any]] = {}
_STOP = threading.Event()
_THREAD: threading.Thread | None = None


def status() -> list[dict[str, Any]]:
    with _LOCK:
        return [dict(job) for job in _JOBS.values()]


def _update(key: str, **values: Any) -> None:
    with _LOCK:
        _JOBS[key].update(values)


def run_once(cfg: Mapping[str, Any], config_dir: Path | str, stop: threading.Event) -> None:
    from api.syncAPI import _is_sync_running

    seen: set[str] = set()
    for instance, root in tracker_roots(cfg, config_dir):
        key = str(root.resolve())
        store = TrackerStore(root)
        if stop.is_set():
            return
        if key in seen or not store.db_path.is_file() or tracker_migration.migrating(root):
            continue
        seen.add(key)
        conn = None
        try:
            conn = store._connect()
            conn.execute("PRAGMA busy_timeout=1000")
            total = int(conn.execute("SELECT count(*) FROM restore_points WHERE typeof(data)='text'").fetchone()[0])
            with _LOCK:
                if total and (key not in _JOBS or _JOBS[key]["state"] == "completed"):
                    _JOBS[key] = {
                        "id": uuid.uuid4().hex, "instance": instance, "state": "running", "phase": "compressing",
                        "started_at": time.time(), "done": 0, "total": total, "bytes_saved": 0,
                    }
                prior_saved = int(_JOBS.get(key, {}).get("bytes_saved", 0))
                prior_done = int(_JOBS.get(key, {}).get("done", 0))

            def progress(done: int, count: int, saved: int) -> None:
                _update(key, state="running", phase="compressing", done=prior_done + done, total=prior_done + count,
                        bytes_saved=prior_saved + saved, finished_at=None)
                stop.wait(0.01)

            if total:
                store._migrate_snapshots(conn, progress, stop.is_set)
            if stop.is_set():
                return
            with _LOCK:
                visible = key in _JOBS and _JOBS[key]["state"] != "completed"
            if visible:
                _update(key, state="running", phase="waiting", reason="idle", finished_at=None)
            if _is_sync_running():
                continue
            with exclusive_operation(config_dir), store.mutation():
                if stop.is_set():
                    return
                if visible:
                    _update(key, phase="compacting")
                outcome = store._compact_snapshots(conn)
            if visible:
                if outcome in {"completed", "skipped"}:
                    _update(key, state="completed", phase="completed", finished_at=time.time())
                else:
                    _update(key, phase="waiting", reason=outcome)
        except SnapshotMaintenanceDeferred as exc:
            _update(key, state="running", phase="waiting", reason=str(exc), finished_at=None)
        except (OSError, sqlite3.Error) as exc:
            reason = _snapshot_failure_reason(exc)
            if reason and key in _JOBS:
                _update(key, state="running", phase="waiting", reason=reason, finished_at=None)
            else:
                _failed(key, instance, exc)
        except RuntimeError as exc:
            if str(exc) != "database_operation_busy":
                _failed(key, instance, exc)
        except Exception as exc:
            _failed(key, instance, exc)
        finally:
            if conn is not None:
                conn.close()


def _failed(key: str, instance: str, exc: Exception) -> None:
    _LOG.warning("Tracker storage maintenance failed for %s: %s", instance, exc)
    with _LOCK:
        job = _JOBS.setdefault(key, {"id": uuid.uuid4().hex, "instance": instance, "started_at": time.time()})
        job.update(state="failed", phase="failed", finished_at=time.time())


def start(load_config: Callable[[], Mapping[str, Any]], config_dir: Path | str) -> None:
    global _THREAD
    with _LOCK:
        if _THREAD is not None and _THREAD.is_alive():
            return
        _STOP.clear()

        def work() -> None:
            while not _STOP.is_set():
                try:
                    run_once(load_config() or {}, config_dir, _STOP)
                except Exception:
                    _LOG.exception("Tracker storage maintenance could not run")
                if _STOP.wait(60):
                    break

        _THREAD = threading.Thread(target=work, name="tracker-storage-maintenance", daemon=True)
        _THREAD.start()


def stop() -> None:
    _STOP.set()
    thread = _THREAD
    if thread is not None:
        thread.join(timeout=2)
