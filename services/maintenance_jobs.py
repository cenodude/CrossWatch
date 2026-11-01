# services/maintenance_jobs.py
# CrossWatch - Persistent status for background database maintenance
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable
from contextlib import ExitStack

from cw_platform.local_db.operation_lock import exclusive_operation

_LOG = logging.getLogger("crosswatch.maintenance")
_LOCK = threading.RLock()
_ACTIVE: dict[str, dict[str, Any]] = {}
_UNSAVED: dict[str, dict[str, Any]] = {}
READ_ONLY_ACTIONS = frozenset({"database-health", "events-health"})


def _path(base: Path) -> Path:
    return base / ".maintenance-job.json"


def _save(base: Path, job: dict[str, Any]) -> None:
    path = _path(base)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(job), encoding="utf-8")
    tmp.replace(path)


def status(base: Path) -> dict[str, Any]:
    with _LOCK:
        key = str(base.resolve())
        if key in _ACTIVE:
            return dict(_ACTIVE[key])
        try:
            job = json.loads(_path(base).read_text(encoding="utf-8"))
        except FileNotFoundError:
            return dict(_UNSAVED.get(key) or {"ok": True, "state": "idle"})
        if key in _UNSAVED and _UNSAVED[key]["id"] == job.get("id"):
            return dict(_UNSAVED[key])
        if job.get("state") in {"queued", "running"}:
            operation = exclusive_operation(base, kind="maintenance")
            try:
                operation.__enter__()
            except RuntimeError:
                return job
            try:
                job.update(state="interrupted", result={"ok": False, "error": "worker_interrupted"})
                try:
                    _save(base, job)
                except Exception:
                    _LOG.exception("Could not persist interrupted maintenance status")
                    _UNSAVED[key] = dict(job)
            finally:
                operation.__exit__(None, None, None)
        return job


def start(base: Path, action: str, work: Callable[[Callable[[str], None]], dict[str, Any]]) -> dict[str, Any]:
    with _LOCK:
        key = str(base.resolve())
        if key in _ACTIVE:
            job = _ACTIVE[key]
            return dict(job) if job["action"] == action else {"ok": False, "error": "maintenance_busy"}
        operation = ExitStack()
        try:
            operation.enter_context(exclusive_operation(base, kind="maintenance"))
            if action not in READ_ONLY_ACTIONS:
                operation.enter_context(exclusive_operation(base))
        except RuntimeError:
            operation.close()
            return {"ok": False, "error": "database_operation_busy"}
        except BaseException:
            operation.close()
            raise
        job = {"ok": True, "id": uuid.uuid4().hex, "action": action, "state": "queued", "step": "queued", "started_at": time.time()}
        try:
            _save(base, job)
        except Exception:
            operation.__exit__(None, None, None)
            raise
        _ACTIVE[key] = job
        _UNSAVED.pop(key, None)

        def progress(step: str) -> None:
            with _LOCK:
                job.update(state="running", step=step)
                _save(base, job)
            _LOG.info("Maintenance %s: %s", action, step)

        def run() -> None:
            outcome = {"state": "interrupted", "result": {"ok": False, "error": "worker_interrupted"}}
            try:
                progress(action)
                result = work(progress)
                outcome = {"state": "completed" if result.get("ok") else "failed", "result": result}
            except BaseException:
                _LOG.exception("Maintenance %s failed", action)
                outcome = {"state": "failed", "result": {"ok": False, "error": "internal_error"}}
            finally:
                with _LOCK:
                    job.update(outcome)
                    job["duration_ms"] = int((time.time() - job["started_at"]) * 1000)
                    try:
                        _save(base, job)
                    except Exception:
                        _LOG.exception("Could not persist maintenance result")
                        job["result"] = {**job["result"], "warnings": [*job["result"].get("warnings", []), "status_persistence_failed"]}
                        _UNSAVED[key] = dict(job)
                    finally:
                        _ACTIVE.pop(key, None)
                        operation.__exit__(None, None, None)
                _LOG.info("Maintenance %s %s in %s ms", action, job["state"], job["duration_ms"])

        try:
            threading.Thread(target=run, name="database-maintenance", daemon=True).start()
        except Exception:
            _ACTIVE.pop(key, None)
            operation.__exit__(None, None, None)
            raise
        return dict(job)
