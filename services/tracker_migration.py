# services/tracker_migration.py
# CrossWatch - Background move of the local tracker from JSON files to its database
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import threading
import time
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from cw_platform.tracker_store import TrackerStore, TrackerUnreadable, tracker_roots

_LOG = logging.getLogger("crosswatch.tracker_migration")
_LOCK = threading.RLock()
_STATE: dict[str, Any] = {}
_ROOTS: set[str] = set()


def _key(root: Path | str) -> str:
    return str(Path(root).resolve())


def busy() -> bool:
    with _LOCK:
        return bool(_ROOTS)


def migrating(root: Path | str) -> bool:
    with _LOCK:
        return _key(root) in _ROOTS


def status() -> dict[str, Any] | None:
    with _LOCK:
        return dict(_STATE) if _STATE else None


def start(cfg: Mapping[str, Any], config_dir: Path | str, *, background: bool = True) -> bool:
    global _STATE
    pending: list[tuple[str, Path]] = []
    seen: set[str] = set()
    for inst, root in tracker_roots(cfg, config_dir):
        key = _key(root)
        if key in seen or not TrackerStore(root).needs_migration():
            continue
        seen.add(key)
        pending.append((inst, root))
    if not pending:
        return False
    with _LOCK:
        if _STATE.get("state") == "running":
            return False
        _STATE = {
            "id": uuid.uuid4().hex, "state": "running", "started_at": time.time(),
            "instance": pending[0][0], "instance_index": 1, "instance_count": len(pending),
            "files_done": 0, "files_total": 0, "bytes_done": 0, "bytes_total": 0,
            "results": [], "errors": [],
        }
        _ROOTS.update(seen)
    if background:
        threading.Thread(target=run, args=(pending,), name="tracker-migration", daemon=True).start()
    else:
        run(pending)
    return True


def run(pending: list[tuple[str, Path]]) -> None:
    global _STATE
    for index, (inst, root) in enumerate(pending, 1):
        with _LOCK:
            _STATE.update(instance=inst, instance_index=index, files_done=0, files_total=0, bytes_done=0, bytes_total=0)

        def progress(files_done: int, files_total: int, bytes_done: int, bytes_total: int) -> None:
            with _LOCK:
                _STATE.update(files_done=files_done, files_total=files_total, bytes_done=bytes_done, bytes_total=bytes_total)

        try:
            result = TrackerStore(root).migrate(progress)
            if result is not None:
                with _LOCK:
                    _STATE["results"].append({"instance": inst, **result})
        except TrackerUnreadable as exc:
            _LOG.warning("Tracker %s was left on its JSON files: %s could not be read", inst, exc.names[0])
            with _LOCK:
                _STATE["errors"].append({"instance": inst, "file": exc.names[0]})
        except Exception:
            _LOG.exception("Tracker %s could not be moved to its database", inst)
            with _LOCK:
                _STATE["errors"].append({"instance": inst, "file": None})
        finally:
            with _LOCK:
                _ROOTS.discard(_key(root))
    with _LOCK:
        if not _STATE["results"] and not _STATE["errors"]:
            _STATE = {}
            return
        _STATE.update(state="failed" if _STATE["errors"] else "completed", finished_at=time.time())
        _LOG.info("Tracker move %s: %s moved, %s left on JSON", _STATE["state"], len(_STATE["results"]), len(_STATE["errors"]))
