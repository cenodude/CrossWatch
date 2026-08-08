# cw_platform/run_control.py
# CrossWatch - Cooperative cancellation for sync runs
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import threading
import time
from typing import Any

ANY_RUN = "*"

_LOCK = threading.RLock()
_STATE: dict[str, Any] = {"run_id": None, "requested_at": None, "reason": "", "queue_stopped": False}


class SyncCancelled(RuntimeError):
    pass


def _active_run_id(run_id: Any = None) -> str:
    wanted = str(run_id or "").strip()
    return wanted or str(os.environ.get("CW_RUN_ID") or "").strip()


def request_cancel(run_id: Any = None, reason: str = "user") -> dict[str, Any]:
    with _LOCK:
        _STATE["run_id"] = str(run_id or "").strip() or ANY_RUN
        _STATE["requested_at"] = time.time()
        _STATE["reason"] = str(reason or "user").strip() or "user"
        _STATE["queue_stopped"] = True
        return dict(_STATE)


def clear_cancel() -> None:
    with _LOCK:
        _STATE.update({"run_id": None, "requested_at": None, "reason": ""})


def queue_stopped() -> bool:
    with _LOCK:
        return bool(_STATE.get("queue_stopped"))


def clear_queue_stop() -> None:
    with _LOCK:
        _STATE["queue_stopped"] = False


def consume_queue_stop() -> bool:
    with _LOCK:
        stopped = bool(_STATE.get("queue_stopped"))
        _STATE["queue_stopped"] = False
        return stopped


def cancel_state() -> dict[str, Any]:
    with _LOCK:
        return dict(_STATE)


def cancel_requested(run_id: Any = None) -> bool:
    with _LOCK:
        target = str(_STATE.get("run_id") or "")
        if not target:
            return False
        if target == ANY_RUN:
            return True
        active = _active_run_id(run_id)
        return not active or active == target


def cancel_reason(run_id: Any = None) -> str:
    with _LOCK:
        return str(_STATE.get("reason") or "user") if cancel_requested(run_id) else ""


def raise_if_cancelled(run_id: Any = None) -> None:
    if cancel_requested(run_id):
        raise SyncCancelled("sync cancelled")
