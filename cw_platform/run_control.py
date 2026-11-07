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
_STATE: dict[str, Any] = {"run_id": None, "requested_at": None, "reason": "", "queue_stopped": False, "queue_stop_requested_at": None}


class SyncCancelled(BaseException):
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
        _STATE["queue_stop_requested_at"] = _STATE["requested_at"]
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
        _STATE["queue_stop_requested_at"] = None


def consume_queue_stop() -> bool:
    return consume_queue_stop_requested_at() is not None


def consume_queue_stop_requested_at() -> float | None:
    with _LOCK:
        requested_at = _STATE.get("queue_stop_requested_at") if _STATE.get("queue_stopped") else None
        _STATE["queue_stopped"] = False
        _STATE["queue_stop_requested_at"] = None
        return float(requested_at) if requested_at is not None else None


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
