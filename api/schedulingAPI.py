# /api/schedulingAPI.py
# CrossWatch - Scheduling API for task management
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import time
from typing import Any, Callable

from fastapi import APIRouter, Body

router = APIRouter(prefix="/api/scheduling", tags=["scheduling"])


def _env() -> tuple[
    Callable[..., dict[str, Any]],
    Callable[..., None],
    Any,
    dict[str, Any],
    Callable[..., int],
    Callable[..., Any],
]:
    try:
        from cw_platform.config_base import (
            load_config as _load_cfg,
            save_config as _save_cfg,
        )
        from crosswatch import (
            scheduler as _scheduler,
            _SCHED_HINT,
            _compute_next_run_from_cfg,
            _UIHostLogger,
        )

        return (
            _load_cfg,
            _save_cfg,
            _scheduler,
            _SCHED_HINT,
            _compute_next_run_from_cfg,
            _UIHostLogger,
        )
    except Exception:
        def _load_cfg() -> dict[str, Any]:
            return {}

        def _save_cfg(*args: Any, **kwargs: Any) -> None:
            return None

        class _DummyScheduler:
            def status(self) -> dict[str, Any]:
                return {}

            def start(self) -> None:
                return None

            def stop(self) -> None:
                return None

            def refresh(self) -> None:
                return None

        def _compute_next_run_from_cfg(*args: Any, **kwargs: Any) -> int:
            return 0

        def _ui_host_logger(*_args: Any, **_kwargs: Any) -> Callable[..., None]:
            def _inner(*_a: Any, **_k: Any) -> None:
                return None
            return _inner

        return _load_cfg, _save_cfg, _DummyScheduler(), {}, _compute_next_run_from_cfg, _ui_host_logger


@router.post("/replan_now")
def replan_now() -> dict[str, Any]:
    load_config, _, scheduler, hint, compute_next, log = _env()
    cfg = load_config() or {}
    scfg = (cfg.get("scheduling") or {}) or {}

    try:
        nxt = int(compute_next(scfg) or 0)
    except Exception:
        nxt = 0

    now = int(time.time())
    try:
        hint["next_run_at"] = nxt
        hint["last_saved_at"] = now
    except Exception:
        pass

    try:
        if scheduler is not None:
            if hasattr(scheduler, "stop"):
                scheduler.stop()
            if hasattr(scheduler, "start"):
                scheduler.start()
            if hasattr(scheduler, "refresh"):
                scheduler.refresh()
    except Exception as e:
        try:
            log("SYNC", "SCHED")(f"replan_now worker refresh failed: {e}", level="ERROR")
        except Exception:
            pass

    try:
        st = scheduler.status()  # type: ignore[union-attr]
        st["config"] = scfg
        if not int(st.get("next_run_at") or 0):
            st["next_run_at"] = hint.get("next_run_at", nxt)
    except Exception:
        st = {"next_run_at": nxt, "config": scfg}

    return {"ok": True, **st}

@router.get("")
def sched_get() -> dict[str, Any]:
    load_config, *_ = _env()
    cfg = load_config() or {}
    return (cfg.get("scheduling") or {}) or {}

@router.post("")
def sched_post(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    load_config, save_config, scheduler, hint, compute_next, _ = _env()
    cfg = load_config() or {}
    cfg["scheduling"] = (payload or {}) or {}
    save_config(cfg)

    scfg = cfg["scheduling"] or {}
    try:
        nxt = int(compute_next(scfg) or 0)
        hint["next_run_at"] = nxt
        hint["last_saved_at"] = int(time.time())
    except Exception:
        nxt = 0

    try:
        if (scfg or {}).get("enabled"):
            if hasattr(scheduler, "start"):
                scheduler.start()
            if hasattr(scheduler, "refresh"):
                scheduler.refresh()
        else:
            if hasattr(scheduler, "stop"):
                scheduler.stop()

        st = scheduler.status()  # type: ignore[union-attr]
        st["config"] = scfg
        return {"ok": True, "next_run_at": int(st.get("next_run_at") or nxt or 0)}
    except Exception:
        return {"ok": True, "next_run_at": int(nxt) if nxt else 0}

@router.get("/status")
def sched_status() -> dict[str, Any]:
    load_config, _, scheduler, hint, *_ = _env()
    try:
        st = scheduler.status()  # type: ignore[union-attr]
    except Exception:
        st = {}

    try:
        cfg = load_config() or {}
        st["config"] = (cfg.get("scheduling") or {}) or {}
        live = int(st.get("next_run_at") or 0)
        hint_val = int((hint.get("next_run_at") or 0)) if isinstance(hint, dict) else 0
        if not live and hint_val:
            st["next_run_at"] = hint_val
    except Exception:
        pass

    return st

@router.get("/next")
def sched_next() -> dict[str, Any]:
    load_config, _, _, _, compute_next, _ = _env()
    
    cfg = load_config() or {}
    scfg = (cfg.get("scheduling") or {}) or {}
    try:
        nxt = int(compute_next(scfg) or 0)
    except Exception:
        nxt = 0
    return {"ok": True, "next_run_at": nxt, "config": scfg}