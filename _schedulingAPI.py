# _schedulingAPI.py
from fastapi import APIRouter, Body

from typing import Dict, Any
import time

router = APIRouter(prefix="/api/scheduling", tags=["scheduling"])

def _env():
    # import here to avoid circular imports
    from cw_platform.config_base import load_config, save_config
    from crosswatch import scheduler, _SCHED_HINT, _compute_next_run_from_cfg, _UIHostLogger
    return load_config, save_config, scheduler, _SCHED_HINT, _compute_next_run_from_cfg, _UIHostLogger

@router.post("/replan_now")
def replan_now():
    load_config, _, scheduler, HINT, compute_next, log = _env()
    cfg = load_config(); scfg = (cfg.get("scheduling") or {})
    nxt = int(compute_next(scfg))
    HINT["next_run_at"] = nxt; HINT["last_saved_at"] = int(time.time())
    try:
        if scheduler is not None:
            if hasattr(scheduler, "stop"):    scheduler.stop()
            if hasattr(scheduler, "start"):   scheduler.start()
            if hasattr(scheduler, "refresh"): scheduler.refresh()
    except Exception as e:
        try: log("SYNC","SCHED")(f"replan_now worker refresh failed: {e}", level="ERROR")
        except Exception: pass
    try:
        st = scheduler.status(); st["config"] = scfg
        if not int(st.get("next_run_at") or 0):
            st["next_run_at"] = HINT.get("next_run_at", nxt)  # fallback only
    except Exception:
        st = {"next_run_at": nxt, "config": scfg}
    return {"ok": True, **st}


@router.get("")
def sched_get():
    load_config, *_ = _env()
    return (load_config().get("scheduling") or {})

@router.post("")
def sched_post(payload: dict = Body(...)):
    load_config, save_config, scheduler, HINT, compute_next, _ = _env()
    cfg = load_config(); cfg["scheduling"] = (payload or {}); save_config(cfg)
    try:
        nxt = int(compute_next(cfg["scheduling"] or {}))
        HINT["next_run_at"] = nxt; HINT["last_saved_at"] = int(time.time())
    except Exception:
        nxt = 0
    try:
        if (cfg["scheduling"] or {}).get("enabled"):
            if hasattr(scheduler, "start"): scheduler.start()
            if hasattr(scheduler, "refresh"): scheduler.refresh()
        else:
            if hasattr(scheduler, "stop"): scheduler.stop()
        st = scheduler.status(); st["config"] = cfg.get("scheduling") or {}
        return {"ok": True, "next_run_at": int(st.get("next_run_at") or nxt)}
    except Exception:
        return {"ok": True, "next_run_at": int(nxt) if nxt else 0}


@router.get("/status")
def sched_status():
    load_config, _, scheduler, HINT, *_ = _env()
    try:
        st = scheduler.status()
    except Exception:
        st = {}
    try:
        st["config"] = (load_config().get("scheduling") or {})
        live = int(st.get("next_run_at") or 0)
        hint = int((HINT.get("next_run_at") or 0))
        if not live and hint:
            st["next_run_at"] = hint   # fallback only
    except Exception:
        pass
    return st

# tiny helper
@router.get("/next")
def sched_next():
    load_config, *_ , compute_next, _ = _env()
    scfg = (load_config().get("scheduling") or {})
    try: nxt = int(compute_next(scfg))
    except Exception: nxt = 0
    return {"ok": True, "next_run_at": nxt, "config": scfg}
