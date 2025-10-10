# _maintenanceAPI.py
from fastapi import APIRouter, Body
from typing import Dict, Any, Optional, List
from pathlib import Path
import os, json, shutil

router = APIRouter(prefix="/api/maintenance", tags=["maintenance"])

def _cw():
    from _syncAPI import _load_state
    from crosswatch import CACHE_DIR, CONFIG_DIR, CW_STATE_DIR, STATS, _append_log
    return CACHE_DIR, CONFIG_DIR, CW_STATE_DIR, STATS, _load_state, _append_log

def _safe_remove_path(p: Path) -> bool:
    try:
        if p.is_dir(): shutil.rmtree(p, ignore_errors=True)
        elif p.exists(): p.unlink(missing_ok=True)
        return True
    except Exception:
        return False

def _clear_cw_state_files() -> List[str]:
    _, _, CW_STATE_DIR, *_ = _cw()
    removed=[]
    if not CW_STATE_DIR.exists(): return removed
    for p in CW_STATE_DIR.iterdir():
        if p.is_file():
            try: p.unlink(missing_ok=True); removed.append(p.name)
            except Exception: pass
    return removed

@router.post("/clear-cache")
def clear_cache() -> Dict[str, Any]:
    CACHE_DIR, *_ = _cw()
    deleted_files=deleted_dirs=0
    if CACHE_DIR.exists():
        for e in CACHE_DIR.iterdir():
            try:
                if e.is_dir(): shutil.rmtree(e, ignore_errors=True); deleted_dirs+=1
                else: e.unlink(missing_ok=True); deleted_files+=1
            except Exception: pass
    _cw()[5]("TRBL", "\x1b[91m[TROUBLESHOOT]\x1b[0m Cleared cache folder.")
    return {"ok": True, "deleted_files": deleted_files, "deleted_dirs": deleted_dirs}

@router.post("/reset-stats")
def reset_stats(
    recalc: bool = Body(False),
    purge_file: bool = Body(False),
    purge_state: bool = Body(False),
    purge_reports: bool = Body(False),
) -> Dict[str, Any]:
    CACHE_DIR, CONFIG_DIR, CW_STATE_DIR, STATS, _load_state, _append_log = _cw()
    try:
        # in-memory cleanup
        try:
            from _syncAPI import _summary_reset, _PROVIDER_COUNTS_CACHE, _find_state_path
        except Exception:
            _summary_reset = None; _PROVIDER_COUNTS_CACHE = None; _find_state_path = None
        try:
            from crosswatch import LOG_BUFFERS
        except Exception:
            LOG_BUFFERS = {}

        if _summary_reset: _summary_reset()
        if isinstance(LOG_BUFFERS, dict): LOG_BUFFERS["SYNC"] = []
        if isinstance(_PROVIDER_COUNTS_CACHE, dict):
            _PROVIDER_COUNTS_CACHE["ts"] = 0.0
            _PROVIDER_COUNTS_CACHE["data"] = None

        # stats reset
        STATS.reset()

        # drop statistics.json
        if purge_file:
            try: STATS.path.unlink(missing_ok=True)
            except Exception: pass
            STATS._load(); STATS._save()

        # drop state.json
        if purge_state and _find_state_path:
            try:
                sp = _find_state_path()
                if sp and sp.exists(): sp.unlink()
            except Exception: pass

        # drop sync-*.json to prevent re-ingest
        if purge_reports:
            try:
                try:
                    from _statistics import REPORT_DIR
                except Exception:
                    from pathlib import Path
                    REPORT_DIR = Path("/config/sync_reports")
                for f in REPORT_DIR.glob("sync-*.json"):
                    try: f.unlink()
                    except Exception: pass
            except Exception: pass

        # optional rebuild from current state
        if recalc:
            try:
                state = _load_state()
                if state: STATS.refresh_from_state(state)
            except Exception: pass

        return {
            "ok": True,
            "dropped": {
                "stats_file": bool(purge_file),
                "state_file": bool(purge_state),
                "reports": bool(purge_reports),
            },
            "recalculated": bool(recalc),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.post("/reset-state")
def reset_state(
    mode: str=Body("clear_both"),  # clear_both|clear_state|clear_tombstones|clear_tombstone_entries|clear_cw_state_only|rebuild
    keep_ttl: bool=Body(True),
    ttl_override: Optional[int]=Body(None),
    feature: str=Body("watchlist"),
) -> Dict[str, Any]:
    _, CONFIG_DIR, CW_STATE_DIR, STATS, _load_state, _ = _cw()
    try:
        state_path = CONFIG_DIR / "state.json"
        tomb_path  = CONFIG_DIR / "tombstones.json"
        last_path  = CONFIG_DIR / "last_sync.json"
        hide_path  = CONFIG_DIR / "watchlist_hide.json"
        ratings_changes_path = CONFIG_DIR / "ratings_changes.json"

        cleared: List[str] = []; cw_state: Dict[str, Any] = {}

        def _try_unlink(p: Path, label: str):
            try: p.unlink(missing_ok=True); cleared.append(label)
            except Exception: pass

        def _ls_cw() -> List[str]:
            if not CW_STATE_DIR.exists(): return []
            return sorted([x.name for x in CW_STATE_DIR.iterdir() if x.is_file()])

        if mode in ("clear_state","clear_both","clear_cw_state_only"):
            pre=_ls_cw(); removed=_clear_cw_state_files(); post=_ls_cw()
            cw_state={"path": str(CW_STATE_DIR), "pre": pre, "removed": removed, "post": post}
            if mode != "clear_cw_state_only":
                _try_unlink(state_path,"state.json")
                _try_unlink(last_path,"last_sync.json")
                _try_unlink(ratings_changes_path,"ratings_changes.json")
                _try_unlink(hide_path,"watchlist_hide.json")

        if mode in ("clear_tombstones","clear_both"):
            _try_unlink(tomb_path,"tombstones.json")

        if mode == "clear_tombstone_entries":
            try: t=json.loads(tomb_path.read_text("utf-8")) if tomb_path.exists() else {}
            except Exception: t={}
            t["keys"]={}
            if isinstance(ttl_override,int) and ttl_override>0: t["ttl_sec"]=ttl_override
            elif not keep_ttl: t["ttl_sec"]=2*24*3600
            tmp=tomb_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(t,ensure_ascii=False,indent=2),"utf-8"); os.replace(tmp, tomb_path)

        if mode == "rebuild":
            try:
                from cw_platform.config_base import load_config
                from _syncAPI import _persist_state_via_orc
                from cw_platform.orchestrator import Orchestrator
                state=_persist_state_via_orc(Orchestrator(config=load_config()), feature=feature)
                STATS.refresh_from_state(state)
            except Exception as e:
                return {"ok": False, "error": f"rebuild failed: {e}"}

        if mode not in {"clear_both","clear_state","clear_tombstones","clear_tombstone_entries","clear_cw_state_only","rebuild"}:
            return {"ok": False, "error": f"Unknown mode: {mode}"}

        return {"ok": True, "mode": mode, "cleared": cleared, "cw_state": cw_state}
    except Exception as e:
        return {"ok": False, "error": str(e)}