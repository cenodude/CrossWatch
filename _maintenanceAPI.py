# _maintenanceAPI.py
# CrossWatch - Maintenance API for CrossWatch
# Copyright (c) 2025 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from fastapi import APIRouter, Body
from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime
import os, json, shutil, threading

router = APIRouter(prefix="/api/maintenance", tags=["maintenance"])

def _cw():
    from _syncAPI import _load_state
    from crosswatch import CACHE_DIR, CONFIG_DIR, CW_STATE_DIR, STATS, _append_log
    return CACHE_DIR, CONFIG_DIR, CW_STATE_DIR, STATS, _load_state, _append_log

def _safe_remove_path(p: Path) -> bool:
    try:
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        elif p.exists():
            p.unlink(missing_ok=True)
        return True
    except Exception:
        return False

def _clear_cw_state_files() -> List[str]:
    _, _, CW_STATE_DIR, *_ = _cw()
    removed: List[str] = []
    if not CW_STATE_DIR.exists():
        return removed
    for p in CW_STATE_DIR.iterdir():
        if p.is_file():
            try:
                p.unlink(missing_ok=True)
                removed.append(p.name)
            except Exception:
                pass
    return removed

# --- CrossWatch tracker helpers -------------------------------------------------
def _cw_tracker_root(config_dir: Path) -> Path:
    """Resolve CrossWatch tracker root dir from config.json or default."""
    cfg_path = config_dir / "config.json"
    root: Optional[str] = None
    try:
        cfg = json.loads(cfg_path.read_text("utf-8"))
        cw_cfg = cfg.get("crosswatch") or {}
        root = (
            cw_cfg.get("root_dir")
            or cw_cfg.get("root")
            or cw_cfg.get("dir")
            or None
        )
    except Exception:
        root = None

    if not root:
        root = ".cw_provider"

    p = Path(root)
    if not p.is_absolute():
        p = config_dir / p
    return p

def _file_meta(path: Path) -> Dict[str, Any]:
    try:
        st = path.stat()
    except Exception:
        return {"name": path.name, "size": 0, "mtime": None}
    return {
        "name": path.name,
        "size": st.st_size,
        "mtime": datetime.utcfromtimestamp(st.st_mtime).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
    }
def _scan_provider_cache() -> Dict[str, Any]:
    _, _, CW_STATE_DIR, *_ = _cw()
    exists = CW_STATE_DIR.exists()
    out: Dict[str, Any] = {
        "exists": exists,
        "root": str(CW_STATE_DIR),
        "files": [],
        "count": 0,
    }
    if not exists:
        return out

    files: List[Dict[str, Any]] = []
    for p in CW_STATE_DIR.glob("*.json"):
        if p.is_file():
            files.append(_file_meta(p))

    files.sort(key=lambda x: x.get("name") or "")
    out["files"] = files
    out["count"] = len(files)
    return out

def _scan_cw_tracker(root: Path) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "exists": root.exists(),
        "state_files": [],
        "snapshots": [],
        "counts": {"state_files": 0, "snapshots": 0},
    }
    if not root.exists():
        return out

    state_files: List[Dict[str, Any]] = []
    for p in root.glob("*.json"):
        if p.is_file():
            state_files.append(_file_meta(p))

    snaps_dir = root / "snapshots"
    snapshots: List[Dict[str, Any]] = []
    if snaps_dir.exists():
        for p in snaps_dir.glob("*.json"):
            if p.is_file():
                snapshots.append(_file_meta(p))

    state_files.sort(key=lambda x: x.get("name") or "")
    snapshots.sort(key=lambda x: x.get("mtime") or "")

    out["state_files"] = state_files
    out["snapshots"] = snapshots
    out["counts"] = {
        "state_files": len(state_files),
        "snapshots": len(snapshots),
    }
    return out

@router.get("/crosswatch-tracker")
def crosswatch_tracker_status() -> Dict[str, Any]:
    """Inspect CrossWatch tracker folder (.cw_provider)."""
    _, CONFIG_DIR, *_ = _cw()
    root = _cw_tracker_root(CONFIG_DIR)
    info = _scan_cw_tracker(root)
    return {
        "ok": True,
        "root": str(root),
        **info,
    }

@router.post("/clear-state")
def clear_state_minimal() -> Dict[str, Any]:
    _, CONFIG_DIR, *_ = _cw()
    state_path = CONFIG_DIR / "state.json"
    existed = state_path.exists()
    try:
        state_path.unlink(missing_ok=True)
        return {
            "ok": True,
            "path": str(state_path),
            "existed": bool(existed),
        }
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "path": str(state_path),
            "existed": bool(existed),
        }

@router.post("/crosswatch-tracker/clear")
def crosswatch_tracker_clear(
    clear_state: bool = Body(True),
    clear_snapshots: bool = Body(False),
) -> Dict[str, Any]:
    _, CONFIG_DIR, *_ = _cw()
    root = _cw_tracker_root(CONFIG_DIR)

    before = _scan_cw_tracker(root)
    removed_state: List[str] = []
    removed_snapshots: List[str] = []

    if clear_state and root.exists():
        for p in root.glob("*.json"):
            if p.is_file() and _safe_remove_path(p):
                removed_state.append(p.name)

    if clear_snapshots:
        snaps_dir = root / "snapshots"
        if snaps_dir.exists():
            for p in snaps_dir.glob("*.json"):
                if p.is_file() and _safe_remove_path(p):
                    removed_snapshots.append(p.name)

    after = _scan_cw_tracker(root)

    return {
        "ok": True,
        "root": str(root),
        "removed": {
            "state_files": removed_state,
            "snapshots": removed_snapshots,
        },
        "before": before,
        "after": after,
    }

@router.post("/clear-cache")
def clear_cache() -> Dict[str, Any]:
    _, _, CW_STATE_DIR, *_ = _cw()

    before = _scan_provider_cache()
    removed = _clear_cw_state_files()
    after = _scan_provider_cache()

    return {
        "ok": True,
        "root": str(CW_STATE_DIR),
        "removed": removed,
        "before": before,
        "after": after,
    }
    
@router.get("/provider-cache")
def provider_cache_status() -> Dict[str, Any]:
    info = _scan_provider_cache()
    return {"ok": True, **info}

# Reset statistics, state, reports, insights
@router.post("/maintenance/reset-stats")
@router.post("/reset-stats")
def reset_stats(
    recalc: bool = Body(False),
    purge_file: bool = Body(False),
    purge_state: bool = Body(False),
    purge_reports: bool = Body(False),
    purge_insights: bool = Body(False),
) -> Dict[str, Any]:
    CACHE_DIR, CONFIG_DIR, CW_STATE_DIR, STATS, _load_state, _append_log = _cw()

    if not any((recalc, purge_file, purge_state, purge_reports, purge_insights)):
        purge_file = purge_state = purge_reports = purge_insights = True
        recalc = False
    try:
        try:
            from _syncAPI import _summary_reset, _PROVIDER_COUNTS_CACHE, _find_state_path
        except Exception:
            _summary_reset = None
            _PROVIDER_COUNTS_CACHE = None
            _find_state_path = None
        try:
            from crosswatch import LOG_BUFFERS
        except Exception:
            LOG_BUFFERS = {}

        if _summary_reset:
            _summary_reset()
        if isinstance(LOG_BUFFERS, dict):
            LOG_BUFFERS["SYNC"] = []
        if isinstance(_PROVIDER_COUNTS_CACHE, dict):
            _PROVIDER_COUNTS_CACHE["ts"] = 0.0
            _PROVIDER_COUNTS_CACHE["data"] = None

        # --- statistics object ---
        STATS.reset()
        if purge_file:
            try:
                STATS.path.unlink(missing_ok=True)
            except Exception:
                pass
            STATS._load()
            STATS._save()

        # --- state.json ---
        if purge_state and _find_state_path:
            try:
                sp = _find_state_path()
                if sp and sp.exists():
                    sp.unlink()
            except Exception:
                pass

        # --- sync-*.json reports ---
        if purge_reports:
            try:
                try:
                    from _statistics import REPORT_DIR
                except Exception:
                    from pathlib import Path as _P

                    REPORT_DIR = _P("/config/sync_reports")
                for f in REPORT_DIR.glob("sync-*.json"):
                    try:
                        f.unlink()
                    except Exception:
                        pass
            except Exception:
                pass

        # --- insights caches & series (files + in-memory) ---
        insights_files_dropped = 0
        if purge_insights:
            from pathlib import Path as _P

            roots = [
                p
                for p in (CW_STATE_DIR, CACHE_DIR, CONFIG_DIR)
                if isinstance(p, _P) and p.exists()
            ]
            patterns = ("insights*.json", ".insights*.json", "insight*.json", "series*.json")
            for root in roots:
                for pat in patterns:
                    for f in root.glob(pat):
                        try:
                            f.unlink()
                            insights_files_dropped += 1
                        except Exception:
                            pass

            try:
                import _insightAPI as IA  # noqa

                for name, obj in list(vars(IA).items()):
                    key = name.lower()
                    if any(s in key for s in ("insight", "series")) and any(
                        s in key for s in ("cache", "memo", "state")
                    ):
                        try:
                            obj.clear() if hasattr(obj, "clear") else None
                            if isinstance(obj, (list, tuple)):
                                obj[:] = []
                        except Exception:
                            pass
                for fn_name in ("reset_insights_cache", "clear_cache"):
                    fn = getattr(IA, fn_name, None)
                    if callable(fn):
                        try:
                            fn()
                        except Exception:
                            pass
            except Exception:
                pass

        # ---  rebuild from current state ---
        if recalc:
            try:
                state = _load_state()
                if state:
                    STATS.refresh_from_state(state)
            except Exception:
                pass

        return {
            "ok": True,
            "dropped": {
                "stats_file": bool(purge_file),
                "state_file": bool(purge_state),
                "reports": bool(purge_reports),
                "insights_files": insights_files_dropped,
                "insights_mem": bool(purge_insights),
            },
            "recalculated": bool(recalc),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

@router.post("/reset-currently-watching")
def reset_currently_watching() -> Dict[str, Any]:
    _, _, CW_STATE_DIR, _, _, _append_log = _cw()
    path = CW_STATE_DIR / "currently_watching.json"
    existed = path.exists()
    try:
        if existed:
            try:
                path.unlink(missing_ok=True)
            except TypeError:
                if path.exists():
                    path.unlink()
        try:
            _append_log(
                "TRBL",
                "\x1b[91m[TROUBLESHOOT]\x1b[0m Reset currently_watching.json (currently playing).",
            )
        except Exception:
            pass
        return {"ok": True, "path": str(path), "existed": bool(existed)}
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "path": str(path),
            "existed": bool(existed),
        }

@router.post("/restart")
def restart_crosswatch() -> Dict[str, Any]:
    _, _, _, _, _, _append_log = _cw()
    try:
        _append_log(
            "TRBL",
            "\x1b[91m[TROUBLESHOOT]\x1b[0m Restart requested via /api/maintenance/restart.",
        )
    except Exception:
        pass

    def _kill():
        try:
            _append_log(
                "TRBL",
                "\x1b[91m[TROUBLESHOOT]\x1b[0m Terminating process for restart.",
            )
        except Exception:
            pass
        os._exit(0)

    threading.Timer(0.75, _kill).start()
    return {"ok": True, "message": "Restart scheduled"}

@router.post("/reset-state")
def reset_state(
    mode: str = Body("clear_both"),
    # clear_both|clear_state|clear_tombstones|clear_tombstone_entries|clear_cw_state_only|rebuild
    keep_ttl: bool = Body(True),
    ttl_override: Optional[int] = Body(None),
    feature: str = Body("watchlist"),
) -> Dict[str, Any]:
    _, CONFIG_DIR, CW_STATE_DIR, STATS, _load_state, _ = _cw()
    try:
        state_path = CONFIG_DIR / "state.json"
        tomb_path = CONFIG_DIR / "tombstones.json"
        last_path = CONFIG_DIR / "last_sync.json"
        hide_path = CONFIG_DIR / "watchlist_hide.json"
        ratings_changes_path = CONFIG_DIR / "ratings_changes.json"

        cleared: List[str] = []
        cw_state: Dict[str, Any] = {}

        def _try_unlink(p: Path, label: str):
            try:
                p.unlink(missing_ok=True)
                cleared.append(label)
            except Exception:
                pass

        def _ls_cw() -> List[str]:
            if not CW_STATE_DIR.exists():
                return []
            return sorted([x.name for x in CW_STATE_DIR.iterdir() if x.is_file()])

        if mode in ("clear_state", "clear_both", "clear_cw_state_only"):
            pre = _ls_cw()
            removed = _clear_cw_state_files()
            post = _ls_cw()
            cw_state = {
                "path": str(CW_STATE_DIR),
                "pre": pre,
                "removed": removed,
                "post": post,
            }
            if mode != "clear_cw_state_only":
                _try_unlink(state_path, "state.json")
                _try_unlink(last_path, "last_sync.json")
                _try_unlink(ratings_changes_path, "ratings_changes.json")
                _try_unlink(hide_path, "watchlist_hide.json")

        if mode in ("clear_tombstones", "clear_both"):
            _try_unlink(tomb_path, "tombstones.json")

        if mode == "clear_tombstone_entries":
            try:
                t = json.loads(tomb_path.read_text("utf-8")) if tomb_path.exists() else {}
            except Exception:
                t = {}
            t["keys"] = {}
            if isinstance(ttl_override, int) and ttl_override > 0:
                t["ttl_sec"] = ttl_override
            elif not keep_ttl:
                t["ttl_sec"] = 2 * 24 * 3600
            tmp = tomb_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(t, ensure_ascii=False, indent=2), "utf-8")
            os.replace(tmp, tomb_path)

        if mode == "rebuild":
            try:
                from cw_platform.config_base import load_config
                from _syncAPI import _persist_state_via_orc
                from cw_platform.orchestrator import Orchestrator

                state = _persist_state_via_orc(Orchestrator(config=load_config()), feature=feature)
                STATS.refresh_from_state(state)
            except Exception as e:
                return {"ok": False, "error": f"rebuild failed: {e}"}

        if mode not in {
            "clear_both",
            "clear_state",
            "clear_tombstones",
            "clear_tombstone_entries",
            "clear_cw_state_only",
            "rebuild",
        }:
            return {"ok": False, "error": f"Unknown mode: {mode}"}

        return {"ok": True, "mode": mode, "cleared": cleared, "cw_state": cw_state}
    except Exception as e:
        return {"ok": False, "error": str(e)}