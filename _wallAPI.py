# _wallAPI.py
from __future__ import annotations

# --- stdlib / typing ---
from typing import Dict, Any

# --- third-party ---
from fastapi import FastAPI, Query

# --- app imports ---
from _syncAPI import _load_state
from _watchlist import build_watchlist
from cw_platform.config_base import load_config


# ----- Runtime hooks (lazy; avoids hard import cycles) -----
def _env():
    try:
        import crosswatch as CW
        return getattr(CW, "_load_state", lambda: {}), getattr(CW, "build_watchlist", lambda *_: [])
    except Exception:
        return (lambda: {}), (lambda *_: [])


# ----- Read: wall snapshot from state (no TMDB) -----
def _load_wall_snapshot() -> list[dict]:
    try:
        st = _load_state() or {}
        wall = st.get("wall") or []
        return wall if isinstance(wall, list) else []
    except Exception:
        return []


# ----- Refresh: rebuild via watchlist (TMDB allowed) -----
def refresh_wall():
    try:
        return build_watchlist(_load_state() or {}, tmdb_ok=True)
    except Exception:
        return []


# ----- API: register /api/state/wall -----
def register_wall(app: FastAPI):
    @app.get("/api/state/wall", tags=["wall"])
    def api_state_wall(both_only: bool = Query(False), active_only: bool = Query(False)) -> Dict[str, Any]:
        """Wall data with simple filters."""
        _load_state, build_watchlist = _env()

        cfg = load_config() or {}
        api_key = str(((cfg.get("tmdb") or {}).get("api_key") or "")).strip()
        st = _load_state() or {}

        items = build_watchlist(st, tmdb_ok=bool(api_key))

        # Active providers from configured pairs
        active = {"plex": False, "simkl": False, "trakt": False, "jellyfin": False}
        try:
            pairs = (cfg.get("pairs") or cfg.get("connections") or []) or []
            for p in pairs:
                s = str(p.get("source") or "").strip().lower()
                t = str(p.get("target") or "").strip().lower()
                if s in active: active[s] = True
                if t in active: active[t] = True
        except Exception:
            pass

        # Keep rules
        def keep_item(it: Dict[str, Any]) -> bool:
            status = str(it.get("status") or "").lower()
            if both_only and status != "both": return False
            if active_only and status.endswith("_only") and not active.get(status.replace("_only",""), False): return False
            return True

        items = [it for it in items if keep_item(it)]

        return {
            "ok": True,
            "items": items,
            "missing_tmdb_key": not bool(api_key),
            "last_sync_epoch": st.get("last_sync_epoch") if isinstance(st, dict) else None,
        }
