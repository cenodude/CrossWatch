# _wallAPI.py
# CrossWatch - Wall API for watchlist management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any
from fastapi import FastAPI, Query

from cw_platform.config_base import load_config
from services.watchlist import build_watchlist, detect_available_watchlist_providers
from .syncAPI import _load_state

def _load_wall_snapshot() -> list[dict[str, Any]]:
    try:
        st = _load_state() or {}
        wall = st.get("wall") or []
        return wall if isinstance(wall, list) else []
    except Exception:
        return []


def refresh_wall() -> list[dict[str, Any]]:
    try:
        return build_watchlist(_load_state() or {}, tmdb_ok=True)
    except Exception:
        return []


def _configured_provider_ids(cfg: dict[str, Any]) -> list[str]:
    try:
        manifest = detect_available_watchlist_providers(cfg) or []
    except Exception:
        manifest = []

    return [
        str(it.get("id") or "").upper()
        for it in manifest
        if isinstance(it, dict)
        and it.get("configured")
        and str(it.get("id") or "").upper() != "ALL"
    ]


def register_wall(app: FastAPI) -> None:
    @app.get("/api/state/wall", tags=["wall"])
    def api_state_wall(
        both_only: bool = Query(False, description="Keep only items present on multiple providers"),
        active_only: bool = Query(False, description="Keep only items from configured providers"),
    ) -> dict[str, Any]:
        cfg = load_config() or {}
        st = _load_state() or {}
        api_key = str(((cfg.get("tmdb") or {}).get("api_key") or "")).strip()

        items = build_watchlist(st, tmdb_ok=bool(api_key)) or []
        active = {pid.lower(): True for pid in _configured_provider_ids(cfg)}

        def keep(it: dict[str, Any]) -> bool:
            status = str(it.get("status") or "").lower()
            if both_only and status != "both":
                return False
            if active_only and status.endswith("_only"):
                base = status[:-5]
                return active.get(base, False)
            return True

        items = [it for it in items if keep(it)]

        return {
            "ok": True,
            "items": items,
            "missing_tmdb_key": not bool(api_key),
            "last_sync_epoch": st.get("last_sync_epoch") if isinstance(st, dict) else None,
        }