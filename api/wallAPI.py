# /api/wallAPI.py
# CrossWatch - Wall API for watchlist management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import threading
from typing import Any
from fastapi import FastAPI, Query

from cw_platform.config_base import config_path, load_config
from services.watchlist import build_watchlist, detect_available_watchlist_providers
from .syncAPI import _load_state, _peek_state_key


_WALL_CACHE_LOCK = threading.Lock()
_WALL_CACHE: dict[str, Any] = {"key": None, "data": None}


def _path_key(path: Any) -> tuple[str, int, int]:
    try:
        p = path if hasattr(path, "stat") else config_path()
        st = p.stat()
        mt = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)))
        return (str(p), mt, int(st.st_size))
    except Exception:
        return (str(path or ""), 0, 0)


def _cache_key(*, both_only: bool, active_only: bool, limit: int) -> tuple[Any, ...]:
    return (
        _peek_state_key(),
        _path_key(config_path()),
        bool(both_only),
        bool(active_only),
        int(limit or 0),
    )


def _tmdb_api_key(cfg: dict[str, Any]) -> str:
    def _pick_from_block(blk: Any) -> str:
        if not isinstance(blk, dict):
            return ""
        k = str(blk.get("api_key") or "").strip()
        if k:
            return k
        insts = blk.get("instances")
        if isinstance(insts, dict):
            for v in insts.values():
                kk = str((v or {}).get("api_key") or "").strip() if isinstance(v, dict) else ""
                if kk:
                    return kk
        return ""

    for key in ("tmdb", "tmdb_sync"):
        found = _pick_from_block(cfg.get(key))
        if found:
            return found
    return ""

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
        limit: int = Query(0, ge=0, le=100, description="Optional item limit"),
    ) -> dict[str, Any]:
        key = _cache_key(both_only=both_only, active_only=active_only, limit=limit)
        with _WALL_CACHE_LOCK:
            if _WALL_CACHE.get("key") == key and isinstance(_WALL_CACHE.get("data"), dict):
                return dict(_WALL_CACHE["data"])

        cfg = load_config() or {}
        st = _load_state() or {}
        api_key = _tmdb_api_key(cfg)

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
        if limit:
            items = items[:limit]

        data = {
            "ok": True,
            "items": items,
            "missing_tmdb_key": not bool(api_key),
            "last_sync_epoch": st.get("last_sync_epoch") if isinstance(st, dict) else None,
        }
        with _WALL_CACHE_LOCK:
            _WALL_CACHE["key"] = key
            _WALL_CACHE["data"] = data
        return data
