# /api/wallAPI.py
# CrossWatch - Wall API for watchlist management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import threading
from collections import OrderedDict
from typing import Any, cast
from fastapi import FastAPI, Query, Request

from cw_platform.config_base import CONFIG, config_path, load_config
from cw_platform.local_db import manual_policy as sqlite_manual_policy
from cw_platform.local_db import state as sqlite_state
from cw_platform.local_db import watchlist_hide as sqlite_watchlist_hide
from cw_platform.orchestrator._state_store import StateStore
from cw_platform.provider_instances import instances_for_user_profile, normalize_instance_id, provider_display_key
from services.watchlist import build_watchlist, detect_available_watchlist_providers


_WALL_CACHE_LOCK = threading.Lock()
_WALL_CACHE: "OrderedDict[tuple[Any, ...], dict[str, Any]]" = OrderedDict()
_WALL_CACHE_MAX = 8


def _wall_cache_get(key: tuple[Any, ...]) -> dict[str, Any] | None:
    with _WALL_CACHE_LOCK:
        data = _WALL_CACHE.get(key)
        if data is None:
            return None
        _WALL_CACHE.move_to_end(key)
        return dict(data)


def _wall_cache_put(key: tuple[Any, ...], data: dict[str, Any]) -> None:
    with _WALL_CACHE_LOCK:
        _WALL_CACHE[key] = dict(data)
        _WALL_CACHE.move_to_end(key)
        while len(_WALL_CACHE) > _WALL_CACHE_MAX:
            _WALL_CACHE.popitem(last=False)


def clear_wall_cache() -> None:
    with _WALL_CACHE_LOCK:
        _WALL_CACHE.clear()


def _path_key(path: Any) -> tuple[str, int, int]:
    try:
        p = path if hasattr(path, "stat") else config_path()
        st = p.stat()
        mt = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)))
        return (str(p), mt, int(st.st_size))
    except Exception:
        return (str(path or ""), 0, 0)


def _cache_key(*, both_only: bool, active_only: bool, limit: int, user_profile: str = "") -> tuple[Any, ...]:
    return (
        sqlite_state.fingerprint(CONFIG, {"watchlist"}),
        sqlite_manual_policy.fingerprint(CONFIG, {"watchlist"}),
        sqlite_watchlist_hide.fingerprint(CONFIG),
        _path_key(config_path()),
        bool(both_only),
        bool(active_only),
        int(limit or 0),
        str(user_profile or "").strip(),
    )


def _cache_version(key: tuple[Any, ...]) -> str:
    blob = json.dumps(key, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


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


def _load_state() -> dict[str, Any]:
    try:
        st = StateStore(CONFIG).load_state_features({"watchlist"}) or {}
        return st if isinstance(st, dict) else {}
    except Exception:
        return {}


def _load_wall_snapshot() -> list[dict[str, Any]]:
    try:
        st = _load_state()
        wall = st.get("wall") or []
        return wall if isinstance(wall, list) else []
    except Exception:
        return []


def refresh_wall() -> list[dict[str, Any]]:
    try:
        return build_watchlist(_load_state(), tmdb_ok=True)
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


def _item_matches_user_filter(item: dict[str, Any], user_filter: dict[str, list[str]]) -> bool:
    if not user_filter:
        return True
    wanted = {
        provider_display_key(provider): {normalize_instance_id(inst) for inst in (instances or [])}
        for provider, instances in user_filter.items()
    }
    sources = item.get("sources_by_provider") or item.get("sourcesByProvider")
    if isinstance(sources, dict):
        for provider, raw_instances in sources.items():
            prov = provider_display_key(provider)
            values = raw_instances if isinstance(raw_instances, list) else [raw_instances]
            for inst in values:
                if normalize_instance_id(inst) in wanted.get(prov, set()):
                    return True
    provider = item.get("added_src") or item.get("source") or item.get("provider")
    instance = item.get("added_instance") or item.get("source_instance") or item.get("instance")
    prov = provider_display_key(provider)
    return bool(prov) and normalize_instance_id(instance) in wanted.get(prov, set())


def _item_for_user_filter(item: dict[str, Any], user_filter: dict[str, list[str]]) -> dict[str, Any] | None:
    if not user_filter:
        return item
    wanted = {
        provider_display_key(provider): {normalize_instance_id(inst) for inst in (instances or [])}
        for provider, instances in user_filter.items()
    }
    sources = item.get("sources_by_provider") or item.get("sourcesByProvider")
    if not isinstance(sources, dict):
        return item if _item_matches_user_filter(item, user_filter) else None
    scoped_sources: dict[str, list[str]] = {}
    for provider, raw_instances in sources.items():
        prov = provider_display_key(provider)
        values = raw_instances if isinstance(raw_instances, list) else [raw_instances]
        keep = [
            normalize_instance_id(inst)
            for inst in values
            if normalize_instance_id(inst) in wanted.get(prov, set())
        ]
        if keep:
            scoped_sources[str(provider).lower()] = keep
    if not scoped_sources:
        return None
    out = dict(item)
    providers = sorted(scoped_sources.keys())
    out["sources_by_provider"] = scoped_sources
    out["sourcesByProvider"] = scoped_sources
    out["sources"] = providers
    out["status"] = f"{providers[0]}_only" if len(providers) == 1 else "both"
    out["sync_count"] = len(providers)
    return out


def register_wall(app: FastAPI) -> None:
    @app.get("/api/state/wall", tags=["wall"])
    def api_state_wall(
        request: Request = cast(Request, None),
        both_only: bool = Query(False, description="Keep only items present on multiple providers"),
        active_only: bool = Query(False, description="Keep only items from configured providers"),
        limit: int = Query(0, ge=0, le=100, description="Optional item limit"),
        user_profile: str = Query("", description="Optional user profile id to scope provider instances"),
        known_version: str = Query("", description="Client cache version to avoid rebuilding unchanged wall data"),
    ) -> dict[str, Any]:
        from api.appAuthAPI import COOKIE_NAME, effective_user_profile_id

        cfg = load_config() or {}
        token = request.cookies.get(COOKIE_NAME) if request is not None else None
        profile = effective_user_profile_id(cfg, token, user_profile)
        key = _cache_key(both_only=both_only, active_only=active_only, limit=limit, user_profile=profile)
        version = _cache_version(key)
        if known_version and str(known_version).strip() == version:
            return {"ok": True, "not_modified": True, "version": version}
        cached = _wall_cache_get(key)
        if cached is not None:
            return cached

        st = _load_state()
        api_key = _tmdb_api_key(cfg)
        scoped = bool(str(profile or "").strip())
        user_filter = instances_for_user_profile(cfg, profile) if scoped else {}
        if scoped and not user_filter:
            user_filter = {"__NONE__": ["__NONE__"]}

        items = build_watchlist(st, tmdb_ok=bool(api_key)) or []
        active = {pid.lower(): True for pid in _configured_provider_ids(cfg)}

        def keep(it: dict[str, Any]) -> bool:
            status = str(it.get("status") or "").lower()
            if both_only and status != "both":
                return False
            if active_only and status.endswith("_only"):
                base = status[:-5]
                if not active.get(base, False):
                    return False
            if not _item_matches_user_filter(it, user_filter):
                return False
            return True

        items = [
            scoped
            for it in items
            if keep(it)
            for scoped in [_item_for_user_filter(it, user_filter)]
            if scoped is not None
        ]
        total = len(items)
        if limit:
            items = items[:limit]

        data = {
            "ok": True,
            "items": items,
            "total": total,
            "missing_tmdb_key": not bool(api_key),
            "last_sync_epoch": st.get("last_sync_epoch") if isinstance(st, dict) else None,
            "version": version,
        }
        _wall_cache_put(key, data)
        return data
