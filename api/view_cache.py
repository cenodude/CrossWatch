# api/view_cache.py
# CrossWatch - Opt-in background snapshots for Hub and Tracker reads
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import inspect
from functools import wraps
from pathlib import Path
from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse

from services.view_cache import CACHE, ViewSuperseded, digest


def _stamp(path: Path) -> tuple[int, int]:
    try:
        stat = path.stat()
        return stat.st_mtime_ns, stat.st_size
    except OSError:
        return 0, 0


def _statistics_revision(base: Path) -> Any:
    from cw_platform.local_db.db import get_conn

    conn = get_conn(base)
    if conn is None:
        raise RuntimeError("Statistics database is unavailable")
    return tuple(tuple(row) for row in conn.execute("SELECT key,updated_at FROM statistics_meta ORDER BY key"))


def _context(request: Request, arguments: dict[str, Any]) -> Any:
    from api import profileAPI
    from api.appAuthAPI import effective_request_profile_id
    from cw_platform.config_base import CONFIG, load_config
    from cw_platform.provider_instances import instances_for_user_profile
    from services import shelf_store

    ctx = profileAPI._profile_context(request)
    if isinstance(ctx, JSONResponse):
        return ctx
    cfg = ctx[0]
    if request.url.path == "/api/profile/watchlist" and ctx[2] != profileAPI.ADMIN_USER_ID and not profileAPI.clean_managed_permissions(ctx[4].get("permissions")).get("watchlist"):
        return JSONResponse({"ok": False, "error": "Watchlist access required"}, status_code=403)
    owner = profileAPI._shelf_owner(ctx)
    path = request.url.path
    profile = effective_request_profile_id(cfg, request, arguments.get("user_profile", ""))
    access = (ctx[2] == profileAPI.ADMIN_USER_ID, profileAPI.clean_managed_permissions(ctx[4].get("permissions")),
              profile, instances_for_user_profile(cfg, profile))
    scope: Any = (str(Path(CONFIG).resolve()), ctx[2], digest(access), profile)
    shelf = lambda: _stamp(shelf_store._path(owner))
    if path.startswith("/api/tracker/"):
        from api import trackerAPI

        _, instance, store = trackerAPI._store(arguments.get("provider_instance") or "default", request, fallback=path.endswith("/summary"))
        user = trackerAPI._user(request)
        allowed = sorted(row["id"] for row in trackerAPI._instances(cfg, user))
        scope = (*scope, instance, str(store.root.resolve()), digest((user.get("profile_id") if user else "", allowed)))
        source = lambda: (store.revision(), shelf())
    else:
        from api.dashboardAPI import _dashboard_widgets_version
        from cw_platform.local_db import state as sqlite_state

        if path == "/api/profile/collection":
            source = lambda: (profileAPI.collection_cache_fingerprint(), shelf(),
                              profileAPI.history_cache_version(cfg, "synced", profile) if arguments.get("shelf") == "unwatched" else None)
        elif path in {"/api/profile/history", "/api/profile/ratings", "/api/profile/watchlist"}:
            kind = arguments.get("source", "synced") if path.endswith("/history") else path.rsplit("/", 1)[1]
            source = lambda: profileAPI.history_cache_version(cfg, kind, profile)
        else:
            features = {"history", "ratings", "watchlist", "collection", "progress", "playlists"}
            source = lambda: (sqlite_state.fingerprint(CONFIG, features), _statistics_revision(CONFIG) if path == "/api/insights" else None, _dashboard_widgets_version(
                CONFIG, cfg=cfg, requested=features | {"scrobble"}, state_features=features, profile=profile, limits={}))

    config_revision = digest(cfg)

    def revision():
        if digest(load_config() or {}) != config_revision:
            raise ViewSuperseded("view_scope_changed")
        value = source()
        if value is None or value == "":
            raise RuntimeError("view_source_unavailable")
        return config_revision, value

    return Path(CONFIG), scope, revision


def background_view(function):
    signature = inspect.signature(function, eval_str=True)

    @wraps(function)
    def wrapped(*args, **kwargs):
        arguments = signature.bind(*args, **kwargs).arguments
        request = arguments.get("request")
        if not isinstance(request, Request) or request.query_params.get("background") != "1" or arguments.get("include_keys") is True:
            return function(*args, **kwargs)
        context = _context(request, arguments)
        if isinstance(context, JSONResponse):
            return context
        base, scope, revision = context
        query = (request.url.path, {key: value for key, value in arguments.items() if key not in {"request", "known_version"}})
        build_kwargs = dict(kwargs)
        if "known_version" in build_kwargs:
            build_kwargs["known_version"] = ""
        return CACHE.get(base, scope, query, revision, lambda: function(*args, **build_kwargs), retry=request.query_params.get("retry_view") == "1")

    setattr(wrapped, "__signature__", signature)
    return wrapped
