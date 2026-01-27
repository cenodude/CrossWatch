from __future__ import annotations

from typing import Callable
from fastapi import FastAPI

from .configAPI import router as config_router
from .tlsAPI import router as tls_router
from .maintenanceAPI import router as maintenance_router
from .metaAPI import router as meta_router
from .insightAPI import register_insights
from .watchlistAPI import router as watchlist_router
from .schedulingAPI import router as scheduling_router
from .probesAPI import (
    register_probes,
    PROBE_CACHE as PROBES_CACHE,
    STATUS_CACHE as PROBES_STATUS_CACHE,
)
from .scrobbleAPI import router as scrobble_router
from .authenticationAPI import register_auth
from .wallAPI import register_wall
from .versionAPI import router as version_router
from .editorAPI import router as editor_router
from .syncAPI import (
    router as sync_router,
    _is_sync_running,
    _load_state,
    _compute_lanes_from_stats,
    _lane_is_empty,
    _parse_epoch,
    api_run_sync,
)

from services.analyzer import router as analyzer_router
from services.export import router as export_router

__all__ = [
    "config_router",
    "tls_router",
    "maintenance_router",
    "meta_router",
    "watchlist_router",
    "scheduling_router",
    "scrobble_router",
    "sync_router",
    "version_router",
    "analyzer_router",
    "export_router",
    "editor_router",
    "register_probes",
    "register_insights",
    "register_wall",
    "register_auth",
    "PROBES_CACHE",
    "PROBES_STATUS_CACHE",
    "_is_sync_running",
    "_load_state",
    "_compute_lanes_from_stats",
    "_lane_is_empty",
    "_parse_epoch",
    "api_run_sync",
    "register",
]

def register(
    app: FastAPI,
    load_config: Callable[[], dict],
    *,
    log_fn: Callable[[str, str], None] | None = None,
) -> None:
    app.include_router(config_router)
    app.include_router(tls_router)
    app.include_router(meta_router)
    app.include_router(watchlist_router)
    app.include_router(maintenance_router)
    app.include_router(scheduling_router)
    app.include_router(scrobble_router)
    app.include_router(sync_router)
    app.include_router(version_router)
    app.include_router(analyzer_router)
    app.include_router(export_router)
    app.include_router(editor_router)

    register_probes(app, load_config)
    register_insights(app)
    register_wall(app)

    if log_fn is not None:
        register_auth(app, log_fn=log_fn, probe_cache=PROBES_CACHE)
