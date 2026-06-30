# cw_platform/modules_registry.py
# CrossWatch - Modules Registry
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
from typing import Any

# Global module registry
MODULES: dict[str, dict[str, str]] = {
    "AUTH": {
        "_auth_PLEX":     "providers.auth._auth_PLEX",
        "_auth_SIMKL":    "providers.auth._auth_SIMKL",
        "_auth_TRAKT":    "providers.auth._auth_TRAKT",
        "_auth_JELLYFIN": "providers.auth._auth_JELLYFIN",
        "_auth_EMBY":     "providers.auth._auth_EMBY",
        "_auth_MDBLIST":  "providers.auth._auth_MDBLIST",
        "_auth_PUBLICMETADB": "providers.auth._auth_PUBLICMETADB",
        "_auth_TAUTULLI": "providers.auth._auth_TAUTULLI",
        "_auth_ANILIST":  "providers.auth._auth_ANILIST",
        "_auth_TMDB":     "providers.auth._auth_TMDB",
    },
    "SYNC": {
        "_mod_PLEX":       "providers.sync._mod_PLEX",
        "_mod_SIMKL":      "providers.sync._mod_SIMKL",
        "_mod_TRAKT":      "providers.sync._mod_TRAKT",
        "_mod_JELLYFIN":   "providers.sync._mod_JELLYFIN",
        "_mod_EMBY":       "providers.sync._mod_EMBY",
        "_mod_MDBLIST":    "providers.sync._mod_MDBLIST",
        "_mod_PUBLICMETADB": "providers.sync._mod_PUBLICMETADB",
        "_mod_CROSSWATCH": "providers.sync._mod_CROSSWATCH",
        "_mod_TAUTULLI":   "providers.sync._mod_TAUTULLI",
        "_mod_ANILIST":    "providers.sync._mod_ANILIST",
        "_mod_TMDB":       "providers.sync._mod_TMDB",
    },
}


def get_sync_module_path_by_name(name: str) -> str | None:
    key = f"_mod_{(name or '').strip().upper()}"
    return MODULES["SYNC"].get(key)


def load_sync_ops(name: str) -> Any | None:
    path = get_sync_module_path_by_name(name)
    if not path:
        return None
    mod = import_module(path)
    return getattr(mod, "OPS", None)


def state_read_features(ops: Any) -> dict[str, bool]:
    """Return features that can provide a complete provider-state inventory.
    Providers may still support writes for a feature that cannot be enumerated
    completely. Captures and provider-state imports must only use the latter.
    """
    fn = getattr(ops, "state_read_features", None)
    if not callable(fn):
        fn = getattr(ops, "features", None)
    if not callable(fn):
        return {}
    try:
        raw = fn() or {}
    except Exception:
        return {}
    if not isinstance(raw, Mapping):
        return {}
    return {str(key): bool(value) for key, value in raw.items()}
