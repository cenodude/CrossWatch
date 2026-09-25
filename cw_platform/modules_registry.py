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
        "_auth_CROSSWATCH": "providers.auth._auth_CROSSWATCH",
        "_auth_PLEX":     "providers.auth._auth_PLEX",
        "_auth_SIMKL":    "providers.auth._auth_SIMKL",
        "_auth_TRAKT":    "providers.auth._auth_TRAKT",
        "_auth_JELLYFIN": "providers.auth._auth_JELLYFIN",
        "_auth_EMBY":     "providers.auth._auth_EMBY",
        "_auth_KODI":     "providers.auth._auth_KODI",
        "_auth_MDBLIST":  "providers.auth._auth_MDBLIST",
        "_auth_PUBLICMETADB": "providers.auth._auth_PUBLICMETADB",
        "_auth_TAUTULLI": "providers.auth._auth_TAUTULLI",
        "_auth_TRACEARR": "providers.auth._auth_TRACEARR",
        "_auth_NUVIO":    "providers.auth._auth_NUVIO",
        "_auth_STREMIO":  "providers.auth._auth_STREMIO",
        "_auth_FLOPPY":   "providers.auth._auth_FLOPPY",
        "_auth_ANILIST":  "providers.auth._auth_ANILIST",
        "_auth_TMDB":     "providers.auth._auth_TMDB",
        "_auth_WETRAKR": "providers.auth._auth_WETRAKR",
        "_auth_PUNCHPLAY": "providers.auth._auth_PUNCHPLAY",
        "_auth_BINGEBASE": "providers.auth._auth_BINGEBASE",
        "_auth_FLICKLIST": "providers.auth._auth_FLICKLIST",
        "_auth_SCROB":    "providers.auth._auth_SCROB",
    },
    "SYNC": {
        "_mod_PLEX":       "providers.sync._mod_PLEX",
        "_mod_SIMKL":      "providers.sync._mod_SIMKL",
        "_mod_TRAKT":      "providers.sync._mod_TRAKT",
        "_mod_JELLYFIN":   "providers.sync._mod_JELLYFIN",
        "_mod_EMBY":       "providers.sync._mod_EMBY",
        "_mod_KODI":       "providers.sync._mod_KODI",
        "_mod_MDBLIST":    "providers.sync._mod_MDBLIST",
        "_mod_PUBLICMETADB": "providers.sync._mod_PUBLICMETADB",
        "_mod_NUVIO":      "providers.sync._mod_NUVIO",
        "_mod_STREMIO":    "providers.sync._mod_STREMIO",
        "_mod_FLOPPY":     "providers.sync._mod_FLOPPY",
        "_mod_CROSSWATCH": "providers.sync._mod_CROSSWATCH",
        "_mod_TAUTULLI":   "providers.sync._mod_TAUTULLI",
        "_mod_TRACEARR":   "providers.sync._mod_TRACEARR",
        "_mod_ANILIST":    "providers.sync._mod_ANILIST",
        "_mod_TMDB":       "providers.sync._mod_TMDB",
        "_mod_PUNCHPLAY":  "providers.sync._mod_PUNCHPLAY",
        "_mod_FLICKLIST":  "providers.sync._mod_FLICKLIST",
        "_mod_SCROB":      "providers.sync._mod_SCROB",
    },
}


PROVIDER_CONNECTION_FIELDS: dict[str, tuple[tuple[str, ...], ...]] = {
    "anilist": (("access_token", "token"),),
    "bingebase": (("access_token",), ("webhook_url",), ("api_key",)),
    "crosswatch": (("root_dir",), ("profile_id",), ("enabled",), ("connected",)),
    "emby": (("server",), ("access_token", "token", "api_key"), ("user_id",), ("verify_ssl",)),
    "flicklist": (("api_key",), ("access_token", "token"), ("auth_method",)),
    "floppy": (("server_url", "server"), ("api_token", "token"), ("verify_ssl",)),
    "jellyfin": (("server",), ("access_token", "token"), ("user_id",), ("verify_ssl",)),
    "kodi": (("server",), ("username",), ("password",), ("verify_ssl",), ("connection_verified",)),
    "mdblist": (("api_key", "key"), ("access_token",), ("auth_method",)),
    "nuvio": (("base_url",), ("server_mode",), ("publishable_key",), ("access_token",), ("profile_id",), ("auth_method",)),
    "plex": (("account_token",), ("pms_token",), ("server_url", "server"), ("user_id",), ("verify_ssl",)),
    "publicmetadb": (("base_url",), ("api_key",)),
    "wetrakr": (("access_token",),),
    "punchplay": (("access_token",),),
    "scrob": (("server_url",), ("api_prefix",), ("api_key",), ("access_token",), ("username",), ("password",), ("verify_ssl",)),
    "simkl": (("client_id", "api_key"), ("access_token", "token")),
    "stremio": (("auth_key", "authKey"), ("stremio_profile_id",)),
    "tautulli": (("server_url",), ("api_key",), ("verify_ssl",)),
    "tmdb": (("api_key",), ("session_id",)),
    "tracearr": (("server_url",), ("api_key",), ("verify_ssl",)),
    "trakt": (("client_id",), ("access_token", "token")),
}


def get_sync_module_path_by_name(name: str) -> str | None:
    key = f"_mod_{(name or '').strip().upper()}"
    return MODULES["SYNC"].get(key)


def provider_names(*, upper: bool = True) -> list[str]:
    names: set[str] = set()
    for group, prefix in (("AUTH", "_auth_"), ("SYNC", "_mod_")):
        for key in (MODULES.get(group) or {}):
            if not key.startswith(prefix):
                continue
            name = key[len(prefix):]
            if name and name.lower() != "base":
                names.add(name.upper() if upper else name.lower())
    return sorted(names)


def sync_provider_names(*, upper: bool = True) -> list[str]:
    names = [
        str(key).replace("_mod_", "")
        for key in (MODULES.get("SYNC") or {}).keys()
        if str(key).startswith("_mod_")
    ]
    out: list[str] = []
    for name in names:
        value = name.upper() if upper else name.lower()
        if value and value not in {"BASE", "base"} and value not in out:
            out.append(value)
    return out


def load_sync_ops(name: str) -> Any | None:
    path = get_sync_module_path_by_name(name)
    if not path:
        return None
    mod = import_module(path)
    return getattr(mod, "OPS", None)


def sync_provider_supports_feature(name: str, feature: str) -> bool:
    provider = str(name or "").strip().lower()
    feat = str(feature or "").strip().lower()
    if not provider or not feat:
        return False
    ops = load_sync_ops(provider)
    if not ops:
        return False
    features = ops.features() if callable(getattr(ops, "features", None)) else {}
    if isinstance(features, Mapping) and features.get(feat) is not True:
        return False
    caps = ops.capabilities() if callable(getattr(ops, "capabilities", None)) else {}
    cap = caps.get(feat) if isinstance(caps, Mapping) else None
    return bool(cap) if isinstance(cap, Mapping) else bool(isinstance(features, Mapping) and features.get(feat))


def state_read_features(ops: Any) -> dict[str, bool]:
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
