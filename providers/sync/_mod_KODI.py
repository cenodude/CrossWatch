# providers/sync/_mod_KODI.py
# CrossWatch Kodi sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from providers.sync._mod_common import build_op_result
from providers.sync.kodi import _history as feat_history
from providers.sync.kodi import _progress as feat_progress
from providers.sync.kodi import _ratings as feat_ratings
from providers.sync.kodi._common import KodiClient, health_payload, is_configured, item_key, make_config, pick_instance_id

__VERSION__ = "0.1"
__all__ = ["get_manifest", "KODIModule", "OPS"]

_FEATURES = {"watchlist": False, "ratings": True, "history": True, "progress": True, "playlists": False}
_FEATURE_MODULES = {"history": feat_history, "ratings": feat_ratings, "progress": feat_progress}


def get_manifest() -> Mapping[str, Any]:
    caps = {
        "bidirectional": True,
        "experimental": True,
        "provides_ids": True,
        "index_semantics": "present",
        "compatibility": {
            "minimum_kodi": "21.0",
            "minimum_jsonrpc": "13.5.0",
            "tested_jsonrpc": "13.5.x",
        },
        "history": {
            "read": True,
            "write": True,
            "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
            "upsert": True,
            "remove": True,
            "observed_deletes": True,
            "aggregated": True,
        },
        "ratings": {
            "read": True,
            "write": True,
            "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
            "upsert": True,
            "remove": True,
            "scale": "1-10",
            "from_date": False,
        },
        "progress": {
            "read": True,
            "write": True,
            "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
            "upsert": True,
            "remove": True,
            "requires_duration": False,
        },
    }
    return {
        "name": "KODI",
        "label": "Kodi",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": True,
        "experimental": True,
        "features": dict(_FEATURES),
        "requires": ["requests"],
        "capabilities": caps,
    }


class KODIModule:
    def __init__(self, cfg: Mapping[str, Any]):
        self.config = cfg or {}
        self.instance_id = pick_instance_id()
        self.cfg = make_config(self.config, self.instance_id)
        if not self.cfg.server:
            raise RuntimeError("Kodi config requires server")
        self.client = KodiClient(self.cfg)
        self._kodi_library_index = None

    @staticmethod
    def supported_features() -> dict[str, bool]:
        return dict(_FEATURES)

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def health(self) -> Mapping[str, Any]:
        payload = health_payload(self)
        features = {name: bool(payload.get("ok") and enabled) for name, enabled in _FEATURES.items()}
        payload["features"] = features
        return payload

    @staticmethod
    def key_of(obj: Mapping[str, Any]) -> str:
        return item_key(obj)

    def build_index(self, feature: str, **kwargs: Any) -> Mapping[str, dict[str, Any]]:
        mod = _FEATURE_MODULES.get(str(feature or "").strip().lower())
        return mod.build_index(self, **kwargs) if mod else {}

    def add(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        mod = _FEATURE_MODULES.get(str(feature or "").strip().lower())
        if not mod:
            return build_op_result(ok=True, count=0, unsupported=True)
        return mod.add(self, items, dry_run=dry_run)

    def remove(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        mod = _FEATURE_MODULES.get(str(feature or "").strip().lower())
        if not mod:
            return build_op_result(ok=True, count=0, unsupported=True)
        return mod.remove(self, items, dry_run=dry_run)


class _KODIOPS:
    def name(self) -> str:
        return "KODI"

    def label(self) -> str:
        return "Kodi"

    def features(self) -> Mapping[str, bool]:
        return dict(_FEATURES)

    def state_read_features(self) -> Mapping[str, bool]:
        return dict(_FEATURES)

    def capabilities(self) -> Mapping[str, Any]:
        return get_manifest()["capabilities"]

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        return is_configured(cfg, "default")

    def _adapter(self, cfg: Mapping[str, Any]) -> KODIModule:
        return KODIModule(cfg)

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)


OPS = _KODIOPS()
