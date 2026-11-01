# providers/sync/_mod_SCROB.py
# CrossWatch - Scrob sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from collections.abc import Iterable, Mapping
from typing import Any

import requests

from ._log import log as cw_log
from ._mod_common import build_session
from cw_platform.app_version import user_agent as http_user_agent
from cw_platform.provider_instances import normalize_instance_id, resolve_provider_block
from providers.auth._auth_SCROB import is_configured as auth_configured
from providers.sync.scrob import _playlists as feat_playlists

try:  # type: ignore[name-defined]
    ctx  # type: ignore[misc]
except Exception:
    ctx = None  # type: ignore[assignment]

__VERSION__ = "1.1"
os.environ.setdefault("CW_SCROB_UA", http_user_agent("Scrob", override_env="CW_SCROB_UA"))
__all__ = ["get_manifest", "SCROBModule", "OPS"]

FEATURES = {"watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": True, "collection": True}

CAPABILITIES: dict[str, Any] = {
    "bidirectional": True,
    "provides_ids": True,
    "index_semantics": "present",
    "can_source": True,
    "can_target": True,
    "watchlist": {
        "types": ["movie", "show"],
        "upsert": True,
        "remove": True,
        "observed_deletes": True,
        "required_ids": ["tmdb"],
    },
    "ratings": {
        "types": ["movie", "show", "season", "episode"],
        "scale": {"min": 1, "max": 10, "step": 1},
        "upsert": True,
        "remove": True,
        "observed_deletes": True,
        "required_ids": ["tmdb"],
        "notes": "Episode ratings need the episode to exist in Scrob already.",
    },
    "history": {
        "types": ["movie", "episode"],
        "upsert": True,
        "remove": True,
        "observed_deletes": True,
        "event_history": True,
        "rewatches": {"read": True, "write": True, "account_gate": False},
        "required_ids": ["tmdb"],
        "notes": "Scrob stores one row per play, so individual watched_at timestamps and repeat plays survive both directions.",
    },
    "progress": {
        "types": ["movie", "episode"],
        "upsert": True,
        "remove": True,
        "observed_deletes": True,
        "required_ids": ["tmdb"],
        "required_duration": True,
        "write_window_percent": {"min": 5, "max": 90},
        "notes": "Scrob only keeps continue-watching state between 5% and 90%.",
    },
    "collection": {
        "types": ["movie", "show", "season", "episode"],
        "upsert": True,
        "remove": True,
        "observed_deletes": True,
        "required_ids": ["tmdb"],
        "notes": "Scrob collection source uses the personal export; show and season writes expand server-side.",
    },
    "playlists": {
        "read": True,
        "create": True,
        "rename": True,
        "delete": True,
        "add": True,
        "remove": True,
        "reorder": False,
        "smart": False,
        "discovery": False,
        "resolve": True,
        "link_example": "http://your-scrob-server:7330/list/12",
        "media_types": ["movies", "shows", "seasons"],
        "accepted_ids": ["tmdb", "tvdb"],
    },
}


def _health(status: str, ok: bool, latency_ms: int) -> None:
    cw_log("SCROB", "health", "info", "health", latency_ms=latency_ms, ok=ok, status=status)


def _log(level: str, msg: str, **fields: Any) -> None:
    cw_log("SCROB", "module", level, msg, **fields)


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "SCROB",
        "label": "Scrob",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": True,
        "experimental": True,
        "features": dict(FEATURES),
        "requires": [],
        "capabilities": dict(CAPABILITIES),
        "description": "Self hosted Scrob tracker (watchlist, ratings, history, progress, collection).",
    }


def _feature_label(method: str, url: str, kw: Mapping[str, Any]) -> str:
    tail = str(url or "").rstrip("/").rsplit("/", 2)[-2:]
    return "api:" + "/".join(part for part in tail if part) if tail else "api"


class SCROBModule:
    def __init__(self, cfg: Mapping[str, Any], *, instance_id: Any = None):
        self.instance_id = normalize_instance_id(instance_id if instance_id is not None else (cfg or {}).get("_cw_provider_instance"))
        block = resolve_provider_block(cfg or {}, "scrob", self.instance_id) or {}
        if not auth_configured(block):
            _log("error", "missing config", instance=self.instance_id)
            raise RuntimeError("Scrob is not connected: server_url, api_key, username and password are required")

        self.config: dict[str, Any] = {**dict(cfg or {}), "scrob": dict(block)}
        self.raw_cfg = self.config
        self.session: requests.Session = build_session("SCROB", ctx, feature_label=_feature_label)
        try:
            self.session.headers["User-Agent"] = os.environ.get("CW_SCROB_UA") or http_user_agent("Scrob", override_env="CW_SCROB_UA")
            self.session.headers.setdefault("Accept", "application/json")
        except Exception:
            pass

    @staticmethod
    def supported_features() -> dict[str, bool]:
        return dict(FEATURES)

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def health(self) -> Mapping[str, Any]:
        from .scrob._common import PATH_NOW_PLAYING, ok_status, scrob_request

        start = time.perf_counter()
        try:
            resp = scrob_request(self, "GET", PATH_NOW_PLAYING)
            latency_ms = int((time.perf_counter() - start) * 1000)
            if ok_status(resp):
                _health("ok", True, latency_ms)
                return {"ok": True, "status": "ok", "latency_ms": latency_ms}
            reason = f"http:{int(resp.status_code)}"
            _health("down", False, latency_ms)
            return {"ok": False, "status": "down", "latency_ms": latency_ms, "reason": reason}
        except Exception as exc:
            latency_ms = int((time.perf_counter() - start) * 1000)
            _health("down", False, latency_ms)
            return {"ok": False, "status": "down", "latency_ms": latency_ms, "reason": exc.__class__.__name__}

    def _module(self, feature: str) -> Any:
        key = str(feature or "").strip().lower()
        if key == "watchlist":
            from .scrob import _watchlist

            return _watchlist
        if key == "ratings":
            from .scrob import _ratings

            return _ratings
        if key == "history":
            from .scrob import _history

            return _history
        if key == "progress":
            from .scrob import _progress

            return _progress
        if key == "collection":
            from .scrob import _collection

            return _collection
        return None

    def build_index(self, feature: str, **kwargs: Any) -> dict[str, dict[str, Any]]:
        mod = self._module(feature)
        if mod is None:
            _log("info", "index_skipped", requested=str(feature), reason="disabled_or_missing")
            return {}
        return mod.build_index(self)

    def add(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        mod = self._module(feature)
        if mod is None:
            return {"ok": True, "count": 0, "confirmed_keys": [], "unresolved_keys": [], "unresolved": [], "reason": "disabled_or_missing"}
        return mod.add(self, items, dry_run=dry_run)

    def remove(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        mod = self._module(feature)
        if mod is None:
            return {"ok": True, "count": 0, "confirmed_keys": [], "unresolved_keys": [], "unresolved": [], "reason": "disabled_or_missing"}
        return mod.remove(self, items, dry_run=dry_run)


class _SCROBOPS:
    def name(self) -> str:
        return "SCROB"

    def label(self) -> str:
        return "Scrob"

    def features(self) -> Mapping[str, bool]:
        return SCROBModule.supported_features()

    def state_read_features(self) -> Mapping[str, bool]:
        return SCROBModule.supported_features()

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def capabilities(self) -> Mapping[str, Any]:
        return dict(CAPABILITIES)

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        block = resolve_provider_block(cfg or {}, "scrob", None) or {}
        if auth_configured(block):
            return True
        root = (cfg or {}).get("scrob")
        instances = root.get("instances") if isinstance(root, Mapping) else None
        if isinstance(instances, Mapping):
            return any(auth_configured(inst) for inst in instances.values() if isinstance(inst, Mapping))
        return False

    def _adapter(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> SCROBModule:
        return SCROBModule(cfg, instance_id=instance_id)

    def health(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> Mapping[str, Any]:
        return self._adapter(cfg, instance_id=instance_id).health()

    def build_index(self, cfg: Mapping[str, Any], *, feature: str, instance_id: Any = None, **kwargs: Any) -> Mapping[str, dict[str, Any]]:
        return self._adapter(cfg, instance_id=instance_id).build_index(feature, **kwargs)

    def add(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
        instance_id: Any = None,
    ) -> dict[str, Any]:
        return self._adapter(cfg, instance_id=instance_id).add(feature, items, dry_run=dry_run)

    def remove(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
        instance_id: Any = None,
    ) -> dict[str, Any]:
        return self._adapter(cfg, instance_id=instance_id).remove(feature, items, dry_run=dry_run)

    def list_playlist_resources(self, cfg: Mapping[str, Any], *, instance: str | None = None) -> list[Any]:
        return list(feat_playlists.list_resources(self._adapter(cfg, instance_id=instance)))

    def get_playlist_snapshot(self, cfg: Mapping[str, Any], playlist_id: str, *, instance: str | None = None) -> Any:
        return feat_playlists.get_snapshot(self._adapter(cfg, instance_id=instance), playlist_id)

    def resolve_playlist(self, cfg: Mapping[str, Any], ref: str, *, instance: str | None = None) -> Any:
        return feat_playlists.resolve(self._adapter(cfg, instance_id=instance), ref)

    def create_playlist(self, cfg: Mapping[str, Any], name: str, *, media_type: str | None = None,
                        items: Iterable[Mapping[str, Any]] | None = None, instance: str | None = None, dry_run: bool = False) -> Any:
        return feat_playlists.create(self._adapter(cfg, instance_id=instance), name, media_type=media_type, items=list(items or []), dry_run=dry_run)

    def rename_playlist(self, cfg: Mapping[str, Any], playlist_id: str, name: str, *, instance: str | None = None, dry_run: bool = False) -> Any:
        return feat_playlists.rename(self._adapter(cfg, instance_id=instance), playlist_id, name, dry_run=dry_run)

    def delete_playlist(self, cfg: Mapping[str, Any], playlist_id: str, *, instance: str | None = None, dry_run: bool = False) -> dict[str, Any]:
        return feat_playlists.delete(self._adapter(cfg, instance_id=instance), playlist_id, dry_run=dry_run)

    def add_playlist_items(self, cfg: Mapping[str, Any], playlist_id: str, items: Iterable[Mapping[str, Any]], *,
                           instance: str | None = None, dry_run: bool = False) -> dict[str, Any]:
        lst = list(items or [])
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True, "unresolved": [], "confirmed_keys": []}
        return feat_playlists.add(self._adapter(cfg, instance_id=instance), playlist_id, lst)

    def remove_playlist_items(self, cfg: Mapping[str, Any], playlist_id: str, items: Iterable[Mapping[str, Any]], *,
                              instance: str | None = None, dry_run: bool = False) -> dict[str, Any]:
        lst = list(items or [])
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True, "unresolved": [], "confirmed_keys": []}
        return feat_playlists.remove(self._adapter(cfg, instance_id=instance), playlist_id, lst)

    def reorder_playlist_items(self, cfg: Mapping[str, Any], playlist_id: str, ordered_keys: Iterable[str], *,
                               instance: str | None = None, dry_run: bool = False) -> dict[str, Any]:
        return feat_playlists.reorder(self._adapter(cfg, instance_id=instance), playlist_id, list(ordered_keys or []))


OPS = _SCROBOPS()
