# providers/sync/_mod_SILO.py
# CrossWatch Silo sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.provider_instances import normalize_instance_id
from providers.auth._auth_SILO import SiloError, is_configured as auth_is_configured, provider_block
from providers.sync._mod_common import build_op_result, build_session, dedup_keys
from providers.sync.silo import _collection as feat_collection
from providers.sync.silo import _history as feat_history
from providers.sync.silo import _playlists as feat_playlists
from providers.sync.silo import _progress as feat_progress
from providers.sync.silo import _ratings as feat_ratings
from providers.sync.silo import _watchlist as feat_watchlist
from providers.sync.silo._common import label_silo, problem_of, shared_limiter, silo_request

__VERSION__ = "0.1"
__all__ = ["get_manifest", "SILOModule", "OPS", "feat_collection", "feat_history", "feat_playlists", "feat_progress", "feat_ratings", "feat_watchlist"]

if "ctx" not in globals():
    class _NullCtx:
        def emit(self, *args: Any, **kwargs: Any) -> None:
            pass

    ctx = _NullCtx()  # type: ignore[assignment]


_FEATURES = {"watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": True, "collection": True}
_FEATURE_MODULES = {
    "watchlist": feat_watchlist,
    "ratings": feat_ratings,
    "history": feat_history,
    "progress": feat_progress,
    "collection": feat_collection,
}
_ACCEPTED_IDS = ["tmdb", "imdb", "tvdb"]
_PLAYLIST_CAPABILITIES: dict[str, Any] = {
    "read": True,
    "create": True,
    "create_endpoint_types": ["collection"],
    "rename": True,
    "delete": True,
    "add": True,
    "remove": True,
    "reorder": True,
    "smart": True,
    "smart_writable": False,
    "media_types": ["movie", "show"],
    "endpoint_types": ["collection", "favorites"],
    "unordered_endpoint_types": ["favorites"],
}


def _current_instance_id(cfg: Mapping[str, Any] | None = None) -> str:
    explicit_instance = (cfg or {}).get("_cw_provider_instance")
    if explicit_instance is not None:
        return normalize_instance_id(explicit_instance)
    if str(os.getenv("CW_PROBE_PROVIDER") or "").upper().strip() == "SILO":
        return normalize_instance_id(os.getenv("CW_PROBE_INSTANCE"))
    if str(os.getenv("CW_PAIR_SRC") or "").upper().strip() == "SILO":
        return normalize_instance_id(os.getenv("CW_PAIR_SRC_INSTANCE"))
    if str(os.getenv("CW_PAIR_DST") or "").upper().strip() == "SILO":
        return normalize_instance_id(os.getenv("CW_PAIR_DST_INSTANCE"))
    return "default"


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "SILO",
        "label": "Silo",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": True,
        "experimental": True,
        "features": dict(_FEATURES),
        "requires": ["requests"],
        "capabilities": {
            "bidirectional": True,
            "experimental": True,
            "provides_ids": True,
            "index_semantics": "present",
            "multi_profile": True,
            "features": dict(_FEATURES),
            "watchlist": {
                "read": True,
                "write": True,
                "types": {"movies": True, "shows": True, "seasons": False, "episodes": False},
                "upsert": True,
                "remove": True,
                "observed_deletes": True,
                "accepted_ids": list(_ACCEPTED_IDS),
                "provides_ids": ["tmdb", "tvdb"],
            },
            "ratings": {
                "read": True,
                "write": True,
                "types": {"movies": True, "shows": True, "seasons": False, "episodes": False},
                "upsert": True,
                "remove": True,
                "observed_deletes": True,
                "accepted_ids": list(_ACCEPTED_IDS),
                "provides_ids": ["tmdb", "tvdb"],
                "scale": "1-10",
                "native_scale": "1-5",
            },
            "history": {
                "read": True,
                "write": True,
                "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
                "upsert": True,
                "remove": True,
                "observed_deletes": True,
                "accepted_ids": list(_ACCEPTED_IDS),
                "provides_ids": ["tmdb", "tvdb"],
                "rewatch": False,
                "event_history": False,
                "requires_watched_at": False,
                "verify_after_write": True,
            },
            "progress": {
                "read": True,
                "write": True,
                "index_semantics": "present",
                "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
                "upsert": True,
                "remove": True,
                "observed_deletes": True,
                "accepted_ids": list(_ACCEPTED_IDS),
                "provides_ids": ["tmdb", "tvdb"],
                "requires_duration": False,
                "verify_after_write": True,
            },
            "collection": {
                "read": True,
                "write": False,
                "index_semantics": "present",
                "observed_deletes": True,
                "types": {"movies": True, "shows": True, "seasons": False, "episodes": True},
                "upsert": False,
                "remove": False,
                "accepted_ids": list(_ACCEPTED_IDS),
                "provides_ids": ["tmdb", "tvdb"],
            },
            "playlists": dict(_PLAYLIST_CAPABILITIES),
        },
    }


def _result_from(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    data = dict(raw or {})
    confirmed = [str(k) for k in (data.get("confirmed_keys") or []) if k]
    unresolved_keys = [str(k) for k in (data.get("unresolved_keys") or []) if k]
    skipped = dedup_keys([str(k) for k in (data.get("skipped_keys") or []) if k])
    extra: dict[str, Any] = {}
    if skipped:
        extra["skipped_keys"] = skipped
        extra["skipped"] = len(skipped)
    accepted = dedup_keys(confirmed + skipped)
    if accepted:
        extra["accepted_keys"] = accepted
    return build_op_result(
        ok=bool(data.get("ok", True)),
        count=len(confirmed),
        confirmed_keys=confirmed,
        unresolved_keys=unresolved_keys,
        unresolved=data.get("unresolved") or [],
        **extra,
    )


class SILOModule:
    def __init__(self, cfg: Mapping[str, Any]):
        self.config = cfg or {}
        self.instance_id = _current_instance_id(cfg)
        session = build_session("SILO", ctx, feature_label=label_silo)
        try:
            session._rate_limiter = shared_limiter(self)
            session._rate_limiter_meta = {}
        except Exception:
            pass
        self.session = session

    @staticmethod
    def supported_features() -> dict[str, bool]:
        return dict(_FEATURES)

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def _section(self) -> Mapping[str, Any]:
        return provider_block(self.config, self.instance_id)

    def health(self) -> Mapping[str, Any]:
        start = time.perf_counter()
        ok = False
        status = "not_configured"
        reason: str | None = None
        api: dict[str, Any] = {}
        if not auth_is_configured(self._section()):
            reason = "missing_authentication"
        else:
            try:
                resp = silo_request(self, "GET", "/progress/capabilities")
                code = int(resp.status_code)
                api["progress_capabilities"] = {"status": code}
                if 200 <= code < 300:
                    ok, status = True, "ok"
                elif code in (401, 403):
                    status = "auth_failed"
                    reason = problem_of(resp) or "unauthorized"
                elif code == 429:
                    status = reason = "rate_limited"
                else:
                    status = f"http:{code}"
                    reason = problem_of(resp) or status
            except SiloError as exc:
                status = "auth_failed" if "reconnect" in str(exc) or "verification" in str(exc) else "service_unavailable"
                reason = str(exc) or exc.__class__.__name__
            except Exception as exc:
                status = "service_unavailable"
                reason = f"exception:{exc.__class__.__name__}"
        if not ok:
            try:
                ctx.emit("debug", msg="silo.health.failed", status=status, reason=reason, api=api)
            except Exception:
                pass
        return {
            "ok": ok,
            "status": status,
            "latency_ms": int((time.perf_counter() - start) * 1000),
            "features": {name: bool(ok and enabled) for name, enabled in _FEATURES.items()},
            "details": {"reason": reason} if reason else None,
            "api": api,
        }

    def build_index(self, feature: str, **kwargs: Any) -> Mapping[str, dict[str, Any]]:
        mod = _FEATURE_MODULES.get(str(feature or "").strip().lower())
        if not mod:
            return {}
        return mod.build_index(self)

    def add(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        mod = _FEATURE_MODULES.get(str(feature or "").strip().lower())
        if not mod:
            return build_op_result(ok=True, count=0, unsupported=True)
        lst = list(items or [])
        if dry_run:
            return build_op_result(ok=True, count=len(lst), dry_run=True)
        return _result_from(mod.add(self, lst))

    def remove(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        mod = _FEATURE_MODULES.get(str(feature or "").strip().lower())
        if not mod:
            return build_op_result(ok=True, count=0, unsupported=True)
        lst = list(items or [])
        if dry_run:
            return build_op_result(ok=True, count=len(lst), dry_run=True)
        return _result_from(mod.remove(self, lst))


class _SILOOPS:
    def name(self) -> str:
        return "SILO"

    def label(self) -> str:
        return "Silo"

    def features(self) -> Mapping[str, bool]:
        return dict(_FEATURES)

    def state_read_features(self) -> Mapping[str, bool]:
        return dict(_FEATURES)

    def capabilities(self) -> Mapping[str, Any]:
        return get_manifest()["capabilities"]

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        return auth_is_configured(provider_block(cfg or {}, _current_instance_id(cfg)))

    def _adapter(self, cfg: Mapping[str, Any]) -> SILOModule:
        return SILOModule(cfg)

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)

    def _playlist_adapter(self, cfg: Mapping[str, Any], instance: str | None) -> SILOModule:
        inst = normalize_instance_id(instance) if instance is not None else _current_instance_id(cfg)
        return SILOModule({**cfg, "_cw_provider_instance": inst})

    def list_playlist_resources(self, cfg: Mapping[str, Any], *, instance: str | None = None):
        return list(feat_playlists.list_resources(self._playlist_adapter(cfg, instance)))

    def get_playlist_snapshot(self, cfg: Mapping[str, Any], playlist_id: str, *, instance: str | None = None):
        return feat_playlists.get_snapshot(self._playlist_adapter(cfg, instance), playlist_id)

    def create_playlist(self, cfg: Mapping[str, Any], name: str, *, media_type: str | None = None,
                        items: Iterable[Mapping[str, Any]] | None = None, instance: str | None = None, dry_run: bool = False):
        return feat_playlists.create(self._playlist_adapter(cfg, instance), name, media_type=media_type, items=list(items or []), dry_run=dry_run)

    def rename_playlist(self, cfg: Mapping[str, Any], playlist_id: str, name: str, *, instance: str | None = None, dry_run: bool = False):
        return feat_playlists.rename(self._playlist_adapter(cfg, instance), playlist_id, name, dry_run=dry_run)

    def delete_playlist(self, cfg: Mapping[str, Any], playlist_id: str, *, instance: str | None = None, dry_run: bool = False) -> dict[str, Any]:
        return feat_playlists.delete(self._playlist_adapter(cfg, instance), playlist_id, dry_run=dry_run)

    def add_playlist_items(self, cfg: Mapping[str, Any], playlist_id: str, items: Iterable[Mapping[str, Any]], *,
                           instance: str | None = None, dry_run: bool = False) -> dict[str, Any]:
        lst = list(items or [])
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True, "unresolved": [], "confirmed_keys": []}
        return feat_playlists.add(self._playlist_adapter(cfg, instance), playlist_id, lst)

    def remove_playlist_items(self, cfg: Mapping[str, Any], playlist_id: str, items: Iterable[Mapping[str, Any]], *,
                              instance: str | None = None, dry_run: bool = False) -> dict[str, Any]:
        lst = list(items or [])
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True, "unresolved": [], "confirmed_keys": []}
        return feat_playlists.remove(self._playlist_adapter(cfg, instance), playlist_id, lst)

    def reorder_playlist_items(self, cfg: Mapping[str, Any], playlist_id: str, ordered_keys: Iterable[str], *,
                               instance: str | None = None, dry_run: bool = False) -> dict[str, Any]:
        keys = list(ordered_keys or [])
        if dry_run:
            return {"ok": True, "count": 0, "dry_run": True}
        return feat_playlists.reorder(self._playlist_adapter(cfg, instance), playlist_id, keys)


OPS = _SILOOPS()
