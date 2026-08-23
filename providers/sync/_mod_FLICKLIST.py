# CrossWatch FlickList sync module
from __future__ import annotations

import os
import time
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from cw_platform.provider_instances import normalize_instance_id, resolve_provider_block
from providers.auth._auth_FLICKLIST import ME_URL, is_configured as auth_is_configured
from providers.sync._mod_common import SimpleRateLimiter, build_op_result, build_session, dedup_keys
from providers.sync.flicklist import _history as feat_history
from providers.sync.flicklist import _playlists as feat_playlists
from providers.sync.flicklist import _progress as feat_progress
from providers.sync.flicklist import _ratings as feat_ratings
from providers.sync.flicklist import _watchlist as feat_watchlist
from providers.sync.flicklist._common import (
    DEFAULT_GET_PER_SEC,
    DEFAULT_POST_PER_SEC,
    cfg_float,
    error_of,
    flicklist_request,
)

__VERSION__ = "0.1"
__all__ = ["get_manifest", "FLICKLISTModule", "OPS", "feat_history", "feat_playlists", "feat_progress", "feat_ratings", "feat_watchlist"]

if "ctx" not in globals():
    class _NullCtx:
        def emit(self, *args: Any, **kwargs: Any) -> None:
            pass

    ctx = _NullCtx()  # type: ignore[assignment]


_SYNC_DISABLED_REASON = "FlickList sync API writes are disabled until the remote bulk sync API is reliable; scrobble support remains enabled."
_FEATURES = {"watchlist": False, "ratings": False, "history": False, "progress": False, "playlists": False}
_FEATURE_MODULES = {
    "watchlist": feat_watchlist,
    "ratings": feat_ratings,
    "history": feat_history,
    "progress": feat_progress,
}
_ACCEPTED_IDS = ["fldb", "tmdb", "imdb", "tvdb"]


def _current_instance_id() -> str:
    if str(os.getenv("CW_PROBE_PROVIDER") or "").upper().strip() == "FLICKLIST":
        return normalize_instance_id(os.getenv("CW_PROBE_INSTANCE"))
    if str(os.getenv("CW_PAIR_SRC") or "").upper().strip() == "FLICKLIST":
        return normalize_instance_id(os.getenv("CW_PAIR_SRC_INSTANCE"))
    if str(os.getenv("CW_PAIR_DST") or "").upper().strip() == "FLICKLIST":
        return normalize_instance_id(os.getenv("CW_PAIR_DST_INSTANCE"))
    return "default"


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "FLICKLIST",
        "label": "FlickList",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": False,
        "experimental": True,
        "disabled": True,
        "disabled_reason": _SYNC_DISABLED_REASON,
        "features": dict(_FEATURES),
        "requires": ["requests"],
        "capabilities": {
            "bidirectional": False,
            "experimental": True,
            "disabled": True,
            "disabled_reason": _SYNC_DISABLED_REASON,
            "provides_ids": True,
            "index_semantics": "present",
            "multi_profile": True,
            "features": dict(_FEATURES),
            "watchlist": {
                "read": False,
                "write": False,
                "types": {"movies": True, "shows": True, "seasons": False, "episodes": False},
                "upsert": False,
                "remove": False,
                "observed_deletes": False,
                "accepted_ids": list(_ACCEPTED_IDS),
                "provides_ids": ["fldb", "tmdb", "imdb", "tvdb", "anilist"],
                "custom_lists": False,
                "batch_size": 1000,
            },
            "ratings": {
                "read": False,
                "write": False,
                "types": {"movies": True, "shows": True, "seasons": False, "episodes": True},
                "upsert": False,
                "remove": False,
                "observed_deletes": False,
                "accepted_ids": list(_ACCEPTED_IDS),
                "provides_ids": ["fldb", "tmdb", "imdb", "tvdb", "anilist"],
                "scale": "0.5-10",
                "rounds_to_half": True,
                "batch_size": 1000,
            },
            "history": {
                "read": False,
                "write": False,
                "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
                "upsert": False,
                "remove": False,
                "observed_deletes": False,
                "accepted_ids": list(_ACCEPTED_IDS),
                "provides_ids": ["fldb", "tmdb", "imdb", "tvdb", "anilist"],
                "rewatch": True,
                "requires_watched_at": False,
                "batch_size": 1000,
            },
            "progress": {
                "read": False,
                "write": False,
                "index_semantics": "present",
                "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
                "upsert": False,
                "remove": False,
                "observed_deletes": False,
                "accepted_ids": list(_ACCEPTED_IDS),
                "provides_ids": ["fldb", "tmdb", "imdb", "tvdb", "anilist"],
                "requires_duration": False,
            },
            "playlists": {
                "read": False,
                "write": False,
                "create": False,
                "remove": False,
                "reorder": False,
                "smart_lists_read_only": True,
                "types": {"movies": True, "shows": True, "seasons": False, "episodes": True},
                "accepted_ids": list(_ACCEPTED_IDS),
                "batch_size": 1000,
            },
            "scrobble": {"read": False, "write": True, "actions": ["start", "pause", "stop"], "accepted_ids": list(_ACCEPTED_IDS)},
        },
    }


def _result_from(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    data = dict(raw or {})
    confirmed = [str(k) for k in (data.get("confirmed_keys") or []) if k]
    unresolved_keys = [str(k) for k in (data.get("unresolved_keys") or []) if k]
    skipped = [str(k) for k in (data.get("skipped_keys") or []) if k]
    deferred = [str(k) for k in (data.get("deferred_keys") or []) if k]
    skipped_all = dedup_keys(list(skipped) + list(deferred))
    accepted_raw = [str(k) for k in (data.get("accepted_keys") or []) if k]
    accepted = dedup_keys(accepted_raw or (list(confirmed) + list(skipped_all)))
    extra: dict[str, Any] = {}
    if skipped_all:
        extra["skipped_keys"] = skipped_all
        extra["skipped"] = len(skipped_all)
    if deferred:
        extra["deferred_keys"] = deferred
        extra["deferred"] = len(deferred)
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


class FLICKLISTModule:
    def __init__(self, cfg: Mapping[str, Any], instance_id: str | None = None):
        self.config = cfg or {}
        self.instance_id = normalize_instance_id(instance_id) if instance_id is not None else _current_instance_id()
        section = (self.config.get("flicklist") or {}) if isinstance(self.config, Mapping) else {}
        get_rps = cfg_float(section, "get_per_sec", DEFAULT_GET_PER_SEC)
        post_rps = cfg_float(section, "post_per_sec", DEFAULT_POST_PER_SEC)
        rl = section.get("rate_limit") if isinstance(section.get("rate_limit"), Mapping) else {}
        if rl:
            get_rps = cfg_float(rl, "get_per_sec", get_rps)
            post_rps = cfg_float(rl, "post_per_sec", post_rps)
        session = build_session("FLICKLIST", ctx)
        try:
            session._rate_limiter = SimpleRateLimiter(rates_per_sec={"GET": get_rps, "POST": post_rps, "DELETE": post_rps})
            session._rate_limiter_meta = {"get_per_sec": get_rps, "post_per_sec": post_rps}
        except Exception:
            pass
        try:
            session.headers.setdefault("Accept", "application/json")
            session.headers.setdefault("User-Agent", f"CrossWatch FLICKLIST/{__VERSION__}")
        except Exception:
            pass
        self.session = session

    @staticmethod
    def supported_features() -> dict[str, bool]:
        return dict(_FEATURES)

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def _section(self) -> Mapping[str, Any]:
        try:
            return resolve_provider_block(self.config, "flicklist", self.instance_id) or {}
        except Exception:
            block = self.config.get("flicklist") if isinstance(self.config, Mapping) else None
            return block if isinstance(block, Mapping) else {}

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
                resp = flicklist_request(self, "GET", ME_URL, timeout=10.0, max_retries=1)
                code = int(resp.status_code)
                api["me"] = {"status": code}
                if 200 <= code < 300:
                    ok = True
                    status = "ok"
                elif code in (401, 403):
                    status = "auth_failed"
                    reason = "unauthorized"
                elif code == 429:
                    status = "rate_limited"
                    reason = "rate_limited"
                else:
                    status = f"http:{code}"
                    reason = error_of(resp) or status
            except Exception as exc:
                status = "service_unavailable"
                reason = f"exception:{exc.__class__.__name__}"
                api["me"] = {"status": status}
        if not ok:
            try:
                ctx.emit("debug", msg="flicklist.health.failed", status=status, reason=reason, api=api)
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
        return mod.build_index(self, **kwargs)

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

    def list_playlist_resources(self) -> Sequence[Any]:
        return feat_playlists.list_resources(self)

    def get_playlist_snapshot(self, playlist_id: str) -> Any:
        return feat_playlists.get_snapshot(self, playlist_id)

    def create_playlist(self, name: str, *, media_type: str | None = None, items: Sequence[Mapping[str, Any]] | None = None, dry_run: bool = False) -> Any:
        return feat_playlists.create(self, name, media_type=media_type, items=items, dry_run=dry_run)

    def add_playlist_items(self, playlist_id: str, items: Sequence[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        if dry_run:
            return {"ok": True, "count": len(list(items or [])), "dry_run": True}
        return feat_playlists.add(self, playlist_id, items)

    def remove_playlist_items(self, playlist_id: str, items: Sequence[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        if dry_run:
            return {"ok": True, "count": len(list(items or [])), "dry_run": True}
        return feat_playlists.remove(self, playlist_id, items)

    def reorder_playlist_items(self, playlist_id: str, ordered_keys: Sequence[str], *, dry_run: bool = False) -> dict[str, Any]:
        return feat_playlists.reorder(self, playlist_id, ordered_keys)


class _FLICKLISTOPS:
    def name(self) -> str:
        return "FLICKLIST"

    def label(self) -> str:
        return "FlickList"

    def features(self) -> Mapping[str, bool]:
        return dict(_FEATURES)

    def state_read_features(self) -> Mapping[str, bool]:
        return dict(_FEATURES)

    def capabilities(self) -> Mapping[str, Any]:
        return get_manifest()["capabilities"]

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        try:
            block = resolve_provider_block(cfg or {}, "flicklist", _current_instance_id())
        except Exception:
            block = (cfg or {}).get("flicklist") if isinstance(cfg, Mapping) else None
        return auth_is_configured(block if isinstance(block, Mapping) else {})

    def _adapter(self, cfg: Mapping[str, Any], instance: str | None = None) -> FLICKLISTModule:
        return FLICKLISTModule(cfg, instance_id=instance)

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)

    def list_playlist_resources(self, cfg: Mapping[str, Any], *, instance: str | None = None) -> Sequence[Any]:
        old = os.environ.get("CW_PROBE_INSTANCE")
        if instance is not None:
            os.environ["CW_PROBE_INSTANCE"] = str(instance)
        try:
            return self._adapter(cfg, instance).list_playlist_resources()
        finally:
            if old is None:
                os.environ.pop("CW_PROBE_INSTANCE", None)
            else:
                os.environ["CW_PROBE_INSTANCE"] = old

    def get_playlist_snapshot(self, cfg: Mapping[str, Any], playlist_id: str, *, instance: str | None = None) -> Any:
        return self._adapter(cfg, instance).get_playlist_snapshot(playlist_id)

    def create_playlist(self, cfg: Mapping[str, Any], name: str, *, media_type: str | None = None, instance: str | None = None, dry_run: bool = False) -> Any:
        return self._adapter(cfg, instance).create_playlist(name, media_type=media_type, dry_run=dry_run)

    def add_playlist_items(self, cfg: Mapping[str, Any], playlist_id: str, items: Sequence[Mapping[str, Any]], *, instance: str | None = None, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg, instance).add_playlist_items(playlist_id, items, dry_run=dry_run)

    def remove_playlist_items(self, cfg: Mapping[str, Any], playlist_id: str, items: Sequence[Mapping[str, Any]], *, instance: str | None = None, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg, instance).remove_playlist_items(playlist_id, items, dry_run=dry_run)

    def reorder_playlist_items(self, cfg: Mapping[str, Any], playlist_id: str, ordered_keys: Sequence[str], *, instance: str | None = None, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg, instance).reorder_playlist_items(playlist_id, ordered_keys, dry_run=dry_run)


OPS = _FLICKLISTOPS()
