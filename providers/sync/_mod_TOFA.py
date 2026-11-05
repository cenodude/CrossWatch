# providers/sync/_mod_TOFA.py
# CrossWatch Tofa sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.provider_instances import normalize_instance_id
from providers.auth._auth_TOFA import TofaError, is_configured as auth_is_configured, provider_block
from providers.sync._mod_common import build_op_result, build_session, dedup_keys
from providers.sync.tofa import _collection as feat_collection
from providers.sync.tofa import _history as feat_history
from providers.sync.tofa import _presence as feat_presence
from providers.sync.tofa import _progress as feat_progress
from providers.sync.tofa import _watchlist as feat_watchlist
from providers.sync.tofa._common import REQUIRED_CAPABILITIES, TofaSyncError, capabilities, label_tofa, shared_limiter

__VERSION__ = "0.1"
__all__ = ["get_manifest", "TOFAModule", "OPS", "feat_collection", "feat_history", "feat_progress", "feat_watchlist"]

if "ctx" not in globals():
    class _NullCtx:
        def emit(self, *args: Any, **kwargs: Any) -> None:
            pass

    ctx = _NullCtx()  # type: ignore[assignment]


_FEATURES = {"watchlist": True, "ratings": False, "history": True, "progress": True, "playlists": False, "collection": True}
_FEATURE_MODULES = {
    "watchlist": feat_watchlist,
    "history": feat_history,
    "progress": feat_progress,
    "collection": feat_collection,
}
_ACCEPTED_IDS = ["tmdb", "imdb", "tvdb"]


def _current_instance_id(cfg: Mapping[str, Any] | None = None) -> str:
    explicit_instance = (cfg or {}).get("_cw_provider_instance")
    if explicit_instance is not None:
        return normalize_instance_id(explicit_instance)
    if str(os.getenv("CW_PROBE_PROVIDER") or "").upper().strip() == "TOFA":
        return normalize_instance_id(os.getenv("CW_PROBE_INSTANCE"))
    if str(os.getenv("CW_PAIR_SRC") or "").upper().strip() == "TOFA":
        return normalize_instance_id(os.getenv("CW_PAIR_SRC_INSTANCE"))
    if str(os.getenv("CW_PAIR_DST") or "").upper().strip() == "TOFA":
        return normalize_instance_id(os.getenv("CW_PAIR_DST_INSTANCE"))
    return "default"


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "TOFA",
        "label": "Tofa",
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
                "accepted_ids": ["tmdb"],
                "provides_ids": ["tmdb"],
            },
            "history": {
                "read": True,
                "write": True,
                "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
                "upsert": True,
                "remove": True,
                "observed_deletes": True,
                "accepted_ids": list(_ACCEPTED_IDS),
                "provides_ids": list(_ACCEPTED_IDS),
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
                "provides_ids": list(_ACCEPTED_IDS),
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
                "provides_ids": list(_ACCEPTED_IDS),
            },
            "library_presence": {"features": list(feat_presence.FEATURES)},
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


class TOFAModule:
    def __init__(self, cfg: Mapping[str, Any]):
        self.config = cfg or {}
        self.instance_id = _current_instance_id(cfg)
        session = build_session("TOFA", ctx, feature_label=label_tofa)
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
                caps = capabilities(self)
                missing = [c for c in REQUIRED_CAPABILITIES if c not in caps]
                api["system_info"] = {"capabilities": len(caps)}
                if missing:
                    status = "unsupported_server"
                    reason = "missing_capabilities:" + ",".join(missing)
                else:
                    ok, status = True, "ok"
            except TofaError as exc:
                status = "auth_failed" if str(exc) in ("reconnect_required", "profile_locked", "authentication_failed") else "service_unavailable"
                reason = str(exc) or exc.__class__.__name__
            except TofaSyncError as exc:
                text = str(exc)
                status = "auth_failed" if text.endswith(("_401", "_403")) else "rate_limited" if text.endswith("_429") else "service_unavailable"
                reason = text
            except Exception as exc:
                status = "service_unavailable"
                reason = f"exception:{exc.__class__.__name__}"
        if not ok:
            try:
                ctx.emit("debug", msg="tofa.health.failed", status=status, reason=reason, api=api)
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

    def library_presence(self, feature: str, items: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        return feat_presence.presence(self, items, feature=feature)


class _TOFAOPS:
    def name(self) -> str:
        return "TOFA"

    def label(self) -> str:
        return "Tofa"

    def features(self) -> Mapping[str, bool]:
        return dict(_FEATURES)

    def state_read_features(self) -> Mapping[str, bool]:
        return dict(_FEATURES)

    def capabilities(self) -> Mapping[str, Any]:
        return get_manifest()["capabilities"]

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        return auth_is_configured(provider_block(cfg or {}, _current_instance_id(cfg)))

    def _adapter(self, cfg: Mapping[str, Any]) -> TOFAModule:
        return TOFAModule(cfg)

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)

    def library_presence(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str) -> list[dict[str, Any]]:
        return self._adapter(cfg).library_presence(feature, items)


OPS = _TOFAOPS()
