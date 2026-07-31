# providers/sync/_mod_STREMIO.py
# CrossWatch Stremio sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import os
import time
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.provider_instances import normalize_instance_id
from providers.auth._auth_STREMIO import StremioClient, StremioAuthError
from providers.sync._mod_common import SimpleRateLimiter, build_op_result, build_session
from providers.sync.stremio import _history as feat_history
from providers.sync.stremio import _progress as feat_progress
from providers.sync.stremio import _ratings as feat_ratings
from providers.sync.stremio import _watchlist as feat_watchlist
from providers.sync.stremio._common import DEFAULT_STREMIO_PROFILE_ID, datastore_meta, is_configured

__VERSION__ = "0.1"
__all__ = ["get_manifest", "STREMIOModule", "OPS", "feat_history", "feat_progress", "feat_ratings", "feat_watchlist"]

if "ctx" not in globals():
    class _NullCtx:
        def emit(self, *args: Any, **kwargs: Any) -> None:
            pass

    ctx = _NullCtx()  # type: ignore[assignment]


_FEATURES = {"watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": False}
_FEATURE_MODULES = {"history": feat_history, "progress": feat_progress, "ratings": feat_ratings, "watchlist": feat_watchlist}


def _detail_text(value: Any) -> str:
    text = str(value or "").replace("\n", " ").replace("\r", " ").strip()
    for token in ("authKey", "auth_key", "password"):
        text = text.replace(token, f"{token[0]}***")
    return text[:180]


def _error_api(exc: StremioAuthError | None, status: str) -> dict[str, Any]:
    out: dict[str, Any] = {"status": getattr(exc, "status_code", None) or status}
    endpoint = str(getattr(exc, "endpoint", "") or "").strip()
    if endpoint:
        out["endpoint"] = endpoint
    detail = _detail_text(getattr(exc, "detail", None))
    if detail:
        out["detail"] = detail
    return out


def _current_instance_id() -> str:
    if str(os.getenv("CW_PROBE_PROVIDER") or "").upper().strip() == "STREMIO":
        return normalize_instance_id(os.getenv("CW_PROBE_INSTANCE"))
    if str(os.getenv("CW_PAIR_SRC") or "").upper().strip() == "STREMIO":
        return normalize_instance_id(os.getenv("CW_PAIR_SRC_INSTANCE"))
    if str(os.getenv("CW_PAIR_DST") or "").upper().strip() == "STREMIO":
        return normalize_instance_id(os.getenv("CW_PAIR_DST_INSTANCE"))
    return "default"


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "STREMIO",
        "label": "Stremio",
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
            "multi_profile": False,
            "features": dict(_FEATURES),
            "history": {
                "read": True,
                "write": True,
                "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
                "upsert": True,
                "remove": True,
                "observed_deletes": True,
                "requires_ids": ["imdb"],
            },
            "watchlist": {
                "read": True,
                "write": True,
                "types": {"movies": True, "shows": True, "seasons": False, "episodes": False},
                "upsert": True,
                "remove": True,
                "observed_deletes": True,
                "requires_ids": ["imdb"],
                "custom_lists": False,
            },
            "ratings": {
                "read": False,
                "write": True,
                "types": {"movies": True, "shows": True, "seasons": False, "episodes": False},
                "upsert": True,
                "remove": True,
                "observed_deletes": False,
                "accepted_ids": ["imdb"],
                "mode": "stremio_reactions",
                "direction": "destination_only",
                "thresholds": {"liked_min": 6.0, "loved_min": 8.0},
            },
            "progress": {
                "read": True,
                "write": True,
                "index_semantics": "present",
                "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
                "upsert": True,
                "remove": True,
                "observed_deletes": True,
                "requires_ids": ["imdb"],
                "requires_duration": True,
                "completion_policy": {"progress_write": {"mode": "none"}},
            },
        },
    }


class STREMIOModule:
    def __init__(self, cfg: Mapping[str, Any]):
        self.config = cfg or {}
        self.instance_id = _current_instance_id()
        self.stremio_profile_id = DEFAULT_STREMIO_PROFILE_ID
        session = build_session("STREMIO", ctx)
        try:
            session._rate_limiter = SimpleRateLimiter(rates_per_sec={"GET": 20.0, "POST": 20.0})
            session._rate_limiter_meta = {"get_per_sec": 20.0, "post_per_sec": 20.0}
        except Exception:
            pass
        self.client = StremioClient(self.config, instance_id=self.instance_id, session=session)

    @staticmethod
    def supported_features() -> dict[str, bool]:
        return dict(_FEATURES)

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def health(self) -> Mapping[str, Any]:
        start = time.perf_counter()
        ok = False
        status = "not_configured"
        reason = None
        api: dict[str, Any] = {}
        try:
            if not is_configured(self.config, self.instance_id):
                reason = "missing_authentication"
            else:
                datastore_meta(self)
                ok = True
                status = "ok"
                api["datastoreMeta"] = {"status": 200}
        except StremioAuthError as exc:
            reason = str(getattr(exc, "reason", "error") or "error")
            status = "auth_failed" if reason in {"missing_auth_key", "invalid_credentials"} else reason
            api["datastoreMeta"] = _error_api(exc, status)
        except Exception:
            status = "service_unavailable"
            reason = "service_unavailable"
            api["datastoreMeta"] = {"status": status}
        if not ok:
            try:
                ctx.emit("debug", msg="stremio.health.failed", status=status, reason=reason, api=api)
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
        return mod.build_index(self, **kwargs) if mod else {}

    def add(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        mod = _FEATURE_MODULES.get(str(feature or "").strip().lower())
        return mod.add(self, items, dry_run=dry_run) if mod else build_op_result(ok=True, count=0, unsupported=True)

    def remove(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        mod = _FEATURE_MODULES.get(str(feature or "").strip().lower())
        return mod.remove(self, items, dry_run=dry_run) if mod else build_op_result(ok=True, count=0, unsupported=True)


class _STREMIOOPS:
    def name(self) -> str:
        return "STREMIO"

    def label(self) -> str:
        return "Stremio"

    def features(self) -> Mapping[str, bool]:
        return dict(_FEATURES)

    def state_read_features(self) -> Mapping[str, bool]:
        return dict(_FEATURES)

    def capabilities(self) -> Mapping[str, Any]:
        return get_manifest()["capabilities"]

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        return is_configured(cfg, "default")

    def _adapter(self, cfg: Mapping[str, Any]) -> STREMIOModule:
        return STREMIOModule(cfg)

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)


OPS = _STREMIOOPS()
