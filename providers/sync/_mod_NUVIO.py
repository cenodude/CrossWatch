# providers/sync/_mod_NUVIO.py
# CrossWatch Nuvio sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
import os
from collections.abc import Iterable, Mapping
from typing import Any

from providers.auth._auth_NUVIO import (
    NuvioAuthError,
    NuvioClient,
    NuvioInvalidResponse,
    NuvioProfileUnavailable,
    NuvioServiceUnavailable,
    NuvioTokenRefreshError,
    is_configured as nuvio_is_configured,
    profile_id_value,
    provider_block,
)

from ._mod_common import SimpleRateLimiter, build_op_result, build_session
from .nuvio import _history as feat_history
from .nuvio import _progress as feat_progress
from .nuvio import _watchlist as feat_watchlist
from .nuvio._common import pull_library_rows, pull_watch_progress_rows, pull_watched_rows
from cw_platform.provider_instances import normalize_instance_id

__VERSION__ = "0.4"
__all__ = ["get_manifest", "NUVIOModule", "OPS"]

if "ctx" not in globals():
    class _NullCtx:
        def emit(self, *args: Any, **kwargs: Any) -> None:
            pass

    ctx = _NullCtx()  # type: ignore[assignment]


def _features_flags() -> dict[str, bool]:
    return {"watchlist": True, "ratings": False, "history": True, "progress": True, "playlists": False}


def _rate_limit_settings(block: Mapping[str, Any]) -> dict[str, float]:
    raw = block.get("rate_limit")
    values = dict(raw) if isinstance(raw, Mapping) else {}

    def _rate(key: str, default: float) -> float:
        try:
            rate = float(values.get(key, default))
        except Exception:
            rate = default
        return max(0.0, rate)

    return {"get_per_sec": _rate("get_per_sec", 100.0), "post_per_sec": _rate("post_per_sec", 100.0)}


def _current_instance_id() -> str:
    probe = str(os.getenv("CW_PROBE_PROVIDER") or "").upper().strip()
    if probe == "NUVIO":
        return normalize_instance_id(os.getenv("CW_PROBE_INSTANCE"))
    if str(os.getenv("CW_PAIR_SRC") or "").upper().strip() == "NUVIO":
        return normalize_instance_id(os.getenv("CW_PAIR_SRC_INSTANCE"))
    if str(os.getenv("CW_PAIR_DST") or "").upper().strip() == "NUVIO":
        return normalize_instance_id(os.getenv("CW_PAIR_DST_INSTANCE"))
    return "default"


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "NUVIO",
        "label": "Nuvio",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": True,
        "experimental": True,
        "features": _features_flags(),
        "requires": [],
        "capabilities": {
            "bidirectional": True,
            "experimental": True,
            "verify_after_write": True,
            "provides_ids": True,
            "index_semantics": "present",
            "features": _features_flags(),
            "progress": {
                "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
                "upsert": True,
                "remove": True,
                "observed_deletes": True,
                "requires_ids": ["tmdb", "imdb", "tvdb"],
                "requires_duration": True,
                "completion_policy": {
                    "progress_write": {
                        "mode": "auto_complete",
                        "percent": 90,
                        "min_duration_seconds": 60,
                    },
                },
            },
            "history": {
                "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
                "upsert": True,
                "remove": True,
                "observed_deletes": True,
                "requires_ids": ["tmdb", "imdb", "tvdb"],
            },
            "watchlist": {
                "types": {"movies": True, "shows": True, "seasons": False, "episodes": False},
                "upsert": True,
                "remove": True,
                "observed_deletes": True,
                "requires_ids": ["tmdb", "imdb", "tvdb"],
            },
        },
    }


class NUVIOModule:
    def __init__(self, cfg: Mapping[str, Any]):
        self.config = cfg or {}
        self.instance_id = _current_instance_id()
        block = provider_block(self.config, self.instance_id)
        rate = _rate_limit_settings(block)
        session = build_session("NUVIO", ctx)
        try:
            session._rate_limiter = SimpleRateLimiter(rates_per_sec={"GET": rate["get_per_sec"], "POST": rate["post_per_sec"]})
            session._rate_limiter_meta = rate
        except Exception:
            pass
        self.client = NuvioClient(self.config, instance_id=self.instance_id, session=session)

    @staticmethod
    def supported_features() -> dict[str, bool]:
        return _features_flags()

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def health(self) -> Mapping[str, Any]:
        start = time.perf_counter()
        status = "not_configured"
        ok = False
        reason: str | None = None
        try:
            block = provider_block(self.config, self.instance_id)
            if not nuvio_is_configured(block):
                if not profile_id_value(block):
                    reason = "missing_profile"
                else:
                    reason = "missing_authentication"
            else:
                profiles = self.client.pull_profiles(self.config, refresh=True)
                pid = profile_id_value(block)
                if not isinstance(profiles, list):
                    status = "invalid_response"
                    reason = "profiles_not_list"
                elif not any(int(p.get("profile_id") or 0) == pid for p in profiles if isinstance(p, Mapping)):
                    status = "profile_unavailable"
                    reason = "profile_unavailable"
                else:
                    pull_watch_progress_rows(self, limit=1, max_pages=1)
                    pull_watched_rows(self, page_size=1, max_pages=1)
                    pull_library_rows(self, limit=1, max_pages=1)
                    ok = True
                    status = "ok"
        except NuvioTokenRefreshError:
            status = "token_refresh_failed"
            reason = "token_refresh_failed"
        except NuvioAuthError:
            status = "auth_failed"
            reason = "auth_failed"
        except NuvioProfileUnavailable:
            status = "profile_unavailable"
            reason = "profile_unavailable"
        except NuvioInvalidResponse:
            status = "invalid_response"
            reason = "invalid_response"
        except NuvioServiceUnavailable:
            status = "service_unavailable"
            reason = "service_unavailable"
        except Exception:
            status = "service_unavailable"
            reason = "service_unavailable"
        latency_ms = int((time.perf_counter() - start) * 1000)
        return {
            "ok": ok,
            "status": status,
            "latency_ms": latency_ms,
            "features": _features_flags(),
            "details": {"reason": reason} if reason else None,
            "api": {"profiles": {"status": status}, "progress": {"status": status}, "history": {"status": status}, "library": {"status": status}},
        }

    def build_index(self, feature: str, **kwargs: Any) -> dict[str, dict[str, Any]]:
        feature_name = str(feature or "").strip().lower()
        if feature_name == "progress":
            return feat_progress.build_index(self)
        if feature_name == "history":
            return feat_history.build_index(self)
        if feature_name == "watchlist":
            return feat_watchlist.build_index(self)
        return {}

    def add(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        feature_name = str(feature or "").strip().lower()
        if feature_name == "progress":
            return feat_progress.add(self, items, dry_run=dry_run)
        if feature_name == "history":
            return feat_history.add(self, items, dry_run=dry_run)
        if feature_name == "watchlist":
            return feat_watchlist.add(self, items, dry_run=dry_run)
        return build_op_result(ok=True, count=0, unsupported=True)

    def remove(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        feature_name = str(feature or "").strip().lower()
        if feature_name == "progress":
            return feat_progress.remove(self, items, dry_run=dry_run)
        if feature_name == "history":
            return feat_history.remove(self, items, dry_run=dry_run)
        if feature_name == "watchlist":
            return feat_watchlist.remove(self, items, dry_run=dry_run)
        return build_op_result(ok=True, count=0, unsupported=True)


class _NUVIOOPS:
    def name(self) -> str:
        return "NUVIO"

    def label(self) -> str:
        return "Nuvio"

    def features(self) -> Mapping[str, bool]:
        return _features_flags()

    def state_read_features(self) -> Mapping[str, bool]:
        return _features_flags()

    def capabilities(self) -> Mapping[str, Any]:
        return get_manifest()["capabilities"]

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        return nuvio_is_configured(provider_block(cfg, "default"))

    def _adapter(self, cfg: Mapping[str, Any]) -> NUVIOModule:
        return NUVIOModule(cfg)

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)


OPS = _NUVIOOPS()
