# providers/sync/_mod_PUNCHPLAY.py
# CrossWatch PunchPlay sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.provider_instances import normalize_instance_id
from providers.auth._auth_PUNCHPLAY import ME_URL, is_configured as auth_is_configured
from providers.sync._mod_common import SimpleRateLimiter, build_op_result, build_session, dedup_keys
from providers.sync.punchplay import _history as feat_history
from providers.sync.punchplay import _progress as feat_progress
from providers.sync.punchplay import _ratings as feat_ratings
from providers.sync.punchplay import _watchlist as feat_watchlist
from providers.sync.punchplay._common import (
    DEFAULT_GET_PER_SEC,
    DEFAULT_POST_PER_SEC,
    cfg_float,
    error_of,
    punchplay_request,
    request_id_of,
)

__VERSION__ = "0.2"
__all__ = ["get_manifest", "PUNCHPLAYModule", "OPS", "feat_history", "feat_progress", "feat_ratings", "feat_watchlist"]

if "ctx" not in globals():
    class _NullCtx:
        def emit(self, *args: Any, **kwargs: Any) -> None:
            pass

    ctx = _NullCtx()  # type: ignore[assignment]


_FEATURES = {"watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": False}
_FEATURE_MODULES = {
    "watchlist": feat_watchlist,
    "ratings": feat_ratings,
    "history": feat_history,
    "progress": feat_progress,
}

_ACCEPTED_IDS = ["tmdb", "imdb", "tvdb", "mal"]


def _current_instance_id() -> str:
    if str(os.getenv("CW_PROBE_PROVIDER") or "").upper().strip() == "PUNCHPLAY":
        return normalize_instance_id(os.getenv("CW_PROBE_INSTANCE"))
    if str(os.getenv("CW_PAIR_SRC") or "").upper().strip() == "PUNCHPLAY":
        return normalize_instance_id(os.getenv("CW_PAIR_SRC_INSTANCE"))
    if str(os.getenv("CW_PAIR_DST") or "").upper().strip() == "PUNCHPLAY":
        return normalize_instance_id(os.getenv("CW_PAIR_DST_INSTANCE"))
    return "default"


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "PUNCHPLAY",
        "label": "PunchPlay",
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
                "provides_ids": ["tmdb"],
                "custom_lists": False,
                "batch_size": 100,
            },
            "ratings": {
                "read": True,
                "write": True,
                "types": {"movies": True, "shows": True, "seasons": True, "episodes": True},
                "upsert": True,
                "remove": True,
                "observed_deletes": True,
                "accepted_ids": list(_ACCEPTED_IDS),
                "provides_ids": ["tmdb"],
                "scale": "1-10",
                "rounds_to_int": True,
                "batch_size": 100,
            },
            "history": {
                "read": True,
                "write": True,
                "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
                "upsert": True,
                "remove": True,
                "observed_deletes": True,
                "accepted_ids": list(_ACCEPTED_IDS),
                "provides_ids": ["tmdb"],
                "rewatch": True,
                "requires_watched_at": True,
                "batch_size": 100,
            },
            "progress": {
                "read": True,
                "write": True,
                "index_semantics": "present",
                "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
                "upsert": True,
                "remove": True,
                "observed_deletes": True,
                "accepted_ids": ["tmdb", "imdb", "tvdb"],
                "provides_ids": ["tmdb"],
                "requires_duration": False,
            },
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
    if isinstance(data.get("status_counts"), Mapping):
        extra["status_counts"] = dict(data.get("status_counts") or {})
    return build_op_result(
        ok=bool(data.get("ok", True)),
        count=len(confirmed),
        confirmed_keys=confirmed,
        unresolved_keys=unresolved_keys,
        unresolved=data.get("unresolved") or [],
        **extra,
    )


class PUNCHPLAYModule:
    def __init__(self, cfg: Mapping[str, Any]):
        self.config = cfg or {}
        self.instance_id = _current_instance_id()
        section = (self.config.get("punchplay") or {}) if isinstance(self.config, Mapping) else {}
        get_rps = cfg_float(section, "get_per_sec", DEFAULT_GET_PER_SEC)
        post_rps = cfg_float(section, "post_per_sec", DEFAULT_POST_PER_SEC)
        rl = section.get("rate_limit") if isinstance(section.get("rate_limit"), Mapping) else {}
        if rl:
            get_rps = cfg_float(rl, "get_per_sec", get_rps)
            post_rps = cfg_float(rl, "post_per_sec", post_rps)

        session = build_session("PUNCHPLAY", ctx)
        try:
            session._rate_limiter = SimpleRateLimiter(rates_per_sec={"GET": get_rps, "POST": post_rps, "DELETE": post_rps})
            session._rate_limiter_meta = {"get_per_sec": get_rps, "post_per_sec": post_rps}
        except Exception:
            pass
        try:
            session.headers.setdefault("Accept", "application/json")
            session.headers.setdefault("User-Agent", f"CrossWatch PUNCHPLAY/{__VERSION__}")
        except Exception:
            pass
        self.session = session

    @staticmethod
    def supported_features() -> dict[str, bool]:
        return dict(_FEATURES)

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def _section(self) -> Mapping[str, Any]:
        block = self.config.get("punchplay") if isinstance(self.config, Mapping) else None
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
                resp = punchplay_request(self, "GET", ME_URL, timeout=10.0, max_retries=1)
                code = int(resp.status_code)
                me_api: dict[str, Any] = {"status": code}
                api["me"] = me_api
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
                    rid = request_id_of(resp)
                    if rid:
                        me_api["request_id"] = rid
            except Exception as exc:
                status = "service_unavailable"
                reason = f"exception:{exc.__class__.__name__}"
                api["me"] = {"status": status}

        if not ok:
            try:
                ctx.emit("debug", msg="punchplay.health.failed", status=status, reason=reason, api=api)
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


class _PUNCHPLAYOPS:
    def name(self) -> str:
        return "PUNCHPLAY"

    def label(self) -> str:
        return "PunchPlay"

    def features(self) -> Mapping[str, bool]:
        return dict(_FEATURES)

    def state_read_features(self) -> Mapping[str, bool]:
        return dict(_FEATURES)

    def capabilities(self) -> Mapping[str, Any]:
        return get_manifest()["capabilities"]

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        block = (cfg or {}).get("punchplay") if isinstance(cfg, Mapping) else None
        return auth_is_configured(block if isinstance(block, Mapping) else {})

    def _adapter(self, cfg: Mapping[str, Any]) -> PUNCHPLAYModule:
        return PUNCHPLAYModule(cfg)

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)


OPS = _PUNCHPLAYOPS()
