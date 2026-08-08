# providers/sync/_mod_FLOPPY.py
# CrossWatch - Floppy sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.provider_instances import normalize_instance_id
from providers.auth._auth_FLOPPY import FloppyAuthError, FloppyClient
from providers.sync._mod_common import SimpleRateLimiter, build_op_result, build_session
from providers.sync.floppy import _history as feat_history
from providers.sync.floppy import _progress as feat_progress
from providers.sync.floppy import _ratings as feat_ratings
from providers.sync.floppy import _watchlist as feat_watchlist
from providers.sync.floppy._common import api_delete, api_get, configured_block, is_configured, media_parts_from_item_id, paged

__VERSION__ = "0.2"
__all__ = ["get_manifest", "FLOPPYModule", "OPS"]

if "ctx" not in globals():
    class _NullCtx:
        def emit(self, *args: Any, **kwargs: Any) -> None:
            pass

    ctx = _NullCtx()  # type: ignore[assignment]


_FEATURES = {"watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": False}
_FEATURE_MODULES = {"watchlist": feat_watchlist, "ratings": feat_ratings, "history": feat_history, "progress": feat_progress}


def _rate_limit_settings(block: Mapping[str, Any]) -> dict[str, float]:
    raw = block.get("rate_limit")
    values = dict(raw) if isinstance(raw, Mapping) else {}

    def _rate(key: str, default: float) -> float:
        try:
            rate = float(values.get(key, default))
        except Exception:
            rate = default
        return max(0.0, rate)

    return {"get_per_sec": _rate("get_per_sec", 20.0), "post_per_sec": _rate("post_per_sec", 20.0)}


def _current_instance_id() -> str:
    if str(os.getenv("CW_PROBE_PROVIDER") or "").upper().strip() == "FLOPPY":
        return normalize_instance_id(os.getenv("CW_PROBE_INSTANCE"))
    if str(os.getenv("CW_PAIR_SRC") or "").upper().strip() == "FLOPPY":
        return normalize_instance_id(os.getenv("CW_PAIR_SRC_INSTANCE"))
    if str(os.getenv("CW_PAIR_DST") or "").upper().strip() == "FLOPPY":
        return normalize_instance_id(os.getenv("CW_PAIR_DST_INSTANCE"))
    return "default"


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "FLOPPY",
        "label": "Floppy",
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
            "features": dict(_FEATURES),
            "watchlist": {"read": True, "write": True, "types": {"movies": True, "shows": True, "seasons": False, "episodes": False}, "upsert": True, "remove": True, "observed_deletes": True, "requires_ids": ["tmdb"], "custom_lists": True},
            "ratings": {"read": True, "write": True, "types": {"movies": True, "shows": True, "seasons": False, "episodes": False}, "upsert": True, "remove": True, "observed_deletes": True, "requires_ids": ["tmdb"], "scale": "0-10"},
            "history": {"read": True, "write": True, "types": {"movies": True, "shows": False, "seasons": False, "episodes": True}, "upsert": True, "remove": True, "observed_deletes": True, "requires_ids": ["tmdb"], "event_history": True, "rewatches": {"read": True, "write": True, "account_gate": False}},
            "progress": {"read": True, "write": True, "types": {"movies": True, "shows": False, "seasons": False, "episodes": True}, "upsert": True, "remove": True, "observed_deletes": False, "requires_ids": ["tmdb"], "units": "seconds", "completion_policy": {"progress_write": {"mode": "none"}}},
            "playlists": {"read": False, "write": False},
        },
    }


class FLOPPYModule:
    def __init__(self, cfg: Mapping[str, Any]):
        self.config = cfg or {}
        self.instance_id = _current_instance_id()
        block = configured_block(self.config, self.instance_id)
        session = build_session("FLOPPY", ctx)
        try:
            rate = _rate_limit_settings(block)
            session._rate_limiter = SimpleRateLimiter(rates_per_sec={"GET": rate["get_per_sec"], "POST": rate["post_per_sec"]})
            session._rate_limiter_meta = rate
        except Exception:
            pass
        self.client = FloppyClient(
            str(block.get("server_url") or ""),
            str(block.get("api_token") or ""),
            verify_ssl=bool(block.get("verify_ssl", False)),
            timeout=float(block.get("timeout", 12.0) or 12.0),
            session=session,
        )

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
                api_get(self, "lists", params={"limit": 1})
                ok = True
                status = "ok"
                api["lists"] = {"status": 200}
        except FloppyAuthError as exc:
            reason = str(getattr(exc, "reason", "") or "request_failed")
            status = "auth_failed" if reason == "invalid_api_token" else reason
            api["lists"] = {"status": getattr(exc, "status_code", None) or status}
        except Exception:
            status = "service_unavailable"
            reason = "service_unavailable"
            api["lists"] = {"status": status}
        return {"ok": ok, "status": status, "latency_ms": int((time.perf_counter() - start) * 1000), "features": {k: bool(ok and v) for k, v in _FEATURES.items()}, "details": {"reason": reason} if reason else None, "api": api}

    def build_index(self, feature: str, **kwargs: Any) -> Mapping[str, dict[str, Any]]:
        mod = _FEATURE_MODULES.get(str(feature or "").strip().lower())
        return mod.build_index(self, **kwargs) if mod else {}

    def prepare_source_snapshot(self, feature: str, items: Any) -> int:
        mod = _FEATURE_MODULES.get(str(feature or "").strip().lower())
        hook = getattr(mod, "prepare_source_snapshot", None)
        if not callable(hook):
            return 0
        seq = list(items.values()) if isinstance(items, Mapping) else list(items or [])
        if not seq:
            return 0
        try:
            produced = hook(seq)
        except Exception:
            return 0
        return produced if isinstance(produced, int) else 0

    def add(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        mod = _FEATURE_MODULES.get(str(feature or "").strip().lower())
        return mod.add(self, items, dry_run=dry_run) if mod else build_op_result(ok=True, count=0, unsupported=True)

    def remove(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        mod = _FEATURE_MODULES.get(str(feature or "").strip().lower())
        return mod.remove(self, items, dry_run=dry_run) if mod else build_op_result(ok=True, count=0, unsupported=True)

    def cleanup_after_features(self, features: Iterable[str]) -> Mapping[str, Any]:
        selected = {str(f or "").strip().lower() for f in features}
        if not {"watchlist", "ratings", "history"}.issubset(selected):
            return {}
        removed = 0
        errors: list[str] = []
        seen: set[tuple[str, str]] = set()
        for media_type in ("movie", "tv"):
            for row in paged(self, f"media/{media_type}"):
                typ, source, media_id, _, _ = media_parts_from_item_id(row.get("item_id"))
                source = str(row.get("source") or source or "").strip()
                media_id = str(row.get("media_id") or media_id or "").strip()
                if source != "tmdb" or not media_id:
                    continue
                key = (media_type, media_id)
                if key in seen:
                    continue
                seen.add(key)
                try:
                    api_delete(self, f"media/{media_type}/tmdb/{media_id}")
                    removed += 1
                except Exception as exc:
                    errors.append(str(exc))
        return {"ok": not errors, "removed": removed, "count": removed, "remaining": 0, "errors": errors}


class _FLOPPYOPS:
    def name(self) -> str:
        return "FLOPPY"

    def label(self) -> str:
        return "Floppy"

    def features(self) -> Mapping[str, bool]:
        return dict(_FEATURES)

    def state_read_features(self) -> Mapping[str, bool]:
        return dict(_FEATURES)

    def capabilities(self) -> Mapping[str, Any]:
        return get_manifest()["capabilities"]

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        return is_configured(cfg, "default")

    def _adapter(self, cfg: Mapping[str, Any]) -> FLOPPYModule:
        return FLOPPYModule(cfg)

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)

    def prepare_source_snapshot(self, cfg: Mapping[str, Any], *, feature: str, items: Any) -> int:
        return self._adapter(cfg).prepare_source_snapshot(feature, items)

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)


OPS = _FLOPPYOPS()
