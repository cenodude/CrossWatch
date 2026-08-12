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
from cw_platform.provider_instances import get_provider_block, normalize_instance_id
from providers.auth._auth_SCROB import is_configured as auth_configured

try:  # type: ignore[name-defined]
    ctx  # type: ignore[misc]
except Exception:
    ctx = None  # type: ignore[assignment]

__VERSION__ = "0.1"
os.environ.setdefault("CW_SCROB_VERSION", __VERSION__)
os.environ.setdefault("CW_SCROB_UA", f"CrossWatch/{__VERSION__} (Scrob)")
__all__ = ["get_manifest", "SCROBModule", "OPS"]

FEATURES = {"watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": False}

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
        "description": "Self hosted Scrob tracker (watchlist, ratings, history, progress).",
    }


def _feature_label(method: str, url: str, kw: Mapping[str, Any]) -> str:
    tail = str(url or "").rstrip("/").rsplit("/", 2)[-2:]
    return "api:" + "/".join(part for part in tail if part) if tail else "api"


class SCROBModule:
    def __init__(self, cfg: Mapping[str, Any], *, instance_id: Any = None):
        self.instance_id = normalize_instance_id(instance_id)
        block = get_provider_block(cfg or {}, "scrob", self.instance_id) or {}
        if not auth_configured(block):
            _log("error", "missing config", instance=self.instance_id)
            raise RuntimeError("Scrob is not connected: server_url, api_key, username and password are required")

        self.config: dict[str, Any] = {**dict(cfg or {}), "scrob": dict(block)}
        self.raw_cfg = self.config
        self.session: requests.Session = build_session("SCROB", ctx, feature_label=_feature_label)
        try:
            self.session.headers.setdefault("User-Agent", os.environ.get("CW_SCROB_UA") or f"CrossWatch/{__VERSION__} (Scrob)")
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
        block = get_provider_block(cfg or {}, "scrob", None) or {}
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


OPS = _SCROBOPS()
