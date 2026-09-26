# providers/sync/_mod_WETRAKR.py
# CrossWatch - WeTrakr tracking, ratings and resume progress sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.provider_instances import list_instance_ids, normalize_instance_id
from cw_platform.run_control import raise_if_cancelled
from providers.auth import _auth_WETRAKR as auth
from providers.sync._mod_common import build_op_result, build_session, make_snapshot_progress
from providers.sync._log import log
from providers.sync.wetrakr import _history, _progress, _ratings, _watchlist
from providers.sync.wetrakr._common import WRITE_BATCH_SIZE, WeTrakrSyncError, body_of, commit_snapshot, request, write_lock

__VERSION__ = "0.1"
__all__ = ["get_manifest", "WETRAKRModule", "OPS"]
_FEATURES = {"watchlist": True, "history": True, "ratings": True, "progress": True, "playlists": False, "collection": False}
_FEATURE_MODULES = {"watchlist": _watchlist, "history": _history, "ratings": _ratings, "progress": _progress}


class _NullCtx:
    def emit(self, *args: Any, **kwargs: Any) -> None:
        pass


if "ctx" not in globals():
    ctx = _NullCtx()


def _current_instance_id(cfg: Mapping[str, Any]) -> str:
    if cfg.get("_cw_provider_instance") is not None:
        return normalize_instance_id(cfg["_cw_provider_instance"])
    hint = cfg.get("_cw_probe")
    if isinstance(hint, Mapping) and hint.get("instance"):
        return normalize_instance_id(hint["instance"])
    for prefix in ("CW_PROBE", "CW_CAPTURE", "CW_PAIR_SRC", "CW_PAIR_DST"):
        name = os.getenv(prefix + "_PROVIDER") if prefix in ("CW_PROBE", "CW_CAPTURE") else os.getenv(prefix)
        if str(name or "").upper() == "WETRAKR":
            return normalize_instance_id(os.getenv(prefix + "_INSTANCE"))
    block = cfg.get("wetrakr")
    if isinstance(block, Mapping) and "instances" not in block and block.get("access_token"):
        full = auth._load_full_cfg()
        matches = [inst for inst in list_instance_ids(full, "wetrakr")
                   if auth.provider_block(full, inst).get("access_token") == block["access_token"]]
        if len(matches) == 1:
            return normalize_instance_id(matches[0])
        if len(matches) > 1:
            raise WeTrakrSyncError("ambiguous_provider_instance")
        raise WeTrakrSyncError("provider_credentials_changed")
    return "default"


def get_manifest() -> Mapping[str, Any]:
    shared = {"read": True, "write": True, "upsert": True, "remove": True, "observed_deletes": True,
              "accepted_ids": ["tmdb", "imdb", "tvdb", "wetrakr"], "provides_ids": ["tmdb", "imdb", "tvdb", "wetrakr"],
              "batch_size": WRITE_BATCH_SIZE, "verify_after_write": True}
    return {"name": "WETRAKR", "label": "WeTrakr", "version": __VERSION__, "type": "sync",
            "bidirectional": True, "experimental": True, "features": dict(_FEATURES), "requires": ["requests"],
            "capabilities": {"bidirectional": True, "experimental": True, "multi_profile": True,
                "provides_ids": True, "index_semantics": "present", "features": dict(_FEATURES),
                "watchlist": {**shared, "types": {"movies": True, "shows": True, "seasons": False, "episodes": False}, "custom_lists": False},
                "history": {**shared, "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
                    "event_history": True, "rewatches": {"read": True, "write": True, "account_gate": False}},
                "ratings": {**shared, "types": {"movies": True, "shows": True, "seasons": True, "episodes": True}},
                "progress": {**shared, "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
                    "accepted_ids": ["tmdb", "imdb", "tvdb"], "requires_duration": False, "batch_size": 1,
                    "completion_policy": {"progress_write": {"mode": "none"}}},
                **{feature: {"read": False, "write": False} for feature in ("playlists", "collection")}}}


class WETRAKRModule:
    def __init__(self, cfg: Mapping[str, Any], instance_id: str | None = None):
        self.config = cfg or {}
        self.instance_id = normalize_instance_id(instance_id) if instance_id is not None else _current_instance_id(self.config)
        self.session = build_session("WETRAKR", ctx)
        self.progress_factory = lambda feature: make_snapshot_progress(ctx, dst="WETRAKR", feature=feature)

    @staticmethod
    def supported_features() -> dict[str, bool]:
        return dict(_FEATURES)

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def health(self) -> Mapping[str, Any]:
        start = time.perf_counter()
        ok, status = False, "not_configured"
        if auth.is_configured(auth.provider_block(self.config, self.instance_id)):
            try:
                account = body_of(request(self, "GET", "/account/settings"))
                ok = bool(isinstance(account, Mapping) and account.get("id"))
                status = "ok" if ok else "invalid_account"
            except WeTrakrSyncError as exc:
                status = exc.reason
        log("WETRAKR", "health", "info", "health", ok=ok, status=status)
        return {"ok": ok, "status": status, "latency_ms": int((time.perf_counter() - start) * 1000),
                "features": {key: bool(ok and value) for key, value in _FEATURES.items()}}

    def build_index(self, feature: str, **kwargs: Any) -> dict[str, dict[str, Any]]:
        module = _FEATURE_MODULES.get(feature)
        if module is None:
            raise WeTrakrSyncError("unsupported_feature")
        with write_lock(self):
            self._pending_snapshot = None
            self._read_progress = self.progress_factory(feature)
            self._read_progress.tick(0, force=True)
            complete = False
            total = None
            try:
                raise_if_cancelled()
                capture = str(os.getenv("CW_CAPTURE_MODE") or "").lower() in ("1", "true", "yes", "on")
                capture = capture and str(os.getenv("CW_CAPTURE_PROVIDER") or "").upper() == "WETRAKR"
                result = module.build_index(self, force=bool(kwargs.get("force_refresh")) or capture)
                raise_if_cancelled()
                commit_snapshot(self)
                total = len(result)
                self._read_progress.tick(total, total=total, force=True)
                complete = True
                return result
            finally:
                self._read_progress.done(ok=complete, total=total)
                self._read_progress = None
                self._pending_snapshot = None

    def add(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        module = _FEATURE_MODULES.get(feature)
        return module.add(self, items, dry_run=dry_run) if module else build_op_result(ok=False, unsupported=True)

    def remove(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        module = _FEATURE_MODULES.get(feature)
        return module.remove(self, items, dry_run=dry_run) if module else build_op_result(ok=False, unsupported=True)


class _WETRAKROPS:
    def name(self) -> str:
        return "WETRAKR"

    def label(self) -> str:
        return "WeTrakr"

    def features(self) -> Mapping[str, bool]:
        return dict(_FEATURES)

    def state_read_features(self) -> Mapping[str, bool]:
        return dict(_FEATURES)

    def capabilities(self) -> Mapping[str, Any]:
        return get_manifest()["capabilities"]

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        return auth.is_configured(auth.provider_block(cfg, _current_instance_id(cfg)))

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return WETRAKRModule(cfg).health()

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        return WETRAKRModule(cfg).build_index(feature)

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return WETRAKRModule(cfg).add(feature, items, dry_run=dry_run)

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return WETRAKRModule(cfg).remove(feature, items, dry_run=dry_run)


OPS = _WETRAKROPS()
