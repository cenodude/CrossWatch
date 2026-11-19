# cw_platform/pair_scope.py
# CrossWatch - Sync Pair State Identity
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .provider_instances import build_provider_config_view, normalize_instance_id

_RUNTIME_KEYS = {"expires", "expires_in", "stored_at", "session_refreshed_at"}
_PAIR_SCOPE_TABLES = (
    "pair_feature_state", "pair_scope_owners", "episode_group_state",
    "episode_group_scrobbles", "pair_analyzer_skips", "pair_continue_rows",
)


def _private_key(key: str) -> bool:
    k = key.lower()
    return k in {"pin", "credential"} or k.endswith("_pin")


def _runtime_key(key: str) -> bool:
    k = key.lower()
    return k.endswith("expires_at") or k in _RUNTIME_KEYS


def pair_feature_scope(config: Mapping[str, Any], pair: Mapping[str, Any], feature: str, index: int = 0,
                       *, legacy: bool = False) -> str:
    def settings(value: Any, endpoint: bool = False) -> Any:
        if isinstance(value, Mapping):
            return {str(k): settings(v, endpoint) for k, v in value.items()
                    if not str(k).startswith("_cw_") and not any(word in str(k).lower() for word in ("token", "password", "secret", "api_key"))
                    and str(k) not in {"auth", "oauth", "label", "sync_users"}
                    and (legacy or not endpoint or not _runtime_key(str(k)))
                    and (legacy or not _private_key(str(k)))}
        if isinstance(value, (list, tuple)):
            return [settings(v, endpoint) for v in value]
        return value

    endpoints = []
    for side in ("source", "target"):
        provider = str(pair.get(side) or "").upper().strip()
        instance = normalize_instance_id(pair.get(f"{side}_instance"))
        view = build_provider_config_view(dict(config), provider, instance)
        block = dict(view.get(provider.lower()) or {})
        for other in ("watchlist", "history", "ratings", "progress", "collection", "playlists"):
            if other != feature:
                block.pop(other, None)
        endpoints.append([provider, instance, settings(block, True)])
    payload = {
        "pair": str(pair.get("id") or pair.get("pair_id") or index),
        "profile": pair.get("profile_id"),
        "mode": pair.get("mode", "one-way"),
        "endpoints": endpoints,
        "feature": feature,
        "rules": settings((pair.get("features") or {}).get(feature)),
        "overrides": settings(pair.get("providers") or {}),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return "cw2_" + hashlib.sha256(encoded).hexdigest()


def _tracker_pair_scoped(pair: Mapping[str, Any]) -> bool:
    import os

    flag = str(os.getenv("CW_CROSSWATCH_PAIR_SCOPED", "")).strip().lower() in ("1", "true", "yes", "y", "on")
    return flag and "CROSSWATCH" in {str(pair.get(side) or "").strip().upper() for side in ("source", "target")}


def _move_scope_files(root: Path, old: str, new: str) -> bool:
    try:
        paths = [p for p in root.iterdir() if p.is_file() and old in p.name] if root.is_dir() else []
    except OSError:
        return False
    done = True
    for path in paths:
        target = path.with_name(path.name.replace(old, new))
        try:
            if target.exists():
                path.unlink()
            else:
                path.replace(target)
        except OSError:
            done = False
    return done


def _move_scope_tombstones(base_path: Path, old: str, new: str) -> bool:
    from .orchestrator._state_store import StateStore

    store = StateStore(base_path)
    if not store.tomb.exists():
        return True
    try:
        text = store.tomb.read_text("utf-8")
    except OSError:
        return False
    if old not in text and old.upper() not in text:
        return True
    tomb = store.load_tomb()
    keys = tomb.get("keys")
    if not isinstance(keys, dict):
        return True
    marks = ((f":{old.upper()}|", f":{new.upper()}|"), (f":{old}|", f":{new}|"))
    moved: dict[str, Any] = {}
    renamed: dict[str, Any] = {}
    for key, value in keys.items():
        text_key = str(key)
        for before, after in marks:
            text_key = text_key.replace(before, after)
        (renamed if text_key == str(key) else moved)[text_key] = value
    for key, value in moved.items():
        renamed.setdefault(key, value)
    if renamed != keys:
        tomb["keys"] = renamed
        try:
            store.save_tomb(tomb)
        except OSError:
            return False
    return True


def scope_move_pending(base_path: str | Path, scope: str) -> bool:
    from .local_db import state as sqlite_state

    return any(new == scope for _, new in sqlite_state.pending_scope_moves(base_path))


def adopt_legacy_scope(base_path: str | Path, config: Mapping[str, Any], pair: Mapping[str, Any], feature: str,
                       index: int = 0) -> bool:
    from .local_db import state as sqlite_state

    moved = False
    if not _tracker_pair_scoped(pair):
        new = pair_feature_scope(config, pair, feature, index)
        old = pair_feature_scope(config, pair, feature, index, legacy=True)
        moved = old != new and sqlite_state.rename_pair_scope(base_path, old, new, _PAIR_SCOPE_TABLES)
    for before, after in sqlite_state.pending_scope_moves(base_path):
        files = _move_scope_files(Path(base_path) / ".cw_state", before, after)
        if _move_scope_tombstones(Path(base_path), before, after) and files:
            sqlite_state.finish_scope_move(base_path, before)
    return moved
