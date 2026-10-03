# services/pair_state.py
# CrossWatch - Sync pair state cleanup
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

from cw_platform.local_db import state as sqlite_state
from cw_platform.orchestrator._pairs import _feature_list_for_pair
from cw_platform.orchestrator._state_store import StateStore
from cw_platform.pair_scope import pair_feature_scope
from cw_platform.provider_instances import normalize_instance_id

FEATURES = ("watchlist", "history", "ratings", "progress", "collection", "playlists")


def pair_scopes(cfg: Mapping[str, Any], pair_id: str, features: Iterable[str] | None = None) -> dict[str, str] | None:
    wanted = [f for f in FEATURES if features is None or f in {str(x or "").strip().lower() for x in features}]
    pairs = cfg.get("pairs")
    for index, pair in enumerate(pairs if isinstance(pairs, list) else [], 1):
        if isinstance(pair, Mapping) and str(pair.get("id") or "") == str(pair_id):
            return {feature: pair_feature_scope(cfg, pair, feature, index) for feature in wanted}
    return None


def _endpoints(pair: Mapping[str, Any]) -> list[tuple[str, str]]:
    out = []
    for side in ("source", "target"):
        provider = str(pair.get(side) or "").strip().upper()
        if provider:
            out.append((provider, normalize_instance_id(pair.get(f"{side}_instance"))))
    return out


def uncovered_inventory(config_dir: str | Path, cfg: Mapping[str, Any], pair: Mapping[str, Any]) -> list[tuple[str, str, str]]:
    covered: set[tuple[str, str, str]] = set()
    pairs = cfg.get("pairs")
    for other in pairs if isinstance(pairs, list) else []:
        if not isinstance(other, Mapping):
            continue
        for feature in _feature_list_for_pair(other):
            for provider, instance in _endpoints(other):
                covered.add((provider, instance, str(feature).strip().lower()))
    endpoints = set(_endpoints(pair))
    return [
        (row["provider"], row["instance"], row["feature"])
        for row in sqlite_state.feature_inventory(config_dir)
        if (row["provider"], normalize_instance_id(row["instance"])) in endpoints
        and (row["provider"], normalize_instance_id(row["instance"]), row["feature"]) not in covered
    ]


def clear_uncovered_inventory(config_dir: str | Path, cfg: Mapping[str, Any], pair: Mapping[str, Any]) -> dict[str, Any]:
    keys = uncovered_inventory(config_dir, cfg, pair)
    baselines, items = sqlite_state.remove_feature_states(config_dir, keys)
    return {"baselines": baselines, "items": items, "features": [list(key) for key in keys]}


def _clear_tombstones(config_dir: Path, scopes: set[str]) -> int:
    store = StateStore(config_dir)
    if not store.tomb.exists():
        return 0
    tomb = store.load_tomb()
    keys = tomb.get("keys")
    if not isinstance(keys, dict):
        return 0
    upper = {scope.upper() for scope in scopes}
    kept = {k: v for k, v in keys.items() if str(k).split("|", 1)[0].partition(":")[2].upper() not in upper}
    removed = len(keys) - len(kept)
    if removed:
        tomb["keys"] = kept
        store.save_tomb(tomb)
    return removed


def clear_pair_state(
    config_dir: str | Path,
    state_dir: str | Path,
    scopes: Iterable[str],
    *,
    keep_file: Callable[[str], bool] | None = None,
) -> dict[str, Any]:
    wanted = {str(scope).strip() for scope in scopes if str(scope or "").strip()}
    out: dict[str, Any] = {"baselines": 0, "items": 0, "tombstones": 0, "files": [], "bytes": 0, "errors": []}
    if not wanted:
        return out

    for scope in sorted(wanted):
        try:
            baselines, items = sqlite_state.clear_pair_state(config_dir, scope)
            out["baselines"] += baselines
            out["items"] += items
        except Exception as exc:
            out["errors"].append(f"database: {exc}")

    try:
        out["tombstones"] = _clear_tombstones(Path(config_dir), wanted)
    except Exception as exc:
        out["errors"].append(f"tombstones: {exc}")

    root = Path(state_dir)
    needles = [scope.lower() for scope in wanted]
    try:
        paths = sorted(p for p in root.iterdir() if p.is_file()) if root.is_dir() else []
    except Exception as exc:
        paths = []
        out["errors"].append(f"scan: {exc}")
    for path in paths:
        name = path.name
        if not any(needle in name.lower() for needle in needles):
            continue
        if keep_file is not None and keep_file(name):
            continue
        try:
            size = path.stat().st_size
            path.unlink(missing_ok=True)
            out["files"].append(name)
            out["bytes"] += int(size or 0)
        except Exception as exc:
            out["errors"].append(f"{name}: {exc}")
    return out
