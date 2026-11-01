# services/pair_state.py
# CrossWatch - Sync pair state cleanup
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any
import logging

from cw_platform.local_db import state as sqlite_state
from cw_platform.orchestrator._pairs import _feature_list_for_pair
from cw_platform.orchestrator._state_store import StateStore
from cw_platform.pair_scope import pair_feature_scope
from cw_platform.provider_instances import normalize_instance_id

FEATURES = ("watchlist", "history", "ratings", "progress", "collection", "playlists")


def configured_scopes(cfg: Mapping[str, Any]) -> dict[str, tuple[str, str]]:
    from cw_platform.orchestrator._pairs import expand_sync_user_pairs
    from cw_platform.value_coercion import coerce_bool

    out: dict[str, tuple[str, str]] = {}
    expanded = expand_sync_user_pairs(cfg)
    for pairs in (expanded, [pair for pair in expanded if coerce_bool(pair.get("enabled", True), True)]):
        for index, pair in enumerate(pairs, 1):
            for feature in FEATURES:
                scope = pair_feature_scope(cfg, pair, feature, index)
                out[scope] = (str(pair.get("id") or pair.get("pair_id") or index), feature)
    return out


def stale_pair_scopes(config_dir: str | Path, cfg: Mapping[str, Any], *, protected: Iterable[str] = ()) -> dict[str, Any]:
    from cw_platform.local_db.db import get_conn

    active = configured_scopes(cfg)
    protected = set(protected)
    from cw_platform.orchestrator._pairs import expand_sync_user_pairs
    from cw_platform.identity_links import clean_identity_map
    from cw_platform.value_coercion import coerce_bool

    deferred_pairs = set()
    deferred_positional = False
    for index, pair in enumerate(cfg.get("pairs") or [], 1):
        if not isinstance(pair, Mapping) or not coerce_bool(pair.get("match_users", False)):
            continue
        expanded = expand_sync_user_pairs({**cfg, "pairs": [pair]})
        links = clean_identity_map(pair.get("user_matches"))
        if not expanded or (expanded != [pair] and len(expanded) < len(links)):
            deferred_pairs.add(str(pair.get("id") or pair.get("pair_id") or index))
            deferred_positional = deferred_positional or not (pair.get("id") or pair.get("pair_id"))
    conn = get_conn(config_dir)
    if conn is None:
        raise RuntimeError("database_unavailable")
    rows = conn.execute("SELECT DISTINCT pair_scope FROM pair_feature_state").fetchall()
    owners = {row[0]: row[1] for row in conn.execute("SELECT pair_scope,pair_id FROM pair_scope_owners")}
    owners.update({scope: owner[0] for scope, owner in active.items()})
    deferred_scopes = [row["pair_scope"] for row in rows
                       if deferred_pairs and (row["pair_scope"] not in owners or owners[row["pair_scope"]] in deferred_pairs
                                              or (deferred_positional and str(owners[row["pair_scope"]]).isdigit()))]
    protected.update(deferred_scopes)
    stale = [str(row["pair_scope"]) for row in rows if str(row["pair_scope"]).startswith("cw2_") and row["pair_scope"] not in active and row["pair_scope"] not in protected]
    items = sum(sqlite_state.pair_state_counts(config_dir, scope)[1] for scope in stale)
    return {"scopes": stale, "items": items,
            "active_scopes": sum(row["pair_scope"] in active for row in rows), "stored_scopes": len(rows),
            "deferred": bool(deferred_pairs), "deferred_pairs": sorted(deferred_pairs), "deferred_scopes": len(deferred_scopes)}


def prune_pair_scopes(config_dir: str | Path, cfg: Mapping[str, Any], *, protected: Iterable[str] = ()) -> dict[str, Any]:
    from cw_platform.local_db.db import get_conn

    inventory = stale_pair_scopes(config_dir, cfg, protected=protected)
    result = clear_pair_state(config_dir, Path(config_dir) / ".cw_state", inventory["scopes"])
    if result["errors"]:
        raise RuntimeError("pair_scope_cleanup_failed")
    conn = get_conn(config_dir)
    if conn is None:
        raise RuntimeError("database_unavailable")
    with conn:
        conn.executemany("INSERT INTO pair_scope_owners(pair_scope,pair_id,feature) VALUES(?,?,?) "
                         "ON CONFLICT(pair_scope) DO UPDATE SET pair_id=excluded.pair_id,feature=excluded.feature",
                         [(scope, *owner) for scope, owner in configured_scopes(cfg).items()])
        conn.execute("DELETE FROM pair_scope_owners WHERE pair_scope NOT IN (SELECT pair_scope FROM pair_feature_state)")
    if result["items"]:
        logging.getLogger("crosswatch.pair_state").info("Pruned %s obsolete pair scopes and %s items", len(inventory["scopes"]), result["items"])
    return {**result, "scopes": len(inventory["scopes"])}


def owned_pair_scopes(config_dir: str | Path, pair_id: str) -> list[str]:
    from cw_platform.local_db.db import get_conn

    conn = get_conn(config_dir)
    if conn is None:
        raise RuntimeError("database_unavailable")
    return [str(row[0]) for row in conn.execute("SELECT pair_scope FROM pair_scope_owners WHERE pair_id=?", (pair_id,))]


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
