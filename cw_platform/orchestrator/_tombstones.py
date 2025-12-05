# cw_platform/orchestrator/_tombstones.py
# tombstone (deleted item) management for orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Callable, TypeVar, AbstractSet

from ..id_map import canonical_key, ID_KEYS
from ._state_store import StateStore

TItem = TypeVar("TItem", bound=Mapping[str, Any])

def pair_key(a: str, b: str) -> str:
    return "-".join(sorted([a.upper(), b.upper()]))

def add_global_keys(
    store: StateStore,
    dbg: Callable[..., Any],
    keys: Iterable[str],
) -> int:
    tomb = store.load_tomb()
    raw = tomb.setdefault("keys", {})
    if not isinstance(raw, dict):
        raw = {}
        tomb["keys"] = raw

    ks: dict[str, Any] = raw
    now = int(time.time())
    added = 0

    for k in keys:
        if k not in ks:
            ks[k] = now
            added += 1

    store.save_tomb(tomb)
    dbg("tombstones.marked", added=added)
    return added


def add_keys_for_feature(
    store: StateStore,
    dbg: Callable[..., Any],
    feature: str,
    keys: Iterable[str],
    *,
    pair: str | None = None,
) -> int:
    tomb = store.load_tomb()
    raw = tomb.setdefault("keys", {})
    if not isinstance(raw, dict):
        raw = {}
        tomb["keys"] = raw

    ks: dict[str, Any] = raw
    now = int(time.time())
    added = 0

    prefixes: list[str] = [feature]
    if pair:
        prefixes.append(f"{feature}:{pair}")

    for k in keys:
        for pref in prefixes:
            nk = f"{pref}|{k}"
            if nk not in ks:
                ks[nk] = now
                added += 1

    store.save_tomb(tomb)
    dbg(
        "tombstones.marked",
        feature=feature,
        added=added,
        scope="global+pair" if pair else "global",
    )
    return added


def keys_for_feature(
    store: StateStore,
    feature: str,
    *,
    pair: str | None = None,
    include_global: bool = True,
) -> dict[str, int]:
    tomb = store.load_tomb()
    raw = tomb.get("keys") or {}
    if isinstance(raw, Mapping):
        ks_all: dict[str, int] = {
            str(k): int(v) for k, v in raw.items()
        }
    else:
        ks_all = {}

    out: dict[str, int] = {}

    def _collect(prefix: str) -> None:
        plen = len(prefix) + 1
        for k, ts in ks_all.items():
            if k.startswith(prefix + "|"):
                orig = k[plen:]
                out[orig] = int(ts)

    if include_global:
        _collect(feature)
    if pair:
        _collect(f"{feature}:{pair}")

    return out


def prune(
    store: StateStore,
    dbg: Callable[..., Any],
    *,
    older_than_secs: int,
) -> int:
    tomb = store.load_tomb()
    raw = tomb.get("keys") or {}
    if not isinstance(raw, Mapping):
        return 0

    ks: dict[str, int] = {str(k): int(v) for k, v in raw.items()}
    if not ks:
        return 0

    now = int(time.time())
    keep: dict[str, int] = {
        k: int(v) for k, v in ks.items()
        if (now - int(v)) < older_than_secs
    }

    removed = len(ks) - len(keep)
    tomb["keys"] = keep
    tomb["pruned_at"] = now
    store.save_tomb(tomb)
    dbg("tombstones.pruned", removed=removed, kept=len(keep))
    return removed


def filter_with(
    store: StateStore,
    items: Sequence[TItem],
    extra_block: AbstractSet[str] | None = None,
) -> list[TItem]:
    tomb = store.load_tomb()
    raw = tomb.get("keys") or {}

    if isinstance(raw, Mapping):
        raw_keys = raw.keys()
    else:
        raw_keys = ()

    base_keys: set[str] = set()
    for tok in raw_keys:
        if isinstance(tok, str):
            base_keys.add(tok.split("|", 1)[-1])

    if extra_block:
        base_keys |= set(extra_block)

    def _hit(keys: set[str], item: Mapping[str, Any]) -> bool:
        ck = canonical_key(item)
        if ck in keys:
            return True

        ids = item.get("ids") or {}
        if isinstance(ids, Mapping):
            for k in ID_KEYS:
                v = ids.get(k)
                if v is not None and f"{k}:{str(v).lower()}" in keys:
                    return True

        t = str(item.get("type") or "").lower()
        ttl = str(item.get("title") or "").strip().lower()
        yr = item.get("year") or ""
        token = f"{t}|title:{ttl}|year:{yr}"
        return token in keys

    return [it for it in items if not _hit(base_keys, it)]

def cascade_removals(
    store: StateStore,
    dbg: Callable[..., Any],
    *,
    feature: str,
    removed_keys: Iterable[str],
) -> dict[str, int]:
    added = add_keys_for_feature(store, dbg, feature, removed_keys)
    return {"tombstones_added": added}
