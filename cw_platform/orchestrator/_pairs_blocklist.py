# cw_platform/orchestration/_pairs_blocklist.py
# Pairs blocklist handling for the orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from collections.abc import Iterable, Mapping
from typing import Any
from ..id_map import canonical_key, ID_KEYS
from ._tombstones import keys_for_feature, filter_with

try:
    from ._unresolved import load_unresolved_keys  # type: ignore
except Exception:  # pragma: no cover
    def load_unresolved_keys(
        dst: str,
        feature: str | None = None,
        *,
        cross_features: bool = True,
    ) -> set[str]:
        return set()

try:
    from ._blackbox import load_blackbox_keys  # type: ignore
except Exception:  # pragma: no cover
    def load_blackbox_keys(
        dst: str,
        feature: str,
        pair: str | None = None,
    ) -> set[str]:
        return set()

def _breakdown(
    state_store,
    dst: str,
    feature: str,
    *,
    pair_key: str | None,
    cross_feature_unresolved: bool,
) -> tuple[set[str], set[str], set[str], set[str]]:
    global_tomb: set[str] = set()

    try:
        pmap = keys_for_feature(state_store, feature, pair=pair_key) or {}
        pair_tomb: set[str] = set(pmap.keys()) if isinstance(pmap, Mapping) else set()
    except Exception:
        pair_tomb = set()

    try:
        unresolved = set(load_unresolved_keys(dst, feature, cross_features=cross_feature_unresolved) or [])
    except Exception:
        unresolved = set()

    try:
        blackbox = set(load_blackbox_keys(dst, feature, pair=pair_key) or [])
    except Exception:
        blackbox = set()

    return global_tomb, pair_tomb, unresolved, blackbox

def blocked_keys_for_destination(
    state_store,
    dst: str,
    feature: str,
    *,
    pair_key: str | None = None,
    cross_feature_unresolved: bool = True,
) -> set[str]:
    g_tomb, p_tomb, unr, bb = _breakdown(
        state_store, dst, feature,
        pair_key=pair_key,
        cross_feature_unresolved=cross_feature_unresolved,
    )
    return g_tomb | p_tomb | unr | bb

def _ts_epoch(v: Any) -> int | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    if s.isdigit():
        try:
            n = int(s)
            return n // 1000 if len(s) >= 13 else n
        except Exception:
            return None
    try:
        from datetime import datetime, timezone
        return int(
            datetime.fromisoformat(s.replace("Z", "+00:00"))
            .astimezone(timezone.utc)
            .timestamp()
        )
    except Exception:
        return None

def _history_is_blocked_by_tomb(item: dict[str, Any], tomb_ts: Mapping[str, int]) -> bool:
    # Allow re-add if the item has a newer watched_at than the tombstone timestamp.
    watched_ts = _ts_epoch(item.get("watched_at"))
    tokens: list[str] = []
    try:
        ck = canonical_key(item)
        if ck:
            tokens.append(ck)
    except Exception:
        pass

    ids = item.get("ids") or {}
    if isinstance(ids, Mapping):
        for k in ID_KEYS:
            v = ids.get(k)
            if v is None or str(v).strip() == "":
                continue
            tokens.append(f"{str(k).lower()}:{str(v).lower()}")

    t = str(item.get("type") or "").lower()
    ttl = str(item.get("title") or "").strip().lower()
    yr = item.get("year") or ""
    tokens.append(f"{t}|title:{ttl}|year:{yr}")

    hit_ts: int | None = None
    for tok in tokens:
        ts = tomb_ts.get(tok)
        if ts is None:
            continue
        ts_i = int(ts)
        hit_ts = ts_i if hit_ts is None else max(hit_ts, ts_i)

    if hit_ts is None:
        return False

    if watched_ts is not None and int(watched_ts) > int(hit_ts):
        return False

    return True

def apply_blocklist(
    state_store,
    items: Iterable[dict[str, Any]],
    *,
    dst: str,
    feature: str,
    pair_key: str | None = None,
    cross_feature_unresolved: bool = True,
    emit=None,
) -> list[dict[str, Any]]:
    global_tomb: set[str] = set()

    try:
        pmap_raw = keys_for_feature(state_store, feature, pair=pair_key) or {}
        pmap: dict[str, int] = {str(k): int(v) for k, v in (pmap_raw or {}).items()} if isinstance(pmap_raw, Mapping) else {}
        pair_tomb: set[str] = set(pmap.keys())
    except Exception:
        pmap = {}
        pair_tomb = set()

    try:
        unresolved = set(load_unresolved_keys(dst, feature, cross_features=cross_feature_unresolved) or [])
    except Exception:
        unresolved = set()

    try:
        blackbox = set(load_blackbox_keys(dst, feature, pair=pair_key) or [])
    except Exception:
        blackbox = set()

    bl = global_tomb | pair_tomb | unresolved | blackbox

    if emit is not None:
        try:
            emit(
                "debug",
                msg="blocked.counts",
                feature=feature,
                dst=dst,
                pair=pair_key,
                blocked_global_tomb=0,
                blocked_pair_tomb=len(pair_tomb),
                blocked_unresolved=len(unresolved),
                blocked_blackbox=len(blackbox),
                blocked_total=len(bl),
            )
        except Exception:
            pass

    items_list = list(items or [])
    if not items_list or not bl:
        return items_list

    if str(feature or "").lower() != "history":
        return filter_with(state_store, items_list, extra_block=bl)

    hard = (global_tomb | unresolved | blackbox)
    if hard:
        items_list = filter_with(state_store, items_list, extra_block=hard)

    if not pair_tomb:
        return items_list

    out: list[dict[str, Any]] = []
    for it in items_list:
        try:
            if bool(it.get("_cw_marked")):
                if _history_is_blocked_by_tomb(it, pmap):
                    continue
            else:
                if filter_with(state_store, [it], extra_block=pair_tomb) == []:
                    continue
        except Exception:
            try:
                if filter_with(state_store, [it], extra_block=pair_tomb) == []:
                    continue
            except Exception:
                pass
        out.append(it)
    return out
