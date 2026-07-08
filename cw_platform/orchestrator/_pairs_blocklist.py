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
    g_tomb, p_tomb, _unr, bb = _breakdown(
        state_store, dst, feature,
        pair_key=pair_key,
        cross_feature_unresolved=cross_feature_unresolved,
    )
    return g_tomb | p_tomb | bb

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

    if watched_ts is not None and int(watched_ts) >= int(hit_ts):
        return False

    return True


def _ratings_is_blocked_by_tomb(item: dict[str, Any], tomb_ts: Mapping[str, int]) -> bool:
    # Allow re-add if the item has a newer rated_at than the tombstone timestamp.
    rated_ts = _ts_epoch(item.get("rated_at"))
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

    if rated_ts is not None and int(rated_ts) >= int(hit_ts):
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
    ignore_pair_tomb: bool = False,
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

    pair_tomb_eff: set[str] = set() if ignore_pair_tomb else set(pair_tomb)
    bl = global_tomb | pair_tomb_eff | blackbox

    items_in = list(items or [])
    input_count = len(items_in)

    def _emit_counts(result_list: list[dict[str, Any]]) -> None:
        if emit is None:
            return
        try:
            kept_bb = filter_with(state_store, items_in, extra_block=blackbox) if blackbox else items_in
            blocked_items: list[dict[str, Any]] = []
            if blackbox:
                kept_ids = {id(x) for x in kept_bb}
                for it in items_in:
                    if id(it) in kept_ids:
                        continue
                    try:
                        ck = canonical_key(it)
                    except Exception:
                        ck = ""
                    if ck:
                        blocked_items.append({"key": ck, "item": it})
            emit(
                "debug",
                msg="blocked.counts",
                feature=feature,
                dst=dst,
                pair=pair_key,
                blocked_global_tomb=0,
                blocked_pair_tomb=len(pair_tomb_eff),
                blocked_unresolved=len(unresolved),
                blocked_blackbox=input_count - len(kept_bb),
                blocked_total=input_count - len(result_list),
                blackbox_items=blocked_items,
            )
        except Exception:
            pass

    if not items_in or not bl:
        _emit_counts(items_in)
        return items_in

    feature_norm = str(feature or "").lower()
    if feature_norm not in {"history", "ratings"}:
        result = filter_with(state_store, items_in, extra_block=bl)
        _emit_counts(result)
        return result

    hard = global_tomb | blackbox
    filtered = filter_with(state_store, items_in, extra_block=hard) if hard else items_in

    if ignore_pair_tomb or not pair_tomb_eff:
        _emit_counts(filtered)
        return filtered

    out: list[dict[str, Any]] = []
    for it in filtered:
        try:
            blocked = (
                _history_is_blocked_by_tomb(it, pmap)
                if feature_norm == "history"
                else _ratings_is_blocked_by_tomb(it, pmap)
            )
            if blocked:
                continue
        except Exception:
            try:
                if filter_with(state_store, [it], extra_block=pair_tomb_eff) == []:
                    continue
            except Exception:
                pass
        out.append(it)
    _emit_counts(out)
    return out
