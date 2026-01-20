# cw_platform/orchestration/_pairs_blocklist.py
# Pairs blocklist handling for the orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from collections.abc import Iterable, Mapping
from typing import Any
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
    g_tomb, p_tomb, unr, bb = _breakdown(
        state_store,
        dst,
        feature,
        pair_key=pair_key,
        cross_feature_unresolved=cross_feature_unresolved,
    )
    bl = g_tomb | p_tomb | unr | bb

    if emit is not None:
        try:
            emit(
                "debug",
                msg="blocked.counts",
                feature=feature,
                dst=dst,
                pair=pair_key,
                blocked_global_tomb=0,
                blocked_pair_tomb=len(p_tomb),
                blocked_unresolved=len(unr),
                blocked_blackbox=len(bb),
                blocked_total=len(bl),
            )
        except Exception:
            pass

    return filter_with(state_store, list(items or []), extra_block=bl)