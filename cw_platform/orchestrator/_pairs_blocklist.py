from __future__ import annotations
from typing import Iterable, Mapping, Optional, Set, Dict, Any, Tuple
from ._tombstones import keys_for_feature, filter_with

try:
    from ._unresolved import load_unresolved_keys  # type: ignore
except Exception:  # pragma: no cover
    def load_unresolved_keys(dst: str, feature: Optional[str] = None, *, cross_features: bool = True) -> Set[str]:
        return set()

try:
    from ._blackbox import load_blackbox_keys  # type: ignore
except Exception:  # pragma: no cover
    def load_blackbox_keys(dst: str, feature: str, pair: Optional[str] = None) -> Set[str]:
        return set()


# Int. 
def _breakdown(
    state_store,
    dst: str,
    feature: str,
    *,
    pair_key: Optional[str],
    cross_feature_unresolved: bool,
) -> Tuple[Set[str], Set[str], Set[str], Set[str]]:

    try:
        gmap = keys_for_feature(state_store, feature, pair=None) or {}
        global_tomb: Set[str] = set(gmap.keys()) if isinstance(gmap, Mapping) else set()
    except Exception:
        global_tomb = set()

    try:
        pmap = keys_for_feature(state_store, feature, pair=pair_key) or {}
        pair_tomb: Set[str] = set(pmap.keys()) if isinstance(pmap, Mapping) else set()
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


# Helpers
def blocked_keys_for_destination(
    state_store,
    dst: str,
    feature: str,
    *,
    pair_key: Optional[str] = None,
    cross_feature_unresolved: bool = True,
) -> Set[str]:
    g_tomb, p_tomb, unr, bb = _breakdown(
        state_store, dst, feature,
        pair_key=pair_key,
        cross_feature_unresolved=cross_feature_unresolved,
    )
    return g_tomb | p_tomb | unr | bb

def apply_blocklist(
    state_store,
    items: Iterable[Dict[str, Any]],
    *,
    dst: str,
    feature: str,
    pair_key: Optional[str] = None,
    cross_feature_unresolved: bool = True,
    emit=None,
) -> list[Dict[str, Any]]:
    g_tomb, p_tomb, unr, bb = _breakdown(
        state_store, dst, feature,
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
                blocked_global_tomb=len(g_tomb),
                blocked_pair_tomb=len(p_tomb),
                blocked_unresolved=len(unr),
                blocked_blackbox=len(bb),
                blocked_total=len(bl),
            )
        except Exception:
            pass

    return filter_with(state_store, list(items or []), extra_block=bl)
