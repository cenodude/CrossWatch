# providers/gmt_hooks.py
# Thin glue so providers can consult/record global tombstones without depending
# directly on orchestrator internals. Keep import surface minimal and stable.

from __future__ import annotations
from typing import Any, Mapping, Optional

try:
    from cw_platform.gmt_store import GlobalTombstoneStore
    from cw_platform.gmt_policy import Scope, should_suppress_write
except Exception:  # pragma: no cover
    GlobalTombstoneStore = object  # type: ignore

    class Scope:  # type: ignore
        def __init__(self, list: str, dim: str) -> None:
            self.list = list
            self.dim = dim

    def should_suppress_write(*_a, **_k) -> bool:  # type: ignore
        return False


__all__ = ["suppress_check", "record_negative"]

def suppress_check(
    *,
    store: GlobalTombstoneStore,
    item: Mapping[str, Any],
    feature: str,
    write_op: str,
    pair_id: Optional[str] = None,
) -> bool:
    """
    Return True when a write for this (feature, op) should be suppressed per tombstones.
    """
    scope = Scope(list=str(feature or "").lower(), dim=str(write_op or "").lower())
    return bool(should_suppress_write(store, entity=item, scope=scope, pair_id=pair_id))

def record_negative(
    *,
    store: GlobalTombstoneStore,
    item: Mapping[str, Any],
    feature: str,
    op: str,
    origin: str,
    pair_id: Optional[str] = None,
    note: Optional[str] = None,
) -> None:
    """
    Record a negative event (remove/unrate/unscrobble) to quarantine re-adds for a period.
    """
    scope = Scope(list=str(feature or "").lower(), dim=str(op or "").lower())
    try:
        store.record_negative_event(entity=item, scope=scope, origin=str(origin or "").upper(), pair_id=pair_id, note=note)
    except Exception:
        # Best-effort: don’t break providers if store is unavailable.
        pass
