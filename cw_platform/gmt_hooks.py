# providers/gmt_hooks.py
# Thin glue so providers can consult/record global tombstones without depending
# directly on orchestrator internals. Keep import surface minimal and stable.

from __future__ import annotations
from typing import Any, Mapping, Optional

# --- Imports (soft; fall back gracefully) -------------------------------------
try:
    # Preferred: shared store + policy
    from cw_platform.gmt_store import GlobalTombstoneStore  # type: ignore
except Exception:  # pragma: no cover
    GlobalTombstoneStore = object  # type: ignore

# Policy helpers (TTL + op normalization)
try:
    from cw_platform.gmt_policy import get_quarantine_ttl_sec, negative_event_key  # type: ignore
except Exception:  # pragma: no cover
    def get_quarantine_ttl_sec(cfg: Mapping[str, Any] | None, feature: str) -> int:
        # Safe defaults: WL=7d, Ratings=3d, History=2d
        feat = (feature or "").lower()
        if feat == "watchlist":
            return 7 * 24 * 3600
        if feat == "ratings":
            return 3 * 24 * 3600
        if feat == "history":
            return 2 * 24 * 3600
        return 7 * 24 * 3600

    def negative_event_key(feature: str, op: str) -> str:
        feat = (feature or "").lower()
        opn = (op or "").lower()
        # Normalize to stable dimensions
        if feat == "watchlist":
            return "remove"
        if feat == "ratings":
            return "unrate" if opn in ("remove", "delete", "unset", "unrate") else "unrate"
        if feat == "history":
            return "unscrobble"
        return opn or "remove"

# Canonical keying (ID-first; title/year fallback)
try:
    from cw_platform.id_map import canonical_key  # type: ignore
except Exception:  # pragma: no cover
    _ID_KEYS = ("tmdb", "imdb", "tvdb", "trakt", "plex", "guid", "slug")

    def canonical_key(item: Mapping[str, Any]) -> str:  # minimal fallback
        ids = (item.get("ids") or {})
        for k in _ID_KEYS:
            v = ids.get(k)
            if v:
                return f"{k}:{str(v).lower()}"
        t = (item.get("type") or "").lower()
        ttl = str(item.get("title") or "").strip().lower()
        yr = item.get("year") or ""
        return f"{t}|title:{ttl}|year:{yr}"

# Optional helper from store: a single entry point that already applies policy
try:
    from cw_platform.gmt_policy import should_suppress_write  # type: ignore
except Exception:  # pragma: no cover
    def should_suppress_write(*_a, **_k) -> bool:  # type: ignore
        return False

__all__ = ["suppress_check", "record_negative"]


def _cfg_from_store(store: GlobalTombstoneStore | Any) -> Mapping[str, Any] | None:
    """Best-effort to pull a config mapping from the store."""
    for attr in ("cfg", "config", "get_config"):
        try:
            v = getattr(store, attr, None)
            if callable(v):
                return v()
            if isinstance(v, dict):
                return v
        except Exception:
            pass
    return None


def suppress_check(
    *,
    store: GlobalTombstoneStore,
    item: Mapping[str, Any],
    feature: str,
    write_op: str,
    pair_id: Optional[str] = None,
) -> bool:
    """
    Return True when a write for this (feature, write_op) should be suppressed due to
    a recent negative event (quarantine window). This prevents immediate re-adds after removes,
    rating toggles thrash, and history re-scrobbles right after unscrobbles.

    Strategy:
    - Resolve canonical key for the entity.
    - Determine TTL via policy for the feature.
    - Prefer store-native predicate if available:
        * store.should_suppress_by_key(key, list, dim, ttl_sec, pair_id)
        * or should_suppress_write(store=..., entity=item, scope=..., pair_id=...)
    - Fallback: lookup last negative timestamp and compare against TTL.
    """
    try:
        key = canonical_key(item)
        feat = str(feature or "").lower()
        dim = str(write_op or "").lower()
        cfg = _cfg_from_store(store)
        ttl_sec = int(get_quarantine_ttl_sec(cfg, feat))

        # 1) Keyed API (best)
        pred = getattr(store, "should_suppress_by_key", None)
        if callable(pred):
            return bool(pred(key=key, list=feat, dim=dim, ttl_sec=ttl_sec, pair_id=pair_id))

        # 2) Generic policy predicate (entity-based)
        if should_suppress_write is not None:
            try:
                return bool(should_suppress_write(store=store, entity=item, scope={"list": feat, "dim": dim}, pair_id=pair_id, ttl_sec=ttl_sec))
            except TypeError:
                # Older signature without ttl_sec
                return bool(should_suppress_write(store=store, entity=item, scope={"list": feat, "dim": dim}, pair_id=pair_id))

        # 3) Dumb fallback: inspect store for last negative ts and compare
        get_ts = getattr(store, "last_negative_ts", None)  # expected signature: (key, list, dim, pair_id) -> int|None
        if callable(get_ts):
            ts = get_ts(key=key, list=feat, dim=dim, pair_id=pair_id)
            if ts is not None:
                import time as _t
                return (_t.time() - int(ts)) < ttl_sec

    except Exception:
        # Fail-open: never block writes because of bookkeeping failures.
        return False

    return False


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
    Record a negative event (remove / unrate / unscrobble).
    Providers should call this *after* a successful negative write so future
    conflicting writes can be quarantined for a short TTL.

    Notes:
    - Uses canonical key for stable identity.
    - Operation name is normalized via policy to keep dimensions tidy.
    - Never raises: bookkeeping must not break provider writes.
    """
    try:
        key = canonical_key(item)
        feat = str(feature or "").lower()
        dim = negative_event_key(feat, op)
        origin_norm = str(origin or "").upper()

        # Prefer key-based API if present
        rec = getattr(store, "record_negative_by_key", None)
        if callable(rec):
            rec(key=key, list=feat, dim=dim, origin=origin_norm, pair_id=pair_id, note=note)
            return

        # Fallback to entity-based store method
        rec2 = getattr(store, "record_negative_event", None)
        if callable(rec2):
            rec2(entity=item, scope={"list": feat, "dim": dim}, origin=origin_norm, pair_id=pair_id, note=note)
            return

        # Last-ditch: generic put()
        put = getattr(store, "put", None)
        if callable(put):
            import time as _t
            put(kind="negative", key=key, list=feat, dim=dim, origin=origin_norm, pair_id=pair_id, ts=int(_t.time()), note=note)

    except Exception:
        # Best-effort by design.
        return
