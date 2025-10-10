from __future__ import annotations
from typing import Any, Dict, List, Mapping, Tuple, Optional
from ..id_map import minimal

# Presence diff (generic)
def diff(src_idx: Mapping[str, Any], dst_idx: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    add, rem = [], []
    for k, v in src_idx.items():
        if k not in dst_idx:
            add.append(minimal(v))
    for k, v in dst_idx.items():
        if k not in src_idx:
            rem.append(minimal(v))
    return add, rem


# ---------- Ratings helpers

def _norm_rating(v: Any) -> Optional[int]:
    """
    Normalize possible rating values to 1..10.
    - Accept ints/floats
    - Accept 0..100 (normalize to 1..10)
    - Return None for invalid or out-of-range
    """
    if v is None:
        return None
    try:
        f = float(v)
    except Exception:
        # strings like "8" are also fine
        try:
            f = float(str(v).strip())
        except Exception:
            return None

    # Plex 0–100 → 1–10 (SIMKL/Trakt use 1..10)
    if 10 < f <= 100:
        f = f / 10.0

    n = int(round(f))
    return n if 1 <= n <= 10 else None


def _pick_rating(d: Any) -> Optional[int]:
    """
    Extract rating from known fields.
    """
    if not isinstance(d, dict):
        return None
    return _norm_rating(
        d.get("rating")
        or d.get("user_rating")
        or d.get("score")
        or d.get("value")
    )


def _pick_rated_at(d: Any) -> Optional[str]:
    if not isinstance(d, dict):
        return None
    v = (d.get("rated_at") or d.get("ratedAt") or d.get("user_rated_at") or "").strip()
    return v or None


def _ts_epoch(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    s = str(s).strip()
    # numeric strings allowed (seconds or milliseconds)
    if s.isdigit():
        try:
            n = int(s)
            return n // 1000 if len(s) >= 13 else n
        except Exception:
            return None
    # ISO → epoch
    try:
        from datetime import datetime, timezone
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp())
    except Exception:
        return None


def _pack_minimal_with_rating(item: Dict[str, Any], rating: int) -> Dict[str, Any]:
    """
    Keep compact ids/type and attach rating + optional rated_at for providers
    that accept/propagate timestamps.
    """
    it = minimal(item)
    it["rating"] = rating
    ra = _pick_rated_at(item)
    if ra:
        it["rated_at"] = ra
    return it


# Ratings: value-aware upsert/unrate, carrying payload
def diff_ratings(
    src_idx: Mapping[str, Any],
    dst_idx: Mapping[str, Any],
    *,
    propagate_timestamp_updates: bool = False,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Compute planned changes for ratings:

    - Upsert when dst missing OR the rating value differs (always).
    - Optionally (propagate_timestamp_updates=True) also upsert when values are
      equal but src.rated_at is strictly newer than dst.rated_at.
    - Unrate when key exists only on dst (caller can gate removals separately).

    NOTE: This does NOT look at any global from_date; the caller should pre-filter
    src and dst consistently if a time window is desired.
    """
    upserts: List[Dict[str, Any]] = []
    unrates: List[Dict[str, Any]] = []

    # plan adds/upserts
    for k, sv in (src_idx or {}).items():
        rs = _pick_rating(sv)
        if rs is None:
            continue

        dv = (dst_idx or {}).get(k)
        rd = _pick_rating(dv) if dv is not None else None

        # destination missing → upsert
        if dv is None:
            upserts.append(_pack_minimal_with_rating(sv, rs))
            continue

        # rating value changed → upsert
        if rd is None or rd != rs:
            upserts.append(_pack_minimal_with_rating(sv, rs))
            continue

        # optionally propagate timestamp-only update (same score, newer time)
        if propagate_timestamp_updates:
            ts_s = _ts_epoch(_pick_rated_at(sv))
            ts_d = _ts_epoch(_pick_rated_at(dv))
            if ts_s is not None and ts_d is not None and ts_s > ts_d:
                upserts.append(_pack_minimal_with_rating(sv, rs))

    # plan removals (dst-only keys)
    for k, dv in (dst_idx or {}).items():
        if k not in (src_idx or {}):
            if _pick_rating(dv) is not None:  # only unrate if there actually is a rating
                unrates.append(minimal(dv))

    return upserts, unrates
