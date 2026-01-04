# cw_platform/orchestrator/_planner.py
# planner helpers for orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..id_map import minimal


# Presence helpers
def diff(
    src_idx: Mapping[str, Any],
    dst_idx: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    add: list[dict[str, Any]] = []
    rem: list[dict[str, Any]] = []

    for k, v in src_idx.items():
        if k not in dst_idx:
            add.append(minimal(v))

    for k, v in dst_idx.items():
        if k not in src_idx:
            rem.append(minimal(v))

    return add, rem


# Ratings helpers
def _round_half_up(f: float) -> int:
    return int(f + 0.5) if f >= 0 else int(f - 0.5)


def _norm_rating(v: Any) -> int | None:
    if v is None:
        return None
    try:
        f = float(v)
    except Exception:
        try:
            f = float(str(v).strip())
        except Exception:
            return None

    if f <= 0:
        return None

    if 10 < f <= 100:
        f = f / 10.0

    n = _round_half_up(f)
    return n if 1 <= n <= 10 else None


def _pick_rating(d: Any) -> int | None:
    if not isinstance(d, dict):
        return None

    for k in ("rating", "user_rating", "score", "value"):
        if k in d and d.get(k) is not None:
            return _norm_rating(d.get(k))
    return None


def _pick_rated_at(d: Any) -> str | None:
    if not isinstance(d, dict):
        return None
    v = (d.get("rated_at") or d.get("ratedAt") or d.get("user_rated_at") or "").strip()
    return v or None


def _ts_epoch(s: str | None) -> int | None:
    if not s:
        return None
    s = str(s).strip()
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


def _pack_minimal_with_rating(item: Mapping[str, Any], rating: int) -> dict[str, Any]:
    it = minimal(item)
    it["rating"] = rating
    ra = _pick_rated_at(item)
    if ra:
        it["rated_at"] = ra
    return it


def diff_ratings(
    src_idx: Mapping[str, Any],
    dst_idx: Mapping[str, Any],
    *,
    propagate_timestamp_updates: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    upserts: list[dict[str, Any]] = []
    unrates: list[dict[str, Any]] = []

    for k, sv in (src_idx or {}).items():
        rs = _pick_rating(sv)
        if rs is None:
            continue

        dv = (dst_idx or {}).get(k)
        rd = _pick_rating(dv) if dv is not None else None

        if dv is None:
            upserts.append(_pack_minimal_with_rating(sv, rs))
            continue

        if rd is None or rd != rs:
            upserts.append(_pack_minimal_with_rating(sv, rs))
            continue

        if propagate_timestamp_updates:
            ts_s = _ts_epoch(_pick_rated_at(sv))
            ts_d = _ts_epoch(_pick_rated_at(dv))
            if ts_s is not None and ts_d is not None and ts_s > ts_d:
                upserts.append(_pack_minimal_with_rating(sv, rs))

    for k, dv in (dst_idx or {}).items():
        if k not in (src_idx or {}):
            if _pick_rating(dv) is not None:
                unrates.append(minimal(dv))

    return upserts, unrates
