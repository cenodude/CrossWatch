# cw_platform/orchestrator/_planner.py
# planner helpers for orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..id_map import minimal, ids_from, coalesce_ids


_STRONG_ID_KEYS: tuple[str, ...] = ("tmdb", "imdb", "tvdb", "trakt")


def _strong_keys(item: Mapping[str, Any]) -> set[str]:
    out: set[str] = set()

    def _tok(k: str, v: Any) -> str | None:
        if v is None:
            return None
        s = str(v).strip().lower()
        return f"{k}:{s}" if s else None

    ids = ids_from(item)
    for k in _STRONG_ID_KEYS:
        t = _tok(k, ids.get(k))
        if t:
            out.add(t)

    typ = str(item.get("type") or "").strip().lower()
    if typ not in ("season", "episode"):
        return out

    s = item.get("season") if item.get("season") is not None else item.get("season_number")
    e = item.get("episode") if item.get("episode") is not None else item.get("episode_number")
    try:
        sn = int(s) if s is not None else None
        en = int(e) if e is not None else None
    except Exception:
        return out

    frag: str | None = None
    if typ == "season" and sn is not None:
        frag = f"#season:{sn}"
    elif typ == "episode" and sn is not None and en is not None:
        frag = f"#s{str(sn).zfill(2)}e{str(en).zfill(2)}"

    if not frag:
        return out

    show_ids_raw = item.get("show_ids")
    if isinstance(show_ids_raw, Mapping) and show_ids_raw:
        sids = coalesce_ids(show_ids_raw)
        for k in _STRONG_ID_KEYS:
            t = _tok(k, sids.get(k))
            if t:
                out.add(f"{t}{frag}")

    return out


# Presence helpers
def diff(
    src_idx: Mapping[str, Any],
    dst_idx: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    add: list[dict[str, Any]] = []
    rem: list[dict[str, Any]] = []
    dst_alias: set[str] = set()
    for dv in (dst_idx or {}).values():
        if isinstance(dv, Mapping):
            dst_alias |= _strong_keys(dv)

    src_alias: set[str] = set()
    for sv in (src_idx or {}).values():
        if isinstance(sv, Mapping):
            src_alias |= _strong_keys(sv)

    for k, v in (src_idx or {}).items():
        if k in (dst_idx or {}):
            continue
        if isinstance(v, Mapping) and (_strong_keys(v) & dst_alias):
            continue
        add.append(minimal(v))

    for k, v in (dst_idx or {}).items():
        if k in (src_idx or {}):
            continue
        if isinstance(v, Mapping) and (_strong_keys(v) & src_alias):
            continue
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

def diff_progress(
    src_idx: dict[str, Any],
    dst_idx: dict[str, Any],
    *,
    fcfg: Mapping[str, Any] | None = None,
    propagate_timestamp_updates: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:

    cfg = dict(fcfg or {})
    min_seconds = int(cfg.get("min_seconds") or cfg.get("minSeconds") or 60)
    delta_seconds = int(cfg.get("delta_seconds") or cfg.get("deltaSeconds") or 30)
    max_percent = float(cfg.get("max_percent") or cfg.get("maxPercent") or 95)

    def _as_int(v: Any) -> int | None:
        try:
            if v is None:
                return None
            if isinstance(v, bool):
                return None
            n = int(float(v))
            return n
        except Exception:
            return None

    def _progress_ms(it: Mapping[str, Any] | None) -> int | None:
        if not it:
            return None
        for k in ("progress_ms", "progressMs", "viewOffset", "progress"):
            v = _as_int(it.get(k))
            if v is not None:
                return v
        return None

    def _duration_ms(it: Mapping[str, Any] | None) -> int | None:
        if not it:
            return None
        for k in ("duration_ms", "durationMs", "duration"):
            v = _as_int(it.get(k))
            if v is not None and v > 0:
                return v
        return None

    def _epoch(v: Any) -> int | None:
        if v is None:
            return None
        try:
            if isinstance(v, (int, float)):
                return int(v)
            s = str(v).strip()
            if not s:
                return None
            if s.isdigit():
                return int(s)
            from datetime import datetime
            s = s.replace("Z", "+00:00")
            return int(datetime.fromisoformat(s).timestamp())
        except Exception:
            return None

    def _progress_epoch(it: Mapping[str, Any] | None) -> int | None:
        if not it:
            return None
        for k in ("progress_at", "progressAt", "last_played", "lastPlayed", "lastViewedAt"):
            v = it.get(k)
            ep = _epoch(v)
            if ep is not None:
                return ep
        return None

    def _pct(ms: int, dur: int | None) -> float | None:
        if dur is None or dur <= 0:
            return None
        try:
            return (float(ms) / float(dur)) * 100.0
        except Exception:
            return None

    def _pack_progress(it: Mapping[str, Any]) -> dict[str, Any]:
        base = minimal(it)
        pm = _progress_ms(it)
        if pm is not None:
            base["progress_ms"] = int(pm)
        dm = _duration_ms(it)
        if dm is not None:
            base["duration_ms"] = int(dm)
        pa = it.get("progress_at") or it.get("progressAt") or it.get("last_played")
        if isinstance(pa, str) and pa.strip():
            base["progress_at"] = pa.strip()
        return base

    upserts: list[dict[str, Any]] = []
    clears: list[dict[str, Any]] = []

    min_ms = max(0, min_seconds) * 1000
    delta_ms = max(0, delta_seconds) * 1000

    # Upserts
    for k, s_it in (src_idx or {}).items():
        if not isinstance(s_it, Mapping):
            continue
        s_ms = _progress_ms(s_it)
        if s_ms is None or s_ms <= 0:
            continue
        if min_ms and s_ms < min_ms:
            continue
        s_dur = _duration_ms(s_it)
        p = _pct(s_ms, s_dur)
        if p is not None and p >= max_percent:
            # Near completion: let history sync handle the played state.
            continue

        d_it = dst_idx.get(k)
        d_ms = _progress_ms(d_it) if isinstance(d_it, Mapping) else None

        # If destination has no progress, always upsert.
        if d_ms is None:
            upserts.append(_pack_progress(s_it))
            continue

        if abs(s_ms - d_ms) < delta_ms:
            continue

        s_ep = _progress_epoch(s_it)
        d_ep = _progress_epoch(d_it) if isinstance(d_it, Mapping) else None

        # If source is ahead by a meaningful margin, always upsert.
        if s_ms > d_ms and (s_ms - d_ms) >= delta_ms:
            upserts.append(_pack_progress(s_it))
            continue

        # Avoid regressing progress unless the source is clearly newer.
        if s_ms < d_ms:
            if s_ep is not None and d_ep is not None:
                if s_ep < d_ep:
                    continue

                if (not propagate_timestamp_updates) and s_ep == d_ep:
                    continue
                upserts.append(_pack_progress(s_it))
                continue
            # No timestamps: do not regress.
            continue

        if s_ep is not None and d_ep is not None and s_ep < d_ep:
            continue

        upserts.append(_pack_progress(s_it))

    # Clears
    for k, s_it in (src_idx or {}).items():
        if not isinstance(s_it, Mapping):
            continue
        s_ms = _progress_ms(s_it)
        if s_ms is None or s_ms > 0:
            continue

        d_it = (dst_idx or {}).get(k)
        if not isinstance(d_it, Mapping):
            continue
        d_ms = _progress_ms(d_it)
        if d_ms is None or d_ms <= 0:
            continue

        base = minimal(s_it)
        base["progress_ms"] = 0
        clears.append(base)

    return upserts, clears
