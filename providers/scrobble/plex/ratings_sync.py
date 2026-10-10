# providers/scrobble/plex/ratings_sync.py
# CrossWatch - Plex rating writes for sync-backed local sinks
from __future__ import annotations

import math
import time
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from cw_platform.provider_instances import build_provider_config_view, normalize_instance_id


def plex_rating_value(value: Any) -> float:
    try:
        rating = float(value)
        return rating if math.isfinite(rating) and 0 <= rating <= 10 else 0.0
    except (TypeError, ValueError):
        return 0.0


def _as_int(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(float(value))
    except Exception:
        return None


def _clean_ids(ids: Mapping[str, Any] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in ("tmdb", "imdb", "tvdb", "trakt", "simkl", "mdblist", "fldb", "plex", "mal", "kitsu", "anilist", "anidb"):
        value = (ids or {}).get(key)
        if value in (None, ""):
            continue
        out[key] = value
    return out


def _year(md: Mapping[str, Any]) -> int | None:
    direct = _as_int(md.get("year"))
    if direct:
        return direct
    date = str(md.get("originallyAvailableAt") or md.get("originally_available_at") or "").strip()
    if len(date) >= 4 and date[:4].isdigit():
        return int(date[:4])
    return None


def item_from_plex_rating(
    media_type: str,
    md: Mapping[str, Any],
    ids: Mapping[str, Any],
    rating: float | None,
    *,
    show_ids: Mapping[str, Any] | None = None,
    episode_ids: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    mt = str(media_type or "").strip().lower()
    if mt not in {"movie", "show", "episode"}:
        return None

    clean = _clean_ids(ids)
    item: dict[str, Any] = {"type": "movie" if mt == "movie" else mt}

    if mt == "episode":
        season = _as_int(md.get("parentIndex") if md.get("parentIndex") is not None else md.get("season"))
        episode = _as_int(md.get("index") or md.get("episode"))
        if season is None or episode is None:
            return None
        ep_ids = _clean_ids(episode_ids) or clean
        sh_ids = _clean_ids(show_ids) if show_ids is not None else clean
        if not ep_ids and not sh_ids:
            return None
        item.update({
            "ids": ep_ids,
            "show_ids": sh_ids,
            "season": season,
            "episode": episode,
            "series_title": str(md.get("grandparentTitle") or md.get("showTitle") or "").strip(),
            "title": str(md.get("title") or f"S{season:02d}E{episode:02d}").strip(),
        })
    else:
        if not clean:
            return None
        item["ids"] = clean
        title = str(md.get("title") or "").strip()
        if title:
            item["title"] = title
        year = _year(md)
        if year:
            item["year"] = year

    if rating is not None and rating > 0:
        item["rating"] = rating
        item["rated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return {k: v for k, v in item.items() if v not in (None, "", {}, [])}


def _ops(provider: str, sinks: Iterable[str] | None = None) -> Any | None:
    from cw_platform.modules_registry import load_sync_ops

    allowed = OPS_RATING_SINKS if sinks is None else tuple(sinks)
    return load_sync_ops(provider) if provider in allowed else None


OPS_RATING_SINKS: tuple[str, ...] = ("crosswatch", "floppy", "punchplay", "flicklist", "wetrakr", "scrob", "kitsu", "myanimelist")
RATING_SINKS: tuple[str, ...] = ("trakt", "simkl", "mdblist", "anilist", *OPS_RATING_SINKS)


def dispatch_ops_ratings(
    media_type: str,
    md: Mapping[str, Any],
    ids: Mapping[str, Any],
    rating: float | None,
    cfg: Mapping[str, Any],
    *,
    enabled: Iterable[str],
    instance_for: Callable[[str], Any],
    show_ids: Mapping[str, Any] | None = None,
    episode_ids: Mapping[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    targets = [s for s in OPS_RATING_SINKS if s in {str(x or "").strip().lower() for x in (enabled or ())}]
    if not targets:
        return {}

    try:
        item = item_from_plex_rating(
            media_type,
            md,
            ids,
            rating if rating else None,
            show_ids=show_ids,
            episode_ids=episode_ids,
        )
    except Exception as exc:
        return {sink: {"ok": False, "error": str(exc)} for sink in targets}

    if not item:
        return {sink: {"ok": False, "error": "no_ids"} for sink in targets}

    out: dict[str, dict[str, Any]] = {}
    for sink in targets:
        out[sink] = send_rating(sink, cfg, instance_for(sink), item, rating)
    return out


def send_rating(
    provider: str,
    cfg: Mapping[str, Any],
    instance: Any,
    item: Mapping[str, Any],
    rating: float | None,
    *,
    sinks: Iterable[str] | None = None,
) -> dict[str, Any]:
    sink = str(provider or "").strip().lower()
    ops = _ops(sink) if sinks is None else _ops(sink, sinks)
    if ops is None:
        return {"ok": False, "error": "unsupported_rating_sink"}

    if sink in {"kitsu", "myanimelist"} and item.get("type") not in {"movie", "show"}:
        return {"ok": True, "skipped": True, "reason": "unsupported_media_type"}
    inst = normalize_instance_id(instance)
    view = build_provider_config_view(dict(cfg or {}), sink, inst)
    view["_cw_provider_instance"] = inst
    clear = rating is None or float(rating) <= 0
    try:
        from cw_platform.orchestrator._pairs_utils import ratings_step_for
        from cw_platform.orchestrator._planner import _quantize_rating

        payload = dict(item)
        if not clear:
            payload["rating"] = _quantize_rating(rating, ratings_step_for(ops))
            if payload["rating"] is None:
                return {"ok": False, "error": "invalid_rating"}
        res = (ops.remove if clear else ops.add)(view, [payload], feature="ratings", dry_run=False)
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    body = dict(res or {}) if isinstance(res, Mapping) else {"raw": res}
    unresolved = list(body.get("unresolved_keys") or [])
    skipped = list(body.get("skipped_keys") or [])
    confirmed = list(body.get("confirmed_keys") or [])
    ok = bool(body.get("ok", True)) and not unresolved
    if skipped and not confirmed:
        ok = False
        body.setdefault("error", "unsupported_media_type")
    return {"ok": ok, "resp": body}
