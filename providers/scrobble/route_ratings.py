# providers/scrobble/route_ratings.py
# CrossWatch - Rating writes for a watcher route destination
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from cw_platform.provider_instances import normalize_instance_id

OPS_SINKS: tuple[str, ...] = (
    "trakt", "simkl", "mdblist", "plex", "crosswatch", "floppy", "punchplay", "flicklist", "wetrakr", "scrob", "kitsu", "myanimelist",
)
EPISODE_SINKS: frozenset[str] = frozenset({"trakt", "plex", "mdblist", "crosswatch", "floppy", "punchplay", "flicklist", "wetrakr", "scrob"})
RATING_SINKS: tuple[str, ...] = (*OPS_SINKS, "anilist")


def _sink(value: Any) -> str:
    return str(value or "").strip().lower()


def supports(sink: Any, media_type: Any) -> bool:
    name = _sink(sink)
    kind = _sink(media_type)
    if name not in RATING_SINKS:
        return False
    if kind == "movie":
        return True
    return kind == "episode" and name in EPISODE_SINKS


def build_item(
    media_type: Any,
    *,
    ids: Mapping[str, Any] | None = None,
    show_ids: Mapping[str, Any] | None = None,
    title: Any = None,
    series_title: Any = None,
    year: Any = None,
    season: Any = None,
    episode: Any = None,
    rating: float | None = None,
) -> dict[str, Any] | None:
    from providers.scrobble.plex.ratings_sync import item_from_plex_rating

    kind = _sink(media_type)
    if kind == "episode":
        md = {"title": title, "grandparentTitle": series_title, "parentIndex": season, "index": episode}
        return item_from_plex_rating("episode", md, {}, rating, show_ids=dict(show_ids or {}), episode_ids=dict(ids or {}))
    if kind == "movie":
        return item_from_plex_rating("movie", {"title": title, "year": year}, dict(ids or {}), rating)
    return None


def send(cfg: Mapping[str, Any], sink: Any, instance: Any, item: Mapping[str, Any], rating: float | None) -> dict[str, Any]:
    name = _sink(sink)
    kind = _sink(item.get("type"))
    if not supports(name, kind):
        return {"ok": True, "skipped": True, "reason": "unsupported_rating_target"}
    inst = normalize_instance_id(instance)
    if name == "anilist":
        from providers.scrobble.anilist.ratings import send_plex_rating

        ids = item.get("ids")
        return send_plex_rating(cfg, inst, kind, {"title": item.get("title")}, dict(ids) if isinstance(ids, Mapping) else {}, None, rating)

    from providers.scrobble.plex.ratings_sync import send_rating

    return send_rating(name, cfg, inst, item, rating, sinks=OPS_SINKS)


__all__ = ["EPISODE_SINKS", "OPS_SINKS", "RATING_SINKS", "build_item", "send", "supports"]
