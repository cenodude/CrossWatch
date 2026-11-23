# services/hub_airing.py
# CrossWatch - Upcoming episodes from the scoped Hub history
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from datetime import date
from typing import Any

from providers.scrobble._show_tmdb import show_tmdb_id
from services.episode_overview import episode_payload, map_episodes


def airing_payload(indices: list[dict[str, Any]], cfg: dict[str, Any], provider: Any, today: date) -> dict[str, Any]:
    candidates: dict[tuple, dict[str, Any]] = {}
    for index in indices:
        for row in index.get("rows", []):
            kind = row.get("_type") or row.get("type")
            if kind not in {"episode", "season", "show"}:
                continue
            ids = row.get("ids") or {}
            show_ids = dict(ids.get("show_ids") or {}) if kind in {"episode", "season"} else dict(ids)
            tmdb = row.get("tmdb") or ids.get("tmdb_show") or show_ids.get("tmdb")
            if tmdb:
                show_ids["tmdb"] = tmdb
            identity = tuple((key, str(show_ids[key])) for key in ("tmdb", "tvdb", "imdb") if show_ids.get(key))
            if not identity:
                continue
            show = candidates.setdefault(identity, {"ids": show_ids, "title": row.get("title"), "year": row.get("year"), "watched": set()})
            season, episode = row.get("season"), row.get("episode")
            if type(season) is int and type(episode) is int:
                show["watched"].add((season, episode))

    def resolve(show: dict[str, Any]) -> dict[str, Any]:
        ids = dict(show["ids"])
        tmdb = str(ids.get("tmdb") or "")
        if not tmdb.isascii() or not tmdb.isdecimal() or int(tmdb) <= 0:
            found = show_tmdb_id(cfg, ids, {}, strict=True)
            if found:
                ids["tmdb"] = str(found)
        return {**show, "ids": ids}

    shows: dict[str, dict[str, Any]] = {}
    unavailable = 0
    for show in map_episodes(resolve, candidates.values()):
        tmdb = str(show["ids"].get("tmdb") or "")
        if not tmdb.isascii() or not tmdb.isdecimal() or int(tmdb) <= 0:
            unavailable += 1
            continue
        if tmdb in shows:
            shows[tmdb]["watched"].update(show["watched"])
        else:
            shows[tmdb] = {**show, "key": f"tmdb:{tmdb}"}
    result = episode_payload(((show, show["watched"]) for show in shows.values()), provider, today)
    result.pop("continue")
    result["unavailable"] += unavailable
    return result
