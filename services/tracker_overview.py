# services/tracker_overview.py
# CrossWatch - Tracker inputs for shared episode suggestions and upcoming dates
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any

from cw_platform.tracker_store import TrackerStore
from providers.scrobble._show_tmdb import show_tmdb_id
from services.episode_overview import episode_payload, map_episodes


def _resolve_tmdb(store: TrackerStore, shows: list[dict[str, Any]], cfg: Mapping[str, Any] | None) -> None:
    missing = [show for show in shows if not show["key"].startswith("tmdb:")]
    if not cfg or not missing:
        return
    imdbs = store.title_show_imdbs(show["key"] for show in missing)
    single = [(show, imdb) for show in missing if len(imdbs.get(show["key"]) or ()) == 1 and (imdb := next(iter(imdbs[show["key"]])))]

    def resolve(pair: tuple[dict[str, Any], str]) -> int | None:
        try:
            return show_tmdb_id(cfg, {"imdb": pair[1]}, {})
        except Exception:
            return None

    for (show, _imdb), found in zip(single, map_episodes(resolve, single)):
        if found:
            show["tmdb"] = str(found)


def _inputs(store: TrackerStore, offset: int, cfg: Mapping[str, Any] | None = None) -> tuple[int, int, list[tuple[dict[str, Any], set[tuple[int, int]]]]]:
    with store.session():
        hidden = {key[5:] for key in store.status_keys("dropped") if key.startswith("show:")}
        total, shows = store.show_summaries(offset=offset, limit=32, exclude=hidden)
        notes = store.notes_for(f"show:{show['key']}" for show in shows)
        watched = store.watched_episodes(show["key"] for show in shows)
        for show in shows:
            show["status"] = (notes.get(f"show:{show['key']}") or {}).get("status")
    _resolve_tmdb(store, shows, cfg)
    aliased = [show for show in shows if show.get("tmdb")]
    if aliased:
        tmdb_notes = store.notes_for(f"show:tmdb:{show['tmdb']}" for show in aliased)
        for show in aliased:
            saved = tmdb_notes.get(f"show:tmdb:{show['tmdb']}")
            if saved is not None:
                show["status"] = saved.get("status")
    kept = [show for show in shows if f"tmdb:{show.get('tmdb')}" not in hidden]
    return total, len(shows), [(show, watched.get(show["key"], set())) for show in kept]


def _latest(show: dict[str, Any], other: dict[str, Any]) -> None:
    if str(other.get("last_watched") or "") > str(show.get("last_watched") or ""):
        show.update({field: other.get(field) for field in ("last_watched", "last_season", "last_episode")})


def _merged(store: TrackerStore, cfg: Mapping[str, Any] | None) -> list[tuple[dict[str, Any], set[tuple[int, int]]]]:
    groups: dict[str, tuple[dict[str, Any], set[tuple[int, int]]]] = {}
    offset = 0
    while True:
        total, size, batch = _inputs(store, offset, cfg)
        for show, watched in batch:
            tmdb = show.get("tmdb") or (show["key"][5:] if show["key"].startswith("tmdb:") else "")
            group = f"tmdb:{tmdb}" if tmdb else show["key"]
            kept = groups.get(group)
            if kept is None:
                groups[group] = (show, set(watched))
                continue
            if show["key"].startswith("tmdb:") and not kept[0]["key"].startswith("tmdb:"):
                _latest(show, kept[0])
                groups[group] = (show, kept[1] | watched)
            else:
                _latest(kept[0], show)
                kept[1].update(watched)
        offset += size
        if not size or offset >= total:
            return list(groups.values())


def overview_payload(store: TrackerStore, provider: Any, today: date, cfg: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return episode_payload(_merged(store, cfg), provider, today)
