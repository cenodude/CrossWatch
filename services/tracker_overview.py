# services/tracker_overview.py
# CrossWatch - Episode suggestions and upcoming dates for the tracker overview
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from threading import Event
from typing import Any

from cw_platform.tracker_store import TrackerStore


_WORKERS = ThreadPoolExecutor(max_workers=4, thread_name_prefix="tracker-overview")
_AIRING_MAX_AGE = 24 * 60 * 60


def _ongoing(detail: dict[str, Any]) -> bool:
    return str(detail.get("status") or "").casefold() not in {"ended", "canceled", "cancelled"} and detail.get("in_production") is not False


def _airing_lookup(provider: Any, tmdb: str, cached: Any, result: dict[str, Any], *, season: int | None = None) -> dict[str, Any]:
    fetched_at = getattr(cached, "fetched_at", 0)
    if fetched_at > 0 and fetched_at >= time.time() - _AIRING_MAX_AGE:
        return cached.data
    try:
        return provider.lookup("show", tmdb, season=season, max_age=_AIRING_MAX_AGE, timeout=5, max_retries=0).data
    except Exception:
        result["unavailable"] = 1
        return cached.data


def _inputs(store: TrackerStore, offset: int) -> tuple[int, list[tuple[dict[str, Any], set[tuple[int, int]]]]]:
    with store.session():
        hidden = {key[5:] for key in store.status_keys("dropped") if key.startswith("show:")}
        total, shows = store.show_summaries(offset=offset, limit=32, exclude=hidden)
        notes = store.notes_for(f"show:{show['key']}" for show in shows)
        watched = store.watched_episodes(show["key"] for show in shows)
        for show in shows:
            show["status"] = (notes.get(f"show:{show['key']}") or {}).get("status")
    return total, [(show, watched.get(show["key"], set())) for show in shows]


async def overview_events(store: TrackerStore, provider: Any, today: date, disconnected: Any):
    cancelled = Event()
    offset = 0
    pending: dict[str, Any] = {"continue": [], "airing": [], "unavailable": 0}
    flushed = 0.0
    loop = asyncio.get_running_loop()
    try:
        while not await disconnected():
            total, inputs = await asyncio.to_thread(_inputs, store, offset)
            if await disconnected():
                return
            if not inputs:
                yield json.dumps({**pending, "total": total, "checked": offset, "done": True}) + "\n"
                return
            for start in range(0, len(inputs), 4):
                if await disconnected():
                    return
                batch = inputs[start:start + 4]
                results = await asyncio.gather(*(loop.run_in_executor(_WORKERS, show_overview, show, watched, provider, today, cancelled) for show, watched in batch))
                if await disconnected():
                    return
                offset += len(batch)
                for result in results:
                    pending["continue"].extend(result["continue"])
                    pending["airing"].extend(result["airing"])
                    pending["unavailable"] += result["unavailable"]
                done = offset >= total
                if done or time.monotonic() - flushed >= .15:
                    yield json.dumps({**pending, "total": total, "checked": offset, "done": done}) + "\n"
                    pending = {"continue": [], "airing": [], "unavailable": 0}
                    flushed = time.monotonic()
                if done:
                    return
    except asyncio.CancelledError:
        raise
    except Exception:
        yield json.dumps({"error": "overview_unavailable", "done": True}) + "\n"
    finally:
        cancelled.set()


def show_overview(show: dict[str, Any], watched: set[tuple[int, int]], provider: Any, today: date, cancelled: Event | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {"continue": [], "airing": [], "unavailable": 0}
    if cancelled is not None and cancelled.is_set():
        return result
    tmdb = show["key"].removeprefix("tmdb:")
    if not show["key"].startswith("tmdb:") or not tmdb.isascii() or not tmdb.isdecimal():
        result["unavailable"] = 1
        return result
    try:
        cached = provider.lookup("show", tmdb, timeout=5, max_retries=0)
        detail = cached.data
        if cancelled is not None and cancelled.is_set():
            return result
        if _ongoing(detail):
            detail = _airing_lookup(provider, tmdb, cached, result)
        seasons = {int(row["season_number"]): int(row.get("episode_count") or 0) for row in detail.get("seasons", []) if row.get("season_number", 0) > 0}
    except Exception:
        result["unavailable"] = 1
        return result
    latest = max((pair for pair in watched if 0 < pair[1] <= seasons.get(pair[0], 0)), default=(0, 0))
    nxt = next(((s, e) for s in sorted(seasons) for e in range(1, seasons[s] + 1) if (s, e) > latest), None)
    end = today + timedelta(days=29)
    wanted = {nxt[0]} if nxt and show.get("status") != "completed" else set()
    airing = set()
    ongoing = _ongoing(detail)
    latest_season = max(seasons, default=0)
    for key in ("next_episode_to_air", "last_episode_to_air"):
        row = detail.get(key) or {}
        aired = str(row.get("air_date") or "")
        if aired and not today.isoformat() <= aired <= end.isoformat():
            continue
        if isinstance(row.get("season_number"), int) and row["season_number"] > 0:
            wanted.add(int(row["season_number"]))
            airing.add(int(row["season_number"]))
    for row in detail.get("seasons", []):
        if today.isoformat() <= str(row.get("air_date") or "") <= end.isoformat() and isinstance(row.get("season_number"), int) and row["season_number"] > 0:
            wanted.add(int(row["season_number"]))
            airing.add(int(row["season_number"]))
        if ongoing and latest_season > 0 and row.get("season_number") == latest_season and str(row.get("air_date") or "") <= end.isoformat():
            wanted.add(int(row["season_number"]))
            airing.add(int(row["season_number"]))
    for season in sorted(wanted):
        if cancelled is not None and cancelled.is_set():
            return result
        try:
            cached = provider.lookup("show", tmdb, season=season, timeout=5, max_retries=0)
            data = cached.data
            if cancelled is not None and cancelled.is_set():
                return result
            if ongoing and season in airing:
                data = _airing_lookup(provider, tmdb, cached, result, season=season)
        except Exception:
            result["unavailable"] = 1
            continue
        for episode in data.get("episodes", []):
            number = episode.get("episode_number")
            try:
                aired = date.fromisoformat(str(episode.get("air_date") or ""))
            except ValueError:
                continue
            if not isinstance(number, int) or number < 1:
                continue
            row = {
                "key": f"{show['key']}:{season}:{number}", "ref": {"show": show["key"]},
                "type": "episode", "title": show.get("title") or detail.get("name") or "Untitled show",
                "year": show.get("year"), "season": season, "episode": number,
                "episode_title": episode.get("name") or "", "air_date": aired.isoformat(),
                "date": show.get("last_watched"), "show_ids": {"tmdb": tmdb},
                "art": {"kind": "tv", "tmdb": tmdb}, "has_still": bool(episode.get("still_path")),
            }
            if (season, number) == nxt and aired <= today and show.get("status") != "completed":
                result["continue"].append(row)
            if today <= aired <= end and (season, number) not in watched:
                result["airing"].append(row)
    return result
