# services/episode_overview.py
# CrossWatch - Shared episode suggestions and upcoming dates for Hub and Tracker
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from itertools import islice
from typing import Any, Callable, Iterable, Iterator, TypeVar


_WORKERS = ThreadPoolExecutor(max_workers=4, thread_name_prefix="episode-overview")
_AIRING_MAX_AGE = 24 * 60 * 60
_T = TypeVar("_T")
_R = TypeVar("_R")


def map_episodes(function: Callable[[_T], _R], inputs: Iterable[_T]) -> Iterator[_R]:
    iterator = iter(inputs)
    while batch := list(islice(iterator, 4)):
        yield from _WORKERS.map(function, batch)


def episode_payload(inputs: Iterable[tuple[dict[str, Any], set[tuple[int, int]]]], provider: Any, today: date) -> dict[str, Any]:
    payload: dict[str, Any] = {"ok": True, "day": today.isoformat(), "continue": [], "airing": [], "unavailable": 0,
                               "total": 0, "checked": 0, "done": True, "metadata_available": True}
    failed = 0
    for result in map_episodes(lambda pair: show_overview(pair[0], pair[1], provider, today), inputs):
        payload["continue"].extend(result["continue"])
        payload["airing"].extend(result["airing"])
        payload["unavailable"] += result["unavailable"]
        failed += bool(result.get("failed"))
        payload["total"] += 1
    if failed:
        payload.update(ok=False, error="episode_metadata_unavailable")
    payload["checked"] = payload["total"]
    payload["airing"].sort(key=lambda row: (row["air_date"], row["title"], row["season"], row["episode"]))
    return payload


def _ongoing(detail: dict[str, Any]) -> bool:
    return str(detail.get("status") or "").casefold() not in {"ended", "canceled", "cancelled"} and detail.get("in_production") is not False


def _not_found(error: Exception) -> bool:
    return getattr(getattr(error, "response", None), "status_code", None) == 404


def _airing_lookup(provider: Any, tmdb: str, cached: Any, result: dict[str, Any], *, season: int | None = None) -> dict[str, Any]:
    fetched_at = getattr(cached, "fetched_at", 0)
    if fetched_at > 0 and fetched_at >= time.time() - _AIRING_MAX_AGE:
        return cached.data
    try:
        return provider.lookup("show", tmdb, season=season, max_age=_AIRING_MAX_AGE, timeout=5, max_retries=0).data
    except Exception as error:
        if _not_found(error):
            raise
        result["unavailable"] = 1
        return cached.data


def show_overview(show: dict[str, Any], watched: set[tuple[int, int]], provider: Any, today: date) -> dict[str, Any]:
    result: dict[str, Any] = {"continue": [], "airing": [], "unavailable": 0}
    tmdb = show["key"].removeprefix("tmdb:")
    if not show["key"].startswith("tmdb:") or not tmdb.isascii() or not tmdb.isdecimal() or int(tmdb) <= 0:
        result["unavailable"] = 1
        return result
    try:
        cached = provider.lookup("show", tmdb, timeout=5, max_retries=0)
        detail = cached.data
        if _ongoing(detail):
            detail = _airing_lookup(provider, tmdb, cached, result)
        seasons = {int(row["season_number"]): int(row.get("episode_count") or 0) for row in detail.get("seasons", []) if row.get("season_number", 0) > 0}
    except Exception as error:
        result.update(unavailable=1, failed=not _not_found(error))
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
    aired_left: dict[int, int] = {}
    for season in sorted(wanted):
        try:
            cached = provider.lookup("show", tmdb, season=season, timeout=5, max_retries=0)
            data = cached.data
            if ongoing and season in airing:
                data = _airing_lookup(provider, tmdb, cached, result, season=season)
        except Exception as error:
            result["unavailable"] = 1
            if not _not_found(error):
                result["failed"] = True
            continue
        episodes = data.get("episodes", [])
        numbers = {ep.get("episode_number") for ep in episodes if type(ep.get("episode_number")) is int and 1 <= ep["episode_number"] <= seasons.get(season, 0)}
        season_progress: dict[str, Any] = {"complete": False, "episodes": []}
        for ep in episodes:
            number = ep.get("episode_number")
            if type(number) is not int or number not in numbers:
                continue
            try:
                air_date = date.fromisoformat(str(ep.get("air_date") or ""))
            except ValueError:
                air_date = None
            status = "watched" if (season, number) in watched else "unknown" if air_date is None else "unwatched" if air_date <= today else "upcoming"
            runtime = ep.get("runtime")
            season_progress["episodes"].append({"episode": number, "title": ep.get("name") or "", "status": status,
                "air_date": air_date.isoformat() if air_date else None, "runtime": runtime if type(runtime) is int and runtime > 0 else None})
        season_progress["episodes"].sort(key=lambda ep: ep["episode"])
        season_progress["complete"] = bool(seasons.get(season, 0) > 0 and len(numbers) == seasons[season] and len(episodes) == len(numbers)
                                           and all(ep["air_date"] for ep in season_progress["episodes"]))
        if seasons.get(season, 0) > 0 and len(numbers) == seasons[season] and len(episodes) == len(numbers):
            aired_left[season] = 0
        for episode in episodes:
            number = episode.get("episode_number")
            try:
                aired = date.fromisoformat(str(episode.get("air_date") or ""))
            except ValueError:
                continue
            if type(number) is not int or number < 1:
                continue
            if season in aired_left and aired <= today and (season, number) not in watched:
                aired_left[season] += 1
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
                result["airing"].append({**row, "season_progress": season_progress})
    needed = {season for season, count in seasons.items() if any((season, number) not in watched for number in range(1, count + 1))}
    if seasons and all(count > 0 for count in seasons.values()) and needed <= aired_left.keys():
        remaining = sum(aired_left[season] for season in needed)
        for row in result["continue"]:
            if remaining > 0:
                row["aired_episodes_left"] = remaining
    return result
