# services/interactive_sync_episodes.py
# CrossWatch - Episode identity suggestions for Interactive Sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
import time
from urllib.parse import quote

import requests
from fastapi import HTTPException

from services.interactive_sync_catalogs import configured, provider_block


def metadata_key(cfg, row):
    provider = str(row["provider"]).upper()
    block = provider_block(cfg, provider, row.get("instance") or "default")
    if not configured(provider, block):
        return None
    return block.get("api_key") if provider == "TMDB" else (cfg.get("tmdb") or {}).get("api_key")


def _number(value, minimum=0):
    if isinstance(value, bool) or not re.fullmatch(r"\d+", str(value)):
        return None
    number = int(value)
    return number if number >= minimum else None


def _title(value):
    return re.sub(r"\W+", "", str(value or "")).casefold()


def match_episode(source, episodes):
    name = _title(source.get("name"))
    if not name or re.fullmatch(r"(?:episode\d+|s\d+e\d+)", name):
        return None
    matches = [ep for ep in episodes if _title(ep.get("name")) == name
               and (not source.get("air_date") or not ep.get("air_date") or source["air_date"] == ep["air_date"])]
    return matches[0] if len(matches) == 1 else None


def suggest_episodes(cfg, edits):
    cache = {}
    requests_left = 60
    deadline = time.monotonic() + 45
    results = []
    with requests.Session() as client:
        def get(key, path, **params):
            nonlocal requests_left
            identity = (key, path, tuple(sorted(params.items())))
            if identity not in cache:
                if requests_left <= 0 or time.monotonic() >= deadline:
                    raise HTTPException(409, "Episode lookup limit reached. Try a smaller batch.")
                requests_left -= 1
                response = client.get(f"https://api.themoviedb.org/3/{path}",
                                      params={"api_key": key, **params}, timeout=8)
                if response.status_code == 404:
                    body = {}
                elif response.status_code >= 400:
                    raise HTTPException(502, f"TMDb episode lookup returned HTTP {response.status_code}.")
                else:
                    body = response.json()
                if not isinstance(body, dict):
                    raise HTTPException(502, "TMDb returned an unreadable episode response.")
                cache[identity] = body
            return cache[identity]

        for row, draft in edits:
            result: dict[str, object] = dict(row_id=row["id"], match=None, reason="No reliable episode match found. Choose the show and adjust numbering manually.")
            results.append(result)
            key = metadata_key(cfg, row)
            if not key:
                result["reason"] = "Configure the destination and a TMDb metadata key to match episodes."
                continue
            original = row["item"]
            if original.get("type") != "episode":
                result["reason"] = "Season and episode suggestions apply to episodes only."
                continue
            try:
                identities = {}
                for namespace in ("tvdb", "imdb"):
                    identifier = (original.get("ids") or {}).get(namespace)
                    if not identifier:
                        continue
                    if str(identifier) == str((original.get("show_ids") or {}).get(namespace)):
                        continue
                    body = get(key, f"find/{quote(str(identifier), safe='')}", external_source=f"{namespace}_id")
                    for ep in body.get("tv_episode_results") or []:
                        if not isinstance(ep, dict):
                            continue
                        identity = (_number(ep.get("show_id"), 1), _number(ep.get("season_number")), _number(ep.get("episode_number"), 1))
                        if all(value is not None for value in identity):
                            identities[identity] = ep
                if len(identities) > 1:
                    result["reason"] = "Episode IDs disagree. Review the match manually."
                    continue
                reason = "Matched the original episode ID in TMDb."
                if identities:
                    show_id, season, episode = next(iter(identities))
                else:
                    show_id = _number((draft.get("show_ids") or draft.get("ids") or {}).get("tmdb"), 1)
                    source = {"name": original.get("episode_title"), "air_date": original.get("air_date")}
                    source_show = _number((original.get("show_ids") or {}).get("tmdb"), 1)
                    source_season = _number(original.get("season"))
                    source_episode = _number(original.get("episode"), 1)
                    if not source.get("name") and source_show and source_season is not None and source_episode:
                        source = get(key, f"tv/{source_show}/season/{source_season}/episode/{source_episode}")
                    if not show_id or not source.get("name"):
                        continue
                    show = get(key, f"tv/{show_id}")
                    seasons = [s for s in show.get("seasons") or [] if isinstance(s, dict) and _number(s.get("season_number")) is not None]
                    if len(seasons) > 30:
                        result["reason"] = "This show has too many seasons for automatic comparison. Use an exact episode ID or adjust manually."
                        continue
                    episodes = []
                    for season_info in seasons:
                        body = get(key, f"tv/{show_id}/season/{season_info['season_number']}")
                        if "episodes" not in body:
                            raise HTTPException(502, "Could not read every season. No episode number was guessed.")
                        episodes.extend(ep for ep in body["episodes"] if isinstance(ep, dict))
                    match = match_episode(source, episodes)
                    if not match:
                        continue
                    season, episode = _number(match.get("season_number")), _number(match.get("episode_number"), 1)
                    if season is None or episode is None:
                        continue
                    reason = "Matched a unique episode title and compatible air date in TMDb."
                show = get(key, f"tv/{show_id}")
                if not show.get("name"):
                    result["reason"] = "Episode found, but show metadata is unavailable. Try again."
                    continue
                result.update(match=dict(ids={"tmdb": str(show_id)}, title=show["name"], season=season, episode=episode), reason=reason)
            except HTTPException as error:
                result["reason"] = str(error.detail)
            except Exception:
                result["reason"] = "Episode lookup is unavailable. Try again; your draft is unchanged."
    return dict(ok=True, results=results, note="Suggestions use TMDb episode numbering. Review the drafts before saving.")
