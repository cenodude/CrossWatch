# services/interactive_sync_mapping.py
# CrossWatch - Destination catalog suggestions for interactive mapping
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re

import requests
from fastapi import HTTPException

from cw_platform.id_map import coalesce_ids, ID_KEYS
from services.interactive_sync_catalogs import destination_rows, provider_block, search_catalogs


def _candidate_ids(item, source):
    raw = item.get("ids")
    ids = dict(raw) if isinstance(raw, dict) else {}
    if ids.get("simkl_id"):
        ids["simkl"] = ids["simkl_id"]
    for key in ID_KEYS:
        value = item.get(key) or item.get(key + "id") or item.get(key + "_id")
        if value:
            ids[key] = value
    if source == "TMDB":
        ids["tmdb"] = item.get("id")
    elif source == "MDBLIST" and str(item.get("id") or "").startswith("tt"):
        ids["imdb"] = item["id"]
    return coalesce_ids({key: value for key, value in ids.items() if key in ID_KEYS})


def _mdblist_metadata(client, cfg, instance, entity, body):
    from providers.sync.mdblist._auth import request_with_auth

    rows = body if isinstance(body, list) else (body.get("search") or body.get("results") or []) if isinstance(body, dict) else []
    if not isinstance(rows, list):
        return body
    rows = [dict(item) for item in rows[:20] if isinstance(item, dict)]
    groups = {}
    for item in rows:
        ids = _candidate_ids(item, "MDBLIST")
        if ids.get("tmdb") and ids.get("imdb"):
            continue
        namespace = next((key for key in ("mdblist", "imdb", "tmdb") if ids.get(key)), None)
        if namespace:
            groups.setdefault(namespace, {}).setdefault(ids[namespace], []).append(item)
    for namespace, targets in groups.items():
        try:
            response = request_with_auth(client, "POST", f"https://api.mdblist.com/{namespace}/{entity}/",
                                         cfg=cfg, instance_id=instance, timeout=10, max_retries=0,
                                         json={"ids": list(targets)})
            if response.status_code >= 400:
                continue
            details = response.json()
            if not isinstance(details, list):
                continue
            by_id = {}
            for detail in details:
                if not isinstance(detail, dict):
                    continue
                ids = _candidate_ids(detail, "MDBLIST")
                if ids.get(namespace) in targets:
                    by_id.setdefault(ids[namespace], []).append(ids)
            for identity, items in targets.items():
                matches = by_id.get(identity, [])
                if len(matches) != 1:
                    continue
                resolved = matches[0]
                for item in items:
                    original = _candidate_ids(item, "MDBLIST")
                    if any(original[key] != resolved[key] for key in original.keys() & resolved.keys()):
                        continue
                    item["ids"] = {**original, **resolved}
        except Exception:
            continue
    return rows


def search_candidates(cfg, row, query, *, catalog="destination"):
    provider = str(row["provider"]).upper()
    instance = row.get("instance") or "default"
    entity = "movie" if row["item"].get("type") == "movie" else "show"
    catalogs = search_catalogs(cfg, row)["catalogs"]
    if not isinstance(catalogs, list) or catalog not in {option["id"] for option in catalogs}:
        raise HTTPException(409, "Search is not available for this configured destination and catalog.")
    source = provider if catalog == "destination" else "TMDB"
    block = provider_block(cfg, provider, instance)
    extra = []
    try:
        with requests.Session() as client:
            if source == "MDBLIST":
                from providers.sync.mdblist._auth import request_with_auth

                response = request_with_auth(client, "GET", f"https://api.mdblist.com/search/{entity}",
                                             cfg=cfg, instance_id=instance, timeout=10, max_retries=0,
                                             params={"query": query, "limit": 20})
            elif source == "SIMKL":
                key = block.get("api_key") or block.get("client_id")
                if not key:
                    raise HTTPException(409, "Configure SIMKL search credentials for this destination instance.")
                response = client.get(f"https://api.simkl.com/search/{'movie' if entity == 'movie' else 'tv'}",
                                      params={"q": query, "client_id": key, "limit": 20}, timeout=10)
                if entity == "show":
                    anime = client.get("https://api.simkl.com/search/anime", params={"q": query, "client_id": key, "limit": 20}, timeout=10)
                    if anime.status_code >= 400:
                        raise HTTPException(502, f"SIMKL anime search returned HTTP {anime.status_code}. Try again.")
                    extra = anime.json()
            elif source == "TRAKT":
                key = block.get("client_id")
                if not key:
                    raise HTTPException(409, "Configure Trakt search credentials for this destination instance.")
                response = client.get(f"https://api.trakt.tv/search/{entity}", params={"query": query, "limit": 20},
                                      headers={"trakt-api-key": key, "trakt-api-version": "2"}, timeout=10)
            elif source == "TMDB":
                key = block.get("api_key") if catalog == "destination" else (cfg.get("tmdb") or {}).get("api_key")
                if not key:
                    raise HTTPException(409, "Add a TMDb API key in metadata settings to search this catalog.")
                response = client.get(f"https://api.themoviedb.org/3/search/{'movie' if entity == 'movie' else 'tv'}",
                                      params={"api_key": key, "query": query, "include_adult": False}, timeout=10)
            else:
                body = destination_rows(client, cfg, row, block, query, entity)
                response = None
            if response is not None and response.status_code >= 400:
                raise HTTPException(502, f"{source} search returned HTTP {response.status_code}. Try again or use manual IDs.")
            if response is not None:
                body = response.json()
            if source == "MDBLIST":
                body = _mdblist_metadata(client, cfg, instance, entity, body)
    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(502, f"{source} search is unavailable. Try again or use manual IDs.") from error
    rows = body if isinstance(body, list) else (body.get("search") or body.get("results") or []) if isinstance(body, dict) else []
    if not isinstance(rows, list):
        raise HTTPException(502, f"{source} returned an unreadable search response.")
    rows = rows[:20] + (extra[:20] if isinstance(extra, list) else [])
    results = []
    seen = set()
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        item = raw.get(entity)
        if not isinstance(item, dict):
            item = raw
        if source == "SIMKL" and entity == "show" and item.get("endpoint_type") == "anime" and item.get("type") == "movie":
            continue
        ids = _candidate_ids(item, source)
        title = str(item.get("title_en") or item.get("title") or item.get("name") or "")[:300]
        identity = tuple(sorted(ids.items()))
        if not ids or not title or identity in seen:
            continue
        seen.add(identity)
        year = str(item.get("year") or item.get("release_date") or item.get("first_air_date") or "")[:4]
        exact = re.sub(r"\W+", "", title).casefold() == re.sub(r"\W+", "", query).casefold()
        results.append(dict(title=title, year=int(year) if year.isdigit() else None, ids=ids,
                            type=entity, exact_title=exact))
        if source == "MDBLIST" and not ids.get("tmdb"):
            results[-1]["mapping_unavailable"] = "TMDb ID could not be retrieved. Try searching again or enter a verified TMDb ID manually."
    results.sort(key=lambda item: not item["exact_title"])
    return dict(ok=True, catalog=source, destination=provider, results=results[:20],
                note="Review the match and episode numbering before saving."
                if catalog == "destination" else "TMDb metadata suggestions; destination availability has not been checked.")
