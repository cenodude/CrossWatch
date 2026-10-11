# services/catalog_lookup.py
# CrossWatch - Read-only season and episode layouts from connected providers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from urllib.parse import quote

import requests

from services.interactive_sync_catalogs import configured, provider_block

TIMEOUT = 10
MAX_SEASONS = 12
ID_SOURCES = ("tmdb", "imdb", "tvdb")
LABELS = {"TRAKT": "Trakt", "SIMKL": "SIMKL", "WETRAKR": "WeTrakr", "MDBLIST": "MDBList",
          "FLICKLIST": "FlickList", "TMDB": "TMDb"}


def _number(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _body(response):
    if getattr(response, "status_code", 0) != 200:
        return None
    try:
        return response.json()
    except Exception:
        return None


def _episode(number, title=None, ids=None, runtime=None):
    clean = {str(key): str(value) for key, value in (ids or {}).items() if key in ID_SOURCES and value not in (None, "")}
    return dict(episode=number, title=str(title or ""), ids=clean, runtime=runtime if isinstance(runtime, (int, float)) else None)


def _wanted(counts, seasons):
    known = [season for season in sorted(counts) if season in seasons]
    known += [season for season in sorted(counts) if season not in seasons and season > 0]
    return known[:MAX_SEASONS]


def _tmdb_shaped(get, show_ids, seasons):
    show = _number(show_ids.get("tmdb"))
    if not show:
        for source in ("imdb", "tvdb"):
            if show_ids.get(source):
                found = get(f"find/{quote(str(show_ids[source]), safe='')}", external_source=f"{source}_id") or {}
                rows = found.get("tv_results") if isinstance(found, dict) else None
                if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                    show = _number(rows[0].get("id"))
                    break
    if not show:
        return None
    detail = get(f"tv/{show}")
    if not isinstance(detail, dict) or not isinstance(detail.get("seasons"), list):
        return None
    counts = {number: _number(row.get("episode_count")) or 0 for row in detail["seasons"] if isinstance(row, dict)
              for number in [_number(row.get("season_number"))] if number is not None}
    layout = {}
    for season in _wanted(counts, seasons):
        body = get(f"tv/{show}/season/{season}")
        rows = body.get("episodes") if isinstance(body, dict) else None
        if isinstance(rows, list):
            layout[season] = [_episode(number, row.get("name"), {"tmdb": row.get("id")}, row.get("runtime"))
                              for row in rows if isinstance(row, dict)
                              for number in [_number(row.get("episode_number"))] if number]
    return dict(title=str(detail.get("name") or ""), counts=counts, seasons=layout)


def _tmdb(client, cfg, instance, show_ids, seasons):
    key = str((cfg.get("tmdb") or {}).get("api_key") or "").strip()
    if not key:
        return None
    return _tmdb_shaped(lambda path, **params: _body(client.get(
        f"https://api.themoviedb.org/3/{path}", params={"api_key": key, **params}, timeout=TIMEOUT)), show_ids, seasons)


def _flicklist(client, cfg, instance, show_ids, seasons):
    from providers.auth._auth_FLICKLIST import API_V3_BASE, request_with_auth

    return _tmdb_shaped(lambda path, **params: _body(request_with_auth(
        client, "GET", f"{API_V3_BASE}/{path}", cfg=cfg, instance_id=instance, timeout=TIMEOUT, max_retries=0,
        params=params)), show_ids, seasons)


def _trakt(client, cfg, instance, show_ids, seasons):
    from cw_platform.trakt_http import paced_request
    from services.interactive_sync_mapping import _trakt_headers

    block = provider_block(cfg, "TRAKT", instance)
    key = block.get("client_id")
    if not key:
        return None
    headers = _trakt_headers(key, block, instance)
    get = lambda path, **params: _body(paced_request(client.get, "GET", f"https://api.trakt.tv/{path}",
                                                     params=params, headers=headers, timeout=TIMEOUT))
    show, title = show_ids.get("trakt"), ""
    if not show:
        for source in ID_SOURCES:
            if show_ids.get(source):
                rows = get(f"search/{source}/{quote(str(show_ids[source]), safe='')}", type="show")
                hit = next((row.get("show") for row in rows or [] if isinstance(row, dict) and isinstance(row.get("show"), dict)), None)
                if hit:
                    show, title = (hit.get("ids") or {}).get("trakt"), str(hit.get("title") or "")
                    break
    if not show:
        return None
    rows = get(f"shows/{quote(str(show), safe='')}/seasons", extended="episodes")
    if not isinstance(rows, list):
        return None
    layout = {}
    for row in rows:
        season = _number(row.get("number")) if isinstance(row, dict) else None
        if season is None:
            continue
        layout[season] = [_episode(number, ep.get("title"), ep.get("ids")) for ep in row.get("episodes") or []
                          if isinstance(ep, dict) for number in [_number(ep.get("number"))] if number]
    return dict(title=title, counts={season: len(eps) for season, eps in layout.items()}, seasons=layout)


def _simkl(client, cfg, instance, show_ids, seasons):
    from cw_platform.simkl_http import pace_session
    from providers.sync.simkl._common import simkl_api_params
    from services.interactive_sync_mapping import _simkl_headers

    block = provider_block(cfg, "SIMKL", instance)
    key = block.get("api_key") or block.get("client_id")
    if not key:
        return None
    pace_session(client)
    headers = _simkl_headers(block, instance)
    get = lambda path, **params: _body(client.get(f"https://api.simkl.com/{path}", params=simkl_api_params(key, **params),
                                                  headers=headers, timeout=TIMEOUT))
    show, title = show_ids.get("simkl"), ""
    if not show:
        for source in ID_SOURCES:
            if show_ids.get(source):
                rows = get("search/id", **{source: show_ids[source]})
                hit = next((row for row in rows or [] if isinstance(row, dict) and row.get("type") == "tv"), None)
                if hit:
                    show, title = (hit.get("ids") or {}).get("simkl"), str(hit.get("title") or "")
                    break
    if not show:
        return None
    rows = get(f"tv/episodes/{quote(str(show), safe='')}", extended="full")
    if not isinstance(rows, list):
        return None
    layout = {}
    for row in rows:
        if not isinstance(row, dict) or row.get("type") not in (None, "episode"):
            continue
        season, number = _number(row.get("season")), _number(row.get("episode"))
        if season is not None and number:
            layout.setdefault(season, []).append(_episode(number, row.get("title")))
    return dict(title=title, counts={season: len(eps) for season, eps in layout.items()}, seasons=layout)


def _wetrakr(client, cfg, instance, show_ids, seasons):
    from providers.auth._auth_WETRAKR import API_BASE, request_with_auth

    get = lambda path, **params: _body(request_with_auth(client, "GET", f"{API_BASE}{path}", cfg=cfg, instance_id=instance,
                                                         timeout=TIMEOUT, max_retries=0, params=params))
    show = None
    for source in ID_SOURCES:
        if show_ids.get(source):
            found = get(f"/media/external/{source}/{quote(str(show_ids[source]), safe='')}", type="show")
            if isinstance(found, dict) and found.get("type") == "show" and _number(found.get("id")):
                show = found["id"]
                break
    if not show:
        return None
    detail = get(f"/shows/{show}")
    if not isinstance(detail, dict) or detail.get("type") != "show":
        return None
    total = _number(detail.get("number_of_seasons")) or 0
    layout = {}
    for season in _wanted({number: 0 for number in range(1, total + 1)}, seasons):
        node = get(f"/shows/{show}/seasons/{season}")
        if not isinstance(node, dict) or not _number(node.get("id")):
            continue
        rows = get(f"/seasons/{node['id']}/episodes")
        if isinstance(rows, list):
            layout[season] = [_episode(number, row.get("title"), row.get("ids")) for row in rows
                              if isinstance(row, dict) for number in [_number(row.get("number"))] if number]
    counts = {season: len(eps) for season, eps in layout.items()}
    counts.update({number: counts.get(number, 0) for number in range(1, total + 1)})
    return dict(title=str(detail.get("title") or ""), counts=counts, seasons=layout)


def _mdblist(client, cfg, instance, show_ids, seasons):
    from providers.sync.mdblist._auth import request_with_auth

    for source in ID_SOURCES:
        if not show_ids.get(source):
            continue
        body = _body(request_with_auth(client, "GET", f"https://api.mdblist.com/{source}/show/{quote(str(show_ids[source]), safe='')}",
                                       cfg=cfg, instance_id=instance, timeout=TIMEOUT, max_retries=0))
        if isinstance(body, dict) and isinstance(body.get("seasons"), list):
            counts = {number: _number(row.get("episode_count")) or 0 for row in body["seasons"] if isinstance(row, dict)
                      for number in [_number(row.get("season_number"))] if number is not None}
            return dict(title=str(body.get("title") or ""), counts=counts, seasons={})
    return None


def _film(title, year, ids):
    clean = {str(key): str(value) for key, value in (ids or {}).items()
             if key in ("imdb", "tmdb", "tvdb", "trakt", "simkl", "mdblist") and value not in (None, "")}
    return dict(title=str(title or ""), year=_number(str(year or "")[:4]), ids=clean) if clean else None


def _tmdb_shaped_movie(get, ids):
    movie = _number(ids.get("tmdb"))
    if not movie and ids.get("imdb"):
        rows = (get(f"find/{quote(str(ids['imdb']), safe='')}", external_source="imdb_id") or {}).get("movie_results")
        movie = _number(rows[0].get("id")) if isinstance(rows, list) and rows and isinstance(rows[0], dict) else None
    body = get(f"movie/{movie}") if movie else None
    if not isinstance(body, dict) or not body.get("id"):
        return None
    return _film(body.get("title"), body.get("release_date"), {"tmdb": body["id"], "imdb": body.get("imdb_id")})


def _tmdb_movie(client, cfg, instance, ids):
    key = str((cfg.get("tmdb") or {}).get("api_key") or "").strip()
    return _tmdb_shaped_movie(lambda path, **params: _body(client.get(
        f"https://api.themoviedb.org/3/{path}", params={"api_key": key, **params}, timeout=TIMEOUT)), ids) if key else None


def _flicklist_movie(client, cfg, instance, ids):
    from providers.auth._auth_FLICKLIST import API_V3_BASE, request_with_auth

    return _tmdb_shaped_movie(lambda path, **params: _body(request_with_auth(
        client, "GET", f"{API_V3_BASE}/{path}", cfg=cfg, instance_id=instance, timeout=TIMEOUT, max_retries=0,
        params=params)), ids)


def _trakt_movie(client, cfg, instance, ids):
    from cw_platform.trakt_http import paced_request
    from services.interactive_sync_mapping import _trakt_headers

    block = provider_block(cfg, "TRAKT", instance)
    if not block.get("client_id"):
        return None
    headers = _trakt_headers(block["client_id"], block, instance)
    for source in ("trakt", "imdb", "tmdb"):
        if not ids.get(source):
            continue
        rows = _body(paced_request(client.get, "GET", f"https://api.trakt.tv/search/{source}/{quote(str(ids[source]), safe='')}",
                                   params={"type": "movie"}, headers=headers, timeout=TIMEOUT))
        hit = next((row["movie"] for row in rows or [] if isinstance(row, dict) and isinstance(row.get("movie"), dict)), None)
        if hit:
            return _film(hit.get("title"), hit.get("year"), hit.get("ids"))
    return None


def _simkl_movie(client, cfg, instance, ids):
    from cw_platform.simkl_http import pace_session
    from providers.sync.simkl._common import simkl_api_params
    from services.interactive_sync_mapping import _simkl_headers

    block = provider_block(cfg, "SIMKL", instance)
    key = block.get("api_key") or block.get("client_id")
    if not key:
        return None
    pace_session(client)
    headers = _simkl_headers(block, instance)
    get = lambda path, **params: _body(client.get(f"https://api.simkl.com/{path}", params=simkl_api_params(key, **params),
                                                  headers=headers, timeout=TIMEOUT))
    found = ids.get("simkl")
    if not found:
        for source in ("imdb", "tmdb"):
            if ids.get(source):
                rows = get("search/id", **{source: ids[source]})
                hit = next((row for row in rows or [] if isinstance(row, dict) and row.get("type") == "movie"), None)
                if hit:
                    found = (hit.get("ids") or {}).get("simkl")
                    break
    body = get(f"movies/{quote(str(found), safe='')}", extended="full") if found else None
    return _film(body.get("title"), body.get("year"), body.get("ids")) if isinstance(body, dict) else None


def _wetrakr_movie(client, cfg, instance, ids):
    from providers.auth._auth_WETRAKR import API_BASE, request_with_auth

    get = lambda path, **params: _body(request_with_auth(client, "GET", f"{API_BASE}{path}", cfg=cfg, instance_id=instance,
                                                         timeout=TIMEOUT, max_retries=0, params=params))
    for source in ("imdb", "tmdb"):
        if not ids.get(source):
            continue
        found = get(f"/media/external/{source}/{quote(str(ids[source]), safe='')}", type="movie")
        if isinstance(found, dict) and found.get("type") == "movie" and _number(found.get("id")):
            body = get(f"/movies/{found['id']}")
            if isinstance(body, dict) and body.get("type") == "movie":
                return _film(body.get("title"), body.get("release_date"), body.get("ids"))
    return None


def _mdblist_movie(client, cfg, instance, ids):
    from providers.sync.mdblist._auth import request_with_auth

    for source in ("imdb", "tmdb", "trakt"):
        if not ids.get(source):
            continue
        body = _body(request_with_auth(client, "GET", f"https://api.mdblist.com/{source}/movie/{quote(str(ids[source]), safe='')}",
                                       cfg=cfg, instance_id=instance, timeout=TIMEOUT, max_retries=0))
        if isinstance(body, dict) and isinstance(body.get("ids"), dict) and body.get("title"):
            return _film(body.get("title"), body.get("year"), body["ids"])
    return None


FETCHERS = {"TMDB": _tmdb, "FLICKLIST": _flicklist, "TRAKT": _trakt, "SIMKL": _simkl, "WETRAKR": _wetrakr, "MDBLIST": _mdblist}
MOVIES = {"TMDB": _tmdb_movie, "FLICKLIST": _flicklist_movie, "TRAKT": _trakt_movie, "SIMKL": _simkl_movie,
          "WETRAKR": _wetrakr_movie, "MDBLIST": _mdblist_movie}


def available(cfg, provider, instance="default"):
    provider = str(provider).upper()
    if provider == "TMDB":
        return bool(str((cfg.get("tmdb") or {}).get("api_key") or "").strip())
    return provider in FETCHERS and configured(provider, provider_block(cfg, provider, instance))


def show_layout(cfg, provider, instance, show_ids, seasons=()):
    provider = str(provider).upper()
    if not available(cfg, provider, instance):
        return None
    ids = {str(key).lower(): str(value) for key, value in (show_ids or {}).items() if value not in (None, "")}
    try:
        with requests.Session() as client:
            layout = FETCHERS[provider](client, cfg, instance or "default", ids, {int(season) for season in seasons})
    except Exception:
        return None
    if not layout or not layout.get("counts"):
        return None
    return dict(provider=provider, label=LABELS[provider], **layout)


def movie_by_id(cfg, provider, instance, ids):
    provider = str(provider).upper()
    if not available(cfg, provider, instance):
        return "unsupported"
    clean = {str(key).lower(): str(value) for key, value in (ids or {}).items() if value not in (None, "")}
    try:
        with requests.Session() as client:
            return MOVIES[provider](client, cfg, instance or "default", clean)
    except Exception:
        return "unsupported"
