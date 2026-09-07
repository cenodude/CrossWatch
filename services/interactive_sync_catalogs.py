# services/interactive_sync_catalogs.py
# CrossWatch - Configured mapping catalogs and destination searches
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from importlib import import_module
from urllib.parse import quote

from fastapi import HTTPException

from cw_platform.provider_instances import get_provider_block


PROVIDERS = frozenset(("PLEX", "JELLYFIN", "EMBY", "KODI", "SCROB", "TRAKT", "MDBLIST", "SIMKL",
                       "PUNCHPLAY", "FLICKLIST", "TMDB", "PUBLICMETADB", "NUVIO", "STREMIO", "FLOPPY"))
NATIVE = PROVIDERS - {"FLICKLIST", "PUBLICMETADB", "NUVIO", "STREMIO"}
LIBRARY = {"JELLYFIN", "EMBY", "KODI"}


def provider_block(cfg, provider, instance):
    key = "tmdb_sync" if provider == "TMDB" else provider.lower()
    block = get_provider_block(cfg, key, instance)
    block.pop("instances", None)
    if instance == "default" and provider in {"PLEX", "JELLYFIN", "EMBY", "TRAKT", "SIMKL"}:
        legacy = (cfg.get("auth") or {}).get(key) or {}
        block = {**legacy, **block}
    return block


def configured(provider, block):
    if provider not in PROVIDERS or not block:
        return False
    if provider in {"SCROB", "PUNCHPLAY", "FLICKLIST", "NUVIO", "STREMIO", "FLOPPY"}:
        return import_module(f"providers.auth._auth_{provider}").is_configured(block)
    if provider == "MDBLIST":
        from providers.sync.mdblist._auth import is_configured
        return is_configured(block)
    if provider in {"TRAKT", "SIMKL"}:
        token = block.get("access_token") or block.get("token") or (block.get("oauth") or {}).get("access_token")
        key = block.get("client_id") if provider == "TRAKT" else block.get("client_id") or block.get("api_key")
        return bool(str(token or "").strip() and str(key or "").strip())
    if provider in {"JELLYFIN", "EMBY"}:
        return all(block.get(key) for key in ("server", "access_token", "user_id"))
    if provider == "KODI":
        return bool(block.get("server") and block.get("connection_verified") is True)
    if provider == "PLEX":
        return bool(block.get("account_token") or block.get("token") or block.get("pms_token")
                    or (block.get("pms") or {}).get("token") or (block.get("pms") or {}).get("x_plex_token"))
    if provider == "TMDB":
        return bool(block.get("api_key") and block.get("session_id"))
    return bool(block.get("api_key"))


def search_catalogs(cfg, row):
    provider = str(row["provider"]).upper()
    block = provider_block(cfg, provider, row.get("instance") or "default")
    options = []
    if configured(provider, block):
        native = provider in NATIVE
        if provider == "PLEX":
            native = bool(block.get("account_token") or block.get("token"))
        if native:
            suffix = "library" if provider in LIBRARY else "catalog"
            options.append(dict(id="destination", label=f"{provider} {suffix}"))
        if (cfg.get("tmdb") or {}).get("api_key"):
            options.append(dict(id="tmdb", label="TMDb metadata"))
    return dict(ok=True, catalogs=options)


def destination_rows(client, cfg, row, block, query, entity):
    provider = str(row["provider"]).upper()
    instance = row.get("instance") or "default"
    if provider in {"JELLYFIN", "EMBY"}:
        response = client.get(f"{str(block['server']).rstrip('/')}/Users/{quote(str(block['user_id']), safe='')}/Items",
                              headers={"X-Emby-Token": block["access_token"], "Accept": "application/json"},
                              params={"SearchTerm": query, "IncludeItemTypes": "Movie" if entity == "movie" else "Series",
                                      "Recursive": "true", "Fields": "ProviderIds", "Limit": 20, "EnableImages": "false"},
                              timeout=10, verify=bool(block.get("verify_ssl", True)))
        body = checked_json(response, provider)
        return [dict(title=r.get("Name"), year=r.get("ProductionYear"),
                     ids={k.lower(): v for k, v in (r.get("ProviderIds") or {}).items()})
                for r in body.get("Items", []) if isinstance(r, dict)]
    if provider == "KODI":
        from providers.sync.kodi._common import KodiClient, make_config, normalize_uniqueids
        kodi = KodiClient(make_config({"kodi": {**block, "timeout": 10}}))
        kind = "Movies" if entity == "movie" else "TVShows"
        body = kodi.rpc(f"VideoLibrary.Get{kind}", {"properties": ["title", "year", "uniqueid"],
                        "filter": {"field": "title", "operator": "contains", "value": query},
                        "limits": {"start": 0, "end": 20}})
        return [dict(title=r.get("title") or r.get("label"), year=r.get("year"), ids=normalize_uniqueids(r.get("uniqueid")))
                for r in body.get(kind.lower(), []) if isinstance(r, dict)]
    if provider == "SCROB":
        from providers.auth._auth_SCROB import request_with_auth
        from providers.sync.scrob._common import url_for
        response = request_with_auth(client, "GET", url_for(block, "media/search"), cfg=cfg, instance_id=instance,
                                     params={"q": query, "type": "movie" if entity == "movie" else "series"}, timeout=10)
    elif provider == "FLOPPY":
        from providers.auth._auth_FLOPPY import FloppyClient
        body = FloppyClient(block["server_url"], block["api_token"], verify_ssl=bool(block.get("verify_ssl", False)),
                            timeout=10, session=client).request_json(f"search/{'movie' if entity == 'movie' else 'tv'}/",
                            params={"search": query, "source": "tmdb", "limit": 20})
        return [dict(r, ids={str(r.get("source")): r.get("media_id")}) for r in body.get("results", []) if isinstance(r, dict)]
    elif provider == "PUNCHPLAY":
        response = client.get("https://punchplay.tv/api/public/v1/catalog/search",
                              params={"q": query[:100], "type": entity}, timeout=10)
        body = checked_json(response, provider)
        return [dict(r, ids={"tmdb": r.get("tmdbId")}) for r in body.get("items", [])
                if isinstance(r, dict) and r.get("type") == entity]
    elif provider == "PLEX":
        response = client.get("https://discover.provider.plex.tv/library/search",
                              headers={"X-Plex-Token": block.get("account_token") or block.get("token"),
                                       "Accept": "application/json", "X-Plex-Product": "CrossWatch",
                                       "X-Plex-Client-Identifier": block.get("client_id") or "crosswatch-mapping"},
                              params={"query": query, "limit": 20, "searchProviders": "discover",
                                      "searchTypes": "movies" if entity == "movie" else "shows", "includeMetadata": 1}, timeout=10)
        body = checked_json(response, provider)
        found = []
        def collect(value):
            if isinstance(value, list):
                for child in value:
                    collect(child)
            elif isinstance(value, dict):
                if value.get("type") == entity and value.get("title"):
                    ids = {}
                    for guid in value.get("Guid") or []:
                        if isinstance(guid, dict) and "://" in str(guid.get("id")):
                            key, identifier = guid["id"].split("://", 1)
                            ids[key] = identifier
                    found.append(dict(title=value["title"], year=value.get("year"), ids=ids))
                for key in ("MediaContainer", "SearchResults", "SearchResult", "Metadata", "Hub"):
                    if key in value:
                        collect(value[key])
        collect(body)
        return found
    else:
        raise HTTPException(409, "This destination does not expose a supported search catalog.")
    body = checked_json(response, provider)
    return body.get("results", [])


def checked_json(response, provider):
    if response.status_code >= 400:
        raise HTTPException(502, f"{provider} search returned HTTP {response.status_code}. Try again or use manual IDs.")
    return response.json()
