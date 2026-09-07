# tests/test_interactive_sync_catalogs.py
# CrossWatch - Mapping catalog availability and provider contract tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from copy import deepcopy

import pytest
import requests
from fastapi import HTTPException

from services.interactive_sync_catalogs import search_catalogs
from services.interactive_sync_mapping import search_candidates


CONNECTIONS = {
    "PLEX": {"account_token": "plex-token"},
    "JELLYFIN": {"server": "https://jellyfin.test", "user_id": "user-two", "access_token": "jf-token"},
    "EMBY": {"server": "https://emby.test", "user_id": "user-two", "access_token": "emby-token"},
    "KODI": {"server": "https://kodi.test", "connection_verified": True},
    "SCROB": {"server_url": "https://scrob.test", "api_key": "scrob-key", "username": "me", "password": "pass"},
    "TRAKT": {"client_id": "trakt-key", "access_token": "trakt-token"},
    "MDBLIST": {"api_key": "mdblist-key"},
    "SIMKL": {"client_id": "simkl-key", "access_token": "simkl-token"},
    "PUNCHPLAY": {"access_token": "punchplay-token"},
    "FLICKLIST": {"api_key": "flicklist-key"},
    "TMDB": {"api_key": "sync-key", "session_id": "sync-session"},
    "PUBLICMETADB": {"api_key": "publicmeta-key"},
    "NUVIO": {"access_token": "nuvio-token", "profile_id": 2, "base_url": "https://nuvio.test"},
    "STREMIO": {"auth_key": "stremio-key"},
    "FLOPPY": {"server_url": "https://floppy.test", "api_token": "floppy-token"},
}


def context(provider, metadata=True):
    key = "tmdb_sync" if provider == "TMDB" else provider.lower()
    cfg = {key: {"instances": {"second": deepcopy(CONNECTIONS[provider])}}}
    if metadata:
        cfg["tmdb"] = {"api_key": "metadata-key"}
    return cfg, dict(provider=provider, instance="second", item=dict(type="episode"))


@pytest.mark.parametrize("provider", CONNECTIONS)
def test_catalogs_require_configured_exact_instance(provider, monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("Availability checks must not access provider APIs")
    monkeypatch.setattr(requests.Session, "request", unexpected)
    cfg, row = context(provider)
    options = search_catalogs(cfg, row)["catalogs"]
    assert options and "metadata-key" not in str(options)
    assert not search_catalogs(cfg, dict(row, instance="missing"))["catalogs"]
    assert not search_catalogs(cfg, dict(row, instance="default"))["catalogs"]
    with pytest.raises(HTTPException) as error:
        search_candidates(cfg, dict(row, instance="missing"), "Monster", catalog="tmdb")
    assert error.value.status_code == 409


@pytest.mark.parametrize("provider", ["FLICKLIST", "PUBLICMETADB", "NUVIO", "STREMIO"])
def test_unverified_destination_search_is_not_advertised_as_native(provider):
    cfg, row = context(provider)
    assert search_catalogs(cfg, row)["catalogs"] == [{"id": "tmdb", "label": "TMDb metadata"}]
    del cfg["tmdb"]
    assert not search_catalogs(cfg, row)["catalogs"]


def test_bingebase_is_excluded():
    assert not search_catalogs({"bingebase": {"api_key": "key"}, "tmdb": {"api_key": "key"}},
                               dict(provider="BINGEBASE"))["catalogs"]


@pytest.mark.parametrize("provider", ["JELLYFIN", "EMBY"])
def test_server_search_uses_selected_user_and_external_ids(provider, monkeypatch):
    cfg, row = context(provider)
    class Response:
        status_code = 200
        def json(self):
            return {"Items": [{"Name": "Monster", "ProductionYear": 2022,
                               "Id": "internal-id", "ProviderIds": {"Tmdb": "113988", "Imdb": "tt13207736"}}]}
    def get(self, url, **kwargs):
        assert url == CONNECTIONS[provider]["server"] + "/Users/user-two/Items"
        assert kwargs["headers"]["X-Emby-Token"] == CONNECTIONS[provider]["access_token"]
        assert kwargs["params"]["IncludeItemTypes"] == "Series"
        assert kwargs["params"]["Limit"] == 20
        return Response()
    monkeypatch.setattr(requests.Session, "get", get)
    result = search_candidates(cfg, row, "Monster")
    assert result["results"][0]["ids"] == {"tmdb": "113988", "imdb": "tt13207736"}


def test_kodi_search_is_bounded_read_only_rpc(monkeypatch):
    from providers.sync.kodi._common import KodiClient
    cfg, row = context("KODI")
    def rpc(self, method, params):
        assert self.cfg.server == "https://kodi.test"
        assert method == "VideoLibrary.GetTVShows"
        assert params["limits"] == {"start": 0, "end": 20}
        assert params["filter"]["value"] == "Monster"
        return {"tvshows": [{"title": "Monster", "year": 2022, "uniqueid": {"tmdb": "113988"}}]}
    monkeypatch.setattr(KodiClient, "rpc", rpc)
    assert search_candidates(cfg, row, "Monster")["results"][0]["ids"] == {"tmdb": "113988"}


def test_floppy_search_uses_catalog_source_ids(monkeypatch):
    from providers.auth._auth_FLOPPY import FloppyClient
    cfg, row = context("FLOPPY")
    def request(self, path, **kwargs):
        assert self.api_token == "floppy-token"
        assert path == "search/tv/" and kwargs["params"]["source"] == "tmdb"
        return {"results": [{"title": "Monster", "media_id": 113988, "source": "tmdb"}]}
    monkeypatch.setattr(FloppyClient, "request_json", request)
    assert search_candidates(cfg, row, "Monster")["results"][0]["ids"] == {"tmdb": "113988"}


def test_scrob_search_uses_instance_auth_helper(monkeypatch):
    from providers.auth import _auth_SCROB
    cfg, row = context("SCROB")
    class Response:
        status_code = 200
        def json(self):
            return {"results": [{"title": "Monster", "tmdb_id": 113988, "type": "series"}]}
    def request(client, method, url, **kwargs):
        assert method == "GET" and url == "https://scrob.test/media/search"
        assert kwargs["instance_id"] == "second"
        assert kwargs["params"] == {"q": "Monster", "type": "series"}
        return Response()
    monkeypatch.setattr(_auth_SCROB, "request_with_auth", request)
    assert search_candidates(cfg, row, "Monster")["results"][0]["ids"] == {"tmdb": "113988"}


def test_punchplay_catalog_does_not_mix_movies_and_shows(monkeypatch):
    cfg, row = context("PUNCHPLAY")
    class Response:
        status_code = 200
        def json(self):
            return {"items": [{"name": "Monster", "tmdbId": 113988, "type": "show"},
                              {"name": "Monster", "tmdbId": 1, "type": "movie"}]}
    def get(self, url, **kwargs):
        assert url == "https://punchplay.tv/api/public/v1/catalog/search"
        assert kwargs["params"] == {"q": "Monster", "type": "show"}
        assert "headers" not in kwargs
        return Response()
    monkeypatch.setattr(requests.Session, "get", get)
    result = search_candidates(cfg, row, "Monster")
    assert len(result["results"]) == 1 and result["results"][0]["ids"] == {"tmdb": "113988"}


def test_tmdb_sync_uses_sync_instance_key_not_metadata_key(monkeypatch):
    cfg, row = context("TMDB")
    keys = []
    class Response:
        status_code = 200
        def json(self):
            return {"results": [{"name": "Monster", "id": 113988}]}
    def get(self, url, **kwargs):
        keys.append(kwargs["params"]["api_key"])
        return Response()
    monkeypatch.setattr(requests.Session, "get", get)
    search_candidates(cfg, row, "Monster")
    result = search_candidates(cfg, row, "Monster", catalog="tmdb")
    assert keys == ["sync-key", "metadata-key"]
    assert "not been checked" in result["note"]
