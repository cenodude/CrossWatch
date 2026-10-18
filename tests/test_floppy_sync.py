# CrossWatch test scripts
from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest


@dataclass
class ResponseStub:
    status_code: int = 200
    payload: Any = None

    @property
    def headers(self) -> dict[str, str]:
        return {"Content-Type": "application/json"}

    @property
    def text(self) -> str:
        import json

        return json.dumps(self.payload if self.payload is not None else {})

    def json(self) -> Any:
        return self.payload if self.payload is not None else {}


class SessionStub:
    def __init__(self, routes: dict[tuple[str, str], Any]) -> None:
        self.routes = routes
        self.calls: list[dict[str, Any]] = []
        self.bucket_lookups: list[dict[str, Any]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> ResponseStub:
        path = url.split("/api/v1/", 1)[1].strip("/")
        call = {"method": method.upper(), "path": path, **kwargs}
        route = self.routes.get((method.upper(), path))
        if route is None and method.upper() == "GET" and path in {"media/tv", "media/anime"}:
            self.bucket_lookups.append(call)
            return ResponseStub(404, {"detail": "not found"})
        self.calls.append(call)
        if callable(route):
            route = route(call)
        if isinstance(route, ResponseStub):
            return route
        if route is None:
            return ResponseStub(404, {"detail": "not found"})
        return ResponseStub(200, route)


class ClientStub:
    api_base = "https://floppy.local/api/v1"
    api_token = "token"
    timeout = 12
    verify_ssl = False

    def __init__(self, session: SessionStub) -> None:
        self.session = session


class AdapterStub:
    def __init__(self, routes: dict[tuple[str, str], Any], cfg: dict[str, Any] | None = None) -> None:
        self.config = cfg or {"floppy": {"watchlist_name": "Watchlist"}}
        self.instance_id = "default"
        self.client = ClientStub(SessionStub(routes))


def _install_tmdb_resolver(monkeypatch: Any, tmdb_id: str) -> list[dict[str, Any]]:
    from providers.metadata import _meta_TMDB

    calls: list[dict[str, Any]] = []

    class FakeTmdb:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def fetch(self, *, entity: str, ids: dict[str, str], need: dict[str, bool] | None = None, **_kwargs: Any) -> dict[str, Any]:
            calls.append({"entity": entity, "ids": dict(ids), "need": dict(need or {})})
            return {"ids": {"tmdb": tmdb_id}}

    monkeypatch.setattr(_meta_TMDB, "TmdbProvider", FakeTmdb)
    return calls


def test_floppy_capabilities_expose_only_requested_features() -> None:
    from providers.sync._mod_FLOPPY import OPS

    features = OPS.features()
    caps = OPS.capabilities()

    assert features == {"watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": False, "collection": True}
    assert caps["watchlist"]["custom_lists"] is True
    assert caps["ratings"]["scale"] == "0-10"
    assert caps["progress"]["read"] is True
    assert caps["progress"]["write"] is True
    assert caps["collection"]["read"] is True
    assert caps["collection"]["write"] is True
    assert caps["collection"]["types"] == {"movies": True, "shows": True, "seasons": True, "episodes": True}


def test_floppy_module_uses_shared_rate_limiter() -> None:
    from providers.sync._mod_FLOPPY import FLOPPYModule

    mod = FLOPPYModule({"floppy": {"server_url": "http://x", "api_token": "t", "rate_limit": {"get_per_sec": 12, "post_per_sec": 9}}})

    assert mod.client.session._rate_limiter is not None
    assert mod.client.session._rate_limiter_meta == {"get_per_sec": 12.0, "post_per_sec": 9.0}


def test_floppy_module_uses_non_default_provider_config_view(monkeypatch: Any) -> None:
    from cw_platform.provider_instances import build_provider_config_view
    from providers.sync._mod_FLOPPY import FLOPPYModule
    from providers.sync.floppy._common import is_configured

    cfg = {
        "floppy": {
            "server_url": "https://default.local",
            "api_token": "default-token",
            "instances": {
                "FLOPPY-P01": {
                    "server_url": "https://p01.local",
                    "api_token": "p01-token",
                    "verify_ssl": True,
                }
            },
        }
    }
    view = build_provider_config_view(cfg, "floppy", "FLOPPY-P01")
    monkeypatch.setenv("CW_PAIR_DST", "FLOPPY")
    monkeypatch.setenv("CW_PAIR_DST_INSTANCE", "FLOPPY-P01")

    mod = FLOPPYModule(view)

    assert is_configured(view, "FLOPPY-P01") is True
    assert mod.client.server_url == "https://p01.local"
    assert mod.client.api_token == "p01-token"
    assert mod.client.verify_ssl is True


def test_floppy_non_default_missing_instance_does_not_reuse_default_block() -> None:
    from cw_platform.provider_instances import build_provider_config_view
    from providers.sync.floppy._common import configured_block, is_configured

    cfg = {"floppy": {"server_url": "https://default.local", "api_token": "default-token"}}
    view = build_provider_config_view(cfg, "floppy", "FLOPPY-P01")

    assert configured_block(view, "FLOPPY-P01") == {}
    assert is_configured(view, "FLOPPY-P01") is False


def test_floppy_watchlist_reads_configured_custom_list() -> None:
    from providers.sync.floppy import _watchlist

    adapter = AdapterStub(
        {
            ("GET", "lists"): {"results": [{"id": 7, "name": "Backlog"}], "count": 1},
            ("GET", "lists/7/items"): {
                "results": [
                    {"item_id": "movie/tmdb/11", "title": "Movie", "lists": [{"list_id": 7, "list_item_id": 3}]},
                    {"item_id": "tv/tmdb/22/1/1", "title": "Episode"},
                ],
                "count": 2,
            },
        },
        {"_pair_providers": {"floppy": {"watchlist_name": "Backlog"}}, "floppy": {}},
    )

    out = _watchlist.build_index(adapter)

    assert list(out) == ["tmdb:11"]
    assert out["tmdb:11"]["_floppy_list_item_id"] == 3


def test_floppy_watchlist_adds_to_existing_list() -> None:
    from providers.sync.floppy import _watchlist

    adapter = AdapterStub(
        {
            ("GET", "lists"): {"results": [{"id": 7, "name": "Watchlist"}], "count": 1},
            ("PUT", "media/movie/tmdb/11/lists/7"): [],
        }
    )

    res = _watchlist.add(adapter, [{"type": "movie", "ids": {"tmdb": "11"}}])

    assert res["count"] == 1
    assert [c["method"] for c in adapter.client.session.calls] == ["GET", "PUT"]


def test_floppy_watchlist_tracks_missing_item_before_add() -> None:
    from providers.sync.floppy import _watchlist

    calls = {"put": 0}

    def put(call: dict[str, Any]) -> ResponseStub:
        calls["put"] += 1
        if calls["put"] == 1:
            return ResponseStub(404, {"detail": "Media not found."})
        return ResponseStub(200, [])

    adapter = AdapterStub(
        {
            ("GET", "lists"): {"results": [{"id": 7, "name": "Watchlist"}], "count": 1},
            ("PUT", "media/movie/tmdb/11/lists/7"): put,
            ("POST", "media/movie"): {"item_id": "movie/tmdb/11"},
        }
    )

    res = _watchlist.add(adapter, [{"type": "movie", "ids": {"tmdb": "11"}}])

    assert res["count"] == 1
    assert [c["method"] for c in adapter.client.session.calls] == ["GET", "PUT", "POST", "PUT"]


def test_floppy_watchlist_resolves_imdb_to_tmdb_when_metadata_configured(monkeypatch: Any) -> None:
    from providers.sync.floppy import _watchlist

    calls = _install_tmdb_resolver(monkeypatch, "550")
    adapter = AdapterStub(
        {
            ("GET", "lists"): {"results": [{"id": 7, "name": "Watchlist"}], "count": 1},
            ("PUT", "media/movie/tmdb/550/lists/7"): [],
        },
        {"floppy": {"watchlist_name": "Watchlist"}, "tmdb": {"api_key": "tmdb-key"}},
    )

    res = _watchlist.add(adapter, [{"type": "movie", "ids": {"imdb": "tt0137523"}, "title": "Fight Club"}])

    assert res["count"] == 1
    assert res["confirmed_keys"] == ["imdb:tt0137523"]
    assert res["confirmed_destinations"]["imdb:tt0137523"]["key"] == "tmdb:550"
    assert res["confirmed_destinations"]["imdb:tt0137523"]["item"]["ids"] == {"tmdb": "550", "imdb": "tt0137523"}
    assert calls == [{"entity": "movie", "ids": {"imdb": "tt0137523"}, "need": {"poster": False, "backdrop": False, "ids": True}}]
    assert adapter.client.session.calls[1]["path"] == "media/movie/tmdb/550/lists/7"


def test_floppy_collection_reads_owned_movies_and_episodes() -> None:
    from providers.sync.floppy import _collection

    adapter = AdapterStub(
        {
            ("GET", "collection"): {
                "results": [
                    {
                        "id": 91,
                        "media_type": "digital",
                        "collected_at": "2026-08-01T12:00:00Z",
                        "item": {"media_type": "movie", "source": "tmdb", "media_id": "11", "title": "Movie", "ids": {"tmdb": "11", "imdb": "tt0000011"}},
                    },
                    {
                        "id": 92,
                        "item": {"media_type": "episode", "source": "tmdb", "media_id": "22", "season_number": 1, "episode_number": 2, "title": "Episode", "ids": {"tmdb": "22"}},
                    },
                ],
                "count": 2,
            }
        }
    )

    out = _collection.build_index(adapter)

    assert out["tmdb:11"]["format"] == "digital"
    assert out["tmdb:11"]["_floppy_collection_id"] == "91"
    assert out["tmdb:11"]["collected_at"] == "2026-08-01T12:00:00Z"
    assert out["tmdb:22#s01e02"]["_floppy_collection_id"] == "92"


def test_floppy_collection_adds_movie_via_item_id() -> None:
    from providers.sync.floppy import _collection

    adapter = AdapterStub(
        {
            ("POST", "media/movie"): {"id": 41, "item_id": "movie/tmdb/11"},
            ("POST", "collection"): {"id": 91},
        }
    )

    res = _collection.add(adapter, [{"type": "movie", "ids": {"tmdb": "11"}, "collected_at": "2026-08-01T12:00:00Z"}])

    assert res["count"] == 1
    assert [c["path"] for c in adapter.client.session.calls] == ["media/movie", "collection"]
    assert adapter.client.session.calls[0]["json"] == {"source": "tmdb", "media_id": "11", "status": 0}
    assert adapter.client.session.calls[1]["json"] == {"item_id": "41", "media_type": "digital", "collected_at": "2026-08-01T12:00:00Z"}


def test_floppy_collection_add_keeps_source_format_through_planner_minimal() -> None:
    from cw_platform.id_map import minimal
    from providers.sync.floppy import _collection

    adapter = AdapterStub(
        {
            ("POST", "media/movie"): {"id": 41, "item_id": "movie/tmdb/11"},
            ("POST", "collection"): {"id": 91},
        }
    )

    planned = minimal({"type": "movie", "ids": {"tmdb": "11"}, "format": "4k_bluray", "_punchplay_collection_id": "7"})
    res = _collection.add(adapter, [planned])

    assert res["count"] == 1
    assert adapter.client.session.calls[1]["json"] == {"item_id": "41", "media_type": "4k_bluray"}


def test_floppy_collection_reads_media_info() -> None:
    from providers.sync.floppy import _collection

    adapter = AdapterStub(
        {
            ("GET", "collection"): {
                "results": [
                    {
                        "id": 8,
                        "media_type": "bluray",
                        "resolution": "4k",
                        "hdr": "Dolby Vision",
                        "audio_codec": "TrueHD Atmos",
                        "audio_channels": "7.1",
                        "is_3d": False,
                        "item": {"media_type": "movie", "source": "tmdb", "media_id": "238", "title": "The Godfather"},
                    },
                    {"id": 9, "media_type": "", "resolution": "", "hdr": "", "audio_codec": "", "audio_channels": "", "is_3d": False, "item": {"media_type": "movie", "source": "tmdb", "media_id": "424"}},
                ],
                "count": 2,
            }
        }
    )

    out = _collection.build_index(adapter)

    assert out["tmdb:238"]["media_info"] == {"resolution": "uhd_4k", "hdr": "dolby_vision", "audio": "dolby_atmos", "audio_channels": "7.1"}
    assert "media_info" not in out["tmdb:424"]


def test_floppy_collection_add_sends_media_info_from_trakt_values() -> None:
    from cw_platform.id_map import minimal
    from providers.sync.floppy import _collection

    adapter = AdapterStub(
        {
            ("POST", "media/movie"): {"id": 41, "item_id": "movie/tmdb/11"},
            ("POST", "collection"): {"id": 91},
        }
    )

    planned = minimal({"type": "movie", "ids": {"tmdb": "11"}, "format": "bluray", "media_info": {"resolution": "uhd_4k", "hdr": "hdr10_plus", "audio": "dts_x", "audio_channels": "7.1.4", "is_3d": True}})
    _collection.add(adapter, [planned])

    assert adapter.client.session.calls[1]["json"] == {
        "item_id": "41",
        "media_type": "bluray",
        "resolution": "4K",
        "hdr": "HDR10+",
        "audio_codec": "DTS:X",
        "audio_channels": "7.1.4",
        "is_3d": True,
    }


def test_floppy_collection_adds_episode_scope() -> None:
    from providers.sync.floppy import _collection

    adapter = AdapterStub(
        {
            ("POST", "media/episode"): {"id": 42, "item_id": "tv/tmdb/22/1/2"},
            ("POST", "collection"): {"id": 92},
        }
    )

    res = _collection.add(adapter, [{"type": "episode", "show_ids": {"tmdb": "22"}, "season": 1, "episode": 2}])

    assert res["count"] == 1
    assert adapter.client.session.calls[0]["json"] == {"source": "tmdb", "media_id": "22", "status": 0, "season_number": 1, "episode_number": 2}
    assert adapter.client.session.calls[1]["json"] == {"item_id": "42", "media_type": "digital"}


def test_floppy_collection_remove_deletes_all_matching_entries() -> None:
    from providers.sync.floppy import _collection

    adapter = AdapterStub(
        {
            ("DELETE", "collection/91"): ResponseStub(204, {}),
            ("DELETE", "collection/92"): ResponseStub(204, {}),
        }
    )

    res = _collection.remove(adapter, [{"type": "movie", "ids": {"tmdb": "11"}, "_floppy_collection_ids": ["91", "92"]}])

    assert res["count"] == 1
    assert [c["path"] for c in adapter.client.session.calls] == ["collection/91", "collection/92"]


@pytest.mark.parametrize("source", ["", "trakt_export", "crosswatch"])
def test_floppy_ratings_read_and_write_native_scale(source: str) -> None:
    from providers.sync.floppy import _ratings

    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {"results": [{"item_id": "movie/tmdb/11", "source": source, "score": 8.5}, {"item_id": "movie/tmdb/12", "source": source, "score": 0}], "count": 2},
            ("GET", "media/tv"): {"results": [{"item_id": "tv/tmdb/22", "source": source, "score": 7.5}], "count": 1},
            ("POST", "media/movie"): {"item_id": "movie/tmdb/11", "score": 9.0},
        }
    )

    out = _ratings.build_index(adapter)
    res = _ratings.add(adapter, [{"type": "movie", "ids": {"tmdb": "11"}, "rating": 9.0}])

    assert out["tmdb:11"]["rating"] == 8.5
    assert out["tmdb:22#show"]["rating"] == 7.5
    assert "tmdb:12" not in out
    assert res["count"] == 1
    assert adapter.client.session.calls[-1]["json"] == {"source": "tmdb", "media_id": "11", "status": 0, "score": 9.0}


def test_floppy_ratings_index_maps_scored_at_to_rated_at() -> None:
    from providers.sync.floppy import _ratings

    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {
                "results": [
                    {"item_id": "movie/tmdb/11", "score": 8.4, "scored_at": "2026-09-29T00:31:53.550042Z"},
                    {"item_id": "movie/tmdb/12", "score": 6.0, "scored_at": None},
                    {"item_id": "movie/tmdb/13", "score": None, "scored_at": "2026-09-29T00:32:01.073470Z"},
                ],
                "count": 3,
            },
            ("GET", "media/tv"): {"results": [{"item_id": "tv/tmdb/37854", "score": 7.0, "scored_at": "2026-09-29T02:37:41.250114+02:00"}], "count": 1},
        }
    )

    out = _ratings.build_index(adapter)

    assert out["tmdb:11"]["rated_at"] == "2026-09-29T00:31:53Z"
    assert out["tmdb:12"]["rating"] == 6.0
    assert "rated_at" not in out["tmdb:12"]
    assert "tmdb:13" not in out
    assert out["tmdb:37854#show"]["rated_at"] == "2026-09-29T00:37:41Z"


def test_floppy_ratings_index_ignores_malformed_scored_at() -> None:
    from providers.sync.floppy import _ratings

    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {"results": [{"item_id": "movie/tmdb/11", "score": 8.0, "scored_at": "not-a-date"}], "count": 1},
            ("GET", "media/tv"): {"results": [], "count": 0},
        }
    )

    out = _ratings.build_index(adapter)

    assert out["tmdb:11"]["rating"] == 8.0
    assert "rated_at" not in out["tmdb:11"]


def test_floppy_ratings_fallback_patch_existing_and_skip_unsupported_scopes() -> None:
    from providers.sync.floppy import _ratings

    adapter = AdapterStub(
        {
            ("POST", "media/movie"): ResponseStub(400, {"detail": "Invalid media data."}),
            ("PATCH", "media/movie/tmdb/11"): {"item_id": "movie/tmdb/11", "score": 7.0},
        }
    )

    res = _ratings.add(
        adapter,
        [
            {"type": "movie", "ids": {"tmdb": "11"}, "rating": 7.0},
            {"type": "person", "ids": {"tmdb": "22"}, "rating": 8.0},
        ],
    )

    assert res["count"] == 1
    assert res["skipped"] == 1
    assert adapter.client.session.calls[1]["method"] == "PATCH"
    assert adapter.client.session.calls[1]["json"] == {"score": 7.0}


def test_floppy_ratings_create_tracked_items() -> None:
    from providers.sync.floppy import _ratings

    adapter = AdapterStub({("POST", "media/movie"): {"item_id": "movie/tmdb/11", "score": 7.0}})

    res = _ratings.add(adapter, [{"type": "movie", "ids": {"tmdb": "11"}, "rating": 7.0}])

    assert res["count"] == 1
    assert res["skipped"] == 0
    assert [c["method"] for c in adapter.client.session.calls] == ["POST"]


def test_floppy_ratings_shadow_covers_read_after_write_lag(monkeypatch: Any) -> None:
    from providers.sync.floppy import _ratings

    _ratings._WRITE_SHADOW.clear()
    monkeypatch.setenv("CW_PAIR_KEY", "pair")
    adapter = AdapterStub(
        {
            ("POST", "media/movie"): {"item_id": "movie/tmdb/11", "score": 7.0},
            ("GET", "media/movie"): {"results": [], "count": 0},
            ("GET", "media/tv"): {"results": [], "count": 0},
        }
    )

    res = _ratings.add(adapter, [{"type": "movie", "ids": {"tmdb": "11"}, "rating": 7.0}])
    out = _ratings.build_index(adapter)

    assert res["count"] == 1
    assert out["tmdb:11"]["rating"] == 7.0


def test_floppy_ratings_remove_clears_score_with_null() -> None:
    from providers.sync.floppy import _ratings

    adapter = AdapterStub({("PATCH", "media/movie/tmdb/11"): {"item_id": "movie/tmdb/11", "score": None}})

    res = _ratings.remove(adapter, [{"type": "movie", "ids": {"tmdb": "11"}, "rating": 7.0}])

    assert res["count"] == 1
    assert [c["method"] for c in adapter.client.session.calls] == ["PATCH"]
    assert adapter.client.session.calls[0]["json"] == {"score": None}


def test_floppy_ratings_index_reads_rated_seasons_and_episodes() -> None:
    from providers.sync.floppy import _ratings

    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {"results": [], "count": 0},
            ("GET", "media/tv"): {"results": [], "count": 0},
            ("GET", "media/season"): {"results": [{"item_id": "tv/tmdb/37854/22", "score": 6.0, "scored_at": "2026-10-02T23:33:09Z"}], "count": 1},
            ("GET", "media/episode"): {
                "results": [
                    {"item_id": "tv/tmdb/37854/21/1007", "score": 7.5, "scored_at": "2026-10-02T23:33:08Z"},
                    {"item_id": "tv/tmdb/37854/2/62", "score": None, "scored_at": "2026-10-02T23:40:00Z"},
                ],
                "count": 2,
            },
        }
    )

    out = _ratings.build_index(adapter)

    assert out["tmdb:37854#season:22"]["rating"] == 6.0
    assert out["tmdb:37854#s21e1007"]["rating"] == 7.5
    assert out["tmdb:37854#s21e1007"]["rated_at"] == "2026-10-02T23:33:08Z"
    assert "tmdb:37854#s02e62" not in out
    nested = [c for c in adapter.client.session.calls if c["path"] in {"media/season", "media/episode"}]
    assert all(c["params"]["rating"] == "rated" for c in nested)


def test_floppy_ratings_index_survives_servers_without_season_and_episode_lists() -> None:
    from providers.sync.floppy import _ratings

    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {"results": [{"item_id": "movie/tmdb/11", "score": 8.0}], "count": 1},
            ("GET", "media/tv"): {"results": [], "count": 0},
        }
    )

    assert list(_ratings.build_index(adapter)) == ["tmdb:11"]


def test_floppy_ratings_write_episode_and_season_scores() -> None:
    from providers.sync.floppy import _ratings

    adapter = AdapterStub(
        {
            ("PATCH", "media/tv/tmdb/22/1/episodes/2/score"): {"score": "8.0"},
            ("PATCH", "media/tv/tmdb/22/3"): {"item_id": "tv/tmdb/22/3"},
        }
    )

    res = _ratings.add(
        adapter,
        [
            {"type": "episode", "show_ids": {"tmdb": "22"}, "season": 1, "episode": 2, "rating": 8.0},
            {"type": "season", "show_ids": {"tmdb": "22"}, "season": 3, "rating": 6.5},
        ],
    )

    assert res["count"] == 2
    assert res["confirmed_keys"] == ["tmdb:22#s01e02", "tmdb:22#season:3"]
    assert [(c["method"], c["path"], c["json"]) for c in adapter.client.session.calls] == [
        ("PATCH", "media/tv/tmdb/22/1/episodes/2/score", {"score": 8.0}),
        ("PATCH", "media/tv/tmdb/22/3", {"score": 6.5}),
    ]


def test_floppy_ratings_remove_clears_episode_and_season_scores() -> None:
    from providers.sync.floppy import _ratings

    adapter = AdapterStub(
        {
            ("PATCH", "media/tv/tmdb/22/1/episodes/2/score"): {"score": None},
            ("PATCH", "media/tv/tmdb/22/3"): {"item_id": "tv/tmdb/22/3"},
        }
    )

    res = _ratings.remove(
        adapter,
        [
            {"type": "episode", "show_ids": {"tmdb": "22"}, "season": 1, "episode": 2, "rating": 8.0},
            {"type": "season", "show_ids": {"tmdb": "22"}, "season": 3, "rating": 6.5},
        ],
    )

    assert res["count"] == 2
    assert [c["json"] for c in adapter.client.session.calls] == [{"score": None}, {"score": None}]


def test_floppy_ratings_untracked_season_is_created_as_planning_with_the_score() -> None:
    from providers.sync.floppy import _ratings

    adapter = AdapterStub(
        {
            ("PATCH", "media/tv/tmdb/70523/1"): ResponseStub(404, {"detail": "No matching item."}),
            ("POST", "media/season"): ResponseStub(201, {"item_id": "tv/tmdb/70523/1", "status": 0, "score": 7.0}),
        }
    )

    res = _ratings.add(adapter, [{"type": "season", "show_ids": {"tmdb": "70523"}, "season": 1, "rating": 7.0}])

    assert res["count"] == 1
    assert [(c["method"], c["path"]) for c in adapter.client.session.calls] == [("PATCH", "media/tv/tmdb/70523/1"), ("POST", "media/season")]
    assert adapter.client.session.calls[1]["json"] == {"source": "tmdb", "media_id": "70523", "season_number": 1, "status": 0, "score": 7.0}


def test_floppy_ratings_clearing_an_untracked_season_creates_nothing() -> None:
    from providers.sync.floppy import _ratings

    adapter = AdapterStub({("PATCH", "media/tv/tmdb/70523/1"): ResponseStub(404, {"detail": "No matching item."})})

    res = _ratings.remove(adapter, [{"type": "season", "show_ids": {"tmdb": "70523"}, "season": 1, "rating": 7.0}])

    assert res["count"] == 1
    assert [c["method"] for c in adapter.client.session.calls] == ["PATCH"]


def test_floppy_ratings_untracked_episode_is_unresolved() -> None:
    from providers.sync.floppy import _ratings

    adapter = AdapterStub(
        {("PATCH", "media/tv/tmdb/70523/1/episodes/1/score"): ResponseStub(404, {"detail": "Season not found or not tracked."})}
    )

    res = _ratings.add(adapter, [{"type": "episode", "show_ids": {"tmdb": "70523"}, "season": 1, "episode": 1, "rating": 7.0}])

    assert res["count"] == 0
    assert res["unresolved"][0]["reason"].startswith("floppy_http_404")
    assert "not tracked" in res["unresolved"][0]["reason"]


def test_floppy_ratings_episode_uses_the_history_episode_mapping(monkeypatch: Any) -> None:
    from providers.sync.floppy import _history, _ratings

    monkeypatch.setattr(
        _history,
        "resolve_source_coordinate",
        lambda *_args, **_kwargs: SimpleNamespace(provider="tmdb", ident="37854", season=23, episode=1),
    )
    monkeypatch.setattr(
        _history,
        "show_layout",
        lambda *_args, **_kwargs: [(1, n) for n in range(1, 1156)] + [(23, n) for n in range(1156, 1182)],
    )
    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    adapter = AdapterStub(
        {
            ("GET", "media/tv/tmdb/37854/23/episodes"): {
                "results": [{"item_id": f"tv/tmdb/37854/23/{n}", "episode_number": n} for n in range(1156, 1182)],
                "count": 26,
            },
            ("PATCH", "media/tv/tmdb/37854/23/episodes/1156/score"): {"score": "9.0"},
        },
        _anime_history_cfg(),
    )

    res = _ratings.add(adapter, [{**_one_piece_s23_source(), "rating": 9.0}])

    assert res["count"] == 1
    assert [c["path"] for c in adapter.client.session.calls if c["method"] == "PATCH"] == ["media/tv/tmdb/37854/23/episodes/1156/score"]


def test_floppy_ratings_zero_is_not_written_as_rating() -> None:
    from providers.sync.floppy import _ratings

    adapter = AdapterStub({})

    res = _ratings.add(adapter, [{"type": "movie", "ids": {"tmdb": "11"}, "rating": 0}])

    assert res["count"] == 0
    assert res["skipped"] == 1
    assert adapter.client.session.calls == []


def test_floppy_progress_reads_movies_and_episodes() -> None:
    from providers.sync.floppy import _progress

    adapter = AdapterStub(
        {
            ("GET", "playback/progress"): {
                "results": [
                    {"media_type": "movie", "source": "tmdb", "media_id": "11", "position_seconds": 120, "duration_seconds": 600, "updated_at": "2026-08-01T12:00:00Z"},
                    {"media_type": "episode", "ids": {"tmdb": "22"}, "season_number": 1, "episode_number": 2, "position_seconds": 300, "duration_seconds": 3000},
                ],
                "count": 2,
            }
        }
    )

    out = _progress.build_index(adapter)

    assert out["tmdb:11"]["progress_ms"] == 120000
    assert out["tmdb:11"]["progress_percent"] == 20.0
    assert out["tmdb:22#s01e02"]["duration_ms"] == 3000000


def test_floppy_progress_reads_position_without_duration() -> None:
    from providers.sync.floppy import _progress

    adapter = AdapterStub(
        {
            ("GET", "playback/progress"): {
                "results": [
                    {"media_type": "episode", "ids": {"tmdb": "1407"}, "season_number": 5, "episode_number": 6, "position_seconds": 1886, "duration_seconds": None, "completed": True},
                    {"media_type": "episode", "ids": {"tmdb": "223300"}, "season_number": 1, "episode_number": 3, "position_seconds": 749, "duration_seconds": None, "completed": False},
                    {"media_type": "episode", "ids": {"tmdb": "87917"}, "season_number": 4, "episode_number": 1, "position_seconds": 1456, "duration_seconds": 3000, "completed": False},
                ],
                "count": 3,
            }
        }
    )

    out = _progress.build_index(adapter)

    assert "tmdb:1407#s05e06" not in out
    assert out["tmdb:223300#s01e03"]["progress_ms"] == 749000
    assert "duration_ms" not in out["tmdb:223300#s01e03"]
    assert "progress_percent" not in out["tmdb:223300#s01e03"]
    assert out["tmdb:87917#s04e01"]["progress_percent"] == 48.533
    assert adapter.client.session.calls[0]["params"]["completed"] == "false"


def test_floppy_progress_write_and_clear_use_playback_progress_api() -> None:
    from providers.sync.floppy import _progress

    adapter = AdapterStub(
        {
            ("PUT", "playback/progress"): ResponseStub(204, {}),
        }
    )
    item = {"type": "episode", "show_ids": {"tmdb": "22"}, "season": 1, "episode": 2, "progress_ms": 300000, "duration_ms": 3000000}

    add = _progress.add(adapter, [item])
    remove = _progress.remove(adapter, [item])

    assert add["count"] == 1
    assert remove["count"] == 1
    assert adapter.client.session.calls[0]["json"] == {"media_type": "episode", "ids": {"tmdb": "22"}, "season_number": 1, "episode_number": 2, "position_seconds": 300, "duration_seconds": 3000}
    assert adapter.client.session.calls[1]["method"] == "PUT"
    assert adapter.client.session.calls[1]["json"] == {"media_type": "episode", "ids": {"tmdb": "22"}, "season_number": 1, "episode_number": 2, "position_seconds": None}


def test_floppy_progress_resolves_show_imdb_to_tmdb_when_metadata_configured(monkeypatch: Any) -> None:
    from providers.sync.floppy import _progress

    calls = _install_tmdb_resolver(monkeypatch, "1399")
    adapter = AdapterStub(
        {("PUT", "playback/progress"): ResponseStub(204, {})},
        {"floppy": {}, "tmdb": {"api_key": "tmdb-key"}},
    )
    item = {
        "type": "episode",
        "show_ids": {"imdb": "tt0944947"},
        "season": 1,
        "episode": 1,
        "progress_ms": 600000,
        "duration_ms": 3600000,
    }

    res = _progress.add(adapter, [item])

    assert res["count"] == 1
    assert res["confirmed_destinations"]["imdb:tt0944947#s01e01"]["key"] == "tmdb:1399#s01e01"
    assert res["confirmed_destinations"]["imdb:tt0944947#s01e01"]["item"]["show_ids"] == {"tmdb": "1399", "imdb": "tt0944947"}
    assert calls == [{"entity": "tv", "ids": {"imdb": "tt0944947"}, "need": {"poster": False, "backdrop": False, "ids": True}}]
    assert adapter.client.session.calls[0]["json"]["ids"] == {"tmdb": "1399"}


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({"item_id": "movie/tmdb/11", "source": "trakt_export"}, {"type": "movie", "ids": {"tmdb": "11"}}),
        ({"source": "trakt_export", "item": {"source": "tmdb", "media_id": "11", "media_type": "movie"}}, {"type": "movie", "ids": {"tmdb": "11"}}),
        ({"source": "tmdb", "media_id": "11", "media_type": "movie"}, {"type": "movie", "ids": {"tmdb": "11"}}),
        ({"item_id": "movie/imdb/tt11", "source": "tmdb"}, None),
        ({"source": "tmdb", "item": {"source": "imdb", "media_id": "tt11", "media_type": "movie"}}, None),
    ],
)
def test_floppy_item_uses_metadata_source(row: dict[str, Any], expected: dict[str, Any] | None) -> None:
    from providers.sync.floppy._common import item_from_row

    assert item_from_row(row) == expected


@pytest.mark.parametrize("source", ["", "trakt_export", "crosswatch"])
def test_floppy_history_reads_movies_and_episodes(source: str) -> None:
    from providers.sync.floppy import _history

    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {
                "results": [
                    {"item_id": "movie/tmdb/11", "source": source, "status": 3, "end_date": "2026-01-01T00:00:00Z", "consumption_id": 41},
                    {"item_id": "movie/tmdb/11", "source": source, "status": 3, "end_date": "2026-01-03T00:00:00Z", "consumption_id": 43},
                ],
                "count": 2,
            },
            ("GET", "media/episode"): {"results": [{"item_id": "tv/tmdb/22/1/2", "source": source, "end_date": "2026-01-02T00:00:00Z", "consumption_id": 42}], "count": 1},
        }
    )

    out = _history.build_index(adapter)

    assert out["tmdb:11"]["watched_at"] == "2026-01-03T00:00:00Z"
    assert out["tmdb:11"]["_floppy_consumption_id"] == 43
    assert out["tmdb:22#s01e02"]["_floppy_consumption_id"] == 42


def test_floppy_history_resolves_imdb_to_tmdb_when_metadata_configured(monkeypatch: Any) -> None:
    from providers.sync.floppy import _history

    _install_tmdb_resolver(monkeypatch, "550")
    adapter = AdapterStub(
        {("POST", "media/movie"): {"item_id": "movie/tmdb/550"}},
        {"floppy": {}, "tmdb": {"api_key": "tmdb-key"}},
    )

    res = _history.add(adapter, [{"type": "movie", "ids": {"imdb": "tt0137523"}, "watched_at": "2026-01-01T00:00:00Z"}])

    assert res["count"] == 1
    assert res["confirmed_keys"] == ["imdb:tt0137523"]
    assert res["confirmed_destinations"]["imdb:tt0137523"]["key"] == "tmdb:550"
    assert res["confirmed_destinations"]["imdb:tt0137523"]["item"]["ids"] == {"tmdb": "550", "imdb": "tt0137523"}
    assert adapter.client.session.calls[0]["json"] == {
        "source": "tmdb",
        "media_id": "550",
        "status": 3,
        "end_date": "2026-01-01T00:00:00Z",
    }


def test_floppy_movie_history_create_does_not_patch_after_create() -> None:
    from providers.sync.floppy import _history

    adapter = AdapterStub({("POST", "media/movie"): {"item_id": "movie/tmdb/11", "status": 3}})

    res = _history.add(adapter, [{"type": "movie", "ids": {"tmdb": "11"}, "watched_at": "2026-01-03T00:00:00Z"}])

    assert res["count"] == 1
    assert [c["method"] for c in adapter.client.session.calls] == ["POST"]
    assert adapter.client.session.calls[0]["json"] == {"source": "tmdb", "media_id": "11", "status": 3, "end_date": "2026-01-03T00:00:00Z"}


def test_floppy_movie_history_rewatch_add_uses_movie_watch_api() -> None:
    from providers.sync.floppy import _history

    adapter = AdapterStub(
        {("POST", "media/movie/tmdb/11/watch"): {"consumption_id": 51, "end_date": "2026-01-03T00:00:00Z"}},
        {"_cw_history_rewatches": True, "floppy": {}},
    )

    res = _history.add(adapter, [{"type": "movie", "ids": {"tmdb": "11"}, "watched_at": "2026-01-03T00:00:00Z"}])

    assert res["count"] == 1
    assert adapter.client.session.calls[0]["path"] == "media/movie/tmdb/11/watch"
    assert adapter.client.session.calls[0]["json"] == {
        "end_date": "2026-01-03T00:00:00Z",
        "external_id": "cw:tmdb:11@1767398400",
    }


@pytest.mark.parametrize("source", ["", "trakt_export", "crosswatch"])
def test_floppy_movie_history_rewatch_index_reads_each_movie_play(source: str) -> None:
    from providers.sync.floppy import _history

    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {
                "results": [{"item_id": "movie/tmdb/11", "status": 3, "end_date": "2026-01-03T00:00:00Z"}],
                "count": 1,
            },
            ("GET", "media/movie/tmdb/11/history"): {
                "results": [
                    {"consumption_id": 51, "source": source, "end_date": "2026-01-01T00:00:00Z"},
                    {"consumption_id": 52, "source": source, "end_date": "2026-01-03T00:00:00Z"},
                ],
                "count": 2,
            },
            ("GET", "media/episode"): {"results": [], "count": 0},
        },
        {"_cw_history_rewatches": True, "floppy": {}},
    )

    out = _history.build_index(adapter)

    assert sorted(out) == ["tmdb:11@1767225600", "tmdb:11@1767398400"]
    assert out["tmdb:11@1767225600"]["_floppy_consumption_id"] == 51
    assert out["tmdb:11@1767398400"]["_floppy_consumption_id"] == 52


@pytest.mark.parametrize("source", ["", "trakt_export", "crosswatch"])
def test_floppy_episode_history_rewatch_index_reads_each_play(source: str) -> None:
    from providers.sync.floppy import _history

    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {"results": [], "count": 0},
            ("GET", "media/episode"): {
                "results": [
                    {"item_id": "tv/tmdb/22/1/2", "source": source, "end_date": "2026-01-01T00:00:00Z", "consumption_id": 41},
                    {"item_id": "tv/tmdb/22/1/2", "source": source, "end_date": "2026-01-03T00:00:00Z", "consumption_id": 42},
                ],
                "count": 2,
            },
        },
        {"_cw_history_rewatches": True, "floppy": {}},
    )

    out = _history.build_index(adapter)

    assert sorted(out) == ["tmdb:22#s01e02@1767225600", "tmdb:22#s01e02@1767398400"]
    assert out["tmdb:22#s01e02@1767225600"]["_floppy_consumption_id"] == 41
    assert out["tmdb:22#s01e02@1767398400"]["_floppy_consumption_id"] == 42


def test_floppy_episode_history_add_prevents_duplicate_play() -> None:
    from providers.sync.floppy import _history

    adapter = AdapterStub(
        {
            ("GET", "media/tv/tmdb/22/1/2/history"): {"results": [{"consumption_id": 42, "end_date": "2026-01-02T00:00:00Z"}], "count": 1},
            ("POST", "media/tv/tmdb/22/1/episodes/2/watch"): {"consumption_id": 43},
        }
    )

    res = _history.add(adapter, [{"type": "episode", "show_ids": {"tmdb": "22"}, "season": 1, "episode": 2}])

    assert res["count"] == 1
    assert not any(c["method"] == "POST" for c in adapter.client.session.calls)


@pytest.mark.parametrize("rewatches", [True, False], ids=["rewatches", "plain"])
def test_floppy_episode_history_add_sends_external_id_only_for_rewatches(rewatches: bool) -> None:
    from providers.sync.floppy import _history

    cfg = {"_cw_history_rewatches": True, "floppy": {}} if rewatches else {"floppy": {}}
    adapter = AdapterStub(
        {
            ("GET", "media/tv/tmdb/22/1/2/history"): {"results": [], "count": 0},
            ("POST", "media/tv/tmdb/22/1/episodes/2/watch"): {"consumption_id": 43},
        },
        cfg,
    )

    res = _history.add(adapter, [{"type": "episode", "show_ids": {"tmdb": "22"}, "season": 1, "episode": 2, "watched_at": "2026-01-03T00:00:00Z"}])

    assert res["count"] == 1
    post = next(c for c in adapter.client.session.calls if c["method"] == "POST")
    expected = {"end_date": "2026-01-03T00:00:00Z"}
    if rewatches:
        expected["external_id"] = "cw:tmdb:22#s01e02@1767398400"
    assert post["json"] == expected


def test_floppy_history_rewatch_remove_uses_exact_consumption_id() -> None:
    from providers.sync.floppy import _history

    adapter = AdapterStub({("DELETE", "media/movie/tmdb/11/history/41"): ResponseStub(204, {})}, {"_cw_history_rewatches": True, "floppy": {}})

    res = _history.remove(adapter, [{"type": "movie", "ids": {"tmdb": "11"}, "_floppy_consumption_id": 41}])

    assert res["count"] == 1
    assert adapter.client.session.calls[0]["path"] == "media/movie/tmdb/11/history/41"


@pytest.mark.parametrize(
    ("watch_status", "remaining", "patched"),
    [
        (204, [{"consumption_id": 52, "end_date": "2026-01-01T00:00:00Z"}], False),
        (204, [{"consumption_id": 1114, "status": 3, "end_date": None}], True),
        (404, None, True),
    ],
    ids=["plays_left", "last_play", "no_plays"],
)
def test_floppy_movie_history_remove_never_deletes_the_movie_entry(watch_status: int, remaining: Any, patched: bool) -> None:
    from providers.sync.floppy import _history

    routes: dict[tuple[str, str], Any] = {
        ("DELETE", "media/movie/tmdb/11/watch"): ResponseStub(watch_status, {}),
        ("PATCH", "media/movie/tmdb/11"): {"id": 1},
    }
    if remaining is not None:
        routes[("GET", "media/movie/tmdb/11/history")] = {"results": remaining, "count": len(remaining)}
    adapter = AdapterStub(routes)

    res = _history.remove(adapter, [{"type": "movie", "ids": {"tmdb": "11"}, "_floppy_consumption_id": 1114}])

    assert res["count"] == 1
    calls = [(c["method"], c["path"]) for c in adapter.client.session.calls]
    assert calls[0] == ("DELETE", "media/movie/tmdb/11/watch")
    assert not any("history/" in path for _, path in calls)
    assert (("PATCH", "media/movie/tmdb/11") in calls) is patched
    if patched:
        assert adapter.client.session.calls[-1]["json"] == {"status": 0, "end_date": None}


def test_floppy_movie_history_remove_keeps_status_when_readback_fails() -> None:
    from providers.sync.floppy import _history

    adapter = AdapterStub(
        {
            ("DELETE", "media/movie/tmdb/11/watch"): ResponseStub(204, {}),
            ("GET", "media/movie/tmdb/11/history"): ResponseStub(500, {}),
            ("PATCH", "media/movie/tmdb/11"): {"id": 1},
        }
    )

    res = _history.remove(adapter, [{"type": "movie", "ids": {"tmdb": "11"}}])

    assert res["count"] == 0
    assert res["unresolved"]
    assert not any(c["method"] == "PATCH" for c in adapter.client.session.calls)


def test_floppy_history_index_keeps_watched_movies_in_any_status() -> None:
    from providers.sync.floppy import _history

    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {
                "results": [
                    {"item_id": "movie/tmdb/11", "status": 1, "end_date": "2026-01-03T00:00:00Z"},
                    {"item_id": "movie/tmdb/12", "status": 1, "end_date": None},
                    {"item_id": "movie/tmdb/13", "status": 4, "end_date": None},
                ],
                "count": 3,
            },
            ("GET", "media/episode"): {"results": [], "count": 0},
        }
    )

    out = _history.build_index(adapter)

    assert sorted(out) == ["tmdb:11"]
    assert "status" not in adapter.client.session.calls[0]["params"]


def test_floppy_cleanup_after_all_features_purges_tracked_media() -> None:
    from providers.sync._mod_FLOPPY import FLOPPYModule

    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {"results": [{"item_id": "movie/tmdb/11"}], "count": 1},
            ("GET", "media/tv"): {"results": [{"item_id": "tv/tmdb/22"}], "count": 1},
            ("DELETE", "media/movie/tmdb/11"): ResponseStub(204, {}),
            ("DELETE", "media/tv/tmdb/22"): ResponseStub(204, {}),
        }
    )

    res = FLOPPYModule.cleanup_after_features(adapter, ["watchlist", "ratings", "history"])

    assert res["removed"] == 2
    assert [c["path"] for c in adapter.client.session.calls if c["method"] == "DELETE"] == ["media/movie/tmdb/11", "media/tv/tmdb/22"]


def test_floppy_cleanup_after_partial_features_does_not_purge_media() -> None:
    from providers.sync._mod_FLOPPY import FLOPPYModule

    adapter = AdapterStub({("GET", "media/movie"): {"results": [{"item_id": "movie/tmdb/11"}], "count": 1}})

    res = FLOPPYModule.cleanup_after_features(adapter, ["ratings"])

    assert res == {}
    assert adapter.client.session.calls == []


def test_floppy_raw_ids_survive_minimal_snapshot() -> None:
    from cw_platform.id_map import minimal

    out = minimal({"type": "movie", "ids": {"tmdb": "11"}, "_floppy_consumption_id": 41, "_floppy_list_item_id": 3})

    assert out["_floppy_consumption_id"] == 41
    assert out["_floppy_list_item_id"] == 3


def test_floppy_coordinates_survive_minimal_snapshot() -> None:
    from cw_platform.id_map import minimal

    out = minimal({"type": "episode", "show_ids": {"tmdb": "12971"}, "season": 1, "episode": 100, "_floppy_season": 5, "_floppy_episode": 12})

    assert out["_floppy_season"] == 5
    assert out["_floppy_episode"] == 12


def _dbz_routes(watched: list[tuple[int, int]], layout: dict[int, int]) -> dict[tuple[str, str], Any]:
    routes: dict[tuple[str, str], Any] = {
        ("GET", "media/movie"): {"results": [], "count": 0},
        ("GET", "media/episode"): {
            "results": [{"item_id": f"tv/tmdb/12971/{s}/{e}", "end_date": "2026-01-02T00:00:00Z", "consumption_id": 1000 + i} for i, (s, e) in enumerate(watched)],
            "count": len(watched),
        },
    }
    for season, total in layout.items():
        routes[("GET", f"media/tv/tmdb/12971/{season}/episodes")] = {
            "results": [{"item_id": f"tv/tmdb/12971/{season}/{n}", "episode_number": n} for n in range(1, total + 1)],
            "count": total,
        }
    return routes


def _anime_history_cfg(*, rewatches: bool = False) -> dict[str, Any]:
    out = {
        "floppy": {"watchlist_name": "Watchlist"},
        "anime_mapping": {"enabled": True, "features": ["history"], "release_tag": "v3"},
        "_cw_pair_feature_options": {"feature": "history", "use_anime_mapping": True},
    }
    if rewatches:
        out["_cw_history_rewatches"] = True
    return out


def _anime_routes(tmdb_id: str, watched: list[tuple[int, int]], layout: dict[int, int]) -> dict[tuple[str, str], Any]:
    routes: dict[tuple[str, str], Any] = {
        ("GET", "media/movie"): {"results": [], "count": 0},
        ("GET", "media/episode"): {
            "results": [{"item_id": f"tv/tmdb/{tmdb_id}/{s}/{e}", "end_date": "2026-01-02T00:00:00Z", "consumption_id": 1000 + i} for i, (s, e) in enumerate(watched)],
            "count": len(watched),
        },
    }
    for season, total in layout.items():
        routes[("GET", f"media/tv/tmdb/{tmdb_id}/{season}/episodes")] = {
            "results": [{"item_id": f"tv/tmdb/{tmdb_id}/{season}/{n}", "episode_number": n} for n in range(1, total + 1)],
            "count": total,
        }
    return routes


def _history_visible_after_post(consumption_id: int = 77) -> Any:
    seen = 0

    def route(_call: dict[str, Any]) -> dict[str, Any]:
        nonlocal seen
        seen += 1
        if seen == 1:
            return {"results": [], "count": 0}
        return {"results": [{"consumption_id": consumption_id, "end_date": "2026-01-02T00:00:00Z"}], "count": 1}

    return route


def _overlord_iv_source(**extra: Any) -> dict[str, Any]:
    return {
        "type": "episode",
        "series_title": "Overlord IV",
        "season": 1,
        "episode": 40,
        "_simkl_episode_number": 1,
        "watched_at": "2026-01-02T00:00:00Z",
        "show_ids": {
            "tmdb": "64196",
            "mal": "48895",
            "anilist": "133844",
            "simkl": "1630649",
        },
        **extra,
    }


def _fake_overlord_edges(tag: str, namespace: str, ident: str) -> list[dict[str, Any]]:
    if tag == "v3" and namespace == "mal" and ident == "48895":
        return [
            {
                "target_provider": "tmdb",
                "target_kind": "show",
                "target_id": "64196",
                "target_scope": "s4",
                "source_range": "1-13",
                "target_range": "1-13",
            }
        ]
    return []


def _mha_final_source(**extra: Any) -> dict[str, Any]:
    return {
        "type": "episode",
        "series_title": "Boku no Hero Academia",
        "season": 8,
        "episode": 1,
        "_simkl_episode_number": 1,
        "watched_at": "2026-01-02T00:00:00Z",
        "show_ids": {
            "tmdb": "308405",
            "imdb": "tt5626028",
            "tvdb": "305074",
            "simkl": "2607190",
            "mal": "60098",
            "anilist": "182896",
            "kitsu": "49279",
            "anidb": "18921",
        },
        **extra,
    }


def _fake_mha_final_edges(tag: str, namespace: str, ident: str) -> list[dict[str, Any]]:
    if tag == "v3" and namespace == "mal" and ident == "60098":
        return [
            {
                "target_provider": "tmdb",
                "target_kind": "show",
                "target_id": "65930",
                "target_scope": "s8",
                "source_range": "1-11",
                "target_range": "1-11",
            }
        ]
    return []


def _one_piece_s23_source(**extra: Any) -> dict[str, Any]:
    return {
        "type": "episode",
        "series_title": "One Piece",
        "season": 23,
        "episode": 1,
        "_simkl_episode_number": 1156,
        "watched_at": "2026-01-02T00:00:00Z",
        "show_ids": {
            "tmdb": "37854",
            "imdb": "tt0388629",
            "tvdb": "81797",
            "simkl": "38636",
            "mal": "21",
            "anilist": "21",
            "kitsu": "12",
            "anidb": "69",
        },
        **extra,
    }


def _one_piece_s16e50_source(**extra: Any) -> dict[str, Any]:
    return _one_piece_s23_source(season=16, episode=50, _simkl_episode_number=628, title="S16E50", **extra)


def test_floppy_history_add_uses_anime_native_coordinate_before_layout_absolute(monkeypatch: Any) -> None:
    from providers.sync.floppy import _history

    monkeypatch.setattr(_history, "query_edges", _fake_overlord_edges)
    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    routes = _anime_routes("64196", [], {1: 13, 2: 13, 3: 13, 4: 13})
    routes[("GET", "media/tv/tmdb/64196/4/1/history")] = _history_visible_after_post()
    routes[("POST", "media/tv/tmdb/64196/4/episodes/1/watch")] = {"consumption_id": 77}
    adapter = AdapterStub(routes, _anime_history_cfg())

    res = _history.add(adapter, [_overlord_iv_source()])

    assert res["count"] == 1
    assert [c["path"] for c in adapter.client.session.calls if c["method"] == "POST"] == ["media/tv/tmdb/64196/4/episodes/1/watch"]
    assert not any(c["path"] == "media/tv/tmdb/64196/1/episodes/1/watch" for c in adapter.client.session.calls)
    assert res["confirmed_keys"] == ["tmdb:64196#s01e40"]


def test_floppy_history_add_prefers_user_anime_override(monkeypatch: Any) -> None:
    from providers.sync.floppy import _history

    monkeypatch.setattr(_history, "query_edges", _fake_overlord_edges)

    def fake_override(namespace: str, ident: str, absolute: int) -> Any:
        if (namespace, ident, absolute) == ("mal", "48895", 1):
            return SimpleNamespace(provider="tmdb", ident="64196", season=9, episode=7)
        return None

    monkeypatch.setattr(_history, "find_source_override", fake_override)
    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    routes = _anime_routes("64196", [], {1: 13, 2: 13, 3: 13, 4: 13, 9: 13})
    routes[("GET", "media/tv/tmdb/64196/9/7/history")] = _history_visible_after_post()
    routes[("POST", "media/tv/tmdb/64196/9/episodes/7/watch")] = {"consumption_id": 77}
    adapter = AdapterStub(routes, _anime_history_cfg())

    res = _history.add(adapter, [_overlord_iv_source()])

    assert res["count"] == 1
    assert [c["path"] for c in adapter.client.session.calls if c["method"] == "POST"] == ["media/tv/tmdb/64196/9/episodes/7/watch"]
    assert not any(c["path"] == "media/tv/tmdb/64196/4/episodes/1/watch" for c in adapter.client.session.calls)


def test_floppy_history_add_ignores_anidb_special_edges_for_regular_anime(monkeypatch: Any) -> None:
    from providers.sync.floppy import _history

    def fake_edges(tag: str, namespace: str, ident: str) -> list[dict[str, Any]]:
        if tag == "v3" and namespace == "anidb" and ident == "18921":
            return [
                {
                    "source_scope": "R",
                    "target_provider": "tmdb",
                    "target_kind": "show",
                    "target_id": "65930",
                    "target_scope": "s8",
                    "source_range": "1-11",
                    "target_range": "1-11",
                },
                {
                    "source_scope": "S",
                    "target_provider": "tmdb",
                    "target_kind": "show",
                    "target_id": "65930",
                    "target_scope": "s0",
                    "source_range": "1",
                    "target_range": "18",
                },
            ]
        return []

    monkeypatch.setattr(_history, "query_edges", fake_edges)
    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    routes = _anime_routes("65930", [], {8: 11})
    routes[("GET", "media/tv/tmdb/65930/8/1/history")] = _history_visible_after_post()
    routes[("POST", "media/tv/tmdb/65930/8/episodes/1/watch")] = {"consumption_id": 77}
    adapter = AdapterStub(routes, _anime_history_cfg())

    res = _history.add(
        adapter,
        [
            {
                "type": "episode",
                "series_title": "Boku no Hero Academia",
                "season": 1,
                "episode": 160,
                "_simkl_episode_number": 1,
                "watched_at": "2026-01-02T00:00:00Z",
                "show_ids": {"tmdb": "65930", "anidb": "18921"},
            }
        ],
    )

    assert res["count"] == 1
    assert [c["path"] for c in adapter.client.session.calls if c["method"] == "POST"] == ["media/tv/tmdb/65930/8/episodes/1/watch"]
    assert not any(c["path"] == "media/tv/tmdb/65930/0/episodes/18/watch" for c in adapter.client.session.calls)


def test_floppy_history_rekeys_anime_native_coordinate_without_false_missing(monkeypatch: Any) -> None:
    from providers.sync.floppy import _history

    monkeypatch.setattr(_history, "query_edges", _fake_overlord_edges)
    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    adapter = AdapterStub(_anime_routes("64196", [(4, 1)], {1: 13, 2: 13, 3: 13, 4: 13}), _anime_history_cfg())

    produced = _history.prepare_source_snapshot([_overlord_iv_source()])
    out = _history.build_index(adapter)

    assert produced == 1
    assert "tmdb:64196#s01e40" in out
    assert "tmdb:64196#s04e01" not in out
    assert out["tmdb:64196#s01e40"]["_floppy_season"] == 4
    assert out["tmdb:64196#s01e40"]["_floppy_episode"] == 1


def test_floppy_history_add_uses_anime_target_tmdb_when_source_tmdb_conflicts(monkeypatch: Any) -> None:
    from providers.sync.floppy import _history

    monkeypatch.setattr(_history, "query_edges", _fake_mha_final_edges)
    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    routes = _anime_routes("65930", [], {8: 11})
    routes[("GET", "media/tv/tmdb/65930/8/1/history")] = _history_visible_after_post()
    routes[("POST", "media/tv/tmdb/65930/8/episodes/1/watch")] = {"consumption_id": 77}
    adapter = AdapterStub(routes, _anime_history_cfg())

    res = _history.add(adapter, [_mha_final_source()])

    assert res["count"] == 1
    assert res["confirmed_keys"] == ["tmdb:308405#s08e01"]
    assert [c["path"] for c in adapter.client.session.calls if c["method"] == "POST"] == ["media/tv/tmdb/65930/8/episodes/1/watch"]
    assert not any("media/tv/tmdb/308405" in c["path"] for c in adapter.client.session.calls)


def test_floppy_history_add_falls_back_when_anime_target_coord_is_not_in_floppy_layout(monkeypatch: Any) -> None:
    from providers.sync.floppy import _history

    monkeypatch.setattr(
        _history,
        "resolve_source_coordinate",
        lambda *_args, **_kwargs: SimpleNamespace(provider="tmdb", ident="37854", season=23, episode=1),
    )
    monkeypatch.setattr(
        _history,
        "show_layout",
        lambda *_args, **_kwargs: [(1, n) for n in range(1, 1156)] + [(23, n) for n in range(1156, 1182)],
    )
    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    routes = {
        ("GET", "media/tv/tmdb/37854/23/episodes"): {
            "results": [{"item_id": f"tv/tmdb/37854/23/{n}", "episode_number": n} for n in range(1156, 1182)],
            "count": 26,
        },
        ("GET", "media/tv/tmdb/37854/23/1156/history"): _history_visible_after_post(),
        ("POST", "media/tv/tmdb/37854/23/episodes/1156/watch"): {"consumption_id": 77},
    }
    adapter = AdapterStub(routes, _anime_history_cfg())

    res = _history.add(adapter, [_one_piece_s23_source()])

    assert res["count"] == 1
    assert res["confirmed_keys"] == ["tmdb:37854#s23e01"]
    assert [c["path"] for c in adapter.client.session.calls if c["method"] == "POST"] == ["media/tv/tmdb/37854/23/episodes/1156/watch"]
    assert not any(c["path"] == "media/tv/tmdb/37854/23/episodes/1/watch" for c in adapter.client.session.calls)


def test_floppy_history_ignores_detached_episode_rows_from_readback(monkeypatch: Any) -> None:
    from providers.sync.floppy import _history

    monkeypatch.setattr(_history, "show_layout", lambda *_args, **_kwargs: [(23, n) for n in range(1156, 1182)])
    _history.prepare_source_snapshot([_one_piece_s23_source()])
    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {"results": [], "count": 0},
            ("GET", "media/episode"): {
                "results": [{"item_id": "tv/tmdb/37854/23/1", "end_date": "2026-01-02T00:00:00Z", "consumption_id": 77}],
                "count": 1,
            },
            ("GET", "media/tv/tmdb/37854/23/episodes"): {
                "results": [{"item_id": f"tv/tmdb/37854/23/{n}", "episode_number": n} for n in range(1156, 1182)],
                "count": 26,
            },
        },
        _anime_history_cfg(),
    )

    out = _history.build_index(adapter)

    assert out == {}


def test_floppy_history_rekeys_real_absolute_row_over_detached_source_row(monkeypatch: Any) -> None:
    from providers.sync.floppy import _history

    monkeypatch.setattr(
        _history,
        "resolve_source_coordinate",
        lambda *_args, **_kwargs: SimpleNamespace(provider="tmdb", ident="37854", season=23, episode=1),
    )
    monkeypatch.setattr(_history, "show_layout", lambda *_args, **_kwargs: [(23, n) for n in range(1156, 1182)])
    _history.prepare_source_snapshot([_one_piece_s23_source()])
    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {"results": [], "count": 0},
            ("GET", "media/episode"): {
                "results": [
                    {"item_id": "tv/tmdb/37854/23/1", "end_date": "2026-01-02T00:00:00Z", "consumption_id": 70},
                    {"item_id": "tv/tmdb/37854/23/1156", "end_date": "2026-01-02T00:00:00Z", "consumption_id": 77},
                ],
                "count": 2,
            },
            ("GET", "media/tv/tmdb/37854/23/episodes"): {
                "results": [{"item_id": f"tv/tmdb/37854/23/{n}", "episode_number": n} for n in range(1156, 1182)],
                "count": 26,
            },
        },
        _anime_history_cfg(),
    )

    out = _history.build_index(adapter)

    assert list(out) == ["tmdb:37854#s23e01"]
    assert out["tmdb:37854#s23e01"]["_floppy_episode"] == 1156
    assert out["tmdb:37854#s23e01"]["_floppy_consumption_id"] == 77


def test_floppy_history_augments_absolute_row_missing_from_global_history(monkeypatch: Any) -> None:
    from providers.sync.floppy import _history

    monkeypatch.setattr(
        _history,
        "resolve_source_coordinate",
        lambda *_args, **_kwargs: SimpleNamespace(provider="tmdb", ident="37854", season=23, episode=1),
    )
    monkeypatch.setattr(_history, "show_layout", lambda *_args, **_kwargs: [(23, n) for n in range(1156, 1182)])
    _history.prepare_source_snapshot([_one_piece_s23_source()])
    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {"results": [], "count": 0},
            ("GET", "media/episode"): {
                "results": [{"item_id": "tv/tmdb/37854/23/1", "end_date": "2026-01-02T00:00:00Z", "consumption_id": 70}],
                "count": 1,
            },
            ("GET", "media/tv/tmdb/37854/23/episodes"): {
                "results": [{"item_id": f"tv/tmdb/37854/23/{n}", "episode_number": n} for n in range(1156, 1182)],
                "count": 26,
            },
            ("GET", "media/tv/tmdb/37854/23/1156/history"): {
                "results": [{"consumption_id": 77, "end_date": "2026-01-02T00:00:00Z"}],
                "count": 1,
            },
        },
        _anime_history_cfg(),
    )

    out = _history.build_index(adapter)

    assert list(out) == ["tmdb:37854#s23e01"]
    assert out["tmdb:37854#s23e01"]["_floppy_episode"] == 1156
    assert out["tmdb:37854#s23e01"]["_floppy_consumption_id"] == 77


def test_floppy_history_rekeys_cross_season_absolute_row(monkeypatch: Any) -> None:
    from providers.sync.floppy import _history

    monkeypatch.setattr(
        _history,
        "resolve_source_coordinate",
        lambda *_args, **_kwargs: SimpleNamespace(provider="tmdb", ident="37854", season=16, episode=50),
    )
    monkeypatch.setattr(_history, "show_layout", lambda *_args, **_kwargs: [(1, n) for n in range(1, 579)] + [(15, n) for n in range(579, 629)])
    _history.prepare_source_snapshot([_one_piece_s16e50_source()])
    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {"results": [], "count": 0},
            ("GET", "media/episode"): {
                "results": [{"item_id": "tv/tmdb/37854/15/628", "end_date": "2026-01-02T00:00:00Z", "consumption_id": 77}],
                "count": 1,
            },
            ("GET", "media/tv/tmdb/37854/16/episodes"): {
                "results": [{"item_id": f"tv/tmdb/37854/16/{n}", "episode_number": n} for n in range(1, 50)],
                "count": 49,
            },
        },
        _anime_history_cfg(),
    )

    out = _history.build_index(adapter)

    assert list(out) == ["tmdb:37854#s16e50"]
    assert out["tmdb:37854#s16e50"]["_floppy_season"] == 15
    assert out["tmdb:37854#s16e50"]["_floppy_episode"] == 628


def test_floppy_history_add_falls_back_to_cross_season_absolute_layout(monkeypatch: Any) -> None:
    from providers.sync.floppy import _history

    monkeypatch.setattr(
        _history,
        "resolve_source_coordinate",
        lambda *_args, **_kwargs: SimpleNamespace(provider="tmdb", ident="37854", season=16, episode=50),
    )
    monkeypatch.setattr(_history, "show_layout", lambda *_args, **_kwargs: [(1, n) for n in range(1, 579)] + [(15, n) for n in range(579, 629)])
    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    adapter = AdapterStub(
        {
            ("GET", "media/tv/tmdb/37854/16/episodes"): {
                "results": [{"item_id": f"tv/tmdb/37854/16/{n}", "episode_number": n} for n in range(1, 50)],
                "count": 49,
            },
            ("GET", "media/tv/tmdb/37854/15/628/history"): _history_visible_after_post(),
            ("POST", "media/tv/tmdb/37854/15/episodes/628/watch"): {"consumption_id": 77},
        },
        _anime_history_cfg(),
    )

    res = _history.add(adapter, [_one_piece_s16e50_source()])

    assert res["count"] == 1
    assert res["confirmed_keys"] == ["tmdb:37854#s16e50"]
    assert [c["path"] for c in adapter.client.session.calls if c["method"] == "POST"] == ["media/tv/tmdb/37854/15/episodes/628/watch"]


def test_floppy_history_uses_anidb_when_native_targets_disagree(monkeypatch: Any) -> None:
    from cw_platform.anime_mapping import episodes
    from providers.sync.floppy import _history

    def fake_edges(tag: str, namespace: str, ident: str, **_kwargs: Any) -> list[dict[str, Any]]:
        if tag != "v3":
            return []
        if namespace == "anidb" and ident == "5841":
            return [
                {
                    "reverse": 1,
                    "source_scope": "R",
                    "target_provider": "tmdb",
                    "target_kind": "show",
                    "target_id": "24835",
                    "target_scope": "s2",
                    "source_range": "1-22",
                    "target_range": "1-22",
                },
                {
                    "reverse": 1,
                    "source_scope": "R",
                    "target_provider": "tvdb",
                    "target_kind": "show",
                    "target_id": "80644",
                    "target_scope": "s2",
                    "source_range": "1-22",
                    "target_range": "1-22",
                },
            ]
        if namespace == "anilist" and ident == "4181":
            return [
                {
                    "reverse": 1,
                    "target_provider": "tmdb",
                    "target_kind": "show",
                    "target_id": "24835",
                    "target_scope": "s0",
                    "source_range": "1-2",
                    "target_range": "3-4",
                },
                {
                    "reverse": 1,
                    "target_provider": "tmdb",
                    "target_kind": "show",
                    "target_id": "24835",
                    "target_scope": "s2",
                    "source_range": "3-24",
                    "target_range": "1-22",
                },
            ]
        return []

    monkeypatch.setattr(episodes, "query_edges", fake_edges)
    monkeypatch.setattr(_history, "query_edges", fake_edges)
    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    routes = _anime_routes("24835", [], {2: 22})
    routes[("GET", "media/tv/tmdb/24835/2/1/history")] = _history_visible_after_post()
    routes[("POST", "media/tv/tmdb/24835/2/episodes/1/watch")] = {"consumption_id": 77}
    adapter = AdapterStub(routes, _anime_history_cfg())

    res = _history.add(
        adapter,
        [
            {
                "type": "episode",
                "series_title": "Clannad: After Story",
                "season": 2,
                "episode": 1,
                "_simkl_episode_number": 1,
                "watched_at": "2026-01-02T00:00:00Z",
                "show_ids": {
                    "tmdb": "248058",
                    "tvdb": "80644",
                    "simkl": "38483",
                    "mal": "4181",
                    "anilist": "4181",
                    "anidb": "5841",
                },
            }
        ],
    )

    assert res["count"] == 1
    assert res["confirmed_keys"] == ["tmdb:248058#s02e01"]
    assert [c["path"] for c in adapter.client.session.calls if c["method"] == "POST"] == ["media/tv/tmdb/24835/2/episodes/1/watch"]
    assert not any("media/tv/tmdb/248058" in c["path"] for c in adapter.client.session.calls)


def test_floppy_history_destination_view_uses_anime_target_tmdb_when_source_tmdb_conflicts(monkeypatch: Any) -> None:
    from providers.sync._mod_FLOPPY import OPS
    from providers.sync.floppy import _history

    monkeypatch.setattr(_history, "query_edges", _fake_mha_final_edges)
    _history.prepare_source_snapshot([])
    cfg = _anime_history_cfg()

    produced = OPS.prepare_source_snapshot(cfg, feature="history", items={"tmdb:308405#s08e01": _mha_final_source()})
    out = OPS.destination_comparison_view(
        cfg,
        feature="history",
        index={
            "tmdb:65930#s08e01": {
                "type": "episode",
                "show_ids": {"tmdb": "65930"},
                "season": 8,
                "episode": 1,
                "watched_at": "2026-01-02T00:00:00Z",
            }
        },
    )

    assert produced == 1
    assert "tmdb:308405#s08e01" in out
    assert "tmdb:65930#s08e01" not in out
    assert out["tmdb:308405#s08e01"]["_floppy_tmdb_id"] == "65930"
    assert out["tmdb:308405#s08e01"]["_floppy_season"] == 8
    assert out["tmdb:308405#s08e01"]["_floppy_episode"] == 1


def test_floppy_history_destination_comparison_view_rekeys_collapsed_anime_coord(monkeypatch: Any) -> None:
    from providers.sync._mod_FLOPPY import OPS
    from providers.sync.floppy import _history

    monkeypatch.setattr(_history, "query_edges", _fake_overlord_edges)
    _history.prepare_source_snapshot([])
    cfg = _anime_history_cfg()
    source = _overlord_iv_source()

    produced = OPS.prepare_source_snapshot(cfg, feature="history", items={"tmdb:64196#s01e40": source})
    out = OPS.destination_comparison_view(
        cfg,
        feature="history",
        index={
            "tmdb:64196#s04e01": {
                "type": "episode",
                "show_ids": {"tmdb": "64196"},
                "season": 4,
                "episode": 1,
                "watched_at": "2026-01-02T00:00:00Z",
            }
        },
    )

    assert produced == 1
    assert "tmdb:64196#s01e40" in out
    assert "tmdb:64196#s04e01" not in out
    assert out["tmdb:64196#s01e40"]["_floppy_season"] == 4
    assert out["tmdb:64196#s01e40"]["_floppy_episode"] == 1


def test_floppy_history_rekeys_anime_event_coordinate_without_layout(monkeypatch: Any) -> None:
    from providers.sync.floppy import _history

    monkeypatch.setattr(_history, "query_edges", _fake_overlord_edges)
    monkeypatch.setattr(_history, "show_layout", lambda *_args, **_kwargs: [])
    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    adapter = AdapterStub(_anime_routes("64196", [(4, 1)], {}), _anime_history_cfg(rewatches=True))

    produced = _history.prepare_source_snapshot([_overlord_iv_source()])
    out = _history.build_index(adapter)

    assert produced == 1
    assert "tmdb:64196#s01e40@1767312000" in out
    assert "tmdb:64196#s04e01@1767312000" not in out
    assert out["tmdb:64196#s01e40@1767312000"]["_floppy_season"] == 4
    assert out["tmdb:64196#s01e40@1767312000"]["_floppy_episode"] == 1


def test_floppy_history_rekeys_anime_special_event_without_stealing_regular(monkeypatch: Any) -> None:
    from providers.sync.floppy import _history

    def fake_edges(tag: str, namespace: str, ident: str) -> list[dict[str, Any]]:
        if tag == "v3" and namespace == "mal" and ident == "11887":
            return [
                {
                    "target_provider": "tmdb",
                    "target_kind": "show",
                    "target_id": "45807",
                    "target_scope": "s1",
                    "source_range": "1-4",
                    "target_range": "1-4",
                }
            ]
        return []

    monkeypatch.setattr(_history, "query_edges", fake_edges)
    monkeypatch.setattr(_history, "show_layout", lambda *_args, **_kwargs: [])
    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    routes = _anime_routes("45807", [(1, 1), (1, 1)], {})
    routes[("GET", "media/episode")]["results"][0]["end_date"] = "2012-07-07T12:00:00Z"
    routes[("GET", "media/episode")]["results"][1]["end_date"] = "2012-11-18T15:30:00Z"
    adapter = AdapterStub(routes, _anime_history_cfg(rewatches=True))
    source = [
        {
            "type": "episode",
            "series_title": "Kokoro Connect",
            "season": 1,
            "episode": 1,
            "watched_at": "2012-07-07T12:00:00Z",
            "show_ids": {"tmdb": "45807", "mal": "11887"},
        },
        {
            "type": "episode",
            "series_title": "Kokoro Connect",
            "season": 0,
            "episode": 1,
            "_simkl_episode_number": 1,
            "watched_at": "2012-11-18T15:30:00Z",
            "show_ids": {"tmdb": "45807", "mal": "11887"},
        },
    ]

    produced = _history.prepare_source_snapshot(source)
    out = _history.build_index(adapter)

    assert produced == 2
    assert "tmdb:45807#s01e01@1341662400" in out
    assert "tmdb:45807#s00e01@1353252600" in out
    assert out["tmdb:45807#s00e01@1353252600"]["_floppy_season"] == 1
    assert out["tmdb:45807#s00e01@1353252600"]["_floppy_episode"] == 1


def test_floppy_history_rekeys_absolute_source_numbering_onto_aired_episodes() -> None:
    from providers.sync.floppy import _history

    _history.prepare_source_snapshot([])
    layout = {1: 39, 2: 35}
    adapter = AdapterStub(_dbz_routes([(2, 5)], layout))

    produced = _history.prepare_source_snapshot(
        [{"type": "episode", "show_ids": {"tmdb": "12971"}, "season": 1, "episode": 44, "_simkl_episode_number": 44}]
    )
    out = _history.build_index(adapter)

    assert produced == 1
    assert "tmdb:12971#s01e44" in out
    assert "tmdb:12971#s02e05" not in out
    assert out["tmdb:12971#s01e44"]["_floppy_season"] == 2
    assert out["tmdb:12971#s01e44"]["_floppy_episode"] == 5
    assert out["tmdb:12971#s01e44"]["_floppy_consumption_id"] == 1000


def test_floppy_history_keeps_native_key_when_source_already_matches() -> None:
    from providers.sync.floppy import _history

    _history.prepare_source_snapshot([])
    adapter = AdapterStub(_dbz_routes([(2, 5)], {1: 39, 2: 35}))

    _history.prepare_source_snapshot([{"type": "episode", "show_ids": {"tmdb": "12971"}, "season": 2, "episode": 5}])
    out = _history.build_index(adapter)

    assert list(out) == ["tmdb:12971#s02e05"]
    assert not any(c["path"].endswith("/episodes") for c in adapter.client.session.calls)


def test_floppy_history_does_not_steal_an_episode_the_source_also_asked_for() -> None:
    from providers.sync.floppy import _history

    _history.prepare_source_snapshot([])
    adapter = AdapterStub(_dbz_routes([(2, 5)], {1: 39, 2: 35}))

    _history.prepare_source_snapshot(
        [
            {"type": "episode", "show_ids": {"tmdb": "12971"}, "season": 1, "episode": 44, "_simkl_episode_number": 44},
            {"type": "episode", "show_ids": {"tmdb": "12971"}, "season": 2, "episode": 5},
        ]
    )
    out = _history.build_index(adapter)

    assert list(out) == ["tmdb:12971#s02e05"]


def test_floppy_history_ignores_a_snapshot_from_another_pair(monkeypatch: Any) -> None:
    from providers.sync.floppy import _history

    monkeypatch.setenv("CW_PAIR_KEY", "pair_simkl_floppy")
    _history.prepare_source_snapshot(
        [{"type": "episode", "show_ids": {"tmdb": "12971"}, "season": 1, "episode": 44, "_simkl_episode_number": 44}]
    )
    monkeypatch.setenv("CW_PAIR_KEY", "pair_floppy_trakt")
    adapter = AdapterStub(_dbz_routes([(2, 5)], {1: 39, 2: 35}))

    out = _history.build_index(adapter)

    assert list(out) == ["tmdb:12971#s02e05"]


def test_floppy_history_add_writes_absolute_episode_to_its_aired_coordinate() -> None:
    from providers.sync.floppy import _history

    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    routes = _dbz_routes([], {1: 39, 2: 35})
    routes[("GET", "media/tv/tmdb/12971/2/5/history")] = _history_visible_after_post()
    routes[("POST", "media/tv/tmdb/12971/2/episodes/5/watch")] = {"consumption_id": 77}
    adapter = AdapterStub(routes)

    res = _history.add(
        adapter,
        [{"type": "episode", "show_ids": {"tmdb": "12971"}, "season": 1, "episode": 44, "_simkl_episode_number": 44, "watched_at": "2026-01-02T00:00:00Z"}],
    )

    assert res["count"] == 1
    assert [c["path"] for c in adapter.client.session.calls if c["method"] == "POST"] == ["media/tv/tmdb/12971/2/episodes/5/watch"]
    assert res["confirmed_keys"] == ["tmdb:12971#s01e44"]


def test_floppy_history_add_marks_translated_episode_unresolved_when_write_is_not_readable() -> None:
    from providers.sync.floppy import _history

    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    routes = _dbz_routes([], {1: 39, 2: 35})
    routes[("GET", "media/tv/tmdb/12971/2/5/history")] = {"results": [], "count": 0}
    routes[("POST", "media/tv/tmdb/12971/2/episodes/5/watch")] = {"consumption_id": 77}
    adapter = AdapterStub(routes)

    res = _history.add(
        adapter,
        [{"type": "episode", "show_ids": {"tmdb": "12971"}, "season": 1, "episode": 44, "_simkl_episode_number": 44, "watched_at": "2026-01-02T00:00:00Z"}],
    )

    assert res["count"] == 0
    assert res["unresolved_keys"] == ["tmdb:12971#s01e44"]
    assert res["unresolved"][0]["reason"] == "floppy_history_write_not_readable"
    assert [c["path"] for c in adapter.client.session.calls if c["method"] == "POST"] == ["media/tv/tmdb/12971/2/episodes/5/watch"]


def test_floppy_history_add_marks_stored_anime_coord_unresolved_when_write_is_not_readable() -> None:
    from providers.sync.floppy import _history

    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    adapter = AdapterStub(
        {
            ("GET", "media/tv/tmdb/64196/4/1/history"): {"results": [], "count": 0},
            ("POST", "media/tv/tmdb/64196/4/episodes/1/watch"): {"consumption_id": 77},
        },
        _anime_history_cfg(),
    )

    res = _history.add(adapter, [_overlord_iv_source(_floppy_season=4, _floppy_episode=1)])

    assert res["count"] == 0
    assert res["unresolved_keys"] == ["tmdb:64196#s01e40"]
    assert res["unresolved"][0]["reason"] == "floppy_history_write_not_readable"
    assert [c["path"] for c in adapter.client.session.calls if c["method"] == "POST"] == ["media/tv/tmdb/64196/4/episodes/1/watch"]


def test_floppy_history_add_marks_stored_anime_coord_unresolved_when_only_old_history_exists() -> None:
    from providers.sync.floppy import _history

    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    adapter = AdapterStub(
        {
            ("GET", "media/tv/tmdb/64196/4/1/history"): {
                "results": [{"consumption_id": 77, "end_date": "2025-01-02T00:00:00Z"}],
                "count": 1,
            },
        },
        _anime_history_cfg(),
    )

    res = _history.add(adapter, [_overlord_iv_source(_floppy_season=4, _floppy_episode=1)])

    assert res["count"] == 0
    assert res["unresolved_keys"] == ["tmdb:64196#s01e40"]
    assert res["unresolved"][0]["reason"] == "floppy_history_write_not_readable"
    assert not any(c["method"] == "POST" for c in adapter.client.session.calls)


def test_floppy_history_add_marks_native_anime_coord_unresolved_when_write_is_not_readable() -> None:
    from providers.sync.floppy import _history

    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    adapter = AdapterStub(
        {
            ("GET", "media/tv/tmdb/64196/1/40/history"): {"results": [], "count": 0},
            ("POST", "media/tv/tmdb/64196/1/episodes/40/watch"): {"consumption_id": 77},
        },
        _anime_history_cfg(),
    )

    res = _history.add(adapter, [_overlord_iv_source(_simkl_episode_number=None)])

    assert res["count"] == 0
    assert res["unresolved_keys"] == ["tmdb:64196#s01e40"]
    assert res["unresolved"][0]["reason"] == "floppy_history_write_not_readable"
    assert [c["path"] for c in adapter.client.session.calls if c["method"] == "POST"] == ["media/tv/tmdb/64196/1/episodes/40/watch"]


def test_floppy_history_unreadable_write_retries_until_blackbox(monkeypatch: Any, tmp_path: Any) -> None:
    from cw_platform.orchestrator import _blackbox
    from cw_platform.orchestrator._applier import apply_add
    from cw_platform.orchestrator._blackbox import record_attempts
    from cw_platform.orchestrator._pairs_blocklist import apply_blocklist

    monkeypatch.setattr(_blackbox, "STATE_DIR", tmp_path)
    event_key = "tmdb:12971#s01e44@1767312000"
    item = {
        "type": "episode",
        "show_ids": {"tmdb": "12971"},
        "season": 1,
        "episode": 44,
        "watched_at": "2026-01-02T00:00:00Z",
        "_cw_event_key": event_key,
        "_cw_rewatch_sync": True,
    }

    class OpsStub:
        def add(self, _cfg: dict[str, Any], _items: list[dict[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
            assert feature == "history"
            assert dry_run is False
            return {
                "ok": False,
                "count": 0,
                "confirmed_keys": [],
                "unresolved_keys": [event_key],
                "unresolved": [{"reason": "floppy_history_write_not_readable", "item": item}],
            }

    res = apply_add(
        dst_ops=OpsStub(),
        cfg={},
        dst_name="FLOPPY",
        feature="history",
        items=[item],
        dry_run=False,
        emit=lambda *_args, **_kwargs: None,
        dbg=lambda *_args, **_kwargs: None,
        chunk_size=100,
        chunk_pause_ms=0,
    )

    assert res["unresolved_keys"] == [event_key]

    pair_key = "FLOPPY-SIMKL"
    assert apply_blocklist(
        None,
        [item],
        dst="FLOPPY",
        feature="history",
        pair_key=pair_key,
        cross_feature_unresolved=True,
    ) == [item]

    record_attempts(
        "FLOPPY",
        "history",
        [event_key],
        reason="apply:add:failed",
        op="add",
        pair=pair_key,
        cfg={"sync": {"blackbox": {"enabled": True, "promote_after": 1, "pair_scoped": True}}},
    )
    assert apply_blocklist(
        None,
        [item],
        dst="FLOPPY",
        feature="history",
        pair_key=pair_key,
        cross_feature_unresolved=True,
    ) == []


def test_floppy_history_add_uses_stored_coordinate_without_layout_lookup() -> None:
    from providers.sync.floppy import _history

    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    adapter = AdapterStub(
        {
            ("GET", "media/tv/tmdb/12971/2/5/history"): {"results": [], "count": 0},
            ("POST", "media/tv/tmdb/12971/2/episodes/5/watch"): {"consumption_id": 77},
        }
    )

    res = _history.add(
        adapter,
        [{"type": "episode", "show_ids": {"tmdb": "12971"}, "season": 1, "episode": 44, "_floppy_season": 2, "_floppy_episode": 5}],
    )

    assert res["count"] == 1
    assert not any(c["path"].endswith("/episodes") for c in adapter.client.session.calls)


def test_floppy_history_add_leaves_regular_shows_untouched() -> None:
    from providers.sync.floppy import _history

    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    adapter = AdapterStub(
        {
            ("GET", "media/tv/tmdb/22/1/2/history"): {"results": [], "count": 0},
            ("POST", "media/tv/tmdb/22/1/episodes/2/watch"): {"consumption_id": 5},
        }
    )

    res = _history.add(adapter, [{"type": "episode", "show_ids": {"tmdb": "22"}, "season": 1, "episode": 2}])

    assert res["count"] == 1
    assert [c["path"] for c in adapter.client.session.calls if c["method"] == "POST"] == ["media/tv/tmdb/22/1/episodes/2/watch"]


def test_floppy_history_remove_targets_the_real_floppy_coordinate() -> None:
    from providers.sync.floppy import _history

    _history.prepare_source_snapshot([])
    _history.reset_layout_cache()
    adapter = AdapterStub({("DELETE", "media/tv/tmdb/12971/2/5/history/1000"): ResponseStub(204, {})})

    res = _history.remove(
        adapter,
        [{"type": "episode", "show_ids": {"tmdb": "12971"}, "season": 1, "episode": 44, "_floppy_season": 2, "_floppy_episode": 5, "_floppy_consumption_id": 1000}],
    )

    assert res["count"] == 1
    assert adapter.client.session.calls[0]["path"] == "media/tv/tmdb/12971/2/5/history/1000"


def test_floppy_history_movie_index_skips_unwatched_rows() -> None:
    from providers.sync.floppy import _history

    _history.prepare_source_snapshot([])
    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {
                "results": [
                    {"item_id": "movie/tmdb/11", "status": 3, "end_date": "2026-01-01T00:00:00Z"},
                    {"item_id": "movie/tmdb/12", "status": 0, "end_date": None},
                    {"item_id": "movie/tmdb/13", "status": 1, "end_date": "2026-01-05T00:00:00Z"},
                ],
                "count": 3,
            },
            ("GET", "media/episode"): {"results": [], "count": 0},
        }
    )

    out = _history.build_index(adapter)

    assert sorted(out) == ["tmdb:11", "tmdb:13"]


def test_floppy_paged_stops_when_endpoint_ignores_offset() -> None:
    from providers.sync.floppy._common import paged

    page = {"results": [{"item_id": f"movie/tmdb/{n}"} for n in range(200)]}
    adapter = AdapterStub({("GET", "media/movie"): page})

    rows = paged(adapter, "media/movie")

    assert len(rows) == 200
    assert len(adapter.client.session.calls) == 2


def test_floppy_paged_stops_on_unpaginated_list_response() -> None:
    from providers.sync.floppy._common import paged

    adapter = AdapterStub({("GET", "media/episode"): [{"item_id": f"tv/tmdb/1/1/{n}"} for n in range(300)]})

    rows = paged(adapter, "media/episode")

    assert len(rows) == 300
    assert len(adapter.client.session.calls) == 1


def test_floppy_paged_still_walks_real_pages() -> None:
    from providers.sync.floppy._common import paged

    def route(call: dict[str, Any]) -> Any:
        offset = int(call["params"]["offset"])
        rows = [{"item_id": f"movie/tmdb/{offset + n}"} for n in range(min(200, 450 - offset))]
        return {"results": rows, "count": 450}

    adapter = AdapterStub({("GET", "media/movie"): route})

    rows = paged(adapter, "media/movie")

    assert len(rows) == 450
    assert len(adapter.client.session.calls) == 3


def _grouped_anime_row(tmdb_id: str) -> dict[str, Any]:
    return {"item_id": f"tv/tmdb/{tmdb_id}", "item": {"media_id": tmdb_id, "source": "tmdb", "media_type": "tv", "library_media_type": "anime"}}


def _plain_tv_row(tmdb_id: str) -> dict[str, Any]:
    return {"item_id": f"tv/tmdb/{tmdb_id}", "item": {"media_id": tmdb_id, "source": "tmdb", "media_type": "tv", "library_media_type": "tv"}}


def _bucket_episode() -> dict[str, Any]:
    return {"type": "episode", "show_ids": {"tmdb": "209867", "mal": "52991"}, "season": 1, "episode": 1, "watched_at": "2026-01-01T10:00:00Z"}


def _bucket_history_calls(adapter: AdapterStub) -> list[tuple[str, str, Any, Any]]:
    return [(c["method"], c["path"], c.get("params"), c.get("json")) for c in adapter.client.session.calls]


def test_floppy_history_add_follows_show_already_in_anime_library() -> None:
    from providers.sync.floppy import _history

    _history.prepare_source_snapshot([])
    adapter = AdapterStub(
        {
            ("GET", "media/tv"): {"results": [], "count": 0},
            ("GET", "media/anime"): {"results": [_grouped_anime_row("209867"), {"item_id": "anime/mal/21", "item": {"media_id": "21", "source": "mal", "media_type": "anime"}}], "count": 2},
            ("POST", "media/tv/tmdb/209867/1/episodes/1/watch"): {"consumption_id": 5},
        }
    )

    res = _history.add(adapter, [{"type": "episode", "show_ids": {"tmdb": "209867"}, "season": 1, "episode": 1, "watched_at": "2026-01-01T10:00:00Z"}])

    assert res["count"] == 1
    calls = _bucket_history_calls(adapter)
    assert ("GET", "media/tv/tmdb/209867/1/1/history", {"library_media_type": "anime", "limit": 200, "offset": 0}, None) in calls
    assert ("POST", "media/tv/tmdb/209867/1/episodes/1/watch", None, {"end_date": "2026-01-01T10:00:00Z", "library_media_type": "anime"}) in calls


@pytest.mark.parametrize(("enabled", "expected"), [(True, {"end_date": "2026-01-01T10:00:00Z", "library_media_type": "anime"}), (False, {"end_date": "2026-01-01T10:00:00Z"})])
def test_floppy_history_add_sends_new_anime_to_anime_library_only_when_enabled(enabled: bool, expected: dict[str, Any]) -> None:
    from providers.sync.floppy import _history

    _history.prepare_source_snapshot([])
    adapter = AdapterStub(
        {("POST", "media/tv/tmdb/209867/1/episodes/1/watch"): {"consumption_id": 5}},
        {"floppy": {"watchlist_name": "Watchlist", "anime_library": enabled}},
    )

    res = _history.add(adapter, [_bucket_episode()])

    assert res["count"] == 1
    assert [c["json"] for c in adapter.client.session.calls if c["method"] == "POST"] == [expected]
    assert [c["path"] for c in adapter.client.session.bucket_lookups] == ["media/tv", "media/anime"]


def test_floppy_history_add_keeps_anime_in_tv_when_show_already_lives_there() -> None:
    from providers.sync.floppy import _history

    _history.prepare_source_snapshot([])
    adapter = AdapterStub(
        {
            ("GET", "media/tv"): {"results": [_plain_tv_row("209867")], "count": 1},
            ("POST", "media/tv/tmdb/209867/1/episodes/1/watch"): {"consumption_id": 5},
        },
        {"floppy": {"watchlist_name": "Watchlist", "anime_library": True}},
    )

    res = _history.add(adapter, [_bucket_episode()])

    assert res["count"] == 1
    assert [c["json"] for c in adapter.client.session.calls if c["method"] == "POST"] == [{"end_date": "2026-01-01T10:00:00Z"}]


def test_floppy_history_add_prefers_anime_library_when_show_is_in_both() -> None:
    from providers.sync.floppy._common import show_bucket

    adapter = AdapterStub(
        {
            ("GET", "media/tv"): {"results": [_plain_tv_row("209867")], "count": 1},
            ("GET", "media/anime"): {"results": [_grouped_anime_row("209867")], "count": 1},
        }
    )

    assert show_bucket(adapter, "209867") == "anime"
    assert show_bucket(adapter, "1") is None


def test_floppy_history_remove_in_anime_library_removes_latest_watch() -> None:
    from providers.sync.floppy import _history

    _history.prepare_source_snapshot([])
    adapter = AdapterStub(
        {
            ("GET", "media/anime"): {"results": [_grouped_anime_row("209867")], "count": 1},
            ("GET", "media/tv/tmdb/209867/1/1/history"): {"results": [{"consumption_id": 9, "end_date": "2026-01-01T10:00:00Z"}], "count": 1},
            ("DELETE", "media/tv/tmdb/209867/1/episodes/1/watch"): ResponseStub(204, {}),
        }
    )

    res = _history.remove(adapter, [{"type": "episode", "show_ids": {"tmdb": "209867"}, "season": 1, "episode": 1, "_floppy_consumption_id": 9}])

    assert res["count"] == 1
    assert [(c["method"], c["path"], c.get("params")) for c in adapter.client.session.calls if c["method"] == "DELETE"] == [("DELETE", "media/tv/tmdb/209867/1/episodes/1/watch", {"library_media_type": "anime"})]


def test_floppy_ratings_use_anime_route_for_shows_in_anime_library() -> None:
    from providers.sync.floppy import _ratings

    adapter = AdapterStub(
        {
            ("GET", "media/anime"): {"results": [_grouped_anime_row("209867")], "count": 1},
            ("POST", "media/tv"): ResponseStub(409, {"detail": "exists"}),
            ("PATCH", "media/anime/tmdb/209867"): {"score": 9},
            ("PATCH", "media/tv/tmdb/209867/1/episodes/2/score"): {"score": "8"},
        }
    )

    res = _ratings.add(
        adapter,
        [
            {"type": "show", "ids": {"tmdb": "209867"}, "rating": 9},
            {"type": "episode", "show_ids": {"tmdb": "209867"}, "season": 1, "episode": 2, "rating": 8},
        ],
    )

    assert res["count"] == 2
    writes = [(c["method"], c["path"], c.get("json")) for c in adapter.client.session.calls if c["method"] != "GET"]
    assert ("POST", "media/tv", {"source": "tmdb", "media_id": "209867", "status": 0, "score": 9.0, "library_media_type": "anime"}) in writes
    assert ("PATCH", "media/anime/tmdb/209867", {"score": 9.0}) in writes
    assert ("PATCH", "media/tv/tmdb/209867/1/episodes/2/score", {"score": 8.0, "library_media_type": "anime"}) in writes


def test_floppy_ratings_index_reads_shows_from_anime_library() -> None:
    from providers.sync.floppy import _ratings

    row = {**_grouped_anime_row("209867"), "score": 9.0}
    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {"results": [], "count": 0},
            ("GET", "media/tv"): {"results": [], "count": 0},
            ("GET", "media/anime"): {"results": [row, {"item_id": "anime/mal/21", "score": 8.0}], "count": 2},
        }
    )

    out = _ratings.build_index(adapter)

    assert list(out) == ["tmdb:209867#show"]
    assert out["tmdb:209867#show"]["type"] == "show"
    assert out["tmdb:209867#show"]["rating"] == 9.0


def test_floppy_watchlist_add_places_new_anime_show_in_anime_library() -> None:
    from providers.sync.floppy import _watchlist

    adapter = AdapterStub(
        {
            ("GET", "lists"): {"results": [{"id": 1, "name": "Watchlist"}], "count": 1},
            ("PUT", "media/tv/tmdb/209867/lists/1"): {},
        },
        {"floppy": {"watchlist_name": "Watchlist", "anime_library": True}},
    )

    res = _watchlist.add(adapter, [{"type": "show", "ids": {"tmdb": "209867", "anilist": "154587"}}])

    assert res["count"] == 1
    assert [(c["path"], c.get("json")) for c in adapter.client.session.calls if c["method"] == "PUT"] == [("media/tv/tmdb/209867/lists/1", {"library_media_type": "anime"})]


def test_floppy_ratings_create_untracked_season_score_in_anime_library() -> None:
    from providers.sync.floppy import _ratings

    adapter = AdapterStub(
        {
            ("GET", "media/anime"): {"results": [_grouped_anime_row("209867")], "count": 1},
            ("PATCH", "media/tv/tmdb/209867/1"): ResponseStub(404, {"detail": "Season not found or not tracked."}),
            ("POST", "media/season"): ResponseStub(201, {"id": 1}),
        }
    )

    res = _ratings.add(adapter, [{"type": "season", "show_ids": {"tmdb": "209867"}, "season": 1, "rating": 7}])

    assert res["count"] == 1
    writes = [(c["method"], c["path"], c.get("params"), c.get("json")) for c in adapter.client.session.calls if c["method"] != "GET"]
    assert writes == [
        ("PATCH", "media/tv/tmdb/209867/1", {"library_media_type": "anime"}, {"score": 7.0}),
        ("POST", "media/season", None, {"source": "tmdb", "media_id": "209867", "season_number": 1, "status": 0, "score": 7.0, "library_media_type": "anime"}),
    ]


def test_floppy_history_remove_in_anime_library_reports_failed_history_read() -> None:
    from providers.sync.floppy import _history

    _history.prepare_source_snapshot([])
    adapter = AdapterStub(
        {
            ("GET", "media/anime"): {"results": [_grouped_anime_row("209867")], "count": 1},
            ("GET", "media/tv/tmdb/209867/1/1/history"): ResponseStub(500, {"detail": "Internal Server Error."}),
        }
    )

    res = _history.remove(adapter, [{"type": "episode", "show_ids": {"tmdb": "209867"}, "season": 1, "episode": 1, "_floppy_consumption_id": 9}])

    assert res["count"] == 0
    assert res["unresolved"][0]["reason"] == "floppy_history_remove_failed"
    assert [c["method"] for c in adapter.client.session.calls if c["method"] == "DELETE"] == []


def test_floppy_collection_add_follows_show_already_in_anime_library() -> None:
    from providers.sync.floppy import _collection

    adapter = AdapterStub(
        {
            ("GET", "media/anime"): {"results": [_grouped_anime_row("209867")], "count": 1},
            ("POST", "media/tv"): ResponseStub(409, {"detail": "Conflict. Media is already tracked."}),
            ("GET", "media/anime/tmdb/209867"): {"id": 7564, "item_id": "anime/tmdb/209867"},
            ("POST", "collection"): {"id": 1},
        }
    )

    res = _collection.add(adapter, [{"type": "show", "ids": {"tmdb": "209867"}}])

    assert res["count"] == 1
    calls = [(c["method"], c["path"], c.get("json")) for c in adapter.client.session.calls if c["path"] != "media/anime"]
    assert calls == [
        ("POST", "media/tv", {"source": "tmdb", "media_id": "209867", "status": 0, "library_media_type": "anime"}),
        ("GET", "media/anime/tmdb/209867", None),
        ("POST", "collection", {"item_id": "7564", "media_type": "digital"}),
    ]


def test_floppy_collection_add_resolves_tracked_anime_season() -> None:
    from providers.sync.floppy import _collection

    adapter = AdapterStub(
        {
            ("GET", "media/anime"): {"results": [_grouped_anime_row("209867")], "count": 1},
            ("POST", "media/season"): ResponseStub(409, {"detail": "Conflict. Media is already tracked."}),
            ("GET", "media/tv/tmdb/209867/1"): {"id": 7565, "item_id": "tv/tmdb/209867/1"},
            ("POST", "collection"): {"id": 2},
        }
    )

    res = _collection.add(adapter, [{"type": "season", "show_ids": {"tmdb": "209867"}, "season": 1}])

    assert res["count"] == 1
    calls = [(c["method"], c["path"], c.get("params"), c.get("json")) for c in adapter.client.session.calls if c["path"] != "media/anime"]
    assert calls == [
        ("POST", "media/season", None, {"source": "tmdb", "media_id": "209867", "status": 0, "library_media_type": "anime", "season_number": 1}),
        ("GET", "media/tv/tmdb/209867/1", {"library_media_type": "anime"}, None),
        ("POST", "collection", None, {"item_id": "7565", "media_type": "digital"}),
    ]


def test_floppy_collection_add_keeps_plain_tv_show_unflagged() -> None:
    from providers.sync.floppy import _collection

    adapter = AdapterStub(
        {
            ("GET", "media/tv"): {"results": [_plain_tv_row("1396")], "count": 1},
            ("POST", "media/tv"): {"id": 1716, "item_id": "tv/tmdb/1396"},
            ("POST", "collection"): {"id": 3},
        }
    )

    res = _collection.add(adapter, [{"type": "show", "ids": {"tmdb": "1396"}}])

    assert res["count"] == 1
    posts = [(c["path"], c.get("json")) for c in adapter.client.session.calls if c["method"] == "POST"]
    assert posts == [("media/tv", {"source": "tmdb", "media_id": "1396", "status": 0}), ("collection", {"item_id": "1716", "media_type": "digital"})]


def _collection_row(entry_id: int, tmdb: int) -> dict[str, Any]:
    return {"id": entry_id, "media_type": "digital", "item": {"media_type": "movie", "source": "tmdb", "media_id": str(tmdb), "title": f"Movie {tmdb}"}}


def test_floppy_collection_read_fails_when_pages_are_missing() -> None:
    from providers.sync.floppy import _collection

    def page(call):
        offset = int(call["params"]["offset"])
        rows = [_collection_row(i, 1000 + i) for i in range(200)] if offset == 0 else []
        return {"results": rows, "pagination": {"total": 450, "limit": 200, "offset": offset}}

    with pytest.raises(RuntimeError, match="floppy_incomplete_read:collection:200/450"):
        _collection.build_index(AdapterStub({("GET", "collection"): page}))


def test_floppy_collection_read_follows_pagination_total() -> None:
    from providers.sync.floppy import _collection

    def page(call):
        offset = int(call["params"]["offset"])
        rows = [_collection_row(i, 1000 + i) for i in range(offset, min(offset + 200, 450))]
        return {"results": rows, "pagination": {"total": 450, "limit": 200, "offset": offset}}

    adapter = AdapterStub({("GET", "collection"): page})
    assert len(_collection.build_index(adapter)) == 450
    assert [c["params"]["offset"] for c in adapter.client.session.calls] == [0, 200, 400]


def test_floppy_paged_without_require_total_keeps_old_behaviour() -> None:
    from providers.sync.floppy._common import paged

    def page(call):
        offset = int(call["params"]["offset"])
        return {"results": [{"id": 1}] * 200 if offset == 0 else [], "pagination": {"total": 450}}

    assert len(paged(AdapterStub({("GET", "lists"): page}), "lists")) == 200


def test_floppy_collection_add_timeout_marks_only_that_item_unresolved() -> None:
    import requests

    from providers.sync.floppy import _collection

    def media(call):
        if call["json"]["media_id"] == "11":
            raise requests.exceptions.ReadTimeout("read timed out")
        return {"id": 42}

    adapter = AdapterStub({("POST", "media/movie"): media, ("POST", "collection"): {"id": 91}})
    res = _collection.add(adapter, [{"type": "movie", "ids": {"tmdb": "11"}}, {"type": "movie", "ids": {"tmdb": "22"}}])

    assert res["count"] == 1
    assert res["confirmed_keys"] == ["tmdb:22"]
    assert res["unresolved_keys"] == ["tmdb:11"]
    assert "floppy_error" in res["unresolved"][0]["reason"] or "timed out" in res["unresolved"][0]["reason"]
    assert [c["json"].get("item_id") for c in adapter.client.session.calls if c["path"] == "collection"] == ["42"]


def test_applier_logs_chunk_retries() -> None:
    from cw_platform.orchestrator import _applier

    events: list[tuple[str, dict]] = []
    attempts = {"n": 0}

    def flaky(_items):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise TimeoutError("slow")
        return {"ok": True, "count": len(_items), "confirmed_keys": []}

    _applier._apply_chunked("apply:add", dst="FLOPPY", feature="collection", items=[{"type": "movie", "ids": {"tmdb": "1"}}], call=flaky, emit=lambda ev, **kw: events.append((ev, kw)), dbg=lambda *a, **k: None, chunk_size=0, chunk_pause_ms=0)

    retries = [kw for ev, kw in events if ev == "apply:add:retry"]
    assert retries == [{"dst": "FLOPPY", "feature": "collection", "attempt": 1, "error": "TimeoutError: slow"}]


def test_applier_retry_survives_a_failing_retry_logger() -> None:
    from cw_platform.orchestrator import _applier

    attempts = {"n": 0}

    def flaky():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise TimeoutError("slow")
        return "ok"

    def broken_logger(_attempt, _exc):
        raise ValueError("logger broke")

    assert _applier._retry(flaky, base_sleep=0, on_retry=broken_logger) == "ok"
    assert attempts["n"] == 2


def test_applier_retry_logger_does_not_swallow_cancel() -> None:
    from cw_platform.orchestrator import _applier
    from cw_platform.run_control import SyncCancelled

    def cancelling_logger(_attempt, _exc):
        raise SyncCancelled()

    with pytest.raises(SyncCancelled):
        _applier._retry(lambda: (_ for _ in ()).throw(TimeoutError("slow")), base_sleep=0, on_retry=cancelling_logger)
