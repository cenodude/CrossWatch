# CrossWatch test scripts
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


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

    def request(self, method: str, url: str, **kwargs: Any) -> ResponseStub:
        path = url.split("/api/v1/", 1)[1].strip("/")
        call = {"method": method.upper(), "path": path, **kwargs}
        self.calls.append(call)
        route = self.routes.get((method.upper(), path))
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


def test_floppy_capabilities_expose_only_requested_features() -> None:
    from providers.sync._mod_FLOPPY import OPS

    features = OPS.features()
    caps = OPS.capabilities()

    assert features == {"watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": False}
    assert caps["watchlist"]["custom_lists"] is True
    assert caps["ratings"]["scale"] == "0-10"
    assert caps["progress"]["read"] is True
    assert caps["progress"]["write"] is True


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


def test_floppy_ratings_read_and_write_native_scale() -> None:
    from providers.sync.floppy import _ratings

    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {"results": [{"item_id": "movie/tmdb/11", "score": 8.5}, {"item_id": "movie/tmdb/12", "score": 0}], "count": 2},
            ("GET", "media/tv"): {"results": [], "count": 0},
            ("POST", "media/movie"): {"item_id": "movie/tmdb/11", "score": 9.0},
        }
    )

    out = _ratings.build_index(adapter)
    res = _ratings.add(adapter, [{"type": "movie", "ids": {"tmdb": "11"}, "rating": 9.0}])

    assert out["tmdb:11"]["rating"] == 8.5
    assert "tmdb:12" not in out
    assert res["count"] == 1
    assert adapter.client.session.calls[-1]["json"] == {"source": "tmdb", "media_id": "11", "status": 0, "score": 9.0}


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
            {"type": "episode", "show_ids": {"tmdb": "22"}, "season": 1, "episode": 2, "rating": 8.0},
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


def test_floppy_history_reads_movies_and_episodes() -> None:
    from providers.sync.floppy import _history

    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {
                "results": [
                    {"item_id": "movie/tmdb/11", "status": 3, "end_date": "2026-01-01T00:00:00Z", "consumption_id": 41},
                    {"item_id": "movie/tmdb/11", "status": 3, "end_date": "2026-01-03T00:00:00Z", "consumption_id": 43},
                ],
                "count": 2,
            },
            ("GET", "media/episode"): {"results": [{"item_id": "tv/tmdb/22/1/2", "end_date": "2026-01-02T00:00:00Z", "consumption_id": 42}], "count": 1},
        }
    )

    out = _history.build_index(adapter)

    assert out["tmdb:11"]["watched_at"] == "2026-01-03T00:00:00Z"
    assert out["tmdb:11"]["_floppy_consumption_id"] == 43
    assert out["tmdb:22#s01e02"]["_floppy_consumption_id"] == 42


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


def test_floppy_movie_history_rewatch_index_reads_each_movie_play() -> None:
    from providers.sync.floppy import _history

    adapter = AdapterStub(
        {
            ("GET", "media/movie"): {
                "results": [{"item_id": "movie/tmdb/11", "status": 3, "end_date": "2026-01-03T00:00:00Z"}],
                "count": 1,
            },
            ("GET", "media/movie/tmdb/11/history"): {
                "results": [
                    {"consumption_id": 51, "end_date": "2026-01-01T00:00:00Z"},
                    {"consumption_id": 52, "end_date": "2026-01-03T00:00:00Z"},
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


def test_floppy_history_remove_uses_exact_consumption_id() -> None:
    from providers.sync.floppy import _history

    adapter = AdapterStub({("DELETE", "media/movie/tmdb/11/history/41"): ResponseStub(204, {})})

    res = _history.remove(adapter, [{"type": "movie", "ids": {"tmdb": "11"}, "_floppy_consumption_id": 41}])

    assert res["count"] == 1
    assert adapter.client.session.calls[0]["path"] == "media/movie/tmdb/11/history/41"


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
    routes[("GET", "media/tv/tmdb/12971/2/5/history")] = {"results": [], "count": 0}
    routes[("POST", "media/tv/tmdb/12971/2/episodes/5/watch")] = {"consumption_id": 77}
    adapter = AdapterStub(routes)

    res = _history.add(
        adapter,
        [{"type": "episode", "show_ids": {"tmdb": "12971"}, "season": 1, "episode": 44, "_simkl_episode_number": 44, "watched_at": "2026-01-02T00:00:00Z"}],
    )

    assert res["count"] == 1
    assert [c["path"] for c in adapter.client.session.calls if c["method"] == "POST"] == ["media/tv/tmdb/12971/2/episodes/5/watch"]
    assert res["confirmed_keys"] == ["tmdb:12971#s01e44"]


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
