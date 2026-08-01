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

    assert features == {"watchlist": True, "ratings": True, "history": True, "progress": False, "playlists": False}
    assert caps["watchlist"]["custom_lists"] is True
    assert caps["ratings"]["scale"] == "0-10"
    assert caps["progress"]["read"] is False


def test_floppy_module_uses_shared_rate_limiter() -> None:
    from providers.sync._mod_FLOPPY import FLOPPYModule

    mod = FLOPPYModule({"floppy": {"server_url": "http://x", "api_token": "t", "rate_limit": {"get_per_sec": 12, "post_per_sec": 9}}})

    assert mod.client.session._rate_limiter is not None
    assert mod.client.session._rate_limiter_meta == {"get_per_sec": 12.0, "post_per_sec": 9.0}


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
