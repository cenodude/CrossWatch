# CrossWatch test scripts
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

import pytest

SERVER = "http://silo.local:8090"
PROBLEM = "https://siloserver.org/docs/api/v2/problems/"


@dataclass
class ResponseStub:
    status_code: int = 200
    payload: Any = None
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def text(self) -> str:
        return "" if self.payload is None else "x"

    def json(self) -> Any:
        return self.payload


def _page(items: list[dict[str, Any]], cursor: str | None = None) -> ResponseStub:
    page: dict[str, Any] = {"has_more": bool(cursor)}
    if cursor:
        page["next_cursor"] = cursor
    return ResponseStub(200, {"items": items, "page": page})


MOVIES = [
    {"content_id": "movie-tmdb-100", "type": "movie", "title": "Alpha", "year": 2024, "runtime": 100},
    {"content_id": "movie-imdb-tt0000200", "type": "movie", "title": "Beta", "year": 2021, "runtime": 90},
    {"content_id": "local-abc", "type": "movie", "title": "Home video"},
]
SERIES = [{"content_id": "series-tvdb-300", "type": "series", "title": "Gamma", "year": 2020}]
EPISODES = [
    {"content_id": "episode-tvdb-300-1-1", "type": "episode", "title": "Pilot", "series_id": "series-tvdb-300", "series_title": "Gamma", "season_number": 1, "episode_number": 1, "runtime": 45},
    {"content_id": "149382054478429162", "type": "episode", "title": "Two", "series_id": "series-tvdb-300", "series_title": "Gamma", "season_number": 1, "episode_number": 2, "runtime": 44},
]


class FakeServer:
    def __init__(self) -> None:
        self.routes: dict[tuple[str, str], Callable[[dict[str, Any]], ResponseStub]] = {}
        self.calls: list[dict[str, Any]] = []
        catalog = {"movie": MOVIES, "series": SERIES, "episode": EPISODES}
        self.on("GET", "/catalog", lambda kw: _page(catalog[kw["params"]["type"]]))
        self.on("GET", "/watchlist/titles", lambda kw: ResponseStub(409, {"type": PROBLEM + "capability_disabled"}))
        self.on("GET", "/catalog/items/movie-imdb-tt0000200", lambda kw: ResponseStub(200, {"content_id": "movie-imdb-tt0000200", "tmdb_id": "200", "imdb_id": "tt0000200"}))

    def on(self, method: str, path: str, handler: Callable[[dict[str, Any]], ResponseStub]) -> None:
        self.routes[(method, path)] = handler

    def __call__(self, session: Any, method: str, url: str, **kwargs: Any) -> ResponseStub:
        path = url.split("/api/v2", 1)[1]
        self.calls.append({"method": method, "path": path, **kwargs})
        handler = self.routes.get((method, path))
        if handler is None:
            raise AssertionError(f"unexpected {method} {path}")
        return handler(kwargs)

    def count(self, method: str, path: str) -> int:
        return sum(1 for c in self.calls if c["method"] == method and c["path"] == path)


@pytest.fixture()
def silo(monkeypatch: pytest.MonkeyPatch, tmp_path: Any):
    from providers.sync._mod_SILO import SILOModule
    from providers.sync.silo import _common

    server = FakeServer()
    monkeypatch.setattr(_common, "request_with_auth", server)
    monkeypatch.setattr(_common, "STATE_DIR", tmp_path)
    _common._LIBRARIES.clear()
    cfg = {"silo": {"server_url": SERVER, "access_token": "a", "refresh_token": "r", "profile_id": "p-1", "user_id": "1"}, "_cw_provider_instance": "default"}
    adapter = SILOModule(cfg)
    yield adapter, server
    _common._LIBRARIES.clear()


def test_manifest_registration_and_ops(silo: Any) -> None:
    from cw_platform.modules_registry import MODULES, load_sync_ops, provider_display_label, sync_provider_supports_feature

    assert MODULES["SYNC"]["_mod_SILO"] == "providers.sync._mod_SILO"
    ops = load_sync_ops("SILO")
    assert ops.name() == "SILO" and provider_display_label("SILO") == "Silo"
    assert dict(ops.features()) == {"watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": True, "collection": True}
    assert sync_provider_supports_feature("SILO", "history") is True
    caps = ops.capabilities()
    assert caps["ratings"]["types"]["episodes"] is False
    assert caps["history"]["types"] == {"movies": True, "shows": False, "seasons": False, "episodes": True}
    assert ops.is_configured(silo[0].config) is True
    assert ops.is_configured({"silo": {"server_url": SERVER}}) is False


def test_watchlist_index_maps_content_ids_and_tolerates_disabled_titles(silo: Any) -> None:
    from providers.sync.silo import _watchlist

    adapter, server = silo
    server.on("GET", "/watchlist", lambda kw: _page([{"content_id": "movie-tmdb-100"}, {"content_id": "series-tvdb-300"}, {"content_id": "local-abc"}]))

    idx = _watchlist.build_index(adapter)

    assert idx["tmdb:100"] == {"type": "movie", "ids": {"tmdb": "100"}, "title": "Alpha", "year": 2024}
    assert idx["tvdb:300"]["type"] == "show"
    assert len(idx) == 2


def test_watchlist_keeps_finished_series_still_on_watchlist(silo: Any) -> None:
    from providers.sync.silo import _watchlist

    adapter, server = silo
    rows = [{"content_id": "series-tvdb-300"}]
    server.on("GET", "/watchlist", lambda kw: _page(rows))
    server.on("GET", "/watchlist/series-tvdb-300/membership", lambda kw: ResponseStub(200, {"is_member": True}))
    assert "tvdb:300" in _watchlist.build_index(adapter)

    rows.clear()
    assert "tvdb:300" in _watchlist.build_index(adapter)
    assert server.count("GET", "/watchlist/series-tvdb-300/membership") == 1

    server.on("GET", "/watchlist/series-tvdb-300/membership", lambda kw: ResponseStub(200, {"is_member": False}))
    assert _watchlist.build_index(adapter) == {}
    assert _watchlist.build_index(adapter) == {}
    assert server.count("GET", "/watchlist/series-tvdb-300/membership") == 2


def test_failed_read_raises_instead_of_empty_index(silo: Any) -> None:
    from providers.sync.silo import _common, _watchlist

    adapter, server = silo
    server.on("GET", "/watchlist", lambda kw: ResponseStub(500, {}))
    with pytest.raises(_common.SiloSyncError):
        _watchlist.build_index(adapter)

    server.on("GET", "/watchlist", lambda kw: _page([], cursor="same"))
    with pytest.raises(_common.SiloSyncError, match="invalid_continuation"):
        _watchlist.build_index(adapter)


def test_watchlist_writes_library_items_and_reports_unresolved(silo: Any) -> None:
    adapter, server = silo
    server.on("PUT", "/watchlist/movie-tmdb-100", lambda kw: ResponseStub(204, None))
    server.on("PUT", "/watchlist/series-tvdb-300", lambda kw: ResponseStub(204, None))
    server.on("PUT", "/watchlist/titles/movie/555", lambda kw: ResponseStub(409, {"type": PROBLEM + "capability_disabled"}))

    res = adapter.add("watchlist", [
        {"type": "movie", "ids": {"tmdb": "100", "imdb": "tt9"}},
        {"type": "show", "ids": {"tvdb": "300"}},
        {"type": "movie", "ids": {"tmdb": "555"}},
        {"type": "episode", "show_ids": {"tvdb": "300"}, "season": 1, "episode": 1},
    ])

    assert res["confirmed_keys"] == ["tmdb:100", "tvdb:300"]
    assert {u["key"]: u["status"] for u in res["unresolved"]} == {"tmdb:555": "not_in_library", "tvdb:300#s01e01": "unsupported_type"}
    assert server.count("GET", "/catalog") == 3


def test_watchlist_uses_titles_api_for_missing_titles_when_enabled(silo: Any) -> None:
    adapter, server = silo
    server.on("PUT", "/watchlist/titles/movie/555", lambda kw: ResponseStub(200, {"tmdb_id": 555, "media_type": "movie", "added_at": "2026-01-01T00:00:00Z", "request": {}}))

    res = adapter.add("watchlist", [{"type": "movie", "ids": {"tmdb": "555"}}])

    assert res["confirmed_keys"] == ["tmdb:555"]


def test_ratings_star_mapping_and_written_value_round_trip(silo: Any) -> None:
    from providers.sync.silo import _ratings

    adapter, server = silo
    sent: list[Any] = []
    server.on("PUT", "/ratings/movie-tmdb-100", lambda kw: sent.append(kw["json"]) or ResponseStub(204, None))
    server.on("PUT", "/ratings/series-tvdb-300", lambda kw: sent.append(kw["json"]) or ResponseStub(204, None))
    stored = [{"item_id": "movie-tmdb-100", "rating": 4, "rated_at": "2026-10-27T18:00:00.000Z"}, {"item_id": "series-tvdb-300", "rating": 2, "rated_at": "2026-10-27T18:00:00.000Z"}, {"item_id": "episode-tvdb-300-1-1", "rating": 5, "rated_at": "2026-10-27T18:00:00.000Z"}]
    server.on("GET", "/ratings", lambda kw: _page(stored))

    res = adapter.add("ratings", [{"type": "movie", "ids": {"tmdb": "100"}, "rating": 7}, {"type": "show", "ids": {"tvdb": "300"}, "rating": 10}, {"type": "episode", "show_ids": {"tvdb": "300"}, "season": 1, "episode": 1, "rating": 8}])
    idx = _ratings.build_index(adapter)

    assert sent == [{"rating": 4}, {"rating": 5}]
    assert res["unresolved"][0]["status"] == "unsupported_type"
    assert idx["tmdb:100"]["rating"] == 7
    assert idx["tmdb:100"]["rated_at"] == "2026-10-27T18:00:00Z"
    assert idx["tvdb:300"]["rating"] == 4
    assert len(idx) == 2
    assert [_ratings.stars_for(v) for v in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)] == [1, 1, 2, 2, 3, 3, 4, 4, 5, 5]


def test_history_index_uses_completed_progress_and_card_coordinates(silo: Any) -> None:
    from providers.sync.silo import _history

    adapter, server = silo
    rows = [
        {"media_item_id": "movie-tmdb-100", "completed": True, "updated_at": "2024-06-01T20:00:00.000Z", "position_seconds": 0, "duration_seconds": 6000},
        {"media_item_id": "149382054478429162", "completed": True, "updated_at": "2024-06-02T20:00:00.000Z", "position_seconds": 0, "duration_seconds": 2640},
        {"media_item_id": "local-abc", "completed": True, "updated_at": "2024-06-03T20:00:00.000Z", "position_seconds": 0, "duration_seconds": 1},
    ]
    server.on("GET", "/progress", lambda kw: _page(rows if kw["params"]["status"] == "completed" else []))

    idx = _history.build_index(adapter)

    assert idx["tmdb:100"]["watched_at"] == "2024-06-01T20:00:00Z"
    ep = idx["tvdb:300#s01e02"]
    assert ep["show_ids"] == {"tvdb": "300"} and ep["season"] == 1 and ep["episode"] == 2 and ep["series_title"] == "Gamma"
    assert len(idx) == 2


def test_history_add_dated_writes_and_reports_server_ignored(silo: Any) -> None:
    adapter, server = silo
    after: list[dict[str, Any]] = []
    sent: list[Any] = []

    def write(kw: dict[str, Any]) -> ResponseStub:
        sent.append(kw["json"])
        for entry in kw["json"]["items"]:
            if entry["media_item_id"] != "episode-tvdb-300-1-1":
                after.append({"media_item_id": entry["media_item_id"], "completed": True, "updated_at": entry["updated_at"]})
        return ResponseStub(200, {"items": [{"index": i, "media_item_id": e["media_item_id"], "status": "success"} for i, e in enumerate(kw["json"]["items"])], "summary": {}})

    server.on("POST", "/sync/progress", write)
    server.on("GET", "/progress", lambda kw: _page(after))
    server.on("POST", "/watched/movie-imdb-tt0000200", lambda kw: after.append({"media_item_id": "movie-imdb-tt0000200", "completed": True, "updated_at": "2026-10-27T00:00:00Z"}) or ResponseStub(204, None))

    res = adapter.add("history", [
        {"type": "movie", "ids": {"tmdb": "100"}, "watched_at": "2024-06-01T20:00:00Z"},
        {"type": "episode", "show_ids": {"tvdb": "300"}, "season": 1, "episode": 1, "watched_at": "2024-06-02T20:00:00Z"},
        {"type": "movie", "ids": {"imdb": "tt0000200"}, "_cw_watched_state": True, "watched": True},
        {"type": "show", "ids": {"tvdb": "300"}, "watched_at": "2024-06-02T20:00:00Z"},
    ])

    assert sent[0]["items"][0] == {"client_ref": "tmdb:100", "media_item_id": "movie-tmdb-100", "position_ms": 6_000_000, "duration_ms": 6_000_000, "updated_at": "2024-06-01T20:00:00Z"}
    assert sent[0]["items"][1]["duration_ms"] == 2_700_000
    assert res["confirmed_keys"] == ["tmdb:100", "imdb:tt0000200"]
    assert {u["key"]: u["status"] for u in res["unresolved"]} == {"tvdb:300": "unsupported_type", "tvdb:300#s01e01": "ignored_by_server"}


def test_history_add_without_duration_is_unresolved_and_remove_unmarks(silo: Any) -> None:
    from providers.sync.silo import _common

    adapter, server = silo
    _common.library(adapter).movies["movie-tmdb-100"]["runtime"] = None
    server.on("GET", "/progress", lambda kw: _page([]))
    res = adapter.add("history", [{"type": "movie", "ids": {"tmdb": "100"}, "watched_at": "2024-06-01T20:00:00Z"}])
    assert res["unresolved"][0]["status"] == "missing_duration"
    assert server.count("POST", "/sync/progress") == 0

    server.on("DELETE", "/watched/movie-tmdb-100", lambda kw: ResponseStub(204, None))
    server.on("POST", "/history/remove", lambda kw: ResponseStub(204, None))
    res = adapter.remove("history", [{"type": "movie", "ids": {"tmdb": "100"}}])
    assert res["confirmed_keys"] == ["tmdb:100"]
    assert server.calls[-1]["json"] == {"targets": [{"content_id": "movie-tmdb-100", "scope": "item"}]}


def test_progress_index_and_write_verification(silo: Any) -> None:
    from providers.sync.silo import _progress

    adapter, server = silo
    rows = [{"media_item_id": "movie-tmdb-100", "completed": False, "position_seconds": 600.4, "duration_seconds": 6000, "updated_at": "2026-10-27T18:00:00.000Z"}]
    server.on("GET", "/progress", lambda kw: _page(rows))
    server.on("POST", "/sync/progress", lambda kw: ResponseStub(200, {"items": [{"index": 0, "media_item_id": e["media_item_id"], "status": "success"} for e in kw["json"]["items"]], "summary": {}}))

    idx = _progress.build_index(adapter)
    assert idx["tmdb:100"]["progress_ms"] == 600400
    assert idx["tmdb:100"]["progress_percent"] == pytest.approx(10.007, abs=0.01)

    res = adapter.add("progress", [
        {"type": "movie", "ids": {"tmdb": "100"}, "progress_ms": 601000, "duration_ms": 6_000_000},
        {"type": "episode", "show_ids": {"tvdb": "300"}, "season": 1, "episode": 1, "progress_ms": 60000, "progress_at": "2020-01-01T00:00:00Z"},
    ])

    assert res["confirmed_keys"] == ["tmdb:100"]
    assert res["skipped_keys"] == ["tvdb:300#s01e01"]
    sent = [c for c in server.calls if c["path"] == "/sync/progress"][0]["json"]["items"]
    assert sent[1] == {"client_ref": "tvdb:300#s01e01", "media_item_id": "episode-tvdb-300-1-1", "position_ms": 60000, "duration_ms": 2_700_000, "updated_at": "2020-01-01T00:00:00Z"}


def test_show_with_only_tmdb_resolves_through_cached_series_details(silo: Any, tmp_path: Any) -> None:
    from providers.sync.silo import _common

    adapter, server = silo
    server.on("GET", "/catalog/items/series-tvdb-300", lambda kw: ResponseStub(200, {"content_id": "series-tvdb-300", "tmdb_id": "77", "imdb_id": "tt77", "tvdb_id": "300"}))
    server.on("GET", "/catalog/items/movie-imdb-tt0000200", lambda kw: ResponseStub(200, {"content_id": "movie-imdb-tt0000200", "tmdb_id": "200", "imdb_id": "tt0000200"}))
    server.on("PUT", "/ratings/series-tvdb-300", lambda kw: ResponseStub(204, None))
    server.on("PUT", "/ratings/movie-imdb-tt0000200", lambda kw: ResponseStub(204, None))

    res = adapter.add("ratings", [{"type": "show", "ids": {"tmdb": "77"}, "rating": 6}, {"type": "show", "ids": {"tmdb": "78"}, "rating": 6}, {"type": "movie", "ids": {"tmdb": "200"}, "rating": 6}])

    assert res["confirmed_keys"] == ["tmdb:77#show", "tmdb:200"]
    assert server.count("GET", "/catalog/items/series-tvdb-300") == 1
    _common._LIBRARIES.clear()
    lib = _common.library(adapter)
    assert lib.series["series-tvdb-300"]["ids"] == {"tvdb": "300", "tmdb": "77", "imdb": "tt77"}
    assert server.count("GET", "/catalog/items/series-tvdb-300") == 1


def test_episode_minimal_carries_enriched_show_ids(silo: Any) -> None:
    from providers.sync.silo import _common

    adapter, _server = silo
    lib = _common.library(adapter)
    lib.merge_series_ids("series-tvdb-300", {"tmdb": "77"})

    item = _common.minimal_for(lib, "episode-tvdb-300-1-1")

    assert item == {"type": "episode", "show_ids": {"tvdb": "300", "tmdb": "77"}, "ids": {}, "season": 1, "episode": 1, "series_title": "Gamma", "title": "Pilot"}
    assert _common.key_of(item) == "tmdb:77#s01e01"
    assert _common.minimal_for(lib, "episode-tvdb-999-2-3") == {"type": "episode", "show_ids": {"tvdb": "999"}, "ids": {}, "season": 2, "episode": 3}
    assert _common.minimal_for(lib, "local-abc") is None


def test_dry_run_and_unsupported_feature(silo: Any) -> None:
    adapter, server = silo
    assert adapter.add("history", [{"type": "movie", "ids": {"tmdb": "1"}}], dry_run=True)["count"] == 1
    assert adapter.add("playlists", [])["unsupported"] is True
    assert server.calls == []


def test_interactive_sync_search_uses_silo_catalog(silo: Any) -> None:
    from services.interactive_sync_catalogs import destination_rows, search_catalogs

    adapter, server = silo
    server.on("GET", "/catalog/items/series-tvdb-300", lambda kw: ResponseStub(200, {"tmdb_id": "77", "imdb_id": "tt77", "tvdb_id": "300"}))
    seen: list[Any] = []

    def search(kw: dict[str, Any]) -> ResponseStub:
        if "q" not in kw["params"]:
            return _page({"movie": MOVIES, "series": SERIES, "episode": EPISODES}[kw["params"]["type"]])
        seen.append(kw["params"])
        return _page([{"content_id": "series-tvdb-300", "title": "Gamma", "year": 2020}, {"content_id": "local-x", "title": "Gamma home"}])

    server.on("GET", "/catalog", search)
    cfg = {"silo": {"instances": {"P1": dict(adapter.config["silo"])}}}
    row = {"provider": "SILO", "instance": "P1", "item": {"type": "show"}}

    assert {"id": "destination", "label": "SILO library"} in search_catalogs(cfg, row)["catalogs"]
    rows = destination_rows(None, cfg, row, cfg["silo"]["instances"]["P1"], "Gam", "show")

    assert seen[0] == {"q": "Gam", "type": "series", "limit": 20, "skip_total": "true"}
    assert rows == [{"title": "Gamma", "year": 2020, "ids": {"tvdb": "300", "tmdb": "77", "imdb": "tt77"}}]


def test_disabled_titles_keep_last_known_titles_instead_of_dropping_them(silo: Any) -> None:
    from providers.sync.silo import _watchlist

    adapter, server = silo
    server.on("GET", "/watchlist", lambda kw: _page([]))
    server.on("GET", "/watchlist/titles", lambda kw: _page([{"tmdb_id": 555, "media_type": "movie", "title": "Remote"}]))
    assert sorted(_watchlist.build_index(adapter)) == ["tmdb:555"]

    server.on("GET", "/watchlist/titles", lambda kw: ResponseStub(409, {"type": PROBLEM + "capability_disabled"}))
    assert sorted(_watchlist.build_index(adapter)) == ["tmdb:555"]

    server.on("GET", "/watchlist/titles", lambda kw: _page([]))
    assert _watchlist.build_index(adapter) == {}


def test_unexpected_extra_pages_raise_for_unpaged_lists(silo: Any) -> None:
    from providers.sync.silo import _common, _playlists

    adapter, server = silo
    server.on("GET", "/collections", lambda kw: ResponseStub(200, {"items": [], "page": {"has_more": True, "next_cursor": "x"}}))
    with pytest.raises(_common.SiloSyncError, match="unexpected_pages"):
        _playlists.list_resources(adapter)
    server.on("GET", "/user/libraries", lambda kw: ResponseStub(200, {"items": [], "page": {"has_more": True, "next_cursor": "x"}}))
    with pytest.raises(_common.SiloSyncError, match="unexpected_pages"):
        _common.list_libraries(adapter)


def test_library_presence_matches_the_write_resolver(silo: Any) -> None:
    from cw_platform import library_presence as lp
    from providers.sync._mod_SILO import OPS, get_manifest

    adapter, server = silo
    assert get_manifest()["capabilities"]["library_presence"] == {"features": ["history", "ratings", "progress"]}
    assert lp.supported(OPS, "history") and lp.supported(OPS, "ratings") and not lp.supported(OPS, "watchlist")
    items = [
        {"type": "movie", "ids": {"tmdb": "100"}},
        {"type": "movie", "ids": {"tmdb": "999"}},
        {"type": "movie", "ids": {"simkl": "1"}},
        {"type": "episode", "show_ids": {"tvdb": "300"}, "season": 1, "episode": 1},
        {"type": "episode", "show_ids": {"tvdb": "300"}, "season": 9, "episode": 9},
        {"type": "show", "ids": {"tvdb": "300"}},
    ]
    res = OPS.library_presence(adapter.config, items, feature="history")
    assert [r["status"] for r in res] == ["present", "absent", "absent", "present", "absent", "unknown"]
    assert res[0]["item_id"] == "movie-tmdb-100" and res[3]["item_id"] == "episode-tvdb-300-1-1"
    rated = OPS.library_presence(adapter.config, [items[5], items[3]], feature="ratings")
    assert [r["status"] for r in rated] == ["present", "unknown"]


def test_library_presence_fails_open_when_the_library_cannot_be_read(silo: Any) -> None:
    from providers.sync._mod_SILO import OPS
    from providers.sync.silo import _common

    adapter, server = silo
    _common._LIBRARIES.clear()
    server.on("GET", "/catalog", lambda kw: ResponseStub(502, {}))
    res = OPS.library_presence(adapter.config, [{"type": "movie", "ids": {"tmdb": "999"}}], feature="history")
    assert res == [{"status": "unknown", "reason": "index_unavailable"}]


def test_strict_id_matching_is_on_by_default_and_title_matching_needs_it_off(silo: Any) -> None:
    from providers.sync._mod_SILO import SILOModule
    from providers.sync.silo import _common

    adapter, server = silo
    lib = _common.library(adapter)
    movie = {"type": "movie", "ids": {"simkl": "1"}, "title": "alpha", "year": 2025}
    assert _common.strict_ids(adapter) is True
    assert _common.resolve_item(adapter, lib, movie, feature="history") == ("", "movie")

    loose = SILOModule({**adapter.config, "silo": {**adapter.config["silo"], "strict_id_matching": False}})
    assert _common.strict_ids(loose) is False
    assert _common.resolve_item(loose, lib, movie, feature="history") == ("movie-tmdb-100", "movie")
    assert _common.resolve_item(loose, lib, {**movie, "year": 2027}, feature="history")[0] == ""
    assert _common.resolve_item(loose, lib, {**movie, "ids": {"tmdb": "999"}}, feature="history")[0] == ""
    assert _common.resolve_item(loose, lib, {"type": "show", "ids": {}, "title": "Gamma"}, feature="watchlist") == ("series-tvdb-300", "show")
    episode = {"type": "episode", "show_ids": {"simkl": "5"}, "series_title": "GAMMA", "season": 1, "episode": 1}
    assert _common.resolve_item(loose, lib, episode, feature="history") == ("episode-tvdb-300-1-1", "episode")
    assert _common.resolve_item(adapter, lib, episode, feature="history") == ("", "episode")


def test_title_matching_skips_ambiguous_titles(silo: Any) -> None:
    from providers.sync._mod_SILO import SILOModule
    from providers.sync.silo import _common

    adapter, server = silo
    MOVIES.append({"content_id": "movie-tmdb-101", "type": "movie", "title": "Alpha", "year": 2024, "runtime": 100})
    try:
        _common._LIBRARIES.clear()
        loose = SILOModule({**adapter.config, "silo": {**adapter.config["silo"], "strict_id_matching": False}})
        lib = _common.library(loose)
        assert _common.resolve_item(loose, lib, {"type": "movie", "ids": {}, "title": "Alpha", "year": 2024}, feature="history")[0] == ""
    finally:
        MOVIES.pop()


def test_pair_strict_override_reaches_silo_and_presence_follows_it(silo: Any) -> None:
    from providers.sync._mod_SILO import OPS

    adapter, server = silo
    item = {"type": "movie", "ids": {"simkl": "1"}, "title": "Alpha", "year": 2024}
    strict = OPS.library_presence(adapter.config, [item], feature="history")
    loose = OPS.library_presence({**adapter.config, "silo": {**adapter.config["silo"], "strict_id_matching": False}}, [item], feature="history")
    assert strict[0]["status"] == "absent"
    assert loose[0] == {"status": "present", "item_id": "movie-tmdb-100"}
