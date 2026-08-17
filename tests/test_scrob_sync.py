# CrossWatch test scripts
from __future__ import annotations

import json
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Any

import pytest

from providers.sync import _mod_SCROB as mod
from providers.sync.scrob import _history, _progress, _ratings, _watchlist


@dataclass
class ResponseStub:
    status_code: int = 200
    payload: Any = None
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def text(self) -> str:
        return json.dumps(self.payload) if self.payload is not None else ""

    def json(self) -> Any:
        if self.payload is None:
            raise ValueError("no json")
        return self.payload


class FakeAdapter:
    def __init__(self, routes: dict[tuple[str, str], Any] | None = None) -> None:
        self.config = {
            "scrob": {
                "server_url": "http://host:7330",
                "api_key": "KEY",
                "username": "u",
                "password": "p",
                "api_prefix": "/api/proxy",
                "access_token": "JWT",
                "expires_at": 4102444800,
                "watchlist_name": "Watchlist",
            }
        }
        self.raw_cfg = self.config
        self.instance_id = "default"
        self.session = object()
        self.routes = routes or {}
        self.calls: list[dict[str, Any]] = []

    def respond(self, method: str, path: str, **kwargs: Any) -> Any:
        self.calls.append({"method": method, "path": path, **kwargs})
        handler = self.routes.get((method, path))
        if handler is None:
            return ResponseStub(200, {})
        return handler(kwargs) if callable(handler) else handler


@pytest.fixture(autouse=True)
def _patch_transport(monkeypatch: pytest.MonkeyPatch):
    from providers.sync.scrob import _common

    def fake_request(adapter: Any, method: str, path: str, **kwargs: Any) -> ResponseStub:
        return adapter.respond(method, path, **kwargs)

    def fake_webhook(adapter: Any, path: str, payload: Any) -> ResponseStub:
        return adapter.respond("POST", path, json=payload)

    for module in (_common, _history, _ratings, _progress, _watchlist):
        if hasattr(module, "scrob_request"):
            monkeypatch.setattr(module, "scrob_request", fake_request)
        if hasattr(module, "webhook_post"):
            monkeypatch.setattr(module, "webhook_post", fake_webhook)
    yield


def history_row(**over: Any) -> dict[str, Any]:
    row = {
        "id": 11,
        "media": {"id": 5, "tmdb_id": 550, "type": "movie", "title": "Fight Club", "release_date": "1999-10-15"},
        "watched_at": "2026-08-01T20:00:00",
        "progress_seconds": 8340,
        "progress_percent": 1.0,
        "completed": True,
        "play_count": 1,
    }
    row.update(over)
    return row


def episode_row(**over: Any) -> dict[str, Any]:
    row = {
        "id": 21,
        "media": {
            "id": 9,
            "tmdb_id": 63056,
            "type": "episode",
            "title": "Winter Is Coming",
            "season_number": 1,
            "episode_number": 1,
            "show_title": "Game of Thrones",
            "show_tmdb_id": 1399,
            "show_tvdb_id": 121361,
        },
        "watched_at": "2026-08-02T21:30:00",
        "completed": True,
        "play_count": 1,
    }
    row.update(over)
    return row


def test_manifest_declares_supported_features_only():
    manifest = mod.get_manifest()
    assert manifest["name"] == "SCROB"
    assert manifest["version"] == "0.1"
    assert manifest["bidirectional"] is True
    assert manifest["features"] == {"watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": False}


def test_capabilities_declare_event_history_and_rewatches():
    caps = mod.OPS.capabilities()
    assert caps["history"]["event_history"] is True
    assert caps["history"]["rewatches"] == {"read": True, "write": True, "account_gate": False}
    assert caps["progress"]["write_window_percent"] == {"min": 5, "max": 90}


def test_registry_discovers_scrob():
    from cw_platform.modules_registry import load_sync_ops, sync_provider_names, sync_provider_supports_feature

    assert "SCROB" in sync_provider_names()
    assert load_sync_ops("SCROB") is mod.OPS
    assert sync_provider_supports_feature("scrob", "history") is True
    assert sync_provider_supports_feature("scrob", "playlists") is False


def test_is_configured_checks_instances():
    assert mod.OPS.is_configured({}) is False
    assert mod.OPS.is_configured({"scrob": {"server_url": "http://s", "api_key": "k", "username": "u", "password": "p"}}) is True
    nested = {"scrob": {"instances": {"second": {"server_url": "http://s", "api_key": "k", "username": "u", "password": "p"}}}}
    assert mod.OPS.is_configured(nested) is True


def test_history_index_keys_are_event_scoped():
    adapter = FakeAdapter({("GET", "history"): ResponseStub(200, {"results": [history_row()], "total_pages": 1})})
    index = _history.build_index(adapter)
    (key, item), = index.items()
    expected = int(datetime(2026, 8, 1, 20, 0, tzinfo=timezone.utc).timestamp())
    assert key.endswith(f"@{expected}")
    assert item["ids"]["tmdb"] == "550"
    assert item["watched_at"] == "2026-08-01T20:00:00.000Z"
    assert item["_scrob_history_id"] == "11"
    assert item["_scrob_media_id"] == "5"

    from cw_platform.history_events import is_history_event_key

    assert is_history_event_key(key)


def test_history_index_keeps_multiple_plays_of_the_same_item():
    rows = [
        history_row(id=11, watched_at="2026-08-01T20:00:00"),
        history_row(id=12, watched_at="2026-07-01T20:00:00"),
        history_row(id=13, watched_at="2025-01-05T09:00:00"),
    ]
    adapter = FakeAdapter({("GET", "history"): ResponseStub(200, {"results": rows, "total_pages": 1})})
    index = _history.build_index(adapter)
    assert len(index) == 3
    assert {item["_scrob_history_id"] for item in index.values()} == {"11", "12", "13"}
    assert len({item["watched_at"] for item in index.values()}) == 3


def test_history_index_disambiguates_identical_timestamps():
    rows = [history_row(id=11), history_row(id=12)]
    adapter = FakeAdapter({("GET", "history"): ResponseStub(200, {"results": rows, "total_pages": 1})})
    index = _history.build_index(adapter)
    assert len(index) == 2
    assert any("~h12" in key for key in index)


def test_history_index_maps_episodes_to_show_scoped_keys():
    adapter = FakeAdapter({("GET", "history"): ResponseStub(200, {"results": [episode_row()], "total_pages": 1})})
    index = _history.build_index(adapter)
    (key, item), = index.items()
    assert "s01e01" in key
    assert item["type"] == "episode"
    assert item["show_ids"] == {"tmdb": "1399", "tvdb": "121361"}
    assert item["season"] == 1 and item["episode"] == 1
    assert item["series_title"] == "Game of Thrones"


def test_history_index_skips_incomplete_rows():
    rows = [history_row(completed=False), history_row(id=14, media={"id": 1, "tmdb_id": None, "type": "movie", "title": "x"})]
    adapter = FakeAdapter({("GET", "history"): ResponseStub(200, {"results": rows, "total_pages": 1})})
    assert _history.build_index(adapter) == {}


def test_history_index_follows_pagination():
    pages = {
        1: {"results": [history_row(id=1, watched_at="2026-08-01T20:00:00")], "total_pages": 2},
        2: {"results": [history_row(id=2, watched_at="2026-08-02T20:00:00")], "total_pages": 2},
    }
    adapter = FakeAdapter({("GET", "history"): lambda kw: ResponseStub(200, pages[kw["params"]["page"]])})
    assert len(_history.build_index(adapter)) == 2


def test_history_add_sends_watched_at_for_movies():
    adapter = FakeAdapter({("POST", "history"): ResponseStub(200, {"status": "ok"})})
    item = {"type": "movie", "ids": {"tmdb": "550"}, "watched_at": "2026-08-01T20:00:00.000Z", "_cw_event_key": "tmdb:550@1754078400"}
    result = _history.add(adapter, [item])
    assert result["ok"] and result["confirmed_keys"] == ["tmdb:550@1754078400"]
    body = adapter.calls[0]["json"]
    assert body == {"completed": True, "watched_at": "2026-08-01T20:00:00.000Z", "media_type": "movie", "tmdb_id": 550}


def test_history_add_event_keys_are_accounted_by_applier(monkeypatch: pytest.MonkeyPatch):
    from cw_platform.orchestrator import _applier

    unresolved_writes: list[Any] = []
    monkeypatch.setattr(_applier, "record_unresolved", lambda *a, **k: unresolved_writes.append((a, k)) or {"ok": True})

    adapter = FakeAdapter({("POST", "history"): ResponseStub(200, {"status": "ok"})})
    item = {"type": "movie", "ids": {"tmdb": "550"}, "watched_at": "2026-08-01T20:00:00.000Z", "_cw_event_key": "tmdb:550@1754078400"}
    raw = _history.add(adapter, [item])

    normalized = _applier._normalize(raw, [item], "apply:add", dst="SCROB", feature="history", emit=lambda *a, **k: None)

    assert normalized["confirmed"] == 1
    assert normalized["unresolved"] == 0
    assert normalized["unresolved_keys"] == []
    assert unresolved_writes == []


def test_history_add_sends_full_episode_context():
    adapter = FakeAdapter({("POST", "history"): ResponseStub(200, {"status": "ok"})})
    item = {
        "type": "episode",
        "ids": {"tmdb": "63056"},
        "show_ids": {"tmdb": "1399", "tvdb": "121361"},
        "season": 1,
        "episode": 1,
        "watched_at": "2026-08-02T21:30:00.000Z",
    }
    result = _history.add(adapter, [item])
    assert result["ok"]
    body = adapter.calls[0]["json"]
    assert body["media_type"] == "episode"
    assert body["series_tmdb_id"] == 1399
    assert body["series_tvdb_id"] == 121361
    assert body["season_number"] == 1 and body["episode_number"] == 1
    assert body["tmdb_id"] == 63056


def test_history_add_marks_items_without_ids_unresolved():
    adapter = FakeAdapter()
    result = _history.add(adapter, [{"type": "movie", "ids": {"imdb": "tt0137523"}, "title": "Fight Club", "year": 1999}])
    assert result["confirmed_keys"] == []
    assert result["unresolved"][0]["status"] == "missing_supported_id"
    assert not adapter.calls


def test_history_add_reports_http_failures():
    adapter = FakeAdapter({("POST", "history"): ResponseStub(404, {"detail": "TMDB Media not found"})})
    result = _history.add(adapter, [{"type": "movie", "ids": {"tmdb": "550"}, "watched_at": "2026-08-01T20:00:00Z"}])
    assert result["ok"] is False
    assert result["unresolved"][0]["status"] == "http:404"
    assert "TMDB Media not found" in result["unresolved"][0]["error"]


def test_history_remove_deletes_a_single_play_by_event_id():
    adapter = FakeAdapter({("DELETE", "history/event/11"): ResponseStub(200, {"status": "ok"})})
    item = {"type": "movie", "ids": {"tmdb": "550"}, "watched_at": "2026-08-01T20:00:00Z", "_scrob_history_id": "11"}
    result = _history.remove(adapter, [item])
    assert result["ok"] and len(result["confirmed_keys"]) == 1
    assert adapter.calls[0]["path"] == "history/event/11"


def test_history_remove_falls_back_to_item_delete():
    adapter = FakeAdapter({("DELETE", "history/item"): ResponseStub(200, {"status": "ok"})})
    item = {"type": "movie", "ids": {"tmdb": "550"}, "_scrob_media_id": "5"}
    result = _history.remove(adapter, [item])
    assert result["ok"]
    assert adapter.calls[0]["params"] == {"media_type": "movie", "id": 5}


def test_history_remove_treats_404_as_done():
    adapter = FakeAdapter({("DELETE", "history/event/11"): ResponseStub(404, {"detail": "gone"})})
    result = _history.remove(adapter, [{"type": "movie", "ids": {"tmdb": "550"}, "_scrob_history_id": "11"}])
    assert result["ok"] and len(result["confirmed_keys"]) == 1


def test_history_dry_run_writes_nothing():
    adapter = FakeAdapter()
    result = _history.add(adapter, [{"type": "movie", "ids": {"tmdb": "550"}, "watched_at": "2026-08-01T20:00:00Z"}], dry_run=True)
    assert result["confirmed_keys"]
    assert not adapter.calls


def test_ratings_index_maps_movies_shows_and_seasons():
    rows = [
        {"id": 1, "media": {"id": 5, "tmdb_id": 550, "type": "movie", "title": "Fight Club", "release_date": "1999-10-15"}, "season_number": None, "rating": 8.5, "rated_at": "2026-08-01T10:00:00"},
        {"id": 2, "media": {"id": 6, "tmdb_id": 1399, "type": "series", "title": "Game of Thrones"}, "season_number": None, "rating": 9.0, "rated_at": "2026-08-01T10:00:00"},
        {"id": 3, "media": {"id": 6, "tmdb_id": 1399, "type": "series", "title": "Game of Thrones"}, "season_number": 2, "rating": 7.0, "rated_at": "2026-08-01T10:00:00"},
    ]
    adapter = FakeAdapter({("GET", "ratings"): ResponseStub(200, {"results": rows})})
    index = _ratings.build_index(adapter)
    types = sorted(item["type"] for item in index.values())
    assert types == ["movie", "season", "show"]
    ratings = sorted(item["rating"] for item in index.values())
    assert ratings == [7, 9, 9]


def test_ratings_index_resolves_episode_context():
    rows = [{"id": 4, "media": {"id": 9, "tmdb_id": 63056, "type": "episode", "title": "Winter Is Coming"}, "season_number": None, "rating": 10, "rated_at": "2026-08-01T10:00:00"}]
    detail = {"tmdb_id": 63056, "type": "episode", "season_number": 1, "episode_number": 1, "show_tmdb_id": 1399, "show_title": "Game of Thrones"}
    adapter = FakeAdapter({
        ("GET", "ratings"): ResponseStub(200, {"results": rows}),
        ("GET", "media/episode/63056"): ResponseStub(200, detail),
    })
    index = _ratings.build_index(adapter)
    (key, item), = index.items()
    assert "s01e01" in key
    assert item["show_ids"] == {"tmdb": "1399"}
    assert item["rating"] == 10


def test_ratings_index_skips_unresolvable_episodes():
    rows = [{"id": 4, "media": {"id": 9, "tmdb_id": 63056, "type": "episode", "title": "x"}, "rating": 10, "rated_at": "2026-08-01T10:00:00"}]
    adapter = FakeAdapter({
        ("GET", "ratings"): ResponseStub(200, {"results": rows}),
        ("GET", "media/episode/63056"): ResponseStub(404, {"detail": "Episode not found"}),
    })
    assert _ratings.build_index(adapter) == {}


def test_ratings_add_maps_each_scope():
    adapter = FakeAdapter({("POST", "ratings"): ResponseStub(200, {"status": "ok"})})
    items = [
        {"type": "movie", "ids": {"tmdb": "550"}, "rating": 8},
        {"type": "show", "ids": {"tmdb": "1399"}, "rating": 9},
        {"type": "season", "show_ids": {"tmdb": "1399"}, "season": 2, "rating": 7},
        {"type": "episode", "ids": {"tmdb": "63056"}, "show_ids": {"tmdb": "1399"}, "season": 1, "episode": 1, "rating": 10},
    ]
    result = _ratings.add(adapter, items)
    assert len(result["confirmed_keys"]) == 4
    bodies = [call["json"] for call in adapter.calls]
    assert bodies[0] == {"tmdb_id": 550, "media_type": "movie", "rating": 8.0}
    assert bodies[1] == {"tmdb_id": 1399, "media_type": "series", "rating": 9.0}
    assert bodies[2] == {"tmdb_id": 1399, "media_type": "series", "season_number": 2, "rating": 7.0}
    assert bodies[3] == {"tmdb_id": 63056, "media_type": "episode", "rating": 10.0}


def test_ratings_rounds_to_the_shared_ten_point_scale():
    adapter = FakeAdapter({("POST", "ratings"): ResponseStub(200, {})})
    _ratings.add(adapter, [{"type": "movie", "ids": {"tmdb": "550"}, "rating": 8.5}])
    assert adapter.calls[0]["json"]["rating"] == 9.0


def test_ratings_remove_sends_scope_params():
    adapter = FakeAdapter({("DELETE", "ratings"): ResponseStub(200, {"status": "deleted"})})
    result = _ratings.remove(adapter, [{"type": "season", "show_ids": {"tmdb": "1399"}, "season": 2, "rating": 7}])
    assert result["ok"]
    assert adapter.calls[0]["params"] == {"tmdb_id": 1399, "media_type": "series", "season_number": 2}


def test_progress_index_converts_fraction_to_percent():
    rows = [{
        "id": 1,
        "media": {"id": 5, "tmdb_id": 550, "type": "movie", "title": "Fight Club", "runtime": 139},
        "watched_at": "2026-08-03T18:00:00",
        "progress_seconds": 3336,
        "progress_percent": 0.4,
        "completed": False,
    }]
    adapter = FakeAdapter({("GET", "history/continue-watching"): ResponseStub(200, {"continue_watching": rows})})
    index = _progress.build_index(adapter)
    (_, item), = index.items()
    assert item["progress_percent"] == 40.0
    assert item["progress_ms"] == 3336000
    assert item["duration_ms"] == 8340000
    assert item["_scrob_media_id"] == "5"
    assert item["progress_at_source"] == "scrob"


def test_progress_add_uses_a_stop_below_the_watched_threshold():
    adapter = FakeAdapter({("POST", "webhooks/kodi"): ResponseStub(200, {"status": "ok"})})
    item = {"type": "movie", "ids": {"tmdb": "550"}, "title": "Fight Club", "progress_percent": 40.0, "duration_ms": 8340000}
    result = _progress.add(adapter, [item])
    assert result["ok"] and result["confirmed_keys"]
    body = adapter.calls[0]["json"]
    assert body["event"] == "playback_stopped"
    assert body["total_seconds"] == 8340
    assert body["position_seconds"] == 3336
    assert body["session_id"].startswith("crosswatch-progress-")


def test_progress_add_refuses_values_scrob_will_not_keep():
    adapter = FakeAdapter()
    items = [
        {"type": "movie", "ids": {"tmdb": "1"}, "progress_percent": 2.0, "duration_ms": 1000},
        {"type": "movie", "ids": {"tmdb": "2"}, "progress_percent": 95.0, "duration_ms": 1000},
    ]
    result = _progress.add(adapter, items)
    assert result["confirmed_keys"] == []
    assert {u["status"] for u in result["unresolved"]} == {"outside_supported_progress_window"}
    assert not adapter.calls


def test_progress_add_requires_duration():
    adapter = FakeAdapter()
    result = _progress.add(adapter, [{"type": "movie", "ids": {"tmdb": "550"}, "progress_percent": 40.0}])
    assert result["unresolved"][0]["status"] == "missing_duration_or_percent"


def test_progress_remove_uses_scrob_media_id():
    adapter = FakeAdapter({("DELETE", "history/continue-watching"): ResponseStub(200, {"status": "ok"})})
    result = _progress.remove(adapter, [{"type": "movie", "ids": {"tmdb": "550"}, "_scrob_media_id": "5"}])
    assert result["ok"]
    assert adapter.calls[0]["params"] == {"media_id": 5}


def test_progress_remove_without_media_id_is_unresolved():
    adapter = FakeAdapter()
    result = _progress.remove(adapter, [{"type": "movie", "ids": {"tmdb": "550"}}])
    assert result["unresolved"][0]["status"] == "missing_scrob_media_id"
    assert not adapter.calls


def test_watchlist_index_reads_the_named_list():
    lists = {"lists": [{"id": 3, "name": "Watchlist"}, {"id": 4, "name": "Favourites"}]}
    items = {
        "id": 3,
        "name": "Watchlist",
        "items": [
            {"id": 71, "added_at": "2026-08-01T10:00:00", "media": {"id": 5, "tmdb_id": 550, "type": "movie", "title": "Fight Club", "release_date": "1999-10-15"}},
            {"id": 72, "added_at": "2026-08-02T10:00:00", "media": {"id": 6, "tmdb_id": 1399, "type": "series", "title": "Game of Thrones"}},
        ],
    }
    adapter = FakeAdapter({("GET", "lists"): ResponseStub(200, lists), ("GET", "lists/3"): ResponseStub(200, items)})
    index = _watchlist.build_index(adapter)
    assert sorted(item["type"] for item in index.values()) == ["movie", "show"]
    movie = next(i for i in index.values() if i["type"] == "movie")
    assert movie["_scrob_list_item_id"] == "71"
    assert movie["_scrob_list_id"] == "3"
    assert movie["year"] == 1999


def test_watchlist_index_is_empty_when_the_list_is_missing():
    adapter = FakeAdapter({("GET", "lists"): ResponseStub(200, {"lists": []})})
    assert _watchlist.build_index(adapter) == {}


def test_watchlist_add_creates_the_list_once():
    created: dict[str, Any] = {}

    def create(kw: Any) -> ResponseStub:
        created.update(kw["json"])
        return ResponseStub(201, {"id": 9, "name": "Watchlist"})

    adapter = FakeAdapter({
        ("GET", "lists"): ResponseStub(200, {"lists": []}),
        ("POST", "lists"): create,
        ("POST", "lists/9/items"): ResponseStub(201, {"id": 1}),
    })
    result = _watchlist.add(adapter, [{"type": "movie", "ids": {"tmdb": "550"}}, {"type": "show", "ids": {"tmdb": "1399"}}])
    assert len(result["confirmed_keys"]) == 2
    assert created == {"name": "Watchlist", "privacy_level": "private"}
    bodies = [c["json"] for c in adapter.calls if c["path"] == "lists/9/items"]
    assert bodies == [{"tmdb_id": 550, "media_type": "movie"}, {"tmdb_id": 1399, "media_type": "series"}]


def test_watchlist_add_treats_conflict_as_present():
    adapter = FakeAdapter({
        ("GET", "lists"): ResponseStub(200, {"lists": [{"id": 3, "name": "Watchlist"}]}),
        ("POST", "lists/3/items"): ResponseStub(409, {"detail": "Item already in list"}),
    })
    result = _watchlist.add(adapter, [{"type": "movie", "ids": {"tmdb": "550"}}])
    assert result["ok"] and result["confirmed_keys"]


def test_watchlist_remove_uses_list_item_id():
    adapter = FakeAdapter({("DELETE", "lists/3/items/71"): ResponseStub(200, {"status": "ok"})})
    item = {"type": "movie", "ids": {"tmdb": "550"}, "_scrob_list_id": "3", "_scrob_list_item_id": "71"}
    result = _watchlist.remove(adapter, [item])
    assert result["ok"]
    assert adapter.calls[0]["path"] == "lists/3/items/71"


def test_watchlist_remove_looks_up_missing_item_ids():
    lists = {"lists": [{"id": 3, "name": "Watchlist"}]}
    items = {"id": 3, "items": [{"id": 71, "media": {"id": 5, "tmdb_id": 550, "type": "movie", "title": "Fight Club"}}]}
    adapter = FakeAdapter({
        ("GET", "lists"): ResponseStub(200, lists),
        ("GET", "lists/3"): ResponseStub(200, items),
        ("DELETE", "lists/3/items/71"): ResponseStub(200, {"status": "ok"}),
    })
    result = _watchlist.remove(adapter, [{"type": "movie", "ids": {"tmdb": "550"}}])
    assert result["ok"] and result["confirmed_keys"]


def test_watchlist_remove_reports_unknown_items():
    adapter = FakeAdapter({("GET", "lists"): ResponseStub(200, {"lists": []})})
    result = _watchlist.remove(adapter, [{"type": "movie", "ids": {"tmdb": "550"}}])
    assert result["unresolved"][0]["status"] == "missing_list_item_id"


def test_module_requires_a_connected_config():
    with pytest.raises(RuntimeError):
        mod.SCROBModule({"scrob": {"server_url": "http://s"}})


def test_module_ignores_unsupported_features():
    adapter = mod.SCROBModule({"scrob": {"server_url": "http://s", "api_key": "k", "username": "u", "password": "p"}})
    assert adapter.build_index("playlists") == {}
    assert adapter.add("playlists", [])["reason"] == "disabled_or_missing"
    assert adapter.remove("playlists", [])["reason"] == "disabled_or_missing"


def test_pair_override_renames_the_watchlist_list():
    from cw_platform.orchestrator._pairs import _deep_merge_provider_overrides

    cfg = {"scrob": {"server_url": "http://s", "api_key": "k", "username": "u", "password": "p", "watchlist_name": "Watchlist"}}
    _deep_merge_provider_overrides(cfg["scrob"], {"watchlist_name": "Plan to watch"})

    adapter = mod.SCROBModule(cfg)
    assert _watchlist.watchlist_name(adapter) == "Plan to watch"


def test_watchlist_name_falls_back_to_the_default():
    adapter = FakeAdapter()
    adapter.config["scrob"].pop("watchlist_name")
    assert _watchlist.watchlist_name(adapter) == "Watchlist"
    adapter.config["scrob"]["watchlist_name"] = "   "
    assert _watchlist.watchlist_name(adapter) == "Watchlist"


def test_renamed_list_is_the_one_read_and_created():
    adapter = FakeAdapter({
        ("GET", "lists"): ResponseStub(200, {"lists": [{"id": 3, "name": "Watchlist"}, {"id": 8, "name": "Plan to watch"}]}),
        ("GET", "lists/8"): ResponseStub(200, {"id": 8, "items": [
            {"id": 90, "media": {"id": 5, "tmdb_id": 550, "type": "movie", "title": "Fight Club"}},
        ]}),
    })
    adapter.config["scrob"]["watchlist_name"] = "Plan to watch"
    index = _watchlist.build_index(adapter)
    assert len(index) == 1
    assert next(iter(index.values()))["_scrob_list_id"] == "8"


def test_missing_renamed_list_is_created_with_that_name():
    created: dict[str, Any] = {}

    def create(kw: Any) -> ResponseStub:
        created.update(kw["json"])
        return ResponseStub(201, {"id": 12, "name": "Plan to watch"})

    adapter = FakeAdapter({
        ("GET", "lists"): ResponseStub(200, {"lists": [{"id": 3, "name": "Watchlist"}]}),
        ("POST", "lists"): create,
        ("POST", "lists/12/items"): ResponseStub(201, {"id": 1}),
    })
    adapter.config["scrob"]["watchlist_name"] = "Plan to watch"
    result = _watchlist.add(adapter, [{"type": "movie", "ids": {"tmdb": "550"}}])
    assert result["ok"]
    assert created["name"] == "Plan to watch"
