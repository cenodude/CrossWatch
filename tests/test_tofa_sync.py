# CrossWatch test scripts
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

import pytest

SERVER = "http://tofa.local:33333"
MOVIES_LIB = "lib-movies"
TV_LIB = "lib-tv"


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


def _movie(fid: str, mid: str, tmdb: int, title: str, *, edition: str | None = None, imdb: str | None = None) -> dict[str, Any]:
    return {"media_file_id": fid, "media_id": mid, "media_type": "movie", "title": title, "edition": edition, "available": True,
            "tmdb_id": tmdb, "imdb_id": imdb, "tvdb_id": None, "episode_id": None, "season_number": None, "episode_number": None}


def _episode(fid: str, s: int, e: int) -> dict[str, Any]:
    return {"media_file_id": fid, "media_id": "show-1", "media_type": "tv", "title": "Gamma", "edition": None, "available": True,
            "tmdb_id": 300, "imdb_id": "tt0300", "tvdb_id": "3000", "episode_id": f"ep-{s}-{e}", "season_number": s, "episode_number": e,
            "tmdb_episode_id": 9000 + e}


FILES = {
    MOVIES_LIB: [
        _movie("f-alpha", "m-alpha", 100, "Alpha", imdb="tt0100"),
        _movie("f-beta", "m-beta", 200, "Beta"),
        _movie("f-beta-dc", "m-beta", 200, "Beta", edition="Director's Cut"),
    ],
    TV_LIB: [_episode("f-e1", 1, 1), _episode("f-e1-4k", 1, 1), _episode("f-e2", 1, 2)],
}


class FakeServer:
    def __init__(self) -> None:
        self.routes: dict[tuple[str, str], Callable[[dict[str, Any]], ResponseStub]] = {}
        self.calls: list[dict[str, Any]] = []
        self.state: dict[str, dict[str, Any]] = {}
        self.watchlist: list[dict[str, Any]] = []
        self.feed: list[dict[str, Any]] = []
        self.details: dict[str, dict[str, Any]] = {}
        self.on("GET", "/libraries", lambda kw: ResponseStub(200, [{"id": MOVIES_LIB, "name": "Movies", "media_type": "movie"},
                                                                   {"id": TV_LIB, "name": "TV Shows", "media_type": "tv"}]))
        self.on("GET", "/media/files", self._files)
        self.on("GET", "/users/me/watch-state", self._watch_state)
        self.on("POST", "/media/watched/batch", self._watched_batch)
        self.on("GET", "/users/me/watchlist", lambda kw: ResponseStub(200, {"items": list(self.watchlist), "page": 1, "per_page": 200,
                                                                            "total": len(self.watchlist), "total_pages": 1}))
        self.on("POST", "/users/me/watchlist/batch", lambda kw: ResponseStub(204, None))
        self.on("GET", "/system/info", lambda kw: ResponseStub(200, {"version": "0.13.1", "capabilities": ["sync.user_state", "watch.bulk_status"]}))
        self.on("GET", "/users/me/state-changes", self._changes)
        for mid in {r["media_id"] for rows in FILES.values() for r in rows}:
            self.on("GET", f"/media/{mid}", lambda kw, mid=mid: ResponseStub(200, self.details.get(mid) or self._detail(mid)))

    def _detail(self, mid: str) -> dict[str, Any]:
        rows = [r for rows in FILES.values() for r in rows if r["media_id"] == mid]
        files = [{"id": r["media_file_id"], "duration_ms": 6_000_000, "created_at": f"2025-01-0{i + 1}T00:00:00Z", "resolution": "1920x1080",
                  "video_range": "SDR", "audio_tracks": [{"codec": "ac3", "channels": 6, "channel_layout": "5.1"}]} for i, r in enumerate(rows)]
        return {"id": mid, "added_at": "2024-12-24T12:00:00Z", "files": files}

    def mark(self, fid: str, completed: bool, completed_at: str | None = None) -> None:
        if completed:
            self.state[fid] = {"completed": True, "completed_at": completed_at, "position_ms": 0}
        else:
            self.state.pop(fid, None)
        self.feed.append({"revision": len(self.feed) + 1, "kind": "watched", "media_file_id": fid, "completed": completed, "completed_at": completed_at})

    def _changes(self, kw: dict[str, Any]) -> ResponseStub:
        start = int(str(kw["params"]["cursor"]).split("-")[0])
        return ResponseStub(200, {"changes": self.feed[start:], "next_cursor": f"{len(self.feed)}-0", "has_more": False, "requires_full_sync": False})

    def _files(self, kw: dict[str, Any]) -> ResponseStub:
        lib = kw["params"]["library_id"]
        if lib not in FILES:
            return ResponseStub(404, {"error": "not_found", "message": "Library not found"})
        rows = FILES[lib]
        if kw["params"].get("after"):
            return ResponseStub(200, {"items": rows[1:], "next_after": None})
        return ResponseStub(200, {"items": rows[:1], "next_after": rows[0]["media_file_id"]})

    def _watch_state(self, kw: dict[str, Any]) -> ResponseStub:
        only = kw["params"].get("completed_only") == "true"
        rows = [{"media_file_id": fid, **row} for fid, row in self.state.items() if row.get("completed") or not only]
        return ResponseStub(200, {"items": rows, "next_after": None, "changes_cursor": f"{len(self.feed)}-0"})

    def _watched_batch(self, kw: dict[str, Any]) -> ResponseStub:
        items = kw["json"]["items"]
        known = {r["media_file_id"] for rows in FILES.values() for r in rows}
        missing = [i["media_file_id"] for i in items if i["media_file_id"] not in known]
        if missing:
            return ResponseStub(404, {"error": "not_found", "message": "Not found: One or more media files not found", "failed_ids": missing})
        for item in items:
            self.mark(item["media_file_id"], bool(item["watched"]), item.get("watched_at") or "2026-11-04T10:00:00Z" if item["watched"] else None)
        return ResponseStub(200, {"updated": len(items)})

    def on(self, method: str, path: str, handler: Callable[[dict[str, Any]], ResponseStub]) -> None:
        self.routes[(method, path)] = handler

    def __call__(self, session: Any, method: str, url: str, **kwargs: Any) -> ResponseStub:
        path = url.split("/api/v1", 1)[1]
        kwargs.setdefault("params", {})
        self.calls.append({"method": method, "path": path, **kwargs})
        handler = self.routes.get((method, path))
        if handler is None:
            raise AssertionError(f"unexpected {method} {path}")
        return handler(kwargs)

    def count(self, method: str, path: str) -> int:
        return sum(1 for c in self.calls if c["method"] == method and c["path"] == path)


CREDS = {"server_url": SERVER, "server_id": "srv-1", "access_token": "a", "refresh_token": "r", "profile_id": "p-1", "user_id": "u-1"}


@pytest.fixture()
def server(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> FakeServer:
    from providers.sync.tofa import _common

    fake = FakeServer()
    monkeypatch.setattr(_common, "request_with_auth", fake)
    monkeypatch.setattr(_common, "STATE_DIR", tmp_path)
    monkeypatch.setattr("cw_platform.played_writes.note_write", lambda *a, **k: None)
    _common._LIBRARIES.clear()
    return fake


def _adapter(**features: list[str]) -> Any:
    from providers.sync._mod_TOFA import TOFAModule

    block: dict[str, Any] = dict(CREDS)
    for feature, libs in features.items():
        block[feature] = {"libraries": libs}
    return TOFAModule({"tofa": block, "_cw_provider_instance": "default"})


def test_manifest_registration_and_ops(server: FakeServer) -> None:
    from cw_platform.modules_registry import MODULES
    from providers.sync import _mod_TOFA

    assert MODULES["SYNC"]["_mod_TOFA"] == "providers.sync._mod_TOFA"
    assert _mod_TOFA.OPS.name() == "TOFA" and _mod_TOFA.OPS.label() == "Tofa"
    feats = _mod_TOFA.OPS.features()
    assert feats["watchlist"] and feats["history"] and feats["progress"] and feats["collection"]
    assert not feats["ratings"] and not feats["playlists"]
    assert _mod_TOFA.OPS.is_configured({"tofa": dict(CREDS)}) is True
    assert _mod_TOFA.OPS.is_configured({"tofa": {**CREDS, "server_id": ""}}) is False
    assert _adapter().health()["ok"] is True


def test_health_reports_missing_capabilities(server: FakeServer) -> None:
    server.on("GET", "/system/info", lambda kw: ResponseStub(200, {"capabilities": ["sync.user_state"]}))
    health = _adapter().health()
    assert health["ok"] is False and health["status"] == "unsupported_server"
    assert "watch.bulk_status" in health["details"]["reason"]


def test_library_index_follows_cursor_pages(server: FakeServer) -> None:
    index = _adapter().build_index("collection")
    assert sorted(index) == ["tmdb:100", "tmdb:200", "tmdb:300#s01e01", "tmdb:300#s01e02", "tmdb:300#show"]
    assert server.count("GET", "/media/files") == 4


def test_history_index_needs_every_file_of_an_edition(server: FakeServer) -> None:
    server.state = {
        "f-alpha": {"completed": True, "completed_at": "2024-05-01T20:00:00Z"},
        "f-beta-dc": {"completed": True, "completed_at": None},
        "f-e1": {"completed": True, "completed_at": "2023-01-01T10:00:00Z"},
    }
    index = _adapter().build_index("history")
    assert index["tmdb:100"]["watched_at"] == "2024-05-01T20:00:00Z"
    assert index["tmdb:200"]["_cw_watched_state"] is True and "watched_at" not in index["tmdb:200"]
    assert "tmdb:300#s01e01" not in index

    server.mark("f-e1-4k", True, "2023-01-01T10:00:00Z")
    index = _adapter().build_index("history")
    episode = index["tmdb:300#s01e01"]
    assert episode["watched_at"] == "2023-01-01T10:00:00Z"
    assert episode["show_ids"] == {"tmdb": "300", "imdb": "tt0300", "tvdb": "3000"} and episode["ids"] == {}


def test_history_add_writes_dated_batch_for_default_edition(server: FakeServer) -> None:
    items = [
        {"type": "movie", "ids": {"tmdb": "200"}, "watched_at": "2022-06-15T22:30:00+02:00"},
        {"type": "episode", "show_ids": {"tvdb": "3000"}, "season": 1, "episode": 1, "watched_at": "2021-12-26T20:00:00Z"},
        {"type": "movie", "ids": {"tmdb": "999"}, "watched_at": "2021-01-01T00:00:00Z"},
        {"type": "movie", "ids": {"tmdb": "100"}},
    ]
    res = _adapter().add("history", items)

    batch = next(c for c in server.calls if c["path"] == "/media/watched/batch")["json"]["items"]
    assert {e["media_file_id"] for e in batch} == {"f-beta", "f-e1", "f-e1-4k"}
    assert all(e["watched"] is True for e in batch)
    assert next(e for e in batch if e["media_file_id"] == "f-beta")["watched_at"] == "2022-06-15T20:30:00Z"
    assert sorted(res["confirmed_keys"]) == ["tmdb:200", "tvdb:3000#s01e01"]
    statuses = {u["key"]: u["status"] for u in res["unresolved"]}
    assert statuses == {"tmdb:999": "not_in_library", "tmdb:100": "missing_watched_at"}


def test_history_add_undated_watch_omits_watched_at(server: FakeServer) -> None:
    res = _adapter().add("history", [{"type": "movie", "ids": {"tmdb": "100"}, "_cw_watched_state": True, "watched": True}])
    batch = next(c for c in server.calls if c["path"] == "/media/watched/batch")["json"]["items"]
    assert batch == [{"media_file_id": "f-alpha", "watched": True}]
    assert res["confirmed_keys"] == ["tmdb:100"]


def test_history_batch_drops_failed_ids_and_retries_once(server: FakeServer) -> None:
    from providers.sync.tofa import _common

    adapter = _adapter()
    lib = _common.library(adapter)
    lib.add_row(_movie("f-stale", "m-stale", 777, "Stale"))
    res = adapter.add("history", [
        {"type": "movie", "ids": {"tmdb": "777"}, "watched_at": "2020-01-01T00:00:00Z"},
        {"type": "movie", "ids": {"tmdb": "100"}, "watched_at": "2020-01-01T00:00:00Z"},
    ])
    assert server.count("POST", "/media/watched/batch") == 2
    assert res["confirmed_keys"] == ["tmdb:100"]
    assert {u["key"]: u["status"] for u in res["unresolved"]} == {"tmdb:777": "file_not_found"}


def test_history_add_reports_unconfirmed_writes(server: FakeServer) -> None:
    server.on("POST", "/media/watched/batch", lambda kw: ResponseStub(200, {"updated": 0}))
    res = _adapter().add("history", [{"type": "movie", "ids": {"tmdb": "100"}, "watched_at": "2020-01-01T00:00:00Z"}])
    assert res["confirmed_keys"] == [] and res["unresolved"][0]["status"] == "not_confirmed"


def test_history_remove_unmarks_every_edition(server: FakeServer) -> None:
    server.state = {"f-beta": {"completed": True}, "f-beta-dc": {"completed": True}}
    res = _adapter().remove("history", [{"type": "movie", "ids": {"tmdb": "200"}}])
    batch = next(c for c in server.calls if c["path"] == "/media/watched/batch")["json"]["items"]
    assert sorted(e["media_file_id"] for e in batch) == ["f-beta", "f-beta-dc"] and all(e["watched"] is False for e in batch)
    assert res["confirmed_keys"] == ["tmdb:200"] and server.state == {}


def test_failed_read_raises_instead_of_empty_index(server: FakeServer) -> None:
    server.on("GET", "/users/me/watch-state", lambda kw: ResponseStub(500, {"error": "internal"}))
    with pytest.raises(Exception, match="tofa_history_http_500"):
        _adapter().build_index("history")


def test_history_whitelist_limits_index_and_missing_library_fails(server: FakeServer) -> None:
    server.state = {"f-alpha": {"completed": True, "completed_at": "2024-05-01T20:00:00Z"},
                    "f-e2": {"completed": True, "completed_at": "2024-05-02T20:00:00Z"}}
    assert sorted(_adapter(history=[TV_LIB]).build_index("history")) == ["tmdb:300#s01e02"]
    res = _adapter(history=[TV_LIB]).add("history", [{"type": "movie", "ids": {"tmdb": "200"}, "watched_at": "2020-01-01T00:00:00Z"}])
    assert res["unresolved"][0]["status"] == "not_in_library"
    with pytest.raises(Exception, match="tofa_whitelisted_library_missing"):
        _adapter(history=[TV_LIB, "lib-gone"]).build_index("history")


def test_pair_libraries_override_reaches_named_instance(server: FakeServer) -> None:
    from cw_platform.orchestrator._pairs_utils import config_with_pair_libraries
    from cw_platform.provider_instances import build_provider_config_view
    from providers.sync._mod_TOFA import TOFAModule

    server.state = {"f-alpha": {"completed": True, "completed_at": "2024-05-01T20:00:00Z"},
                    "f-e2": {"completed": True, "completed_at": "2024-05-02T20:00:00Z"}}
    cfg = {"tofa": {"instances": {"TOFA-P01": dict(CREDS)}}}
    view = {**build_provider_config_view(cfg, "tofa", "TOFA-P01"), "_cw_provider_instance": "TOFA-P01"}
    scoped = config_with_pair_libraries(view, {"libraries": {"TOFA#TOFA-P01": [MOVIES_LIB]}}, "history", ("TOFA", "TRAKT"))

    assert sorted(TOFAModule(view).build_index("history")) == ["tmdb:100", "tmdb:300#s01e02"]
    assert sorted(TOFAModule(scoped).build_index("history")) == ["tmdb:100"]


def test_watchlist_index_and_batch_writes(server: FakeServer) -> None:
    server.watchlist = [
        {"media_id": "m-alpha", "media_type": "movie", "tmdb_id": 100, "title": "Alpha", "year": 2024, "owned": True, "added_at": "2021-04-04T04:04:04.5Z"},
        {"media_id": None, "media_type": "tv", "tmdb_id": 95396, "title": "Severance", "year": 2022, "owned": False, "added_at": "2022-01-01T00:00:00Z"},
        {"media_id": None, "media_type": "movie", "tmdb_id": None, "title": "", "owned": False, "added_at": "2022-01-01T00:00:00Z"},
    ]
    index = _adapter().build_index("watchlist")
    assert index["tmdb:100"]["ids"] == {"tmdb": "100", "imdb": "tt0100"} and index["tmdb:100"]["added_at"] == "2021-04-04T04:04:04Z"
    assert index["tmdb:95396#show"]["type"] == "show" and len(index) == 2

    res = _adapter().add("watchlist", [
        {"type": "show", "ids": {"tmdb": "95396", "tvdb": "371980"}, "title": "Severance", "year": 2022, "listed_at": "2020-02-02T02:02:02Z"},
        {"type": "show", "ids": {"tvdb": "3000"}, "title": "Gamma"},
        {"type": "movie", "ids": {"imdb": "tt9999"}, "title": "Nowhere"},
        {"type": "episode", "show_ids": {"tmdb": "300"}, "season": 1, "episode": 1},
    ])
    body = next(c for c in server.calls if c["path"] == "/users/me/watchlist/batch")["json"]["items"]
    assert body[0] == {"on_watchlist": True, "media_type": "tv", "tmdb_id": 95396, "title": "Severance", "year": 2022, "tvdb_id": 371980,
                       "added_at": "2020-02-02T02:02:02Z"}
    assert body[1]["tmdb_id"] == 300 and body[1]["media_type"] == "tv"
    assert len(res["confirmed_keys"]) == 2
    assert {u["status"] for u in res["unresolved"]} == {"missing_tmdb_id", "unsupported_type"}

    server.calls.clear()
    _adapter().remove("watchlist", [{"type": "movie", "ids": {"tmdb": "100"}, "title": "Alpha"}])
    body = next(c for c in server.calls if c["path"] == "/users/me/watchlist/batch")["json"]["items"]
    assert body == [{"on_watchlist": False, "media_type": "movie", "tmdb_id": 100}]


def test_progress_index_reads_positions_and_durations(server: FakeServer) -> None:
    server.state = {"f-alpha": {"completed": False, "position_ms": 600000, "last_watched_at": "2026-11-01T21:00:00Z"},
                    "f-e2": {"completed": False, "position_ms": 0}}
    server.on("GET", "/media/m-alpha", lambda kw: ResponseStub(200, {"files": [{"id": "f-alpha", "duration_ms": 6000000}]}))
    index = _adapter().build_index("progress")
    item = index["tmdb:100"]
    assert item["progress_ms"] == 600000 and item["duration_ms"] == 6000000 and item["progress_percent"] == 10.0
    assert item["progress_at"] == "2026-11-01T21:00:00Z" and len(index) == 1
    _adapter().build_index("progress")
    assert server.count("GET", "/media/m-alpha") == 1


def test_progress_add_skips_completed_and_short_positions(server: FakeServer) -> None:
    server.state = {"f-beta": {"completed": True, "completed_at": "2024-01-01T00:00:00Z"}}
    server.on("PUT", "/media/f-alpha/progress", lambda kw: ResponseStub(200, {"media_file_id": "f-alpha", "position_ms": kw["json"]["position_ms"]}))
    res = _adapter().add("progress", [
        {"type": "movie", "ids": {"tmdb": "100"}, "progress_ms": 120000},
        {"type": "movie", "ids": {"tmdb": "200"}, "progress_ms": 120000},
        {"type": "episode", "show_ids": {"tmdb": "300"}, "season": 1, "episode": 2, "progress_ms": 30000},
    ])
    assert res["confirmed_keys"] == ["tmdb:100"]
    assert sorted(res["skipped_keys"]) == ["tmdb:200", "tmdb:300#s01e02"]
    assert server.count("PUT", "/media/f-alpha/progress") == 1


def test_collection_is_read_only(server: FakeServer) -> None:
    res = _adapter().add("collection", [{"type": "movie", "ids": {"tmdb": "100"}}])
    assert res["confirmed_keys"] == [] and res["unresolved"][0]["status"] == "tofa_collection_write_unsupported"
    assert not any(c["method"] != "GET" for c in server.calls)


def test_library_presence_matches_the_write_resolver(server: FakeServer) -> None:
    rows = _adapter().library_presence("history", [
        {"type": "movie", "ids": {"tmdb": "100"}},
        {"type": "episode", "show_ids": {"tvdb": "3000"}, "season": 1, "episode": 2},
        {"type": "movie", "ids": {"tmdb": "999"}},
        {"type": "show", "ids": {"tmdb": "300"}},
    ])
    assert [r["status"] for r in rows] == ["present", "present", "absent", "unknown"]


def test_dry_run_and_unsupported_feature(server: FakeServer) -> None:
    adapter = _adapter()
    assert adapter.add("history", [{"type": "movie", "ids": {"tmdb": "100"}}], dry_run=True)["dry_run"] is True
    assert adapter.add("ratings", [{"type": "movie", "ids": {"tmdb": "100"}}])["unsupported"] is True
    assert server.count("POST", "/media/watched/batch") == 0


def test_interactive_sync_search_uses_tofa_media_list(server: FakeServer) -> None:
    from services import interactive_sync_catalogs as cat

    server.on("GET", "/media", lambda kw: ResponseStub(200, {"items": [
        {"id": "m-alpha", "title": "Alpha", "release_date": "2024-02-03", "tmdb_id": 100},
        {"id": "m-local", "title": "Home video", "release_date": None, "tmdb_id": None},
    ]}))
    rows = cat.destination_rows(None, {}, {"provider": "TOFA", "instance": "default"}, dict(CREDS), "alp", "movie")
    assert rows == [{"title": "Alpha", "year": "2024", "ids": {"tmdb": 100}}]
    call = next(c for c in server.calls if c["path"] == "/media")
    assert call["params"] == {"q": "alp", "media_type": "movie", "per_page": 20}


def test_history_feed_applies_changes_without_full_reads(server: FakeServer) -> None:
    server.mark("f-alpha", True, "2024-05-01T20:00:00Z")
    adapter = _adapter()
    assert sorted(adapter.build_index("history")) == ["tmdb:100"]
    assert server.count("GET", "/users/me/watch-state") == 1

    server.mark("f-beta", True, "2022-02-02T02:02:02Z")
    server.mark("f-alpha", False)
    server.mark("f-alpha", False)
    index = _adapter().build_index("history")
    assert sorted(index) == ["tmdb:200"] and index["tmdb:200"]["watched_at"] == "2022-02-02T02:02:02Z"
    assert server.count("GET", "/users/me/watch-state") == 1
    assert server.count("GET", "/users/me/state-changes") == 1


def test_feed_reset_or_failure_falls_back_to_a_full_read(server: FakeServer) -> None:
    server.mark("f-alpha", True, "2024-05-01T20:00:00Z")
    _adapter().build_index("history")

    server.on("GET", "/users/me/state-changes", lambda kw: ResponseStub(200, {"changes": [], "next_cursor": "0-0", "has_more": False, "requires_full_sync": True}))
    assert sorted(_adapter().build_index("history")) == ["tmdb:100"]
    assert server.count("GET", "/users/me/watch-state") == 2

    server.on("GET", "/users/me/state-changes", lambda kw: ResponseStub(500, {"error": "internal"}))
    server.mark("f-beta", True, "2022-02-02T02:02:02Z")
    assert sorted(_adapter().build_index("history")) == ["tmdb:100", "tmdb:200"]
    assert server.count("GET", "/users/me/watch-state") == 3

    server.on("GET", "/users/me/watch-state", lambda kw: ResponseStub(500, {"error": "internal"}))
    with pytest.raises(Exception, match="tofa_history_http_500"):
        _adapter().build_index("history")


def test_feed_snapshot_is_fully_reread_once_a_day(server: FakeServer, tmp_path: Any) -> None:
    import json as _json

    from providers.sync.tofa import _common

    server.mark("f-alpha", True, "2024-05-01T20:00:00Z")
    adapter = _adapter()
    adapter.build_index("history")
    path = _common.state_path("feed_watched", _common.account_scope(adapter))
    state = _json.loads(path.read_text(encoding="utf-8"))
    state["full_at"] -= _common.FULL_RESYNC_SEC + 1
    path.write_text(_json.dumps(state), encoding="utf-8")
    _adapter().build_index("history")
    assert server.count("GET", "/users/me/watch-state") == 2 and server.count("GET", "/users/me/state-changes") == 0


def test_watchlist_feed_keeps_titles_and_dates(server: FakeServer) -> None:
    server.watchlist = [{"media_id": None, "media_type": "tv", "tmdb_id": 95396, "title": "Severance", "year": 2022, "owned": False,
                         "added_at": "2022-01-01T00:00:00Z"}]
    assert sorted(_adapter().build_index("watchlist")) == ["tmdb:95396#show"]

    server.feed += [
        {"revision": 90, "kind": "watchlist", "media_type": "movie", "tmdb_id": 100, "on_watchlist": True, "added_at": "2019-09-09T09:09:09Z"},
        {"revision": 91, "kind": "watchlist", "media_type": "tv", "tmdb_id": 95396, "on_watchlist": False},
        {"revision": 92, "kind": "watchlist", "media_type": "movie", "tmdb_id": 100, "on_watchlist": True, "added_at": "2019-09-09T09:09:09Z"},
    ]
    index = _adapter().build_index("watchlist")
    assert list(index) == ["tmdb:100"]
    assert index["tmdb:100"] == {"type": "movie", "ids": {"tmdb": "100", "imdb": "tt0100"}, "title": "Alpha", "added_at": "2019-09-09T09:09:09Z"}
    assert server.count("GET", "/users/me/watchlist") == 1


def test_collection_dates_and_opt_in_media_info(server: FakeServer) -> None:
    server.details["m-beta"] = {"id": "m-beta", "added_at": "2023-03-03T03:03:03Z", "files": [
        {"id": "f-beta", "resolution": "1920x1080", "bitrate": 8_000_000, "video_range": "SDR",
         "audio_tracks": [{"codec": "eac3", "profile": "Dolby Digital Plus + Dolby Atmos", "channels": 6, "channel_layout": "5.1(side)", "atmos": True}]},
        {"id": "f-beta-dc", "resolution": "3840x1606", "bitrate": 40_000_000, "video_range": "HDR10", "color_transfer": "smpte2084",
         "audio_tracks": [{"codec": "eac3", "channels": 6, "atmos": False, "title": "DTS-HD MA 7.1"},
                          {"codec": "dts", "profile": "DTS-HD MA + DTS:X", "channels": 8, "channel_layout": "7.1", "atmos": False}]},
    ]}
    plain = _adapter().build_index("collection")
    assert plain["tmdb:200"]["collected_at"] == "2023-03-03T03:03:03Z" and "media_info" not in plain["tmdb:200"]
    assert plain["tmdb:300#show"]["collected_at"] == "2024-12-24T12:00:00Z"
    assert plain["tmdb:300#s01e01"]["collected_at"] == "2025-01-01T00:00:00Z"
    assert plain["tmdb:300#s01e02"]["collected_at"] == "2025-01-03T00:00:00Z"
    assert "media_info" not in plain["tmdb:300#show"]
    detail_calls = sum(1 for c in server.calls if c["path"].startswith("/media/m-") or c["path"] == "/media/show-1")
    assert detail_calls == 3

    rich = _adapter(collection=[]).build_index("collection")
    assert "media_info" not in rich["tmdb:200"]
    from providers.sync._mod_TOFA import TOFAModule

    on = TOFAModule({"tofa": {**CREDS, "collection": {"media_info": True}}, "_cw_provider_instance": "default"}).build_index("collection")
    assert on["tmdb:200"]["format"] == "digital"
    assert on["tmdb:200"]["media_info"] == {"resolution": "uhd_4k", "hdr": "hdr10", "audio": "dts_x", "audio_channels": "7.1"}
    assert on["tmdb:300#s01e01"]["media_info"]["resolution"] == "hd_1080p"
    assert sum(1 for c in server.calls if c["path"].startswith("/media/m-") or c["path"] == "/media/show-1") == 3


def test_collection_details_refetch_only_changed_titles(server: FakeServer) -> None:
    from providers.sync.tofa import _common

    _adapter().build_index("collection")
    FILES[MOVIES_LIB].append(_movie("f-alpha-4k", "m-alpha", 100, "Alpha"))
    try:
        _common._LIBRARIES.clear()
        _adapter().build_index("collection")
    finally:
        FILES[MOVIES_LIB].pop()
    assert server.count("GET", "/media/m-alpha") == 2
    assert server.count("GET", "/media/m-beta") == 1 and server.count("GET", "/media/show-1") == 1


def test_collection_detail_failure_keeps_items_without_details(server: FakeServer) -> None:
    server.on("GET", "/media/m-beta", lambda kw: ResponseStub(500, {"error": "internal"}))
    index = _adapter().build_index("collection")
    assert "tmdb:200" in index and "collected_at" not in index["tmdb:200"]
    assert index["tmdb:100"]["collected_at"] == "2024-12-24T12:00:00Z"


def _prefs_server(server: FakeServer, playback: dict[str, Any]) -> list[dict[str, Any]]:
    prefs = {"accent_color": "#6b21a8", "playback": playback}
    puts: list[dict[str, Any]] = []
    server.on("GET", "/system/info", lambda kw: ResponseStub(200, {"capabilities": [], "watched_rule": {"credits": True, "percent": 90, "earlier_episodes": True}}))
    server.on("GET", "/users/me", lambda kw: ResponseStub(200, {"id": "u-1", "preferences": prefs}))

    def put(kw: dict[str, Any]) -> ResponseStub:
        puts.append(kw["json"])
        prefs["playback"] = {**prefs["playback"], **kw["json"]["preferences"]["playback"]}
        return ResponseStub(200, {"id": "u-1", "preferences": prefs})

    server.on("PUT", "/users/me/preferences", put)
    return puts


def test_watched_rule_reads_server_default_or_custom(server: FakeServer) -> None:
    from providers.sync.tofa._common import watched_rule

    _prefs_server(server, {"default_quality": "auto"})
    rule = watched_rule(_adapter())
    assert rule == {"percent": 90, "credits": True, "earlier_episodes": True, "custom": False, "default_percent": 90, "choices": [70, 75, 80, 85, 90, 95]}

    _prefs_server(server, {"watched_rule": {"custom": True, "percent": 80, "credits": False, "earlier_episodes": True}})
    rule = watched_rule(_adapter())
    assert rule["percent"] == 80 and rule["custom"] is True and rule["credits"] is False and rule["default_percent"] == 90

    _prefs_server(server, {"watched_rule": {"custom": False, "percent": 70, "credits": True, "earlier_episodes": True}})
    assert watched_rule(_adapter())["percent"] == 90


def test_set_watched_rule_keeps_other_playback_preferences(server: FakeServer) -> None:
    from providers.sync.tofa._common import set_watched_rule

    puts = _prefs_server(server, {"default_quality": "auto", "auto_play_next": "auto", "segment_actions": {"intro": "ask"}})
    rule = set_watched_rule(_adapter(), 85)
    assert rule["percent"] == 85 and rule["custom"] is True
    assert puts == [{"preferences": {"playback": {"default_quality": "auto", "auto_play_next": "auto", "segment_actions": {"intro": "ask"},
                                                  "watched_rule": {"custom": True, "percent": 85, "credits": True, "earlier_episodes": True}}}}]

    for bad in (82, 100, None, "x"):
        with pytest.raises(Exception, match="tofa_watched_rule_invalid_percent"):
            set_watched_rule(_adapter(), bad)
    assert len(puts) == 1

    server.on("PUT", "/users/me/preferences", lambda kw: ResponseStub(200, {}))
    with pytest.raises(Exception, match="tofa_watched_rule_not_saved"):
        set_watched_rule(_adapter(), 75)


def test_route_modal_offers_tofa_rule_alignment() -> None:
    from pathlib import Path

    js = (Path(__file__).resolve().parents[1] / "assets" / "js" / "modals" / "scrobbler-route" / "index.js").read_text(encoding="utf-8")
    assert 'r.provider === "tofa" ? `<div class="scrm-note" id="scr-tofa-rule">' in js
    assert "/api/tofa/watched-rule?instance=" in js and "data-tofa-apply" in js and 'method: "POST"' in js
    assert 'if (e.target.id === "scr-watched") paintTofaRule();' in js


def test_progress_never_unwatches_a_completed_part(server: FakeServer) -> None:
    from providers.sync.tofa import _common

    FILES[MOVIES_LIB].extend([_movie("f-p1", "m-parts", 555, "Parts"), _movie("f-p2", "m-parts", 555, "Parts")])
    try:
        _common._LIBRARIES.clear()
        server.mark("f-p1", True, "2024-01-01T00:00:00Z")
        puts: list[Any] = []
        server.on("PUT", "/media/f-p1/progress", lambda kw: puts.append(kw) or ResponseStub(200, {"position_ms": 120000}))
        res = _adapter().add("progress", [{"type": "movie", "ids": {"tmdb": "555"}, "progress_ms": 120000}])
        assert res["skipped_keys"] == ["tmdb:555"] and puts == []

        server.calls.clear()
        _adapter().remove("progress", [{"type": "movie", "ids": {"tmdb": "555"}}])
        batch = next(c for c in server.calls if c["path"] == "/media/watched/batch")["json"]["items"]
        assert batch == [{"media_file_id": "f-p2", "watched": False}]
        assert server.state["f-p1"]["completed"] is True
    finally:
        del FILES[MOVIES_LIB][-2:]


def test_malformed_reads_raise_instead_of_partial_results(server: FakeServer) -> None:
    server.on("GET", "/users/me/watchlist", lambda kw: ResponseStub(200, {"items": [{"tmdb_id": 1, "media_type": "movie"}], "page": 1}))
    with pytest.raises(Exception, match="tofa_watchlist_invalid_response"):
        _adapter().build_index("watchlist")

    server.on("GET", "/users/me/watchlist", lambda kw: ResponseStub(200, {"items": ["junk"], "page": 1, "total_pages": 1}))
    with pytest.raises(Exception, match="tofa_watchlist_invalid_response"):
        _adapter().build_index("watchlist")

    from providers.sync.tofa import _common

    _common._LIBRARIES.clear()
    server.on("GET", "/media/files", lambda kw: ResponseStub(200, {"items": [FILES[MOVIES_LIB][0], "junk"], "next_after": None}))
    with pytest.raises(Exception, match="tofa_library_invalid_response"):
        _adapter().build_index("collection")


def test_malformed_feed_entries_fall_back_to_a_full_read(server: FakeServer) -> None:
    server.mark("f-alpha", True, "2024-05-01T20:00:00Z")
    _adapter().build_index("history")
    server.on("GET", "/users/me/state-changes", lambda kw: ResponseStub(200, {"changes": ["junk"], "next_cursor": "9-0", "has_more": False, "requires_full_sync": False}))
    assert sorted(_adapter().build_index("history")) == ["tmdb:100"]
    assert server.count("GET", "/users/me/watch-state") == 2


def test_invalid_state_rows_raise_or_fall_back(server: FakeServer) -> None:
    server.on("GET", "/users/me/watch-state", lambda kw: ResponseStub(200, {"items": [{}], "next_after": None, "changes_cursor": "0-0"}))
    with pytest.raises(Exception, match="invalid_response"):
        _adapter().build_index("history")

    server.on("GET", "/users/me/watch-state", server._watch_state)
    server.mark("f-alpha", True, "2024-05-01T20:00:00Z")
    _adapter().build_index("history")
    server.on("GET", "/users/me/state-changes", lambda kw: ResponseStub(200, {"changes": [{"revision": 5, "kind": "watched", "media_file_id": "f-alpha"}],
                                                                            "next_cursor": "9-0", "has_more": False, "requires_full_sync": False}))
    before = server.count("GET", "/users/me/watch-state")
    assert sorted(_adapter().build_index("history")) == ["tmdb:100"]
    assert server.count("GET", "/users/me/watch-state") == before + 1

    from providers.sync.tofa import _common

    _common._LIBRARIES.clear()
    server.on("GET", "/media/files", lambda kw: ResponseStub(200, {"items": [{"media_file_id": "f-x", "media_type": "movie"}], "next_after": None}))
    with pytest.raises(Exception, match="tofa_library_invalid_response"):
        _adapter().build_index("collection")


def test_progress_remove_uses_full_batches(server: FakeServer) -> None:
    from providers.sync.tofa import _common

    rows = [_movie(f"f-m{i}-{j}", f"m-big-{i}", 1000 + i, f"Big {i}") for i in range(100) for j in range(6)]
    FILES[MOVIES_LIB].extend(rows)
    try:
        _common._LIBRARIES.clear()
        res = _adapter().remove("progress", [{"type": "movie", "ids": {"tmdb": str(1000 + i)}} for i in range(100)])
        sizes = [len(c["json"]["items"]) for c in server.calls if c["path"] == "/media/watched/batch"]
        assert sizes == [500, 100]
        assert len(res["confirmed_keys"]) == 100 and res["unresolved"] == []
    finally:
        del FILES[MOVIES_LIB][-len(rows):]


def test_feed_transport_failure_falls_back_to_a_full_read(server: FakeServer) -> None:
    import requests

    server.mark("f-alpha", True, "2024-05-01T20:00:00Z")
    _adapter().build_index("history")

    def timeout(kw: dict[str, Any]) -> ResponseStub:
        raise requests.Timeout("read timed out")

    server.on("GET", "/users/me/state-changes", timeout)
    assert sorted(_adapter().build_index("history")) == ["tmdb:100"]
    assert server.count("GET", "/users/me/watch-state") == 2
