# CrossWatch test scripts
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

SERVER = "http://silo.local:8090"
CREDS = {"server_url": SERVER, "access_token": "a", "refresh_token": "r", "profile_id": "p-1", "user_id": "1"}
MOVIE_PATH = "/mnt/media/movies/Alpha (2024)/Alpha (2024) [1080p].mkv"
EPISODE_PATH = "/mnt/media/tv/Gamma (2020)/Season 1/Gamma S01E01.mkv"


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


def _page(items: list[dict[str, Any]]) -> ResponseStub:
    return ResponseStub(200, {"items": items, "page": {"has_more": False}})


class FakeServer:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.sent: list[Any] = []
        self.completed: list[str] = ["movie-tmdb-100"]
        self.catalog = {
            "movie": [{"content_id": "movie-tmdb-100", "title": "Alpha", "year": 2024, "runtime": 100, "added_at": "2026-10-27T12:00:00Z"},
                      {"content_id": "movie-tmdb-101", "title": "Alpha copy", "year": 2024, "runtime": 100, "added_at": "2026-10-27T12:00:00Z"}],
            "series": [{"content_id": "series-tvdb-300", "title": "Gamma", "added_at": "2026-10-27T12:00:00Z"}],
            "episode": [{"content_id": "episode-tvdb-300-1-1", "series_id": "series-tvdb-300", "season_number": 1, "episode_number": 1,
                         "runtime": 45, "added_at": "2026-10-27T12:00:00Z"}],
        }
        self.paths = {"movie-tmdb-100": [MOVIE_PATH], "movie-tmdb-101": ["/other/place/Copy/Copy.mkv"], "episode-tvdb-300-1-1": [EPISODE_PATH]}
        self.detail_status = 200

    def __call__(self, session: Any, method: str, url: str, **kwargs: Any) -> ResponseStub:
        path = url.split("/api/v2", 1)[1]
        params = kwargs.get("params") or {}
        self.calls.append(f"{method} {path}")
        if path == "/catalog":
            return _page(self.catalog[params["type"]])
        if path.startswith("/catalog/items/"):
            cid = path.rsplit("/", 1)[1]
            if self.detail_status != 200:
                return ResponseStub(self.detail_status, {})
            return ResponseStub(200, {"content_id": cid, "versions": [{"file_path": p} for p in self.paths.get(cid, [])]})
        if path == "/progress":
            if params.get("status") == "completed":
                return _page([{"media_item_id": cid, "completed": True, "updated_at": "2026-10-27T18:00:00Z"} for cid in self.completed])
            return _page([])
        if method == "POST" and path == "/sync/progress":
            self.sent.append(kwargs["json"])
            for entry in kwargs["json"]["items"]:
                self.completed.append(entry["media_item_id"])
            return ResponseStub(200, {"items": [{"index": i, "media_item_id": e["media_item_id"], "status": "success"} for i, e in enumerate(kwargs["json"]["items"])]})
        raise AssertionError(f"unexpected {method} {path}")


@pytest.fixture()
def server(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> FakeServer:
    from providers.sync.silo import _common

    fake = FakeServer()
    monkeypatch.setattr(_common, "request_with_auth", fake)
    monkeypatch.setattr(_common, "STATE_DIR", tmp_path)
    _common._LIBRARIES.clear()
    yield fake
    _common._LIBRARIES.clear()


def _adapter(path_matching: bool = True) -> Any:
    from providers.sync._mod_SILO import SILOModule

    return SILOModule({"silo": dict(CREDS), "_cw_path_matching": path_matching})


def test_pair_toggle_allows_silo_with_other_media_servers() -> None:
    from cw_platform.path_matching import pair_enabled

    assert pair_enabled({"source": "SILO", "target": "PLEX", "path_matching": True})
    assert pair_enabled({"source": "JELLYFIN", "target": "SILO", "path_matching": True})
    assert not pair_enabled({"source": "SILO", "target": "TRAKT", "path_matching": True})


def test_history_index_carries_file_paths_only_when_enabled(server: FakeServer) -> None:
    assert "media_paths" not in _adapter(False).build_index("history")["tmdb:100"]
    assert not any(c.startswith("GET /catalog/items/") for c in server.calls)

    idx = _adapter(True).build_index("history")
    assert idx["tmdb:100"]["media_paths"] == [MOVIE_PATH]
    assert server.calls.count("GET /catalog/items/movie-tmdb-100") == 1
    _adapter(True).build_index("history")
    assert server.calls.count("GET /catalog/items/movie-tmdb-100") == 1


def test_failed_path_lookup_without_cache_fails_the_read(server: FakeServer) -> None:
    from providers.sync.silo import _common

    server.detail_status = 502
    with pytest.raises(_common.SiloSyncError, match="file_details_failed"):
        _adapter(True).build_index("history")


def test_write_resolves_by_path_when_ids_do_not_match(server: FakeServer) -> None:
    ad = _adapter(True)
    movie = {"type": "movie", "ids": {"imdb": "tt0999"}, "title": "Alpha", "media_paths": ["/data/films/Alpha (2024)/Alpha (2024) [1080p].mkv"],
             "watched_at": "2026-10-27T19:00:00Z"}
    episode = {"type": "episode", "show_ids": {"tmdb": "777"}, "season": 1, "episode": 1, "watched_at": "2026-10-27T19:00:00Z",
               "media_paths": ["/volume1/tv/Gamma (2020)/Season 1/Gamma S01E01.mkv"]}
    res = ad.add("history", [movie, episode])
    assert sorted(res["confirmed_keys"]) == ["imdb:tt0999", "tmdb:777#s01e01"]
    assert sorted(e["media_item_id"] for e in server.sent[0]["items"]) == ["episode-tvdb-300-1-1", "movie-tmdb-100"]

    plain = _adapter(False).add("history", [dict(movie, watched_at="2026-10-27T19:00:00Z")])
    assert plain["count"] == 0 and plain["unresolved_keys"] == ["imdb:tt0999"]


def test_conflicting_ids_and_ambiguous_paths_are_not_matched(server: FakeServer) -> None:
    from providers.sync.silo import _common

    ad = _adapter(True)
    lib = _common.library(ad, feature="history")
    assert _common.resolve_item(ad, lib, {"type": "movie", "ids": {"tmdb": "555"}, "media_paths": [MOVIE_PATH]}, feature="history")[0] == ""
    assert _common.resolve_item(ad, lib, {"type": "movie", "ids": {"imdb": "tt5"}, "media_paths": [MOVIE_PATH]}, feature="history")[0] == "movie-tmdb-100"
    server.paths["movie-tmdb-101"] = [MOVIE_PATH]
    _common._LIBRARIES.clear()
    _common.write_state(_common.FILES_STATE, _common.server_scope(ad), {})
    lib = _common.library(ad, feature="history")
    assert _common.resolve_item(ad, lib, {"type": "movie", "ids": {"imdb": "tt5"}, "media_paths": [MOVIE_PATH]}, feature="history")[0] == ""


def test_presence_uses_the_path_fallback(server: FakeServer) -> None:
    from providers.sync._mod_SILO import OPS

    item = {"type": "movie", "ids": {"imdb": "tt0999"}, "media_paths": ["/x/Alpha (2024)/Alpha (2024) [1080p].mkv"]}
    on = OPS.library_presence({"silo": dict(CREDS), "_cw_path_matching": True}, [item], feature="history")
    off = OPS.library_presence({"silo": dict(CREDS)}, [item], feature="history")
    assert on[0]["status"] == "present" and on[0]["item_id"] == "movie-tmdb-100"
    assert off[0]["status"] == "absent"


def test_progress_and_ratings_indexes_carry_file_paths(server: FakeServer, monkeypatch: pytest.MonkeyPatch) -> None:
    real = server.__call__

    def extra(session: Any, method: str, url: str, **kwargs: Any) -> ResponseStub:
        path = url.split("/api/v2", 1)[1]
        params = kwargs.get("params") or {}
        if path == "/progress" and params.get("status") == "in_progress":
            return _page([{"media_item_id": "episode-tvdb-300-1-1", "completed": False, "position_seconds": 60.0, "duration_seconds": 2700}])
        if path == "/ratings":
            return _page([{"item_id": "movie-tmdb-100", "rating": 4}])
        return real(session, method, url, **kwargs)

    from providers.sync.silo import _common

    monkeypatch.setattr(_common, "request_with_auth", extra)
    ad = _adapter(True)
    assert ad.build_index("progress")["tvdb:300#s01e01"]["media_paths"] == [EPISODE_PATH]
    assert ad.build_index("ratings")["tmdb:100"]["media_paths"] == [MOVIE_PATH]
