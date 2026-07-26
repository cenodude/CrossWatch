from __future__ import annotations

import json
from typing import Any, Mapping


class Response:
    def __init__(self, status_code: int, payload: Any = None) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = "" if payload is None else json.dumps(payload)
        self.headers: dict[str, str] = {}

    def json(self) -> Any:
        return self._payload


class FakeClient:
    BASE = "https://api.mdblist.com"

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = list(rows or [])
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []
        self.next_id = 1000

    def get(self, url: str, **kwargs: Any) -> Response:
        self.calls.append(("GET", url, kwargs.get("params")))
        return Response(200, list(self.rows))

    def post(self, url: str, json: Mapping[str, Any], **_: Any) -> Response:
        body = dict(json)
        self.calls.append(("POST", url, body))
        if url.endswith("/scrobble/pause"):
            self.next_id += 1
            if "movie" in body:
                movie = {"title": "Fight Club", "year": 1999, "ids": {"imdbid": body["movie"]["ids"]["imdb"], "tmdbid": 550}}
                self.rows = [row for row in self.rows if not (row.get("type") == "movie" and (row.get("movie") or {}).get("ids", {}).get("imdbid") == movie["ids"]["imdbid"])]
                self.rows.append({"id": self.next_id, "type": "movie", "progress": body["progress"], "paused_at": "2026-07-25T12:00:00Z", "movie": movie, "episode": None, "show": None})
            elif "show" in body:
                show_body = dict(body["show"])
                season = dict(show_body["season"])
                episode = dict(season["episode"])
                self.rows.append(
                    {
                        "id": self.next_id,
                        "type": "episode",
                        "progress": body["progress"],
                        "paused_at": "2026-07-25T12:00:00Z",
                        "movie": None,
                        "episode": {"season": season["number"], "number": episode["number"], "ids": {}},
                        "show": {"title": "Friends", "year": 1994, "ids": {"imdbid": show_body["ids"]["imdb"], "tmdbid": 1668, "traktid": 1668, "tvdbid": 79168}},
                    }
                )
            return Response(200, {"id": self.next_id, "action": "pause", "progress": body.get("progress")})
        if url.endswith("/scrobble/clear"):
            playback_id = int(body["id"])
            self.rows = [row for row in self.rows if int(row.get("id") or 0) != playback_id]
            return Response(200, {"action": "clear", "deleted": True})
        return Response(404, {"error": "not found"})


class FakeAdapter:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.client = FakeClient(rows)
        self.cfg = type("Cfg", (), {"api_key": "k"})()


def movie_row(progress: float = 45.25) -> dict[str, Any]:
    return {
        "id": 42,
        "progress": progress,
        "paused_at": "2026-07-25T10:00:00Z",
        "type": "movie",
        "movie": {"title": "Fight Club", "year": 1999, "ids": {"imdbid": "tt0137523", "tmdbid": 550, "traktid": 1}},
        "episode": None,
        "show": None,
    }


def episode_row(progress: float = 68.5) -> dict[str, Any]:
    return {
        "id": 43,
        "progress": progress,
        "paused_at": "2026-07-25T10:00:00Z",
        "type": "episode",
        "movie": None,
        "episode": {"season": 1, "number": 3, "title": "The One with the Thumb", "ids": {"imdbid": "tt0583459", "tmdbid": 63174, "traktid": 73640, "tvdbid": 349232}},
        "show": {"title": "Friends", "year": 1994, "ids": {"imdbid": "tt0108778", "tmdbid": 1668, "traktid": 1668, "tvdbid": 79168}},
    }


def test_build_index_maps_official_playback_rows_as_percent() -> None:
    from providers.sync.mdblist import _progress

    index = _progress.build_index(FakeAdapter([movie_row(), episode_row()]))

    assert index["tmdb:550"]["progress_percent"] == 45.25
    assert "progress_ms" not in index["tmdb:550"]
    assert index["tmdb:550"]["_mdblist_playback_id"] == 42
    assert index["tmdb:1668#s01e03"]["progress_percent"] == 68.5


def test_add_movie_uses_documented_scrobble_pause_payload() -> None:
    from providers.sync.mdblist import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "movie",
        "ids": {"imdb": "tt0137523", "tmdb": "550"},
        "progress_ms": 120_000,
        "duration_ms": 600_000,
        "progress_at": "2026-07-25T11:00:00Z",
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:550"]
    post = [call for call in adapter.client.calls if call[0] == "POST" and call[1].endswith("/scrobble/pause")][0]
    assert post[2] == {"movie": {"ids": {"imdb": "tt0137523"}}, "progress": 20.0, "app_version": _progress._app_version()}


def test_add_episode_uses_documented_nested_show_season_episode_payload() -> None:
    from providers.sync.mdblist import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "show_ids": {"imdb": "tt0108778", "tmdb": "1668"},
        "ids": {"imdb": "tt0583459"},
        "season": 1,
        "episode": 3,
        "progress_ms": 411_000,
        "duration_ms": 600_000,
        "progress_at": "2026-07-25T11:00:00Z",
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    post = [call for call in adapter.client.calls if call[0] == "POST" and call[1].endswith("/scrobble/pause")][0]
    assert post[2] == {
        "show": {"ids": {"imdb": "tt0108778"}, "season": {"number": 1, "episode": {"number": 3}}},
        "progress": 68.5,
        "app_version": _progress._app_version(),
    }


def test_add_episode_requires_documented_show_imdb_and_episode_numbers() -> None:
    from providers.sync.mdblist import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "show_ids": {"tmdb": "1668"},
        "ids": {"imdb": "tt0583459"},
        "season": 1,
        "episode": 3,
        "progress_ms": 411_000,
        "duration_ms": 600_000,
        "progress_at": "2026-07-25T11:00:00Z",
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is False
    assert result["attempted"] == 0
    assert result["unresolved"][0]["reason"] == "mdblist_show_imdb_missing"
    assert not any(call[0] == "POST" for call in adapter.client.calls)


def test_remove_clears_current_playback_id() -> None:
    from providers.sync.mdblist import _progress

    adapter = FakeAdapter([movie_row()])

    result = _progress.remove(adapter, [{"type": "movie", "ids": {"imdb": "tt0137523", "tmdb": "550"}}])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:550"]
    post = [call for call in adapter.client.calls if call[0] == "POST" and call[1].endswith("/scrobble/clear")][0]
    assert post[2] == {"id": 42}


def test_mdblist_module_exposes_progress_feature() -> None:
    import providers.sync._mod_MDBLIST as mdblist_mod

    assert mdblist_mod.OPS.features()["progress"] is True
    assert mdblist_mod.OPS.capabilities()["progress"]["upsert"] is True
    assert mdblist_mod.OPS.capabilities()["progress"]["remove"] is True
