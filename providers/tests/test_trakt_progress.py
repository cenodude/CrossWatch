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
    BASE = "https://api.trakt.tv"

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = list(rows or [])
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []
        self.next_id = 1000

    def get(self, url: str, **kwargs: Any) -> Response:
        self.calls.append(("GET", url, kwargs.get("params")))
        if url.endswith("/sync/playback/movies"):
            rows = [row for row in self.rows if row.get("type") == "movie"]
        elif url.endswith("/sync/playback/episodes"):
            rows = [row for row in self.rows if row.get("type") == "episode"]
        else:
            rows = []
        return Response(200, rows)

    def post(self, url: str, json: Mapping[str, Any], **_: Any) -> Response:
        self.calls.append(("POST", url, dict(json)))
        self.next_id += 1
        if "movie" in json:
            movie = dict(json["movie"])
            movie.setdefault("runtime", 10)
            self.rows = [row for row in self.rows if not (row.get("type") == "movie" and (row.get("movie") or {}).get("ids") == movie.get("ids"))]
            self.rows.append({"id": self.next_id, "type": "movie", "progress": json["progress"], "paused_at": "2026-07-25T12:00:00Z", "movie": movie})
        elif "episode" in json:
            episode = dict(json["episode"])
            episode.setdefault("runtime", 20)
            episode.setdefault("season", 1)
            episode.setdefault("number", 1)
            episode.setdefault("title", "Pilot")
            self.rows = [row for row in self.rows if not (row.get("type") == "episode" and (row.get("episode") or {}).get("ids") == episode.get("ids"))]
            show = dict(json.get("show") or {"title": "Show", "year": 2026, "ids": {"trakt": 1, "slug": "show"}})
            self.rows.append({"id": self.next_id, "type": "episode", "progress": json["progress"], "paused_at": "2026-07-25T12:00:00Z", "episode": episode, "show": show})
        return Response(201, {"id": self.next_id, "progress": json["progress"], "action": "pause"})

    def delete(self, url: str, **_: Any) -> Response:
        self.calls.append(("DELETE", url, None))
        playback_id = int(url.rsplit("/", 1)[-1])
        self.rows = [row for row in self.rows if int(row.get("id") or 0) != playback_id]
        return Response(204)


class FakeAdapter:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.client = FakeClient(rows)
        self.cfg = type("Cfg", (), {"progress_per_page": 100, "progress_max_pages": 10})()


def movie_row(progress: float = 20.0) -> dict[str, Any]:
    return {
        "id": 42,
        "type": "movie",
        "progress": progress,
        "paused_at": "2026-07-25T10:00:00Z",
        "movie": {
            "title": "Fight Club",
            "year": 1999,
            "runtime": 10,
            "ids": {"trakt": 1, "slug": "fight-club-1999", "imdb": "tt0137523", "tmdb": 550},
        },
    }


def test_build_index_maps_official_playback_rows() -> None:
    from providers.sync.trakt import _progress

    index = _progress.build_index(FakeAdapter([movie_row()]))

    assert list(index) == ["tmdb:550"]
    item = index["tmdb:550"]
    assert item["progress_ms"] == 120_000
    assert item["duration_ms"] == 600_000
    assert item["progress_at"] == "2026-07-25T10:00:00Z"
    assert item["_trakt_playback_id"] == 42


def test_add_movie_uses_documented_scrobble_pause_payload() -> None:
    from providers.sync.trakt import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "movie",
        "title": "Fight Club",
        "year": 1999,
        "ids": {"tmdb": "550"},
        "progress_ms": 120_000,
        "duration_ms": 600_000,
        "progress_at": "2026-07-25T11:00:00Z",
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:550"]
    post = [call for call in adapter.client.calls if call[0] == "POST"][0]
    assert post[1] == "https://api.trakt.tv/scrobble/pause"
    assert post[2] == {"progress": 20.0, "movie": {"title": "Fight Club", "year": 1999, "ids": {"tmdb": 550}}}


def test_add_movie_accepts_percent_only_progress() -> None:
    from providers.sync.trakt import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "movie",
        "title": "Fight Club",
        "year": 1999,
        "ids": {"tmdb": "550"},
        "progress_percent": 21.0,
        "progress_at": "2026-07-25T11:00:00Z",
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:550"]
    post = [call for call in adapter.client.calls if call[0] == "POST"][0]
    assert post[2] == {"progress": 21.0, "movie": {"title": "Fight Club", "year": 1999, "ids": {"tmdb": 550}}}


def test_add_episode_requires_episode_or_show_identity() -> None:
    from providers.sync.trakt import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "season": 1,
        "episode": 1,
        "progress_ms": 120_000,
        "duration_ms": 1_200_000,
        "progress_at": "2026-07-25T11:00:00Z",
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is False
    assert result["attempted"] == 0
    assert result["unresolved"][0]["reason"] == "trakt_episode_id_missing"
    assert not any(call[0] == "POST" for call in adapter.client.calls)


def test_add_episode_accepts_documented_episode_tmdb_id() -> None:
    from providers.sync.trakt import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "ids": {"tmdb": "62085"},
        "show_ids": {"trakt": "1"},
        "season": 1,
        "episode": 1,
        "progress_ms": 120_000,
        "duration_ms": 1_200_000,
        "progress_at": "2026-07-25T11:00:00Z",
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    post = [call for call in adapter.client.calls if call[0] == "POST"][0]
    assert post[2] == {"progress": 10.0, "show": {"ids": {"trakt": 1}}, "episode": {"season": 1, "number": 1}}


def test_add_episode_accepts_show_coordinate_and_percent_only_progress() -> None:
    from providers.sync.trakt import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "series_title": "The Handmaid's Tale",
        "year": 2017,
        "show_ids": {"tmdb": "69478"},
        "season": 6,
        "episode": 2,
        "progress_percent": 21.0,
        "progress_at": "2026-07-25T11:00:00Z",
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:69478#s06e02"]
    post = [call for call in adapter.client.calls if call[0] == "POST"][0]
    assert post[2] == {
        "progress": 21.0,
        "show": {"ids": {"tmdb": 69478}, "title": "The Handmaid's Tale", "year": 2017},
        "episode": {"season": 6, "number": 2},
    }


def test_add_episode_prefers_show_coordinate_when_ids_duplicate_show_id() -> None:
    from providers.sync.trakt import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "series_title": "House of the Dragon",
        "ids": {"tmdb": "94997"},
        "show_ids": {"tmdb": "94997"},
        "season": 2,
        "episode": 7,
        "progress_percent": 21.0,
        "progress_at": "2026-07-25T20:41:00Z",
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:94997#s02e07"]
    post = [call for call in adapter.client.calls if call[0] == "POST"][0]
    assert post[2] == {
        "progress": 21.0,
        "show": {"ids": {"tmdb": 94997}, "title": "House of the Dragon"},
        "episode": {"season": 2, "number": 7},
    }


def test_remove_deletes_current_playback_id() -> None:
    from providers.sync.trakt import _progress

    adapter = FakeAdapter([movie_row()])

    result = _progress.remove(adapter, [{"type": "movie", "ids": {"tmdb": "550"}}])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:550"]
    assert ("DELETE", "https://api.trakt.tv/sync/playback/42", None) in adapter.client.calls


def test_trakt_module_exposes_progress_feature() -> None:
    import providers.sync._mod_TRAKT as trakt_mod

    assert trakt_mod.OPS.features()["progress"] is True
    assert trakt_mod.OPS.capabilities()["progress"]["upsert"] is True
    assert trakt_mod.OPS.capabilities()["progress"]["remove"] is True
    assert trakt_mod.OPS.capabilities()["progress"]["completion_policy"]["progress_write"]["mode"] == "none"
    assert trakt_mod.OPS.capabilities()["progress"]["completion_policy"]["stop_scrobble"]["marks_watched_percent"] == 80
