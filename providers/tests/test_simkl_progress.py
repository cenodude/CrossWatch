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
    BASE = "https://api.simkl.com"

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = list(rows or [])
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []
        self.next_id = 1000

    def _request(self, method: str, url: str, **kwargs: Any) -> Response:
        if method == "GET":
            self.calls.append(("GET", url, kwargs.get("params")))
            return Response(200, list(self.rows))
        if method == "POST":
            body = dict(kwargs.get("json") or {})
            self.calls.append(("POST", url, body))
            self.next_id += 1
            if "movie" in body:
                movie = dict(body["movie"])
                self.rows = [row for row in self.rows if not (row.get("type") == "movie" and (row.get("movie") or {}).get("ids") == movie.get("ids"))]
                self.rows.append({"id": self.next_id, "type": "movie", "progress": body["progress"], "paused_at": "2026-07-25T12:00:00Z", "movie": movie})
            elif "show" in body:
                show = dict(body["show"])
                episode = dict(body["episode"])
                self.rows.append({"id": self.next_id, "type": "episode", "progress": body["progress"], "paused_at": "2026-07-25T12:00:00Z", "show": show, "episode": episode})
            elif "anime" in body:
                anime = dict(body["anime"])
                episode = dict(body["episode"])
                self.rows.append({"id": self.next_id, "type": "episode", "progress": body["progress"], "paused_at": "2026-07-25T12:00:00Z", "anime": anime, "episode": episode})
            return Response(201, {"id": self.next_id, "action": "pause", "progress": body.get("progress")})
        if method == "DELETE":
            self.calls.append(("DELETE", url, None))
            playback_id = int(url.rsplit("/", 1)[-1])
            self.rows = [row for row in self.rows if int(row.get("id") or 0) != playback_id]
            return Response(204)
        return Response(405, {})


class FakeAdapter:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.client = FakeClient(rows)
        self.cfg = type("Cfg", (), {"progress_limit": 10000, "date_from": ""})()


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
            "ids": {"simkl": 10, "slug": "fight-club", "imdb": "tt0137523", "tmdb": 550},
        },
    }


def episode_row() -> dict[str, Any]:
    return {
        "id": 43,
        "type": "episode",
        "progress": 50,
        "paused_at": "2026-07-25T10:00:00Z",
        "episode": {"season": 1, "number": 3, "title": "Holly Jolly", "runtime": 40},
        "show": {"title": "Stranger Things", "year": 2016, "ids": {"simkl": 39687, "imdb": "tt4574334", "tvdb": 305288}},
    }


def test_build_index_maps_official_playback_rows() -> None:
    from providers.sync.simkl import _progress

    index = _progress.build_index(FakeAdapter([movie_row(), episode_row()]))

    assert index["tmdb:550"]["progress_ms"] == 120_000
    assert index["tmdb:550"]["duration_ms"] == 600_000
    assert index["tmdb:550"]["progress_percent"] == 20.0
    assert index["tmdb:550"]["_simkl_playback_id"] == 42
    assert index["imdb:tt4574334#s01e03"]["progress_ms"] == 1_200_000


def test_add_movie_uses_documented_scrobble_pause_payload() -> None:
    from providers.sync.simkl import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "movie",
        "title": "Fight Club",
        "year": 1999,
        "ids": {"tmdb": "550", "slug": "fight-club"},
        "progress_ms": 120_000,
        "duration_ms": 600_000,
        "progress_at": "2026-07-25T11:00:00Z",
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:550"]
    post = [call for call in adapter.client.calls if call[0] == "POST"][0]
    assert post[1] == "https://api.simkl.com/scrobble/pause"
    assert post[2] == {"progress": 20.0, "movie": {"ids": {"tmdb": 550}, "title": "Fight Club", "year": 1999}}


def test_add_episode_uses_show_and_episode_shape() -> None:
    from providers.sync.simkl import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "series_title": "Stranger Things",
        "show_ids": {"tvdb": "305288", "slug": "stranger-things"},
        "season": 1,
        "episode": 3,
        "progress_ms": 1_200_000,
        "duration_ms": 2_400_000,
        "progress_at": "2026-07-25T11:00:00Z",
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    post = [call for call in adapter.client.calls if call[0] == "POST"][0]
    assert post[2] == {
        "progress": 50.0,
        "show": {"ids": {"tvdb": "305288"}, "title": "Stranger Things"},
        "episode": {"season": 1, "number": 3},
    }


def test_add_episode_prefers_show_coordinate_when_ids_duplicate_show_id() -> None:
    from providers.sync.simkl import _progress

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


def test_add_episode_requires_documented_parent_or_episode_identity() -> None:
    from providers.sync.simkl import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "show_ids": {"slug": "stranger-things"},
        "season": 1,
        "episode": 3,
        "progress_ms": 1_200_000,
        "duration_ms": 2_400_000,
        "progress_at": "2026-07-25T11:00:00Z",
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is False
    assert result["attempted"] == 0
    assert result["unresolved"][0]["reason"] == "simkl_parent_id_missing"
    assert not any(call[0] == "POST" for call in adapter.client.calls)


def test_remove_deletes_current_playback_id() -> None:
    from providers.sync.simkl import _progress

    adapter = FakeAdapter([movie_row()])

    result = _progress.remove(adapter, [{"type": "movie", "ids": {"tmdb": "550"}}])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:550"]
    assert ("DELETE", "https://api.simkl.com/sync/playback/42", None) in adapter.client.calls


def test_simkl_module_exposes_progress_feature() -> None:
    import providers.sync._mod_SIMKL as simkl_mod

    assert simkl_mod.OPS.features()["progress"] is True
    assert simkl_mod.OPS.capabilities()["progress"]["upsert"] is True
    assert simkl_mod.OPS.capabilities()["progress"]["remove"] is True
