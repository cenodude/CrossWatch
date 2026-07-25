# CrossWatch test scripts
from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class FakeClient:
    def __init__(self, rows: list[dict[str, Any]] | None = None):
        self.rows = list(rows or [])
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def request_json(self, method: str, path: str, *, payload: Mapping[str, Any] | None = None, **_: Any) -> Any:
        body = dict(payload or {})
        name = path.rsplit("/", 1)[-1]
        self.calls.append((name, body))
        if name == "sync_pull_watched_items":
            page = int(body.get("p_page") or 1)
            size = int(body.get("p_page_size") or 900)
            start = (page - 1) * size
            return self.rows[start : start + size]
        if name == "sync_push_watched_items":
            for entry in body.get("p_items") or []:
                row = dict(entry)
                row["profile_id"] = body.get("p_profile_id")
                self.rows.append(row)
            return {}
        if name == "sync_delete_watched_items":
            keys = body.get("p_keys") or []
            for key in keys:
                if not isinstance(key, Mapping):
                    continue
                self.rows = [
                    row
                    for row in self.rows
                    if not (
                        row.get("content_id") == key.get("content_id")
                        and row.get("season") == key.get("season")
                        and row.get("episode") == key.get("episode")
                    )
                ]
            return {}
        return {}


class FakeAdapter:
    def __init__(self, rows: list[dict[str, Any]] | None = None):
        self.config = {"nuvio": {"base_url": "https://api.nuvio.tv", "refresh_token": "refresh", "profile_id": 1}}
        self.instance_id = "default"
        self.client = FakeClient(rows)


def test_history_reads_and_deletes_episode_with_official_key_shape() -> None:
    from providers.sync.nuvio import _history

    adapter = FakeAdapter(
        [
            {
                "profile_id": 1,
                "content_id": "tmdb:1396",
                "content_type": "series",
                "title": "Breaking Bad",
                "season": 1,
                "episode": 1,
                "watched_at": 1_785_000_000_000,
            }
        ]
    )

    index = _history.build_index(adapter)
    assert list(index) == ["tmdb:1396#s01e01"]

    result = _history.remove(adapter, [{"type": "episode", "show_ids": {"tmdb": "1396"}, "season": 1, "episode": 1}])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:1396#s01e01"]
    delete = [body for name, body in adapter.client.calls if name == "sync_delete_watched_items"][0]
    assert delete["p_keys"] == [{"content_id": "tmdb:1396", "season": 1, "episode": 1}]


def test_history_adds_movie_and_verifies() -> None:
    from providers.sync.nuvio import _history

    adapter = FakeAdapter([])
    result = _history.add(adapter, [{"type": "movie", "ids": {"imdb": "tt0137523"}, "title": "Fight Club", "watched_at": 1_785_000_000_000}])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["imdb:tt0137523"]
    push = [body for name, body in adapter.client.calls if name == "sync_push_watched_items"][0]
    assert push["p_items"][0]["content_id"] == "tt0137523"
    assert push["p_items"][0]["content_type"] == "movie"


def test_history_adds_episode_with_show_tmdb_id_not_episode_tmdb_id() -> None:
    from providers.sync.nuvio import _history

    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "ids": {"tmdb": "5978363", "imdb": "tt35707151"},
        "show_ids": {"tmdb": "69478"},
        "series_title": "The Boys",
        "season": 6,
        "episode": 2,
        "watched_at": 1_785_000_000_000,
    }

    result = _history.add(adapter, [item])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:69478#s06e02"]
    push = [body for name, body in adapter.client.calls if name == "sync_push_watched_items"][0]
    row = push["p_items"][0]
    assert row["content_id"] == "tmdb:69478"
    assert row["content_type"] == "series"
    assert row["season"] == 6
    assert row["episode"] == 2
