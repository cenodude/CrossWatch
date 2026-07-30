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


def test_history_index_exposes_watched_at_as_iso8601() -> None:
    from providers.sync.nuvio import _history

    adapter = FakeAdapter(
        [
            {
                "content_id": "tmdb:1396",
                "content_type": "series",
                "title": "Breaking Bad",
                "season": 1,
                "episode": 1,
                "watched_at": 1_785_000_000_000,
            }
        ]
    )

    item = _history.build_index(adapter)["tmdb:1396#s01e01"]

    assert item["watched_at"] == "2026-07-25T17:20:00Z"


def test_history_write_payload_sends_epoch_ms_for_iso_source_item() -> None:
    from providers.sync.nuvio import _history

    adapter = FakeAdapter([])
    result = _history.add(adapter, [{"type": "movie", "ids": {"tmdb": "550"}, "title": "Fight Club", "watched_at": "2026-07-25T17:20:00Z"}])

    assert result["ok"] is True
    push = [body for name, body in adapter.client.calls if name == "sync_push_watched_items"][0]
    assert push["p_items"][0]["watched_at"] == 1_785_000_000_000


def test_history_skips_unchanged_when_index_is_iso_and_source_is_epoch_ms() -> None:
    from providers.sync.nuvio import _history

    adapter = FakeAdapter(
        [
            {
                "content_id": "tmdb:550",
                "content_type": "movie",
                "title": "Fight Club",
                "watched_at": 1_785_000_000_000,
            }
        ]
    )

    result = _history.add(adapter, [{"type": "movie", "ids": {"tmdb": "550"}, "title": "Fight Club", "watched_at": 1_785_000_000_000}])

    assert result["skipped"] == 1
    assert [name for name, _ in adapter.client.calls if name == "sync_push_watched_items"] == []


def test_history_read_enriches_episode_code_title_from_tmdb(monkeypatch: Any) -> None:
    from providers.metadata import _meta_TMDB
    from providers.sync.nuvio import _history

    class FakeTmdb:
        def __init__(self, *_: Any, **__: Any) -> None:
            pass

        def fetch(self, *, entity: str, ids: dict[str, str], locale: str | None = None, need: dict[str, bool] | None = None) -> dict[str, Any]:
            assert entity == "tv"
            assert ids == {"tmdb": "299167"}
            return {"title": "Example Show"}

    adapter = FakeAdapter(
        [
            {
                "content_id": "tmdb:299167",
                "content_type": "series",
                "title": "S01E08",
                "season": 1,
                "episode": 8,
                "watched_at": 1_783_890_886_000,
            }
        ]
    )
    adapter.config["tmdb"] = {"api_key": "tmdb-key"}
    monkeypatch.setattr(_meta_TMDB, "TmdbProvider", FakeTmdb)

    item = _history.build_index(adapter)["tmdb:299167#s01e08"]

    assert item["series_title"] == "Example Show"


def test_history_read_strips_episode_code_from_series_title() -> None:
    from providers.sync.nuvio import _history

    adapter = FakeAdapter(
        [
            {
                "content_id": "tmdb:1396",
                "content_type": "series",
                "title": "Breaking Bad - S01E03",
                "season": 1,
                "episode": 3,
                "watched_at": 1_785_000_000_000,
            }
        ]
    )

    item = _history.build_index(adapter)["tmdb:1396#s01e03"]

    assert item["series_title"] == "Breaking Bad"
    assert item["season"] == 1
    assert item["episode"] == 3


def test_history_adds_movie_and_verifies() -> None:
    from providers.sync.nuvio import _history

    adapter = FakeAdapter([])
    result = _history.add(adapter, [{"type": "movie", "ids": {"tmdb": "550"}, "title": "Fight Club", "watched_at": 1_785_000_000_000}])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:550"]
    push = [body for name, body in adapter.client.calls if name == "sync_push_watched_items"][0]
    assert push["p_items"][0]["content_id"] == "tmdb:550"
    assert push["p_items"][0]["content_type"] == "movie"


def test_history_adds_movie_resolves_imdb_to_tmdb_content_id_when_tmdb_configured(monkeypatch: Any) -> None:
    from providers.metadata import _meta_TMDB
    from providers.sync.nuvio import _history

    class FakeTmdb:
        def __init__(self, *_: Any, **__: Any) -> None:
            pass

        def fetch(self, *, entity: str, ids: dict[str, str], locale: str | None = None, need: dict[str, bool] | None = None) -> dict[str, Any]:
            assert entity == "movie"
            assert ids == {"imdb": "tt0137523"}
            return {"ids": {"tmdb": "550"}}

    adapter = FakeAdapter([])
    adapter.config["tmdb"] = {"api_key": "tmdb-key"}
    monkeypatch.setattr(_meta_TMDB, "TmdbProvider", FakeTmdb)

    result = _history.add(adapter, [{"type": "movie", "ids": {"imdb": "tt0137523"}, "title": "Fight Club", "watched_at": 1_785_000_000_000}])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["imdb:tt0137523"]
    push = [body for name, body in adapter.client.calls if name == "sync_push_watched_items"][0]
    assert push["p_items"][0]["content_id"] == "tmdb:550"


def test_history_adds_episode_with_show_tmdb_id_not_episode_tmdb_id() -> None:
    from providers.sync.nuvio import _history

    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "ids": {"tmdb": "5978363", "imdb": "tt35707151"},
        "show_ids": {"tmdb": "69478"},
        "series_title": "The Boys",
        "title": "The Big Ride",
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
    assert row["title"] == "The Boys - S06E02"
    assert row["season"] == 6
    assert row["episode"] == 2


def test_history_adds_episode_resolves_tvdb_to_tmdb_content_id_when_tmdb_configured(monkeypatch: Any) -> None:
    from providers.metadata import _meta_TMDB
    from providers.sync.nuvio import _history

    class FakeTmdb:
        def __init__(self, *_: Any, **__: Any) -> None:
            pass

        def fetch(self, *, entity: str, ids: dict[str, str], locale: str | None = None, need: dict[str, bool] | None = None) -> dict[str, Any]:
            assert entity == "tv"
            assert ids == {"tvdb": "355567"}
            return {"ids": {"tmdb": "69478"}}

    adapter = FakeAdapter([])
    adapter.config["tmdb"] = {"api_key": "tmdb-key"}
    monkeypatch.setattr(_meta_TMDB, "TmdbProvider", FakeTmdb)
    item = {
        "type": "episode",
        "show_ids": {"tvdb": "355567"},
        "series_title": "The Boys",
        "season": 6,
        "episode": 2,
        "watched_at": 1_785_000_000_000,
    }

    result = _history.add(adapter, [item])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tvdb:355567#s06e02"]
    push = [body for name, body in adapter.client.calls if name == "sync_push_watched_items"][0]
    row = push["p_items"][0]
    assert row["content_id"] == "tmdb:69478"
    assert row["content_type"] == "series"
    assert row["title"] == "The Boys - S06E02"
    assert row["season"] == 6
    assert row["episode"] == 2
