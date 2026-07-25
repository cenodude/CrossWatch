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
        if name == "sync_pull_watch_progress":
            since = int(body.get("p_since_last_watched") or 0)
            limit = int(body.get("p_limit") or 1000)
            rows = [row for row in self.rows if int(row.get("last_watched") or 0) > since]
            return rows[:limit]
        if name == "sync_push_watch_progress":
            for entry in body.get("p_entries") or []:
                row = dict(entry)
                row["id"] = row.get("content_id")
                row["progress_key"] = row.get("content_id") if not row.get("season") else f"{row.get('content_id')}_s{row.get('season')}e{row.get('episode')}"
                row["profile_id"] = body.get("p_profile_id")
                self.rows = [old for old in self.rows if old.get("progress_key") != row["progress_key"]]
                self.rows.append(row)
            return {}
        if name == "sync_delete_watch_progress":
            keys = set(str(x) for x in body.get("p_keys") or [])
            single = str(body.get("p_progress_key") or "")
            if single:
                keys.add(single)
            self.rows = [row for row in self.rows if str(row.get("progress_key") or "") not in keys]
            return {}
        return {}


class FakeAdapter:
    def __init__(self, rows: list[dict[str, Any]] | None = None, profile_id: int = 1):
        self.config = {
            "nuvio": {
                "base_url": "https://api.nuvio.tv",
                "access_token": "access",
                "refresh_token": "refresh",
                "profile_id": profile_id,
            }
        }
        self.instance_id = "default"
        self.client = FakeClient(rows)


def _row(content_id: str, position: int = 120_000, duration: int = 600_000, last_watched: int = 1_785_000_000_000) -> dict[str, Any]:
    return {
        "id": content_id,
        "profile_id": 1,
        "content_id": content_id,
        "content_type": "movie",
        "video_id": content_id,
        "season": None,
        "episode": None,
        "progress_key": content_id,
        "position": position,
        "duration": duration,
        "last_watched": last_watched,
    }


def test_build_index_maps_movies_and_skips_malformed_rows() -> None:
    from providers.sync.nuvio import _progress

    adapter = FakeAdapter([_row("tt1234567"), {"content_type": "movie", "content_id": "bad"}, {"content_type": "series", "content_id": "tmdb:99"}])

    index = _progress.build_index(adapter)

    assert list(index) == ["imdb:tt1234567"]
    item = index["imdb:tt1234567"]
    assert item["type"] == "movie"
    assert item["ids"] == {"imdb": "tt1234567"}
    assert item["progress_ms"] == 120_000
    assert item["duration_ms"] == 600_000
    assert item["progress_at"] == 1_785_000_000_000
    assert item["_nuvio_video_id"] == "tt1234567"
    assert item["progress_key"] == "tt1234567"


def test_build_index_merges_duplicate_movie_progress_by_newest_last_watched() -> None:
    from providers.sync.nuvio import _progress

    adapter = FakeAdapter([_row("tmdb:550", position=10_000, last_watched=1_785_000_000_000), _row("tmdb:550", position=20_000, last_watched=1_785_000_001_000)])

    index = _progress.build_index(adapter)

    assert index["tmdb:550"]["progress_ms"] == 20_000
    assert index["tmdb:550"]["progress_at"] == 1_785_000_001_000


def test_build_index_enriches_series_progress_title_from_tmdb(monkeypatch: Any) -> None:
    from providers.metadata import _meta_TMDB
    from providers.sync.nuvio import _progress

    class FakeTmdb:
        def __init__(self, *_: Any, **__: Any) -> None:
            pass

        def fetch(self, *, entity: str, ids: dict[str, str], locale: str | None = None, need: dict[str, bool] | None = None) -> dict[str, Any]:
            assert entity == "tv"
            assert ids == {"tmdb": "69478"}
            return {"title": "The Boys"}

    adapter = FakeAdapter(
        [
            {
                "content_id": "tmdb:69478",
                "content_type": "series",
                "video_id": "tmdb:69478:6:2",
                "season": 6,
                "episode": 2,
                "progress_key": "tmdb:69478_s6e2",
                "position": 703_000,
                "duration": 3_307_930,
                "last_watched": 1_784_848_068_000,
            }
        ]
    )
    adapter.config["tmdb"] = {"api_key": "tmdb-key"}
    monkeypatch.setattr(_meta_TMDB, "TmdbProvider", FakeTmdb)

    item = _progress.build_index(adapter)["tmdb:69478#s06e02"]

    assert item["series_title"] == "The Boys"


def test_add_encodes_tmdb_and_verifies_exact_keys() -> None:
    from providers.sync.nuvio import _progress

    adapter = FakeAdapter([])
    items = [
        {"type": "movie", "ids": {"tmdb": "550"}, "progress_ms": 200_000, "duration_ms": 600_000, "progress_at": 1_785_000_100_000},
    ]

    result = _progress.add(adapter, items)

    assert result["ok"] is True
    assert result["attempted"] == 1
    assert result["confirmed_keys"] == ["tmdb:550"]
    push = [body for name, body in adapter.client.calls if name == "sync_push_watch_progress"][0]
    assert push["p_profile_id"] == 1
    assert [entry["content_id"] for entry in push["p_entries"]] == ["tmdb:550"]
    assert all(entry["duration"] for entry in push["p_entries"])
    assert push["p_entries"][0]["last_watched"] == 1_785_000_100_000


def test_add_encodes_percent_progress_as_position() -> None:
    from providers.sync.nuvio import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "movie",
        "ids": {"tmdb": "550"},
        "progress_percent": 25,
        "duration_ms": 600_000,
        "progress_at": 1_785_000_100_000,
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    push = [body for name, body in adapter.client.calls if name == "sync_push_watch_progress"][0]
    assert push["p_entries"][0]["position"] == 150_000
    assert push["p_entries"][0]["duration"] == 600_000


def test_add_movie_progress_resolves_imdb_to_tmdb_content_id_when_tmdb_configured(monkeypatch: Any) -> None:
    from providers.metadata import _meta_TMDB
    from providers.sync.nuvio import _progress

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
    item = {"type": "movie", "ids": {"imdb": "tt0137523"}, "progress_ms": 100_000, "duration_ms": 500_000, "progress_at": 1_785_000_000_000}

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["imdb:tt0137523"]
    push = [body for name, body in adapter.client.calls if name == "sync_push_watch_progress"][0]
    assert push["p_entries"][0]["content_id"] == "tmdb:550"


def test_add_returns_unresolved_for_unsupported_id_and_missing_duration() -> None:
    from providers.sync.nuvio import _progress

    adapter = FakeAdapter([])
    result = _progress.add(
        adapter,
        [
            {"type": "movie", "ids": {"tvdb": "99"}, "progress_ms": 100_000, "duration_ms": 500_000, "progress_at": 1_785_000_000_000},
            {"type": "movie", "ids": {"imdb": "tt0137523"}, "progress_ms": 100_000, "duration_ms": 500_000, "progress_at": 1_785_000_000_000},
            {"type": "movie", "ids": {"trakt": "12"}, "progress_ms": 100_000, "duration_ms": 500_000, "progress_at": 1_785_000_000_000},
            {"type": "movie", "ids": {"tmdb": "550"}, "progress_ms": 100_000, "progress_at": 1_785_000_000_000},
        ],
        dry_run=True,
    )

    assert result["ok"] is False
    assert result["attempted"] == 0
    assert {row["reason"] for row in result["unresolved"]} == {"nuvio_id_missing", "nuvio_duration_missing"}
    assert not any(name == "sync_push_watch_progress" for name, _body in adapter.client.calls)


def test_add_does_not_write_title_only_unresolved_item() -> None:
    from providers.sync.nuvio import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "movie",
        "title": "Untold UK Liverpool's Miracle of Istanbul",
        "year": 2026,
        "ids": {"jellyfin": "26889526862003328"},
        "progress_ms": 100_000,
        "duration_ms": 500_000,
        "progress_at": 1_785_000_000_000,
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is False
    assert result["attempted"] == 0
    assert result["unresolved"][0]["reason"] == "nuvio_id_missing"
    assert not any(name == "sync_push_watch_progress" for name, _body in adapter.client.calls)


def test_add_is_idempotent_for_unchanged_progress_and_allows_newer_rewind() -> None:
    from providers.sync.nuvio import _progress

    same = {"type": "movie", "ids": {"tmdb": "550"}, "progress_ms": 120_000, "duration_ms": 600_000, "progress_at": 1_785_000_000_000}
    adapter = FakeAdapter([_row("tmdb:550", position=120_000, duration=600_000, last_watched=1_785_000_000_000)])

    unchanged = _progress.add(adapter, [same])
    rewind = _progress.add(adapter, [{**same, "progress_ms": 60_000, "progress_at": 1_785_001_000_000}])

    assert unchanged["attempted"] == 0
    assert unchanged["skipped"] == 1
    assert rewind["attempted"] == 1
    assert rewind["confirmed_keys"] == ["tmdb:550"]
    assert _progress.build_index(adapter)["tmdb:550"]["progress_ms"] == 60_000


def test_remove_deletes_by_progress_key_and_treats_missing_as_idempotent() -> None:
    from providers.sync.nuvio import _progress

    adapter = FakeAdapter([_row("tmdb:550")])
    item = {"type": "movie", "ids": {"tmdb": "550"}}

    removed = _progress.remove(adapter, [item])
    removed_again = _progress.remove(adapter, [item])

    assert removed["ok"] is True
    assert removed["confirmed_keys"] == ["tmdb:550"]
    assert removed_again["ok"] is True
    assert removed_again["attempted"] == 0
    assert removed_again["skipped"] == 1
    assert _progress.build_index(adapter) == {}


def test_add_episode_progress_uses_documented_series_payload() -> None:
    from providers.sync.nuvio import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "show_ids": {"tmdb": "1396"},
        "season": 1,
        "episode": 1,
        "progress_ms": 100_000,
        "duration_ms": 2_600_000,
        "progress_at": 1_785_000_000_000,
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:1396#s01e01"]
    push = [body for name, body in adapter.client.calls if name == "sync_push_watch_progress"][0]
    entry = push["p_entries"][0]
    assert entry["content_id"] == "tmdb:1396"
    assert entry["content_type"] == "series"
    assert entry["video_id"] == "tmdb:1396:1:1"
    assert entry["season"] == 1
    assert entry["episode"] == 1


def test_add_episode_progress_prefers_show_tmdb_id_over_episode_tmdb_id() -> None:
    from providers.sync.nuvio import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "ids": {"tmdb": "5978363", "imdb": "tt35707151"},
        "show_ids": {"tmdb": "69478"},
        "season": 6,
        "episode": 2,
        "progress_ms": 703_000,
        "duration_ms": 3_307_930,
        "progress_at": 1_785_000_000_000,
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:69478#s06e02"]
    push = [body for name, body in adapter.client.calls if name == "sync_push_watch_progress"][0]
    entry = push["p_entries"][0]
    assert entry["content_id"] == "tmdb:69478"
    assert entry["video_id"] == "tmdb:69478:6:2"


def test_add_episode_progress_resolves_tvdb_to_tmdb_content_id_when_tmdb_configured(monkeypatch: Any) -> None:
    from providers.metadata import _meta_TMDB
    from providers.sync.nuvio import _progress

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
        "season": 6,
        "episode": 2,
        "progress_ms": 703_000,
        "duration_ms": 3_307_930,
        "progress_at": 1_785_000_000_000,
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tvdb:355567#s06e02"]
    push = [body for name, body in adapter.client.calls if name == "sync_push_watch_progress"][0]
    entry = push["p_entries"][0]
    assert entry["content_id"] == "tmdb:69478"
    assert entry["video_id"] == "tmdb:69478:6:2"


def test_add_episode_progress_requires_show_ids() -> None:
    from providers.sync.nuvio import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "ids": {"tmdb": "5978363"},
        "season": 6,
        "episode": 2,
        "progress_ms": 703_000,
        "duration_ms": 3_307_930,
        "progress_at": 1_785_000_000_000,
    }

    result = _progress.add(adapter, [item], dry_run=True)

    assert result["ok"] is False
    assert result["unresolved"][0]["reason"] == "nuvio_id_missing"


def test_dry_run_does_not_write() -> None:
    from cw_platform.id_map import canonical_key
    from providers.sync.nuvio import _progress

    adapter = FakeAdapter([])
    item = {"type": "movie", "ids": {"tmdb": "550"}, "progress_ms": 100_000, "duration_ms": 500_000, "progress_at": 1_785_000_000_000}

    result = _progress.add(adapter, [item], dry_run=True)

    assert result["ok"] is True
    assert result["attempted"] == 1
    assert result["confirmed_keys"] == []
    assert not any(name == "sync_push_watch_progress" for name, _body in adapter.client.calls)
    assert canonical_key(item) == "tmdb:550"


def test_profile_id_is_sent_with_progress_requests() -> None:
    from providers.sync.nuvio import _progress

    adapter = FakeAdapter([], profile_id=3)
    item = {"type": "movie", "ids": {"tmdb": "550"}, "progress_ms": 100_000, "duration_ms": 500_000, "progress_at": 1_785_000_000_000}

    _progress.add(adapter, [item])

    calls = [body for name, body in adapter.client.calls if name in {"sync_pull_watch_progress", "sync_push_watch_progress"}]
    assert calls
    assert {body["p_profile_id"] for body in calls} == {3}
