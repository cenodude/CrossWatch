from __future__ import annotations

import copy
from typing import Any

from providers.sync import _mod_STREMIO as mod
from providers.sync.stremio import _common, _history, _progress, _ratings, _watchlist


class FakeClient:
    def __init__(self, records: list[dict[str, Any]]):
        self.records = {str(row["_id"]): copy.deepcopy(row) for row in records}
        self.puts: list[list[dict[str, Any]]] = []

    def request_json(self, path: str, payload: dict[str, Any] | None = None, **_kwargs: Any) -> dict[str, Any]:
        payload = dict(payload or {})
        if path == "datastoreGet":
            if payload.get("all"):
                return {"result": list(copy.deepcopy(list(self.records.values())))}
            ids = [str(x) for x in payload.get("ids") or []]
            return {"result": [copy.deepcopy(self.records[x]) for x in ids if x in self.records]}
        if path == "datastoreMeta":
            return {"result": [{"_id": key, "_mtime": value.get("_mtime")} for key, value in self.records.items()]}
        if path == "datastorePut":
            changes = [copy.deepcopy(row) for row in payload.get("changes") or []]
            self.puts.append(changes)
            for row in changes:
                self.records[str(row["_id"])] = row
            return {"result": {"success": True}}
        raise AssertionError(path)


class FakeAdapter:
    def __init__(self, records: list[dict[str, Any]]):
        self.client = FakeClient(records)


class FakeResponse:
    def __init__(self, status_code: int = 200, text: str = "", payload: dict[str, Any] | None = None):
        self.status_code = status_code
        self.text = text
        self.payload = payload if payload is not None else {}

    def json(self) -> dict[str, Any]:
        return dict(self.payload)


class FakeLikesSession:
    def __init__(self):
        self.remote: dict[tuple[str, str], str | None] = {}
        self.gets: list[dict[str, Any]] = []
        self.posts: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.gets.append({"url": url, **kwargs})
        params = kwargs.get("params") or {}
        key = (str(params.get("mediaId") or ""), str(params.get("mediaType") or ""))
        return FakeResponse(payload={"status": self.remote.get(key)})

    def post(self, url: str, **kwargs: Any) -> FakeResponse:
        self.posts.append({"url": url, **kwargs})
        payload = kwargs.get("json") or {}
        key = (str(payload.get("mediaId") or ""), str(payload.get("mediaType") or ""))
        self.remote[key] = payload.get("status")
        return FakeResponse()


class FakeLikesClient:
    def __init__(self):
        self.session = FakeLikesSession()

    def auth_key(self) -> str:
        return "auth"


class FakeLikesAdapter:
    def __init__(self, config: dict[str, Any] | None = None):
        self.client = FakeLikesClient()
        self.config = config or {}
        self.instance_id = "default"


def movie_record(**extra: Any) -> dict[str, Any]:
    row = {
        "_id": "tt0137523",
        "type": "movie",
        "name": "Fight Club",
        "poster": "https://image.example/fight-club.jpg",
        "_mtime": 1_785_441_200_000,
        "state": {
            "lastWatched": 1_785_441_100_000,
            "timesWatched": 1,
            "flaggedWatched": 1,
            "timeOffset": 0,
            "duration": 8_400_000,
        },
    }
    row.update(extra)
    return row


def series_record(watched: str = "") -> dict[str, Any]:
    return {
        "_id": "tt0903747",
        "type": "series",
        "name": "Breaking Bad",
        "poster": "https://image.example/breaking-bad.jpg",
        "_mtime": 1_785_441_200_000,
        "state": {
            "watched": watched,
            "lastWatched": 1_785_441_100_000,
            "timesWatched": 7,
            "flaggedWatched": 1,
            "timeOffset": 0,
            "duration": 0,
            "video_id": None,
            "season": None,
            "episode": None,
        },
    }


def bb_videos() -> list[dict[str, Any]]:
    return [{"id": f"tt0903747:1:{episode}", "season": 1, "episode": episode, "title": f"Episode {episode}"} for episode in range(1, 8)]


def test_parse_watched_movie_record() -> None:
    item = _history.parse_movie_history_record(movie_record())

    assert item is not None
    assert item["watched"] is True
    assert item["ids"] == {"imdb": "tt0137523"}
    assert item["poster"] == "https://image.example/fight-club.jpg"


def test_parse_unwatched_movie_record() -> None:
    item = _history.parse_movie_history_record(movie_record(state={"lastWatched": None, "timesWatched": 0, "flaggedWatched": 0}))

    assert item is not None
    assert item["watched"] is False


def test_decode_breaking_bad_watched_value_as_s01e01() -> None:
    watched = _history.decode_watched_episodes("tt0903747:1:1:6:eJxTYIACAAEpACE=", [row["id"] for row in bb_videos()])

    assert watched == {"tt0903747:1:1"}


def test_mark_s01e02_watched_preserves_s01e01() -> None:
    video_ids = [row["id"] for row in bb_videos()]
    updated = _history.set_episode_watched_value("tt0903747:1:1:6:eJxTYIACAAEpACE=", video_ids, "tt0903747:1:2", True)

    assert _history.decode_watched_episodes(updated, video_ids) == {"tt0903747:1:1", "tt0903747:1:2"}


def test_mark_s01e01_unwatched_preserves_other_episodes() -> None:
    video_ids = [row["id"] for row in bb_videos()]
    both = _history.set_episode_watched_value("tt0903747:1:1:6:eJxTYIACAAEpACE=", video_ids, "tt0903747:1:2", True)
    updated = _history.set_episode_watched_value(both, video_ids, "tt0903747:1:1", False)

    assert _history.decode_watched_episodes(updated, video_ids) == {"tt0903747:1:2"}


def test_episode_history_read_does_not_expose_stremio_mtime_as_watched_at(monkeypatch) -> None:
    monkeypatch.setattr(_history, "cinemeta_videos", lambda _adapter, _imdb: bb_videos())
    adapter = FakeAdapter([series_record("tt0903747:1:1:6:eJxTYIACAAEpACE=")])

    index = _history.build_index(adapter)
    item = index["imdb:tt0903747#s01e01"]

    assert item["watched"] is True
    assert "watched_at" not in item
    assert item["_stremio_changed_at"] == "2026-07-30T19:53:20Z"


def test_history_write_resolves_imdb_with_tmdb_and_uses_metahub_poster(monkeypatch) -> None:
    class Provider:
        def fetch(self, **_kwargs: Any) -> dict[str, Any]:
            return {
                "ids": {"tmdb": "124800", "imdb": "tt14586350"},
                "images": {"poster": [{"url": "https://image.tmdb.org/t/p/w780/love-death.jpg"}]},
            }

    monkeypatch.setattr(_history, "tmdb_metadata_provider", lambda _adapter: Provider())
    monkeypatch.setattr(_history, "cinemeta_videos", lambda _adapter, _imdb: [{"id": "tt14586350:1:7", "season": 1, "episode": 7}])
    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "series_title": "Love & Death",
        "show_ids": {"tmdb": "124800"},
        "season": 1,
        "episode": 7,
        "watched_at": "2026-07-30T20:00:00Z",
    }

    result = _history.add(adapter, [item])
    written = adapter.client.puts[-1][0]

    assert result["count"] == 1
    assert written["_id"] == "tt14586350"
    assert written["poster"] == "https://images.metahub.space/poster/small/tt14586350/img"
    assert _history.decode_watched_episodes(written["state"]["watched"], ["tt14586350:1:7"]) == {"tt14586350:1:7"}


def test_history_write_batches_episodes_per_series(monkeypatch) -> None:
    monkeypatch.setattr(_history, "cinemeta_videos", lambda _adapter, _imdb: bb_videos())
    adapter = FakeAdapter([])
    items = [
        {"type": "episode", "series_title": "Breaking Bad", "show_ids": {"imdb": "tt0903747"}, "season": 1, "episode": 1},
        {"type": "episode", "series_title": "Breaking Bad", "show_ids": {"imdb": "tt0903747"}, "season": 1, "episode": 2},
    ]

    result = _history.add(adapter, items)
    written = adapter.client.puts[-1][0]

    assert result["count"] == 2
    assert len(adapter.client.puts) == 1
    assert len(adapter.client.puts[-1]) == 1
    assert _history.decode_watched_episodes(written["state"]["watched"], [row["id"] for row in bb_videos()]) == {"tt0903747:1:1", "tt0903747:1:2"}


def test_movie_unwatch_write_uses_empty_last_watched() -> None:
    adapter = FakeAdapter([movie_record()])
    item = {"type": "movie", "ids": {"imdb": "tt0137523"}, "title": "Fight Club"}

    result = _history.remove(adapter, [item])
    written = adapter.client.puts[-1][0]

    assert result["count"] == 1
    assert written["state"]["lastWatched"] == ""
    assert written["state"]["timesWatched"] == 0
    assert written["state"]["flaggedWatched"] == 0


def test_stremio_reuses_tmdb_metadata_provider(monkeypatch) -> None:
    class Provider:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

    adapter = FakeAdapter([])
    adapter.config = {"tmdb": {"api_key": "key"}}
    monkeypatch.setattr("providers.metadata._meta_TMDB.TmdbProvider", Provider)

    first = _common.tmdb_metadata_provider(adapter)
    second = _common.tmdb_metadata_provider(adapter)

    assert first is second


def test_parse_movie_progress_from_time_offset_and_duration() -> None:
    item = _progress.parse_movie_progress_record(movie_record(state={"timeOffset": 120_000, "duration": 600_000, "lastWatched": 1_785_441_100_000}))

    assert item is not None
    assert item["progress_ms"] == 120_000
    assert item["duration_ms"] == 600_000
    assert item["progress_percent"] == 20.0


def test_parse_episode_progress_uses_video_id() -> None:
    record = series_record()
    record["state"].update({"video_id": "tt0903747:1:2", "season": 1, "episode": 2, "timeOffset": 60_000, "duration": 600_000})

    item = _progress.parse_episode_progress_record(record)

    assert item is not None
    assert item["show_ids"] == {"imdb": "tt0903747"}
    assert item["season"] == 1
    assert item["episode"] == 2
    assert item["_stremio_video_id"] == "tt0903747:1:2"


def test_progress_write_preserves_history_bitfield_and_unknown_fields() -> None:
    record = series_record("tt0903747:1:1:6:eJxTYIACAAEpACE=")
    record["unknown"] = {"keep": True}
    adapter = FakeAdapter([record])
    item = {
        "type": "episode",
        "show_ids": {"imdb": "tt0903747"},
        "season": 1,
        "episode": 2,
        "progress_ms": 120_000,
        "duration_ms": 600_000,
    }

    result = _progress.add(adapter, [item])
    written = adapter.client.puts[-1][0]

    assert result["count"] == 1
    assert written["unknown"] == {"keep": True}
    assert written["state"]["watched"] == "tt0903747:1:1:6:eJxTYIACAAEpACE="
    assert written["state"]["timesWatched"] == 7
    assert written["state"]["flaggedWatched"] == 1
    assert written["state"]["lastWatched"] == 1_785_441_100_000
    assert written["state"]["video_id"] == "tt0903747:1:2"
    assert isinstance(written["_mtime"], str)
    assert written["_mtime"].endswith("Z")


def test_progress_write_enriches_percent_only_episode_from_tmdb(monkeypatch) -> None:
    class Provider:
        def fetch(self, **_kwargs: Any) -> dict[str, Any]:
            return {
                "ids": {"tmdb": "124800", "imdb": "tt14586350"},
                "runtime_minutes": None,
                "images": {"poster": [{"url": "https://image.tmdb.org/t/p/w780/love-death.jpg"}]},
            }

        def _get(self, _url: str) -> dict[str, Any]:
            return {"runtime": 49}

    monkeypatch.setattr(_progress, "tmdb_metadata_provider", lambda _adapter: Provider())
    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "series_title": "Love & Death",
        "show_ids": {"tmdb": "124800"},
        "season": 1,
        "episode": 7,
        "progress_percent": 10.0,
    }

    result = _progress.add(adapter, [item])
    written = adapter.client.puts[-1][0]

    assert result["count"] == 1
    assert written["_id"] == "tt14586350"
    assert written["poster"] == "https://images.metahub.space/poster/small/tt14586350/img"
    assert written["state"]["video_id"] == "tt14586350:1:7"
    assert written["state"]["duration"] == 2_940_000
    assert written["state"]["timeOffset"] == 294_000


def test_progress_percent_without_duration_reports_duration_missing() -> None:
    adapter = FakeAdapter([series_record()])
    item = {"type": "episode", "show_ids": {"imdb": "tt0903747"}, "season": 1, "episode": 2, "progress_percent": 10.0}

    result = _progress.add(adapter, [item])

    assert result["count"] == 0
    assert result["unresolved"][0]["reason"] == "stremio_duration_missing"


def test_created_record_uses_stremio_library_shape() -> None:
    record = _common.default_record("tt0137523", "movie", {"title": "Fight Club", "poster": "not-a-url"}, timestamp=1_785_441_200_000)

    assert record["_ctime"] == _common.iso_from_epoch_ms(1_785_441_200_000)
    assert record["_mtime"] == _common.iso_from_epoch_ms(1_785_441_200_000)
    assert record["poster"] == "https://images.metahub.space/poster/small/tt0137523/img"
    assert record["state"]["lastWatched"] == ""
    assert record["state"]["video_id"] == ""
    assert record["state"]["watched"] == ""
    assert record["state"]["noNotif"] is False
    assert record["state"]["season"] == 0
    assert record["state"]["episode"] == 0
    assert record["behaviorHints"] == {"defaultVideoId": None, "featuredVideoId": None, "hasScheduledVideos": False}


def test_history_write_backfills_empty_poster_from_metahub() -> None:
    record = movie_record(poster="")
    adapter = FakeAdapter([record])
    item = {"type": "movie", "ids": {"imdb": "tt0137523"}, "title": "Fight Club"}

    result = _history.add(adapter, [item])
    written = adapter.client.puts[-1][0]

    assert result["count"] == 1
    assert written["poster"] == "https://images.metahub.space/poster/small/tt0137523/img"


def test_history_write_skips_tmdb_for_imdb_only_poster(monkeypatch) -> None:
    def fail_provider(_adapter: Any) -> Any:
        raise AssertionError("tmdb should not be used")

    monkeypatch.setattr(_history, "tmdb_metadata_provider", fail_provider)
    adapter = FakeAdapter([])
    item = {"type": "movie", "ids": {"imdb": "tt0137523"}, "title": "Fight Club"}

    result = _history.add(adapter, [item])
    written = adapter.client.puts[-1][0]

    assert result["count"] == 1
    assert written["poster"] == "https://images.metahub.space/poster/small/tt0137523/img"


def test_watchlist_reads_explicit_library_movies_and_series() -> None:
    movie = movie_record(removed=False, temp=False)
    show = series_record()
    show["removed"] = False
    show["temp"] = False
    adapter = FakeAdapter([movie, show])

    index = _watchlist.build_index(adapter)

    assert set(index) == {"imdb:tt0137523", "imdb:tt0903747"}
    assert index["imdb:tt0903747"]["type"] == "show"


def test_watchlist_excludes_temporary_history_progress_records() -> None:
    movie = movie_record(removed=True, temp=True)
    adapter = FakeAdapter([movie])

    assert _watchlist.build_index(adapter) == {}


def test_watchlist_removed_marker_is_observed_as_absent() -> None:
    movie = movie_record(removed=True, temp=False)
    adapter = FakeAdapter([movie])

    assert _watchlist.build_index(adapter) == {}


def test_watchlist_add_existing_history_record_preserves_history() -> None:
    record = movie_record(removed=True, temp=True)
    adapter = FakeAdapter([record])
    item = {"type": "movie", "ids": {"imdb": "tt0137523"}, "title": "Fight Club"}

    result = _watchlist.add(adapter, [item])
    written = adapter.client.puts[-1][0]

    assert result["count"] == 1
    assert written["removed"] is False
    assert written["temp"] is False
    assert written["state"]["lastWatched"] == 1_785_441_100_000
    assert written["state"]["timesWatched"] == 1
    assert written["state"]["flaggedWatched"] == 1


def test_watchlist_remove_preserves_history_and_progress() -> None:
    record = series_record("tt0903747:1:1:6:eJxTYIACAAEpACE=")
    record["removed"] = False
    record["temp"] = False
    record["state"].update({"timeOffset": 120_000, "duration": 600_000, "video_id": "tt0903747:1:2", "season": 1, "episode": 2})
    adapter = FakeAdapter([record])
    item = {"type": "show", "ids": {"imdb": "tt0903747"}, "title": "Breaking Bad"}

    result = _watchlist.remove(adapter, [item])
    written = adapter.client.puts[-1][0]

    assert result["count"] == 1
    assert written["removed"] is True
    assert written["temp"] is False
    assert isinstance(written["_mtime"], str)
    assert written["_mtime"].endswith("Z")
    assert written["state"]["watched"] == "tt0903747:1:1:6:eJxTYIACAAEpACE="
    assert written["state"]["lastWatched"] == 1_785_441_100_000
    assert written["state"]["timesWatched"] == 7
    assert written["state"]["flaggedWatched"] == 1
    assert written["state"]["timeOffset"] == 120_000
    assert written["state"]["duration"] == 600_000
    assert written["state"]["video_id"] == "tt0903747:1:2"


def test_ratings_write_maps_numeric_rating_to_stremio_reaction(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(_ratings, "STATE_DIR", tmp_path)
    monkeypatch.setenv("CW_PAIR_KEY", "trakt-stremio")
    adapter = FakeLikesAdapter()
    items = [
        {"type": "movie", "ids": {"imdb": "tt0137523"}, "title": "Fight Club", "rating": 7.9},
        {"type": "show", "ids": {"imdb": "tt0903747"}, "title": "Breaking Bad", "rating": 8.0},
    ]

    result = _ratings.add(adapter, items)
    payloads = [row["json"] for row in adapter.client.session.posts]

    assert result["count"] == 2
    assert payloads[0]["status"] == "liked"
    assert payloads[0]["mediaType"] == "movie"
    assert payloads[1]["status"] == "loved"
    assert payloads[1]["mediaType"] == "series"
    assert _ratings.build_index(adapter)["imdb:tt0137523"]["rating"] == 7.9


def test_ratings_write_uses_configured_stremio_thresholds(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(_ratings, "STATE_DIR", tmp_path)
    monkeypatch.setenv("CW_PAIR_KEY", "trakt-stremio")
    adapter = FakeLikesAdapter({"stremio": {"ratings": {"liked_min": 5.0, "loved_min": 9.0}}})
    items = [
        {"type": "movie", "ids": {"imdb": "tt0137523"}, "title": "Fight Club", "rating": 8.5},
        {"type": "movie", "ids": {"imdb": "tt0111161"}, "title": "The Shawshank Redemption", "rating": 9.0},
    ]

    result = _ratings.add(adapter, items)
    payloads = [row["json"] for row in adapter.client.session.posts]

    assert result["count"] == 2
    assert payloads[0]["status"] == "liked"
    assert payloads[1]["status"] == "loved"


def test_ratings_write_skips_values_below_stremio_threshold(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(_ratings, "STATE_DIR", tmp_path)
    monkeypatch.setenv("CW_PAIR_KEY", "simkl-stremio")
    adapter = FakeLikesAdapter()

    result = _ratings.add(adapter, [{"type": "movie", "ids": {"imdb": "tt0137523"}, "title": "Fight Club", "rating": 5.9}])

    assert result["count"] == 0
    assert result["skipped"] == 1
    assert result["unresolved"] == []
    assert adapter.client.session.posts == []
    cached = _ratings.build_index(adapter)["imdb:tt0137523"]
    assert cached["rating"] == 5.9
    assert cached["_stremio_reaction"] is None


def test_ratings_write_updates_below_threshold_cache_when_rating_becomes_liked(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(_ratings, "STATE_DIR", tmp_path)
    monkeypatch.setenv("CW_PAIR_KEY", "simkl-stremio")
    adapter = FakeLikesAdapter()

    _ratings.add(adapter, [{"type": "movie", "ids": {"imdb": "tt0137523"}, "title": "Fight Club", "rating": 5}])
    result = _ratings.add(adapter, [{"type": "movie", "ids": {"imdb": "tt0137523"}, "title": "Fight Club", "rating": 6}])

    assert result["count"] == 1
    assert adapter.client.session.posts[-1]["json"]["status"] == "liked"
    assert _ratings.build_index(adapter)["imdb:tt0137523"]["_stremio_reaction"] == "liked"


def test_ratings_write_skips_when_remote_reaction_already_matches_after_cache_clear(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(_ratings, "STATE_DIR", tmp_path)
    monkeypatch.setenv("CW_PAIR_KEY", "simkl-stremio")
    adapter = FakeLikesAdapter()
    adapter.client.session.remote[("tt0137523", "movie")] = "liked"

    result = _ratings.add(adapter, [{"type": "movie", "ids": {"imdb": "tt0137523"}, "title": "Fight Club", "rating": 7}])

    assert result["count"] == 0
    assert result["skipped"] == 1
    assert adapter.client.session.posts == []
    assert _ratings.build_index(adapter)["imdb:tt0137523"]["_stremio_reaction"] == "liked"


def test_ratings_below_threshold_cache_preserves_source_keyspace(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(_ratings, "STATE_DIR", tmp_path)
    monkeypatch.setenv("CW_PAIR_KEY", "simkl-stremio")
    adapter = FakeLikesAdapter()

    _ratings.add(adapter, [{"type": "movie", "ids": {"tmdb": "1111873", "imdb": "tt27489557"}, "title": "Abigail", "rating": 2}])

    assert list(_ratings.build_index(adapter)) == ["tmdb:1111873"]


def test_ratings_cache_is_ignored_in_capture_mode(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(_ratings, "STATE_DIR", tmp_path)
    monkeypatch.setenv("CW_PAIR_KEY", "simkl-stremio")
    adapter = FakeLikesAdapter()

    _ratings.add(adapter, [{"type": "movie", "ids": {"imdb": "tt0137523"}, "title": "Fight Club", "rating": 8}])
    assert _ratings.build_index(adapter)

    monkeypatch.setenv("CW_CAPTURE_MODE", "1")
    assert _ratings.build_index(adapter) == {}


def test_ratings_remove_clears_stremio_reaction(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(_ratings, "STATE_DIR", tmp_path)
    monkeypatch.setenv("CW_PAIR_KEY", "trakt-stremio")
    adapter = FakeLikesAdapter()
    item = {"type": "movie", "ids": {"imdb": "tt0137523"}, "title": "Fight Club", "rating": 8}

    _ratings.add(adapter, [item])
    result = _ratings.remove(adapter, [item])

    assert result["count"] == 1
    assert adapter.client.session.posts[-1]["json"]["status"] is None
    assert _ratings.build_index(adapter) == {}


def test_ratings_write_requires_stremio_imdb_id(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(_ratings, "STATE_DIR", tmp_path)
    monkeypatch.setenv("CW_PAIR_KEY", "tmdb-stremio")
    adapter = FakeLikesAdapter()

    result = _ratings.add(adapter, [{"type": "movie", "ids": {"tmdb": "550"}, "title": "Fight Club", "rating": 8.5}])

    assert result["count"] == 0
    assert result["unresolved"][0]["reason"] == "stremio_rating_id_missing"
    assert adapter.client.session.posts == []


def test_capability_set_advertises_history_progress_and_watchlist() -> None:
    caps = mod.OPS.capabilities()

    assert mod.OPS.features() == {"watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": False}
    assert caps["history"]["read"] is True
    assert caps["history"]["write"] is True
    assert caps["progress"]["read"] is True
    assert caps["progress"]["write"] is True
    assert caps["watchlist"]["read"] is True
    assert caps["watchlist"]["write"] is True
    assert caps["watchlist"]["custom_lists"] is False
    assert caps["ratings"]["read"] is False
    assert caps["ratings"]["write"] is True
    assert caps["ratings"]["direction"] == "destination_only"
    assert caps["ratings"]["thresholds"] == {"liked_min": 6.0, "loved_min": 8.0}
    assert caps["ratings"]["accepted_ids"] == ["imdb"]
    assert caps["progress"]["index_semantics"] == "present"
    assert caps["progress"]["requires_duration"] is True
    assert caps["progress"]["completion_policy"]["progress_write"] == {"mode": "none"}
    assert caps.get("multi_profile") is not True
    assert "stremio_profile_id" not in caps
    assert "server_completion_percent" not in caps["progress"]


def test_incremental_cache_is_stremio_profile_scoped() -> None:
    adapter = FakeAdapter([movie_record()])

    _progress.build_index(adapter)

    assert getattr(adapter, "_stremio_mtime_cache_default") == {"tt0137523": 1_785_441_200_000}
    assert not hasattr(adapter, "_stremio_mtime_cache")


def test_incremental_cache_accepts_stremio_iso_mtime() -> None:
    first = movie_record(_mtime="2026-07-30T18:33:20Z")
    adapter = FakeAdapter([first])

    _common.library_records(adapter)
    adapter.client.records["tt0137523"]["_mtime"] = "2026-07-30T18:34:20Z"
    rows = _common.library_records(adapter, incremental=True)

    assert [row["_id"] for row in rows] == ["tt0137523"]
    assert getattr(adapter, "_stremio_mtime_cache_default") == {"tt0137523": _common.epoch_ms("2026-07-30T18:34:20Z")}


def test_library_records_capture_mode_forces_full_read(monkeypatch) -> None:
    adapter = FakeAdapter([movie_record(), movie_record(_id="tt0903747", _mtime=1)])

    _common.library_records(adapter)
    monkeypatch.setenv("CW_CAPTURE_MODE", "1")
    rows = _common.library_records(adapter, incremental=True)

    assert {row["_id"] for row in rows} == {"tt0137523", "tt0903747"}


def test_health_reports_stremio_api_error_detail(monkeypatch) -> None:
    def fail(_adapter):
        raise mod.StremioAuthError("bad", reason="request_failed", detail={"message": "bad authKey"}, status_code=200, endpoint="datastoreMeta")

    monkeypatch.setattr(mod, "datastore_meta", fail)

    health = mod.STREMIOModule({"stremio": {"auth_key": "key"}}).health()

    assert health["status"] == "request_failed"
    assert health["details"] == {"reason": "request_failed"}
    assert health["api"]["datastoreMeta"]["status"] == 200
    assert health["api"]["datastoreMeta"]["endpoint"] == "datastoreMeta"
    assert "authKey" not in health["api"]["datastoreMeta"]["detail"]
