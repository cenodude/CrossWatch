from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from providers.sync import _mod_KODI as mod
from providers.sync.kodi import _common as common
from providers.auth._auth_KODI import KodiAuthError


class FakeKodiClient:
    def __init__(self, movies=None, episodes=None, tvshows=None):
        self.movies = list(movies or [])
        self.episodes = list(episodes or [])
        self.tvshows = list(tvshows or [])
        self.movie_writes = []
        self.episode_writes = []

    def get_movies(self, *_args, **_kwargs):
        return self.movies

    def get_episodes(self, *_args, **_kwargs):
        return self.episodes

    def get_tvshows(self, *_args, **_kwargs):
        return self.tvshows

    def set_movie(self, movieid: int, payload: dict[str, Any]):
        self.movie_writes.append((movieid, payload))
        return "OK"

    def set_episode(self, episodeid: int, payload: dict[str, Any]):
        self.episode_writes.append((episodeid, payload))
        return "OK"


def adapter(client: FakeKodiClient):
    return SimpleNamespace(client=client, _kodi_library_index=None)


def movie(**extra):
    row = {
        "movieid": 10,
        "title": "Arrival",
        "year": 2016,
        "uniqueid": {"imdb": "tt2543164", "tmdb": "329865"},
        "playcount": 1,
        "lastplayed": "2026-01-02 03:04:05",
        "userrating": 8,
        "resume": {"position": 120.5, "total": 6000.0},
    }
    row.update(extra)
    return row


def episode(**extra):
    row = {
        "episodeid": 20,
        "tvshowid": 5,
        "title": "The Target",
        "showtitle": "The Expanse",
        "year": 2015,
        "season": 1,
        "episode": 1,
        "uniqueid": {"tvdb": "5534478"},
        "playcount": 1,
        "lastplayed": "2026-02-03 04:05:06",
        "userrating": 9,
        "resume": {"position": 300.0, "total": 2700.0},
    }
    row.update(extra)
    return row


def tvshow(**extra):
    row = {"tvshowid": 5, "title": "The Expanse", "year": 2015, "uniqueid": {"tmdb": "63639", "tvdb": "280619"}}
    row.update(extra)
    return row


def test_kodi_manifest_capabilities_are_scoped():
    manifest = mod.get_manifest()

    assert manifest["features"] == {"watchlist": False, "ratings": True, "history": True, "progress": True, "playlists": False}
    assert manifest["capabilities"]["history"]["read"] is True
    assert manifest["capabilities"]["ratings"]["write"] is True
    assert manifest["capabilities"]["progress"]["types"]["episodes"] is True


def test_kodi_sync_properties_match_jsonrpc_v13_5_contract():
    movie_allowed = {"title", "year", "uniqueid", "playcount", "lastplayed", "userrating", "resume"}
    episode_allowed = {"title", "showtitle", "season", "episode", "tvshowid", "uniqueid", "playcount", "lastplayed", "userrating", "resume"}
    show_allowed = {"title", "year", "uniqueid"}

    for feature in ("history", "ratings", "progress", ""):
        movie_props, episode_props, show_props = common.properties_for_feature(feature)
        assert set(movie_props) <= movie_allowed
        assert set(episode_props) <= episode_allowed
        assert set(show_props) <= show_allowed
        assert "year" not in episode_props


def test_history_index_reads_movies_and_episodes():
    ad = adapter(FakeKodiClient([movie()], [episode()], [tvshow()]))

    index = mod.feat_history.build_index(ad)

    assert "tmdb:329865" in index
    assert index["tmdb:329865"]["watched"] is True
    assert index["tmdb:329865"]["watched_at"] == "2026-01-02T03:04:05Z"
    ep = index["tmdb:63639#s01e01"]
    assert ep["type"] == "episode"
    assert ep["title"] == "The Target"
    assert ep["series_title"] == "The Expanse"
    assert ep["show_ids"]["tmdb"] == "63639"
    assert ep["season"] == 1
    assert ep["episode"] == 1


def test_history_index_requests_only_history_properties():
    class RecordingClient(FakeKodiClient):
        def __init__(self):
            super().__init__([movie()], [episode()], [tvshow()])
            self.calls: list[tuple[str, list[str]]] = []

        def get_movies(self, properties=None):
            self.calls.append(("movies", list(properties or [])))
            return self.movies

        def get_episodes(self, properties=None):
            self.calls.append(("episodes", list(properties or [])))
            return self.episodes

        def get_tvshows(self, properties=None):
            self.calls.append(("tvshows", list(properties or [])))
            return self.tvshows

    client = RecordingClient()

    mod.feat_history.build_index(adapter(client))

    props = {name: values for name, values in client.calls}
    assert "playcount" in props["movies"]
    assert "lastplayed" in props["episodes"]
    assert "year" not in props["episodes"]
    assert "userrating" not in props["movies"]
    assert "resume" not in props["episodes"]


def test_kodi_library_rows_falls_back_after_batch_error(monkeypatch):
    calls: list[tuple[str, dict[str, Any]]] = []

    def batch_fail(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise KodiAuthError("Kodi JSON-RPC error for 0:VideoLibrary.GetMovies: Invalid params. (-32602)", reason="jsonrpc_error")

    def rpc_ok(_server: str, method: str, *, params=None, **_kwargs: Any) -> dict[str, Any]:
        calls.append((method, dict(params or {})))
        if method == "VideoLibrary.GetMovies":
            return {"movies": [movie()]}
        if method == "VideoLibrary.GetEpisodes":
            return {"episodes": [episode()]}
        if method == "VideoLibrary.GetTVShows":
            return {"tvshows": [tvshow()]}
        return {}

    monkeypatch.setattr(common, "jsonrpc_batch_call", batch_fail)
    monkeypatch.setattr(common, "jsonrpc_call", rpc_ok)

    client = common.KodiClient(common.KodiConfig(server="http://kodi.local", connection_verified=True))
    movies, episodes, tvshows = client.library_rows("history")

    assert len(movies) == 1
    assert len(episodes) == 1
    assert len(tvshows) == 1
    assert [call[0] for call in calls] == ["VideoLibrary.GetMovies", "VideoLibrary.GetEpisodes", "VideoLibrary.GetTVShows"]
    assert "userrating" not in calls[0][1]["properties"]
    assert "resume" not in calls[1][1]["properties"]


def test_kodi_library_rows_uses_v13_5_path_filters_without_extra_properties(monkeypatch):
    seen: list[tuple[str, dict[str, Any]]] = []

    def batch_ok(_server: str, calls, **_kwargs: Any) -> dict[str, Any]:
        for method, params in calls:
            seen.append((method, dict(params or {})))
        return {"0:VideoLibrary.GetMovies": {"movies": []}, "1:VideoLibrary.GetEpisodes": {"episodes": []}, "2:VideoLibrary.GetTVShows": {"tvshows": []}}

    monkeypatch.setattr(common, "jsonrpc_batch_call", batch_ok)

    client = common.KodiClient(common.KodiConfig(server="http://kodi.local", connection_verified=True))
    filt = common.path_filter_for(["/media/movies"], ["/media/movies/private"])
    client.library_rows("history", path_filter=filt)

    assert [call[0] for call in seen] == ["VideoLibrary.GetMovies", "VideoLibrary.GetEpisodes", "VideoLibrary.GetTVShows"]
    for _, params in seen:
        assert params["filter"]["and"][0] == {"field": "path", "operator": "startswith", "value": "/media/movies"}
        assert params["filter"]["and"][1] == {"field": "path", "operator": "doesnotcontain", "value": "/media/movies/private"}
        assert "file" not in params["properties"]
        assert "path" not in params["properties"]


def test_kodi_sync_logs_library_scope_filter_when_configured(monkeypatch):
    logs: list[tuple[str, str, str, dict[str, Any]]] = []
    monkeypatch.setattr(common, "log", lambda feature, level, event, **fields: logs.append((feature, level, event, fields)))
    ad = SimpleNamespace(
        client=FakeKodiClient([movie()], [], []),
        config={"kodi": {"history": {"libraries": ["/media/movies"]}}},
        instance_id="default",
    )

    common.library_index(ad, "history")

    assert logs
    feature, level, event, fields = logs[-1]
    assert (feature, level, event) == ("history", "debug", "index_fetch_counts")
    assert fields["source"] == "selected_libraries"
    assert fields["count"] == 1
    assert fields["allowed_library_paths"] == ["/media/movies"]
    assert fields["movies"] == 1


def test_ratings_and_progress_indexes_normalize_values():
    ad = adapter(FakeKodiClient([movie(userrating=0), movie(movieid=11, userrating=7)], [episode()], [tvshow()]))

    ratings = mod.feat_ratings.build_index(ad)
    progress = mod.feat_progress.build_index(ad)

    assert "tmdb:329865" in ratings
    assert ratings["tmdb:329865"]["rating"] == 7
    assert progress["tmdb:329865"]["progress_ms"] == 120500
    assert progress["tmdb:329865"]["duration_ms"] == 6000000
    assert int(progress["tmdb:63639#s01e01"]["progress_percent"]) == 11


def test_kodi_progress_assigns_managed_timestamp_for_new_resume(monkeypatch):
    monkeypatch.setattr(common, "_utc_now_iso", lambda: "2026-07-27T21:00:00Z")
    ad = adapter(FakeKodiClient([movie()], [], []))
    ad._kodi_progress_baseline = {}

    progress = mod.feat_progress.build_index(ad)

    assert progress["tmdb:329865"]["progress_at"] == "2026-07-27T21:00:00Z"
    assert progress["tmdb:329865"]["progress_at_source"] == "kodi_first_observed"


def test_kodi_progress_reuses_timestamp_until_resume_changes(monkeypatch):
    monkeypatch.setattr(common, "_utc_now_iso", lambda: "2026-07-29T21:00:00Z")
    old = {
        "type": "movie",
        "ids": {"tmdb": "329865"},
        "title": "Arrival",
        "year": 2016,
        "progress_ms": 120500,
        "duration_ms": 6000000,
        "progress_percent": 2.008,
        "progress_at": "2026-07-27T21:00:00Z",
        "progress_at_source": "kodi_first_observed",
    }
    ad = adapter(FakeKodiClient([movie()], [], []))
    ad._kodi_progress_baseline = {"tmdb:329865": old}

    unchanged = mod.feat_progress.build_index(ad)
    assert unchanged["tmdb:329865"]["progress_at"] == "2026-07-27T21:00:00Z"
    assert unchanged["tmdb:329865"]["progress_at_source"] == "kodi_first_observed"

    ad2 = adapter(FakeKodiClient([movie(resume={"position": 240.0, "total": 6000.0})], [], []))
    ad2._kodi_progress_baseline = {"tmdb:329865": old}
    changed = mod.feat_progress.build_index(ad2)

    assert changed["tmdb:329865"]["progress_ms"] == 240000
    assert changed["tmdb:329865"]["progress_at"] == "2026-07-29T21:00:00Z"
    assert changed["tmdb:329865"]["progress_at_source"] == "kodi_resume_changed"


def test_writes_history_ratings_and_progress_to_resolved_library_items():
    client = FakeKodiClient([movie()], [episode()], [tvshow()])
    cfg = {"kodi": {"server": "http://localhost:8080", "connection_verified": True}}
    module = mod.KODIModule(cfg)
    module.client = client

    source_movie = {"type": "movie", "ids": {"tmdb": "329865"}, "watched_at": "2026-06-01T12:00:00Z", "rating": 6, "progress_ms": 90000, "duration_ms": 6000000}
    source_ep = {"type": "episode", "show_ids": {"tmdb": "63639"}, "season": 1, "episode": 1, "rating": 10, "progress_ms": 1000}

    assert module.add("history", [source_movie])["count"] == 1
    assert module.add("ratings", [source_ep])["count"] == 1
    assert module.add("progress", [source_movie])["count"] == 1

    assert client.movie_writes[0] == (10, {"playcount": 1, "lastplayed": "2026-06-01 12:00:00"})
    assert client.episode_writes[0] == (20, {"userrating": 10})
    assert client.movie_writes[1] == (10, {"resume": {"position": 90.0, "total": 6000.0}})


def test_removes_clear_kodi_fields():
    client = FakeKodiClient([movie()], [episode()], [tvshow()])
    module = mod.KODIModule({"kodi": {"server": "http://localhost:8080", "connection_verified": True}})
    module.client = client

    source_movie = {"type": "movie", "ids": {"tmdb": "329865"}}

    assert module.remove("history", [source_movie])["count"] == 1
    assert module.remove("ratings", [source_movie])["count"] == 1
    assert module.remove("progress", [source_movie])["count"] == 1

    assert client.movie_writes[0] == (10, {"playcount": 0, "lastplayed": ""})
    assert client.movie_writes[1] == (10, {"userrating": 0})
    assert client.movie_writes[2] == (10, {"resume": {"position": 0.0, "total": 6000.0}})


def test_ambiguous_library_resolution_is_unresolved():
    client = FakeKodiClient(
        [
            movie(movieid=1, uniqueid={}, title="Twin", year=2020),
            movie(movieid=2, uniqueid={}, title="Twin", year=2020),
        ],
        [],
        [],
    )
    module = mod.KODIModule({"kodi": {"server": "http://localhost:8080", "connection_verified": True}})
    module.client = client

    result = module.add("ratings", [{"type": "movie", "title": "Twin", "year": 2020, "rating": 8}])

    assert result["count"] == 0
    assert result["reason_counts"]["ambiguous"] == 1
    assert client.movie_writes == []


def test_external_id_source_does_not_fallback_to_title_year():
    client = FakeKodiClient([movie(uniqueid={}, title="Arrival", year=2016)], [], [])
    module = mod.KODIModule({"kodi": {"server": "http://localhost:8080", "connection_verified": True}})
    module.client = client

    result = module.add("ratings", [{"type": "movie", "ids": {"tmdb": "329865"}, "title": "Arrival", "year": 2016, "rating": 8}])

    assert result["count"] == 0
    assert result["reason_counts"]["not_found"] == 1
    assert client.movie_writes == []
