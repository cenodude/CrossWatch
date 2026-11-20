from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest


class _Resp:
    def __init__(self, status: int = 200, payload: Any = None) -> None:
        self.status_code = status
        self._payload = payload if payload is not None else {}
        self.text = json.dumps(self._payload)
        self.headers = {"Content-Type": "application/json"}

    def json(self) -> Any:
        return self._payload


class _Session:
    def __init__(
        self,
        get_payload: Any = None,
        post_payload: Any = None,
        get_by_url: dict[str, Any] | None = None,
        get_status: int = 200,
    ) -> None:
        self.get_payload = get_payload if get_payload is not None else {}
        self.post_payload = post_payload if post_payload is not None else {"added": {"movies": 1}}
        self.get_by_url = get_by_url or {}
        self.get_status = get_status
        self.gets: list[str] = []
        self.posts: list[dict[str, Any]] = []

    def get(self, url: str, **_kwargs: Any) -> _Resp:
        self.gets.append(url)
        return _Resp(self.get_status, self.get_by_url.get(url, self.get_payload))

    def post(self, url: str, **kwargs: Any) -> _Resp:
        self.posts.append({"url": url, "json": kwargs.get("json")})
        return _Resp(201, self.post_payload)


def _adapter(session: _Session) -> Any:
    return SimpleNamespace(
        client=SimpleNamespace(session=session),
        cfg=SimpleNamespace(timeout=5, watchlist_batch_size=100),
    )


def _movie() -> dict[str, Any]:
    return {
        "type": "movie",
        "title": "A Cure for Wellness",
        "year": 2016,
        "ids": {"imdb": "tt4731136", "tmdb": "340837"},
    }


def _movie_imdb_only() -> dict[str, Any]:
    movie = _movie()
    movie["ids"] = {"imdb": "tt4731136"}
    return movie


@pytest.fixture(autouse=True)
def simkl_watchlist_state(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    from providers.sync.simkl import _watchlist as m

    store: dict[str, str] = {}
    monkeypatch.setattr(m, "state_file", lambda name: name)
    monkeypatch.setattr(m, "load_json_state", lambda path: json.loads(store.get(str(path)) or "{}"))
    monkeypatch.setattr(m, "save_json_state", lambda path, data: store.__setitem__(str(path), json.dumps(data)))
    monkeypatch.setattr(m, "_headers", lambda *a, **k: {})
    monkeypatch.setattr(m, "simkl_api_params_from_headers", lambda headers=None, **k: dict(k))
    monkeypatch.setattr(m, "fetch_activities", lambda *a, **k: ({}, None))
    monkeypatch.setattr(m, "get_watermark", lambda *a, **k: None)
    monkeypatch.setattr(m, "save_watermark", lambda *a, **k: None)
    monkeypatch.setattr(m, "normalize_flat_watermarks", lambda: None)
    m._STATUS_HISTORY_COVERED = {}
    return store


def _history_cache_doc(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema": 4,
        "generated_at": "2026-08-30T00:00:00Z",
        "rewatches": False,
        "items": {
            "tmdb:340837@1788085800": {
                "type": "movie",
                "watched": True,
                "watched_at": "2026-07-18T17:58:00Z",
                "title": item["title"],
                "year": item["year"],
                "ids": dict(item["ids"]),
                "simkl_bucket": "movies",
            }
        },
    }


def test_add_skips_movie_already_covered_by_simkl_history(simkl_watchlist_state: dict[str, str]) -> None:
    from providers.sync.simkl import _watchlist as m

    movie = _movie()
    simkl_watchlist_state["simkl.history.cache.json"] = json.dumps(_history_cache_doc(movie))
    session = _Session()

    result = m.add(_adapter(session), [movie])

    assert result["ok"] is True
    assert result["count"] == 0
    assert result["unresolved"] == []
    assert result["skipped_keys"] == [m.simkl_key_of(m.id_minimal(movie))]
    assert result["presence_confirmed_keys"] == []
    assert session.posts == []


def test_history_cache_does_not_make_simkl_watchlist_read_export_virtual_items(
    simkl_watchlist_state: dict[str, str],
) -> None:
    from providers.sync.simkl import _watchlist as m

    history_movie = _movie()
    simkl_watchlist_state["simkl.history.cache.json"] = json.dumps(_history_cache_doc(history_movie))

    index = m.build_index(_adapter(_Session(get_payload={})))

    assert index == {}


def test_completed_all_items_row_is_not_exported_as_simkl_watchlist() -> None:
    from providers.sync.simkl import _watchlist as m

    movie = _movie()
    session = _Session(
        get_payload={
            "movies": [
                {
                    "status": "completed",
                    "movie": {
                        "title": movie["title"],
                        "year": movie["year"],
                        "ids": dict(movie["ids"]),
                    },
                }
            ],
            "shows": [],
            "anime": [],
        }
    )

    index = m.build_index(_adapter(session))

    assert index == {}


def test_completed_all_items_row_still_prevents_plantowatch_write() -> None:
    from providers.sync.simkl import _watchlist as m

    history_movie = _movie()
    source_movie = _movie_imdb_only()
    session = _Session(
        get_payload={
            "movies": [
                {
                    "status": "completed",
                    "movie": {
                        "title": history_movie["title"],
                        "year": history_movie["year"],
                        "ids": dict(history_movie["ids"]),
                    },
                }
            ],
            "shows": [],
            "anime": [],
        }
    )
    assert m.build_index(_adapter(session)) == {}

    result = m.add(_adapter(session), [source_movie])

    assert result["ok"] is True
    assert result["skipped_keys"] == [m.simkl_key_of(m.id_minimal(source_movie))]
    assert result["presence_confirmed_keys"] == []
    assert session.posts == []


def test_completed_status_cover_survives_restart_without_virtual_export(
    simkl_watchlist_state: dict[str, str],
) -> None:
    from providers.sync.simkl import _watchlist as m

    history_movie = _movie()
    source_movie = _movie_imdb_only()
    session = _Session(
        get_payload={
            "movies": [
                {
                    "status": "completed",
                    "movie": {
                        "title": history_movie["title"],
                        "year": history_movie["year"],
                        "ids": dict(history_movie["ids"]),
                    },
                }
            ],
            "shows": [],
            "anime": [],
        }
    )

    assert m.build_index(_adapter(session)) == {}
    assert "simkl.watchlist.history_cover.json" in simkl_watchlist_state

    m._STATUS_HISTORY_COVERED = {}
    result = m.add(_adapter(session), [source_movie])

    assert result["ok"] is True
    assert result["skipped_keys"] == [m.simkl_key_of(m.id_minimal(source_movie))]
    assert result["presence_confirmed_keys"] == []
    assert session.posts == []


def test_add_unwatched_movie_still_posts_to_plantowatch() -> None:
    from providers.sync.simkl import _watchlist as m

    movie = _movie()
    session = _Session()

    result = m.add(_adapter(session), [movie])

    assert result["ok"] is True
    assert result["confirmed_keys"] == [m.simkl_key_of(m.id_minimal(movie))]
    assert result["skipped_keys"] == []
    assert result["presence_confirmed_keys"] == [m.simkl_key_of(m.id_minimal(movie))]
    assert len(session.posts) == 1
    assert session.posts[0]["url"] == "https://api.simkl.com/sync/add-to-list"
    assert session.posts[0]["json"] == {
        "movies": [{"ids": {"tmdb": "340837", "imdb": "tt4731136"}, "to": "plantowatch"}]
    }


_PTW_MOVIES = "https://api.simkl.com/sync/all-items/movies/plantowatch"
_PTW_SHOWS = "https://api.simkl.com/sync/all-items/shows/plantowatch"
_PTW_ANIME = "https://api.simkl.com/sync/all-items/anime/plantowatch"
_HISTORY_REMOVE = "https://api.simkl.com/sync/history/remove"


def _ptw_movie_row(item: dict[str, Any]) -> dict[str, Any]:
    return {"movie": {"ids": dict(item["ids"])}}


def test_remove_skips_movie_no_longer_on_simkl_plantowatch() -> None:
    from providers.sync.simkl import _watchlist as m

    movie = _movie()
    session = _Session(get_by_url={_PTW_MOVIES: {"movies": []}, _PTW_ANIME: {"anime": []}})

    ok, unresolved = m.remove(_adapter(session), [movie])

    assert ok == 1
    assert unresolved == []
    assert session.posts == []
    assert _PTW_MOVIES in session.gets
    assert _PTW_SHOWS not in session.gets


def test_remove_deletes_movie_still_on_simkl_plantowatch() -> None:
    from providers.sync.simkl import _watchlist as m

    movie = _movie()
    session = _Session(
        get_by_url={_PTW_MOVIES: {"movies": [_ptw_movie_row(movie)]}, _PTW_ANIME: {"anime": []}},
        post_payload={"deleted": {"movies": 1, "shows": 0}},
    )

    ok, unresolved = m.remove(_adapter(session), [movie])

    assert ok == 1
    assert unresolved == []
    assert [p["url"] for p in session.posts] == [_HISTORY_REMOVE]
    assert session.posts[0]["json"] == {"movies": [{"ids": {"tmdb": "340837", "imdb": "tt4731136"}}]}


def test_remove_only_deletes_items_still_on_plantowatch() -> None:
    from providers.sync.simkl import _watchlist as m

    watched = _movie()
    planned = {"type": "movie", "title": "Planned", "year": 2020, "ids": {"tmdb": "555"}}
    session = _Session(
        get_by_url={_PTW_MOVIES: {"movies": [_ptw_movie_row(planned)]}, _PTW_ANIME: {"anime": []}},
        post_payload={"deleted": {"movies": 1, "shows": 0}},
    )

    ok, unresolved = m.remove(_adapter(session), [watched, planned])

    assert ok == 2
    assert unresolved == []
    assert len(session.posts) == 1
    assert session.posts[0]["json"] == {"movies": [{"ids": {"tmdb": "555"}}]}


def test_remove_does_not_match_show_with_same_tmdb_id() -> None:
    from providers.sync.simkl import _watchlist as m

    movie = _movie()
    session = _Session(
        get_by_url={
            _PTW_MOVIES: {"movies": []},
            _PTW_ANIME: {"anime": [{"show": {"ids": {"tmdb": "340837"}}}]},
        }
    )

    ok, unresolved = m.remove(_adapter(session), [movie])

    assert ok == 1
    assert unresolved == []
    assert session.posts == []


def test_remove_fails_closed_when_live_check_fails() -> None:
    from providers.sync.simkl import _watchlist as m

    movie = _movie()
    session = _Session(get_status=500)

    ok, unresolved = m.remove(_adapter(session), [movie])

    assert ok == 0
    assert session.posts == []
    assert [u["hint"] for u in unresolved] == ["plantowatch_check_failed"]


def test_remove_drops_skipped_item_from_shadow(simkl_watchlist_state: dict[str, str]) -> None:
    from providers.sync.simkl import _watchlist as m

    movie = _movie()
    m._shadow_add_items([movie])
    assert m._shadow_load()["items"]
    session = _Session(get_by_url={_PTW_MOVIES: {"movies": []}, _PTW_ANIME: {"anime": []}})

    m.remove(_adapter(session), [movie])

    assert m._shadow_load()["items"] == {}


def test_status_activity_refreshes_cached_simkl_watchlist(
    simkl_watchlist_state: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from providers.sync.simkl import _watchlist as m

    movie = _movie()
    key, shadow_item = m._mk_shadow_item(movie)
    m._shadow_save("old", {key: shadow_item}, {"movies": True, "shows": True, "anime": True})
    acts = {
        "movies": {
            "all": "2026-11-20T19:48:24Z",
            "plantowatch": "2026-11-20T19:47:59Z",
            "completed": "2026-11-20T19:48:24Z",
            "removed_from_list": "2026-11-19T13:07:45Z",
        }
    }
    marks = {
        "watchlist": "2026-11-20T19:47:59Z",
        "watchlist_removed": "2026-11-19T13:07:45Z",
        "watchlist_status": "2026-11-19T13:07:45Z",
    }
    saved: dict[str, str] = {}
    monkeypatch.setattr(m, "fetch_activities", lambda *a, **k: (acts, None))
    monkeypatch.setattr(m, "get_watermark", lambda name: marks.get(name))
    monkeypatch.setattr(m, "save_watermark", lambda name, value: saved.__setitem__(name, value))
    session = _Session(get_by_url={_PTW_MOVIES: {"movies": []}})

    index = m.build_index(_adapter(session))

    assert index == {}
    assert _PTW_MOVIES in session.gets
    assert saved["watchlist_status"] == "2026-11-20T19:48:24Z"


def test_unchanged_status_activity_keeps_cached_simkl_watchlist(
    simkl_watchlist_state: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from providers.sync.simkl import _watchlist as m

    movie = _movie()
    key, shadow_item = m._mk_shadow_item(movie)
    m._shadow_save("old", {key: shadow_item}, {"movies": True, "shows": True, "anime": True})
    acts = {
        "movies": {
            "plantowatch": "2026-11-20T19:47:59Z",
            "completed": "2026-11-19T13:07:45Z",
            "removed_from_list": "2026-11-19T13:07:45Z",
        }
    }
    marks = {
        "watchlist": "2026-11-20T19:47:59Z",
        "watchlist_removed": "2026-11-19T13:07:45Z",
        "watchlist_status": "2026-11-19T13:07:45Z",
    }
    monkeypatch.setattr(m, "fetch_activities", lambda *a, **k: (acts, None))
    monkeypatch.setattr(m, "get_watermark", lambda name: marks.get(name))
    session = _Session(get_by_url={_PTW_MOVIES: {"movies": []}})

    index = m.build_index(_adapter(session))

    assert list(index) == [key]
    assert session.gets == []


def _trakt_show() -> dict[str, Any]:
    return {"type": "show", "title": "Joe Pera Talks with You", "year": 2018, "ids": {"imdb": "tt8199790", "tmdb": 79864, "tvdb": 347590, "trakt": 130748}}


def _trakt_anime_movie() -> dict[str, Any]:
    return {"type": "movie", "title": "Patema Inverted", "year": 2013, "ids": {"imdb": "tt2981768", "tmdb": 212167, "trakt": 131230}}


def _anime_ptw_row(simkl_id: int, tmdb: str, imdb: str, anime_type: str) -> dict[str, Any]:
    return {
        "status": "plantowatch",
        "show": {"title": "Anime", "ids": {"simkl": simkl_id, "tmdb": tmdb, "imdb": imdb}},
        "anime_type": anime_type,
    }


def test_remove_skips_show_that_moved_to_watching() -> None:
    from providers.sync.simkl import _watchlist as m

    session = _Session(get_by_url={_PTW_SHOWS: {"shows": []}, _PTW_ANIME: {"anime": []}})

    ok, unresolved = m.remove(_adapter(session), [_trakt_show()])

    assert ok == 1
    assert unresolved == []
    assert session.posts == []
    assert _PTW_MOVIES not in session.gets


def test_remove_deletes_anime_movie_still_on_simkl_anime_plantowatch() -> None:
    from providers.sync.simkl import _watchlist as m

    session = _Session(
        get_by_url={
            _PTW_MOVIES: {"movies": []},
            _PTW_ANIME: {"anime": [_anime_ptw_row(39322, "212167", "tt2981768", "movie")]},
        },
        post_payload={"deleted": {"movies": 1, "shows": 0}},
    )

    ok, unresolved = m.remove(_adapter(session), [_trakt_anime_movie()])

    assert ok == 1
    assert unresolved == []
    assert [p["url"] for p in session.posts] == [_HISTORY_REMOVE]


def test_remove_skips_watched_anime_movie_and_anime_show() -> None:
    from providers.sync.simkl import _watchlist as m

    anime_show = {"type": "show", "title": "Flying Witch", "year": 2016, "ids": {"imdb": "tt5621006", "tmdb": 65934}}
    session = _Session(get_by_url={_PTW_MOVIES: {"movies": []}, _PTW_SHOWS: {"shows": []}, _PTW_ANIME: {"anime": []}})

    ok, unresolved = m.remove(_adapter(session), [_trakt_anime_movie(), anime_show])

    assert ok == 2
    assert unresolved == []
    assert session.posts == []


def test_status_refresh_keeps_titles_of_items_still_on_watchlist(
    simkl_watchlist_state: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from providers.sync.simkl import _watchlist as m

    kept = {"type": "movie", "title": "Street Fighter", "year": 2026, "ids": {"simkl": 2212913, "tmdb": "1153576"}}
    gone = _movie()
    kept_key, kept_row = m._mk_shadow_item(kept)
    gone_key, gone_row = m._mk_shadow_item(gone)
    m._shadow_save("old", {kept_key: kept_row, gone_key: gone_row}, {"movies": True, "shows": True, "anime": True})
    acts = {"movies": {"plantowatch": "2026-11-20T19:47:59Z", "completed": "2026-11-20T19:48:24Z"}}
    marks = {
        "watchlist": "2026-11-20T19:47:59Z",
        "watchlist_removed": "2026-11-19T13:07:45Z",
        "watchlist_status": "2026-11-19T13:07:45Z",
    }
    monkeypatch.setattr(m, "fetch_activities", lambda *a, **k: (acts, None))
    monkeypatch.setattr(m, "get_watermark", lambda name: marks.get(name))
    session = _Session(get_by_url={_PTW_MOVIES: {"movies": [{"movie": {"ids": {"simkl": 2212913, "tmdb": "1153576"}}}]}})

    index = m.build_index(_adapter(session))

    assert list(index) == [kept_key]
    assert index[kept_key]["title"] == "Street Fighter"
    assert index[kept_key]["year"] == 2026


def test_shows_status_stamp_uses_tv_shows_activity_group() -> None:
    from providers.sync.simkl import _watchlist as m

    ts = m._bucket_ts({"tv_shows": {"watching": "2026-11-20T20:25:26Z"}, "anime": {"completed": "2026-11-20T20:25:26Z"}})

    assert ts["shows"]["st"] == "2026-11-20T20:25:26Z"
    assert ts["anime"]["st"] == "2026-11-20T20:25:26Z"
    assert ts["movies"]["st"] is None
