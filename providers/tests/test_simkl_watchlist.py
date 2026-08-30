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
    def __init__(self, get_payload: Any = None, post_payload: Any = None) -> None:
        self.get_payload = get_payload if get_payload is not None else {}
        self.post_payload = post_payload if post_payload is not None else {"added": {"movies": 1}}
        self.posts: list[dict[str, Any]] = []

    def get(self, url: str, **_kwargs: Any) -> _Resp:
        return _Resp(200, self.get_payload)

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
