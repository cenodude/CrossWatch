from __future__ import annotations

import importlib
from typing import Any

pl = importlib.import_module("providers.sync.simkl._playlists")
mod = importlib.import_module("providers.sync._mod_SIMKL")


class FakeResp:
    def __init__(self, status: int, payload: Any = None):
        self.status_code = status
        self._payload = payload
        self.text = "x" if payload is not None else ""
        self.headers = {}
        self.ok = 200 <= status < 300

    def json(self) -> Any:
        return self._payload


class FakeSession:
    def __init__(self, responses: dict[tuple[str, str], Any] | None = None):
        self.responses = responses or {}
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, **kw: Any) -> FakeResp:
        self.calls.append({"method": "GET", "url": url, "params": kw.get("params")})
        return FakeResp(200, self.responses.get(("GET", url), {}))

    def post(self, url: str, **kw: Any) -> FakeResp:
        self.calls.append({"method": "POST", "url": url, "json": kw.get("json"), "params": kw.get("params")})
        return FakeResp(200, self.responses.get(("POST", url), {}))


class FakeCfg:
    api_key = "client"
    access_token = "token"
    timeout = 5
    watchlist_batch_size = 100


class FakeClient:
    def __init__(self, session: FakeSession):
        self.session = session


class FakeAdapter:
    def __init__(self, session: FakeSession):
        self.cfg = FakeCfg()
        self.client = FakeClient(session)
        self.instance_id = "default"


def _movie(tmdb: int = 1) -> dict[str, Any]:
    return {"type": "movie", "title": "Movie", "ids": {"tmdb": tmdb}}


def _show(tvdb: int = 2) -> dict[str, Any]:
    return {"type": "show", "title": "Show", "ids": {"tvdb": tvdb}}


def test_enumerates_fixed_status_bucket_endpoints():
    resources = pl.list_resources(FakeAdapter(FakeSession()))
    by_id = {r.id: r for r in resources}
    assert list(by_id) == ["plantowatch", "watching", "hold", "dropped", "completed"]
    assert by_id["plantowatch"].name == "Plan to Watch"
    assert by_id["watching"].media_types == ("show", "anime")
    assert by_id["completed"].can_add is True
    assert by_id["completed"].can_remove is True
    assert by_id["completed"].can_reorder is False
    assert by_id["completed"].extra["endpoint_type"] == "status_bucket"
    assert by_id["completed"].extra["destructive_remove"] is True
    assert by_id["completed"].extra["custom_lists_supported"] is False


def test_status_snapshot_reads_all_allowed_categories():
    session = FakeSession(
        {
            ("GET", "https://api.simkl.com/sync/all-items/movies/completed"): {
                "movies": [{"movie": {"title": "Dune", "year": 2021, "ids": {"tmdb": 438631, "simkl": 1}}}]
            },
            ("GET", "https://api.simkl.com/sync/all-items/shows/completed"): {
                "shows": [{"show": {"title": "Severance", "year": 2022, "ids": {"tvdb": 371980, "simkl": 2}}}]
            },
            ("GET", "https://api.simkl.com/sync/all-items/anime/completed"): {
                "anime": [{"anime": {"title": "Frieren", "year": 2023, "ids": {"mal": 52991, "simkl": 3}}}]
            },
            ("GET", "https://api.simkl.com/sync/activities"): {
                "movies": {"completed": "2026-01-01T00:00:00Z"},
                "shows": {"completed": "2026-01-02T00:00:00Z"},
                "anime": {"completed": "2026-01-03T00:00:00Z"},
            },
        }
    )

    snap = pl.get_snapshot(FakeAdapter(session), "completed")

    assert snap.resource.id == "completed"
    assert len(snap.items) == 3
    assert {c["url"] for c in session.calls if c["method"] == "GET"} >= {
        "https://api.simkl.com/sync/all-items/movies/completed",
        "https://api.simkl.com/sync/all-items/shows/completed",
        "https://api.simkl.com/sync/all-items/anime/completed",
    }
    assert {it.item["simkl_bucket"] for it in snap.items} == {"movies", "shows", "anime"}
    assert snap.checkpoint == "2026-01-03T00:00:00Z"


def test_movies_are_blocked_for_watching_and_hold():
    session = FakeSession()
    res = pl.add(FakeAdapter(session), "watching", [_movie()])

    assert res["count"] == 0
    assert res["unresolved"][0]["hint"] == "unsupported_media_type"
    assert not [c for c in session.calls if c["method"] == "POST"]


def test_add_moves_item_to_selected_bucket():
    session = FakeSession(
        {
            ("POST", "https://api.simkl.com/sync/add-to-list"): {
                "added": {"shows": [{"ids": {"tvdb": 2}, "status": "dropped"}]}
            }
        }
    )

    res = pl.add(FakeAdapter(session), "dropped", [_show()])

    assert res["count"] == 1
    assert res["confirmed_keys"] == ["tvdb:2"]
    call = next(c for c in session.calls if c["method"] == "POST")
    assert call["url"] == "https://api.simkl.com/sync/add-to-list"
    assert call["json"] == {"shows": [{"ids": {"tvdb": "2"}, "to": "dropped"}]}


def test_server_rewritten_status_is_not_confirmed():
    session = FakeSession(
        {
            ("POST", "https://api.simkl.com/sync/add-to-list"): {
                "added": {"movies": [{"ids": {"tmdb": 1}, "status": "completed"}]}
            }
        }
    )

    res = pl.add(FakeAdapter(session), "dropped", [_movie()])

    assert res["count"] == 0
    assert res["confirmed_keys"] == []
    assert res["unresolved"][0]["hint"] == "status_rewritten:completed"
    assert res["unresolved"][0]["actual_status"] == "completed"


def test_remove_uses_destructive_history_remove_warning():
    session = FakeSession(
        {
            ("POST", "https://api.simkl.com/sync/history/remove"): {
                "removed": {"movies": [{"ids": {"tmdb": 1}}]}
            }
        }
    )

    res = pl.remove(FakeAdapter(session), "completed", [_movie()])

    assert res["count"] == 1
    assert pl.SIMKL_REMOVE_WARNING in res["warnings"]
    call = next(c for c in session.calls if c["method"] == "POST")
    assert call["url"] == "https://api.simkl.com/sync/history/remove"
    assert call["json"] == {"movies": [{"ids": {"tmdb": "1"}}]}


def test_existing_watchlist_path_still_delegates_to_plan_to_watch(monkeypatch):
    called: dict[str, Any] = {}

    class FakeWatchlist:
        @staticmethod
        def add(adapter: Any, items: Any) -> tuple[int, list[Any]]:
            called["items"] = list(items)
            return 1, []

    monkeypatch.setitem(mod._FEATURES, "watchlist", FakeWatchlist)
    monkeypatch.setattr(mod, "feat_watchlist", FakeWatchlist)
    adapter = mod.SIMKLModule.__new__(mod.SIMKLModule)
    adapter.key_of = staticmethod(lambda item: "tmdb:1")

    res = mod.SIMKLModule.add(adapter, "watchlist", [_movie()])

    assert res["count"] == 1
    assert called["items"][0]["ids"]["tmdb"] == 1
