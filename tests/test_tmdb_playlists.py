# /tests/test_tmdb_playlists.py
# CrossWatch - TMDb v4 list playlists and v4 connect flow
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import authenticationAPI as auth
from cw_platform.playlists import supports_playlists
from providers.sync import _mod_TMDB as mod
from providers.sync.tmdb import _playlists as playlists


class _Resp:
    def __init__(self, status: int, data: Any) -> None:
        self.status_code = status
        self._data = data
        self.content = b"x" if data is not None else b""
        self.text = "x" if data is not None else ""
        self.headers: dict[str, str] = {}

    def json(self) -> Any:
        return self._data


def _cfg(**extra: Any) -> dict[str, Any]:
    return {"tmdb_sync": {"api_key": "a" * 32, "session_id": "sess", "account_id": "7", **extra}}


def _v4_cfg() -> dict[str, Any]:
    return _cfg(access_token="user-access", account_object_id="obj-1")


@pytest.fixture(autouse=True)
def no_enrich(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import probesAPI

    monkeypatch.setattr(playlists, "enrich_ids_dict", lambda *args, **kwargs: None)
    monkeypatch.setattr(probesAPI, "invalidate_provider_caches", lambda *args: None)


def _fake_v4(monkeypatch: pytest.MonkeyPatch, adapter: Any, handler: Any) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def v4(method: str, path: str, *, params: Any = None, json_body: Any = None, timeout: Any = None) -> _Resp:
        calls.append({"method": method, "path": path, "params": params, "json": json_body})
        return handler(method, path, params, json_body)

    monkeypatch.setattr(adapter.client, "v4", v4)
    return calls


def test_tmdb_exposes_playlist_ops() -> None:
    assert supports_playlists(mod.OPS)
    assert mod.OPS.features()["playlists"] is True
    assert mod.OPS.capabilities()["playlists"]["reorder"] is False


def test_v3_only_connection_needs_reconnect_for_lists() -> None:
    assert mod.OPS.is_configured(_cfg()) is True
    assert mod.OPS.playlists_configured(_cfg()) is False
    assert mod.OPS.playlists_configured(_v4_cfg()) is True


def test_lists_without_v4_token_raise_user_error() -> None:
    adapter = mod.TMDBModule(_cfg())
    with pytest.raises(mod.TMDBListsAuthError, match="API Read Access Token"):
        adapter.list_playlist_resources()


def test_v4_request_uses_user_access_token(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = mod.TMDBModule(_v4_cfg())
    seen: dict[str, Any] = {}

    def fake(session: Any, method: str, url: str, **kwargs: Any) -> _Resp:
        seen.update({"method": method, "url": url, **kwargs})
        return _Resp(200, {})

    monkeypatch.setattr(mod, "request_with_retries", fake)
    adapter.client.v4("DELETE", "/list/9/items", json_body={"items": []})
    assert seen["url"] == "https://api.themoviedb.org/4/list/9/items"
    assert seen["headers"]["Authorization"] == "Bearer user-access"
    assert seen["json"] == {"items": []}


def test_list_resources_pages_account_lists(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = mod.TMDBModule(_v4_cfg(), instance_id="TMDB-P01")
    pages = {
        1: {"page": 1, "total_pages": 2, "results": [{"id": 11, "name": "Faves", "number_of_items": 3, "public": 1}]},
        2: {"page": 2, "total_pages": 2, "results": [{"id": 12, "name": "Later", "number_of_items": 0, "public": 0}]},
    }
    calls = _fake_v4(monkeypatch, adapter, lambda method, path, params, body: _Resp(200, pages[params["page"]]))
    resources = adapter.list_playlist_resources()
    assert [c["path"] for c in calls] == ["/account/obj-1/lists", "/account/obj-1/lists"]
    assert [(r.id, r.name, r.instance) for r in resources] == [("11", "Faves", "TMDB-P01"), ("12", "Later", "TMDB-P01")]
    assert resources[0].can_add and resources[0].can_remove and not resources[0].can_reorder
    assert resources[1].extra["private"] is True


def test_snapshot_maps_movies_and_shows(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = mod.TMDBModule(_v4_cfg())
    data = {
        "id": 11,
        "name": "Faves",
        "total_pages": 1,
        "total_results": 3,
        "results": [
            {"media_type": "movie", "id": 550, "title": "Fight Club", "release_date": "1999-10-15"},
            {"media_type": "tv", "id": 1399, "name": "Game of Thrones", "first_air_date": "2011-04-17"},
            {"media_type": "person", "id": 287},
        ],
    }
    _fake_v4(monkeypatch, adapter, lambda *args: _Resp(200, data))
    snap = adapter.get_playlist_snapshot("11")
    assert snap.resource.name == "Faves"
    assert [(it.item["type"], it.item["ids"]["tmdb"], it.item["year"]) for it in snap.items] == [("movie", "550", 1999), ("show", "1399", 2011)]
    assert [it.position for it in snap.items] == [0, 1]


def test_add_items_confirms_per_item_results(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = mod.TMDBModule(_v4_cfg())
    monkeypatch.setattr(playlists, "resolve_tmdb_id", lambda adapter, item, want: (item.get("ids") or {}).get("tmdb"))

    def handler(method: str, path: str, params: Any, body: Any) -> _Resp:
        return _Resp(200, {"success": True, "results": [
            {"media_type": "movie", "media_id": 550, "success": True},
            {"media_type": "tv", "media_id": 1399, "success": False, "error": ["Media already taken"]},
        ]})

    calls = _fake_v4(monkeypatch, adapter, handler)
    items = [
        {"type": "movie", "title": "Fight Club", "ids": {"tmdb": 550, "imdb": "tt0137523"}},
        {"type": "show", "title": "Game of Thrones", "ids": {"tmdb": 1399}},
        {"type": "episode", "title": "Pilot", "season": 1, "episode": 1, "ids": {"tmdb": 63056}, "show_ids": {"tmdb": 1399}},
        {"type": "movie", "title": "No id", "ids": {"imdb": "tt0000001"}},
    ]
    res = adapter.add_playlist_items("11", items)
    assert calls[0]["method"] == "POST" and calls[0]["path"] == "/list/11/items"
    assert calls[0]["json"] == {"items": [{"media_type": "movie", "media_id": 550}, {"media_type": "tv", "media_id": 1399}]}
    assert res["ok"] is False
    assert res["count"] == 1
    assert len(res["confirmed_keys"]) == 1
    hints = sorted(u["hint"] for u in res["unresolved"])
    assert hints == ["missing_tmdb_id", "rejected", "unsupported_type"]


def test_remove_items_uses_delete_and_whole_success(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = mod.TMDBModule(_v4_cfg())
    calls = _fake_v4(monkeypatch, adapter, lambda *args: _Resp(200, {"success": True}))
    res = adapter.remove_playlist_items("11", [{"type": "movie", "title": "Fight Club", "ids": {"tmdb": 550}}])
    assert calls[0]["method"] == "DELETE"
    assert res["ok"] is True and res["count"] == 1


def test_create_list_is_private(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = mod.TMDBModule(_v4_cfg())
    calls = _fake_v4(monkeypatch, adapter, lambda *args: _Resp(201, {"id": 77, "success": True}))
    res = adapter.create_playlist("Synced")
    assert calls[0]["json"] == {"name": "Synced", "iso_639_1": "en", "public": False}
    assert res.id == "77" and res.name == "Synced"


def test_playlist_providers_flag_reconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    from services import playlists as svc

    monkeypatch.setattr(svc, "_providers", lambda: {"TMDB": mod.OPS})
    rows = svc.list_playlist_providers(_cfg())
    assert rows and rows[0]["configured"] is False and rows[0]["reason"] == "reconnect_required"
    rows = svc.list_playlist_providers(_v4_cfg())
    assert rows[0]["configured"] is True and "reason" not in rows[0]


def test_v4_connect_flow_stores_tokens_and_session(monkeypatch: pytest.MonkeyPatch) -> None:
    read_token = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJ4In0.sig"
    store: dict[str, Any] = {"tmdb_sync": {"api_key": "a" * 32, "session_id": "old-session"}}
    approved = False
    posts: list[tuple[str, Any, Any]] = []

    monkeypatch.setattr(auth, "load_config", lambda: deepcopy(store))

    def save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(deepcopy(cfg))

    def post(url: str, **kwargs: Any) -> Any:
        posts.append((url, (kwargs.get("headers") or {}).get("Authorization"), kwargs.get("json")))
        if url.endswith("/4/auth/request_token"):
            body = {"request_token": "req-1", "success": True}
        elif url.endswith("/4/auth/access_token"):
            if not approved:
                raise RuntimeError("not approved")
            body = {"access_token": "user-access", "account_id": "obj-1", "success": True}
        elif url.endswith("/authentication/session/convert/4"):
            body = {"session_id": "new-session", "success": True}
        else:
            raise AssertionError(url)
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: body)

    def get(url: str, **kwargs: Any) -> Any:
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"id": 7, "username": "user"})

    monkeypatch.setattr(auth, "save_config", save)
    monkeypatch.setattr(auth, "requests", SimpleNamespace(post=post, get=get, exceptions=auth.requests.exceptions))
    app = FastAPI()
    auth.register_auth(app)
    client = TestClient(app)

    start = client.post("/api/tmdb_sync/connect/start", json={"api_key": "a" * 32, "read_token": read_token}).json()
    assert start["ok"] is True
    assert start["auth_url"] == "https://www.themoviedb.org/auth/access?request_token=req-1"
    assert posts[0][1] == f"Bearer {read_token}"
    assert store["tmdb_sync"]["_pending_v4"] is True

    pending = client.get("/api/tmdb_sync/verify").json()
    assert pending["pending"] is True
    assert store["tmdb_sync"]["session_id"] == "old-session"

    approved = True
    done = client.get("/api/tmdb_sync/verify").json()
    assert done["connected"] is True and done["lists_ready"] is True
    tm = store["tmdb_sync"]
    assert (tm["access_token"], tm["account_object_id"], tm["session_id"]) == ("user-access", "obj-1", "new-session")
    assert "_pending_request_token" not in tm and "_pending_v4" not in tm
    assert posts[-1][2] == {"access_token": "user-access"}


def test_connect_rejects_api_key_pasted_as_read_token(monkeypatch: pytest.MonkeyPatch) -> None:
    store: dict[str, Any] = {"tmdb_sync": {}}
    monkeypatch.setattr(auth, "load_config", lambda: deepcopy(store))
    monkeypatch.setattr(auth, "save_config", lambda cfg: None)
    app = FastAPI()
    auth.register_auth(app)
    res = TestClient(app).post("/api/tmdb_sync/connect/start", json={"api_key": "a" * 32, "read_token": "b" * 32}).json()
    assert res["ok"] is False
    assert "Read Access Token" in res["error"]


def test_snapshot_refuses_lists_beyond_page_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = mod.TMDBModule(_v4_cfg())
    big = {"id": 11, "name": "Huge", "total_pages": playlists.MAX_PAGES + 1, "results": []}
    _fake_v4(monkeypatch, adapter, lambda *args: _Resp(200, big))
    with pytest.raises(playlists.TMDBPlaylistError, match="too large"):
        adapter.get_playlist_snapshot("11")


def _auth_client(monkeypatch: pytest.MonkeyPatch, store: dict[str, Any], post: Any) -> TestClient:
    monkeypatch.setattr(auth, "load_config", lambda: deepcopy(store))

    def save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(deepcopy(cfg))

    def get(url: str, **kwargs: Any) -> Any:
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"id": 8, "username": "other"})

    monkeypatch.setattr(auth, "save_config", save)
    monkeypatch.setattr(auth, "requests", SimpleNamespace(post=post, get=get, exceptions=auth.requests.exceptions))
    app = FastAPI()
    auth.register_auth(app)
    return TestClient(app)


def test_v3_reconnect_drops_previous_v4_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    store: dict[str, Any] = {"tmdb_sync": {
        "api_key": "a" * 32, "session_id": "old", "read_token": "", "access_token": "user-a",
        "account_object_id": "obj-a", "_pending_request_token": "req-b", "_pending_v4": False,
    }}

    def post(url: str, **kwargs: Any) -> Any:
        assert url.endswith("/authentication/session/new")
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"session_id": "sess-b"})

    done = _auth_client(monkeypatch, store, post).get("/api/tmdb_sync/verify").json()
    tm = store["tmdb_sync"]
    assert done["connected"] is True and done["lists_ready"] is False
    assert tm["session_id"] == "sess-b" and not tm.get("access_token") and "account_object_id" not in tm


def test_failed_session_conversion_keeps_redeemed_access_token(monkeypatch: pytest.MonkeyPatch) -> None:
    store: dict[str, Any] = {"tmdb_sync": {
        "api_key": "a" * 32, "session_id": "old", "read_token": "eyJa.eyJb.sig",
        "_pending_request_token": "req-1", "_pending_v4": True,
    }}
    redeemed: list[str] = []
    convert_ok = False

    def post(url: str, **kwargs: Any) -> Any:
        if url.endswith("/4/auth/access_token"):
            redeemed.append(kwargs["json"]["request_token"])
            if len(redeemed) > 1:
                raise RuntimeError("request token already used")
            body = {"access_token": "user-access", "account_id": "obj-1"}
        elif url.endswith("/authentication/session/convert/4"):
            if not convert_ok:
                raise RuntimeError("timeout")
            body = {"session_id": "new-session"}
        else:
            raise AssertionError(url)
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: body)

    client = _auth_client(monkeypatch, store, post)
    assert client.get("/api/tmdb_sync/verify").json()["pending"] is True
    assert store["tmdb_sync"]["_pending_access_token"] == "user-access"
    assert store["tmdb_sync"]["session_id"] == "old"
    convert_ok = True
    done = client.get("/api/tmdb_sync/verify").json()
    tm = store["tmdb_sync"]
    assert done["connected"] is True and done["lists_ready"] is True
    assert redeemed == ["req-1"]
    assert (tm["access_token"], tm["account_object_id"], tm["session_id"]) == ("user-access", "obj-1", "new-session")
    assert "_pending_access_token" not in tm and "_pending_account_object_id" not in tm
