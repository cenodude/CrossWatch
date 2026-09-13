from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient


def _client(monkeypatch, cfg: dict[str, Any], rpc) -> TestClient:
    from api.authenticationAPI import register_auth

    monkeypatch.setattr("api.authenticationAPI.load_config", lambda: cfg)
    monkeypatch.setattr("api.authenticationAPI.save_config", lambda *_a, **_k: None)
    monkeypatch.setattr("providers.sync.kodi._common.jsonrpc_call", rpc)
    app = FastAPI()
    register_auth(app)
    return TestClient(app)


def test_kodi_users_lists_profiles_and_marks_current(monkeypatch) -> None:
    cfg: dict[str, Any] = {"kodi": {"server": "http://kodi.local", "connection_verified": True}}

    def rpc(_server: str, method: str, **_kwargs: Any) -> Any:
        if method == "Profiles.GetProfiles":
            return {"limits": {"start": 0, "end": 3, "total": 3}, "profiles": [{"label": "Master user"}, {"label": "Anna"}, {"label": "Anna"}]}
        if method == "Profiles.GetCurrentProfile":
            return {"label": "Anna"}
        raise AssertionError(method)

    res = _client(monkeypatch, cfg, rpc).get("/api/kodi/users")

    assert res.status_code == 200
    assert res.json()["users"] == [
        {"id": "Master user", "name": "Master user", "current": False},
        {"id": "Anna", "name": "Anna", "current": True},
    ]


def test_kodi_users_requires_verified_connection(monkeypatch) -> None:
    cfg: dict[str, Any] = {"kodi": {"server": "http://kodi.local"}}

    def rpc(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("should not call Kodi")

    res = _client(monkeypatch, cfg, rpc).get("/api/kodi/users")

    assert res.status_code == 401
