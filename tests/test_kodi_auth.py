# CrossWatch test scripts
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
import requests


@dataclass
class ResponseStub:
    status_code: int = 200
    payload: Any = None

    def json(self) -> Any:
        return self.payload if self.payload is not None else {}


def _ok_payloads() -> list[ResponseStub]:
    return [
        ResponseStub(payload={"jsonrpc": "2.0", "id": "JSONRPC.Ping", "result": "pong"}),
        ResponseStub(
            payload={
                "jsonrpc": "2.0",
                "id": "Application.GetProperties",
                "result": {"name": "Kodi", "version": {"major": 21, "minor": 3, "patch": 0}},
            }
        ),
        ResponseStub(payload={"jsonrpc": "2.0", "id": "JSONRPC.Version", "result": {"version": {"major": 13, "minor": 5, "patch": 0}}}),
    ]


def test_kodi_successful_connection_without_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_KODI as kodi

    calls: list[dict[str, Any]] = []
    payloads = _ok_payloads()

    def fake_post(url: str, **kwargs: Any) -> ResponseStub:
        calls.append({"url": url, **kwargs})
        return payloads.pop(0)

    monkeypatch.setattr(kodi.requests, "post", fake_post)
    cfg: dict[str, Any] = {"kodi": {"server": "localhost:8080/", "verify_ssl": False}}

    result = kodi.KodiAuth().start(cfg)

    assert result["ok"] is True
    assert result["kodi_version"] == "21.3.0"
    assert result["jsonrpc_version"] == "13.5.0"
    assert cfg["kodi"]["server"] == "http://localhost:8080"
    assert cfg["kodi"]["auth_method"] == "none"
    assert cfg["kodi"]["connection_verified"] is True
    assert all(call["url"] == "http://localhost:8080/jsonrpc" for call in calls)
    assert all(call["auth"] is None for call in calls)


def test_kodi_successful_connection_with_http_basic_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    from requests.auth import HTTPBasicAuth

    from providers.auth import _auth_KODI as kodi

    calls: list[dict[str, Any]] = []
    payloads = _ok_payloads()

    def fake_post(url: str, **kwargs: Any) -> ResponseStub:
        calls.append({"url": url, **kwargs})
        return payloads.pop(0)

    monkeypatch.setattr(kodi.requests, "post", fake_post)
    cfg: dict[str, Any] = {"kodi": {"server": "https://kodi.local", "username": "kodi-user", "password": "kodi-pass", "verify_ssl": True}}

    result = kodi.KodiAuth().start(cfg)

    assert result["ok"] is True
    assert cfg["kodi"]["password"] == "kodi-pass"
    assert cfg["kodi"]["auth_method"] == "basic"
    assert all(isinstance(call["auth"], HTTPBasicAuth) for call in calls)
    assert all(call["auth"].username == "kodi-user" for call in calls)
    assert all(call["auth"].password == "kodi-pass" for call in calls)
    assert all(call["verify"] is True for call in calls)


def test_kodi_invalid_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_KODI as kodi

    monkeypatch.setattr(kodi.requests, "post", lambda *_args, **_kwargs: ResponseStub(status_code=401, payload={"error": "Unauthorized"}))

    with pytest.raises(kodi.KodiAuthError) as exc:
        kodi.KodiAuth().start({"kodi": {"server": "http://kodi.local", "username": "bad", "password": "bad"}})

    assert exc.value.reason == "invalid_credentials"


def test_kodi_unreachable_server(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_KODI as kodi

    def fail_post(*_args: Any, **_kwargs: Any) -> ResponseStub:
        raise requests.ConnectionError("refused")

    monkeypatch.setattr(kodi.requests, "post", fail_post)

    with pytest.raises(kodi.KodiAuthError) as exc:
        kodi.KodiAuth().start({"kodi": {"server": "http://127.0.0.1:8080"}})

    assert exc.value.reason == "unreachable"


def test_kodi_jsonrpc_error_includes_method_and_detail(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_KODI as kodi

    monkeypatch.setattr(
        kodi.requests,
        "post",
        lambda *_args, **_kwargs: ResponseStub(
            payload={
                "jsonrpc": "2.0",
                "id": "VideoLibrary.GetMovies",
                "error": {"code": -32602, "message": "Invalid params.", "data": {"method": "VideoLibrary.GetMovies"}},
            }
        ),
    )

    with pytest.raises(kodi.KodiAuthError) as exc:
        kodi.jsonrpc_call("http://kodi.local", "VideoLibrary.GetMovies", params={"properties": ["bad"]})

    assert exc.value.reason == "jsonrpc_error"
    assert "VideoLibrary.GetMovies" in str(exc.value)
    assert "Invalid params." in str(exc.value)
    assert "-32602" in str(exc.value)
    assert exc.value.detail == {"code": -32602, "message": "Invalid params.", "data": {"method": "VideoLibrary.GetMovies"}}


def test_kodi_disconnect_clears_verified_connection() -> None:
    from providers.auth import _auth_KODI as kodi

    cfg: dict[str, Any] = {
        "kodi": {
            "server": "http://kodi.local",
            "username": "kodi-user",
            "password": "secret",
            "kodi_version": "21.3.0",
            "jsonrpc_version": "13.5.0",
            "auth_method": "basic",
            "connection_verified": True,
        }
    }

    status = kodi.KodiAuth().disconnect(cfg)

    assert status.connected is False
    assert cfg["kodi"]["server"] == "http://kodi.local"
    assert cfg["kodi"]["username"] == "kodi-user"
    assert cfg["kodi"]["password"] == ""
    assert "kodi_version" not in cfg["kodi"]
    assert "jsonrpc_version" not in cfg["kodi"]
    assert "auth_method" not in cfg["kodi"]
    assert "connection_verified" not in cfg["kodi"]
