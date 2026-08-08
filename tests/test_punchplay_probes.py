# CrossWatch test scripts
from __future__ import annotations

import json
from typing import Any

import pytest


def _configured(token: str = "access-token") -> dict[str, Any]:
    return {
        "punchplay": {
            "access_token": token,
            "refresh_token": "refresh-token",
            "expires_at": 1_900_000_000,
            "username": "scott",
            "scope": "profile:read history:write",
        }
    }


class _Resp:
    def __init__(self, status_code: int, payload: Any = None) -> None:
        self.status_code = status_code
        self.text = json.dumps(payload) if payload is not None else ""


def _stub_request(monkeypatch: pytest.MonkeyPatch, resp: _Resp) -> list[dict[str, Any]]:
    from providers.auth import runtime

    calls: list[dict[str, Any]] = []

    def fake(provider: str, session: Any, method: str, url: str, **kwargs: Any) -> _Resp:
        calls.append({"provider": provider, "method": method, "url": url, **kwargs})
        return resp

    monkeypatch.setattr(runtime, "request_with_auth", fake)
    return calls


def test_punchplay_probe_is_registered() -> None:
    from api import probesAPI as probes

    assert "punchplay" in probes.PROVIDERS
    assert "punchplay" in probes.PROBE_CACHE
    assert probes.PROBE_CFG_KEY["PUNCHPLAY"] == "punchplay"
    assert probes._cfg_key("PUNCHPLAY") == "punchplay"
    assert probes.DETAIL_PROBES["PUNCHPLAY"] is probes._probe_punchplay_detail
    assert probes.USERINFO_FNS["PUNCHPLAY"] is probes.punchplay_user_info


def test_punchplay_probe_unconfigured_reason() -> None:
    from api import probesAPI as probes

    probes.invalidate_provider_caches("punchplay")

    ok, reason = probes._probe_punchplay_detail({"punchplay": {}}, max_age_sec=0)

    assert ok is False
    assert reason == "PunchPlay: missing authentication"


def test_punchplay_probe_key_is_credential_scoped_and_opaque() -> None:
    from api import probesAPI as probes

    key = probes._probe_key("punchplay", _configured())
    other = probes._probe_key("punchplay", _configured("different-token"))
    empty = probes._probe_key("punchplay", {"punchplay": {}})

    assert key.startswith("punchplay|tok:")
    assert "access-token" not in key
    assert key != other
    assert empty == "punchplay|unconfigured"


def test_punchplay_probe_ok_hits_me_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import probesAPI as probes
    from providers.auth import _auth_PUNCHPLAY as pp

    probes.invalidate_provider_caches("punchplay")
    calls = _stub_request(monkeypatch, _Resp(200, {"id": "usr_1", "username": "scott", "scopes": ["profile:read"]}))

    ok, reason = probes._probe_punchplay_detail(_configured(), max_age_sec=0)

    assert (ok, reason) == (True, "")
    assert calls[0]["provider"] == "punchplay"
    assert calls[0]["url"] == pp.ME_URL
    assert calls[0]["method"] == "GET"


def test_punchplay_probe_401_asks_for_reconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import probesAPI as probes

    probes.invalidate_provider_caches("punchplay")
    _stub_request(monkeypatch, _Resp(401, {"error": "invalid_token"}))

    ok, reason = probes._probe_punchplay_detail(_configured(), max_age_sec=0)

    assert ok is False
    assert reason == "PunchPlay: reconnect required"


def test_punchplay_probe_rejects_response_without_id(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import probesAPI as probes

    probes.invalidate_provider_caches("punchplay")
    _stub_request(monkeypatch, _Resp(200, {"username": "scott"}))

    ok, reason = probes._probe_punchplay_detail(_configured(), max_age_sec=0)

    assert ok is False
    assert reason == "PunchPlay: invalid response"


def test_punchplay_user_info_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import probesAPI as probes

    probes.invalidate_provider_caches("punchplay")
    _stub_request(
        monkeypatch,
        _Resp(200, {
            "id": "usr_1",
            "username": "scott",
            "profile": {"displayName": "Scott", "avatarUrl": "https://punchplay.tv/a.png"},
            "scopes": ["profile:read", "history:write"],
        }),
    )

    info = probes.punchplay_user_info(_configured(), max_age_sec=0)

    assert info["username"] == "scott"
    assert info["user_id"] == "usr_1"
    assert info["avatar"] == "https://punchplay.tv/a.png"
    assert info["scopes"] == ["profile:read", "history:write"]


def test_punchplay_user_info_empty_when_unconfigured() -> None:
    from api import probesAPI as probes

    probes.invalidate_provider_caches("punchplay")

    assert probes.punchplay_user_info({"punchplay": {}}, max_age_sec=0) == {}


def test_punchplay_counts_as_configured_provider() -> None:
    from api import probesAPI as probes

    assert probes._prov_configured(_configured(), "PUNCHPLAY", "default") is True
    assert probes._prov_configured({"punchplay": {}}, "PUNCHPLAY", "default") is False


def test_auth_routes_bust_the_punchplay_probe_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import api.authenticationAPI as authapi
    from api import probesAPI as probes
    import providers.auth._auth_PUNCHPLAY as pp

    store: dict[str, Any] = {"cfg": _configured()}
    monkeypatch.setattr(authapi, "load_config", lambda: store["cfg"])
    monkeypatch.setattr(pp, "_load_full_cfg", lambda: store["cfg"])
    monkeypatch.setattr(pp, "_save_full_cfg", lambda cfg: store.__setitem__("cfg", cfg))
    monkeypatch.setattr(pp, "revoke_token", lambda *a, **k: {"ok": True})

    app = FastAPI()
    authapi.register_auth(app, probe_cache=probes.PROBE_CACHE)
    probes.PROBE_CACHE["punchplay"] = (123.0, True)

    res = TestClient(app).post("/api/punchplay/disconnect")

    assert res.status_code == 200
    assert res.json()["ok"] is True
    assert probes.PROBE_CACHE["punchplay"] == (0.0, False)
    assert store["cfg"]["punchplay"]["access_token"] == ""
