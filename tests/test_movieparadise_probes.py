# tests/test_movieparadise_probes.py
# CrossWatch - Movie Paradise connection probe, account info and auth routes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from typing import Any

import pytest


def _configured(token: str = "mp_at_token", **extra: Any) -> dict[str, Any]:
    return {
        "movieparadise": {
            "access_token": token,
            "refresh_token": "mp_rt_token",
            "expires_at": 1_900_000_000,
            "username": "pazzie",
            **extra,
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


def test_movieparadise_probe_is_registered() -> None:
    from api import probesAPI as probes

    assert "movieparadise" in probes.PROVIDERS
    assert probes.PROBE_CFG_KEY["MOVIEPARADISE"] == "movieparadise"
    assert probes.DETAIL_PROBES["MOVIEPARADISE"] is probes._probe_movieparadise_detail
    assert probes.USERINFO_FNS["MOVIEPARADISE"] is probes.movieparadise_user_info


def test_movieparadise_probe_key_is_credential_scoped_and_opaque() -> None:
    from api import probesAPI as probes

    key = probes._probe_key("movieparadise", _configured())

    assert key.startswith("movieparadise|tok:")
    assert "mp_at_token" not in key
    assert key != probes._probe_key("movieparadise", _configured("mp_at_other"))
    assert key != probes._probe_key("movieparadise", _configured(auth_error="reconnect_required"))
    assert probes._probe_key("movieparadise", {"movieparadise": {}}) == "movieparadise|unconfigured"


def test_movieparadise_probe_ok_hits_me_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import probesAPI as probes
    from providers.auth import _auth_MOVIEPARADISE as mp

    probes.invalidate_provider_caches("movieparadise")
    calls = _stub_request(monkeypatch, _Resp(200, {"id": 40391, "username": "pazzie", "credential": {"scopes": ["library:read"]}}))

    ok, reason = probes._probe_movieparadise_detail(_configured(), max_age_sec=0)

    assert (ok, reason) == (True, "")
    assert calls[0]["provider"] == "movieparadise"
    assert calls[0]["url"] == mp.ME_URL


def test_movieparadise_probe_reconnect_flag_skips_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import probesAPI as probes

    probes.invalidate_provider_caches("movieparadise")
    calls = _stub_request(monkeypatch, _Resp(200, {"id": 1, "username": "x"}))

    ok, reason = probes._probe_movieparadise_detail(_configured(auth_error="reconnect_required"), max_age_sec=0)

    assert (ok, reason) == (False, "Movie Paradise: reconnect required")
    assert calls == []


def test_movieparadise_probe_401_asks_for_reconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import probesAPI as probes

    probes.invalidate_provider_caches("movieparadise")
    _stub_request(monkeypatch, _Resp(401, {"code": "invalid_token"}))

    ok, reason = probes._probe_movieparadise_detail(_configured(), max_age_sec=0)

    assert (ok, reason) == (False, "Movie Paradise: reconnect required")


def _stub_by_url(monkeypatch: pytest.MonkeyPatch, answers: dict[str, _Resp]) -> list[str]:
    from providers.auth import runtime

    urls: list[str] = []

    def fake(provider: str, session: Any, method: str, url: str, **kwargs: Any) -> _Resp:
        urls.append(url)
        return answers[url]

    monkeypatch.setattr(runtime, "request_with_auth", fake)
    return urls


@pytest.mark.parametrize(("tier", "vip"), [("premium", True), ("free", False)])
def test_movieparadise_user_info_reads_plan_from_capabilities(monkeypatch: pytest.MonkeyPatch, tier: str, vip: bool) -> None:
    from api import probesAPI as probes
    from providers.auth import _auth_MOVIEPARADISE as mp

    probes.invalidate_provider_caches("movieparadise")
    urls = _stub_by_url(monkeypatch, {
        mp.ME_URL: _Resp(200, {"id": 40391, "username": "pazzie", "credential": {"id": 89, "scopes": ["library:read", "library:write"]}}),
        mp.CAPABILITIES_URL: _Resp(200, {"limits": {"tier": tier, "requests_per_minute": 120}, "client": {"verified": True}}),
    })

    info = probes.movieparadise_user_info(_configured(), max_age_sec=0)

    assert info == {"username": "pazzie", "user_id": 40391, "scopes": ["library:read", "library:write"], "plan": tier, "vip": vip}
    assert urls == [mp.ME_URL, mp.CAPABILITIES_URL]


def test_movieparadise_user_info_without_capabilities_has_no_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import probesAPI as probes
    from providers.auth import _auth_MOVIEPARADISE as mp

    probes.invalidate_provider_caches("movieparadise")
    _stub_by_url(monkeypatch, {
        mp.ME_URL: _Resp(200, {"id": 40391, "username": "pazzie"}),
        mp.CAPABILITIES_URL: _Resp(503, {"code": "unavailable"}),
    })

    info = probes.movieparadise_user_info(_configured(), max_age_sec=0)

    assert info["username"] == "pazzie"
    assert "plan" not in info and "vip" not in info


def test_movieparadise_counts_as_configured_provider() -> None:
    from api import probesAPI as probes

    assert probes._prov_configured(_configured(), "MOVIEPARADISE", "default") is True
    assert probes._prov_configured({"movieparadise": {}}, "MOVIEPARADISE", "default") is False


def test_auth_routes_disconnect_and_bust_probe_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import api.authenticationAPI as authapi
    from api import probesAPI as probes
    import providers.auth._auth_MOVIEPARADISE as mp

    store: dict[str, Any] = {"cfg": _configured()}
    monkeypatch.setattr(authapi, "load_config", lambda: store["cfg"])
    monkeypatch.setattr(mp, "_load_full_cfg", lambda: store["cfg"])
    monkeypatch.setattr(mp, "_save_full_cfg", lambda cfg: store.__setitem__("cfg", cfg))
    monkeypatch.setattr(mp, "cleanup_remote", lambda *a, **k: None)

    app = FastAPI()
    authapi.register_auth(app, probe_cache=probes.PROBE_CACHE)
    probes.PROBE_CACHE["movieparadise"] = (123.0, True)
    client = TestClient(app)

    status = client.get("/api/movieparadise/status").json()
    assert status["connected"] is True and status["username"] == "pazzie"

    res = client.post("/api/movieparadise/disconnect")

    assert res.status_code == 200 and res.json()["ok"] is True
    assert probes.PROBE_CACHE["movieparadise"] == (0.0, False)
    assert store["cfg"]["movieparadise"]["access_token"] == ""
