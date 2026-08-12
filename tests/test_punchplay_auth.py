# CrossWatch test scripts
from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]


@dataclass
class ResponseStub:
    status_code: int = 200
    payload: Any = None
    text: str = "{}"
    headers: dict[str, str] = field(default_factory=dict)

    def json(self) -> Any:
        if self.payload is None:
            raise ValueError("no json")
        return self.payload


class FakePost:
    def __init__(self, responses: list[ResponseStub]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def __call__(self, url: str, **kwargs: Any) -> ResponseStub:
        self.calls.append({"url": url, **kwargs})
        if not self.responses:
            raise AssertionError(f"unexpected post {url}")
        return self.responses.pop(0)


@pytest.fixture(autouse=True)
def _reset_auth_rate_guard():
    import providers.auth._auth_PUNCHPLAY as pp

    pp._AUTH_GUARD.reset()
    pp._LAST_FORCED_REFRESH.clear()
    yield
    pp._AUTH_GUARD.reset()
    pp._LAST_FORCED_REFRESH.clear()


@pytest.fixture()
def punchplay(monkeypatch: pytest.MonkeyPatch):
    import providers.auth._auth_PUNCHPLAY as pp

    store: dict[str, Any] = {"cfg": {"punchplay": {}}}
    monkeypatch.setattr(pp, "_load_full_cfg", lambda: store["cfg"])
    monkeypatch.setattr(pp, "_save_full_cfg", lambda cfg: store.__setitem__("cfg", cfg))
    monkeypatch.setattr(pp, "_store_identity", lambda block, token: None)
    return pp, store


def test_manifest_is_device_code_without_credential_fields() -> None:
    from providers.auth._auth_PUNCHPLAY import PunchPlayAuth

    manifest = PunchPlayAuth().manifest()

    assert manifest.name == "PUNCHPLAY"
    assert manifest.flow == "device_code"
    assert manifest.fields == []
    assert manifest.verify_url == "https://punchplay.tv/link"
    assert manifest.actions == {"start": True, "finish": True, "refresh": True, "disconnect": True}


def test_device_start_sends_client_id_only_and_stores_pending(punchplay, monkeypatch: pytest.MonkeyPatch) -> None:
    pp, store = punchplay
    post = FakePost([
        ResponseStub(
            200,
            {
                "user_code": "AK76-P48D",
                "device_code": "raw-device-code",
                "verification_uri": "https://punchplay.tv/link",
                "verification_uri_complete": "https://punchplay.tv/link?code=AK76-P48D&type=platform",
                "verification_uri_qr": "data:image/png;base64,AAAA",
                "expires_in": 600,
                "scope": "profile:read history:write",
            },
        )
    ])
    monkeypatch.setattr(pp.requests, "post", post)

    res = pp.start_device_code(store["cfg"], instance_id="default")

    assert res["ok"] is True
    assert res["user_code"] == "AK76-P48D"
    assert res["interval"] == pp.POLL_INTERVAL_SEC
    assert res["verification_uri_complete"].endswith("type=platform")

    sent = post.calls[0]["json"]
    assert sent["client_id"] == pp.app_client_id()
    assert "client_secret" not in sent

    pending = store["cfg"]["punchplay"]["_pending_device"]
    assert pending["device_code"] == "raw-device-code"
    assert pending["expires_at"] > int(time.time())


def test_poll_treats_http_400_authorization_pending_as_pending(punchplay, monkeypatch: pytest.MonkeyPatch) -> None:
    pp, store = punchplay
    store["cfg"]["punchplay"]["_pending_device"] = {
        "device_code": "raw-device-code",
        "user_code": "AK76-P48D",
        "expires_at": int(time.time()) + 600,
    }
    monkeypatch.setattr(
        pp.requests,
        "post",
        FakePost([ResponseStub(400, {"error": "authorization_pending", "message": "not yet"})]),
    )

    res = pp.poll_device_code(store["cfg"], instance_id="default")

    assert res["ok"] is False
    assert res["status"] == "authorization_pending"
    assert store["cfg"]["punchplay"]["_pending_device"]["device_code"] == "raw-device-code"


def test_poll_success_stores_tokens_and_clears_pending(punchplay, monkeypatch: pytest.MonkeyPatch) -> None:
    pp, store = punchplay
    store["cfg"]["punchplay"]["_pending_device"] = {
        "device_code": "raw-device-code",
        "expires_at": int(time.time()) + 600,
    }
    post = FakePost([
        ResponseStub(
            200,
            {
                "access_token": "at-1",
                "refresh_token": "rt-1",
                "token_type": "bearer",
                "expires_in": 3600,
                "refresh_expires_in": 31536000,
                "username": "scott",
                "scope": "profile:read history:write",
            },
        )
    ])
    monkeypatch.setattr(pp.requests, "post", post)

    res = pp.poll_device_code(store["cfg"], instance_id="default")

    assert res["ok"] is True
    assert res["status"] == "authorized"

    blk = store["cfg"]["punchplay"]
    assert blk["access_token"] == "at-1"
    assert blk["refresh_token"] == "rt-1"
    assert blk["username"] == "scott"
    assert blk["expires_at"] > int(time.time()) + 3000
    assert "_pending_device" not in blk

    sent = post.calls[0]["json"]
    assert sent["device_name"] == "CrossWatch"
    assert sent["device_id"].startswith("crosswatch-")
    assert "client_secret" not in sent


def test_poll_expired_clears_pending(punchplay, monkeypatch: pytest.MonkeyPatch) -> None:
    pp, store = punchplay
    store["cfg"]["punchplay"]["_pending_device"] = {
        "device_code": "raw-device-code",
        "expires_at": int(time.time()) + 600,
    }
    monkeypatch.setattr(pp.requests, "post", FakePost([ResponseStub(400, {"error": "expired"})]))

    res = pp.poll_device_code(store["cfg"], instance_id="default")

    assert res["status"] == "expired"
    assert "_pending_device" not in store["cfg"]["punchplay"]


def test_refresh_rotates_refresh_token(punchplay, monkeypatch: pytest.MonkeyPatch) -> None:
    pp, store = punchplay
    store["cfg"]["punchplay"].update({
        "access_token": "at-old",
        "refresh_token": "rt-old",
        "expires_at": int(time.time()) + 10,
    })
    post = FakePost([
        ResponseStub(
            200,
            {
                "access_token": "at-new",
                "refresh_token": "rt-new",
                "token_type": "bearer",
                "expires_in": 3600,
                "refresh_expires_in": 31415926,
                "scope": "profile:read",
            },
        )
    ])
    monkeypatch.setattr(pp.requests, "post", post)

    res = pp.refresh_token(store["cfg"], instance_id="default")

    assert res["ok"] is True
    assert post.calls[0]["url"] == pp.REFRESH_URL
    assert store["cfg"]["punchplay"]["access_token"] == "at-new"
    assert store["cfg"]["punchplay"]["refresh_token"] == "rt-new"


def test_refresh_is_skipped_while_token_is_still_fresh(punchplay, monkeypatch: pytest.MonkeyPatch) -> None:
    pp, store = punchplay
    store["cfg"]["punchplay"].update({
        "access_token": "at-fresh",
        "refresh_token": "rt-1",
        "expires_at": int(time.time()) + 3600,
    })

    def _boom(*_a: Any, **_k: Any):
        raise AssertionError("refresh must not hit the network while the token is fresh")

    monkeypatch.setattr(pp.requests, "post", _boom)

    res = pp.refresh_token(store["cfg"], instance_id="default")

    assert res == {
        "ok": True,
        "status": "fresh",
        "instance": "default",
        "expires_at": store["cfg"]["punchplay"]["expires_at"],
    }


def test_refresh_rate_limit_keeps_tokens(punchplay, monkeypatch: pytest.MonkeyPatch) -> None:
    pp, store = punchplay
    store["cfg"]["punchplay"].update({
        "access_token": "at-old",
        "refresh_token": "rt-old",
        "expires_at": int(time.time()) + 10,
    })
    monkeypatch.setattr(
        pp.requests,
        "post",
        FakePost([ResponseStub(429, {"error": "rate_limited"}, headers={"Retry-After": "42"})]),
    )

    res = pp.refresh_token(store["cfg"], instance_id="default")

    assert res["ok"] is False
    assert res["status"] == "rate_limited"
    assert res["retry_after"] == 42
    assert store["cfg"]["punchplay"]["refresh_token"] == "rt-old"


def test_refresh_invalid_grant_clears_tokens_for_reconnect(punchplay, monkeypatch: pytest.MonkeyPatch) -> None:
    pp, store = punchplay
    store["cfg"]["punchplay"].update({
        "access_token": "at-old",
        "refresh_token": "rt-dead",
        "expires_at": int(time.time()) + 10,
    })
    monkeypatch.setattr(pp.requests, "post", FakePost([ResponseStub(400, {"error": "invalid_grant"})]))

    res = pp.refresh_token(store["cfg"], instance_id="default")

    assert res["ok"] is False
    assert res["status"] == "invalid_grant"
    assert res["reconnect_required"] is True
    assert store["cfg"]["punchplay"]["access_token"] == ""
    assert store["cfg"]["punchplay"]["refresh_token"] == ""


def test_auth_budgets_match_the_documented_limits() -> None:
    import providers.auth._auth_PUNCHPLAY as pp

    assert pp.DEVICE_CODE_BUDGET == (10, 3600.0)
    assert pp.DEVICE_TOKEN_BUDGET == (200, 600.0)
    assert pp.REFRESH_BUDGET == (20, 3600.0)


def test_device_code_is_capped_locally_at_ten_per_hour(punchplay, monkeypatch: pytest.MonkeyPatch) -> None:
    pp, store = punchplay
    ok = ResponseStub(200, {
        "user_code": "AAAA-BBBB", "device_code": "dc", "verification_uri": "https://punchplay.tv/link",
        "verification_uri_complete": "", "verification_uri_qr": "", "expires_in": 600, "scope": "profile:read",
    })
    post = FakePost([ok] * 20)
    monkeypatch.setattr(pp.requests, "post", post)

    results = [pp.start_device_code(store["cfg"], instance_id="default") for _ in range(12)]

    assert len(post.calls) == 10
    assert all(r["ok"] for r in results[:10])
    assert results[10]["error"] == "rate_limited"
    assert results[10]["local"] is True
    assert results[10]["retry_after"] > 0


def test_refresh_is_capped_locally_at_twenty_per_hour(punchplay, monkeypatch: pytest.MonkeyPatch) -> None:
    pp, store = punchplay
    store["cfg"]["punchplay"].update({"access_token": "at", "refresh_token": "rt", "expires_at": 0})

    tok = {"access_token": "a", "refresh_token": "r", "token_type": "bearer", "expires_in": 0, "refresh_expires_in": 0, "scope": "x"}
    post = FakePost([ResponseStub(200, dict(tok)) for _ in range(30)])
    monkeypatch.setattr(pp.requests, "post", post)

    results = [pp.refresh_token(store["cfg"], instance_id="default", force=True) for _ in range(22)]

    assert len(post.calls) == 20
    assert results[20]["status"] == "rate_limited"
    assert results[20]["local"] is True


def test_server_429_blocks_further_local_attempts(punchplay, monkeypatch: pytest.MonkeyPatch) -> None:
    pp, store = punchplay
    post = FakePost([ResponseStub(429, {"error": "rate_limited"}, headers={"Retry-After": "120"})])
    monkeypatch.setattr(pp.requests, "post", post)

    first = pp.start_device_code(store["cfg"], instance_id="default")
    second = pp.start_device_code(store["cfg"], instance_id="default")

    assert first["error"] == "rate_limited"
    assert len(post.calls) == 1
    assert second.get("local") is True
    assert second["retry_after"] >= 120


def test_forced_refresh_is_throttled_so_401s_cannot_storm(monkeypatch: pytest.MonkeyPatch) -> None:
    import providers.auth._auth_PUNCHPLAY as pp

    assert pp._allow_forced_refresh("default") is True
    assert pp._allow_forced_refresh("default") is False
    assert pp._allow_forced_refresh("other") is True



def test_request_with_auth_does_not_refresh_on_every_401(monkeypatch: pytest.MonkeyPatch) -> None:
    import providers.auth._auth_PUNCHPLAY as pp

    refreshes: list[int] = []

    monkeypatch.setattr(pp, "merge_auth_kwargs", lambda *a, **k: {"headers": {}})
    monkeypatch.setattr(pp, "refresh_token", lambda *a, **k: (refreshes.append(1), {"ok": False})[1])

    calls = []

    def fake_call(session, method, url, **kw):
        calls.append(url)
        return ResponseStub(401, {"error": "insufficient_scope"})

    import requests as _requests

    session = _requests.Session()
    for _ in range(25):
        pp.request_with_auth(session, "GET", "https://punchplay.tv/x", cfg={}, instance_id="default", request_func=fake_call)

    assert len(calls) == 25
    assert len(refreshes) == 1


def test_status_for_block_reports_pending_and_scopes() -> None:
    import providers.auth._auth_PUNCHPLAY as pp

    out = pp.status_for_block({
        "access_token": "at",
        "scope": "profile:read history:write",
        "username": "scott",
        "expires_at": 123,
        "_pending_device": {"user_code": "AK76-P48D", "expires_at": 456},
    })

    assert out["connected"] is True
    assert out["auth_method"] == "device_code"
    assert out["scopes"] == ["profile:read", "history:write"]
    assert out["username"] == "scott"
    assert out["pending"]["user_code"] == "AK76-P48D"
    assert out["pending"]["interval"] == pp.POLL_INTERVAL_SEC


def test_default_config_block_is_present_and_unconnected() -> None:
    import providers.auth._auth_PUNCHPLAY as pp
    from cw_platform.config_base import DEFAULT_CFG

    blk = DEFAULT_CFG["punchplay"]

    for field in pp._TOKEN_KEYS:
        assert field in blk, f"DEFAULT_CFG punchplay is missing {field}"
    assert blk["auth_method"] == "device_code"
    assert blk["device_id"] == ""
    assert blk["scope"] == ""

    assert pp.is_configured(blk) is False
    status = pp.status_for_block(blk)
    assert status["connected"] is False
    assert status["scopes"] == []
    assert "pending" not in status


def test_default_config_carries_no_transient_login_state() -> None:
    from cw_platform.config_base import DEFAULT_CFG

    assert "_pending_device" not in DEFAULT_CFG["punchplay"]
    assert "client_id" not in DEFAULT_CFG["punchplay"]
    assert "client_secret" not in DEFAULT_CFG["punchplay"]


def test_empty_pending_device_does_not_report_pending() -> None:
    import providers.auth._auth_PUNCHPLAY as pp

    blank = pp.status_for_block({"_pending_device": {"user_code": "", "expires_at": 0}})
    real = pp.status_for_block({"_pending_device": {"user_code": "AK76-P48D", "expires_at": 123}})

    assert "pending" not in blank
    assert real["pending"]["user_code"] == "AK76-P48D"


def test_runtime_dispatch_resolves_punchplay_backend() -> None:
    from providers.auth import runtime

    import providers.auth._auth_PUNCHPLAY as pp

    assert runtime._backend("punchplay") is pp
    assert runtime.is_configured("punchplay", {"access_token": "at"}) is True
    assert runtime.is_configured("punchplay", {}) is False


def test_registered_in_modules_registry() -> None:
    from cw_platform.modules_registry import MODULES

    assert MODULES["AUTH"]["_auth_PUNCHPLAY"] == "providers.auth._auth_PUNCHPLAY"


def test_auth_api_routes_are_registered(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi import FastAPI

    from api.authenticationAPI import register_auth

    app = FastAPI()
    register_auth(app)
    paths = {getattr(r, "path", "") for r in app.routes}

    assert "/api/punchplay/device/start" in paths
    assert "/api/punchplay/device/poll" in paths
    assert "/api/punchplay/device/cancel" in paths
    assert "/api/punchplay/refresh" in paths
    assert "/api/punchplay/status" in paths
    assert "/api/punchplay/disconnect" in paths


def test_tokens_are_redacted_from_config() -> None:
    from cw_platform.config_base import redact_config

    redacted = redact_config({
        "punchplay": {
            "access_token": "at",
            "refresh_token": "rt",
            "device_id": "crosswatch-abc",
            "username": "scott",
            "instances": {"p2": {"access_token": "at2", "refresh_token": "rt2"}},
        }
    })

    blk = redacted["punchplay"]
    assert blk["access_token"] not in {"at", ""}
    assert blk["refresh_token"] not in {"rt", ""}
    assert blk["instances"]["p2"]["access_token"] not in {"at2", ""}
    assert blk["device_id"] == "crosswatch-abc"
    assert blk["username"] == "scott"


def test_discovery_branding_and_frontend_wiring() -> None:
    from providers.auth.registry import auth_providers_html

    html = auth_providers_html()
    assert html.index('id="sec-punchplay"') > html.index('id="sec-auth-trackers"')

    meta = (ROOT / "assets" / "helpers" / "provider-meta.js").read_text(encoding="utf-8")
    providers_css = (ROOT / "assets" / "css" / "providers.css").read_text(encoding="utf-8")
    auth_css = (ROOT / "assets" / "css" / "auth-providers.css").read_text(encoding="utf-8")
    loader = (ROOT / "assets" / "auth" / "auth_loader.js").read_text(encoding="utf-8")
    ui = (ROOT / "assets" / "helpers" / "providers-ui.js").read_text(encoding="utf-8")
    links = (ROOT / "assets" / "helpers" / "help-links.js").read_text(encoding="utf-8")

    auth_ui = (ROOT / "providers" / "auth" / "_auth_PUNCHPLAY.py").read_text(encoding="utf-8")

    assert 'PUNCHPLAY: { key: "PUNCHPLAY"' in meta
    assert 'rgb: "244,64,15"' in meta
    assert 'logoFile: "PUNCHPLAY.png"' in meta
    assert ".prov-card.brand-punchplay" in providers_css
    assert "/assets/img/PUNCHPLAY.png" in providers_css
    assert "--punchplay-rgb:244,64,15" in providers_css
    assert "--punchplay-blue-rgb:0,132,216" in providers_css
    assert "#punchplay_disconnect" in auth_css
    assert 'punchplay: "/assets/auth/auth.punchplay.js"' in loader
    assert '"sec-punchplay": "punchplay"' in loader
    assert '"ANILIST", "PUNCHPLAY", "FLOPPY"' in ui
    assert 'provider: "punchplay", logo: "PUNCHPLAY"' in ui
    assert '"244,64,15", "0,132,216", "PUNCHPLAY"' in ui
    assert "--cw-auth-c1:244,64,15;--cw-auth-c2:0,132,216" in auth_ui
    assert "/assets/img/PUNCHPLAY.png" in auth_ui
    assert "punchplay:" in links
    assert "#sec-punchplay>.head" in providers_css
    assert "#page-settings #sec-punchplay>.head" in providers_css
    assert ".wl-provider-card.provider-punchplay" in providers_css

    core = (ROOT / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    assert 'key: "PUNCHPLAY", paths: [["punchplay"], ["auth", "punchplay"]], keys: ["access_token"]' in core

    flat_css = (ROOT / "assets" / "themes" / "flat.css").read_text(encoding="utf-8")
    assert flat_css.count("#punchplay_device_start") == flat_css.count("#mdblist_device_start")
    assert flat_css.count("#punchplay_device_start") > 0

    assert (ROOT / "assets" / "img" / "PUNCHPLAY.png").exists()
    assert not (ROOT / "assets" / "img" / "PUNCHPLAY.svg").exists()
    assert (ROOT / "assets" / "auth" / "auth.punchplay.js").exists()


def test_auth_html_has_no_credential_inputs() -> None:
    import providers.auth._auth_PUNCHPLAY as pp

    markup = pp.html()

    assert 'id="punchplay_device_start"' in markup
    assert 'id="punchplay_qc_code"' in markup
    assert 'id="punchplay_disconnect"' in markup
    assert "client_secret" not in markup
    assert "client_id" not in markup
