# CrossWatch test scripts
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]


@dataclass
class ResponseStub:
    status_code: int = 200
    payload: Any = None
    text: str = "{}"

    def json(self) -> Any:
        return self.payload if self.payload is not None else {}


def test_stremio_login_posts_official_payload_and_returns_auth_key() -> None:
    from providers.auth import _auth_STREMIO as stremio

    calls: list[dict[str, Any]] = []

    class SessionStub:
        def post(self, url: str, **kwargs: Any) -> ResponseStub:
            calls.append({"url": url, **kwargs})
            return ResponseStub(payload={"result": {"authKey": "auth-123", "user": {"id": "u1"}}})

    result = stremio.login("user@example.com", "secret", session=SessionStub())  # type: ignore[arg-type]

    assert result["auth_key"] == "auth-123"
    assert calls[0]["url"] == "https://api.strem.io/api/login"
    assert calls[0]["json"] == {
        "type": "Login",
        "email": "user@example.com",
        "password": "secret",
        "facebook": False,
    }


def test_stremio_connect_stores_only_auth_key_and_preserves_instances(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_STREMIO as stremio

    monkeypatch.setattr(stremio, "login", lambda email, password: {"auth_key": "auth-456", "user": {"email": email}})
    cfg: dict[str, Any] = {
        "stremio": {
            "auth_key": "old",
            "email": "old@example.com",
            "password": "old-password",
            "instances": {"kid": {"auth_key": "kid-key"}},
        }
    }

    result = stremio.StremioAuth().connect(cfg, email="user@example.com", password="secret")

    assert result["ok"] is True
    assert cfg["stremio"]["auth_key"] == "auth-456"
    assert cfg["stremio"]["instances"]["kid"]["auth_key"] == "kid-key"
    assert "email" not in cfg["stremio"]
    assert "password" not in cfg["stremio"]


def test_stremio_invalid_credentials_reason() -> None:
    from providers.auth import _auth_STREMIO as stremio

    class SessionStub:
        def post(self, *_args: Any, **_kwargs: Any) -> ResponseStub:
            return ResponseStub(status_code=401, payload={"error": "Unauthorized"})

    with pytest.raises(stremio.StremioAuthError) as exc:
        stremio.login("user@example.com", "bad", session=SessionStub())  # type: ignore[arg-type]

    assert exc.value.reason == "invalid_credentials"


def test_stremio_api_error_keeps_non_auth_reason() -> None:
    from providers.auth import _auth_STREMIO as stremio

    class SessionStub:
        def post(self, *_args: Any, **_kwargs: Any) -> ResponseStub:
            return ResponseStub(payload={"error": "method not found"})

    with pytest.raises(stremio.StremioAuthError) as exc:
        stremio._post_api("datastoreMeta", {"authKey": "key"}, session=SessionStub())  # type: ignore[arg-type]

    assert exc.value.reason == "request_failed"
    assert exc.value.endpoint == "datastoreMeta"
    assert exc.value.status_code == 200


def test_stremio_auth_key_is_redacted_and_normalized() -> None:
    from cw_platform.config_base import _normalize_stremio, redact_config

    cfg: dict[str, Any] = {
        "stremio": {
            "authKey": "secret-key",
            "email": "drop@example.com",
            "instances": {"alt": {"authKey": "alt-secret", "password": "drop"}},
        }
    }

    _normalize_stremio(cfg)

    assert cfg["stremio"]["auth_key"] == "secret-key"
    assert "authKey" not in cfg["stremio"]
    assert "email" not in cfg["stremio"]
    alt = cfg["stremio"]["instances"]["alt"]
    assert alt["auth_key"] == "alt-secret"
    assert "authKey" not in alt
    assert "password" not in alt
    redacted = redact_config(cfg)
    assert redacted["stremio"]["auth_key"] not in {"secret-key", ""}
    assert redacted["stremio"]["instances"]["alt"]["auth_key"] not in {"alt-secret", ""}


def test_stremio_auth_key_value_unwraps_encrypted_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_STREMIO as stremio

    monkeypatch.setattr(stremio, "_decrypt_secret", lambda value: "plain-key" if value == "enc:v1:test" else value)

    assert stremio.auth_key_value({"auth_key": "enc:v1:test"}) == "plain-key"


def test_stremio_auth_discovery_and_sync_registration() -> None:
    from cw_platform.modules_registry import MODULES
    from providers.auth.registry import auth_providers_html

    assert MODULES["AUTH"]["_auth_STREMIO"] == "providers.auth._auth_STREMIO"
    assert MODULES["SYNC"]["_mod_STREMIO"] == "providers.sync._mod_STREMIO"

    html = auth_providers_html()
    clients = html.index('id="sec-auth-clients"')
    assert html.index('id="sec-stremio"') > clients


def test_stremio_probe_key_is_profile_scoped() -> None:
    from api.probesAPI import _probe_key

    key = _probe_key("stremio", {"stremio": {"auth_key": "secret-key"}})

    assert key.startswith("stremio|auth:")
    assert key.endswith("|profile:default")


def test_stremio_ui_branding_and_local_logo() -> None:
    meta = (ROOT / "assets" / "helpers" / "provider-meta.js").read_text(encoding="utf-8")
    providers_css = (ROOT / "assets" / "css" / "providers.css").read_text(encoding="utf-8")
    loader = (ROOT / "assets" / "auth" / "auth_loader.js").read_text(encoding="utf-8")
    ui = (ROOT / "assets" / "helpers" / "providers-ui.js").read_text(encoding="utf-8")
    auth_ui = (ROOT / "assets" / "auth" / "auth.stremio.js").read_text(encoding="utf-8")
    logo = ROOT / "assets" / "img" / "STREMIO.png"

    assert 'STREMIO: { key: "STREMIO"' in meta
    assert 'logoFile: "STREMIO.png"' in meta
    assert ".prov-card.brand-stremio" in providers_css
    assert "/assets/img/STREMIO.png" in providers_css
    assert "--stremio-rgb:114,44,254" in providers_css
    assert 'stremio: "/assets/auth/auth.stremio.js"' in loader
    assert '{ id: "sec-auth-clients", title: "Media clients", keys: ["NUVIO", "KODI", "STREMIO"] }' in ui
    assert 'provider: "stremio", logo: "STREMIO"' in ui
    assert "password" in auth_ui
    assert "settings-collect" in auth_ui
    assert "/api/stremio/status?verify=1" in auth_ui
    assert logo.exists()
    assert logo.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")


def test_stremio_invalid_credentials_returns_handled_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    from api.authenticationAPI import register_auth
    from providers.auth import _auth_STREMIO as stremio

    cfg: dict[str, Any] = {"stremio": {"auth_key": ""}}
    monkeypatch.setattr("api.authenticationAPI.load_config", lambda: cfg)
    monkeypatch.setattr("api.authenticationAPI.save_config", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(stremio, "login", lambda *_args, **_kwargs: (_ for _ in ()).throw(stremio.StremioAuthError("bad", reason="invalid_credentials")))

    app = FastAPI()
    register_auth(app)
    res = TestClient(app).post("/api/stremio/connect", json={"email": "user@example.com", "password": "bad"})

    assert res.status_code == 200
    assert res.json()["ok"] is False
    assert res.json()["reason"] == "invalid_credentials"


def test_stremio_connect_validates_returned_auth_key(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.authenticationAPI as auth_api
    from api.authenticationAPI import register_auth
    from providers.auth import _auth_STREMIO as stremio

    saved: list[dict[str, Any]] = []
    cfg: dict[str, Any] = {"stremio": {"auth_key": ""}}

    class BadClient:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def validate(self) -> bool:
            raise stremio.StremioAuthError("bad", reason="invalid_credentials")

    monkeypatch.setattr("api.authenticationAPI.load_config", lambda: cfg)
    monkeypatch.setattr("api.authenticationAPI.save_config", lambda next_cfg, **_kwargs: saved.append(dict(next_cfg)))
    monkeypatch.setattr(stremio, "login", lambda *_args, **_kwargs: {"auth_key": "dead-key", "user": {}})
    monkeypatch.setattr(auth_api, "StremioClient", BadClient)

    app = FastAPI()
    register_auth(app)
    res = TestClient(app).post("/api/stremio/connect", json={"email": "user@example.com", "password": "secret"})

    assert res.status_code == 200
    assert res.json()["ok"] is False
    assert res.json()["reason"] == "invalid_credentials"
    assert saved == []
