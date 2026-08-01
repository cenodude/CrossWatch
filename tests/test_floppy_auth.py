# CrossWatch test scripts
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest


ROOT = Path(__file__).resolve().parents[1]


@dataclass
class ResponseStub:
    status_code: int = 200
    payload: Any = None

    def json(self) -> Any:
        return self.payload if self.payload is not None else {}


class SessionStub:
    def __init__(self, response: ResponseStub) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> ResponseStub:
        self.calls.append({"method": method, "url": url, **kwargs})
        return self.response


def test_floppy_validation_uses_authenticated_lists_probe() -> None:
    from providers.auth import _auth_FLOPPY as floppy

    session = SessionStub(ResponseStub(payload={"results": []}))

    ok, reason = floppy.validate_credentials("floppy.local/", "tok-123", verify_ssl=False, session=session)  # type: ignore[arg-type]

    assert ok is True
    assert reason == ""
    assert session.calls[0]["method"] == "GET"
    assert session.calls[0]["url"] == "http://floppy.local/api/v1/lists"
    assert session.calls[0]["headers"]["Authorization"] == "Bearer tok-123"
    assert session.calls[0]["headers"]["Accept"] == "application/json"
    assert session.calls[0]["headers"]["User-Agent"] == "CrossWatch/1.0"
    assert session.calls[0]["params"] == {"limit": 1}
    assert session.calls[0]["verify"] is False


def test_floppy_invalid_token_maps_to_clear_reason() -> None:
    from providers.auth import _auth_FLOPPY as floppy

    ok, reason = floppy.validate_credentials("https://floppy.local", "bad", session=SessionStub(ResponseStub(status_code=401)))  # type: ignore[arg-type]

    assert ok is False
    assert reason == "invalid_api_token"


def test_floppy_missing_url_or_token_is_rejected() -> None:
    from providers.auth import _auth_FLOPPY as floppy

    assert floppy.validate_credentials("", "tok") == (False, "server_url_required")
    assert floppy.validate_credentials("https://floppy.local", "") == (False, "api_token_required")


def test_floppy_disconnect_clears_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    from api.authenticationAPI import register_auth

    cfg: dict[str, Any] = {"floppy": {"server_url": "https://floppy.local", "api_token": "secret", "verify_ssl": True}}
    saved: list[dict[str, Any]] = []
    monkeypatch.setattr("api.authenticationAPI.load_config", lambda: cfg)
    monkeypatch.setattr("api.authenticationAPI.save_config", lambda next_cfg, **_kwargs: saved.append(next_cfg.copy()))

    app = FastAPI()
    register_auth(app)
    res = TestClient(app).post("/api/floppy/disconnect")

    assert res.status_code == 200
    assert res.json()["ok"] is True
    assert cfg["floppy"]["server_url"] == ""
    assert cfg["floppy"]["api_token"] == ""
    assert saved


def test_floppy_instances_are_isolated(monkeypatch: pytest.MonkeyPatch) -> None:
    import providers.auth._auth_FLOPPY as floppy
    from api.authenticationAPI import register_auth

    cfg: dict[str, Any] = {
        "floppy": {
            "server_url": "https://default.local",
            "api_token": "default-token",
            "verify_ssl": True,
        }
    }
    saved: list[dict[str, Any]] = []
    monkeypatch.setattr("api.authenticationAPI.load_config", lambda: cfg)
    monkeypatch.setattr("api.authenticationAPI.save_config", lambda next_cfg, **_kwargs: saved.append(next_cfg.copy()))
    monkeypatch.setattr(floppy, "validate_credentials", lambda *_args, **_kwargs: (True, ""))

    app = FastAPI()
    register_auth(app)
    res = TestClient(app).post(
        "/api/floppy/save?instance=second",
        json={"server_url": "second.local/", "api_token": "second-token", "verify_ssl": False},
    )

    assert res.status_code == 200
    assert res.json()["ok"] is True
    assert cfg["floppy"]["server_url"] == "https://default.local"
    assert cfg["floppy"]["api_token"] == "default-token"
    second = cfg["floppy"]["instances"]["second"]
    assert second["server_url"] == "http://second.local"
    assert second["api_token"] == "second-token"
    assert second["verify_ssl"] is False
    assert saved


def test_floppy_invalid_save_does_not_persist(monkeypatch: pytest.MonkeyPatch) -> None:
    import providers.auth._auth_FLOPPY as floppy
    from api.authenticationAPI import register_auth

    cfg: dict[str, Any] = {"floppy": {"server_url": "", "api_token": ""}}
    saved: list[dict[str, Any]] = []
    monkeypatch.setattr("api.authenticationAPI.load_config", lambda: cfg)
    monkeypatch.setattr("api.authenticationAPI.save_config", lambda next_cfg, **_kwargs: saved.append(next_cfg.copy()))
    monkeypatch.setattr(floppy, "validate_credentials", lambda *_args, **_kwargs: (False, "invalid_api_token"))

    app = FastAPI()
    register_auth(app)
    res = TestClient(app).post("/api/floppy/save", json={"server_url": "https://floppy.local", "api_token": "bad"})

    assert res.status_code == 200
    assert res.json()["ok"] is False
    assert res.json()["error"] == "invalid_api_token"
    assert cfg["floppy"]["server_url"] == ""
    assert cfg["floppy"]["api_token"] == ""
    assert saved == []


def test_floppy_auth_discovery_branding_and_secret_redaction() -> None:
    from cw_platform.config_base import redact_config
    from providers.auth.registry import auth_providers_html

    html = auth_providers_html()
    trackers = html.index('id="sec-auth-trackers"')
    assert html.index('id="sec-floppy"') > trackers

    meta = (ROOT / "assets" / "helpers" / "provider-meta.js").read_text(encoding="utf-8")
    providers_css = (ROOT / "assets" / "css" / "providers.css").read_text(encoding="utf-8")
    auth_css = (ROOT / "assets" / "css" / "auth-providers.css").read_text(encoding="utf-8")
    loader = (ROOT / "assets" / "auth" / "auth_loader.js").read_text(encoding="utf-8")
    ui = (ROOT / "assets" / "helpers" / "providers-ui.js").read_text(encoding="utf-8")
    auth_ui = (ROOT / "providers" / "auth" / "_auth_FLOPPY.py").read_text(encoding="utf-8")
    assert 'FLOPPY: { key: "FLOPPY"' in meta
    assert 'rgb: "245,101,30"' in meta
    assert ".prov-card.brand-floppy" in providers_css
    assert "/assets/img/FLOPPY.png" in providers_css
    assert "--floppy-rgb:245,101,30" in providers_css
    assert "--floppy-cyan-rgb:4,181,220" in providers_css
    assert "--floppy-purple-rgb:203,100,229" in providers_css
    assert "#floppy_disconnect" in auth_css
    assert 'floppy: "/assets/auth/auth.floppy.js"' in loader
    assert 'provider: "floppy", logo: "FLOPPY"' in ui
    assert '"245,101,30", "4,181,220", "FLOPPY"' in ui
    assert "--cw-auth-c1:245,101,30;--cw-auth-c2:4,181,220" in auth_ui
    assert "#f5651e" in auth_ui and "#04b5dc" in auth_ui
    assert (ROOT / "assets" / "img" / "FLOPPY.png").exists()

    redacted = redact_config({"floppy": {"server_url": "https://floppy.local", "api_token": "secret"}})
    assert redacted["floppy"]["server_url"] == "https://floppy.local"
    assert redacted["floppy"]["api_token"] not in {"secret", ""}


def test_floppy_probe_key_and_status_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    import providers.auth._auth_FLOPPY as floppy
    from api.probesAPI import PROBE_DETAIL_CACHE, STATUS_CACHE, _probe_key, register_probes

    cfg = {"floppy": {"server_url": "https://floppy.local", "api_token": "secret", "verify_ssl": False}}
    key = _probe_key("floppy", cfg)

    assert key.startswith("floppy|srv:")
    assert "|key:" in key

    STATUS_CACHE["ts"] = 0.0
    STATUS_CACHE["data"] = None
    PROBE_DETAIL_CACHE.clear()
    monkeypatch.setattr(floppy, "validate_credentials", lambda *_args, **_kwargs: (True, ""))

    app = FastAPI()
    register_probes(app, lambda: cfg)
    res = TestClient(app).get("/api/status?fresh=1")
    body = res.json()

    assert res.status_code == 200
    assert body["floppy_connected"] is True
    assert body["providers"]["FLOPPY"]["connected"] is True
    assert body["providers"]["FLOPPY"]["experimental"] is True
