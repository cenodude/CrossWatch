# CrossWatch BingeBase auth tests
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class _Resp:
    def __init__(self, status_code: int = 200, payload: Any = None) -> None:
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = json.dumps(self._payload)
        self.headers: dict[str, str] = {}

    def json(self) -> Any:
        return self._payload


def test_device_code_start_uses_bingebase_kodi_endpoint(monkeypatch) -> None:
    from providers.auth import _auth_BINGEBASE as auth

    cfg: dict[str, Any] = {"bingebase": {}}
    calls: list[dict[str, Any]] = []

    def fake_post(url: str, **kwargs: Any) -> _Resp:
        calls.append({"url": url, **kwargs})
        return _Resp(200, {"device_code": "dev-1", "user_code": "ABCD", "expires_in": 600, "interval": 7})

    monkeypatch.setattr(auth.requests, "post", fake_post)
    monkeypatch.setattr(auth, "_save_full_cfg", lambda c: None)

    res = auth.start_device_code(cfg, instance_id="default")

    assert res["ok"] is True
    assert calls[0]["url"] == auth.DEVICE_CODE_URL
    assert calls[0].get("data") == b""
    assert "json" not in calls[0], "BingeBase device-code start does not use OAuth client payload"
    assert cfg["bingebase"]["_pending_device"]["device_code"] == "dev-1"
    assert cfg["bingebase"]["_pending_device"]["interval"] == 7


def test_device_code_poll_stores_access_token_and_kodi_webhook(monkeypatch) -> None:
    from providers.auth import _auth_BINGEBASE as auth

    cfg: dict[str, Any] = {"bingebase": {"_pending_device": {"device_code": "dev-1", "expires_at": auth.now() + 60}}}
    calls: list[dict[str, Any]] = []

    def fake_post(url: str, **kwargs: Any) -> _Resp:
        calls.append({"url": url, **kwargs})
        return _Resp(200, {"access_token": "device-access", "username": "Lee"})

    monkeypatch.setattr(auth.requests, "post", fake_post)
    monkeypatch.setattr(auth, "_save_full_cfg", lambda c: None)

    res = auth.poll_device_code(cfg)

    assert res["ok"] is True
    assert res["generated_webhook_url"] == "https://bingebase.com/webhooks/kodi/device-access"
    assert calls[0]["url"] == auth.DEVICE_TOKEN_URL
    assert calls[0]["json"] == {"device_code": "dev-1"}
    assert cfg["bingebase"]["access_token"] == "device-access"
    assert cfg["bingebase"]["username"] == "Lee"
    assert cfg["bingebase"]["webhook_url"] == "https://bingebase.com/webhooks/kodi/device-access"
    assert "_pending_device" not in cfg["bingebase"]


def test_device_code_poll_preserves_custom_personal_webhook(monkeypatch) -> None:
    from providers.auth import _auth_BINGEBASE as auth

    custom_url = "https://bingebase.com/api/webhooks/jellyfin?token=personal-webhook"
    cfg: dict[str, Any] = {
        "bingebase": {
            "webhook_url": custom_url,
            "_pending_device": {"device_code": "dev-1", "expires_at": auth.now() + 60},
        }
    }

    def fake_post(url: str, **kwargs: Any) -> _Resp:
        return _Resp(200, {"access_token": "new-device-access"})

    monkeypatch.setattr(auth.requests, "post", fake_post)
    monkeypatch.setattr(auth, "_save_full_cfg", lambda c: None)

    res = auth.poll_device_code(cfg)

    assert res["ok"] is True
    assert "generated_webhook_url" not in res
    assert cfg["bingebase"]["access_token"] == "new-device-access"
    assert cfg["bingebase"]["webhook_url"] == custom_url


def test_status_backfills_kodi_webhook_for_existing_connection(monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from api.authenticationAPI import register_auth
    from providers.auth import _auth_BINGEBASE as auth

    cfg: dict[str, Any] = {"bingebase": {"access_token": "device-access", "webhook_url": ""}}
    saved: list[dict[str, Any]] = []

    monkeypatch.setattr("api.authenticationAPI.load_config", lambda: cfg)
    monkeypatch.setattr("api.authenticationAPI.save_config", lambda next_cfg, **_kwargs: saved.append(dict(next_cfg)))

    app = FastAPI()
    register_auth(app)
    res = TestClient(app).get("/api/bingebase/status")

    assert res.status_code == 200
    assert res.json()["connected"] is True
    assert res.json()["webhook_configured"] is True
    assert cfg["bingebase"]["webhook_url"] == auth.kodi_webhook_url("device-access")
    assert saved and saved[-1]["bingebase"]["webhook_url"] == auth.kodi_webhook_url("device-access")


def test_status_replaces_masked_webhook_with_kodi_webhook(monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from api.authenticationAPI import register_auth
    from providers.auth import _auth_BINGEBASE as auth

    cfg: dict[str, Any] = {"bingebase": {"access_token": "device-access", "webhook_url": "\u2022" * 8}}
    saved: list[dict[str, Any]] = []

    monkeypatch.setattr("api.authenticationAPI.load_config", lambda: cfg)
    monkeypatch.setattr("api.authenticationAPI.save_config", lambda next_cfg, **_kwargs: saved.append(dict(next_cfg)))

    app = FastAPI()
    register_auth(app)
    res = TestClient(app).get("/api/bingebase/status")

    assert res.status_code == 200
    assert res.json()["connected"] is True
    assert res.json()["webhook_configured"] is True
    assert cfg["bingebase"]["webhook_url"] == auth.kodi_webhook_url("device-access")
    assert saved and saved[-1]["bingebase"]["webhook_url"] == auth.kodi_webhook_url("device-access")


def test_bingebase_connected_state_and_redaction() -> None:
    from cw_platform.config_base import DEFAULT_CFG, redact_config
    from providers.auth import _auth_BINGEBASE as auth
    from providers.webhooks.config import sink_configured

    assert "bingebase" in DEFAULT_CFG
    assert "api_key" in DEFAULT_CFG["bingebase"]
    assert auth.is_configured({"access_token": "at"}) is True
    assert auth.is_configured({"webhook_url": "https://bingebase.com/api/webhooks/jellyfin?token=secret"}) is False
    assert auth.sink_configured({"webhook_url": "\u2022" * 8}) is False
    assert sink_configured({"bingebase": {"webhook_url": "https://bingebase.com/api/webhooks/jellyfin?token=secret"}}, "bingebase", "default") is True

    redacted = redact_config({
        "bingebase": {
            "access_token": "at",
            "webhook_url": "https://bingebase.com/api/webhooks/jellyfin?token=secret",
            "api_key": "bearer-secret",
            "_pending_device": {"device_code": "dev"},
        }
    })
    text = json.dumps(redacted)
    assert "secret" not in text
    assert "device_code" not in text


def test_bingebase_runtime_registry_and_frontend_metadata() -> None:
    from providers.auth import runtime

    root = Path(__file__).resolve().parents[1]
    meta = (root / "assets" / "helpers" / "provider-meta.js").read_text(encoding="utf-8")
    core = (root / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    loader = (root / "assets" / "auth" / "auth_loader.js").read_text(encoding="utf-8")
    modules = (root / "cw_platform" / "modules_registry.py").read_text(encoding="utf-8")

    assert runtime.is_configured("bingebase", {"access_token": "at"}) is True
    assert 'BINGEBASE: { key: "BINGEBASE"' in meta
    assert 'scrobblerSink: true' in meta.split("BINGEBASE", 1)[1].split("SCROB", 1)[0]
    assert 'scrobbleOnly: true' in meta.split("BINGEBASE", 1)[1].split("SCROB", 1)[0]
    assert 'syncSurface: false' in meta.split("BINGEBASE", 1)[1].split("SCROB", 1)[0]
    assert 'watchlist: false' in meta.split("BINGEBASE", 1)[1].split("SCROB", 1)[0]
    assert 'keys: ["access_token", "webhook_url"]' in core
    assert "auth.bingebase.js" in loader
    assert "_auth_BINGEBASE" in modules
    assert "_mod_BINGEBASE" not in modules


def test_bingebase_is_not_rendered_as_sync_statistics_provider() -> None:
    root = Path(__file__).resolve().parents[1]
    meta = (root / "assets" / "helpers" / "provider-meta.js").read_text(encoding="utf-8")
    insights = (root / "assets" / "js" / "insights.js").read_text(encoding="utf-8")

    assert "function syncSurfaceProvider(v){ return get(v)?.syncSurface !== false; }" in meta
    assert "const providerOnSyncSurface = v =>" in insights
    assert ".filter(k => providerOnSyncSurface(k))" in insights


def test_bingebase_modal_layout_uses_tracker_connection_shell() -> None:
    from providers.auth import _auth_BINGEBASE as auth

    root = Path(__file__).resolve().parents[1]
    html = auth.html()
    ui = (root / "assets" / "helpers" / "providers-ui.js").read_text(encoding="utf-8")
    css = (root / "assets" / "css" / "auth-providers.css").read_text(encoding="utf-8")

    auth_panel = html.index('<div class="cw-subpanel active" data-sub="auth">')
    realtime_panel = html.index('<div id="bingebase_realtime_panel"')
    actions = html.index('<div class="bb-actions">')

    assert auth_panel < realtime_panel < actions
    assert 'data-sub="webhook"' not in html
    assert '<button type="button" class="cw-subtile" data-sub="webhook">' not in html
    assert 'id="bingebase_webhook_save"' not in html
    assert 'id="bingebase_webhook_url"' in html
    assert 'id="bingebase_api_key"' in html
    assert "bb-realtime-grid" in html
    assert ".bb-realtime-panel{display:grid;gap:10px;margin-top:-8px}" in html
    assert "BingeBase webhook URL (optional override)" in html
    assert "Click Connect BingeBase and approve the code at bingebase.com/activate.<br>BingeBase only provides scrobble." in html
    assert "Device auth fills automatically the realtime webhook." in html
    assert "Auto-filled from Kodi device auth" in html
    assert 'placeholder="https://bingebase.com/api/webhooks/jellyfin?token=..."' not in html
    assert "Webhook API key / bearer token (optional)" in html
    assert "Optional Authorization bearer token" in html
    assert "Optional Authorization bearer token for the realtime webhook POST." not in html
    bb_ui = ui.split("BINGEBASE:", 1)[1].split("PUBLICMETADB:", 1)[0]
    assert 'tabs: { auth: ["lock", "Authentication", "Connect with a device code"] }' in bb_ui
    assert "BingeBase Realtime" not in bb_ui
    assert "BingeBase only provides scrobble." in bb_ui
    assert "Device auth also prepares Kodi realtime scrobbling" not in bb_ui
    assert "meta?.scrobbleOnly === true" in ui
    assert "cw-auth-scrobble-only-badge" in ui
    assert "Scrobble only (Watcher or Webhook)" in ui
    assert ".cw-auth-scrobble-only-badge" in css
    assert "left:12px;top:8px" in css
    assert 'order: ["#bingebase_realtime_panel", ".bb-actions", "#bingebase_device_panel"]' in ui
    assert 'allowSaveWithoutConnection: true' in ui.split("BINGEBASE:", 1)[1].split("PUBLICMETADB:", 1)[0]
    assert "bingebase_webhook_save" not in ui.split("BINGEBASE:", 1)[1].split("PUBLICMETADB:", 1)[0]


def test_bingebase_webhook_stages_until_global_modal_save() -> None:
    root = Path(__file__).resolve().parents[1]
    auth_js = (root / "assets" / "auth" / "auth.bingebase.js").read_text(encoding="utf-8")
    api_py = (root / "api" / "authenticationAPI.py").read_text(encoding="utf-8")

    assert 'document.addEventListener("settings-collect"' in auth_js
    assert "collectRealtime" in auth_js
    assert "function revealDeviceWebhook" in auth_js
    assert 'wh.type = "text"' in auth_js
    assert 'wh.type = "password"' in auth_js
    assert "revealDeviceWebhook(data?.generated_webhook_url)" in auth_js
    assert "if (state.masked) return;" in auth_js
    assert "block[key] = state.value || \"\";" in auth_js
    assert 'collectSecretField(cfg, "bingebase_api_key", "api_key")' in auth_js
    assert "window.invalidateConfigCache?.();" in auth_js
    assert "void hydrate().then(() => revealDeviceWebhook(data?.generated_webhook_url));" in auth_js
    assert "/api/bingebase/webhook/save" not in auth_js
    assert "bingebase_webhook_save" not in auth_js
    assert "/api/bingebase/webhook/save" not in api_py
    assert 'confirm("Delete BingeBase connection?")' not in auth_js
    assert "Connected. Add webhook URL for realtime scrobbling." not in auth_js


def test_bingebase_device_code_uses_forwarding_page() -> None:
    root = Path(__file__).resolve().parents[1]
    auth_js = (root / "assets" / "auth" / "auth.bingebase.js").read_text(encoding="utf-8")

    assert "function writeForwardingPage" in auth_js
    assert 'window.open("about:blank", "_blank")' in auth_js
    assert "Opening the BingeBase approval page" in auth_js
    assert "cw-bb-count" in auth_js
    assert "Redirecting in <span" in auth_js
    assert "location.href=target" in auth_js
    assert "window.open(VERIFY_URL" not in auth_js
