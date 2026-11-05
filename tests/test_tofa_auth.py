# CrossWatch test scripts
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
SERVER = "http://tofa.local:33333"
CLOUD = "https://api.tofa.tv"


@dataclass
class ResponseStub:
    status_code: int = 200
    payload: Any = None
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def text(self) -> str:
        return "" if self.payload is None else "x"

    def json(self) -> Any:
        if self.payload is None:
            raise ValueError("no body")
        return self.payload


class FakeSession:
    def __init__(self, responses: list[ResponseStub] | None = None) -> None:
        self.responses = list(responses or [])
        self.calls: list[dict[str, Any]] = []

    def _next(self, method: str, url: str, kwargs: dict[str, Any]) -> ResponseStub:
        self.calls.append({"method": method, "url": url, **kwargs})
        if not self.responses:
            raise AssertionError(f"unexpected {method} {url}")
        return self.responses.pop(0)

    def get(self, url: str, **kwargs: Any) -> ResponseStub:
        return self._next("GET", url, kwargs)

    def post(self, url: str, **kwargs: Any) -> ResponseStub:
        return self._next("POST", url, kwargs)

    def request(self, method: str, url: str, **kwargs: Any) -> ResponseStub:
        return self._next(method.upper(), url, kwargs)


def _err(code: str, status: int) -> ResponseStub:
    return ResponseStub(status, {"error": code, "message": code})


def _store(monkeypatch: pytest.MonkeyPatch, cfg: dict[str, Any]) -> dict[str, Any]:
    from providers.auth import _auth_TOFA as tofa

    holder = {"cfg": cfg}

    def save(new_cfg: dict[str, Any]) -> None:
        holder["cfg"] = new_cfg

    monkeypatch.setattr(tofa, "save_config", save)
    monkeypatch.setattr(tofa, "load_config", lambda: holder["cfg"])
    monkeypatch.setattr(tofa, "_cloud_limiter", lambda: type("L", (), {"wait": staticmethod(lambda key: 0.0)})())
    return holder


def _connected(**extra: Any) -> dict[str, Any]:
    block = {
        "server_url": SERVER,
        "server_id": "srv-1",
        "connect_url": CLOUD,
        "verify_ssl": False,
        "access_token": "access-1",
        "refresh_token": "refresh-1",
        "expires_at": 4_000_000_000,
        "profile_id": "p-1",
        "profile_name": "Alice",
        "profile_locked": False,
        "profile_token": "",
    }
    block.update(extra)
    return {"tofa": block}


def _profiles(*rows: tuple[str, str, bool]) -> ResponseStub:
    return ResponseStub(200, [{"id": pid, "name": name, "is_primary": i == 0, "is_locked": locked, "is_kids": False}
                              for i, (pid, name, locked) in enumerate(rows)])


def test_url_rules_and_connect_host_allowlist() -> None:
    from providers.auth import _auth_TOFA as tofa

    assert tofa.normalize_server_url("192.168.1.5:33333/api/v1/") == "http://192.168.1.5:33333"
    assert tofa.normalize_server_url("ftp://x") == "" and tofa.normalize_server_url("http://u:p@x") == ""
    assert tofa.normalize_connect_url("https://api.tofa.tv/") == CLOUD
    for bad in ("http://api.tofa.tv", "https://evil.example", "https://tofa.tv.evil.example", "https://u:p@api.tofa.tv"):
        assert tofa.normalize_connect_url(bad) == ""
    assert tofa.connect_url_of({"connect_url": "https://evil.example"}) == CLOUD


@pytest.mark.parametrize(("resp", "error"), [
    (ResponseStub(200, {"claimed": False, "server_id": "srv-1", "connect_url": CLOUD}), "server_unclaimed"),
    (ResponseStub(200, {"claimed": True, "server_id": "srv-1", "connect_url": "https://evil.example"}), "invalid_connect_url"),
    (ResponseStub(200, {"state": "available"}), "not_tofa"),
    (ResponseStub(404, None), "not_tofa"),
])
def test_check_server_rejects_unusable_servers(resp: ResponseStub, error: str) -> None:
    from providers.auth import _auth_TOFA as tofa

    with pytest.raises(tofa.TofaServerError, match=error):
        tofa.check_server(SERVER, session=FakeSession([resp]))


def test_device_start_stores_pending_and_returns_qr(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    cfg: dict[str, Any] = {}
    _store(monkeypatch, cfg)
    session = FakeSession([
        ResponseStub(200, {"claimed": True, "server_id": "srv-1", "connect_url": CLOUD}),
        ResponseStub(200, {"device_code": "dc-secret", "user_code": "ABCD-EFGH", "verification_uri": "https://app.tofa.tv/link",
                           "verification_uri_complete": "https://app.tofa.tv/link?code=ABCDEFGH", "expires_in": 600, "interval": 5,
                           "qr_code_svg": "<svg/>"}),
    ])
    res = tofa.TofaClient(cfg, session=session).start_device_login(cfg, server_url="tofa.local:33333")

    assert session.calls[0]["url"] == f"{SERVER}/api/v1/auth/status"
    assert session.calls[1]["url"] == f"{CLOUD}/device/code"
    assert session.calls[1]["json"] == {"server_id": "srv-1", "client_name": "CrossWatch", "client_type": "script"}
    assert res["user_code"] == "ABCD-EFGH" and res["qr_code_svg"] == "<svg/>" and "device_code" not in res
    block = cfg["tofa"]
    assert block["server_id"] == "srv-1" and block["_pending_device"]["device_code"] == "dc-secret"
    assert "qr_code_svg" not in block["_pending_device"]


def test_server_change_requires_disconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    cfg = _connected()
    _store(monkeypatch, cfg)
    session = FakeSession([ResponseStub(200, {"claimed": True, "server_id": "srv-2", "connect_url": CLOUD})])
    with pytest.raises(tofa.TofaServerError, match="disconnect_first"):
        tofa.TofaClient(cfg, session=session).configure_server(cfg, "http://other:33333")


def test_poll_pending_slow_down_then_approved_auto_selects_single_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    cfg = {"tofa": {"server_url": SERVER, "server_id": "srv-1", "_pending_device": {"device_code": "dc", "user_code": "ABCD-EFGH", "expires_at": 4_000_000_000, "interval": 5}}}
    _store(monkeypatch, cfg)
    session = FakeSession([
        _err("authorization_pending", 400),
        _err("slow_down", 400),
        ResponseStub(200, {"access_token": "a1", "refresh_token": "r1", "token_type": "Bearer", "expires_in": 2592000}),
        _profiles(("p-1", "Alice", False)),
        ResponseStub(200, {"id": "u-1", "username": "alice", "is_admin": True}),
    ])
    client = tofa.TofaClient(cfg, session=session)

    assert client.poll_device_login(cfg)["status"] == "pending"
    assert client.poll_device_login(cfg) == {"ok": False, "status": "slow_down", "interval": 10, "instance": "default"}
    res = client.poll_device_login(cfg)

    assert session.calls[0]["url"] == f"{CLOUD}/device/token"
    assert session.calls[0]["data"] == {"grant_type": "urn:ietf:params:oauth:grant-type:device_code", "device_code": "dc"}
    assert res["status"] == "authorized"
    block = cfg["tofa"]
    assert block["access_token"] == "a1" and block["profile_id"] == "p-1" and block["username"] == "alice"
    assert "_pending_device" not in block and tofa.is_configured(block)
    assert session.calls[4]["headers"]["X-Profile-Id"] == "p-1"


def test_poll_approved_with_locked_profile_leaves_profile_unselected(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    cfg = {"tofa": {"server_url": SERVER, "server_id": "srv-1", "_pending_device": {"device_code": "dc", "user_code": "X", "expires_at": 4_000_000_000}}}
    _store(monkeypatch, cfg)
    session = FakeSession([ResponseStub(200, {"access_token": "a1", "refresh_token": "r1", "expires_in": 2592000}), _profiles(("p-1", "Alice", True))])
    res = tofa.TofaClient(cfg, session=session).poll_device_login(cfg)
    assert res["profiles"][0]["has_pin"] is True and cfg["tofa"]["profile_id"] == ""
    assert tofa.is_configured(cfg["tofa"]) is False


@pytest.mark.parametrize(("code", "status"), [("expired_token", "expired"), ("access_denied", "denied")])
def test_poll_terminal_states_clear_pending(monkeypatch: pytest.MonkeyPatch, code: str, status: str) -> None:
    from providers.auth import _auth_TOFA as tofa

    cfg = {"tofa": {"server_url": SERVER, "server_id": "srv-1", "_pending_device": {"device_code": "dc", "user_code": "X", "expires_at": 4_000_000_000}}}
    _store(monkeypatch, cfg)
    res = tofa.TofaClient(cfg, session=FakeSession([_err(code, 400)])).poll_device_login(cfg)
    assert res["status"] == status and "_pending_device" not in cfg["tofa"]


def test_refresh_skips_fresh_token_and_stores_returned_pair(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    cfg = _connected()
    holder = _store(monkeypatch, cfg)
    assert tofa.TofaClient(cfg, session=FakeSession()).refresh_token(cfg)["status"] == "fresh"

    session = FakeSession([ResponseStub(200, {"access_token": "access-2", "refresh_token": "refresh-2", "expires_in": 2592000})])
    res = tofa.TofaClient(cfg, session=session).refresh_token(cfg, force=True)
    assert res["ok"] is True
    assert session.calls[0]["url"] == f"{CLOUD}/servers/srv-1/device-token/refresh"
    assert session.calls[0]["json"] == {"refresh_token": "refresh-1"}
    assert holder["cfg"]["tofa"]["access_token"] == "access-2" and holder["cfg"]["tofa"]["refresh_token"] == "refresh-2"


def test_refresh_keeps_refresh_token_when_not_rotated_and_flags_rejection(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    cfg = _connected()
    holder = _store(monkeypatch, cfg)
    tofa.TofaClient(cfg, session=FakeSession([ResponseStub(200, {"access_token": "access-2", "expires_in": 60})])).refresh_token(cfg, force=True)
    assert holder["cfg"]["tofa"]["refresh_token"] == "refresh-1"

    res = tofa.TofaClient(cfg, session=FakeSession([_err("invalid_grant", 401)])).refresh_token(cfg, force=True)
    assert res["reconnect_required"] is True and holder["cfg"]["tofa"]["auth_error"] == "reconnect_required"
    assert tofa.status_for_block(holder["cfg"]["tofa"])["connected"] is False


def test_refresh_keeps_session_on_cloud_outage(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    cfg = _connected()
    holder = _store(monkeypatch, cfg)
    res = tofa.TofaClient(cfg, session=FakeSession([ResponseStub(503, None, {"Retry-After": "30"})])).refresh_token(cfg, force=True)
    assert res == {"ok": False, "status": "service_unavailable", "retry_after": 30, "instance": "default"}
    assert holder["cfg"]["tofa"].get("auth_error", "") == ""


def test_request_sends_profile_headers_and_retries_once_after_401(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    cfg = _connected(profile_locked=True, profile_token="ptok", profile_token_expires_at=4_000_000_000)
    _store(monkeypatch, cfg)
    session = FakeSession([
        _err("unauthorized", 401),
        ResponseStub(200, {"access_token": "access-2", "refresh_token": "refresh-2", "expires_in": 2592000}),
        ResponseStub(200, {"id": "u-1"}),
    ])
    resp = tofa.TofaClient(cfg, session=session).request("GET", "/users/me")
    assert resp.status_code == 200
    assert session.calls[0]["headers"]["X-Profile-Token"] == "ptok"
    assert session.calls[2]["headers"]["Authorization"] == "Bearer access-2"


def test_expired_unlock_token_on_unlocked_profile_is_dropped_and_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    cfg = _connected(profile_locked=True, profile_token="old", profile_token_expires_at=4_000_000_000)
    holder = _store(monkeypatch, cfg)
    session = FakeSession([_err("profile_unlock_expired", 401), _profiles(("p-1", "Alice", False)), ResponseStub(200, {"id": "u-1"})])
    resp = tofa.TofaClient(cfg, session=session).request("GET", "/users/me")
    assert resp.status_code == 200
    assert "X-Profile-Token" not in session.calls[2]["headers"]
    assert holder["cfg"]["tofa"]["profile_locked"] is False and holder["cfg"]["tofa"]["profile_token"] == ""


def test_locked_profile_without_valid_unlock_flags_pin_required(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    cfg = _connected(profile_locked=True, profile_token="old", profile_token_expires_at=4_000_000_000)
    holder = _store(monkeypatch, cfg)
    session = FakeSession([_err("profile_unlock_expired", 401), _profiles(("p-1", "Alice", True))])
    with pytest.raises(tofa.TofaProfileLocked):
        tofa.TofaClient(cfg, session=session).request("GET", "/users/me")
    assert holder["cfg"]["tofa"]["auth_error"] == "profile_pin_required"
    assert tofa.status_for_block(holder["cfg"]["tofa"])["profile_pin_required"] is True

    locked = _connected(profile_locked=True, profile_token="ptok", profile_token_expires_at=1)["tofa"]
    assert tofa.needs_profile_pin(locked) is True


def test_select_pin_profile_remembers_device_and_stores_token(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    cfg = _connected(profile_id="", profile_name="")
    holder = _store(monkeypatch, cfg)
    session = FakeSession([
        _profiles(("p-1", "Alice", True)),
        ResponseStub(200, {"profile_token": "ptok", "expires_at": "2026-12-04T22:17:05.387770378Z"}),
        ResponseStub(200, {"id": "u-1", "username": "alice", "is_admin": False}),
    ])
    selected = tofa.TofaClient(cfg, session=session).select_profile(cfg, "p-1", "3113")

    assert selected["id"] == "p-1"
    assert session.calls[1]["json"] == {"pin": "3113", "remember_device": True}
    block = holder["cfg"]["tofa"]
    assert block["profile_token"] == "ptok" and block["profile_locked"] is True
    assert block["profile_token_expires_at"] == tofa.parse_epoch("2026-12-04T22:17:05Z")
    assert "pin" not in block and "3113" not in str(block)


@pytest.mark.parametrize(("responses", "pin", "error"), [
    ([_profiles(("p-1", "Alice", True))], "", "pin_required"),
    ([_profiles(("p-1", "Alice", True)), _err("invalid_pin", 401)], "0000", "invalid_pin"),
    ([_profiles(("p-2", "Bob", False))], "", "profile_unavailable"),
])
def test_select_profile_errors(monkeypatch: pytest.MonkeyPatch, responses: list[ResponseStub], pin: str, error: str) -> None:
    from providers.auth import _auth_TOFA as tofa

    cfg = _connected(profile_id="")
    _store(monkeypatch, cfg)
    with pytest.raises(tofa.TofaError, match=error):
        tofa.TofaClient(cfg, session=FakeSession(responses)).select_profile(cfg, "p-1", pin)


def test_request_with_auth_resolves_named_profile_from_narrowed_view(monkeypatch: pytest.MonkeyPatch) -> None:
    from cw_platform.provider_instances import build_provider_config_view
    from providers.auth import _auth_TOFA as tofa

    cfg = {"tofa": {"server_url": "", "instances": {"TOFA-P01": _connected()["tofa"]}}}
    _store(monkeypatch, cfg)
    view = build_provider_config_view(cfg, "tofa", "TOFA-P01")
    session = FakeSession([ResponseStub(200, {"items": []})])

    def call(sess: Any, method: str, url: str, **kwargs: Any) -> ResponseStub:
        return sess.request(method, url, **kwargs)

    resp = tofa.request_with_auth(session, "GET", f"{SERVER}/api/v1/users/me/watch-state", cfg=view, instance_id="TOFA-P01", request_func=call)
    assert resp.status_code == 200
    assert session.calls[0]["headers"]["Authorization"] == "Bearer access-1" and session.calls[0]["headers"]["X-Profile-Id"] == "p-1"
    assert tofa.provider_block(cfg, "TOFA-P99") == {}


def test_disconnect_clears_session(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    cfg = _connected(profile_token="ptok", _pending_device={"device_code": "dc", "user_code": "X"})
    holder = _store(monkeypatch, cfg)
    tofa.TofaClient(cfg, session=FakeSession()).disconnect(cfg)
    block = holder["cfg"]["tofa"]
    assert block["access_token"] == "" and block["refresh_token"] == "" and block["profile_token"] == ""
    assert "_pending_device" not in block and block["server_url"] == SERVER


def test_keepalive_refreshes_only_stale_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    import time as _time

    from cw_platform import token_refresh
    from providers.auth import _auth_TOFA as tofa

    assert token_refresh.PROVIDERS["TOFA"] == "providers.auth._auth_TOFA"
    cfg = _connected(session_refreshed_at=int(_time.time()))
    cfg["tofa"]["instances"] = {"TOFA-P01": {**_connected()["tofa"], "session_refreshed_at": 1}}
    _store(monkeypatch, cfg)
    forced: list[Any] = []
    monkeypatch.setattr(tofa.TofaClient, "refresh_token", lambda self, c, force=False, **k: forced.append((self.instance_id, force)) or {"ok": True, "status": "ok"})
    assert tofa.refresh_all() == {"TOFA-P01": "ok"}
    assert forced == [("TOFA-P01", True)]


def test_default_config_redaction_and_registration() -> None:
    from cw_platform.config_base import DEFAULT_CFG, redact_config
    from cw_platform.modules_registry import MODULES
    from providers.auth.runtime import _backend

    assert set(DEFAULT_CFG["tofa"]) == {"server_url", "verify_ssl", "access_token", "refresh_token", "expires_at", "profile_id", "profile_name"}
    assert DEFAULT_CFG["tofa"]["verify_ssl"] is False
    cfg = _connected(profile_token="ptok", _pending_device={"device_code": "dc", "user_code": "1"})
    red = redact_config(copy.deepcopy(cfg))["tofa"]
    for key in ("access_token", "refresh_token", "profile_token", "_pending_device"):
        assert red[key] != cfg["tofa"][key]
    assert red["server_url"] == SERVER and red["profile_id"] == "p-1"
    assert MODULES["AUTH"]["_auth_TOFA"] == "providers.auth._auth_TOFA"
    assert _backend("tofa").__name__ == "providers.auth._auth_TOFA"


def test_auth_section_is_grouped_with_media_servers() -> None:
    from providers.auth.registry import auth_providers_html

    html = auth_providers_html()
    assert html.index('id="sec-auth-media"') < html.index('id="sec-tofa"') < html.index('id="sec-auth-trackers"')


def test_probe_registration_key_opacity_and_reasons(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import probesAPI as probes
    from providers.auth import _auth_TOFA as tofa

    assert "tofa" in probes.PROVIDERS and probes.PROBE_CFG_KEY["TOFA"] == "tofa"
    assert probes.DETAIL_PROBES["TOFA"] is probes._probe_tofa_detail
    assert probes._probe_key("tofa", {"tofa": {}}) == "tofa|unconfigured"
    key = probes._probe_key("tofa", _connected())
    assert "access-1" not in key and "refresh-1" not in key and SERVER not in key
    assert probes._prov_configured(_connected(), "TOFA", "default") is True

    probes.invalidate_provider_caches("tofa")
    assert probes._probe_tofa_detail({"tofa": {}}, max_age_sec=0) == (False, "Tofa: missing authentication")
    assert probes._probe_tofa_detail(_connected(auth_error="reconnect_required"), max_age_sec=0) == (False, "Tofa: reconnect required")
    probes.invalidate_provider_caches("tofa")
    expired = _connected(profile_locked=True, profile_token="ptok", profile_token_expires_at=1)
    assert probes._probe_tofa_detail(expired, max_age_sec=0) == (False, "Tofa: profile PIN required")

    monkeypatch.setattr(tofa.TofaClient, "list_profiles", lambda self: [{"id": "p-1", "name": "Alice", "has_pin": False}])
    monkeypatch.setattr(tofa.TofaClient, "account", lambda self: {"id": "u-1"})
    probes.invalidate_provider_caches("tofa")
    assert probes._probe_tofa_detail(_connected(), max_age_sec=0) == (True, "")
    probes.invalidate_provider_caches("tofa")
    assert probes._probe_tofa_detail(_connected(profile_id="gone"), max_age_sec=0) == (False, "Tofa: profile unavailable")


def test_frontend_wiring_and_brand_assets() -> None:
    meta = (ROOT / "assets" / "helpers" / "provider-meta.js").read_text(encoding="utf-8")
    loader = (ROOT / "assets" / "auth" / "auth_loader.js").read_text(encoding="utf-8")
    core = (ROOT / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    ui = (ROOT / "assets" / "helpers" / "providers-ui.js").read_text(encoding="utf-8")
    flat = (ROOT / "assets" / "themes" / "flat.css").read_text(encoding="utf-8")
    providers_css = (ROOT / "assets" / "css" / "providers.css").read_text(encoding="utf-8")
    auth_css = (ROOT / "assets" / "css" / "auth-providers.css").read_text(encoding="utf-8")
    libs = (ROOT / "assets" / "js" / "modals" / "pair-config" / "libraries.js").read_text(encoding="utf-8")

    assert 'TOFA: { key: "TOFA"' in meta and '"SILO","TOFA"' in meta
    assert 'tofa: "/assets/auth/auth.tofa.js"' in loader and '"sec-tofa": "tofa"' in loader
    assert 'set.add("TOFA")' in core
    assert '"PLEX", "JELLYFIN", "EMBY", "SILO", "TOFA"' in ui and "window.cwAuth?.tofa?.saveSelectedProfile" in ui
    assert flat.count("#tofa_connect,") == flat.count("#silo_connect,")
    for needle in (".brand-ico.tofa{", ".prov-card.brand-tofa{", 'tile[data-provider="tofa"]', 'ss-badge[data-provider="tofa"]', "#sec-tofa>.head{", "#page-settings #sec-tofa>.head,"):
        assert needle in providers_css
    assert "#tofa_disconnect" in auth_css and providers_css.count("{") == providers_css.count("}")
    assert '"tofa-hist-libs"' in libs and '"tofa-libs-load"' in libs
    for name in ("TOFA.svg", "TOFA-log.svg"):
        svg = (ROOT / "assets" / "img" / name).read_text(encoding="utf-8")
        assert svg.startswith("<svg") and 'viewBox="-52.50 0.00 632.00 632.00"' in svg
