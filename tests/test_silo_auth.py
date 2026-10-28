# CrossWatch test scripts
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
SERVER = "http://silo.local:8090"
PROBLEM = "https://siloserver.org/docs/api/v2/problems/"


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


def _problem(code: str, status: int) -> ResponseStub:
    return ResponseStub(status, {"type": PROBLEM + code, "status": status})


def _store(monkeypatch: pytest.MonkeyPatch, cfg: dict[str, Any]) -> dict[str, Any]:
    from providers.auth import _auth_SILO as silo

    holder = {"cfg": cfg}

    def save(new_cfg: dict[str, Any]) -> None:
        holder["cfg"] = new_cfg

    monkeypatch.setattr(silo, "save_config", save)
    monkeypatch.setattr(silo, "load_config", lambda: holder["cfg"])
    return holder


def _connected(**extra: Any) -> dict[str, Any]:
    block = {
        "server_url": SERVER,
        "verify_ssl": True,
        "device_id": "crosswatch-test",
        "access_token": "access-1",
        "refresh_token": "refresh-1",
        "expires_at": 4_000_000_000,
        "profile_id": "p-1",
        "profile_name": "Alice",
        "profile_has_pin": False,
        "profile_token": "",
    }
    block.update(extra)
    return {"silo": block}


def test_normalize_server_url_rules() -> None:
    from providers.auth._auth_SILO import normalize_server_url

    assert normalize_server_url("192.168.1.10:8090") == "http://192.168.1.10:8090"
    assert normalize_server_url("https://silo.example.com/") == "https://silo.example.com"
    assert normalize_server_url("https://silo.example.com/base/api/v2") == "https://silo.example.com/base"
    assert normalize_server_url("ftp://silo") == ""
    assert normalize_server_url("https://user:pw@silo") == ""
    assert normalize_server_url("") == ""


def test_check_server_accepts_device_capability_and_rejects_others() -> None:
    from providers.auth import _auth_SILO as silo

    ok = silo.check_server(SERVER, session=FakeSession([ResponseStub(200, {"state": "available", "protocol_versions": [2], "cancel": True})]))
    assert ok == {"server_url": SERVER, "cancel": True}

    with pytest.raises(silo.SiloServerError, match="device_login_unavailable"):
        silo.check_server(SERVER, session=FakeSession([ResponseStub(200, {"state": "disabled", "protocol_versions": [2]})]))
    with pytest.raises(silo.SiloServerError, match="not_silo"):
        silo.check_server(SERVER, session=FakeSession([ResponseStub(200, None)]))
    with pytest.raises(silo.SiloServerError, match="not_silo"):
        silo.check_server(SERVER, session=FakeSession([ResponseStub(404, {})]))


def test_device_start_stores_pending_and_hides_device_code(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg: dict[str, Any] = {"silo": {}}
    _store(monkeypatch, cfg)
    session = FakeSession([
        ResponseStub(200, {"state": "available", "protocol_versions": [2], "cancel": True}),
        ResponseStub(201, {
            "device_code": "dc-secret",
            "user_code": "6521-6693",
            "verification_uri": f"{SERVER}/activate",
            "verification_uri_complete": f"{SERVER}/activate?code=65216693",
            "expires_at": "2030-01-01T00:00:00Z",
            "expires_in": 900,
            "interval": 3,
        }),
    ])

    out = silo.SiloClient(cfg, session=session).start_device_login(cfg, server_url="silo.local:8090")

    assert out["ok"] is True
    assert out["user_code"] == "6521-6693"
    assert out["verification_uri_complete"].endswith("code=65216693")
    assert "dc-secret" not in str(out)
    assert cfg["silo"]["server_url"] == SERVER
    assert cfg["silo"]["_pending_device"]["device_code"] == "dc-secret"
    assert cfg["silo"]["_pending_device"]["expires_at"] == 1_893_456_000
    start = session.calls[1]
    assert start["url"] == f"{SERVER}/api/v2/auth/device/start"
    assert start["json"] == {"client_purpose": "device_login", "device_name": "CrossWatch", "device_platform": "CrossWatch"}
    assert start["headers"]["X-Silo-Device-Name"] == "CrossWatch"
    assert start["headers"]["X-Silo-Device-Id"].startswith("crosswatch-")


def test_server_change_requires_disconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _connected()
    _store(monkeypatch, cfg)
    session = FakeSession([ResponseStub(200, {"state": "available", "protocol_versions": [2]})])

    with pytest.raises(silo.SiloServerError, match="disconnect_first"):
        silo.SiloClient(cfg, session=session).configure_server(cfg, "http://other:8090")
    assert cfg["silo"]["server_url"] == SERVER


def _pending_cfg() -> dict[str, Any]:
    return {"silo": {"server_url": SERVER, "device_id": "crosswatch-test", "_pending_device": {"device_code": "dc", "user_code": "1234-5678", "expires_at": 4_000_000_000, "interval": 3}}}


def test_poll_pending_then_approved_stores_tokens_and_auto_selects_single_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _pending_cfg()
    _store(monkeypatch, cfg)
    session = FakeSession([
        ResponseStub(200, {"status": "pending", "opened": True, "poll_after": 3, "expires_at": "2030-01-01T00:05:00Z", "profile_id": "", "profile_token": "", "temporary": False}),
        ResponseStub(200, {"status": "approved", "poll_after": 3, "opened": False, "profile_id": "", "profile_token": "", "temporary": False,
                           "tokens": {"access_token": "a1", "refresh_token": "r1", "expires_in": 3600, "user": {"id": "1", "username": "Alice", "role": "admin"}}}),
        ResponseStub(200, {"items": [{"id": "p-1", "name": "Alice", "is_primary": True, "has_pin": False, "is_child": False}]}),
    ])
    client = silo.SiloClient(cfg, session=session)

    pending = client.poll_device_login(cfg)
    approved = client.poll_device_login(cfg)

    assert pending == {"ok": False, "status": "pending", "opened": True, "interval": 3, "expires_at": 1_893_456_300, "instance": "default"}
    assert approved["ok"] is True and approved["status"] == "authorized"
    assert "a1" not in str(approved) and "r1" not in str(approved)
    block = cfg["silo"]
    assert block["access_token"] == "a1" and block["refresh_token"] == "r1"
    assert block["username"] == "Alice" and block["role"] == "admin"
    assert block["profile_id"] == "p-1" and block["profile_name"] == "Alice"
    assert "_pending_device" not in block
    assert silo.is_configured(block)
    profiles_call = session.calls[2]
    assert profiles_call["url"] == f"{SERVER}/api/v2/profiles"
    assert "X-Profile-Id" not in profiles_call["headers"]


def test_poll_approved_with_several_or_pin_profiles_leaves_profile_unselected(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _pending_cfg()
    _store(monkeypatch, cfg)
    session = FakeSession([
        ResponseStub(200, {"status": "approved", "tokens": {"access_token": "a1", "refresh_token": "r1", "expires_in": 3600}}),
        ResponseStub(200, {"items": [{"id": "p-1", "name": "Kid", "has_pin": True}]}),
    ])

    out = silo.SiloClient(cfg, session=session).poll_device_login(cfg)

    assert out["ok"] is True
    assert out["profiles"][0]["has_pin"] is True
    assert not cfg["silo"].get("profile_id")
    assert silo.is_configured(cfg["silo"]) is False
    assert silo.status_for_block(cfg["silo"])["authenticated"] is True


@pytest.mark.parametrize("resp,status", [
    (ResponseStub(200, {"status": "denied"}), "denied"),
    (ResponseStub(200, {"status": "canceled"}), "canceled"),
    (ResponseStub(404, {"type": PROBLEM + "not_found"}), "expired"),
])
def test_poll_terminal_states_clear_pending(monkeypatch: pytest.MonkeyPatch, resp: ResponseStub, status: str) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _pending_cfg()
    _store(monkeypatch, cfg)

    out = silo.SiloClient(cfg, session=FakeSession([resp])).poll_device_login(cfg)

    assert out["status"] == status
    assert "_pending_device" not in cfg["silo"]


def test_cancel_calls_server_and_clears_pending(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _pending_cfg()
    _store(monkeypatch, cfg)
    session = FakeSession([ResponseStub(200, {"status": "canceled"})])

    out = silo.SiloClient(cfg, session=session).cancel_device_login(cfg)

    assert out["cancelled"] is True
    assert session.calls[0]["url"] == f"{SERVER}/api/v2/auth/device/cancel"
    assert session.calls[0]["json"] == {"device_code": "dc"}
    assert "_pending_device" not in cfg["silo"]


def test_refresh_skips_fresh_token_and_updates_expired(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _connected()
    holder = _store(monkeypatch, cfg)
    assert silo.SiloClient(cfg, session=FakeSession()).refresh_token(cfg)["status"] == "fresh"

    cfg["silo"]["expires_at"] = 1
    session = FakeSession([ResponseStub(200, {"access_token": "access-2", "refresh_token": "refresh-1", "expires_in": 3600})])
    out = silo.SiloClient(cfg, session=session).refresh_token(cfg)

    assert out["ok"] is True
    assert holder["cfg"]["silo"]["access_token"] == "access-2"
    assert holder["cfg"]["silo"]["refresh_token"] == "refresh-1"
    assert session.calls[0]["json"] == {"refresh_token": "refresh-1"}


def test_refresh_rejection_flags_reconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _connected(expires_at=1)
    _store(monkeypatch, cfg)

    out = silo.SiloClient(cfg, session=FakeSession([_problem("session_expired", 401)])).refresh_token(cfg)

    assert out["reconnect_required"] is True
    assert cfg["silo"]["auth_error"] == "reconnect_required"
    assert silo.is_configured(cfg["silo"]) is False
    with pytest.raises(silo.SiloReconnectRequired):
        silo.SiloClient(cfg, session=FakeSession()).access_token()


def test_refresh_keeps_session_on_server_outage(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _connected(expires_at=1)
    _store(monkeypatch, cfg)

    out = silo.SiloClient(cfg, session=FakeSession([ResponseStub(503, {}, {"Retry-After": "5"})])).refresh_token(cfg)

    assert out["ok"] is False and out["status"] == "service_unavailable"
    assert "auth_error" not in cfg["silo"]


def test_request_sends_profile_headers_and_retries_once_after_401(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _connected(profile_token="ptok")
    _store(monkeypatch, cfg)
    session = FakeSession([
        _problem("token_refresh_required", 401),
        ResponseStub(200, {"access_token": "access-2", "expires_in": 3600}),
        ResponseStub(200, {"items": []}),
    ])

    resp = silo.SiloClient(cfg, session=session).request("GET", "/watchlist")

    assert resp.status_code == 200
    first, refresh, retry = session.calls
    assert first["headers"]["Authorization"] == "Bearer access-1"
    assert first["headers"]["X-Profile-Id"] == "p-1"
    assert first["headers"]["X-Profile-Token"] == "ptok"
    assert refresh["url"].endswith("/api/v2/auth/refresh")
    assert retry["headers"]["Authorization"] == "Bearer access-2"


def test_stale_profile_token_flags_pin_required(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _connected(profile_has_pin=True, profile_token="old")
    _store(monkeypatch, cfg)

    with pytest.raises(silo.SiloProfileVerificationRequired):
        silo.SiloClient(cfg, session=FakeSession([_problem("profile_verification_required", 403)])).request("GET", "/ratings")

    assert cfg["silo"]["auth_error"] == "profile_pin_required"
    assert cfg["silo"]["profile_token"] == ""
    assert silo.status_for_block(cfg["silo"])["profile_pin_required"] is True


def _profiles(*rows: dict[str, Any]) -> ResponseStub:
    return ResponseStub(200, {"items": list(rows)})


def test_select_pin_profile_verifies_pin_and_stores_token(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _connected(profile_id="", profile_name="")
    _store(monkeypatch, cfg)
    kid = {"id": "p-2", "name": "Kid", "has_pin": True, "is_child": True}
    session = FakeSession([_profiles(kid), ResponseStub(200, {"valid": True, "profile_token": "pin-token", "expires_at": None})])

    selected = silo.SiloClient(cfg, session=session).select_profile(cfg, "p-2", "1234")

    assert selected["id"] == "p-2"
    assert cfg["silo"]["profile_id"] == "p-2"
    assert cfg["silo"]["profile_token"] == "pin-token"
    assert cfg["silo"]["profile_has_pin"] is True
    verify = session.calls[1]
    assert verify["url"] == f"{SERVER}/api/v2/profiles/p-2/verify-pin"
    assert verify["json"] == {"pin": "1234"}
    assert "1234" not in str(cfg)


@pytest.mark.parametrize("responses,pin,error", [
    ([_profiles({"id": "p-2", "name": "Kid", "has_pin": True})], "", "pin_required"),
    ([_profiles({"id": "p-2", "name": "Kid", "has_pin": True}), ResponseStub(200, {"valid": False, "expires_at": None})], "0000", "invalid_pin"),
    ([_profiles({"id": "p-1", "name": "Alice"})], "", "profile_unavailable"),
])
def test_select_profile_errors(monkeypatch: pytest.MonkeyPatch, responses: list[ResponseStub], pin: str, error: str) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _connected(profile_id="", profile_name="")
    _store(monkeypatch, cfg)

    with pytest.raises(silo.SiloProfileError, match=error):
        silo.SiloClient(cfg, session=FakeSession(responses)).select_profile(cfg, "p-2", pin)
    assert cfg["silo"]["profile_id"] == ""


def test_select_profile_pin_lockout_reports_retry_after(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _connected(profile_id="")
    _store(monkeypatch, cfg)
    session = FakeSession([_profiles({"id": "p-2", "name": "Kid", "has_pin": True}), ResponseStub(429, {"type": PROBLEM + "rate_limited"}, {"Retry-After": "300"})])

    with pytest.raises(silo.SiloRateLimited) as exc:
        silo.SiloClient(cfg, session=session).select_profile(cfg, "p-2", "9999")
    assert exc.value.retry_after == 300


def test_disconnect_logs_out_and_clears_session(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _connected(profile_token="ptok")
    _store(monkeypatch, cfg)
    session = FakeSession([ResponseStub(204, None)])

    silo.SiloClient(cfg, session=session).disconnect(cfg)

    assert session.calls[0]["url"] == f"{SERVER}/api/v2/auth/logout"
    block = cfg["silo"]
    assert block["access_token"] == "" and block["refresh_token"] == "" and block["profile_token"] == ""
    assert block["profile_id"] == ""
    assert block["server_url"] == SERVER
    assert silo.status_for_block(block)["connected"] is False


def test_request_with_auth_resolves_named_profile_from_narrowed_view(monkeypatch: pytest.MonkeyPatch) -> None:
    from cw_platform.provider_instances import build_provider_config_view
    from providers.auth import _auth_SILO as silo

    named = _connected()["silo"]
    cfg = {"silo": {"server_url": "", "instances": {"SILO-P01": named}}}
    _store(monkeypatch, cfg)
    view = build_provider_config_view(cfg, "silo", "SILO-P01")
    session = FakeSession([ResponseStub(200, {"items": []})])

    def call(sess: Any, method: str, url: str, **kwargs: Any) -> ResponseStub:
        return sess.request(method, url, **kwargs)

    resp = silo.request_with_auth(session, "GET", f"{SERVER}/api/v2/ratings", cfg=view, instance_id="SILO-P01", request_func=call)

    assert resp.status_code == 200
    assert session.calls[0]["headers"]["Authorization"] == "Bearer access-1"
    assert session.calls[0]["headers"]["X-Profile-Id"] == "p-1"
    assert silo.provider_block(cfg, "SILO-P99") == {}


def test_default_config_redaction_and_registration() -> None:
    from cw_platform.config_base import DEFAULT_CFG, redact_config
    from cw_platform.modules_registry import MODULES
    from providers.auth.runtime import _backend

    block = DEFAULT_CFG["silo"]
    assert set(block) == {"server_url", "verify_ssl", "access_token", "refresh_token", "expires_at", "profile_id", "profile_name"}
    cfg = _connected(profile_token="ptok", _pending_device={"device_code": "dc", "user_code": "1"})
    red = redact_config(copy.deepcopy(cfg))["silo"]
    for key in ("access_token", "refresh_token", "profile_token", "_pending_device"):
        assert red[key] != cfg["silo"][key]
    assert red["server_url"] == SERVER and red["profile_id"] == "p-1"
    assert MODULES["AUTH"]["_auth_SILO"] == "providers.auth._auth_SILO"
    assert _backend("silo").__name__ == "providers.auth._auth_SILO"


def test_auth_section_is_grouped_with_media_servers() -> None:
    from providers.auth.registry import auth_providers_html

    html = auth_providers_html()
    media = html.index('id="sec-auth-media"')
    trackers = html.index('id="sec-auth-trackers"')

    assert media < html.index('id="sec-silo"') < trackers


def test_probe_registration_key_opacity_and_reasons(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import probesAPI as probes
    from providers.auth import _auth_SILO as silo

    assert "silo" in probes.PROVIDERS
    assert probes.PROBE_CFG_KEY["SILO"] == "silo"
    assert probes.DETAIL_PROBES["SILO"] is probes._probe_silo_detail
    assert probes._probe_key("silo", {"silo": {}}) == "silo|unconfigured"
    key = probes._probe_key("silo", _connected())
    assert "access-1" not in key and "refresh-1" not in key and SERVER not in key
    assert probes._prov_configured(_connected(), "SILO", "default") is True

    probes.invalidate_provider_caches("silo")
    assert probes._probe_silo_detail({"silo": {}}, max_age_sec=0) == (False, "Silo: missing authentication")
    assert probes._probe_silo_detail(_connected(auth_error="reconnect_required"), max_age_sec=0) == (False, "Silo: reconnect required")

    monkeypatch.setattr(silo.SiloClient, "list_profiles", lambda self: [{"id": "p-1", "name": "Alice", "has_pin": False}])
    probes.invalidate_provider_caches("silo")
    assert probes._probe_silo_detail(_connected(), max_age_sec=0) == (True, "")
    probes.invalidate_provider_caches("silo")
    assert probes._probe_silo_detail(_connected(profile_id="gone"), max_age_sec=0) == (False, "Silo: profile unavailable")


def test_frontend_wiring_and_unmodified_brand_assets() -> None:
    meta = (ROOT / "assets" / "helpers" / "provider-meta.js").read_text(encoding="utf-8")
    loader = (ROOT / "assets" / "auth" / "auth_loader.js").read_text(encoding="utf-8")
    core = (ROOT / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    ui = (ROOT / "assets" / "helpers" / "providers-ui.js").read_text(encoding="utf-8")
    flat = (ROOT / "assets" / "themes" / "flat.css").read_text(encoding="utf-8")
    providers_css = (ROOT / "assets" / "css" / "providers.css").read_text(encoding="utf-8")
    auth_css = (ROOT / "assets" / "css" / "auth-providers.css").read_text(encoding="utf-8")

    assert 'SILO: { key: "SILO"' in meta and 'authGroupId: "sec-auth-media"' in meta
    assert 'silo: "/assets/auth/auth.silo.js"' in loader and '"sec-silo": "silo"' in loader
    assert 'set.add("SILO")' in core
    assert '"PLEX", "JELLYFIN", "EMBY", "SILO"' in ui
    assert 'window.cwAuth?.silo?.saveSelectedProfile' in ui
    assert flat.count("#silo_connect,") == flat.count("#punchplay_device_start,")
    for needle in (".brand-ico.silo{", ".prov-card.brand-silo{", 'tile[data-provider="silo"]', 'ss-badge[data-provider="silo"]', "#sec-silo>.head{", "#page-settings #sec-silo>.head,"):
        assert needle in providers_css
    assert "#silo_disconnect" in auth_css
    assert providers_css.count("{") == providers_css.count("}")
    for name in ("SILO.svg", "SILO-log.svg"):
        svg = (ROOT / "assets" / "img" / name).read_text(encoding="utf-8")
        assert "Silo Media L.L.C." in svg and 'viewBox="0 0 1024 1024"' in svg


def test_keepalive_refreshes_only_stale_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    import time as _time

    from cw_platform import token_refresh
    from providers.auth import _auth_SILO as silo

    assert token_refresh.PROVIDERS["SILO"] == "providers.auth._auth_SILO"
    cfg = _connected(session_refreshed_at=int(_time.time()))
    cfg["silo"]["instances"] = {"SILO-P01": {**_connected()["silo"], "session_refreshed_at": 1}}
    _store(monkeypatch, cfg)
    forced: list[Any] = []
    monkeypatch.setattr(silo.SiloClient, "refresh_token", lambda self, c, force=False, **k: forced.append((self.instance_id, force)) or {"ok": True, "status": "ok"})

    assert silo.refresh_all() == {"SILO-P01": "ok"}
    assert forced == [("SILO-P01", True)]


def test_verify_ssl_defaults_off_for_self_signed_servers() -> None:
    from cw_platform.config_base import DEFAULT_CFG
    from providers.auth import _auth_SILO as silo

    assert silo.verify_ssl_of({}) is False and silo.verify_ssl_of({"verify_ssl": True}) is True
    assert DEFAULT_CFG["silo"]["verify_ssl"] is False
    assert 'id="silo_verify_ssl" type="checkbox">' in silo.html()


def test_new_named_profile_does_not_borrow_default_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    from cw_platform.provider_instances import build_provider_config_view
    from providers.auth import _auth_SILO as silo

    cfg = _connected(profile_has_pin=True, profile_token="owner-pin-token", history={"libraries": ["1"]})
    cfg["silo"]["instances"] = {"SILO-P01": {"label": "Kids server"}}
    _store(monkeypatch, cfg)

    block = silo.provider_block(cfg, "SILO-P01")
    assert block == {"label": "Kids server"}
    assert silo.status_for_block(block)["connected"] is False
    session = FakeSession([ResponseStub(200, {"state": "available", "protocol_versions": [2]})])
    res = silo.SiloClient(cfg, instance_id="SILO-P01", session=session).configure_server(cfg, "http://other:8090")
    assert res["ok"] is True and cfg["silo"]["instances"]["SILO-P01"]["server_url"] == "http://other:8090"
    assert cfg["silo"]["server_url"] == SERVER

    pending = cfg["silo"]["instances"]["SILO-P01"]
    pending["_pending_device"] = {"device_code": "dc", "user_code": "1234-5678", "expires_at": 4_000_000_000, "interval": 3}
    poll = FakeSession([
        ResponseStub(200, {"status": "approved", "tokens": {"access_token": "a2", "refresh_token": "r2", "expires_in": 3600}}),
        ResponseStub(200, {"items": [{"id": "p-9", "name": "Kid", "has_pin": False}, {"id": "p-8", "name": "Other", "has_pin": False}]}),
    ])
    silo.SiloClient(cfg, instance_id="SILO-P01", session=poll).poll_device_login(cfg)
    view = silo.provider_block(build_provider_config_view(cfg, "silo", "SILO-P01"), "SILO-P01")
    assert view["access_token"] == "a2" and view["profile_id"] == "" and view["profile_token"] == ""
    assert silo.is_configured(view) is False



def test_named_profile_reads_its_own_block() -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _connected(history={"libraries": ["1"]}, timeout=60)
    cfg["silo"]["instances"] = {"SILO-P01": {"server_url": "http://other:8090", "access_token": "a2", "refresh_token": "r2", "profile_id": "p-9"}}

    own = silo.provider_block(cfg, "SILO-P01")
    assert own == cfg["silo"]["instances"]["SILO-P01"]
    assert silo.provider_block(cfg, "SILO-P02") == {}
    assert silo.provider_block(cfg, "default")["access_token"] == "access-1"
