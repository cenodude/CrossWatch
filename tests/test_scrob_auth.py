# CrossWatch test scripts
from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass, field
from typing import Any

import pytest
import requests

import providers.auth._auth_SCROB as scrob


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


class FakeSession(requests.Session):
    def __init__(self, handler: Any) -> None:
        super().__init__()
        self._handler = handler
        self.calls: list[dict[str, Any]] = []

    def request(self, method: Any, url: Any, **kwargs: Any) -> Any:  # type: ignore[override]
        call = {"method": method, "url": url, **kwargs}
        self.calls.append(call)
        return self._handler(call)

    def post(self, url: Any, **kwargs: Any) -> Any:  # type: ignore[override]
        return self.request("POST", url, **kwargs)


def make_jwt(exp: int) -> str:
    head = base64.urlsafe_b64encode(b'{"alg":"HS256"}').decode().rstrip("=")
    body = base64.urlsafe_b64encode(json.dumps({"sub": "1", "exp": exp}).encode()).decode().rstrip("=")
    return f"{head}.{body}.sig"


def test_normalize_server_url_adds_scheme_and_strips_slash():
    assert scrob.normalize_server_url("192.168.2.163:7330/") == "http://192.168.2.163:7330"
    assert scrob.normalize_server_url("https://scrob.example.com") == "https://scrob.example.com"
    assert scrob.normalize_server_url("") == ""


def test_normalize_api_prefix():
    assert scrob.normalize_api_prefix("") == ""
    assert scrob.normalize_api_prefix("api/proxy") == "/api/proxy"
    assert scrob.normalize_api_prefix("/api/proxy/") == "/api/proxy"


def test_is_configured_requires_every_credential():
    full = {"server_url": "http://s", "api_key": "k", "username": "u", "password": "p"}
    assert scrob.is_configured(full)
    for missing in ("server_url", "api_key", "username", "password"):
        partial = dict(full)
        partial[missing] = ""
        assert not scrob.is_configured(partial)


def test_api_key_is_sent_as_header_and_query():
    session = FakeSession(lambda call: ResponseStub(200, {"status": "ok"}))
    client = scrob.ScrobClient("http://host:7330", "KEY", session=session)
    client.request_json("GET", "health")
    call = session.calls[0]
    assert call["headers"]["X-Api-Key"] == "KEY"
    assert call["params"]["api_key"] == "KEY"
    assert call["allow_redirects"] is False


def test_bearer_token_is_attached_when_present():
    session = FakeSession(lambda call: ResponseStub(200, {"id": 1}))
    client = scrob.ScrobClient("http://host", "KEY", access_token="JWT", session=session)
    client.request_json("GET", "auth/me")
    assert session.calls[0]["headers"]["Authorization"] == "Bearer JWT"


def test_detect_api_prefix_falls_back_to_frontend_proxy():
    def handler(call: dict[str, Any]) -> ResponseStub:
        if call["url"] == "http://host:7330/health":
            return ResponseStub(302, None)
        if call["url"] == "http://host:7330/api/proxy/health":
            return ResponseStub(200, {"status": "ok", "app": "Scrob"})
        raise AssertionError(call["url"])

    session = FakeSession(handler)
    assert scrob.detect_api_prefix("http://host:7330", "KEY", session=session) == "/api/proxy"


def test_detect_api_prefix_prefers_direct_backend():
    session = FakeSession(lambda call: ResponseStub(200, {"status": "ok"}))
    assert scrob.detect_api_prefix("http://host:7331", "KEY", session=session) == ""
    assert session.calls[0]["url"] == "http://host:7331/health"


def test_detect_api_prefix_raises_when_unreachable():
    def handler(call: dict[str, Any]) -> ResponseStub:
        return ResponseStub(404, None)

    with pytest.raises(scrob.ScrobAuthError) as exc:
        scrob.detect_api_prefix("http://host", "KEY", session=FakeSession(handler))
    assert exc.value.reason in ("validation_http_404", "api_not_found")


def test_login_returns_token_and_expiry():
    exp = int(time.time()) + 3600
    session = FakeSession(lambda call: ResponseStub(200, {"access_token": make_jwt(exp), "token_type": "bearer"}))
    token = scrob.login("http://host", "KEY", "user", "pw", session=session)
    assert token["expires_at"] == exp
    body = session.calls[0]["data"]
    assert body["username"] == "user" and body["password"] == "pw"


def test_login_rejects_bad_password():
    session = FakeSession(lambda call: ResponseStub(401, {"detail": "Incorrect username or password"}))
    with pytest.raises(scrob.ScrobAuthError) as exc:
        scrob.login("http://host", "KEY", "user", "bad", session=session)
    assert exc.value.reason == "invalid_credentials"


def test_login_rejects_two_factor_accounts():
    session = FakeSession(lambda call: ResponseStub(200, {"requires_2fa": True, "temp_token": "t"}))
    with pytest.raises(scrob.ScrobAuthError) as exc:
        scrob.login("http://host", "KEY", "user", "pw", session=session)
    assert exc.value.reason == "totp_required"


def test_login_reports_sso_only_servers():
    session = FakeSession(lambda call: ResponseStub(403, {"detail": "Password login is disabled. Please use SSO."}))
    with pytest.raises(scrob.ScrobAuthError) as exc:
        scrob.login("http://host", "KEY", "user", "pw", session=session)
    assert exc.value.reason == "password_login_disabled"


def test_token_expired_uses_refresh_skew():
    fresh = {"access_token": "t", "expires_at": int(time.time()) + 3600}
    stale = {"access_token": "t", "expires_at": int(time.time()) + 10}
    assert not scrob.token_expired(fresh)
    assert scrob.token_expired(stale)
    assert scrob.token_expired({"access_token": "", "expires_at": 0})


def test_refresh_token_is_single_flight(monkeypatch: pytest.MonkeyPatch):
    exp = int(time.time()) + 3600
    stored = {
        "scrob": {
            "server_url": "http://host",
            "api_key": "KEY",
            "username": "u",
            "password": "p",
            "access_token": "",
            "expires_at": 0,
        }
    }
    saved: list[dict[str, Any]] = []
    logins: list[int] = []

    monkeypatch.setattr(scrob, "load_config", lambda: stored)
    monkeypatch.setattr(scrob, "save_config", lambda cfg: saved.append(cfg))

    def fake_login(*args: Any, **kwargs: Any) -> dict[str, Any]:
        logins.append(1)
        token = {"access_token": make_jwt(exp), "expires_at": exp}
        stored["scrob"]["access_token"] = token["access_token"]
        stored["scrob"]["expires_at"] = exp
        return token

    monkeypatch.setattr(scrob, "login", fake_login)

    first = scrob.refresh_token(dict(stored))
    second = scrob.refresh_token(dict(stored))
    assert first["access_token"]
    assert second["access_token"] == first["access_token"]
    assert len(logins) == 1
    assert saved


def test_refresh_token_clears_credentials_on_terminal_error(monkeypatch: pytest.MonkeyPatch):
    stored = {
        "scrob": {
            "server_url": "http://host",
            "api_key": "KEY",
            "username": "u",
            "password": "bad",
            "access_token": "old",
            "expires_at": 1,
        }
    }
    monkeypatch.setattr(scrob, "load_config", lambda: stored)
    monkeypatch.setattr(scrob, "save_config", lambda cfg: None)

    def fake_login(*args: Any, **kwargs: Any):
        raise scrob.ScrobAuthError("nope", reason="invalid_credentials")

    monkeypatch.setattr(scrob, "login", fake_login)
    with pytest.raises(scrob.ScrobAuthError):
        scrob.refresh_token(dict(stored))
    assert stored["scrob"]["access_token"] == ""
    assert stored["scrob"]["expires_at"] == 0


def test_request_with_auth_retries_once_after_401(monkeypatch: pytest.MonkeyPatch):
    exp = int(time.time()) + 3600
    cfg = {
        "scrob": {
            "server_url": "http://host",
            "api_key": "KEY",
            "username": "u",
            "password": "p",
            "access_token": make_jwt(exp),
            "expires_at": exp,
        }
    }
    seen: list[str] = []

    def handler(call: dict[str, Any]) -> ResponseStub:
        seen.append(call["headers"].get("Authorization", ""))
        return ResponseStub(401 if len(seen) == 1 else 200, {"ok": True})

    session = FakeSession(handler)
    monkeypatch.setattr(scrob, "refresh_token", lambda *a, **k: {"access_token": "SECOND", "expires_at": exp})
    resp = scrob.request_with_auth(session, "GET", "http://host/history", cfg=cfg)
    assert resp.status_code == 200
    assert seen[1] == "Bearer SECOND"


def test_capability_report_reflects_reachable_endpoints():
    def handler(call: dict[str, Any]) -> ResponseStub:
        url = call["url"]
        if url.endswith("/lists"):
            return ResponseStub(403, {"detail": "nope"})
        return ResponseStub(200, {"results": [], "id": 1})

    client = scrob.ScrobClient("http://host", "KEY", access_token="JWT", session=FakeSession(handler))
    report = scrob.capability_report(client)
    assert report["read_history"] is True
    assert report["read_ratings"] is True
    assert report["read_lists"] is False
    assert report["write_history"] is True
    assert report["write_scrobble"] is True


def test_validate_credentials_returns_prefix_and_capabilities():
    def handler(call: dict[str, Any]) -> ResponseStub:
        url = call["url"]
        if url.endswith("/health"):
            return ResponseStub(200, {"status": "ok"})
        if url.endswith("/auth/login"):
            return ResponseStub(200, {"access_token": make_jwt(int(time.time()) + 900)})
        return ResponseStub(200, {"id": 1, "results": []})

    ok, reason, detail = scrob.validate_credentials("http://host", "KEY", "u", "p", session=FakeSession(handler))
    assert ok and reason == ""
    assert detail["api_prefix"] == ""
    assert detail["capabilities"]["read_history"] is True


def test_validate_credentials_rejects_a_key_from_another_account():
    def handler(call: dict[str, Any]) -> ResponseStub:
        url = call["url"]
        if url.endswith("/health"):
            return ResponseStub(200, {"status": "ok"})
        if url.endswith("/auth/login"):
            return ResponseStub(200, {"access_token": make_jwt(int(time.time()) + 900)})
        if url.endswith("/auth/me"):
            return ResponseStub(200, {"id": 2, "username": "someone-else", "api_key": "A-DIFFERENT-KEY"})
        return ResponseStub(200, {"results": []})

    ok, reason, detail = scrob.validate_credentials("http://host", "MY-KEY", "u", "p", session=FakeSession(handler))
    assert not ok
    assert reason == "credentials_mismatch"
    assert detail == {}


def test_validate_credentials_accepts_a_matching_account():
    def handler(call: dict[str, Any]) -> ResponseStub:
        url = call["url"]
        if url.endswith("/health"):
            return ResponseStub(200, {"status": "ok"})
        if url.endswith("/auth/login"):
            return ResponseStub(200, {"access_token": make_jwt(int(time.time()) + 900)})
        if url.endswith("/auth/me"):
            return ResponseStub(200, {"id": 1, "username": "cenodude", "api_key": "MY-KEY"})
        return ResponseStub(200, {"results": []})

    ok, reason, detail = scrob.validate_credentials("http://host", "MY-KEY", "u", "p", session=FakeSession(handler))
    assert ok and reason == ""
    assert detail["account"] == "cenodude"
    assert detail["capabilities"]["write_history"] is True


def test_identity_lookup_is_not_repeated_for_the_capability_probe():
    calls: list[str] = []

    def handler(call: dict[str, Any]) -> ResponseStub:
        calls.append(call["url"])
        url = call["url"]
        if url.endswith("/health"):
            return ResponseStub(200, {"status": "ok"})
        if url.endswith("/auth/login"):
            return ResponseStub(200, {"access_token": make_jwt(int(time.time()) + 900)})
        if url.endswith("/auth/me"):
            return ResponseStub(200, {"id": 1, "username": "u", "api_key": "MY-KEY"})
        return ResponseStub(200, {"results": []})

    scrob.validate_credentials("http://host", "MY-KEY", "u", "p", session=FakeSession(handler))
    assert sum(1 for u in calls if u.endswith("/auth/me")) == 1


def test_writes_are_reported_unavailable_when_the_login_is_not_usable():
    def handler(call: dict[str, Any]) -> ResponseStub:
        if call["url"].endswith("/auth/me"):
            return ResponseStub(401, {"detail": "Not authenticated"})
        return ResponseStub(200, {"results": []})

    client = scrob.ScrobClient("http://host", "KEY", session=FakeSession(handler))
    report = scrob.capability_report(client)
    assert report["read_history"] is True
    assert report["write_history"] is False
    assert report["write_watchlist"] is False


def test_validate_credentials_reports_invalid_api_key():
    def handler(call: dict[str, Any]) -> ResponseStub:
        if call["url"].endswith("/health"):
            return ResponseStub(200, {"status": "ok"})
        return ResponseStub(401, {"detail": "Not authenticated"})

    ok, reason, detail = scrob.validate_credentials("http://host", "KEY", "u", "p", session=FakeSession(handler))
    assert not ok
    assert reason == "invalid_api_key"
    assert detail == {}


def totp_server(good_code: str = "123456", exp: int | None = None):
    expiry = exp if exp is not None else int(time.time()) + 604800

    def handler(call: dict[str, Any]) -> ResponseStub:
        url = call["url"]
        if url.endswith("/health"):
            return ResponseStub(200, {"status": "ok"})
        if url.endswith("/profile/me"):
            return ResponseStub(200, {"display_name": "x"})
        if url.endswith("/auth/login"):
            return ResponseStub(200, {"requires_2fa": True, "temp_token": "TEMP"})
        if url.endswith("/auth/2fa/verify-login"):
            sent = (call.get("json") or {}).get("code")
            if sent != good_code:
                return ResponseStub(401, {"detail": "Invalid or expired token"})
            return ResponseStub(200, {"access_token": make_jwt(expiry), "token_type": "bearer"})
        if url.endswith("/auth/me"):
            return ResponseStub(200, {"id": 1, "username": "u", "api_key": "KEY", "totp_enabled": True})
        return ResponseStub(200, {"results": []})

    return handler


def test_login_asks_for_a_code_when_none_is_supplied():
    with pytest.raises(scrob.ScrobAuthError) as exc:
        scrob.login("http://host", "KEY", "u", "p", session=FakeSession(totp_server()))
    assert exc.value.reason == "totp_required"


def test_login_completes_the_two_factor_challenge():
    token = scrob.login("http://host", "KEY", "u", "p", totp_code="123456", session=FakeSession(totp_server()))
    assert token["access_token"]
    assert token["expires_at"] > int(time.time())


def test_login_rejects_a_wrong_code():
    with pytest.raises(scrob.ScrobAuthError) as exc:
        scrob.login("http://host", "KEY", "u", "p", totp_code="000000", session=FakeSession(totp_server()))
    assert exc.value.reason == "invalid_totp_code"


def test_verify_totp_needs_a_challenge_token():
    client = scrob.ScrobClient("http://host", "KEY", session=FakeSession(totp_server()))
    with pytest.raises(scrob.ScrobAuthError) as exc:
        scrob.verify_totp(client, "", "123456")
    assert exc.value.reason == "validation_bad_response"


def test_validate_credentials_records_that_the_account_uses_2fa():
    ok, reason, detail = scrob.validate_credentials(
        "http://host", "KEY", "u", "p", totp_code="123456", session=FakeSession(totp_server())
    )
    assert ok and reason == ""
    assert detail["totp_enabled"] is True
    assert detail["reauth_required"] is False
    assert detail["capabilities"]["write_history"] is True


def test_expired_two_factor_session_keeps_reads_and_flags_reauth(monkeypatch: pytest.MonkeyPatch):
    cfg = {
        "scrob": {
            "server_url": "http://host",
            "api_key": "KEY",
            "username": "u",
            "password": "p",
            "totp_enabled": True,
            "reauth_required": False,
            "access_token": make_jwt(int(time.time()) - 10),
            "expires_at": int(time.time()) - 10,
        }
    }
    monkeypatch.setattr(scrob, "load_config", lambda: cfg)
    monkeypatch.setattr(scrob, "save_config", lambda c: None)
    session = FakeSession(totp_server())

    read = scrob.request_with_auth(session, "GET", "http://host/history", cfg=cfg)
    assert read.status_code == 200

    with pytest.raises(scrob.ScrobAuthError) as exc:
        scrob.request_with_auth(session, "POST", "http://host/history", cfg=cfg, json={})
    assert exc.value.reason == "totp_required"

    assert scrob.is_configured(cfg["scrob"]) is True
    assert scrob.needs_reauth(cfg["scrob"]) is True


def test_a_successful_refresh_clears_the_reauth_flag(monkeypatch: pytest.MonkeyPatch):
    exp = int(time.time()) + 3600
    cfg = {
        "scrob": {
            "server_url": "http://host",
            "api_key": "KEY",
            "username": "u",
            "password": "p",
            "reauth_required": True,
            "access_token": "",
            "expires_at": 0,
        }
    }
    monkeypatch.setattr(scrob, "load_config", lambda: cfg)
    monkeypatch.setattr(scrob, "save_config", lambda c: None)
    monkeypatch.setattr(scrob, "login", lambda *a, **k: {"access_token": make_jwt(exp), "expires_at": exp})

    scrob.refresh_token(cfg)
    assert scrob.needs_reauth(cfg["scrob"]) is False


def test_a_plain_bad_password_still_clears_the_token(monkeypatch: pytest.MonkeyPatch):
    cfg = {
        "scrob": {
            "server_url": "http://host",
            "api_key": "KEY",
            "username": "u",
            "password": "wrong",
            "access_token": "old",
            "expires_at": 1,
        }
    }
    monkeypatch.setattr(scrob, "load_config", lambda: cfg)
    monkeypatch.setattr(scrob, "save_config", lambda c: None)

    def bad_login(*a: Any, **k: Any):
        raise scrob.ScrobAuthError("no", reason="invalid_credentials")

    monkeypatch.setattr(scrob, "login", bad_login)
    with pytest.raises(scrob.ScrobAuthError):
        scrob.refresh_token(cfg)
    assert cfg["scrob"]["access_token"] == ""
    assert scrob.needs_reauth(cfg["scrob"]) is False


def test_auth_section_exposes_the_two_factor_field():
    markup = scrob.html()
    assert 'id="scrob_totp"' in markup
    assert 'id="scrob_totp_row"' in markup
    assert 'id="scrob_reauth"' in markup
