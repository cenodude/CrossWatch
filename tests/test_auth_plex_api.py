from __future__ import annotations

import json

import pytest
from starlette.requests import Request


@pytest.fixture(autouse=True)
def _reset_plex_flow_state():
    from services import authPlex

    authPlex._PENDING_FLOWS.clear()
    authPlex._START_EVENTS.clear()
    authPlex._PENDING_STARTS_IN_FLIGHT = 0
    yield
    authPlex._PENDING_FLOWS.clear()
    authPlex._START_EVENTS.clear()
    authPlex._PENDING_STARTS_IN_FLIGHT = 0


def _auth_cfg() -> dict:
    from api import appAuthAPI as auth

    salt = b"0123456789abcdef"
    password = "secrett1"
    return {
        "security": {},
        "app_auth": {
            "enabled": True,
            "username": "admin",
            "reset_required": False,
            "remember_session_enabled": True,
            "remember_session_days": 45,
            "plex_sso": {
                "enabled": False,
                "client_id": "crosswatch-test",
                "linked_plex_account_id": "",
                "linked_username": "",
                "linked_email": "",
                "linked_thumb": "",
                "linked_at": 0,
            },
            "password": {
                "scheme": "pbkdf2_sha256",
                "iterations": 260_000,
                "salt": auth._b64e(salt),
                "hash": auth._b64e(auth._pbkdf2_hash(password, salt, iterations=260_000)),
            },
            "session": {"token_hash": "", "expires_at": 0},
            "sessions": [],
            "last_login_at": 0,
        },
    }


def _request(
    path: str,
    *,
    method: str = "POST",
    headers: dict[str, str] | None = None,
    client: tuple[str, int] = ("127.0.0.1", 12345),
) -> Request:
    raw_headers = [(b"host", b"testserver"), (b"sec-fetch-site", b"same-origin")]
    for k, v in (headers or {}).items():
        raw_headers.append((str(k).lower().encode("latin-1"), str(v).encode("latin-1")))
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("latin-1"),
        "query_string": b"",
        "headers": raw_headers,
        "client": client,
        "server": ("testserver", 80),
    }
    return Request(scope)


def _bind_config(monkeypatch, plex_api, cfg: dict) -> None:
    from api import appAuthAPI as auth

    monkeypatch.setattr(plex_api, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)


def _json_body(resp) -> dict:
    return json.loads(resp.body.decode("utf-8"))


def _enable_plex_sso(cfg: dict) -> None:
    cfg["app_auth"]["plex_sso"].update({"enabled": True, "linked_plex_account_id": "plex-123", "linked_username": "plexadmin"})


class _PlexPinResponse:
    def __init__(self, pin_id: str = "pin-1", code: str = "ABCD") -> None:
        self.pin_id = pin_id
        self.code = code

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {"id": self.pin_id, "code": self.code}


def _all_set_cookie_headers(resp) -> str:
    return "\n".join(
        value.decode("latin-1")
        for key, value in getattr(resp, "raw_headers", [])
        if key.decode("latin-1").lower() == "set-cookie"
    )


def test_plex_login_check_issues_cookie_for_linked_identity(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    cfg["app_auth"]["plex_sso"].update({"enabled": True, "linked_plex_account_id": "plex-123", "linked_username": "plexadmin"})
    _bind_config(monkeypatch, plex_api, cfg)
    monkeypatch.setattr(
        plex_api.authPlex,
        "check_flow",
        lambda *_args, **_kwargs: {
            "ok": True,
            "pending": False,
            "remember_me": True,
            "flow_nonce_hash": plex_api.authPlex._sha256_hex("flow-nonce"),
            "identity": {"id": "plex-123", "username": "plexadmin", "email": "plex@example.com", "thumb": ""},
        },
    )

    req = _request("/api/app-auth/plex/check", headers={"cookie": f"{plex_api.FLOW_COOKIE_NAME}=flow-nonce"})
    resp = plex_api.api_plex_check(req, {"state": "ok"})

    assert resp.status_code == 200
    assert _json_body(resp)["ok"] is True
    assert len(cfg["app_auth"]["sessions"]) == 1
    set_cookie = _all_set_cookie_headers(resp)
    assert "cw_auth=" in set_cookie
    assert "Max-Age=" in set_cookie


def test_plex_login_check_issues_cookie_for_managed_linked_identity(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    cfg["app_auth"]["plex_sso"]["enabled"] = True
    cfg["app_auth"]["users"] = {
        "u1": {
            "username": "pascal",
            "enabled": True,
            "role": "user",
            "profile_id": "profile-1",
            "permissions": {"dashboard": True, "watchlist": True, "playback": True, "write": False},
            "plex_sso": {"account_id": "plex-managed", "username": "plexpascal", "email": "plex@example.com", "thumb": ""},
        }
    }
    _bind_config(monkeypatch, plex_api, cfg)
    monkeypatch.setattr(
        plex_api.authPlex,
        "check_flow",
        lambda *_args, **_kwargs: {
            "ok": True,
            "pending": False,
            "remember_me": False,
            "flow_nonce_hash": plex_api.authPlex._sha256_hex("flow-nonce"),
            "identity": {"id": "plex-managed", "username": "plexpascal", "email": "plex@example.com", "thumb": ""},
        },
    )

    req = _request("/api/app-auth/plex/check", headers={"cookie": f"{plex_api.FLOW_COOKIE_NAME}=flow-nonce"})
    resp = plex_api.api_plex_check(req, {"state": "ok"})

    assert resp.status_code == 200
    assert _json_body(resp)["ok"] is True
    assert len(cfg["app_auth"]["sessions"]) == 1
    assert cfg["app_auth"]["sessions"][0]["user_id"] == "u1"
    assert cfg["app_auth"]["sessions"][0]["role"] == "user"
    assert cfg["app_auth"]["sessions"][0]["profile_id"] == "profile-1"
    assert "cw_auth=" in _all_set_cookie_headers(resp)


def test_plex_login_check_requires_admin_totp_when_enabled(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    secret = "JBSWY3DPEHPK3PXP"
    cfg["app_auth"]["plex_sso"].update({"enabled": True, "linked_plex_account_id": "plex-123", "linked_username": "plexadmin"})
    cfg["app_auth"]["totp"] = {"enabled": True, "secret": secret, "pending_secret": "", "pending_created_at": 0}
    _bind_config(monkeypatch, plex_api, cfg)
    monkeypatch.setattr(auth, "_now", lambda: 1_800_000_000)
    auth._LOGIN_FAILS.clear()
    plex_api._PENDING_2FA.clear()
    monkeypatch.setattr(
        plex_api.authPlex,
        "check_flow",
        lambda *_args, **_kwargs: {
            "ok": True,
            "pending": False,
            "remember_me": True,
            "flow_nonce_hash": plex_api.authPlex._sha256_hex("flow-nonce"),
            "identity": {"id": "plex-123", "username": "plexadmin", "email": "plex@example.com", "thumb": ""},
        },
    )

    req = _request("/api/app-auth/plex/check", headers={"cookie": f"{plex_api.FLOW_COOKIE_NAME}=flow-nonce"})
    first = plex_api.api_plex_check(req, {"state": "ok"})
    bad = plex_api.api_plex_check(req, {"state": "ok", "totp_code": "000000"})
    assert cfg["app_auth"]["sessions"] == []
    code = auth._hotp(secret, 1_800_000_000 // auth.TOTP_STEP_SECONDS)
    good = plex_api.api_plex_check(req, {"state": "ok", "totp_code": code})

    assert first.status_code == 401
    assert _json_body(first)["requires_2fa"] is True
    assert bad.status_code == 401
    assert _json_body(bad)["requires_2fa"] is True
    assert good.status_code == 200
    assert _json_body(good)["ok"] is True
    assert len(cfg["app_auth"]["sessions"]) == 1
    assert "cw_auth=" in _all_set_cookie_headers(good)


def test_plex_login_check_rejects_wrong_identity(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    cfg["app_auth"]["plex_sso"].update({"enabled": True, "linked_plex_account_id": "plex-123", "linked_username": "plexadmin"})
    _bind_config(monkeypatch, plex_api, cfg)
    monkeypatch.setattr(
        plex_api.authPlex,
        "check_flow",
        lambda *_args, **_kwargs: {
            "ok": True,
            "pending": False,
            "remember_me": False,
            "flow_nonce_hash": plex_api.authPlex._sha256_hex("flow-nonce"),
            "identity": {"id": "plex-999", "username": "stranger", "email": "", "thumb": ""},
        },
    )

    req = _request("/api/app-auth/plex/check", headers={"cookie": f"{plex_api.FLOW_COOKIE_NAME}=flow-nonce"})
    resp = plex_api.api_plex_check(req, {"state": "bad"})

    assert resp.status_code == 403
    assert _json_body(resp)["error"] == "This Plex account is not linked for CrossWatch sign-in"
    assert cfg["app_auth"]["sessions"] == []


def test_plex_link_check_requires_existing_app_session(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    _bind_config(monkeypatch, plex_api, cfg)

    req = _request("/api/app-auth/plex/link/check")
    resp = plex_api.api_plex_link_check(req, {"state": "missing"})

    assert resp.status_code == 401
    assert _json_body(resp)["error"] == "Unauthorized"


def test_plex_link_check_persists_linked_identity(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    _bind_config(monkeypatch, plex_api, cfg)
    monkeypatch.setattr(
        plex_api.authPlex,
        "check_flow",
        lambda *_args, **_kwargs: {
            "ok": True,
            "pending": False,
            "flow_nonce_hash": plex_api.authPlex._sha256_hex("flow-nonce"),
            "identity": {"id": "plex-abc", "username": "plexowner", "email": "owner@example.com", "thumb": "https://img"},
        },
    )

    seed_req = _request("/api/app-auth/login")
    token, _exp = auth._issue_session(cfg, seed_req)

    req = _request(
        "/api/app-auth/plex/link/check",
        headers={"cookie": f"{auth.COOKIE_NAME}={token}; {plex_api.FLOW_COOKIE_NAME}=flow-nonce"},
    )
    resp = plex_api.api_plex_link_check(req, {"state": "ok"})

    assert resp.status_code == 200
    body = _json_body(resp)
    assert body["ok"] is True
    assert cfg["app_auth"]["plex_sso"]["enabled"] is True
    assert cfg["app_auth"]["plex_sso"]["linked_plex_account_id"] == "plex-abc"
    assert cfg["app_auth"]["plex_sso"]["linked_username"] == "plexowner"


def test_plex_start_requires_linked_account_for_login(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    _bind_config(monkeypatch, plex_api, cfg)

    req = _request("/api/app-auth/plex/start")
    resp = plex_api.api_plex_start(req, {})

    assert resp.status_code == 400
    assert _json_body(resp)["error"] == "Plex sign-in is not linked yet"


def test_plex_start_sets_flow_cookie(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    cfg["app_auth"]["plex_sso"].update({"enabled": True, "linked_plex_account_id": "plex-123", "linked_username": "plexadmin"})
    _bind_config(monkeypatch, plex_api, cfg)
    monkeypatch.setattr(
        plex_api.authPlex,
        "start_flow",
        lambda *_args, **_kwargs: {"ok": True, "state": "abc", "pin_id": "pin1", "auth_url": "https://app.plex.tv/auth", "expires_at": 123},
    )

    req = _request("/api/app-auth/plex/start")
    resp = plex_api.api_plex_start(req, {})

    assert resp.status_code == 200
    assert f"{plex_api.FLOW_COOKIE_NAME}=" in _all_set_cookie_headers(resp)


def test_plex_start_allows_reasonable_unauthenticated_starts(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    _enable_plex_sso(cfg)
    _bind_config(monkeypatch, plex_api, cfg)
    calls: list[str] = []

    def fake_post(_url, **_kwargs):
        calls.append(_url)
        return _PlexPinResponse(f"pin-{len(calls)}", f"CODE{len(calls)}")

    monkeypatch.setattr(plex_api.authPlex.requests, "post", fake_post)

    responses = [
        plex_api.api_plex_start(_request("/api/app-auth/plex/start", client=("198.51.100.10", 4000 + i)), {})
        for i in range(6)
    ]

    assert [resp.status_code for resp in responses] == [200] * 6
    assert len(calls) == 6
    assert len(plex_api.authPlex._PENDING_FLOWS) == 6


def test_plex_start_throttles_excessive_starts_from_one_client(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    _enable_plex_sso(cfg)
    _bind_config(monkeypatch, plex_api, cfg)
    calls: list[str] = []

    def fake_post(_url, **_kwargs):
        calls.append(_url)
        return _PlexPinResponse(f"pin-{len(calls)}", f"CODE{len(calls)}")

    monkeypatch.setattr(plex_api.authPlex, "START_RATE_LIMIT", 2)
    monkeypatch.setattr(plex_api.authPlex.requests, "post", fake_post)
    req = _request("/api/app-auth/plex/start", client=("198.51.100.20", 5000))

    first = plex_api.api_plex_start(req, {})
    second = plex_api.api_plex_start(req, {})
    third = plex_api.api_plex_start(req, {})

    assert [first.status_code, second.status_code, third.status_code] == [200, 200, 429]
    assert _json_body(third)["retry_after"] >= 1
    assert len(calls) == 2
    assert len(plex_api.authPlex._PENDING_FLOWS) == 2


def test_plex_start_pending_flow_global_cap_is_enforced(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    _enable_plex_sso(cfg)
    _bind_config(monkeypatch, plex_api, cfg)
    plex_api.authPlex._PENDING_FLOWS.update(
        {
            "a": {"expires_at": 1 << 40},
            "b": {"expires_at": 1 << 40},
        }
    )
    calls: list[str] = []

    monkeypatch.setattr(plex_api.authPlex, "MAX_PENDING_FLOWS", 2)
    monkeypatch.setattr(plex_api.authPlex.requests, "post", lambda *_args, **_kwargs: calls.append("post") or _PlexPinResponse())

    resp = plex_api.api_plex_start(_request("/api/app-auth/plex/start", client=("198.51.100.30", 5000)), {})

    assert resp.status_code == 503
    assert _json_body(resp)["retry_after"] >= 1
    assert calls == []
    assert len(plex_api.authPlex._PENDING_FLOWS) == 2


def test_plex_start_prunes_expired_flows_before_capacity_check(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    _enable_plex_sso(cfg)
    _bind_config(monkeypatch, plex_api, cfg)
    plex_api.authPlex._PENDING_FLOWS["old"] = {"expires_at": 99}

    monkeypatch.setattr(plex_api.authPlex, "MAX_PENDING_FLOWS", 1)
    monkeypatch.setattr(plex_api.authPlex, "_now", lambda: 100)
    monkeypatch.setattr(plex_api.authPlex.requests, "post", lambda *_args, **_kwargs: _PlexPinResponse())

    resp = plex_api.api_plex_start(_request("/api/app-auth/plex/start", client=("198.51.100.40", 5000)), {})

    assert resp.status_code == 200
    assert "old" not in plex_api.authPlex._PENDING_FLOWS
    assert len(plex_api.authPlex._PENDING_FLOWS) == 1


def test_plex_start_rate_limit_is_per_client(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    _enable_plex_sso(cfg)
    _bind_config(monkeypatch, plex_api, cfg)
    calls: list[str] = []

    def fake_post(_url, **_kwargs):
        calls.append(_url)
        return _PlexPinResponse(f"pin-{len(calls)}", f"CODE{len(calls)}")

    monkeypatch.setattr(plex_api.authPlex, "START_RATE_LIMIT", 1)
    monkeypatch.setattr(plex_api.authPlex.requests, "post", fake_post)

    client_a_1 = plex_api.api_plex_start(_request("/api/app-auth/plex/start", client=("198.51.100.50", 5000)), {})
    client_a_2 = plex_api.api_plex_start(_request("/api/app-auth/plex/start", client=("198.51.100.50", 5001)), {})
    client_b_1 = plex_api.api_plex_start(_request("/api/app-auth/plex/start", client=("198.51.100.51", 5000)), {})

    assert [client_a_1.status_code, client_a_2.status_code, client_b_1.status_code] == [200, 429, 200]
    assert len(calls) == 2
    assert len(plex_api.authPlex._PENDING_FLOWS) == 2


def test_plex_start_issues_pin_after_config_update_lock(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    _enable_plex_sso(cfg)
    monkeypatch.setattr(plex_api, "load_config", lambda: cfg)
    in_update = {"active": False}
    observed = {"post_inside_update": None}

    def fake_update(mutator):
        in_update["active"] = True
        try:
            result = mutator(cfg)
        finally:
            in_update["active"] = False
        return cfg, result

    def fake_post(_url, **_kwargs):
        observed["post_inside_update"] = in_update["active"]
        return _PlexPinResponse()

    monkeypatch.setattr(plex_api.app_auth, "_update_config", fake_update)
    monkeypatch.setattr(plex_api.authPlex.requests, "post", fake_post)

    resp = plex_api.api_plex_start(_request("/api/app-auth/plex/start"), {})

    assert resp.status_code == 200
    assert observed["post_inside_update"] is False


def test_plex_start_failed_outbound_does_not_consume_pending_capacity(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    _enable_plex_sso(cfg)
    _bind_config(monkeypatch, plex_api, cfg)
    calls = {"n": 0}

    def fake_post(_url, **_kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("plex unavailable")
        return _PlexPinResponse("pin-ok", "OKOK")

    monkeypatch.setattr(plex_api.authPlex, "MAX_PENDING_FLOWS", 1)
    monkeypatch.setattr(plex_api.authPlex.requests, "post", fake_post)

    first = plex_api.api_plex_start(_request("/api/app-auth/plex/start", client=("198.51.100.60", 5000)), {})
    second = plex_api.api_plex_start(_request("/api/app-auth/plex/start", client=("198.51.100.60", 5001)), {})

    assert first.status_code == 502
    assert second.status_code == 200
    assert len(plex_api.authPlex._PENDING_FLOWS) == 1
    assert plex_api.authPlex._PENDING_STARTS_IN_FLIGHT == 0


def test_plex_start_then_callback_login_behavior_still_issues_session(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    _enable_plex_sso(cfg)
    _bind_config(monkeypatch, plex_api, cfg)

    class Response:
        def __init__(self, data: dict) -> None:
            self._data = data

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return self._data

    monkeypatch.setattr(plex_api.authPlex.requests, "post", lambda *_args, **_kwargs: Response({"id": "pin-1", "code": "ABCD"}))

    def fake_get(url, **_kwargs):
        if str(url).endswith("/pin-1"):
            return Response({"authToken": "plex-token"})
        return Response({"id": "plex-123", "username": "plexadmin", "email": "plex@example.com", "thumb": ""})

    monkeypatch.setattr(plex_api.authPlex.requests, "get", fake_get)

    start = plex_api.api_plex_start(_request("/api/app-auth/plex/start"), {})
    start_body = _json_body(start)
    flow_cookie = _all_set_cookie_headers(start).split(f"{plex_api.FLOW_COOKIE_NAME}=", 1)[1].split(";", 1)[0]
    check = plex_api.api_plex_check(
        _request("/api/app-auth/plex/check", headers={"cookie": f"{plex_api.FLOW_COOKIE_NAME}={flow_cookie}"}),
        {"state": start_body["state"]},
    )

    assert start.status_code == 200
    assert check.status_code == 200
    assert _json_body(check)["ok"] is True
    assert len(cfg["app_auth"]["sessions"]) == 1
    assert plex_api.authPlex._PENDING_FLOWS == {}


def test_plex_sso_start_reuses_provider_client_id(monkeypatch) -> None:
    from services import authPlex

    cfg = _auth_cfg()
    cfg["plex"] = {"client_id": "main-plex-client"}
    cfg["app_auth"]["plex_sso"]["client_id"] = "old-sso-client"
    sent: dict[str, str] = {}

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"id": "pin-1", "code": "ABCD"}

    def fake_post(_url, *, headers, **_kwargs):
        sent.update(headers)
        return Response()

    monkeypatch.setattr(authPlex.requests, "post", fake_post)

    res = authPlex.start_flow(
        cfg,
        intent="login",
        callback_url="https://crosswatch.example/callback",
        flow_nonce_hash="nonce-hash",
    )

    assert sent["X-Plex-Client-Identifier"] == "main-plex-client"
    assert "clientID=main-plex-client" in res["auth_url"]
    assert cfg["app_auth"]["plex_sso"]["client_id"] == "main-plex-client"


def test_plex_login_check_requires_matching_flow_cookie(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    cfg["app_auth"]["plex_sso"].update({"enabled": True, "linked_plex_account_id": "plex-123", "linked_username": "plexadmin"})
    _bind_config(monkeypatch, plex_api, cfg)
    monkeypatch.setattr(
        plex_api.authPlex,
        "check_flow",
        lambda *_args, **_kwargs: {
            "ok": True,
            "pending": False,
            "remember_me": False,
            "flow_nonce_hash": plex_api.authPlex._sha256_hex("expected-nonce"),
            "identity": {"id": "plex-123", "username": "plexadmin", "email": "", "thumb": ""},
        },
    )

    req = _request("/api/app-auth/plex/check")
    resp = plex_api.api_plex_check(req, {"state": "ok"})

    assert resp.status_code == 400
    assert _json_body(resp)["error"] == "Plex sign-in expired. Start again."
    assert cfg["app_auth"]["sessions"] == []
