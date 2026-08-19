from __future__ import annotations

import json

from starlette.requests import Request


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
            "oidc": {
                "enabled": True,
                "issuer": "https://accounts.google.com",
                "client_id": "client-abc",
                "client_secret": "shh",
                "scopes": "openid profile email",
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


def _admin_identity_record(sub: str = "google-admin") -> dict:
    return {
        "iss": "https://accounts.google.com",
        "sub": sub,
        "username": "adminoidc",
        "email": "admin@example.com",
        "picture": "https://img/admin",
        "linked_at": 1_700_000_000,
    }


def _managed_user(oidc: dict | None = None) -> dict:
    raw = {
        "username": "pascal",
        "enabled": True,
        "role": "user",
        "profile_id": "profile-1",
        "permissions": {"dashboard": True, "watchlist": True, "playback": True, "write": False},
    }
    if oidc is not None:
        raw["oidc"] = oidc
    return raw


def _managed_identity_record(sub: str = "google-managed") -> dict:
    return {
        "iss": "https://accounts.google.com",
        "sub": sub,
        "username": "pascaloidc",
        "email": "pascal@example.com",
        "picture": "https://img/pascal",
        "linked_at": 1_700_000_100,
    }


def _request(
    path: str,
    *,
    method: str = "GET",
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


def _bind_config(monkeypatch, oidc_api, cfg: dict) -> None:
    from api import appAuthAPI as auth

    monkeypatch.setattr(oidc_api, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)


def _json_body(resp) -> dict:
    return json.loads(resp.body.decode("utf-8"))


def _all_set_cookie_headers(resp) -> str:
    return "\n".join(
        value.decode("latin-1")
        for key, value in getattr(resp, "raw_headers", [])
        if key.decode("latin-1").lower() == "set-cookie"
    )


def _login_admin(cfg: dict) -> str:
    from api import appAuthAPI as auth

    token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login", method="POST"))
    return token


def _login_managed(cfg: dict, user_id: str) -> str:
    from api import appAuthAPI as auth

    raw = cfg["app_auth"]["users"][user_id]
    token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login", method="POST"), auth._public_user(user_id, raw))
    return token


def _status(oidc_api, token: str | None = None):
    from api import appAuthAPI as auth

    headers = {"cookie": f"{auth.COOKIE_NAME}={token}"} if token else None
    return oidc_api.api_oidc_status(_request("/api/app-auth/oidc/status", headers=headers))


def test_oidc_status_reports_admin_linked_identity(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["oidc_identity"] = _admin_identity_record()
    _bind_config(monkeypatch, oidc_api, cfg)

    body = _json_body(_status(oidc_api, _login_admin(cfg)))

    assert body["linked"] is True
    assert body["configured"] is True
    assert body["login_available"] is True
    assert body["issuer"] == "https://accounts.google.com"
    assert body["linked_username"] == "adminoidc"
    assert body["linked_email"] == "admin@example.com"
    assert body["linked_picture"] == "https://img/admin"
    assert body["linked_at"] == 1_700_000_000


def test_oidc_status_admin_without_identity_is_not_linked(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    _bind_config(monkeypatch, oidc_api, cfg)

    body = _json_body(_status(oidc_api, _login_admin(cfg)))

    assert body["linked"] is False
    assert body["configured"] is True
    assert body["login_available"] is False
    assert body["linked_username"] == ""
    assert body["linked_email"] == ""
    assert body["linked_at"] == 0


def test_oidc_status_admin_never_reads_provider_config_as_identity(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["oidc"].update({"iss": "https://accounts.google.com", "sub": "not-an-identity", "username": "bogus"})
    _bind_config(monkeypatch, oidc_api, cfg)

    body = _json_body(_status(oidc_api, _login_admin(cfg)))

    assert body["linked"] is False
    assert body["linked_username"] == ""


def test_oidc_status_reports_managed_user_link(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user(_managed_identity_record())}
    _bind_config(monkeypatch, oidc_api, cfg)

    body = _json_body(_status(oidc_api, _login_managed(cfg, "u1")))

    assert body["linked"] is True
    assert body["issuer"] == ""
    assert body["linked_username"] == "pascaloidc"
    assert body["linked_email"] == "pascal@example.com"
    assert body["linked_at"] == 1_700_000_100


def test_oidc_status_managed_user_does_not_inherit_admin_link(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["oidc_identity"] = _admin_identity_record()
    cfg["app_auth"]["users"] = {"u1": _managed_user()}
    _bind_config(monkeypatch, oidc_api, cfg)

    body = _json_body(_status(oidc_api, _login_managed(cfg, "u1")))

    assert body["linked"] is False
    assert body["linked_username"] == ""
    assert body["linked_email"] == ""


def test_oidc_status_hides_identity_fields_when_signed_out(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["oidc_identity"] = _admin_identity_record()
    _bind_config(monkeypatch, oidc_api, cfg)

    body = _json_body(_status(oidc_api))

    assert body["linked"] is True
    assert body["issuer"] == ""
    assert body["linked_username"] == ""
    assert body["linked_email"] == ""
    assert body["linked_at"] == 0


def test_oidc_link_check_persists_admin_identity(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    _bind_config(monkeypatch, oidc_api, cfg)
    monkeypatch.setattr(
        oidc_api.authOidc,
        "consume_link",
        lambda *_args, **_kwargs: {
            "ok": True,
            "pending": False,
            "target_user_id": "",
            "identity": {"iss": "https://accounts.google.com", "sub": "google-admin", "username": "adminoidc", "email": "admin@example.com", "picture": ""},
        },
    )

    token = _login_admin(cfg)
    req = _request("/api/app-auth/oidc/link/check", method="POST", headers={"cookie": f"{auth.COOKIE_NAME}={token}"})
    resp = oidc_api.api_oidc_link_check(req, {"state": "ok"})

    assert resp.status_code == 200
    body = _json_body(resp)
    assert body["ok"] is True
    assert body["linked"] is True
    assert cfg["app_auth"]["oidc_identity"]["sub"] == "google-admin"
    assert "sub" not in cfg["app_auth"]["oidc"]
    assert _json_body(_status(oidc_api, token))["linked"] is True


def test_oidc_link_check_persists_managed_identity(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user()}
    _bind_config(monkeypatch, oidc_api, cfg)
    monkeypatch.setattr(
        oidc_api.authOidc,
        "consume_link",
        lambda *_args, **_kwargs: {
            "ok": True,
            "pending": False,
            "target_user_id": "u1",
            "identity": {"iss": "https://accounts.google.com", "sub": "google-managed", "username": "pascaloidc", "email": "pascal@example.com", "picture": ""},
        },
    )

    token = _login_admin(cfg)
    req = _request("/api/app-auth/oidc/link/check", method="POST", headers={"cookie": f"{auth.COOKIE_NAME}={token}"})
    resp = oidc_api.api_oidc_link_check(req, {"state": "ok"})

    assert resp.status_code == 200
    assert _json_body(resp)["linked"] is True
    assert cfg["app_auth"]["users"]["u1"]["oidc"]["sub"] == "google-managed"
    assert "oidc_identity" not in cfg["app_auth"]
    assert _json_body(_status(oidc_api, _login_managed(cfg, "u1")))["linked_username"] == "pascaloidc"


def test_oidc_link_check_rejects_identity_linked_to_another_account(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user(_managed_identity_record("google-shared"))}
    _bind_config(monkeypatch, oidc_api, cfg)
    monkeypatch.setattr(
        oidc_api.authOidc,
        "consume_link",
        lambda *_args, **_kwargs: {
            "ok": True,
            "pending": False,
            "target_user_id": "",
            "identity": {"iss": "https://accounts.google.com", "sub": "google-shared", "username": "adminoidc", "email": "admin@example.com", "picture": ""},
        },
    )

    token = _login_admin(cfg)
    req = _request("/api/app-auth/oidc/link/check", method="POST", headers={"cookie": f"{auth.COOKIE_NAME}={token}"})
    resp = oidc_api.api_oidc_link_check(req, {"state": "ok"})

    assert resp.status_code == 409
    assert _json_body(resp)["error"] == oidc_api._oidc_link_conflict_error()
    assert "oidc_identity" not in cfg["app_auth"]


def test_oidc_link_check_requires_existing_app_session(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    _bind_config(monkeypatch, oidc_api, cfg)

    resp = oidc_api.api_oidc_link_check(_request("/api/app-auth/oidc/link/check", method="POST"), {"state": "missing"})

    assert resp.status_code == 401
    assert _json_body(resp)["error"] == "Unauthorized"


def test_oidc_unlink_clears_admin_identity(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["oidc_identity"] = _admin_identity_record()
    _bind_config(monkeypatch, oidc_api, cfg)

    token = _login_admin(cfg)
    req = _request("/api/app-auth/oidc/unlink", method="POST", headers={"cookie": f"{auth.COOKIE_NAME}={token}"})
    resp = oidc_api.api_oidc_unlink(req, {})

    assert resp.status_code == 200
    assert _json_body(resp)["linked"] is False
    assert "oidc_identity" not in cfg["app_auth"]
    assert _json_body(_status(oidc_api, token))["linked"] is False


def test_oidc_unlink_clears_managed_identity(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["oidc_identity"] = _admin_identity_record()
    cfg["app_auth"]["users"] = {"u1": _managed_user(_managed_identity_record())}
    _bind_config(monkeypatch, oidc_api, cfg)

    token = _login_managed(cfg, "u1")
    req = _request("/api/app-auth/oidc/unlink", method="POST", headers={"cookie": f"{auth.COOKIE_NAME}={token}"})
    resp = oidc_api.api_oidc_unlink(req, {})

    assert resp.status_code == 200
    assert _json_body(resp)["linked"] is False
    assert "oidc" not in cfg["app_auth"]["users"]["u1"]
    assert cfg["app_auth"]["oidc_identity"]["sub"] == "google-admin"


def _bind_callback(monkeypatch, oidc_api, identity: dict) -> None:
    monkeypatch.setattr(oidc_api.authOidc, "flow_nonce_hash", lambda *_a, **_k: oidc_api.authOidc._sha256_hex("flow-nonce"))
    monkeypatch.setattr(
        oidc_api.authOidc,
        "check_callback",
        lambda *_args, **_kwargs: {
            "ok": True,
            "pending": False,
            "intent": "login",
            "remember_me": True,
            "target_user_id": "",
            "next_url": "/",
            "identity": identity,
        },
    )


def test_oidc_callback_signs_in_linked_admin(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["oidc_identity"] = _admin_identity_record()
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, {"iss": "https://accounts.google.com", "sub": "google-admin", "username": "adminoidc", "email": "admin@example.com", "picture": ""})

    req = _request("/api/app-auth/oidc/callback", headers={"cookie": f"{oidc_api.FLOW_COOKIE_NAME}=flow-nonce"})
    resp = oidc_api.api_oidc_callback(req, state="ok", code="code", error="")

    assert resp.status_code == 302
    assert len(cfg["app_auth"]["sessions"]) == 1
    assert cfg["app_auth"]["sessions"][0]["user_id"] == "administrator"
    assert "cw_auth=" in _all_set_cookie_headers(resp)


def test_oidc_callback_signs_in_linked_managed_user(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user(_managed_identity_record())}
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, {"iss": "https://accounts.google.com", "sub": "google-managed", "username": "pascaloidc", "email": "pascal@example.com", "picture": ""})

    req = _request("/api/app-auth/oidc/callback", headers={"cookie": f"{oidc_api.FLOW_COOKIE_NAME}=flow-nonce"})
    resp = oidc_api.api_oidc_callback(req, state="ok", code="code", error="")

    assert resp.status_code == 302
    assert resp.headers["location"] == "/profile"
    assert cfg["app_auth"]["sessions"][0]["user_id"] == "u1"
    assert "cw_auth=" in _all_set_cookie_headers(resp)


def test_oidc_callback_rejects_unlinked_identity(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["oidc_identity"] = _admin_identity_record()
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, {"iss": "https://accounts.google.com", "sub": "stranger", "username": "nope", "email": "", "picture": ""})

    req = _request("/api/app-auth/oidc/callback", headers={"cookie": f"{oidc_api.FLOW_COOKIE_NAME}=flow-nonce"})
    resp = oidc_api.api_oidc_callback(req, state="ok", code="code", error="")

    assert resp.status_code == 403
    assert cfg["app_auth"]["sessions"] == []


def test_oidc_callback_requires_matching_flow_cookie(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["oidc_identity"] = _admin_identity_record()
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, {"iss": "https://accounts.google.com", "sub": "google-admin", "username": "adminoidc", "email": "", "picture": ""})

    resp = oidc_api.api_oidc_callback(_request("/api/app-auth/oidc/callback"), state="ok", code="code", error="")

    assert resp.status_code == 400
    assert cfg["app_auth"]["sessions"] == []
