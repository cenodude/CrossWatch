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


def _pending(claim: str, value: str, iss: str = "https://accounts.google.com") -> dict:
    return {"expect_iss": iss, "expect_claim": claim, "expect_value": value, "expect_set_at": 1_700_000_200}


def _callback(oidc_api):
    req = _request("/api/app-auth/oidc/callback", headers={"cookie": f"{oidc_api.FLOW_COOKIE_NAME}=flow-nonce"})
    return oidc_api.api_oidc_callback(req, state="ok", code="code", error="")


def _pending_identity(**extra) -> dict:
    return {"iss": "https://accounts.google.com", "sub": "authentik-42", "username": "pascal", "preferred_username": "pascal", "email": "", "picture": "", **extra}


def test_oidc_callback_binds_pending_username_on_first_sign_in(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user(_pending("preferred_username", "pascal"))}
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, _pending_identity())

    resp = _callback(oidc_api)

    assert resp.status_code == 302
    assert cfg["app_auth"]["sessions"][0]["user_id"] == "u1"
    link = cfg["app_auth"]["users"]["u1"]["oidc"]
    assert link["sub"] == "authentik-42"
    assert "expect_claim" not in link and "expect_value" not in link


def test_oidc_bound_link_ignores_later_username_claim(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user(_pending("preferred_username", "pascal"))}
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, _pending_identity())
    assert _callback(oidc_api).status_code == 302

    _bind_callback(monkeypatch, oidc_api, _pending_identity(sub="someone-else"))
    resp = _callback(oidc_api)

    assert resp.status_code == 403
    assert cfg["app_auth"]["users"]["u1"]["oidc"]["sub"] == "authentik-42"


def test_oidc_pending_email_requires_verified_claim(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user(_pending("email", "pascal@example.com"))}
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, _pending_identity(preferred_username="", email="Pascal@example.com", email_verified=False))

    assert _callback(oidc_api).status_code == 403
    assert "sub" not in cfg["app_auth"]["users"]["u1"]["oidc"]

    _bind_callback(monkeypatch, oidc_api, _pending_identity(preferred_username="", email="Pascal@example.com", email_verified=True))

    assert _callback(oidc_api).status_code == 302
    assert cfg["app_auth"]["users"]["u1"]["oidc"]["sub"] == "authentik-42"


def test_oidc_pending_claim_needs_same_issuer(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user(_pending("preferred_username", "pascal", iss="https://old-idp.example"))}
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, _pending_identity())

    assert _callback(oidc_api).status_code == 403
    assert cfg["app_auth"]["sessions"] == []


def test_oidc_pending_claim_skips_identity_linked_to_disabled_user(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    disabled = _managed_user(_managed_identity_record("authentik-42"))
    disabled["enabled"] = False
    cfg["app_auth"]["users"] = {"u1": _managed_user(_pending("preferred_username", "pascal")), "u2": disabled}
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, _pending_identity())

    assert _callback(oidc_api).status_code == 403
    assert "sub" not in cfg["app_auth"]["users"]["u1"]["oidc"]


def test_oidc_pending_claim_makes_sign_in_available() -> None:
    from services import authOidc

    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user(_pending("preferred_username", "pascal"))}

    assert authOidc.login_available(cfg) is True


def test_set_expectation_rejects_duplicate_pending_value() -> None:
    import pytest
    from services import authOidc

    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user(_pending("preferred_username", "pascal")), "u2": _managed_user()}

    with pytest.raises(ValueError):
        authOidc.set_expectation(cfg, cfg["app_auth"]["users"]["u2"], {"claim": "preferred_username", "value": "pascal"})


def test_set_expectation_username_is_case_sensitive_email_is_not() -> None:
    import pytest
    from services import authOidc

    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user(_pending("preferred_username", "alice")), "u2": _managed_user(), "u3": _managed_user(_pending("email", "bob@example.com")), "u4": _managed_user()}
    users = cfg["app_auth"]["users"]

    authOidc.set_expectation(cfg, users["u2"], {"claim": "preferred_username", "value": "Alice"})
    with pytest.raises(ValueError):
        authOidc.set_expectation(cfg, users["u4"], {"claim": "email", "value": "BOB@example.com"})


def test_oidc_pending_username_does_not_match_other_case(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user(_pending("preferred_username", "Alice"))}
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, _pending_identity(username="alice", preferred_username="alice"))

    assert _callback(oidc_api).status_code == 403
    assert "sub" not in cfg["app_auth"]["users"]["u1"]["oidc"]


def test_set_expectation_subject_links_directly_and_checks_conflicts() -> None:
    import pytest
    from services import authOidc

    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user(), "u2": _managed_user()}
    users = cfg["app_auth"]["users"]

    out = authOidc.set_expectation(cfg, users["u1"], {"claim": "sub", "value": "authentik-42"})

    assert out["linked"] is True
    assert users["u1"]["oidc"]["iss"] == "https://accounts.google.com"
    assert users["u1"]["oidc"]["sub"] == "authentik-42"
    with pytest.raises(ValueError):
        authOidc.set_expectation(cfg, users["u2"], {"claim": "sub", "value": "authentik-42"})
    with pytest.raises(ValueError):
        authOidc.set_expectation(cfg, users["u1"], {"claim": "preferred_username", "value": "pascal"})


def test_set_expectation_clears_pending_value() -> None:
    from services import authOidc

    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user(_pending("preferred_username", "pascal"))}

    authOidc.set_expectation(cfg, cfg["app_auth"]["users"]["u1"], {"claim": "", "value": ""})

    assert "oidc" not in cfg["app_auth"]["users"]["u1"]


def test_profile_login_sets_pending_oidc_and_users_list_reports_it(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user()}
    _bind_config(monkeypatch, oidc_api, cfg)
    monkeypatch.setattr(auth, "_profile_id_valid", lambda *_a, **_k: True)

    auth.apply_profile_login(cfg, "profile-1", {"user_id": "u1", "oidc": {"claim": "preferred_username", "value": "pascal"}})

    assert cfg["app_auth"]["users"]["u1"]["oidc"]["expect_value"] == "pascal"
    token = _login_admin(cfg)
    body = _json_body(auth.api_users_all(_request("/api/app-auth/users", headers={"cookie": f"{auth.COOKIE_NAME}={token}"})))
    row = next(item for item in body["items"] if item["id"] == "u1")
    assert body["oidc_configured"] is True
    assert row["oidc"]["pending_claim"] == "preferred_username"
    assert row["oidc"]["pending_value"] == "pascal"


def test_profile_login_rejects_bad_oidc_claim(monkeypatch) -> None:
    import pytest
    from api import appAuthAPI as auth

    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user()}
    monkeypatch.setattr(auth, "_profile_id_valid", lambda *_a, **_k: True)

    with pytest.raises(auth.ProfileLoginError):
        auth.apply_profile_login(cfg, "profile-1", {"user_id": "u1", "oidc": {"claim": "name", "value": "pascal"}})


def test_identity_from_claims_keeps_matching_claims() -> None:
    from services import authOidc

    identity = authOidc._identity_from_claims("https://idp.example/", {"sub": "s1", "preferred_username": "pascal", "email": "p@example.com", "email_verified": "true"}, {})

    assert identity["preferred_username"] == "pascal"
    assert identity["email_verified"] is True


def test_identity_from_claims_takes_email_and_verified_from_same_source() -> None:
    from services import authOidc

    claims = {"sub": "s1", "email": "token@example.com", "email_verified": True}
    identity = authOidc._identity_from_claims("https://idp.example", claims, {"sub": "s1", "email": "userinfo@example.com"})

    assert identity["email"] == "userinfo@example.com"
    assert identity["email_verified"] is False


def _provision_cfg(access: str = "full") -> dict:
    cfg = _auth_cfg()
    cfg["app_auth"]["oidc"].update({"provision": True, "allowed_groups": ["crosswatch"], "groups_claim": "groups", "provision_access": access})
    return cfg


def test_oidc_provisioning_creates_login_profile_and_link(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api
    from cw_platform.provider_instances import list_user_profiles

    cfg = _provision_cfg("readonly")
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, _pending_identity(groups=["crosswatch"]))

    resp = _callback(oidc_api)

    assert resp.status_code == 302
    assert resp.headers["location"] == "/profile"
    [(uid, raw)] = cfg["app_auth"]["users"].items()
    assert raw["username"] == "pascal"
    assert raw["role"] == "user"
    assert raw["permissions"]["write"] is False
    assert raw["oidc"]["sub"] == "authentik-42"
    assert raw["oidc_provisioned"] is True
    assert [p["id"] for p in list_user_profiles(cfg)] == [raw["profile_id"]]
    assert cfg["app_auth"]["sessions"][0]["user_id"] == uid


def test_oidc_provisioning_requires_allowed_group(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _provision_cfg()
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, _pending_identity(groups=["family"]))

    assert _callback(oidc_api).status_code == 403
    assert not cfg["app_auth"].get("users")


def test_oidc_provisioning_off_does_not_create_logins(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _provision_cfg()
    cfg["app_auth"]["oidc"]["provision"] = False
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, _pending_identity(groups=["crosswatch"]))

    assert _callback(oidc_api).status_code == 403
    assert not cfg["app_auth"].get("users")


def test_oidc_provisioned_login_blocked_after_leaving_group(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _provision_cfg()
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, _pending_identity(groups=["crosswatch"]))
    assert _callback(oidc_api).status_code == 302

    _bind_callback(monkeypatch, oidc_api, _pending_identity(groups=[]))

    assert _callback(oidc_api).status_code == 403
    assert len(cfg["app_auth"]["users"]) == 1
    assert len(cfg["app_auth"]["sessions"]) == 1


def test_oidc_provisioning_picks_free_username(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _provision_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user()}
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, _pending_identity(groups=["crosswatch"]))

    assert _callback(oidc_api).status_code == 302
    names = sorted(raw["username"] for raw in cfg["app_auth"]["users"].values())
    assert names == ["pascal", "pascal-2"]


def test_oidc_provisioning_skips_identity_linked_to_disabled_user(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _provision_cfg()
    disabled = _managed_user(_managed_identity_record("authentik-42"))
    disabled["enabled"] = False
    cfg["app_auth"]["users"] = {"u1": disabled}
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, _pending_identity(groups=["crosswatch"]))

    assert _callback(oidc_api).status_code == 403
    assert list(cfg["app_auth"]["users"]) == ["u1"]


def test_oidc_provisioning_prefers_pending_expectation(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _provision_cfg()
    cfg["app_auth"]["users"] = {"u1": _managed_user(_pending("preferred_username", "pascal"))}
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, _pending_identity(groups=["crosswatch"]))

    assert _callback(oidc_api).status_code == 302
    assert list(cfg["app_auth"]["users"]) == ["u1"]
    assert "oidc_provisioned" not in cfg["app_auth"]["users"]["u1"]


def test_oidc_provisioning_blocked_by_disabled_pending_login(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _provision_cfg()
    disabled = _managed_user(_pending("preferred_username", "pascal"))
    disabled["enabled"] = False
    cfg["app_auth"]["users"] = {"u1": disabled}
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, _pending_identity(groups=["crosswatch"]))

    assert _callback(oidc_api).status_code == 403
    assert list(cfg["app_auth"]["users"]) == ["u1"]


def test_oidc_provisioned_login_blocked_when_no_groups_remain(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = _provision_cfg()
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, _pending_identity(groups=["crosswatch"]))
    assert _callback(oidc_api).status_code == 302

    cfg["app_auth"]["oidc"].update({"provision": False, "allowed_groups": []})

    assert _callback(oidc_api).status_code == 403


def test_oidc_provisioned_user_self_relink_keeps_group_check(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from api import authOidcAPI as oidc_api

    cfg = _provision_cfg()
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, _pending_identity(groups=["crosswatch"]))
    assert _callback(oidc_api).status_code == 302
    [uid] = list(cfg["app_auth"]["users"])
    token = _login_managed(cfg, uid)
    headers = {"cookie": f"{auth.COOKIE_NAME}={token}"}

    oidc_api.api_oidc_unlink(_request("/api/app-auth/oidc/unlink", method="POST", headers=headers), {})
    monkeypatch.setattr(oidc_api.authOidc, "consume_link", lambda *_a, **_k: {"ok": True, "pending": False, "target_user_id": uid, "identity": _pending_identity(sub="authentik-99")})
    assert oidc_api.api_oidc_link_check(_request("/api/app-auth/oidc/link/check", method="POST", headers=headers), {"state": "ok"}).status_code == 200

    assert cfg["app_auth"]["users"][uid]["oidc_provisioned"] is True
    _bind_callback(monkeypatch, oidc_api, _pending_identity(sub="authentik-99", groups=[]))
    assert _callback(oidc_api).status_code == 403


def test_oidc_admin_relink_releases_group_check(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from api import authOidcAPI as oidc_api

    cfg = _provision_cfg()
    _bind_config(monkeypatch, oidc_api, cfg)
    _bind_callback(monkeypatch, oidc_api, _pending_identity(groups=["crosswatch"]))
    assert _callback(oidc_api).status_code == 302
    [uid] = list(cfg["app_auth"]["users"])
    headers = {"cookie": f"{auth.COOKIE_NAME}={_login_admin(cfg)}"}
    monkeypatch.setattr(oidc_api.authOidc, "consume_link", lambda *_a, **_k: {"ok": True, "pending": False, "target_user_id": uid, "identity": _pending_identity()})

    assert oidc_api.api_oidc_link_check(_request("/api/app-auth/oidc/link/check", method="POST", headers=headers), {"state": "ok"}).status_code == 200

    assert "oidc_provisioned" not in cfg["app_auth"]["users"][uid]
    _bind_callback(monkeypatch, oidc_api, _pending_identity(groups=[]))
    assert _callback(oidc_api).status_code == 302


def test_identity_groups_are_not_truncated() -> None:
    from services import authOidc

    cfg = _provision_cfg()
    groups = [f"g{n}" for n in range(80)] + ["crosswatch"]
    identity = authOidc._identity_from_claims("https://accounts.google.com", {"sub": "s1", "groups": groups}, {})

    assert authOidc.identity_in_allowed_groups(cfg, identity) is True


def test_oidc_config_rejects_provisioning_without_groups(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from api import authOidcAPI as oidc_api

    cfg = _auth_cfg()
    _bind_config(monkeypatch, oidc_api, cfg)
    token = _login_admin(cfg)
    req = _request("/api/app-auth/oidc/config", method="POST", headers={"cookie": f"{auth.COOKIE_NAME}={token}"})

    resp = oidc_api.api_oidc_save_config(req, {"enabled": True, "issuer": "https://accounts.google.com", "client_id": "client-abc", "keep_client_secret": True, "provision": True, "allowed_groups": ""})

    assert resp.status_code == 400
    assert "provision" not in cfg["app_auth"]["oidc"]


def test_oidc_config_keeps_provisioning_fields() -> None:
    from services import authOidc

    out = authOidc.sanitize_config({"provision": True, "allowed_groups": "crosswatch, family,crosswatch", "groups_claim": "", "provision_access": "admin"})

    assert out["provision"] is True
    assert out["allowed_groups"] == ["crosswatch", "family"]
    assert out["groups_claim"] == "groups"
    assert out["provision_access"] == "full"


def test_identity_from_claims_reads_custom_groups_claim() -> None:
    from services import authOidc

    identity = authOidc._identity_from_claims("https://idp.example", {"sub": "s1", "roles": ["crosswatch"], "groups": ["other"]}, {}, "roles")

    assert identity["groups"] == ["crosswatch"]


def test_identity_from_claims_ignores_userinfo_for_other_subject() -> None:
    from services import authOidc

    claims = {"sub": "s1", "preferred_username": "pascal"}
    identity = authOidc._identity_from_claims("https://idp.example", claims, {"sub": "s2", "preferred_username": "mallory", "email": "m@example.com", "email_verified": True})

    assert identity["preferred_username"] == "pascal"
    assert identity["email"] == ""
    assert identity["email_verified"] is False
