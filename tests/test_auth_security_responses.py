from __future__ import annotations

import json
from typing import Any, Callable

from starlette.requests import Request


def _auth_cfg() -> dict[str, Any]:
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
    cookies: dict[str, str] | None = None,
) -> Request:
    raw_headers = [(b"host", b"testserver"), (b"sec-fetch-site", b"same-origin")]
    if cookies:
        raw_headers.append((b"cookie", "; ".join(f"{k}={v}" for k, v in cookies.items()).encode("latin-1")))
    for k, v in (headers or {}).items():
        raw_headers.append((str(k).lower().encode("latin-1"), str(v).encode("latin-1")))
    return Request(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": method,
            "scheme": "http",
            "path": path,
            "raw_path": path.encode("latin-1"),
            "query_string": b"",
            "headers": raw_headers,
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
            "state": {},
        }
    )


def _json_body(resp) -> dict[str, Any]:
    return json.loads(resp.body.decode("utf-8"))


def _raise(exc: Exception) -> Callable[[Any], tuple[dict[str, Any], Any]]:
    def _boom(_mutator: Any) -> tuple[dict[str, Any], Any]:
        raise exc

    return _boom


def _admin_cookie(auth, cfg: dict[str, Any]) -> str:
    token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"))
    return f"{auth.COOKIE_NAME}={token}"


def test_oidc_callback_error_query_is_not_reflected() -> None:
    from api import authOidcAPI as oidc_api

    marker = "<script>alert(1)</script>"
    resp = oidc_api.api_oidc_callback(_request("/api/app-auth/oidc/callback", method="GET"), state="", code="", error=marker)
    body = resp.body.decode("utf-8")

    assert resp.status_code == 400
    assert marker not in body
    assert "OIDC sign-in was cancelled or failed." in body


def test_oidc_2fa_retry_uses_fixed_error_message() -> None:
    from api import authOidcAPI as oidc_api
    from services import authOidc

    token = "pending-token"
    nonce = "flow-nonce"
    oidc_api._PENDING_2FA.clear()
    oidc_api._PENDING_2FA[token] = {
        "flow_nonce_hash": authOidc._sha256_hex(nonce),
        "expires_at": oidc_api.app_auth._now() + 600,
    }
    try:
        resp = oidc_api.api_oidc_2fa_retry(
            _request(
                "/api/app-auth/oidc/2fa/retry",
                method="GET",
                cookies={oidc_api.FLOW_COOKIE_NAME: nonce, oidc_api.TOTP_COOKIE_NAME: token},
            ),
        )
    finally:
        oidc_api._PENDING_2FA.clear()

    body = resp.body.decode("utf-8")
    assert resp.status_code == 200
    assert token not in body
    assert "Invalid verification code" in body


def test_app_auth_mutation_errors_do_not_expose_exception_text(monkeypatch) -> None:
    from api import appAuthAPI as auth

    profile_id = "11111111111141118111111111111111"
    user_id = "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    secret = "traceback C:\\secret\\config.json <script>alert(1)</script>"
    cfg = _auth_cfg()
    cfg["user_profiles"] = {profile_id: {"label": "Pascal", "instances": {"PLEX": ["PLEX-P01"]}}}
    cfg["app_auth"]["users"] = {
        user_id: {
            "username": "pascal",
            "enabled": True,
            "role": "user",
            "profile_id": profile_id,
            "permissions": {"dashboard": True},
            "password": auth._password_hash("secrett2"),
        }
    }
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)
    cookie = _admin_cookie(auth, cfg)

    cases = [
        (
            ValueError(secret),
            lambda req: auth.api_users_create(req, {"username": "newuser", "password": "secrett3", "profile_id": profile_id}),
            409,
            "Unable to create user",
        ),
        (
            KeyError(secret),
            lambda req: auth.api_users_create(req, {"username": "newuser", "password": "secrett3", "profile_id": profile_id}),
            400,
            "Unable to create user",
        ),
        (
            RuntimeError(secret),
            lambda req: auth.api_users_update(req, user_id, {"username": "newname"}),
            409,
            "Unable to update user",
        ),
        (
            ValueError(secret),
            lambda req: auth.api_users_update(req, user_id, {"profile_id": profile_id}),
            400,
            "Unable to update user",
        ),
        (
            RuntimeError(secret),
            lambda req: auth.api_set_credentials(req, {"enabled": True, "username": "admin", "password": "secrett3"}),
            409,
            "Unable to update credentials",
        ),
        (
            ValueError(secret),
            lambda req: auth.api_set_credentials(req, {"enabled": True, "username": "admin", "password": "secrett3"}),
            400,
            "Unable to update credentials",
        ),
        (
            ValueError(secret),
            lambda req: auth.api_totp_verify(req, {"user_id": "administrator", "code": "000000"}),
            400,
            "Unable to verify two-factor setup",
        ),
        (
            RuntimeError(secret),
            lambda req: auth.api_totp_verify(req, {"user_id": "administrator", "code": "000000"}),
            400,
            "Unable to verify two-factor setup",
        ),
    ]

    for exc, call, expected_status, expected_error in cases:
        monkeypatch.setattr(auth, "_update_config", _raise(exc))
        resp = call(_request("/api/app-auth/security-test", headers={"cookie": cookie}))
        body = resp.body.decode("utf-8")
        assert resp.status_code == expected_status
        assert secret not in body
        assert _json_body(resp)["error"] == expected_error


def test_snapshots_api_errors_do_not_expose_exception_text(monkeypatch) -> None:
    from api import snapshotsAPI

    secret = "Traceback C:\\secret\\snapshots.json <script>alert(1)</script>"

    def _boom() -> list[dict[str, Any]]:
        raise RuntimeError(secret)

    monkeypatch.setattr(snapshotsAPI, "list_snapshots", _boom)

    resp = snapshotsAPI.api_snapshots_list(_request("/api/snapshots/list", method="GET"))
    body = resp.body.decode("utf-8")

    assert resp.status_code == 400
    assert secret not in body
    assert _json_body(resp)["error"] == "snapshot_list_failed"


def test_watchlist_delete_errors_do_not_expose_exception_text(monkeypatch) -> None:
    from api import watchlistAPI
    import cw_platform.config_base as config_base

    secret = "Traceback C:\\secret\\watchlist.json <script>alert(1)</script>"

    monkeypatch.setattr(config_base, "load_config", lambda: {})
    monkeypatch.setattr(
        watchlistAPI,
        "delete_watchlist_item",
        lambda **_kwargs: {
            "ok": False,
            "status": "error",
            "provider": "PLEX",
            "error": secret,
            "details": {"default": {"ok": False, "error": secret}},
        },
    )

    resp = watchlistAPI.api_watchlist_delete(
        "tmdb:603",
        request=_request("/api/watchlist/tmdb:603", method="DELETE"),
        provider="PLEX",
    )
    body = resp.body.decode("utf-8")
    payload = _json_body(resp)

    assert resp.status_code == 400
    assert secret not in body
    assert payload["error"] == "delete_failed"
    assert payload["details"]["default"]["error"] == "delete_failed"


def test_oidc_link_conflict_does_not_expose_exception_text(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from api import authOidcAPI as oidc_api

    secret = "traceback C:\\secret\\oidc.json <script>alert(1)</script>"
    cfg = _auth_cfg()
    monkeypatch.setattr(oidc_api, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)
    cookie = _admin_cookie(auth, cfg)
    monkeypatch.setattr(oidc_api.authOidc, "consume_link", lambda _state: {"ok": True, "identity": {"sub": "sub-1"}})
    monkeypatch.setattr(auth, "_update_config", _raise(ValueError(secret)))

    resp = oidc_api.api_oidc_link_check(_request("/api/app-auth/oidc/link/check", headers={"cookie": cookie}), {"state": "ok"})
    body = resp.body.decode("utf-8")

    assert resp.status_code == 409
    assert secret not in body
    assert _json_body(resp)["error"] == "This OIDC account is already linked to another CrossWatch account"


def test_plex_link_conflict_does_not_expose_exception_text(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from api import authPlexAPI as plex_api

    secret = "traceback C:\\secret\\plex.json <script>alert(1)</script>"
    cfg = _auth_cfg()
    nonce = "flow-nonce"
    monkeypatch.setattr(plex_api, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)
    cookie = _admin_cookie(auth, cfg)
    monkeypatch.setattr(
        plex_api.authPlex,
        "check_flow",
        lambda *_args, **_kwargs: {
            "ok": True,
            "pending": False,
            "flow_nonce_hash": plex_api.authPlex._sha256_hex(nonce),
            "identity": {"id": "plex-1", "username": "plexuser"},
        },
    )
    monkeypatch.setattr(auth, "_update_config", _raise(ValueError(secret)))

    resp = plex_api.api_plex_link_check(
        _request(
            "/api/app-auth/plex/link/check",
            headers={"cookie": f"{cookie}; {plex_api.FLOW_COOKIE_NAME}={nonce}"},
        ),
        {"state": "ok"},
    )
    body = resp.body.decode("utf-8")

    assert resp.status_code == 409
    assert secret not in body
    assert _json_body(resp)["error"] == "This Plex account is already linked to another CrossWatch account"
