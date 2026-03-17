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
    raw_headers = [(b"host", b"testserver")]
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


def _json_body(resp) -> dict:
    return json.loads(resp.body.decode("utf-8"))


def test_plex_login_check_issues_cookie_for_linked_identity(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    cfg["app_auth"]["plex_sso"].update({"enabled": True, "linked_plex_account_id": "plex-123", "linked_username": "plexadmin"})
    monkeypatch.setattr(plex_api, "load_config", lambda: cfg)
    monkeypatch.setattr(plex_api, "save_config", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        plex_api.authPlex,
        "check_flow",
        lambda *_args, **_kwargs: {
            "ok": True,
            "pending": False,
            "remember_me": True,
            "identity": {"id": "plex-123", "username": "plexadmin", "email": "plex@example.com", "thumb": ""},
        },
    )

    req = _request("/api/app-auth/plex/check")
    resp = plex_api.api_plex_check(req, {"state": "ok"})

    assert resp.status_code == 200
    assert _json_body(resp)["ok"] is True
    assert len(cfg["app_auth"]["sessions"]) == 1
    set_cookie = resp.headers.get("set-cookie", "")
    assert "cw_auth=" in set_cookie
    assert "Max-Age=" in set_cookie


def test_plex_login_check_rejects_wrong_identity(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    cfg["app_auth"]["plex_sso"].update({"enabled": True, "linked_plex_account_id": "plex-123", "linked_username": "plexadmin"})
    monkeypatch.setattr(plex_api, "load_config", lambda: cfg)
    monkeypatch.setattr(plex_api, "save_config", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        plex_api.authPlex,
        "check_flow",
        lambda *_args, **_kwargs: {
            "ok": True,
            "pending": False,
            "remember_me": False,
            "identity": {"id": "plex-999", "username": "stranger", "email": "", "thumb": ""},
        },
    )

    req = _request("/api/app-auth/plex/check")
    resp = plex_api.api_plex_check(req, {"state": "bad"})

    assert resp.status_code == 403
    assert _json_body(resp)["error"] == "This Plex account is not linked for CrossWatch sign-in"
    assert cfg["app_auth"]["sessions"] == []


def test_plex_link_check_requires_existing_app_session(monkeypatch) -> None:
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    monkeypatch.setattr(plex_api, "load_config", lambda: cfg)

    req = _request("/api/app-auth/plex/link/check")
    resp = plex_api.api_plex_link_check(req, {"state": "missing"})

    assert resp.status_code == 401
    assert _json_body(resp)["error"] == "Unauthorized"


def test_plex_link_check_persists_linked_identity(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from api import authPlexAPI as plex_api

    cfg = _auth_cfg()
    monkeypatch.setattr(plex_api, "load_config", lambda: cfg)
    monkeypatch.setattr(plex_api, "save_config", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        plex_api.authPlex,
        "check_flow",
        lambda *_args, **_kwargs: {
            "ok": True,
            "pending": False,
            "identity": {"id": "plex-abc", "username": "plexowner", "email": "owner@example.com", "thumb": "https://img"},
        },
    )

    seed_req = _request("/api/app-auth/login")
    token, _exp = auth._issue_session(cfg, seed_req)

    req = _request(
        "/api/app-auth/plex/link/check",
        headers={"cookie": f"{auth.COOKIE_NAME}={token}"},
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
    monkeypatch.setattr(plex_api, "load_config", lambda: cfg)

    req = _request("/api/app-auth/plex/start")
    resp = plex_api.api_plex_start(req, {})

    assert resp.status_code == 400
    assert _json_body(resp)["error"] == "Plex sign-in is not linked yet"
