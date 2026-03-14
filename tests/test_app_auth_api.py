from __future__ import annotations

import json

from starlette.requests import Request


def _auth_cfg(*, enabled: bool = True, remember_session_enabled: bool = False, remember_session_days: int = 30) -> dict:
    from api import appAuthAPI as auth

    salt = b"0123456789abcdef"
    password = "secrett1"
    return {
        "security": {},
        "app_auth": {
            "enabled": enabled,
            "username": "admin",
            "reset_required": False,
            "remember_session_enabled": remember_session_enabled,
            "remember_session_days": remember_session_days,
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


def test_successful_login_clears_failed_login_state(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg()
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)
    auth._LOGIN_FAILS.clear()

    req = _request("/api/app-auth/login")

    r = auth.api_login(req, {"username": "admin", "password": "wrong"})
    assert r.status_code == 401

    assert len(auth._LOGIN_FAILS) == 1
    next(iter(auth._LOGIN_FAILS.values()))["until"] = 0

    ok = auth.api_login(req, {"username": "admin", "password": "secrett1"})
    assert ok.status_code == 200
    assert _json_body(ok)["ok"] is True
    assert auth._LOGIN_FAILS == {}


def test_logout_all_rejects_wrong_origin(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg()
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)

    seed_req = _request("/api/app-auth/login")
    token, _exp = auth._issue_session(cfg, seed_req)
    assert len(cfg["app_auth"]["sessions"]) == 1

    bad_req = _request(
        "/api/app-auth/logout-all",
        headers={
            "cookie": f"{auth.COOKIE_NAME}={token}",
            "origin": "http://evil.example",
        },
    )
    bad = auth.api_logout_all(bad_req)
    assert bad.status_code == 403
    assert _json_body(bad)["error"] == "Origin mismatch"
    assert len(cfg["app_auth"]["sessions"]) == 1


def test_logout_all_accepts_forwarded_origin_for_trusted_proxy(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg()
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(auth, "_is_trusted_proxy_request", lambda _request: True)

    seed_req = _request("/api/app-auth/login")
    token, _exp = auth._issue_session(cfg, seed_req)
    assert len(cfg["app_auth"]["sessions"]) == 1

    ok_req = _request(
        "/api/app-auth/logout-all",
        headers={
            "cookie": f"{auth.COOKIE_NAME}={token}",
            "origin": "https://app.example.com",
            "x-forwarded-proto": "https",
            "x-forwarded-host": "app.example.com",
        },
    )
    ok = auth.api_logout_all(ok_req)
    assert ok.status_code == 200
    assert _json_body(ok)["ok"] is True
    assert cfg["app_auth"]["sessions"] == []


def test_bootstrap_credentials_still_work_without_origin(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = {
        "security": {},
        "app_auth": {
            "enabled": False,
            "username": "",
            "reset_required": False,
            "remember_session_enabled": False,
            "remember_session_days": 30,
            "password": {"scheme": "pbkdf2_sha256", "iterations": 260_000, "salt": "", "hash": ""},
            "session": {"token_hash": "", "expires_at": 0},
            "sessions": [],
            "last_login_at": 0,
        },
    }
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)

    req = _request("/api/app-auth/credentials")
    r = auth.api_set_credentials(req, {"enabled": True, "username": "admin", "password": "secrett1"})
    assert r.status_code == 200
    data = _json_body(r)
    assert data["ok"] is True
    assert data["enabled"] is True
    assert cfg["app_auth"]["username"] == "admin"
    assert cfg["app_auth"]["sessions"]


def test_login_sets_session_cookie_when_remember_disabled(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg(remember_session_enabled=False, remember_session_days=30)
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)

    req = _request("/api/app-auth/login")
    resp = auth.api_login(req, {"username": "admin", "password": "secrett1"})

    assert resp.status_code == 200
    set_cookie = resp.headers.get("set-cookie", "")
    assert "Max-Age=" not in set_cookie
    assert "expires=" not in set_cookie.lower()


def test_login_sets_persistent_cookie_when_remember_enabled(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg(remember_session_enabled=True, remember_session_days=45)
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)

    req = _request("/api/app-auth/login")
    resp = auth.api_login(req, {"username": "admin", "password": "secrett1", "remember_me": True})

    assert resp.status_code == 200
    set_cookie = resp.headers.get("set-cookie", "")
    assert "Max-Age=3888000" in set_cookie
    assert "expires=" in set_cookie.lower()


def test_credentials_clamp_and_store_remember_session_settings(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg(enabled=False)
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)

    req = _request("/api/app-auth/credentials")
    resp = auth.api_set_credentials(
        req,
        {
            "enabled": True,
            "username": "admin",
            "password": "secrett1",
            "remember_session_enabled": True,
            "remember_session_days": 999,
        },
    )

    assert resp.status_code == 200
    assert cfg["app_auth"]["remember_session_enabled"] is True
    assert cfg["app_auth"]["remember_session_days"] == 365


def test_credentials_clear_reset_required_flag(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg(enabled=False)
    cfg["app_auth"]["reset_required"] = True
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)

    req = _request("/api/app-auth/credentials")
    resp = auth.api_set_credentials(
        req,
        {
            "enabled": True,
            "username": "admin",
            "password": "secrett1",
        },
    )

    assert resp.status_code == 200
    assert cfg["app_auth"]["reset_required"] is False


def test_credentials_mark_upgrade_pending_when_config_outdated(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg(enabled=False)
    cfg["version"] = "0.9.13"
    monkeypatch.setattr(auth, "_current_version_text", lambda: "0.9.14")
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)

    req = _request("/api/app-auth/credentials")
    resp = auth.api_set_credentials(
        req,
        {
            "enabled": True,
            "username": "admin",
            "password": "secrett1",
        },
    )

    assert resp.status_code == 200
    assert cfg["ui"]["_pending_upgrade_from_version"] == "0.9.13"


def test_status_reports_not_authenticated_while_reset_is_pending(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg(enabled=False)
    cfg["app_auth"]["reset_required"] = True
    monkeypatch.setattr(auth, "load_config", lambda: cfg)

    req = _request("/api/app-auth/status", method="GET")
    resp = auth.api_status(req)

    assert resp.status_code == 200
    data = _json_body(resp)
    assert data["reset_required"] is True
    assert data["authenticated"] is False


def test_setup_lock_required_for_upgrade_without_auth(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg(enabled=False)
    cfg["version"] = "0.9.11"
    monkeypatch.setattr(auth, "_current_version_text", lambda: "0.9.14")

    assert auth.setup_lock_required(cfg) is True


def test_setup_lock_not_required_when_up_to_date_without_auth(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg(enabled=False)
    cfg["version"] = "0.9.14"
    monkeypatch.setattr(auth, "_current_version_text", lambda: "0.9.14")

    assert auth.setup_lock_required(cfg) is False


def test_credentials_reject_too_short_password(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg(enabled=False)
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)

    req = _request("/api/app-auth/credentials")
    resp = auth.api_set_credentials(
        req,
        {
            "enabled": True,
            "username": "admin",
            "password": "short1",
        },
    )

    assert resp.status_code == 400
    assert _json_body(resp)["error"] == f"Password must be at least {auth.MIN_PASSWORD_LENGTH} characters"


def test_login_shows_help_banner_and_60s_timeout_after_three_failures(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg()
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)
    auth._LOGIN_FAILS.clear()

    req = _request("/api/app-auth/login")

    assert auth.api_login(req, {"username": "admin", "password": "wrong1"}).status_code == 401
    assert auth.api_login(req, {"username": "admin", "password": "wrong2"}).status_code == 401
    third = auth.api_login(req, {"username": "admin", "password": "wrong3"})

    assert third.status_code == 429
    data = _json_body(third)
    assert data["retry_after"] == 60
    assert data["show_help_banner"] is True
    assert data["forgot_help_url"] == auth.FORGOT_HELP_URL


def test_login_timeout_steps_up_to_five_and_ten_minutes(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg()
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)
    auth._LOGIN_FAILS.clear()

    req = _request("/api/app-auth/login")

    last = None
    for i in range(6):
        last = auth.api_login(req, {"username": "admin", "password": f"bad{i}"})
        if i < 5:
            rec = next(iter(auth._LOGIN_FAILS.values()))
            rec["until"] = 0

    assert last is not None
    assert last.status_code == 429
    assert _json_body(last)["retry_after"] == 300

    for i in range(7, 11):
        rec = next(iter(auth._LOGIN_FAILS.values()))
        rec["until"] = 0
        last = auth.api_login(req, {"username": "admin", "password": f"bad{i}"})

    assert last is not None
    assert last.status_code == 429
    assert _json_body(last)["retry_after"] == 600
