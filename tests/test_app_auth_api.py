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
    sec_fetch_site: str | None = "same-origin",
) -> Request:
    raw_headers = [(b"host", b"testserver")]
    if sec_fetch_site:
        raw_headers.append((b"sec-fetch-site", sec_fetch_site.encode("latin-1")))
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


def _roundtrip_app_auth_user(monkeypatch, tmp_path, user: dict) -> dict:
    from cw_platform import config_base

    uid = "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {uid: user}
    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    monkeypatch.setattr(config_base, "_get_cipher", lambda *, create: None)

    config_base.save_config(cfg)

    return config_base.load_config()["app_auth"]["users"][uid]


def _roundtrip_app_auth_root(monkeypatch, tmp_path, extra: dict) -> dict:
    from cw_platform import config_base

    cfg = _auth_cfg()
    cfg["app_auth"].update(extra)
    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    monkeypatch.setattr(config_base, "_get_cipher", lambda *, create: None)

    config_base.save_config(cfg)

    return config_base.load_config()["app_auth"]


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


def test_managed_user_profile_fields_survive_config_roundtrip(monkeypatch, tmp_path) -> None:
    password = _auth_cfg()["app_auth"]["password"]
    user = {
        "username": "pascal",
        "enabled": True,
        "role": "user",
        "profile_id": "11111111111141118111111111111111",
        "permissions": {"dashboard": True, "watchlist": True, "playback": True, "write": True},
        "password": password,
        "display_name": "  Pascal   van den Berg  ",
        "recovery_codes": [{"hash": "one", "used_at": 0}, "bad", {"hash": "two"}],
        "avatar": {
            "file": "abcdefabcdefabcdefabcdefabcdefab.png",
            "content_type": "image/png",
            "updated_at": "1786660000000",
        },
    }

    saved = _roundtrip_app_auth_user(monkeypatch, tmp_path, user)

    assert saved["display_name"] == "Pascal van den Berg"
    assert saved["permissions"]["write"] is True
    assert saved["recovery_codes"] == [{"hash": "one", "used_at": 0}, {"hash": "two"}]
    assert saved["avatar"] == {
        "file": "abcdefabcdefabcdefabcdefabcdefab.png",
        "content_type": "image/png",
        "updated_at": 1786660000000,
    }


def test_managed_user_bogus_avatar_is_stripped_on_config_roundtrip(monkeypatch, tmp_path) -> None:
    password = _auth_cfg()["app_auth"]["password"]
    user = {
        "username": "pascal",
        "enabled": True,
        "role": "user",
        "profile_id": "11111111111141118111111111111111",
        "permissions": {"dashboard": True, "watchlist": True, "playback": True},
        "password": password,
        "avatar": {
            "file": "../abcdefabcdefabcdefabcdefabcdefab.png",
            "content_type": "image/png",
            "updated_at": 123,
        },
    }

    saved = _roundtrip_app_auth_user(monkeypatch, tmp_path, user)

    assert "avatar" not in saved


def test_admin_profile_fields_survive_config_roundtrip(monkeypatch, tmp_path) -> None:
    saved = _roundtrip_app_auth_root(
        monkeypatch,
        tmp_path,
        {
            "display_name": "  CrossWatch   Admin  ",
            "recovery_codes": [{"hash": "one"}, "bad", {"hash": "two", "used_at": 0}],
            "avatar": {
                "file": "1234567890abcdef1234567890abcdef.webp",
                "content_type": "image/webp",
                "updated_at": "1786660000000",
            },
        },
    )

    assert saved["display_name"] == "CrossWatch Admin"
    assert saved["recovery_codes"] == [{"hash": "one"}, {"hash": "two", "used_at": 0}]
    assert saved["avatar"] == {
        "file": "1234567890abcdef1234567890abcdef.webp",
        "content_type": "image/webp",
        "updated_at": 1786660000000,
    }


def test_admin_bogus_avatar_is_stripped_on_config_roundtrip(monkeypatch, tmp_path) -> None:
    saved = _roundtrip_app_auth_root(
        monkeypatch,
        tmp_path,
        {
            "avatar": {
                "file": "1234567890abcdef1234567890abcdef.svg",
                "content_type": "image/svg+xml",
                "updated_at": 123,
            },
        },
    )

    assert "avatar" not in saved


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

    req = _request("/api/app-auth/credentials", sec_fetch_site=None)
    r = auth.api_set_credentials(req, {"enabled": True, "username": "admin", "password": "secrett1"})
    assert r.status_code == 200
    data = _json_body(r)
    assert data["ok"] is True
    assert data["enabled"] is True
    assert cfg["app_auth"]["username"] == "admin"
    assert cfg["app_auth"]["sessions"]


def test_bootstrap_credentials_clear_untrusted_pre_auth_tokens(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg(enabled=False)
    cfg["app_auth"]["username"] = ""
    cfg["app_auth"]["password"] = {"scheme": "pbkdf2_sha256", "iterations": 260_000, "salt": "", "hash": ""}
    cfg["app_auth"]["api_tokens"] = [
        {"id": "legacy1", "token_hash": {"scheme": "pbkdf2_sha256", "iterations": 1, "salt": "a", "hash": "b"}},
        {"id": "abcdefabcdefabcd", "version": 2, "token_digest": "a" * 64, "digest_scheme": "hmac_sha256"},
    ]
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)

    req = _request("/api/app-auth/credentials")
    resp = auth.api_set_credentials(req, {"enabled": True, "username": "admin", "password": "secrett1"})

    assert resp.status_code == 200
    assert cfg["app_auth"]["api_tokens"] == []


def test_bootstrap_credentials_preserve_local_cli_pre_auth_tokens(monkeypatch) -> None:
    from api import appAuthAPI as auth

    local_token = {
        "id": "abcdefabcdefabcd",
        "version": 2,
        "token_digest": "a" * 64,
        "digest_scheme": "hmac_sha256",
        "created_via": "local_cli",
    }
    cfg = _auth_cfg(enabled=False)
    cfg["app_auth"]["username"] = ""
    cfg["app_auth"]["password"] = {"scheme": "pbkdf2_sha256", "iterations": 260_000, "salt": "", "hash": ""}
    cfg["app_auth"]["api_tokens"] = [local_token]
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)

    req = _request("/api/app-auth/credentials")
    resp = auth.api_set_credentials(req, {"enabled": True, "username": "admin", "password": "secrett1"})

    assert resp.status_code == 200
    assert cfg["app_auth"]["api_tokens"] == [local_token]


def test_credentials_update_preserves_existing_tokens(monkeypatch) -> None:
    from api import appAuthAPI as auth

    existing_token = {
        "id": "abcdefabcdefabcd",
        "version": 2,
        "token_digest": "a" * 64,
        "digest_scheme": "hmac_sha256",
        "created_via": "api",
    }
    cfg = _auth_cfg()
    cfg["app_auth"]["api_tokens"] = [existing_token]
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)

    session, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"))
    req = _request("/api/app-auth/credentials", headers={"cookie": f"{auth.COOKIE_NAME}={session}"})
    resp = auth.api_set_credentials(req, {"enabled": True, "username": "admin", "password": "newpass1"})

    assert resp.status_code == 200
    assert cfg["app_auth"]["api_tokens"] == [existing_token]


def test_disabling_credentials_clears_tokens(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg()
    cfg["app_auth"]["api_tokens"] = [{"id": "abcdefabcdefabcd", "version": 2, "token_digest": "a" * 64}]
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)

    session, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"))
    req = _request("/api/app-auth/credentials", headers={"cookie": f"{auth.COOKIE_NAME}={session}"})
    resp = auth.api_set_credentials(req, {"enabled": False, "username": "admin"})

    assert resp.status_code == 200
    assert cfg["app_auth"]["api_tokens"] == []


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


def test_setup_lock_required_when_up_to_date_without_auth(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg(enabled=False)
    cfg["version"] = "0.9.14"
    monkeypatch.setattr(auth, "_current_version_text", lambda: "0.9.14")

    assert auth.setup_lock_required(cfg) is True


def test_setup_lock_blocks_regular_api_until_credentials_exist(monkeypatch) -> None:
    from fastapi.testclient import TestClient

    import crosswatch

    cfg = _auth_cfg(enabled=False)
    cfg["app_auth"]["username"] = ""
    cfg["app_auth"]["password"] = {"scheme": "pbkdf2_sha256", "iterations": 260_000, "salt": "", "hash": ""}
    monkeypatch.setattr(crosswatch, "load_config", lambda: cfg)

    client = TestClient(crosswatch.app)

    blocked = client.get("/api/editor")
    assert blocked.status_code == 403
    assert blocked.json()["error"] == "Authentication setup required"

    assert client.get("/api/app-auth/status").status_code == 200
    assert client.get("/api/version").status_code == 200


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


def test_admin_totp_requires_code_after_password(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg()
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(auth, "_now", lambda: 1_800_000_000)
    auth._LOGIN_FAILS.clear()

    admin_token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"))
    admin_req = _request("/api/app-auth/totp/setup", headers={"cookie": f"{auth.COOKIE_NAME}={admin_token}"})
    setup = auth.api_totp_setup(admin_req, {"user_id": "administrator"})
    setup_data = _json_body(setup)
    secret = setup_data["secret"]
    code = auth._hotp(secret, 1_800_000_000 // auth.TOTP_STEP_SECONDS)
    verified = auth.api_totp_verify(admin_req, {"user_id": "administrator", "code": code})

    missing = auth.api_login(_request("/api/app-auth/login"), {"username": "admin", "password": "secrett1"})
    bad = auth.api_login(_request("/api/app-auth/login", client=("127.0.0.2", 12345)), {"username": "admin", "password": "secrett1", "totp_code": "000000"})
    good = auth.api_login(_request("/api/app-auth/login", client=("127.0.0.3", 12345)), {"username": "admin", "password": "secrett1", "totp_code": code})

    assert setup.status_code == 200
    assert setup_data["otpauth_url"].startswith("otpauth://totp/")
    assert verified.status_code == 200
    assert cfg["app_auth"]["totp"]["enabled"] is True
    assert missing.status_code == 401
    assert _json_body(missing)["requires_2fa"] is True
    assert bad.status_code == 401
    assert _json_body(bad)["requires_2fa"] is True
    assert good.status_code == 200
    assert _json_body(good)["ok"] is True


def test_admin_can_enable_totp_for_managed_user(monkeypatch) -> None:
    from api import appAuthAPI as auth

    user_id = "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    profile_id = "11111111111141118111111111111111"
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
    monkeypatch.setattr(auth, "_now", lambda: 1_800_000_000)
    auth._LOGIN_FAILS.clear()

    admin_token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"), auth._admin_identity(cfg["app_auth"]))
    admin_req = _request("/api/app-auth/totp/setup", headers={"cookie": f"{auth.COOKIE_NAME}={admin_token}"})
    setup = auth.api_totp_setup(admin_req, {"user_id": user_id})
    secret = _json_body(setup)["secret"]
    code = auth._hotp(secret, 1_800_000_000 // auth.TOTP_STEP_SECONDS)
    verified = auth.api_totp_verify(admin_req, {"user_id": user_id, "code": code})
    list_resp = auth.api_users_all(_request("/api/app-auth/users", method="GET", headers={"cookie": f"{auth.COOKIE_NAME}={admin_token}"}))
    login = auth.api_login(_request("/api/app-auth/login"), {"username": "pascal", "password": "secrett2", "totp_code": code})

    assert setup.status_code == 200
    assert verified.status_code == 200
    assert cfg["app_auth"]["users"][user_id]["totp"]["enabled"] is True
    assert next(row for row in _json_body(list_resp)["items"] if row["id"] == user_id)["totp_enabled"] is True
    assert login.status_code == 200
    assert _json_body(login)["user"]["id"] == user_id


def test_managed_user_cannot_manage_totp(monkeypatch) -> None:
    from api import appAuthAPI as auth

    user_id = "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {
        user_id: {
            "username": "pascal",
            "enabled": True,
            "role": "user",
            "profile_id": "11111111111141118111111111111111",
            "permissions": {"dashboard": True},
            "password": auth._password_hash("secrett2"),
        }
    }
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)

    user = auth._public_user(user_id, cfg["app_auth"]["users"][user_id])
    token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"), user)
    req = _request("/api/app-auth/totp/setup", headers={"cookie": f"{auth.COOKIE_NAME}={token}"})
    resp = auth.api_totp_setup(req, {})

    assert resp.status_code == 401
    assert _json_body(resp)["error"] == "Unauthorized"


def test_managed_profile_totp_setup_returns_real_qr_svg(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from api import profileAPI as profile_api

    user_id = "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    profile_id = "11111111111141118111111111111111"
    cfg = _auth_cfg()
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
    monkeypatch.setattr(profile_api, "load_config", lambda: cfg)
    monkeypatch.setattr(profile_api, "update_config", lambda mutator: (cfg, mutator(cfg)))

    user = auth._public_user(user_id, cfg["app_auth"]["users"][user_id])
    token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"), user)
    req = _request("/api/profile/totp/setup", headers={"cookie": f"{auth.COOKIE_NAME}={token}"})

    resp = profile_api.api_profile_totp_setup(req)
    body = _json_body(resp)

    assert resp.status_code == 200
    assert body["otpauth_url"].startswith("otpauth://totp/")
    assert body["qr_svg"].startswith("<svg")
    assert "<path" in body["qr_svg"]
    assert 'shape-rendering="crispEdges"' not in body["qr_svg"]


def test_admin_can_create_profile_linked_user_and_user_can_login(monkeypatch) -> None:
    from api import appAuthAPI as auth

    profile_id = "11111111111141118111111111111111"
    cfg = _auth_cfg()
    cfg["user_profiles"] = {profile_id: {"label": "Pascal", "instances": {"PLEX": ["PLEX-P01"]}}}
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)
    auth._LOGIN_FAILS.clear()

    admin_token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"))
    admin_req = _request("/api/app-auth/users", headers={"cookie": f"{auth.COOKIE_NAME}={admin_token}"})

    created = auth.api_users_create(
        admin_req,
        {"username": "pascal", "password": "secrett2", "profile_id": profile_id, "permissions": {"watchlist": False}},
    )
    data = _json_body(created)
    user_id = data["user"]["id"]

    login = auth.api_login(_request("/api/app-auth/login"), {"username": "pascal", "password": "secrett2"})
    login_body = _json_body(login)

    assert created.status_code == 200
    assert data["user"]["profile_id"] == profile_id
    assert data["user"]["permissions"]["watchlist"] is False
    assert login.status_code == 200
    assert login_body["user"]["id"] == user_id
    assert login_body["user"]["is_admin"] is False
    assert cfg["app_auth"]["sessions"][-1]["user_id"] == user_id
    assert cfg["app_auth"]["sessions"][-1]["profile_id"] == profile_id


def test_created_managed_user_defaults_to_full_access(monkeypatch) -> None:
    from api import appAuthAPI as auth

    profile_id = "11111111111141118111111111111111"
    cfg = _auth_cfg()
    cfg["user_profiles"] = {profile_id: {"label": "Pascal", "instances": {"PLEX": ["PLEX-P01"]}}}
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)

    admin_token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"))
    admin_req = _request("/api/app-auth/users", headers={"cookie": f"{auth.COOKIE_NAME}={admin_token}"})

    created = auth.api_users_create(
        admin_req,
        {"username": "pascal", "password": "secrett2", "profile_id": profile_id},
    )
    restricted = auth.api_users_create(
        admin_req,
        {"username": "readonly", "password": "secrett3", "profile_id": profile_id, "permissions": {"write": False}},
    )

    created_perms = _json_body(created)["user"]["permissions"]
    restricted_perms = _json_body(restricted)["user"]["permissions"]

    assert created.status_code == 200
    assert created_perms == {"dashboard": True, "watchlist": True, "playback": True, "write": True}
    assert restricted.status_code == 200
    assert restricted_perms["write"] is False
    assert restricted_perms["dashboard"] is True
    assert restricted_perms["watchlist"] is True
    assert restricted_perms["playback"] is True


def test_managed_user_cannot_manage_users(monkeypatch) -> None:
    from api import appAuthAPI as auth

    profile_id = "11111111111141118111111111111111"
    cfg = _auth_cfg()
    cfg["user_profiles"] = {profile_id: {"label": "Pascal", "instances": {"PLEX": ["PLEX-P01"]}}}
    cfg["app_auth"]["users"] = {
        "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa": {
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

    user = auth._public_user("aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa", cfg["app_auth"]["users"]["aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"])
    token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"), user)
    req = _request("/api/app-auth/users", headers={"cookie": f"{auth.COOKIE_NAME}={token}"})

    resp = auth.api_users_all(req)

    assert resp.status_code == 401
    assert _json_body(resp)["error"] == "Unauthorized"


def test_managed_user_cannot_apply_auth_restart(monkeypatch) -> None:
    from api import appAuthAPI as auth

    profile_id = "11111111111141118111111111111111"
    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {
        "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa": {
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

    user = auth._public_user("aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa", cfg["app_auth"]["users"]["aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"])
    token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"), user)
    req = _request("/api/app-auth/apply-now", headers={"cookie": f"{auth.COOKIE_NAME}={token}"})

    resp = auth.api_apply_now(req, {})

    assert resp.status_code == 401
    assert _json_body(resp)["error"] == "Unauthorized"


def test_non_admin_api_policy_blocks_sensitive_routes() -> None:
    from api import appAuthAPI as auth

    assert auth.non_admin_api_allowed("/api/dashboard/widgets", "GET") is True
    assert auth.non_admin_api_allowed("/api/state/wall", "GET") is True
    assert auth.non_admin_api_allowed("/api/state/wallpaper", "GET") is False
    assert auth.non_admin_api_allowed("/api/watchlist", "GET") is True
    assert auth.non_admin_api_allowed("/api/watchlist/", "GET") is True
    assert auth.non_admin_api_allowed("/api/watchlist-delete", "GET") is False
    assert auth.non_admin_api_allowed("/api/user-profiles", "GET") is True
    assert auth.non_admin_api_allowed("/api/config/meta", "GET") is True
    assert auth.non_admin_api_allowed("/api/watch/currently_watching", "GET") is True
    assert auth.non_admin_api_allowed("/api/playback_progress/providers", "GET") is True
    assert auth.non_admin_api_allowed("/api/playback_progress/settings", "GET") is True
    assert auth.non_admin_api_allowed("/api/playback_progress/items", "GET") is True
    assert auth.non_admin_api_allowed("/api/config", "GET") is False
    assert auth.non_admin_api_allowed("/api/status", "GET") is True
    assert auth.non_admin_api_allowed("/api/metadata/bulk", "POST") is True
    assert auth.non_admin_api_allowed("/api/metadata/search", "GET") is True
    assert auth.non_admin_api_allowed("/api/metadata/resolve", "POST") is True
    assert auth.non_admin_api_allowed("/api/provider-instances", "GET") is True
    assert auth.non_admin_api_allowed("/api/provider-instances/CROSSWATCH", "GET") is True
    assert auth.non_admin_api_allowed("/api/provider-instances/CROSSWATCH/CW-P01", "PATCH") is False
    assert auth.non_admin_api_allowed("/api/pairs", "GET") is True
    assert auth.non_admin_api_allowed("/api/pairs", "POST") is True
    assert auth.non_admin_api_allowed("/api/run", "POST") is True
    assert auth.non_admin_api_allowed("/api/analyzer/state", "GET") is True
    assert auth.non_admin_api_allowed("/api/events/search", "GET") is True
    assert auth.non_admin_api_allowed("/api/export/options", "GET") is True
    assert auth.non_admin_api_allowed("/api/import/commit", "POST") is True
    assert auth.non_admin_api_allowed("/api/editor", "POST") is True
    assert auth.non_admin_api_allowed("/api/playlists/mappings", "POST") is True
    assert auth.non_admin_api_allowed("/api/snapshots/create", "POST") is True
    assert auth.non_admin_api_allowed("/api/watch/start", "POST") is False
    assert auth.non_admin_api_allowed("/api/playback_progress/settings", "POST") is False
    assert auth.non_admin_api_allowed("/api/playback_progress/actions/remove", "POST") is True
    assert auth.non_admin_api_allowed("/api/watchlist/delete", "POST") is True
    assert auth.non_admin_api_allowed("/api/dashboard/widgets", "POST") is False
    assert auth.non_admin_api_allowed("/api/app-auth/credentials", "POST") is False


def test_global_unsafe_api_origin_gate_rejects_wrong_origin(monkeypatch) -> None:
    import crosswatch
    from api import appAuthAPI as auth

    cfg = _auth_cfg()
    token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"))
    bad = _request(
        "/api/config",
        method="POST",
        headers={"cookie": f"{auth.COOKIE_NAME}={token}", "origin": "http://evil.example"},
    )
    good = _request(
        "/api/config",
        method="POST",
        headers={"cookie": f"{auth.COOKIE_NAME}={token}", "origin": "http://testserver"},
    )

    assert crosswatch._unsafe_api_origin_blocked(cfg, bad, token) is True
    assert crosswatch._unsafe_api_origin_blocked(cfg, good, token) is False


def test_global_unsafe_api_origin_gate_rejects_wrong_referer(monkeypatch) -> None:
    import crosswatch
    from api import appAuthAPI as auth

    cfg = _auth_cfg()
    token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"))
    bad = _request(
        "/api/config",
        method="POST",
        headers={"cookie": f"{auth.COOKIE_NAME}={token}", "referer": "http://evil.example/settings"},
    )
    good = _request(
        "/api/config",
        method="POST",
        headers={"cookie": f"{auth.COOKIE_NAME}={token}", "referer": "http://testserver/settings"},
    )

    assert crosswatch._unsafe_api_origin_blocked(cfg, bad, token) is True
    assert crosswatch._unsafe_api_origin_blocked(cfg, good, token) is False


def test_fastapi_docs_are_disabled() -> None:
    import crosswatch

    assert crosswatch.app.docs_url is None
    assert crosswatch.app.redoc_url is None
    assert crosswatch.app.openapi_url is None


def test_cli_only_env_disables_webui_but_not_api(monkeypatch: pytest.MonkeyPatch) -> None:
    import crosswatch

    monkeypatch.setenv("CW_CLI_ONLY", "1")

    assert crosswatch._cli_only_mode() is True
    assert crosswatch._webui_disabled_path("/") is True
    assert crosswatch._webui_disabled_path("/login") is True
    assert crosswatch._webui_disabled_path("/assets/crosswatch.js") is True
    assert crosswatch._webui_disabled_path("/api/status") is False
    assert crosswatch._webui_disabled_path("/webhook/plex") is False
    assert crosswatch._webui_disabled_path("/callback/trakt") is False


def test_cli_only_mode_blocks_ui_response(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient

    import crosswatch

    monkeypatch.setenv("CW_CLI_ONLY", "1")
    monkeypatch.setattr(crosswatch, "load_config", lambda: {"app_auth": {"enabled": False}})

    res = TestClient(crosswatch.app).get("/", follow_redirects=False)

    assert res.status_code == 404
    assert "web UI is disabled" in res.text


def test_non_admin_permissions_gate_feature_reads() -> None:
    import crosswatch

    user = {"permissions": {"dashboard": True, "watchlist": False, "playback": True, "write": False}}

    assert crosswatch._non_admin_permission_allowed(user, "/api/dashboard/widgets") is True
    assert crosswatch._non_admin_permission_allowed(user, "/api/state/wall") is True
    assert crosswatch._non_admin_permission_allowed(user, "/api/activity/recent") is True
    assert crosswatch._non_admin_permission_allowed(user, "/api/watch/currently_watching") is True
    assert crosswatch._non_admin_permission_allowed(user, "/api/status") is True
    assert crosswatch._non_admin_permission_allowed(user, "/api/pairs") is True
    assert crosswatch._non_admin_permission_allowed(user, "/api/playback_progress/items") is True
    assert crosswatch._non_admin_permission_allowed(user, "/api/metadata/bulk", "POST") is True
    assert crosswatch._non_admin_permission_allowed(user, "/api/watchlist") is False
    assert crosswatch._non_admin_permission_allowed(user, "/api/watchlist/") is False
    assert crosswatch._non_admin_permission_allowed(user, "/api/run", "POST") is False
    assert crosswatch._non_admin_permission_allowed(user, "/api/watchlist/delete", "POST") is False
    assert crosswatch._non_admin_permission_allowed(user, "/api/analyzer/state", "GET") is False
    assert crosswatch._non_admin_permission_allowed(user, "/api/events/search", "GET") is False
    assert crosswatch._non_admin_permission_allowed(user, "/api/export/options", "GET") is False

    full = {"permissions": {"dashboard": True, "watchlist": True, "playback": True, "write": True}}
    assert crosswatch._non_admin_permission_allowed(full, "/api/run", "POST") is True
    assert crosswatch._non_admin_permission_allowed(full, "/api/pairs", "GET") is True
    assert crosswatch._non_admin_permission_allowed(full, "/api/analyzer/state", "GET") is True
    assert crosswatch._non_admin_permission_allowed(full, "/api/events/search", "GET") is True
    assert crosswatch._non_admin_permission_allowed(full, "/api/export/options", "GET") is True

    blocked = {"permissions": {"dashboard": False, "watchlist": True, "playback": False}}
    assert crosswatch._non_admin_permission_allowed(blocked, "/api/watch/currently_watching") is False
    assert crosswatch._non_admin_permission_allowed(blocked, "/api/playback_progress/items") is False
    assert crosswatch._non_admin_permission_allowed(blocked, "/api/metadata/bulk", "POST") is True


def test_non_admin_page_routes_are_not_gated_by_api_permissions() -> None:
    import crosswatch

    readonly = {"permissions": {"dashboard": True, "watchlist": False, "playback": False, "write": False}}
    writer = {"permissions": {"dashboard": True, "watchlist": True, "playback": True, "write": True}}
    none = {"permissions": {}}

    for user in (readonly, writer, none):
        assert crosswatch._non_admin_permission_allowed(user, "/") is True
        assert crosswatch._non_admin_permission_allowed(user, "/profile") is True
        assert crosswatch._non_admin_permission_allowed(user, "/login") is True


def test_full_access_managed_user_reaches_every_allowlisted_api() -> None:
    import crosswatch
    from api import appAuthAPI as auth

    full = {"permissions": {"dashboard": True, "watchlist": True, "playback": True, "write": True}}
    routes = [
        ("/api/config/meta", "GET"),
        ("/api/provider-instances", "GET"),
        ("/api/provider-instances/PLEX", "GET"),
        ("/api/user-profiles", "GET"),
        ("/api/version", "GET"),
        ("/api/status", "GET"),
        ("/api/insights", "GET"),
        ("/api/watch/currently_watching", "GET"),
        ("/api/watchlist", "GET"),
        ("/api/pairs", "GET"),
        ("/api/run", "POST"),
        ("/api/metadata/bulk", "POST"),
    ]

    for path, method in routes:
        assert auth.non_admin_api_allowed(path, method) is True, path
        assert crosswatch._non_admin_permission_allowed(full, path, method) is True, path


def test_disabling_managed_user_clears_sessions_and_invalidates_token(monkeypatch) -> None:
    from api import appAuthAPI as auth

    user_id = "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    profile_id = "11111111111141118111111111111111"
    cfg = _auth_cfg()
    cfg["user_profiles"] = {profile_id: {"label": "Pascal", "instances": {"PLEX": ["PLEX-P01"]}}}
    cfg["app_auth"]["users"] = {
        user_id: {
            "username": "pascal",
            "enabled": True,
            "role": "user",
            "profile_id": profile_id,
            "permissions": {"dashboard": True, "watchlist": True},
            "password": auth._password_hash("secrett2"),
        }
    }
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)

    user = auth._public_user(user_id, cfg["app_auth"]["users"][user_id])
    token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"), user)
    current = auth.current_user(cfg, token)
    assert current is not None
    assert current["id"] == user_id

    admin_token, _admin_exp = auth._issue_session(cfg, _request("/api/app-auth/login"), auth._admin_identity(cfg["app_auth"]))
    req = _request(f"/api/app-auth/users/{user_id}", method="PUT", headers={"cookie": f"{auth.COOKIE_NAME}={admin_token}"})
    resp = auth.api_users_update(req, user_id, {"enabled": False})

    assert resp.status_code == 200
    assert cfg["app_auth"]["sessions"] == [s for s in cfg["app_auth"]["sessions"] if s.get("user_id") != user_id]
    assert auth.current_user(cfg, token) is None


def test_deleting_managed_user_clears_sessions_and_invalidates_token(monkeypatch) -> None:
    from api import appAuthAPI as auth

    user_id = "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    profile_id = "11111111111141118111111111111111"
    cfg = _auth_cfg()
    cfg["user_profiles"] = {profile_id: {"label": "Pascal", "instances": {"PLEX": ["PLEX-P01"]}}}
    cfg["app_auth"]["users"] = {
        user_id: {
            "username": "pascal",
            "enabled": True,
            "role": "user",
            "profile_id": profile_id,
            "permissions": {"dashboard": True, "watchlist": True},
            "password": auth._password_hash("secrett2"),
        }
    }
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)

    user = auth._public_user(user_id, cfg["app_auth"]["users"][user_id])
    token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"), user)
    current = auth.current_user(cfg, token)
    assert current is not None
    assert current["id"] == user_id

    admin_token, _admin_exp = auth._issue_session(cfg, _request("/api/app-auth/login"), auth._admin_identity(cfg["app_auth"]))
    req = _request(f"/api/app-auth/users/{user_id}", method="DELETE", headers={"cookie": f"{auth.COOKIE_NAME}={admin_token}"})
    resp = auth.api_users_delete(req, user_id)

    assert resp.status_code == 200
    assert _json_body(resp)["deleted"] is True
    assert user_id not in cfg["app_auth"]["users"]
    assert cfg["app_auth"]["sessions"] == [s for s in cfg["app_auth"]["sessions"] if s.get("user_id") != user_id]
    assert auth.current_user(cfg, token) is None


def test_effective_user_profile_uses_managed_user_profile(monkeypatch) -> None:
    from api import appAuthAPI as auth

    profile_id = "11111111111141118111111111111111"
    other_profile_id = "22222222222242228222222222222222"
    cfg = _auth_cfg()
    cfg["app_auth"]["users"] = {
        "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa": {
            "username": "pascal",
            "enabled": True,
            "role": "user",
            "profile_id": profile_id,
            "permissions": {"dashboard": True},
            "password": auth._password_hash("secrett2"),
        }
    }
    user = auth._public_user("aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa", cfg["app_auth"]["users"]["aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"])
    token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"), user)

    assert auth.effective_user_profile_id(cfg, token, other_profile_id) == profile_id
    assert auth.effective_user_profile_id(cfg, token, "") == profile_id
    assert auth.effective_user_profile_id(cfg, "", other_profile_id) == other_profile_id


def test_managed_dashboard_user_can_read_scoped_insights_but_not_updates() -> None:
    from api import appAuthAPI as auth

    assert auth.non_admin_api_allowed("/api/insights", "GET") is True
    assert auth.non_admin_api_allowed("/api/update", "GET") is False


def test_login_audit_records_success_and_failure_without_secrets(monkeypatch) -> None:
    from api import appAuthAPI as auth

    cfg = _auth_cfg()
    calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(auth, "record_audit", lambda action, **kwargs: calls.append((action, kwargs)) or 1)
    auth._LOGIN_FAILS.clear()

    req = _request("/api/app-auth/login")
    auth.api_login(req, {"username": "admin", "password": "wrong"})
    auth.api_login(req, {"username": "admin", "password": "secrett1", "remember_me": True})

    assert [call[0] for call in calls] == ["login_failed", "login"]
    failed = calls[0][1]
    assert failed["fields"]["username"] == "admin"
    assert "password" not in failed["fields"]
    success = calls[1][1]
    assert success["actor"]["username"] == "admin"
    assert success["fields"] == {"remember_me": True}


def test_audit_recorder_sanitizes_sensitive_fields(monkeypatch) -> None:
    from cw_platform.event_archive import audit

    rows: list[dict] = []
    monkeypatch.setattr(audit, "record_events", lambda values: rows.extend(values) or len(values))

    audit.record_audit("login_failed", actor={"id": "u1", "username": "pascal", "role": "user"}, fields={"password": "secret", "username": "pascal"})

    assert rows[0]["domain"] == "audit"
    detail = json.loads(rows[0]["detail"])
    assert detail["actor"]["username"] == "pascal"
    assert detail["fields"]["password"] == "[redacted]"
    assert detail["fields"]["username"] == "pascal"
