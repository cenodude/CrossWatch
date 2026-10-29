# tests/test_api_tokens.py
# CrossWatch - API token tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from pathlib import Path

import pytest
from starlette.requests import Request


def _request(path: str, *, method: str = "GET", headers: dict[str, str] | None = None) -> Request:
    raw_headers = [(b"host", b"testserver")]
    for key, value in (headers or {}).items():
        raw_headers.append((str(key).lower().encode("latin-1"), str(value).encode("latin-1")))
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
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }
    return Request(scope)


@pytest.fixture()
def tokens_env(config_base: Path, monkeypatch: pytest.MonkeyPatch):
    import importlib

    from cw_platform import config_base as cfg_base

    importlib.reload(cfg_base)
    from api import apiTokensAPI

    importlib.reload(apiTokensAPI)

    from api import appAuthAPI as auth

    salt = b"0123456789abcdef"
    cfg = {
        "app_auth": {
            "enabled": True,
            "username": "admin",
            "password": {
                "scheme": "pbkdf2_sha256",
                "iterations": 260_000,
                "salt": auth._b64e(salt),
                "hash": auth._b64e(auth._pbkdf2_hash("secrett1", salt, iterations=260_000)),
            },
            "sessions": [],
            "api_tokens": [],
        }
    }
    (config_base / "config.json").write_text(json.dumps(cfg), encoding="utf-8")
    apiTokensAPI._TOUCH_CACHE.clear()
    return apiTokensAPI


def test_issue_and_resolve_round_trip(tokens_env) -> None:
    mod = tokens_env
    from cw_platform.config_base import config_path, load_config

    raw, entry = mod.issue_api_token(load_config(), name="cli", expires_days=0)

    assert raw.startswith(mod.TOKEN_PREFIX)
    assert "." in raw
    assert entry["name"] == "cli"
    assert entry["version"] == 2
    assert entry["expires_at"] == 0
    assert raw not in json.dumps(load_config())
    stored = load_config()["app_auth"]["api_tokens"][0]
    assert stored["version"] == 2
    assert isinstance(stored["secret"], str)
    assert stored["secret"] in raw
    assert stored["secret"] not in config_path().read_text(encoding="utf-8")
    assert "token_hash" not in stored
    assert "token_digest" not in stored

    identity = mod.resolve_api_token(load_config(), raw)
    assert identity is not None
    assert identity["is_admin"] is True
    assert identity["auth_kind"] == "api_token"
    assert identity["api_token_id"] == entry["id"]


def test_legacy_v1_tokens_still_resolve(tokens_env) -> None:
    mod = tokens_env
    from api import appAuthAPI as auth
    from cw_platform.config_base import load_config, update_config

    raw = mod.TOKEN_PREFIX + "legacysecret"

    def _add_legacy(cfg: dict) -> None:
        cfg["app_auth"]["api_tokens"].append(
            {
                "id": "legacy1",
                "name": "old cli",
                "token_hash": auth._password_hash(raw),
                "prefix": raw[: len(mod.TOKEN_PREFIX) + 6],
                "user_id": auth.ADMIN_USER_ID,
                "username": "admin",
                "created_at": auth._now(),
                "expires_at": 0,
                "last_used_at": 0,
            }
        )

    update_config(_add_legacy)

    identity = mod.resolve_api_token(load_config(), raw)
    assert identity is not None
    assert identity["is_admin"] is True
    assert identity["api_token_id"] == "legacy1"


def test_unknown_and_malformed_tokens_are_rejected(tokens_env) -> None:
    mod = tokens_env
    from cw_platform.config_base import load_config

    mod.issue_api_token(load_config(), name="cli")
    cfg = load_config()

    assert mod.resolve_api_token(cfg, "") is None
    assert mod.resolve_api_token(cfg, "not-a-token") is None
    assert mod.resolve_api_token(cfg, mod.TOKEN_PREFIX + "wrongwrongwrong") is None


def test_malformed_v2_tokens_do_not_trigger_legacy_hash_scan(tokens_env, monkeypatch: pytest.MonkeyPatch) -> None:
    mod = tokens_env
    from api import appAuthAPI as auth
    from cw_platform.config_base import load_config, update_config

    def _add_legacy(cfg: dict) -> None:
        cfg["app_auth"]["api_tokens"].append(
            {
                "id": "legacy1",
                "name": "old cli",
                "token_hash": auth._password_hash(mod.TOKEN_PREFIX + "legacysecret"),
                "prefix": mod.TOKEN_PREFIX + "legacy",
                "user_id": auth.ADMIN_USER_ID,
                "username": "admin",
                "created_at": auth._now(),
                "expires_at": 0,
                "last_used_at": 0,
            }
        )

    update_config(_add_legacy)

    def _boom(*_args, **_kwargs):
        raise AssertionError("legacy PBKDF2 scan should not run for malformed v2 tokens")

    monkeypatch.setattr(mod, "_password_matches", _boom)

    assert mod.resolve_api_token(load_config(), mod.TOKEN_PREFIX + "abcdefabcdefabcd.bad") is None


def test_remote_token_management_requires_configured_auth(tokens_env, monkeypatch: pytest.MonkeyPatch) -> None:
    mod = tokens_env

    cfg = {
        "app_auth": {
            "enabled": False,
            "username": "",
            "password": {"scheme": "pbkdf2_sha256", "iterations": 260_000, "salt": "", "hash": ""},
            "sessions": [],
            "api_tokens": [],
        }
    }
    monkeypatch.setattr(mod, "load_config", lambda: cfg)

    resp = mod.api_tokens_create(_request("/api/app-auth/tokens", method="POST"), {"name": "remote"})
    assert resp.status_code == 403
    assert json.loads(resp.body.decode("utf-8"))["error"] == "Authentication is not configured"


def test_expired_token_is_rejected(tokens_env) -> None:
    mod = tokens_env
    from cw_platform.config_base import load_config, update_config

    raw, entry = mod.issue_api_token(load_config(), name="cli", expires_days=1)
    assert mod.resolve_api_token(load_config(), raw) is not None

    def _expire(cfg: dict) -> None:
        for item in cfg["app_auth"]["api_tokens"]:
            if item["id"] == entry["id"]:
                item["expires_at"] = 1

    update_config(_expire)

    assert mod.resolve_api_token(load_config(), raw) is None
    assert mod.list_api_tokens(load_config()) == []


def test_plain_config_save_cannot_clobber_tokens(tokens_env) -> None:
    mod = tokens_env
    from cw_platform.config_base import load_config, save_config

    raw, _entry = mod.issue_api_token(load_config(), name="cli")

    cfg = load_config()
    cfg["app_auth"]["api_tokens"] = []
    cfg["scheduling"] = {"enabled": True}
    save_config(cfg)

    assert mod.resolve_api_token(load_config(), raw) is not None


def test_revoke_removes_the_token(tokens_env) -> None:
    mod = tokens_env
    from cw_platform.config_base import load_config

    raw, entry = mod.issue_api_token(load_config(), name="cli")
    assert mod.revoke_api_token(entry["id"]) is True
    assert mod.resolve_api_token(load_config(), raw) is None
    assert mod.revoke_api_token(entry["id"]) is False


def test_listing_never_exposes_the_secret(tokens_env) -> None:
    mod = tokens_env
    from cw_platform.config_base import load_config

    raw, _entry = mod.issue_api_token(load_config(), name="cli")
    listed = mod.list_api_tokens(load_config())

    assert len(listed) == 1
    blob = json.dumps(listed)
    assert raw not in blob
    assert "token_hash" not in blob
    assert "token_digest" not in blob
    assert "secret" not in blob


def test_extract_reads_header_and_bearer() -> None:
    from api import apiTokensAPI as mod

    assert mod.extract_api_token(_request("/api/status", headers={"X-CW-Token": "cwt_abc"})) == "cwt_abc"
    assert mod.extract_api_token(_request("/api/status", headers={"Authorization": "Bearer cwt_xyz"})) == "cwt_xyz"
    assert mod.extract_api_token(_request("/api/status")) == ""


def test_redaction_masks_token_secrets(tokens_env) -> None:
    mod = tokens_env
    from cw_platform.config_base import load_config, redact_config

    mod.issue_api_token(load_config(), name="cli")
    redacted = redact_config(load_config())

    for entry in redacted["app_auth"]["api_tokens"]:
        assert entry["secret"] == "•" * 8


def test_admin_token_passes_provider_instance_admin_check(tokens_env) -> None:
    mod = tokens_env
    from api import providerInstancesAPI as provider_api
    from cw_platform.config_base import load_config

    raw, _entry = mod.issue_api_token(load_config(), name="cli")
    cfg = load_config()
    path = "/api/provider-instances/CROSSWATCH/next"

    assert provider_api._admin_required_response(_request(path, method="POST", headers={mod.TOKEN_HEADER: raw}), cfg) is None
    assert provider_api._admin_required_response(_request(path, method="POST", headers={"Authorization": f"Bearer {raw}"}), cfg) is None

    for headers in ({}, {mod.TOKEN_HEADER: mod.TOKEN_PREFIX + "abcdefabcdefabcd.bad"}):
        blocked = provider_api._admin_required_response(_request(path, method="POST", headers=headers), cfg)
        assert blocked is not None
        assert blocked.status_code == 401


def test_managed_user_token_fails_provider_instance_admin_check(tokens_env) -> None:
    mod = tokens_env
    from api import providerInstancesAPI as provider_api
    from api import appAuthAPI as auth
    from cw_platform.config_base import load_config, update_config

    def _add_user(cfg: dict) -> None:
        cfg["app_auth"]["users"] = {
            "b" * 32: {
                "username": "bob",
                "enabled": True,
                "role": "user",
                "profile_id": "a" * 32,
                "permissions": {"write": True},
                "password": auth._password_hash("secrett2"),
            }
        }

    update_config(_add_user)
    raw, _entry = mod.issue_api_token(load_config(), name="bob", user_id="b" * 32)
    cfg = load_config()

    assert mod.resolve_api_token(cfg, raw)["is_admin"] is False
    blocked = provider_api._admin_required_response(_request("/api/user-profiles", method="POST", headers={mod.TOKEN_HEADER: raw}), cfg)
    assert blocked is not None
    assert blocked.status_code == 401


def test_profile_reads_resolve_api_token_identity(tokens_env) -> None:
    mod = tokens_env
    from api import appAuthAPI as auth
    from api import providerInstancesAPI as provider_api
    from cw_platform.config_base import load_config, update_config

    alice = "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    bob = "bbbbbbbbbbbb4bbb8bbbbbbbbbbbbbbb"

    def _seed(cfg: dict) -> None:
        cfg["user_profiles"] = {alice: {"label": "Alice", "instances": {}}, bob: {"label": "Bob", "instances": {}}}
        cfg["app_auth"]["users"] = {
            "b" * 32: {
                "username": "bob",
                "enabled": True,
                "role": "user",
                "profile_id": bob,
                "permissions": {"dashboard": True},
                "password": auth._password_hash("secrett2"),
            }
        }

    update_config(_seed)
    admin_raw, _a = mod.issue_api_token(load_config(), name="cli")
    bob_raw, _b = mod.issue_api_token(load_config(), name="bob", user_id="b" * 32)

    def _get(path: str, raw: str):
        return _request(path, headers={mod.TOKEN_HEADER: raw})

    admin_list = json.loads(provider_api.api_user_profiles_all(_get("/api/user-profiles", admin_raw)).body)
    bob_list = json.loads(provider_api.api_user_profiles_all(_get("/api/user-profiles", bob_raw)).body)
    assert sorted(row["id"] for row in admin_list["items"]) == [alice, bob]
    assert "resources" in admin_list
    assert [row["id"] for row in bob_list["items"]] == [bob]
    assert "resources" not in bob_list

    admin_detail = provider_api.api_user_profiles_get(alice, _get(f"/api/user-profiles/{alice}", admin_raw))
    bob_detail = provider_api.api_user_profiles_get(bob, _get(f"/api/user-profiles/{bob}", bob_raw))
    assert admin_detail.status_code == 200
    assert "resources" in json.loads(admin_detail.body)
    assert bob_detail.status_code == 200
    assert json.loads(bob_detail.body)["profile"]["id"] == bob
    assert "resources" not in json.loads(bob_detail.body)
    assert provider_api.api_user_profiles_get(alice, _get(f"/api/user-profiles/{alice}", bob_raw)).status_code == 404
    assert provider_api.api_user_profiles_get(alice, _request(f"/api/user-profiles/{alice}")).status_code == 401


MONITOR_ID = "c" * 32
MONITOR_PROFILE = "cccccccccccc4ccc8ccccccccccccccc"
MONITOR_ONLY = {"dashboard": False, "watchlist": False, "playback": False, "write": False, "monitor": True}
MONITOR_CLEAN = {**MONITOR_ONLY, "pairs": False, "scrobbler": False, "schedule": False, "connections": False}


def _seed_monitor_user(permissions: dict[str, bool]) -> None:
    from api import appAuthAPI as auth
    from cw_platform.config_base import update_config

    def _seed(cfg: dict) -> None:
        cfg["user_profiles"] = {MONITOR_PROFILE: {"label": "Ops", "instances": {}}}
        cfg["app_auth"]["users"] = {
            MONITOR_ID: {
                "username": "Monitor Bot",
                "enabled": True,
                "role": "user",
                "profile_id": MONITOR_PROFILE,
                "permissions": permissions,
                "password": auth._password_hash("secrett3"),
            }
        }

    update_config(_seed)


def test_monitor_permission_survives_config_round_trip(tokens_env) -> None:
    from cw_platform.config_base import load_config

    _seed_monitor_user(dict(MONITOR_ONLY))

    assert load_config()["app_auth"]["users"][MONITOR_ID]["permissions"] == MONITOR_ONLY


def test_token_owner_resolves_by_username_and_rejects_unknown_users(tokens_env) -> None:
    mod = tokens_env
    from cw_platform.config_base import load_config

    _seed_monitor_user(dict(MONITOR_ONLY))

    _raw, by_name = mod.issue_api_token(load_config(), name="mon", user_id="monitor bot")
    _raw, by_id = mod.issue_api_token(load_config(), name="mon", user_id=MONITOR_ID)
    _raw, admin = mod.issue_api_token(load_config(), name="cli", user_id="admin")

    assert by_name["user_id"] == MONITOR_ID
    assert by_id["user_id"] == MONITOR_ID
    assert admin["user_id"] == mod.ADMIN_USER_ID
    for unknown in ("nobody", "No Such User", "d" * 32):
        with pytest.raises(KeyError):
            mod.issue_api_token(load_config(), name="x", user_id=unknown)


def test_monitor_allowlist_is_get_only_and_explicit() -> None:
    from cw_platform.access_policy import monitor_api_allowed

    monitor = {"is_admin": False, "permissions": {"monitor": True}}
    plain = {"is_admin": False, "permissions": {"dashboard": True, "write": True}}

    for path in (
        "/api/status",
        "/api/watch/status",
        "/api/scheduling/status",
        "/api/run/summary",
        "/api/run/unresolved",
        "/api/pairs",
        "/api/snapshots/list",
        "/api/events/recent",
        "/api/events/groups/12/events",
    ):
        assert monitor_api_allowed(monitor, path, "GET") is True, path
        assert monitor_api_allowed(monitor, path, "POST") is False, path
        assert monitor_api_allowed(plain, path, "GET") is False, path
    for path in (
        "/api/config",
        "/api/scheduling",
        "/api/pairs/abc",
        "/api/snapshots/read",
        "/api/events/clear",
        "/api/logs/stream",
        "/api/app-auth/users",
        "/api/watch/start",
    ):
        assert monitor_api_allowed(monitor, path, "GET") is False, path
    assert monitor_api_allowed({"is_admin": True, "permissions": {"monitor": True}}, "/api/pairs", "GET") is False


def test_monitor_token_gets_operator_view_without_writing(tokens_env, monkeypatch: pytest.MonkeyPatch) -> None:
    mod = tokens_env
    import crosswatch
    from api import syncAPI
    from cw_platform.config_base import load_config

    _seed_monitor_user(dict(MONITOR_ONLY))
    raw, _entry = mod.issue_api_token(load_config(), name="mon", user_id=MONITOR_ID)
    user = mod.resolve_api_token(load_config(), raw)

    request = _request("/api/pairs", headers={mod.TOKEN_HEADER: raw})
    assert crosswatch._apply_monitor_view(request, user, "/api/pairs") is True
    seen = request.state.cw_user
    assert seen["is_admin"] is True and seen["monitor_view"] is True
    assert user["is_admin"] is False

    saved: list[dict] = []
    cfg = {"pairs": [{"id": "p1", "source": "PLEX", "target": "TRAKT", "source_instance": "Default", "target_instance": "default", "profile_id": "zzz"}]}
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: cfg, saved.append))
    response = syncAPI.api_pairs_list(request)
    assert [row["id"] for row in json.loads(response.body)] == ["p1"]
    assert saved == []

    for method, path in (("POST", "/api/pairs"), ("GET", "/api/config"), ("POST", "/api/run")):
        blocked = _request(path, method=method, headers={mod.TOKEN_HEADER: raw})
        assert crosswatch._apply_monitor_view(blocked, user, path) is False
        assert not (crosswatch.app_non_admin_api_allowed(path, method) and crosswatch._non_admin_permission_allowed(user, path, method))


def test_monitor_token_through_the_auth_gate(tokens_env, monkeypatch: pytest.MonkeyPatch) -> None:
    mod = tokens_env
    from fastapi.testclient import TestClient

    import crosswatch
    from api import appAuthAPI as auth
    from cw_platform.config_base import load_config, update_config

    _seed_monitor_user(dict(MONITOR_ONLY))

    def _add_plain_user(cfg: dict) -> None:
        cfg["app_auth"]["users"]["d" * 32] = {
            "username": "plain",
            "enabled": True,
            "role": "user",
            "profile_id": MONITOR_PROFILE,
            "permissions": {"dashboard": True, "watchlist": True, "playback": True, "write": True},
            "password": auth._password_hash("secrett4"),
        }

    update_config(_add_plain_user)
    monitor_raw, _m = mod.issue_api_token(load_config(), name="mon", user_id="Monitor Bot")
    plain_raw, _p = mod.issue_api_token(load_config(), name="plain", user_id="plain")
    monkeypatch.setattr(crosswatch, "load_config", load_config)
    monkeypatch.setattr(crosswatch, "app_resolve_api_token", mod.resolve_api_token)
    monkeypatch.setattr(crosswatch, "app_touch_api_token", lambda _tid: None)

    client = TestClient(crosswatch.app)
    monitor = {mod.TOKEN_HEADER: monitor_raw}

    assert client.get("/api/watch/status", headers=monitor).status_code == 200
    for method, path in (
        ("GET", "/api/config"),
        ("GET", "/api/scheduling"),
        ("GET", "/api/watchlist"),
        ("POST", "/api/watch/start"),
        ("POST", "/api/run"),
        ("POST", "/api/pairs"),
        ("POST", "/api/snapshots/create"),
        ("GET", "/api/profile"),
        ("PUT", "/api/profile/preferences"),
        ("GET", "/api/provider-instances"),
        ("GET", "/"),
        ("POST", "/api/app-auth/tokens"),
        ("DELETE", "/api/app-auth/tokens/abcdefabcdefabcd"),
    ):
        res = client.request(method, path, headers=monitor)
        assert res.status_code == 403, (method, path, res.status_code)
    assert client.get("/api/watch/status", headers={mod.TOKEN_HEADER: plain_raw}).status_code == 403
    assert len(mod.list_api_tokens(load_config(), user_id=MONITOR_ID)) == 1


def test_monitor_excludes_every_other_permission(tokens_env) -> None:
    mod = tokens_env
    from api import appAuthAPI as auth
    from cw_platform.access_policy import clean_managed_permissions
    from cw_platform.config_base import load_config

    _seed_monitor_user({"dashboard": True, "watchlist": True, "playback": True, "write": True, "monitor": True})
    assert load_config()["app_auth"]["users"][MONITOR_ID]["permissions"] == MONITOR_ONLY
    assert clean_managed_permissions({"monitor": True, "write": True}) == MONITOR_CLEAN

    admin_raw, _entry = mod.issue_api_token(load_config(), name="cli")
    request = _request("/api/app-auth/users", method="POST", headers={mod.TOKEN_HEADER: admin_raw})
    created = auth.api_users_create(
        request,
        {"username": "mon2", "password": "secrett9", "profile_id": MONITOR_PROFILE, "permissions": {"monitor": True}},
    )
    assert created.status_code == 200
    assert json.loads(created.body)["user"]["permissions"] == MONITOR_CLEAN


def test_monitor_sessions_and_sso_logins_do_not_authenticate(tokens_env, monkeypatch: pytest.MonkeyPatch) -> None:
    from api import appAuthAPI as auth
    from api import authOidcAPI, authPlexAPI
    from cw_platform.config_base import load_config, update_config

    _seed_monitor_user(dict(MONITOR_ONLY))
    monitor_cookie = "monitor-session-token"
    admin_cookie = "admin-session-token"

    def _seed(cfg: dict) -> None:
        cfg["app_auth"]["users"]["d" * 32] = {
            "username": "reader",
            "enabled": True,
            "role": "user",
            "profile_id": MONITOR_PROFILE,
            "permissions": {"write": False},
            "password": auth._password_hash("secrett4"),
        }
        cfg["app_auth"]["sessions"] = [
            {"id": "m1", "user_id": MONITOR_ID, "token_hash": auth._sha256_hex(monitor_cookie), "expires_at": 4102444800},
            {"id": "a1", "token_hash": auth._sha256_hex(admin_cookie), "expires_at": 4102444800},
        ]

    update_config(_seed)
    cfg = load_config()
    a = cfg["app_auth"]

    assert auth._find_session(a, monitor_cookie) is None
    assert auth.is_authenticated(cfg, monitor_cookie) is False
    assert auth.current_user(cfg, monitor_cookie) is None
    assert auth.is_authenticated(cfg, admin_cookie) is True
    assert auth.current_user(cfg, admin_cookie)["is_admin"] is True

    def _matches(link: dict, identity: dict) -> bool:
        return bool(link) and link.get("marker") == identity.get("marker")

    monkeypatch.setattr(authPlexAPI.authPlex, "identity_matches_link", _matches)
    monkeypatch.setattr(authOidcAPI.authOidc, "identity_matches_link", _matches)
    for user_id, marker in ((MONITOR_ID, "mon"), ("d" * 32, "reader")):
        a["users"][user_id]["plex_sso"] = {"marker": marker}
        a["users"][user_id]["oidc"] = {"marker": marker}
    assert authPlexAPI._resolve_plex_user(cfg, {"marker": "mon"}) is None
    assert authOidcAPI._resolve_oidc_user(cfg, {"marker": "mon"}) is None
    assert authPlexAPI._resolve_plex_user(cfg, {"marker": "reader"})["id"] == "d" * 32
    assert authOidcAPI._resolve_oidc_user(cfg, {"marker": "reader"})["id"] == "d" * 32
    admin = auth._admin_identity(a)
    assert authPlexAPI._target_user_for_link(a, admin, MONITOR_ID)[0] == ""
    assert authOidcAPI._target_user_for_link(a, admin, MONITOR_ID)[0] == ""


def test_monitor_view_hides_webhook_secrets_and_audit_events(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import eventsAPI, schedulingAPI
    from cw_platform.access_policy import monitor_identity

    class _Scheduler:
        def status(self) -> dict:
            return {"running": True}

    cfg = {"scheduling": {"enabled": True, "webhooks": {"url": "https://hc.example/secret", "pushover_token": "tok"}}}
    monkeypatch.setattr(schedulingAPI, "_env", lambda: (lambda: cfg, None, _Scheduler(), {}, None, None))

    monitor_request = _request("/api/scheduling/status")
    monitor_request.state.cw_user = monitor_identity({"id": MONITOR_ID, "is_admin": False, "permissions": {"monitor": True}})
    admin_request = _request("/api/scheduling/status")
    admin_request.state.cw_user = {"id": "administrator", "is_admin": True}

    monitor_config = schedulingAPI.sched_status(monitor_request)["config"]
    assert "webhooks" not in monitor_config
    assert monitor_config["enabled"] is True
    assert "webhooks" in schedulingAPI.sched_status(admin_request)["config"]
    assert "webhooks" in cfg["scheduling"]

    audit_row = {"id": 1, "domain": "audit"}
    sync_row = {"id": 2, "domain": "sync"}
    assert eventsAPI._audit_hidden(monitor_request) is True
    assert eventsAPI._audit_hidden(admin_request) is False
    assert eventsAPI._event_row_allowed({}, monitor_request, audit_row) is False
    assert eventsAPI._event_row_allowed({}, monitor_request, sync_row) is True
    assert eventsAPI._event_row_allowed({}, admin_request, audit_row) is True
    filtered = eventsAPI._filter_event_payload({}, monitor_request, {"items": [audit_row, sync_row], "total": 2})
    assert filtered["items"] == [sync_row]
    assert filtered["total"] == 1
    paged = eventsAPI._filter_event_payload({}, monitor_request, {"items": [sync_row], "total": 100})
    assert paged["total"] == 100
    unfiltered = eventsAPI._filter_event_payload({}, admin_request, {"items": [audit_row, sync_row], "total": 2})
    assert unfiltered["items"] == [audit_row, sync_row]

    monkeypatch.setattr(eventsAPI, "_status", lambda: {"ok": True, "events": 3, "by_type": {"audit_login_failed": 2, "run_completed": 1}})
    assert json.loads(eventsAPI.events_status(monitor_request).body)["by_type"] == {"run_completed": 1}
    assert "audit_login_failed" in json.loads(eventsAPI.events_status(admin_request).body)["by_type"]

    assert eventsAPI._effective_profile_from_request(monitor_request, "") == ""


def test_monitor_view_ignores_view_as_and_managed_users_never_see_audit_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import eventsAPI
    from cw_platform.access_policy import monitor_identity, request_user

    identity = monitor_identity({"id": MONITOR_ID, "is_admin": False, "permissions": {"monitor": True}})
    request = _request("/api/events/context", headers={"x-cw-view-as": MONITOR_PROFILE})
    request.state.cw_user = identity

    assert request_user(request) is identity

    managed = _request("/api/events/context")
    managed.state.cw_user = {"id": "d" * 32, "is_admin": False, "profile_id": MONITOR_PROFILE, "permissions": {"write": True}}
    monkeypatch.setattr(eventsAPI, "_group_has_allowed_event", lambda *_a, **_k: True)

    assert eventsAPI._event_row_allowed({}, managed, {"id": 7, "group_hash": "g", "domain": "audit"}) is False
    assert eventsAPI._event_row_allowed({}, managed, {"id": 7, "group_hash": "g", "domain": "sync"}) is True
    assert eventsAPI._event_row_allowed({}, managed, {"id": 7, "event_hash": "e", "domain": "sync"}) is False


def test_token_requests_cannot_pick_another_profile(tokens_env, monkeypatch: pytest.MonkeyPatch) -> None:
    mod = tokens_env
    from fastapi.testclient import TestClient

    import crosswatch
    from api import appAuthAPI as auth
    from cw_platform.access_policy import monitor_identity
    from cw_platform.config_base import load_config, update_config

    other_profile = "dddddddddddd4ddd8ddddddddddddddd"
    _seed_monitor_user({"dashboard": True, "watchlist": True, "playback": True, "write": False})

    def _add_profile(cfg: dict) -> None:
        cfg["user_profiles"][other_profile] = {"label": "Other", "instances": {}}

    update_config(_add_profile)
    cfg = load_config()
    managed = {"id": MONITOR_ID, "is_admin": False, "profile_id": MONITOR_PROFILE, "permissions": {"dashboard": True}}

    def _req(user: dict | None) -> Request:
        request = _request("/api/activity/recent")
        if user is not None:
            request.state.cw_user = user
        return request

    assert auth.effective_request_profile_id(cfg, _req(managed), other_profile) == MONITOR_PROFILE
    assert auth.effective_request_profile_id(cfg, _req(managed), "") == MONITOR_PROFILE
    assert auth.effective_request_profile_id(cfg, _req({**managed, "profile_id": ""}), other_profile) == "__none__"
    assert auth.effective_request_profile_id(cfg, _req({"id": "administrator", "is_admin": True}), other_profile) == other_profile
    assert auth.effective_request_profile_id(cfg, _req(monitor_identity(managed)), other_profile) == other_profile
    assert auth.effective_request_profile_id(cfg, _req(None), other_profile) == other_profile

    raw, _entry = mod.issue_api_token(load_config(), name="managed", user_id=MONITOR_ID)
    monkeypatch.setattr(crosswatch, "load_config", load_config)
    monkeypatch.setattr(crosswatch, "app_resolve_api_token", mod.resolve_api_token)
    monkeypatch.setattr(crosswatch, "app_touch_api_token", lambda _tid: None)

    res = TestClient(crosswatch.app).get(f"/api/activity/recent?user_profile={other_profile}", headers={mod.TOKEN_HEADER: raw})
    assert res.status_code == 200
    assert res.json().get("user_profile") == MONITOR_PROFILE


def test_monitor_accounts_are_token_only(tokens_env) -> None:
    mod = tokens_env
    from api import appAuthAPI as auth
    from cw_platform.config_base import load_config, update_config

    _seed_monitor_user(dict(MONITOR_ONLY))

    def _add_reader(cfg: dict) -> None:
        cfg["app_auth"]["users"]["d" * 32] = {
            "username": "reader",
            "enabled": True,
            "role": "user",
            "profile_id": MONITOR_PROFILE,
            "permissions": {"dashboard": True, "watchlist": True, "playback": True, "write": False},
            "password": auth._password_hash("secrett4"),
        }
        cfg["app_auth"]["sessions"] = [{"id": "s1", "user_id": "d" * 32, "token_hash": "0" * 64, "expires_at": 4102444800}]

    update_config(_add_reader)
    a = load_config()["app_auth"]

    assert auth._find_user_for_login(a, "Monitor Bot", "secrett3") is None
    assert auth._find_user_for_login(a, "reader", "secrett4")["id"] == "d" * 32
    assert auth._session_identity(a, {"user_id": MONITOR_ID}) is None
    assert auth._session_identity(a, {"user_id": "d" * 32})["id"] == "d" * 32

    raw, _entry = mod.issue_api_token(load_config(), name="mon", user_id=MONITOR_ID)
    assert mod.resolve_api_token(load_config(), raw)["permissions"]["monitor"] is True

    admin_raw, _admin = mod.issue_api_token(load_config(), name="cli")
    request = _request(f"/api/app-auth/users/{'d' * 32}", method="PUT", headers={mod.TOKEN_HEADER: admin_raw})
    assert auth.api_users_update(request, "d" * 32, {"permissions": dict(MONITOR_ONLY)}).status_code == 200
    after = load_config()["app_auth"]
    assert [s for s in after.get("sessions") or [] if s.get("user_id") == "d" * 32] == []
    assert auth._find_user_for_login(after, "reader", "secrett4") is None


def test_token_owner_named_like_the_admin_id_is_refused(tokens_env) -> None:
    mod = tokens_env
    from api import appAuthAPI as auth
    from cw_platform.config_base import load_config, update_config

    def _seed(cfg: dict) -> None:
        cfg["app_auth"]["users"] = {
            "e" * 32: {
                "username": "Administrator",
                "enabled": True,
                "role": "user",
                "profile_id": MONITOR_PROFILE,
                "permissions": {"write": False},
                "password": auth._password_hash("secrett5"),
            }
        }

    update_config(_seed)

    with pytest.raises(KeyError):
        mod.issue_api_token(load_config(), name="x", user_id="administrator")
    _raw, by_id = mod.issue_api_token(load_config(), name="x", user_id="e" * 32)
    _raw, default = mod.issue_api_token(load_config(), name="x")
    assert by_id["user_id"] == "e" * 32
    assert default["user_id"] == mod.ADMIN_USER_ID


def test_audit_actor_carries_the_api_token(tokens_env, monkeypatch: pytest.MonkeyPatch) -> None:
    mod = tokens_env
    from cw_platform.config_base import load_config
    from cw_platform.event_archive import audit

    raw, entry = mod.issue_api_token(load_config(), name="backup job")
    user = mod.resolve_api_token(load_config(), raw)

    recorded: list[dict] = []
    monkeypatch.setattr(audit, "record_events", lambda events: recorded.extend(events) or len(events))
    audit.record_audit("api_action", actor=user, target_type="captures", target_id="/api/snapshots/create")
    audit.record_audit("login", actor={"id": "administrator", "username": "admin", "is_admin": True})

    def _actor(event: dict) -> dict:
        detail = event["detail"]
        return (json.loads(detail) if isinstance(detail, str) else detail)["actor"]

    assert _actor(recorded[0])["api_token_id"] == entry["id"]
    assert _actor(recorded[0])["api_token_name"] == "backup job"
    assert "api_token_id" not in _actor(recorded[1])


def test_admin_token_manages_users_and_other_tokens_cannot(tokens_env, monkeypatch: pytest.MonkeyPatch) -> None:
    mod = tokens_env
    from api import appAuthAPI as auth
    from cw_platform.config_base import load_config

    _seed_monitor_user(dict(MONITOR_ONLY))
    admin_raw, admin_entry = mod.issue_api_token(load_config(), name="cli")
    monitor_raw, _m = mod.issue_api_token(load_config(), name="mon", user_id=MONITOR_ID)
    audited: list[dict] = []
    monkeypatch.setattr(auth, "record_audit", lambda action, **kwargs: audited.append({"action": action, **kwargs}) or 1)

    def _req(path: str, method: str, raw: str | None) -> Request:
        return _request(path, method=method, headers={mod.TOKEN_HEADER: raw} if raw else None)

    listed = auth.api_users_all(_req("/api/app-auth/users", "GET", admin_raw))
    assert listed.status_code == 200
    assert [row["id"] for row in json.loads(listed.body)["items"]] == [mod.ADMIN_USER_ID, MONITOR_ID]

    payload = {"username": "ops", "password": "secrett9", "profile_id": MONITOR_PROFILE, "permissions": dict(MONITOR_ONLY)}
    created = auth.api_users_create(_req("/api/app-auth/users", "POST", admin_raw), dict(payload))
    assert created.status_code == 200
    new_user = json.loads(created.body)["user"]
    assert new_user["permissions"] == MONITOR_CLEAN
    assert audited[-1]["action"] == "user_created"
    assert audited[-1]["actor"]["api_token_id"] == admin_entry["id"]

    path = f"/api/app-auth/users/{new_user['id']}"
    updated = auth.api_users_update(_req(path, "PUT", admin_raw), new_user["id"], {"enabled": False})
    assert updated.status_code == 200
    assert json.loads(updated.body)["user"]["enabled"] is False
    deleted = auth.api_users_delete(_req(path, "DELETE", admin_raw), new_user["id"])
    assert json.loads(deleted.body)["deleted"] is True
    assert new_user["id"] not in load_config()["app_auth"]["users"]

    for raw in (monitor_raw, None, mod.TOKEN_PREFIX + "abcdefabcdefabcd.bad"):
        assert auth.api_users_all(_req("/api/app-auth/users", "GET", raw)).status_code == 401
        assert auth.api_users_create(_req("/api/app-auth/users", "POST", raw), dict(payload)).status_code == 401
        assert auth.api_users_update(_req(f"/api/app-auth/users/{MONITOR_ID}", "PUT", raw), MONITOR_ID, {"permissions": {"write": True}}).status_code == 401
        assert auth.api_users_delete(_req(f"/api/app-auth/users/{MONITOR_ID}", "DELETE", raw), MONITOR_ID).status_code == 401
    assert load_config()["app_auth"]["users"][MONITOR_ID]["permissions"] == MONITOR_ONLY
