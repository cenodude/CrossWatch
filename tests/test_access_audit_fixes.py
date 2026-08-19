from __future__ import annotations

import json
from typing import Any, cast

import pytest
from starlette.requests import Request

MANAGED_PROFILE = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
OWNED_UID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
FOREIGN_UID = "cccccccccccccccccccccccccccccccc"
FOREIGN_PROFILE = "dddddddddddddddddddddddddddddddd"


def _request(
    path: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
    client: tuple[str, int] = ("127.0.0.1", 12345),
    query: str = "",
) -> Request:
    raw_headers = [(b"host", b"testserver"), (b"sec-fetch-site", b"same-origin")]
    if cookies:
        jar = "; ".join(f"{k}={v}" for k, v in cookies.items())
        raw_headers.append((b"cookie", jar.encode("latin-1")))
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
            "query_string": query.encode("latin-1"),
            "headers": raw_headers,
            "client": client,
            "server": ("testserver", 80),
            "state": {},
        }
    )


def _managed_user() -> dict[str, Any]:
    return {
        "id": "11111111111111111111111111111111",
        "username": "alice",
        "is_admin": False,
        "role": "user",
        "profile_id": MANAGED_PROFILE,
        "permissions": {"dashboard": True, "watchlist": True, "playback": True, "write": True},
    }


def _pairs_cfg() -> dict[str, Any]:
    return {
        "plex": {"server_url": "http://plex:32400", "account_token": "t", "instances": {}},
        "trakt": {"client_id": "c", "client_secret": "s", "instances": {}},
        "provider_instance_ids": {
            OWNED_UID: {"provider": "PLEX", "instance": "default"},
            FOREIGN_UID: {"provider": "TRAKT", "instance": "default"},
        },
        "user_profiles": {
            MANAGED_PROFILE: {"label": "Alice", "instance_uids": [OWNED_UID]},
            FOREIGN_PROFILE: {"label": "Bob", "instance_uids": [FOREIGN_UID]},
        },
        "pairs": [
            {
                "id": "mine",
                "enabled": True,
                "source": "PLEX",
                "target": "PLEX",
                "source_instance": "default",
                "target_instance": "default",
                "profile_id": MANAGED_PROFILE,
                "features": {"watchlist": {"enable": True}},
            },
            {
                "id": "theirs",
                "enabled": True,
                "source": "TRAKT",
                "target": "TRAKT",
                "source_instance": "default",
                "target_instance": "default",
                "profile_id": FOREIGN_PROFILE,
                "features": {"watchlist": {"enable": True}},
            },
        ],
    }


# H1 - a managed user must not reach another profile's pair through any override key
@pytest.mark.parametrize("key", ["pair_id", "pairId", "pair_scope"])
def test_run_sync_rejects_foreign_pair_for_every_override_key(monkeypatch, key: str) -> None:
    from api import syncAPI as sync_api

    cfg = _pairs_cfg()
    started: list[dict[str, Any]] = []

    monkeypatch.setattr(sync_api, "_env", lambda: (lambda: cfg, lambda *_: None))
    monkeypatch.setattr(sync_api, "_is_sync_running", lambda: False)

    class _Thread:
        def __init__(self, *_a: Any, **kw: Any) -> None:
            started.append(dict(kw.get("kwargs", {}).get("overrides") or {}))

        def start(self) -> None:
            pass

    monkeypatch.setattr(sync_api.threading, "Thread", _Thread)

    req = _request("/api/run", method="POST")
    req.scope["state"]["cw_user"] = _managed_user()

    res = sync_api.api_run_sync(payload={key: "theirs"}, request=req)

    assert res.get("ok") is not True
    assert "theirs" in str(res.get("error") or "")
    assert started == []


def test_run_sync_allows_own_pair_and_pins_server_side_scope(monkeypatch) -> None:
    from api import syncAPI as sync_api

    cfg = _pairs_cfg()
    started: list[dict[str, Any]] = []

    monkeypatch.setattr(sync_api, "_env", lambda: (lambda: cfg, lambda *_: None))
    monkeypatch.setattr(sync_api, "_is_sync_running", lambda: False)

    class _Thread:
        def __init__(self, *_a: Any, **kw: Any) -> None:
            started.append(dict(kw.get("kwargs", {}).get("overrides") or {}))

        def start(self) -> None:
            pass

    monkeypatch.setattr(sync_api.threading, "Thread", _Thread)

    req = _request("/api/run", method="POST")
    req.scope["state"]["cw_user"] = _managed_user()

    res = sync_api.api_run_sync(payload={"pair_scope": "mine", "pair_scope_ids": ["theirs"]}, request=req)

    assert res.get("ok") is True
    assert len(started) == 1
    overrides = started[0]
    assert overrides["pair_id"] == "mine"
    assert overrides["pair_scope_ids"] == ["mine"]
    assert "pair_scope" not in overrides


def test_run_pairs_thread_scope_filter_precedes_single_pair_selection() -> None:
    from api import syncAPI as sync_api
    import inspect

    src = inspect.getsource(sync_api._run_pairs_thread)
    scope_at = src.find('cfg["pairs"] = [p for p in (cfg.get("pairs") or []) if str(p.get("id") or "") in req_pair_ids]')
    single_at = src.find('if req_pair_id:\n            pair = next(')
    assert scope_at > 0 and single_at > 0
    assert scope_at < single_at


# H2 - the OIDC login callback must be bound to the browser that started the flow
def test_oidc_callback_rejects_missing_flow_cookie(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api
    from services import authOidc

    monkeypatch.setattr(oidc_api, "load_config", lambda: {})
    monkeypatch.setattr(authOidc, "_PENDING_FLOWS", {"st": {"flow_nonce_hash": authOidc._sha256_hex("real-nonce"), "expires_at": 1 << 40}})

    def _boom(*_a: Any, **_kw: Any) -> dict[str, Any]:
        raise AssertionError("token exchange must not run for an unbound callback")

    monkeypatch.setattr(authOidc, "check_callback", _boom)

    res = oidc_api.api_oidc_callback(_request("/api/app-auth/oidc/callback"), state="st", code="c", error="")
    assert res.status_code == 400
    assert b"expired" in res.body


def test_oidc_callback_rejects_wrong_flow_cookie(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api
    from services import authOidc

    monkeypatch.setattr(oidc_api, "load_config", lambda: {})
    monkeypatch.setattr(authOidc, "_PENDING_FLOWS", {"st": {"flow_nonce_hash": authOidc._sha256_hex("real-nonce"), "expires_at": 1 << 40}})
    monkeypatch.setattr(authOidc, "check_callback", lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not run")))

    req = _request("/api/app-auth/oidc/callback", cookies={oidc_api.FLOW_COOKIE_NAME: "attacker-nonce"})
    res = oidc_api.api_oidc_callback(req, state="st", code="c", error="")
    assert res.status_code == 400


def test_oidc_start_sets_flow_cookie(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api
    from services import authOidc

    captured: dict[str, Any] = {}

    monkeypatch.setattr(oidc_api, "load_config", lambda: {})
    monkeypatch.setattr(oidc_api.app_auth, "auth_required", lambda _cfg: True)
    monkeypatch.setattr(authOidc, "login_available", lambda _cfg: True)

    def _start(_cfg: Any, **kw: Any) -> dict[str, Any]:
        captured.update(kw)
        return {"ok": True, "state": "st", "auth_url": "https://idp.example/auth"}

    monkeypatch.setattr(authOidc, "start_flow", _start)

    res = oidc_api.api_oidc_start(_request("/api/app-auth/oidc/start"))
    assert res.status_code == 302
    assert captured.get("flow_nonce_hash")
    assert oidc_api.FLOW_COOKIE_NAME in res.headers.get("set-cookie", "")


# L2 - OIDC login on a 2FA account gets a verification step, not a dead end
def test_oidc_login_with_totp_returns_verification_page(monkeypatch) -> None:
    from api import authOidcAPI as oidc_api

    cfg = {"app_auth": {"enabled": True, "totp": {"enabled": True, "secret": "JBSWY3DPEHPK3PXP"}}}
    user = {"id": "administrator", "username": "admin", "is_admin": True}
    res = oidc_api._totp_pending_response(
        _request("/api/app-auth/oidc/callback"),
        cast(dict, cfg),
        {"remember_me": False, "next_url": "/"},
        {"sub": "s", "iss": "https://idp.example"},
        user,
    )
    assert res is not None
    assert res.status_code == 200
    assert b"Two-factor verification" in res.body
    assert len(oidc_api._PENDING_2FA) == 1


def test_oidc_login_without_totp_has_no_pending_step() -> None:
    from api import authOidcAPI as oidc_api

    cfg = {"app_auth": {"enabled": True}}
    user = {"id": "administrator", "username": "admin", "is_admin": True}
    assert oidc_api._totp_pending_response(
        _request("/api/app-auth/oidc/callback"),
        cast(dict, cfg),
        {},
        {"sub": "s"},
        user,
    ) is None


# M2 - the client cannot mint a fresh rate-limit bucket by prepending X-Forwarded-For
def test_effective_client_ip_ignores_client_supplied_forwarded_prefix(monkeypatch) -> None:
    from api import appAuthAPI as auth

    monkeypatch.setattr(auth, "load_config", lambda: {"security": {"trusted_proxies": ["10.0.0.0/8"]}})
    auth._TRUSTED_PROXY_CACHE["at"] = 0.0

    spoofed = _request(
        "/api/app-auth/login",
        headers={"x-forwarded-for": "1.2.3.4, 203.0.113.9"},
        client=("10.0.0.5", 1234),
    )
    assert auth._effective_client_ip(spoofed) == "203.0.113.9"

    chained = _request(
        "/api/app-auth/login",
        headers={"x-forwarded-for": "9.9.9.9, 203.0.113.9, 10.0.0.7"},
        client=("10.0.0.5", 1234),
    )
    assert auth._effective_client_ip(chained) == "203.0.113.9"


def test_effective_client_ip_ignores_forwarded_from_untrusted_peer(monkeypatch) -> None:
    from api import appAuthAPI as auth

    monkeypatch.setattr(auth, "load_config", lambda: {"security": {"trusted_proxies": ["10.0.0.0/8"]}})
    auth._TRUSTED_PROXY_CACHE["at"] = 0.0

    req = _request("/api/app-auth/login", headers={"x-forwarded-for": "1.2.3.4"}, client=("198.51.100.4", 1234))
    assert auth._effective_client_ip(req) == "198.51.100.4"


def test_login_fail_table_prunes_stale_entries(monkeypatch) -> None:
    from api import appAuthAPI as auth

    monkeypatch.setattr(auth, "load_config", lambda: {"security": {}})
    auth._TRUSTED_PROXY_CACHE["at"] = 0.0
    auth._LOGIN_FAILS.clear()
    auth._LOGIN_FAILS["stale.host"] = {"n": 9, "until": 0, "at": auth._now() - auth.LOGIN_FAIL_TTL_SEC - 10}

    auth._rate_limit_fail(_request("/api/app-auth/login", client=("203.0.113.1", 1)))

    assert "stale.host" not in auth._LOGIN_FAILS
    assert auth._LOGIN_FAILS["203.0.113.1"]["at"] > 0
    auth._LOGIN_FAILS.clear()


# M3 - the watcher log is admin-only; the sync log is gated on owning the active run
def test_watcher_log_denied_to_managed_users() -> None:
    import crosswatch
    from api import appAuthAPI as auth

    user = _managed_user()
    assert auth.non_admin_api_allowed("/api/logs/watcher", "GET") is False
    assert crosswatch._non_admin_permission_allowed(user, "/api/logs/watcher", "GET") is False


def test_sync_log_reachable_for_full_access_managed_users() -> None:
    import crosswatch
    from api import appAuthAPI as auth

    assert auth.non_admin_api_allowed("/api/logs/stream", "GET") is True
    assert crosswatch._non_admin_permission_allowed(_managed_user(), "/api/logs/stream", "GET") is True

    readonly = _managed_user()
    readonly["permissions"] = {"dashboard": True, "watchlist": True, "playback": True, "write": False}
    assert crosswatch._non_admin_permission_allowed(readonly, "/api/logs/stream", "GET") is False


def test_run_log_visibility_requires_owning_the_whole_run(monkeypatch) -> None:
    from api import syncAPI as sync_api

    cfg = _pairs_cfg()
    user = _managed_user()
    admin = {"id": "administrator", "is_admin": True}

    def _scope(ids: Any) -> None:
        monkeypatch.setattr(sync_api, "_summary_snapshot", lambda: {"pair_scope_ids": ids})

    _scope(["mine"])
    assert sync_api.run_log_visible_to_user(cfg, user) is True

    _scope(["theirs"])
    assert sync_api.run_log_visible_to_user(cfg, user) is False

    _scope(["mine", "theirs"])
    assert sync_api.run_log_visible_to_user(cfg, user) is False

    _scope([])
    assert sync_api.run_log_visible_to_user(cfg, user) is False

    _scope(None)
    assert sync_api.run_log_visible_to_user(cfg, user) is False

    _scope(["theirs"])
    assert sync_api.run_log_visible_to_user(cfg, admin) is True
    assert sync_api.run_log_visible_to_user(cfg, None) is True


def test_summary_stream_no_longer_relies_on_absent_pair_attribution() -> None:
    from api import syncAPI as sync_api
    import inspect

    src = inspect.getsource(sync_api.api_run_summary_stream)
    assert 'obj.get("pair_id")' not in src
    assert "run_log_visible_to_user" in src


# M4 - next-URL guards reject backslash-smuggled absolute targets
@pytest.mark.parametrize(
    "raw",
    ["/\\evil.com", "//evil.com", "\\\\evil.com", "/\\/evil.com", "https://evil.com", "/path\\x", "/a\r\nSet-Cookie: x=1"],
)
def test_safe_next_rejects_offsite_targets(raw: str) -> None:
    from api.authOidcAPI import _safe_next

    assert _safe_next(raw) == "/"


@pytest.mark.parametrize("raw", ["/insights", "/?main=1#watchlist", "/profile?tab=security"])
def test_safe_next_keeps_same_origin_paths(raw: str) -> None:
    from api.authOidcAPI import _safe_next

    assert _safe_next(raw) == raw


def test_safe_next_rejects_login_and_logout_with_query() -> None:
    from api.authOidcAPI import _safe_next

    assert _safe_next("/login") == "/"
    assert _safe_next("/logout?x=1") == "/"


def test_login_page_next_guard_rejects_backslash() -> None:
    from api.appAuthAPI import _login_html

    html = _login_html("admin")
    assert "u.origin === location.origin" in html
    assert "!next.includes('\\\\')" in html


# L1 - a present but invalid session user id must not resolve to the administrator
def test_session_with_corrupt_user_id_is_not_admin() -> None:
    from api import appAuthAPI as auth

    a = {"username": "admin", "display_name": "Admin"}
    assert auth._session_identity(a, {"user_id": "not a valid id!"}) is None
    assert auth._session_identity(a, {"user_id": ""}) is not None
    legacy = auth._session_identity(a, {})
    assert legacy is not None and legacy["is_admin"] is True


def test_prune_sessions_drops_corrupt_user_ids() -> None:
    from api import appAuthAPI as auth

    now = auth._now()
    good = {"id": "a", "token_hash": "a" * 64, "expires_at": now + 999, "created_at": now, "user_id": "1" * 32}
    bad = {"id": "b", "token_hash": "b" * 64, "expires_at": now + 999, "created_at": now, "user_id": "!!bad!!"}
    kept = auth._prune_sessions([good, bad])
    assert [s["id"] for s in kept] == ["a"]


# L3 - a managed user's read of /api/pairs must not persist config
def test_pairs_list_does_not_save_for_managed_user(monkeypatch) -> None:
    from api import syncAPI as sync_api

    cfg = _pairs_cfg()
    cfg["pairs"][0]["profile_id"] = "NOT-A-VALID-PROFILE"
    saves: list[Any] = []

    monkeypatch.setattr(sync_api, "_env", lambda: (lambda: cfg, lambda c: saves.append(c)))

    req = _request("/api/pairs")
    req.scope["state"]["cw_user"] = _managed_user()
    sync_api.api_pairs_list(request=req)
    assert saves == []

    admin_cfg = _pairs_cfg()
    admin_cfg["pairs"][0]["profile_id"] = "NOT-A-VALID-PROFILE"
    monkeypatch.setattr(sync_api, "_env", lambda: (lambda: admin_cfg, lambda c: saves.append(c)))
    admin_req = _request("/api/pairs")
    admin_req.scope["state"]["cw_user"] = {"id": "administrator", "is_admin": True}
    sync_api.api_pairs_list(request=admin_req)
    assert len(saves) == 1


# L4 - the managed shell must never ship admin-only markup
def test_managed_shell_strips_admin_markup() -> None:
    import ui_frontend

    src = ui_frontend._get_index_html_static()
    for perms in (
        {"write": True, "dashboard": True, "watchlist": True, "playback": True},
        {"write": False, "dashboard": True, "watchlist": True, "playback": True},
        {"write": False, "dashboard": False, "watchlist": False, "playback": False},
    ):
        html = ui_frontend._managed_user_shell(src, {"permissions": perms})
        for marker, _label in ui_frontend._ADMIN_ONLY_MARKERS:
            assert marker not in html


def test_managed_shell_refuses_to_render_when_markers_drift() -> None:
    import ui_frontend

    drifted = ui_frontend._get_index_html_static().replace(
        '  <section id="page-settings" class="card hidden">',
        '  <section  id="page-settings" class="card hidden">',
        1,
    )
    with pytest.raises(RuntimeError, match="managed_user_shell_strip_failed"):
        ui_frontend._managed_user_shell(drifted, {"permissions": {"write": True, "dashboard": True}})


# L5 - config/meta withholds UI state while a credential reset is pending
def _meta(monkeypatch, tmp_path, raw: dict[str, Any], cookies: dict[str, str] | None = None) -> dict[str, Any]:
    from api import configAPI as cfg_api
    from types import SimpleNamespace

    path = tmp_path / "config.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    monkeypatch.setattr(
        cfg_api,
        "_env",
        lambda: {
            "CW": None,
            "cfg_base": SimpleNamespace(config_path=lambda: path),
            "load": lambda: {},
            "save": lambda *_: None,
            "prune": lambda *_: None,
            "ensure": lambda *_: None,
            "norm_pair": lambda *_: None,
            "probes_cache": None,
            "probes_status_cache": None,
            "scheduler": None,
        },
    )
    res = cfg_api.api_config_meta(cast(Any, SimpleNamespace(cookies=cookies or {})))
    return json.loads(bytes(res.body).decode("utf-8"))


def test_config_meta_hides_ui_while_reset_pending(monkeypatch, tmp_path) -> None:
    data = _meta(
        monkeypatch,
        tmp_path,
        {
            "version": "0.11.0",
            "ui": {"show_watchlist_preview": True},
            "tmdb": {"api_key": "real-key"},
            "app_auth": {"enabled": True, "reset_required": True, "username": "admin", "password": {"hash": "h", "salt": "s"}},
        },
    )
    assert "ui" not in data
    assert "tmdb_configured" not in data
    assert data["auth_reset_required"] is True


def test_config_meta_withholds_config_path_from_non_admins(monkeypatch, tmp_path) -> None:
    data = _meta(monkeypatch, tmp_path, {"version": "0.11.0", "ui": {"show_watchlist_preview": True}})
    assert "ui" in data
    for key in ("path", "size", "mtime"):
        assert key not in data


# Role matrix - admin unchanged, read-only sees only what it may use, full access gets its tabs working
def _roles() -> dict[str, dict[str, Any]]:
    ro = _managed_user()
    ro["permissions"] = {"dashboard": True, "watchlist": True, "playback": True, "write": False}
    return {"admin": {"id": "administrator", "is_admin": True}, "readonly": ro, "full": _managed_user()}


def _allowed(user: dict[str, Any], path: str, method: str) -> bool:
    import crosswatch
    from api import appAuthAPI as auth

    if user.get("is_admin"):
        return True
    return auth.non_admin_api_allowed(path, method) and crosswatch._non_admin_permission_allowed(user, path, method)


def test_admin_gate_is_bypassed_entirely_for_admins() -> None:
    import crosswatch
    import inspect

    src = inspect.getsource(crosswatch.app_auth_gate)
    assert 'if not user.get("is_admin"):' in src
    assert "app_non_admin_api_allowed" in src


def test_editor_metadata_tools_follow_the_editor_tab() -> None:
    roles = _roles()
    for path, method in (("/api/metadata/search", "GET"), ("/api/metadata/resolve", "POST")):
        assert _allowed(roles["full"], path, method) is True
        assert _allowed(roles["readonly"], path, method) is False
        assert _allowed(roles["admin"], path, method) is True


def test_playback_actions_track_the_write_permission() -> None:
    roles = _roles()
    for path in (
        "/api/playback_progress/actions/bulk",
        "/api/playback_progress/actions/mark_watched",
        "/api/playback_progress/actions/remove",
        "/api/playback_progress/actions/update_progress",
    ):
        assert _allowed(roles["full"], path, "POST") is True
        assert _allowed(roles["readonly"], path, "POST") is False
    for role in ("full", "readonly"):
        assert _allowed(roles[role], "/api/playback_progress/items", "GET") is True
        assert _allowed(roles[role], "/api/playback_progress/settings", "POST") is False


def test_run_unresolved_allowed_for_write_and_scoped_to_owned_runs(monkeypatch) -> None:
    from api import syncAPI as sync_api

    roles = _roles()
    assert _allowed(roles["full"], "/api/run/unresolved", "GET") is True
    assert _allowed(roles["readonly"], "/api/run/unresolved", "GET") is False

    cfg = _pairs_cfg()
    monkeypatch.setattr(sync_api, "_env", lambda: (lambda: cfg, lambda *_: None))
    monkeypatch.setattr(sync_api, "_summary_snapshot", lambda: {"pair_scope_ids": ["theirs"]})
    req = _request("/api/run/unresolved")
    req.scope["state"]["cw_user"] = roles["full"]
    body = json.loads(bytes(sync_api.api_run_unresolved(request=req).body).decode("utf-8"))
    assert body == {"total": 0, "items": []}


def test_captures_scheduler_queue_is_hidden_from_managed_users() -> None:
    import pathlib

    src = pathlib.Path("assets/js/snapshots.js").read_text(encoding="utf-8")
    assert 'const schedulerAvailable = () => !isManagedUser();' in src
    assert "if (schedulerAvailable()) addScheduleBtn?.addEventListener" in src
    assert 'wrap.classList.toggle("hidden", !schedulerAvailable() || !items.length)' in src


def test_playback_ui_splits_write_actions_from_admin_settings() -> None:
    import pathlib

    src = pathlib.Path("assets/js/playback_progress.js").read_text(encoding="utf-8")
    assert "const isReadOnly = () => isManagedUser() && !canWrite();" in src
    assert "const canEditSettings = () => !isManagedUser();" in src
    assert 'document.getElementById("pp-settings")?.classList.toggle("hidden", !canEditSettings());' in src


def test_insights_snapshot_picker_is_admin_only() -> None:
    import pathlib

    src = pathlib.Path("assets/js/insights.js").read_text(encoding="utf-8")
    idx = src.find("openCrosswatchSnapshotPicker(_feature)")
    assert idx > 0
    assert 'cwRole === "user") return;' in src[max(0, idx - 400):idx]


# Manual policy is shared storage keyed by provider/instance - concurrent editor
# saves from different managed users must not clobber each other.
def test_manual_policy_updates_are_read_modify_write_safe(tmp_path) -> None:
    import threading, time
    from cw_platform.local_db import manual_policy

    base = tmp_path
    manual_policy.save_policy(base, {"version": 1, "providers": {}})
    names = [f"PROV{i}" for i in range(8)]
    errors: list[BaseException] = []

    def writer(provider: str) -> None:
        def _mutate(raw: dict[str, Any]) -> None:
            snapshot = dict(raw["providers"])
            time.sleep(0.01)
            raw["providers"] = snapshot
            raw["providers"][provider] = {"watchlist": {"blocks": [provider], "adds": {"items": {}}}}

        try:
            manual_policy.update_policy(base, _mutate)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(n,)) for n in names]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)

    assert not errors, errors
    providers = manual_policy.load_policy(base).get("providers") or {}
    missing = sorted(set(names) - set(providers))
    assert not missing, f"concurrent editor saves lost updates for {missing}"


def test_editor_policy_writes_go_through_update_policy() -> None:
    from api import editorAPI
    import inspect

    src = inspect.getsource(editorAPI._save_policy_manual)
    assert "update_policy" in src
    assert "_save_policy(raw)" not in src


# Shared-instance note - the Editor must say when blocks are shared with other profiles
def _sharing_cfg(*, shared: bool) -> dict[str, Any]:
    cfg = _pairs_cfg()
    if shared:
        cfg["user_profiles"][FOREIGN_PROFILE]["instance_uids"] = [OWNED_UID]
    return cfg


def test_shared_instance_note_lists_every_owning_profile() -> None:
    from api.editorAPI import _shared_instance_note

    note = _shared_instance_note(_sharing_cfg(shared=True), "PLEX", "default")
    assert note["shared"] is True
    assert note["owners"] == ["Alice", "Bob"]


def test_shared_instance_note_is_quiet_for_sole_and_unowned_instances() -> None:
    from api.editorAPI import _shared_instance_note

    sole = _shared_instance_note(_sharing_cfg(shared=False), "PLEX", "default")
    assert sole == {"owners": ["Alice"], "shared": False}

    unowned = _shared_instance_note({"plex": {"server_url": "x", "account_token": "t"}}, "PLEX", "default")
    assert unowned == {"owners": [], "shared": False}


def test_editor_state_response_carries_instance_sharing() -> None:
    from api import editorAPI
    import inspect

    src = inspect.getsource(editorAPI.api_editor_get_state)
    assert src.count('"instance_sharing": _shared_instance_note(cfg, chosen, inst)') == 2


def test_editor_ui_renders_the_shared_instance_note() -> None:
    import pathlib

    shell = pathlib.Path("assets/js/editor.js").read_text(encoding="utf-8")
    assert 'id="cw-instance-shared"' in shell

    ctrl = pathlib.Path("assets/js/editor/load-controller.js").read_text(encoding="utf-8")
    assert "function renderInstanceSharingNote(sharing)" in ctrl
    assert "renderInstanceSharingNote(data && data.instance_sharing);" in ctrl
    assert "apply to every user of this instance" in ctrl

    css = pathlib.Path("assets/css/pages.css").read_text(encoding="utf-8")
    assert ".cw-shared-note{" in css


def test_events_clear_is_hidden_from_managed_users() -> None:
    import pathlib

    from api import eventsAPI

    assert 'if _managed_request(request):' in pathlib.Path("api/eventsAPI.py").read_text(encoding="utf-8")
    assert eventsAPI.events_clear.__module__ == "api.eventsAPI"

    modal = pathlib.Path("assets/js/modals/events/index.js").read_text(encoding="utf-8")
    assert '${isAdmin ? `<button class="ev-tbtn" id="ev-clear"' in modal
    assert 'Q("#ev-clear", root)?.addEventListener' in modal
    assert 'showToast("Clear failed.", null, "error")' in modal


def test_editor_profile_picker_does_not_invent_a_default_profile() -> None:
    import pathlib

    editor = pathlib.Path("assets/js/editor.js").read_text(encoding="utf-8")
    assert 'norm.unshift({ id: "default", label: "Default" })' not in editor
    assert 'if (!norm.length) norm.push({ id: "default", label: "Default" });' in editor
    assert 'next = ids.includes("default") ? "default" : ids[0];' in editor


# Profile page: created_at, preferences, and the Main widget grid
def test_user_shapes_expose_created_at_and_preferences() -> None:
    from api.appAuthAPI import _admin_identity, _public_user, clean_user_preferences

    managed = _public_user("a" * 32, {"username": "Pascal", "created_at": 1770000000, "preferences": {"quick_add": False}})
    assert managed["created_at"] == 1770000000
    assert managed["preferences"] == {"playing_card": True, "quick_add": False}

    admin = _admin_identity({"username": "admin", "created_at": 1760000000})
    assert admin["created_at"] == 1760000000
    assert admin["preferences"] == {"playing_card": True, "quick_add": True}

    assert clean_user_preferences(None) == {"playing_card": True, "quick_add": True}


def test_managed_user_creation_stamps_created_at() -> None:
    from api import appAuthAPI as auth
    import inspect

    src = inspect.getsource(auth.api_users_create)
    assert '"created_at": _now(),' in src


def test_admin_credentials_stamp_created_at_once() -> None:
    from api import appAuthAPI as auth
    import inspect

    src = inspect.getsource(auth.api_set_credentials)
    assert 'if not int(a.get("created_at") or 0):' in src
    assert 'a["created_at"] = _now()' in src


def test_settings_oidc_security_form_uses_two_column_layout() -> None:
    import pathlib
    import ui_frontend

    html = ui_frontend.get_index_html()
    css = pathlib.Path("assets/css/app-users.css").read_text(encoding="utf-8")

    assert 'class="cw-auth-oidc-field"' in html
    assert 'class="cw-auth-plex-field"' in html
    assert "#page-settings #app_auth_fields > .cw-settings-2col > .cw-auth-oidc-field" in css
    assert "#page-settings #app_auth_fields .cw-auth-plex-field > .cw-settings-inline-action" in css
    assert "grid-column: 1 / -1;" in css
    assert "#page-settings .cw-auth-oidc-field .cw-app-oidc-grid" in css
    assert "grid-template-columns: repeat(2, minmax(0, 1fr));" in css


def test_profile_page_hosts_the_main_widget_grid() -> None:
    import ui_frontend

    html = ui_frontend.get_profile_html(
        {"is_admin": False, "username": "P", "permissions": {"dashboard": True, "watchlist": True, "playback": True, "write": True}}
    )
    main_html = ui_frontend.get_index_html(include_admin=True)
    assert 'id="dashboard-widgets-card"' in html
    assert 'id="placeholder-card"' not in html
    assert 'id="placeholder-card"' in main_html
    assert "/assets/js/dashboard-widgets.js" in html
    assert "cwIsAuthSetupPending" in html


def test_profile_widget_layout_is_stored_separately_from_main() -> None:
    import pathlib

    js = pathlib.Path("assets/js/dashboard-widgets.js").read_text(encoding="utf-8")
    preview_js = pathlib.Path("assets/helpers/watchlist-preview.js").read_text(encoding="utf-8")
    assert 'const ON_PROFILE_PAGE = !!document.getElementById("profile-hero");' in js
    assert 'ON_PROFILE_PAGE ? "cw.profileWidgets.layout.v3" : "cw.dashboardWidgets.layout.v3"' in js
    assert 'const ALL_WIDGETS = [' in js
    assert 'const WIDGETS = ON_PROFILE_PAGE ? ALL_WIDGETS.filter((widget) => widget.key !== "watchlist") : ALL_WIDGETS;' in js
    assert 'watchlist: { order: 0, size: "large", span: 1, view: "icon", horizontalView: "media", hidden: false }' in js
    assert 'ratings: { order: 2, size: "small", span: 1, view: "grid", horizontalView: "media", hidden: false }' in js
    reset_branch = js[js.index('} else if (action === "reset") {'):js.index("markWidgetsDirty(0);", js.index('} else if (action === "reset") {'))]
    assert "customizeOpen = false;" in reset_branch
    assert "function profileWatchlistWidgetHidden()" in preview_js
    assert "return !!document.getElementById(\"profile-hero\");" in preview_js
    assert "if (profileWatchlistWidgetHidden()) { hidePreviewCard(card, row, msg, { preserve: true }); return false; }" in preview_js


# Profile page rebuild: hero, layout, preferences
def _profile_html() -> str:
    import ui_frontend

    return ui_frontend.get_profile_html(
        {"is_admin": False, "username": "P", "created_at": 1770000000,
         "permissions": {"dashboard": True, "watchlist": True, "playback": True, "write": True}}
    )


def test_profile_hero_has_both_art_layers_and_now_playing_readout() -> None:
    html = _profile_html()
    import pathlib

    css = pathlib.Path("assets/css/profile-page.css").read_text(encoding="utf-8")
    for marker in ('id="profile-hero-art"', 'id="profile-hero-now"', 'id="profile-hero-seam"',
                   'id="profile-now"', 'id="profile-now-fill"', 'id="profile-member-since"',
                   'id="profile-hero-chips"', 'class="cw-profile-last hidden" role="button" tabindex="0"',
                   'class="cw-profile-last-poster"'):
        assert marker in html, marker
    assert "profile-last-details" not in html
    assert ".cw-profile-shell{width:calc(100vw - 40px);max-width:none;" in css
    assert "width:min(1380px,calc(100vw - 40px))" not in css


def test_profile_overview_uses_the_new_pair_layout() -> None:
    html = _profile_html()
    assert "Continue Watching" in html
    assert "Recent Watchlist" in html
    assert 'id="recent-activity"' in html
    assert 'id="profile-quick-stats"' in html
    assert html.count('class="cw-profile-pair-grid"') == 2
    # the old duplicated blocks are gone, the Main widget grid took over
    assert 'id="profile-history"' not in html
    assert 'id="profile-ratings"' not in html
    assert 'id="profile-scrobble"' not in html
    assert 'id="dashboard-widgets-card"' in html


def test_profile_preferences_tab_and_panel_exist() -> None:
    html = _profile_html()

    assert 'data-profile-tab="preferences"' in html
    assert 'id="profile-panel-preferences"' in html
    assert 'id="profile-pref-playing-card"' in html
    assert 'id="profile-pref-quick-add"' in html
    assert html.count('id="profile-pref-playing-card" type="checkbox" checked') == 1


def test_profile_js_wires_hero_scrobble_and_preferences() -> None:
    import pathlib

    js = pathlib.Path("assets/js/profile-page.js").read_text(encoding="utf-8")
    assert "/api/watch/currently_watching" in js
    assert "function renderNowPlaying(payload)" in js
    assert "function renderMemberSince(user)" in js
    assert "wirePreferences();" in js
    assert "visibleProviderLabel" in js
    assert "last.dataset.profilePosterKey = key" in js
    assert 'event.key !== "Enter" && event.key !== " "' in js
    assert '"preferences": ' not in js and "preferences: {" in js


def test_profile_css_defines_the_crossover() -> None:
    import pathlib

    css = pathlib.Path("assets/css/profile-page.css").read_text(encoding="utf-8")
    assert ".cw-profile-hero-now{" in css
    assert ".cw-profile-hero-art{position:absolute;inset:0;" in css
    assert ".cw-profile-last{position:absolute;z-index:3;right:46px;bottom:34px;width:min(470px,38vw);" in css
    assert ".cw-profile-last-poster{width:90px;height:auto;justify-self:end;margin:-11px -15px -11px 0;" in css
    assert "border-radius:0 15px 15px 0" in css
    assert ".cw-profile-hero.is-playing .cw-profile-last{display:none}" in css
    assert ".cw-profile-hero.is-playing .cw-profile-hero-art{" in css
    assert css.count("{") == css.count("}")


# Profile page corrections: reuse Main's machinery instead of re-implementing it
def test_widgets_are_not_suppressed_on_the_profile_page() -> None:
    import pathlib

    js = pathlib.Path("assets/js/dashboard-widgets.js").read_text(encoding="utf-8")
    body = js[js.index("function isOnMain()"):js.index("function widgetSettings")]
    assert "if (ON_PROFILE_PAGE) return true;" in body


def test_profile_reuses_the_real_activity_feed() -> None:
    html = _profile_html()

    import pathlib

    js = pathlib.Path("assets/js/profile-page.js").read_text(encoding="utf-8")
    assert 'id="recent-activity-block"' in html
    assert 'id="recent-activity"' in html
    assert "/assets/js/activity.js" in html
    # the hand-rolled media feed is gone
    assert "activityRows(" not in js
    assert 'id="profile-activity"' not in html


def test_services_chip_counts_connected_instances_only() -> None:
    import pathlib

    js = pathlib.Path("assets/js/profile-page.js").read_text(encoding="utf-8")
    assert "function connectedServices(status)" in js
    assert 'api("/api/status")' in js
    assert "providerCount(" not in js


def test_continue_watching_and_watchlist_use_designed_rows() -> None:
    import pathlib

    js = pathlib.Path("assets/js/profile-page.js").read_text(encoding="utf-8")
    assert "function progressCard(item)" in js
    assert "function watchlistRow(item)" in js
    assert "function mediaKindLabel(item)" in js
    assert "map(progressCard)" in js
    assert "map(watchlistRow)" in js

    css = pathlib.Path("assets/css/profile-page.css").read_text(encoding="utf-8")
    assert ".cw-cw-card{" in css


# Continue Watching card + widget layout controls
def test_continue_watching_card_matches_the_spec() -> None:
    import pathlib

    js = pathlib.Path("assets/js/profile-page.js").read_text(encoding="utf-8")
    card = js[js.index("function progressCard(item)"):js.index("function mediaKindLabel(item)")]
    # landscape artwork, not a portrait poster
    assert "watchlistPreviewArt(item) || poster(item" in card
    assert 'class="cw-cw-art"' in card
    # title, episode/year sub-line, bar + percentage, nothing else
    for marker in ("cw-cw-title", "cw-cw-sub", "cw-cw-track", "cw-cw-pct"):
        assert marker in card, marker
    assert 'const episode = episodeOf(item);' in card
    assert 'class="cw-cw-episode"' in card
    # provider icons overlay the artwork; no provider text on the card body
    assert 'class="cw-cw-providers"' in card
    assert "progressProviders(item).map(providerIconHtml)" in card
    body = card[card.index('class="cw-cw-title"'):]
    assert "provider" not in body.lower()

    css = pathlib.Path("assets/css/profile-page.css").read_text(encoding="utf-8")
    assert "aspect-ratio:16/9" in css
    assert "#profile-progress .cw-cw-episode{" in css
    assert "-webkit-line-clamp:2" in css
    assert "#profile-progress{display:flex" in css and "overflow-x:auto" in css


def test_progress_fill_width_is_not_overridden_by_flex_grow() -> None:
    import pathlib

    css = pathlib.Path("assets/css/profile-page.css").read_text(encoding="utf-8")
    fill = css[css.index(".cw-cw-track i{"):css.index(".cw-cw-pct{")]
    # the fill must not grow - its inline width:N% is the single source of truth
    assert "flex:" not in fill
    assert "width" not in fill


def test_widget_layout_toolbar_offers_reset() -> None:
    import pathlib

    js = pathlib.Path("assets/js/dashboard-widgets.js").read_text(encoding="utf-8")
    body = js[js.index("function updateLayoutToolbar()"):js.index("function ensureLayoutToolbar()")]
    assert "card.prepend(bar)" in body
    assert "if (!ON_PROFILE_PAGE)" not in body
    assert '"reset"' in js
    for action in ("customize", "show-all", "reset"):
        assert f'["{action}"' in js or f'"{action}",' in js


def test_widget_layout_controls_are_available_on_main_too() -> None:
    import pathlib

    js = pathlib.Path("assets/js/dashboard-widgets.js").read_text(encoding="utf-8")
    css = pathlib.Path("assets/crosswatch.css").read_text(encoding="utf-8")
    body = js[js.index("function ensureWidgetControls()"):js.index("function syncControlIcons()")]

    assert "querySelectorAll(\".cw-dash-layout-controls\").forEach((node) => node.remove())" not in body
    assert "if (!ON_PROFILE_PAGE)" not in body
    assert ".cw-dash-layout-controls{position:absolute;right:104px" in css
    assert "display:flex;align-items:center;gap:10px" in css
    assert "#placeholder-card .cw-dash-layout-controls{right:104px" in css


def test_profile_widget_items_open_the_profile_preview_drawer() -> None:
    import pathlib

    js = pathlib.Path("assets/js/dashboard-widgets.js").read_text(encoding="utf-8")
    helper = js[js.index("function openProfileWidgetPreview"):js.index("function historyCard")]
    click = js[js.index('const itemLink = event.target?.closest?.("[data-cw-widget-item]")'):js.index('const btn = event.target?.closest?.("[data-cw-widget-more]")')]

    assert "ON_PROFILE_PAGE" in helper
    assert "window.CW?.WatchlistPreview?.openPreviewDrawer || window.openPreviewDrawer" in helper
    assert "latestItems[kind]?.[Number(index)]" in helper
    assert "void open(item);" in helper
    assert "if (openProfileWidgetPreview(kind, index)) return;" in click
    assert "void openDetailCard(kind, index);" in click


def test_profile_widgets_adopt_the_profile_card_style() -> None:
    import pathlib

    css = pathlib.Path("assets/css/profile-page.css").read_text(encoding="utf-8")
    assert "body.cw-profile-page #dashboard-widgets-card .cw-dash-widget{" in css
    assert "body.cw-profile-page #dashboard-widgets-card{--cw-widget-cols:3" in css


# Continue Watching / Recent Watchlist sizing and the profile widget skin
def test_three_continue_watching_cards_fit_without_clipping() -> None:
    import pathlib

    css = pathlib.Path("assets/css/profile-page.css").read_text(encoding="utf-8")
    assert "flex:0 0 calc((100% - 24px)/3)" in css
    # the row still scrolls once there are more than three
    assert "#profile-progress{display:flex" in css and "overflow-x:auto" in css


def test_watchlist_shows_three_rows_with_an_added_stamp() -> None:
    import pathlib

    js = pathlib.Path("assets/js/profile-page.js").read_text(encoding="utf-8")
    assert "watchlistItems.slice(0, 3).map((item) => watchlistRow(item, wall?.last_sync_epoch))" in js
    added = js[js.index("function addedEpoch(item)"):js.index("function watchlistRow(item, fallbackSyncEpoch = 0)")]
    assert "added_epoch" in added and "added_when" in added and "Date.parse" in added
    assert "function syncedEpoch(item, fallbackEpoch = 0)" in added
    assert "updated ${relTime(when)}" in js
    assert 'class="cw-profile-watchlist-sync"' in js


def test_watchlist_widget_is_renamed() -> None:
    html = _profile_html()
    assert "<h2>Recent Watchlist</h2>" in html
    assert "Recent Watchlist Additions" not in html


def test_profile_widget_skin_is_scoped_and_unmuted() -> None:
    import pathlib

    css = pathlib.Path("assets/css/profile-page.css").read_text(encoding="utf-8")
    skin = css[css.index("/* Profile-only widget skin."):]
    # every rule in the skin must be scoped so Main keeps its own look
    for line in skin.splitlines():
        if line.strip().startswith(("body.cw-profile-page", "@media", "}", "/*", "--", "background:", "box-shadow:", "border:")) or not line.strip():
            continue
        assert line.strip().endswith(("{", ",")) is False or "body.cw-profile-page" in line, line
    assert "cw-dash-title-row>.material-symbols-rounded{display:none}" in skin
    assert ".cw-dash-title-row h3{font-size:16px" in skin
    assert ".cw-dash-widget.is-auto-collapsed{opacity:1}" in skin
    assert ".cw-dashboard-layout-tools{top:-54px" in skin
    assert '#placeholder-card[data-widget-size="small"][data-widget-view="grid"] .poster{' in skin
    assert "grid-template-columns:clamp(150px,34%,220px) minmax(190px,1fr) auto!important" in skin
    assert "#placeholder-card[data-widget-size=\"small\"][data-widget-view=\"grid\"] .poster .wl-status" in skin


def test_main_dashboard_widget_title_icons_stay_visible() -> None:
    import pathlib
    import ui_frontend

    html = ui_frontend.get_index_html(include_admin=True)
    crosswatch_css = pathlib.Path("assets/crosswatch.css").read_text(encoding="utf-8")
    flat_css = pathlib.Path("assets/themes/flat.css").read_text(encoding="utf-8")
    profile_css = pathlib.Path("assets/css/profile-page.css").read_text(encoding="utf-8")

    for icon in ("play_arrow", "star", "sensors", "timelapse", "queue_music"):
        assert f'<span class="material-symbols-rounded" aria-hidden="true">{icon}</span>' in html

    assert ".cw-dash-title-row .material-symbols-rounded{display:inline-grid" in crosswatch_css
    assert "body:not(.cw-profile-page) #dashboard-widgets-card .cw-dash-title-row .material-symbols-rounded" in flat_css
    assert "display:inline-grid!important" in flat_css
    assert "body.cw-profile-page #dashboard-widgets-card .cw-dash-title-row>.material-symbols-rounded{display:none}" in profile_css


def test_latest_ratings_age_badges_are_not_hard_clipped() -> None:
    import pathlib

    css = pathlib.Path("assets/css/components.css").read_text(encoding="utf-8")
    rule = css[css.index("#latest-ratings-grid .cw-rating-age-badge{"):css.index("#latest-ratings-grid .cw-rating-provider-icons{")]

    assert "width:max-content" in rule
    assert "max-width:calc(100% - 44px)" in rule
    assert "max-width:78px" not in rule


def test_sync_hub_watcher_tooltip_stays_inside_viewport() -> None:
    import pathlib

    js = pathlib.Path("assets/js/schedulerbanner.js").read_text(encoding="utf-8")

    assert ":is(#chip-watch,#chip-hook) .cw-hub-tip" in js
    assert "left:0;right:auto;width:min(360px,calc(100vw - 32px));min-width:0;max-width:calc(100vw - 32px)" in js
    assert ":is(#chip-watch,#chip-hook)[data-tip]:hover>.cw-hub-tip" in js


def test_main_dashboard_styles_are_not_touched() -> None:
    import pathlib

    main_css = pathlib.Path("assets/crosswatch.css").read_text(encoding="utf-8")
    assert "cw-profile-page" not in main_css


def test_preferences_tab_is_hidden_from_read_only_users() -> None:
    import ui_frontend

    def page(**kw: Any) -> str:
        return ui_frontend.get_profile_html(kw)

    full = {"dashboard": True, "watchlist": True, "playback": True, "write": True}
    readonly = {**full, "write": False}

    for html in (page(is_admin=True, username="a"),
                 page(is_admin=False, username="f", permissions=full)):
        assert 'data-profile-tab="preferences"' in html
        assert 'id="profile-panel-preferences"' in html

    ro = page(is_admin=False, username="r", permissions=readonly)
    assert 'data-profile-tab="preferences"' not in ro
    assert 'id="profile-panel-preferences"' not in ro
    assert 'id="profile-pref-playing-card"' not in ro
    assert ro.count("<section") == ro.count("</section>")
