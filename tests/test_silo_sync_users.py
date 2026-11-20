# CrossWatch test scripts
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any

import pytest

SERVER = "http://silo.local:8090"


@dataclass
class ResponseStub:
    status_code: int = 200
    payload: Any = None
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def text(self) -> str:
        return "" if self.payload is None else "x"

    def json(self) -> Any:
        return self.payload


def _cfg() -> dict[str, Any]:
    return {
        "silo": {
            "server_url": SERVER, "access_token": "a1", "refresh_token": "r1", "expires_at": 4_000_000_000,
            "profile_id": "p-owner", "profile_name": "Alice", "profile_token": "owner-pin-token", "profile_has_pin": True,
            "sync_users": {
                "p-kid": {"profile_id": "p-kid", "profile_name": "Kid", "username": "Kid", "user": "Kid", "profile_has_pin": True, "profile_token": "kid-token"},
                "p-guest": {"profile_id": "p-guest", "profile_name": "Guest", "username": "Guest", "user": "Guest", "profile_has_pin": False, "profile_token": ""},
            },
        },
    }


def test_sync_user_block_uses_parent_tokens_and_own_profile() -> None:
    from cw_platform.provider_instances import build_provider_config_view, list_sync_user_ids
    from providers.auth import _auth_SILO as silo

    cfg = _cfg()
    assert sorted(list_sync_user_ids(cfg, "silo")) == ["default~p-guest", "default~p-kid"]
    kid = silo.provider_block(cfg, "default~p-kid")
    assert kid["access_token"] == "a1" and kid["profile_id"] == "p-kid" and kid["profile_token"] == "kid-token"
    guest = silo.provider_block(cfg, "default~p-guest")
    assert guest["profile_id"] == "p-guest" and guest["profile_token"] == ""
    view = build_provider_config_view(cfg, "silo", "default~p-kid")
    assert silo.provider_block(view, "default~p-kid")["profile_id"] == "p-kid"
    assert silo.provider_block(cfg, "default~p-nobody") == {}
    headers = silo.base_headers(kid, token="a1", profile=True)
    assert headers["X-Profile-Id"] == "p-kid" and headers["X-Profile-Token"] == "kid-token"


def test_refresh_for_sync_user_writes_parent_tokens_only(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _cfg()
    cfg["silo"]["expires_at"] = 1
    holder = {"cfg": cfg}
    monkeypatch.setattr(silo, "save_config", lambda c: holder.__setitem__("cfg", c))
    monkeypatch.setattr(silo, "load_config", lambda: holder["cfg"])

    class Session:
        def post(self, url: str, **kw: Any) -> ResponseStub:
            assert url.endswith("/api/v2/auth/refresh")
            return ResponseStub(200, {"access_token": "a2", "refresh_token": "r1", "expires_in": 3600})

    res = silo.SiloClient(cfg, instance_id="default~p-kid", session=Session()).refresh_token(cfg)

    block = holder["cfg"]["silo"]
    assert res["ok"] is True and block["access_token"] == "a2"
    assert "instances" not in block
    assert block["sync_users"]["p-kid"]["profile_token"] == "kid-token"
    assert silo.provider_block(holder["cfg"], "default~p-kid")["access_token"] == "a2"


def test_stale_pin_token_is_flagged_on_the_user_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _cfg()
    holder = {"cfg": cfg}
    monkeypatch.setattr(silo, "save_config", lambda c: holder.__setitem__("cfg", c))
    monkeypatch.setattr(silo, "load_config", lambda: holder["cfg"])

    silo.SiloClient(cfg, instance_id="default~p-kid")._flag_profile_pin()

    entry = holder["cfg"]["silo"]["sync_users"]["p-kid"]
    assert entry["auth_error"] == "profile_pin_required" and entry["profile_token"] == ""
    assert "auth_error" not in holder["cfg"]["silo"] and holder["cfg"]["silo"]["profile_token"] == "owner-pin-token"


def test_sync_users_api_entry_verifies_pin_and_never_stores_it(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import providerInstancesAPI as api
    from providers.auth import _auth_SILO as silo

    calls: list[Any] = []

    def profiles(self: Any) -> list[dict[str, Any]]:
        return [{"id": "p-kid", "name": "Kid", "has_pin": True}, {"id": "p-guest", "name": "Guest", "has_pin": False}]

    def verify(self: Any, pid: str, pin: Any) -> tuple[str, int]:
        calls.append((pid, pin))
        if pin != "1234":
            raise silo.SiloProfileError("invalid_pin")
        return "fresh-kid-token", 0

    monkeypatch.setattr(silo.SiloClient, "list_profiles", profiles)
    monkeypatch.setattr(silo.SiloClient, "verify_pin", verify)
    wanted: dict[str, Any] = {"p-kid": {"key": "p-kid", "username": "Kid", "home_pin": "1234"}, "p-guest": {"key": "p-guest", "username": "Guest"}}

    assert api._silo_prepare_sync_users(_cfg(), "default", wanted) == ""
    kid = api._build_sync_user_entry("silo", "p-kid", wanted["p-kid"], {}, 100)
    guest = api._build_sync_user_entry("silo", "p-guest", wanted["p-guest"], {}, 100)
    assert kid["profile_token"] == "fresh-kid-token" and kid["profile_has_pin"] is True and "home_pin" not in kid
    assert guest["profile_token"] == "" and guest["profile_id"] == "p-guest" and guest["added_at"] == 100
    assert calls == [("p-kid", "1234")]
    assert "silo" in api._SYNC_USER_PROVIDERS
    bad = {"p-kid": {"key": "p-kid", "username": "Kid", "home_pin": "0000"}}
    assert api._silo_prepare_sync_users(_cfg(), "default", bad) == "invalid_pin"
    assert api._silo_prepare_sync_users(_cfg(), "default", {"p-x": {"key": "p-x", "username": "X"}}) == "not_found"


def test_pairs_and_routes_link_silo_identities() -> None:
    from cw_platform.orchestrator._pairs import _expand_sync_user_pair
    from providers.scrobble.routes import sync_user_endpoints_supported, sync_user_matches

    cfg = _cfg()
    cfg["jellyfin"] = {"server": "http://jf", "access_token": "t", "user_id": "admin",
                       "sync_users": {"u-kid": {"user_id": "u-kid", "username": "kid", "user": "kid"}}}
    pairs = _expand_sync_user_pair(cfg, {"id": "P1", "source": "SILO", "target": "JELLYFIN", "match_users": True, "user_matches": {"p-kid": "u-kid"}})
    assert [(p["source_instance"], p["target_instance"]) for p in pairs] == [("default~p-kid", "default~u-kid")]

    assert sync_user_endpoints_supported("silo", "default", "jellyfin", "default")
    matches = sync_user_matches(cfg, "silo", "default", "jellyfin", "default", {"p-kid": "u-kid"})
    assert matches == [{"key": "p-kid", "username": "Kid", "sink_instance": "default~u-kid", "ids": ["p-kid"]}]


def test_watcher_adds_ticked_profiles_only_for_user_filtered_routes(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.silo import watch

    class Dispatch:
        def __init__(self, needs: bool) -> None:
            self.needs = needs

        def needs_user_resolution(self) -> bool:
            return self.needs

        def dispatch(self, ev: Any) -> bool:
            return True

    cfg = _cfg()
    plain = watch.SiloWatchService(dispatcher=Dispatch(False), cfg_provider=lambda: cfg)
    matched = watch.SiloWatchService(dispatcher=Dispatch(True), cfg_provider=lambda: cfg)

    assert plain._profile_ids(cfg, cfg["silo"]) == {"p-owner"}
    assert matched._profile_ids(cfg, cfg["silo"]) == {"p-owner", "p-kid", "p-guest"}


def _account_cfg() -> dict[str, Any]:
    cfg = _cfg()
    cfg["silo"]["user_id"] = "1"
    cfg["silo"]["sync_users"]["p-other"] = {
        "profile_id": "p-other", "profile_name": "Other", "username": "Other", "user": "Other", "profile_has_pin": False,
        "profile_token": "", "profile_token_expires_at": 0, "account_user_id": "2", "account_username": "other",
        "access_token": "acc-a1", "refresh_token": "acc-r1", "expires_at": 4_000_000_000, "session_refreshed_at": 1, "auth_error": "",
    }
    return cfg


def _holder(monkeypatch: pytest.MonkeyPatch, cfg: dict[str, Any]) -> dict[str, Any]:
    from providers.auth import _auth_SILO as silo

    holder = {"cfg": cfg}
    monkeypatch.setattr(silo, "save_config", lambda c: holder.__setitem__("cfg", c))
    monkeypatch.setattr(silo, "load_config", lambda: holder["cfg"])
    return holder


def test_other_account_profiles_skip_self_owner_disabled_and_admins_unless_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    seen: list[tuple[str, str, dict[str, str]]] = []
    accounts = [
        {"id": "1", "username": "me", "role": "admin", "is_owner": False, "enabled": True},
        {"id": "9", "username": "owner", "role": "admin", "is_owner": True, "enabled": True},
        {"id": "2", "username": "other", "role": "user", "is_owner": False, "enabled": True},
        {"id": "3", "username": "staff", "role": "admin", "is_owner": False, "enabled": True},
        {"id": "4", "username": "gone", "role": "user", "is_owner": False, "enabled": False},
    ]

    class Session:
        def request(self, method: str, url: str, headers: dict[str, str], **kw: Any) -> ResponseStub:
            path = url.split("/api/v2", 1)[1]
            seen.append((method, path, headers))
            if path == "/profiles":
                return ResponseStub(200, {"items": [{"id": "p-owner", "name": "Alice", "is_primary": True, "has_pin": True}]})
            if path == "/admin/users":
                return ResponseStub(200, {"items": accounts, "page": {"has_more": False}})
            if path == "/admin/users/2/profiles":
                return ResponseStub(200, {"items": [{"id": "p-other", "name": "Other", "is_primary": True}]})
            raise AssertionError(path)

    rows = silo.SiloClient(_account_cfg(), session=Session()).list_other_account_profiles()

    assert rows == [{"id": "p-other", "profile_id": "p-other", "name": "Other", "is_primary": True, "has_pin": False, "is_child": False,
                     "account_user_id": "2", "account_username": "other", "account_primary_profile_id": "p-other"}]
    admin = [h for m, p, h in seen if p.startswith("/admin/")]
    assert admin and all(h["X-Profile-Id"] == "p-owner" and h["X-Profile-Token"] == "owner-pin-token" for h in admin)


def test_admin_headers_need_the_primary_profile_pin_token(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _account_cfg()
    cfg["silo"]["profile_id"] = "p-guest"
    monkeypatch.setattr(silo.SiloClient, "list_profiles", lambda self: [{"id": "p-owner", "name": "Alice", "is_primary": True, "has_pin": True}])

    with pytest.raises(silo.SiloProfileError):
        silo.SiloClient(cfg)._admin_headers()
    assert silo.SiloClient(cfg).list_other_account_profiles() == []


def test_saving_an_account_identity_opens_a_session_and_stores_it_on_the_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import providerInstancesAPI as api
    from providers.auth import _auth_SILO as silo

    opened: list[str] = []
    monkeypatch.setattr(silo.SiloClient, "list_profiles", lambda self: [{"id": "p-kid", "name": "Kid", "has_pin": True}])
    monkeypatch.setattr(silo.SiloClient, "list_other_account_profiles", lambda self: [{"id": "p-new", "name": "New", "is_primary": True, "is_child": False,
                                                                                        "account_user_id": "5", "account_username": "newbie"}])
    monkeypatch.setattr(silo.SiloClient, "open_account_session", lambda self, uid: opened.append(uid) or {
        "access_token": "new-a", "refresh_token": "new-r", "expires_at": 500, "session_refreshed_at": 100})
    wanted: dict[str, Any] = {"p-new": {"key": "p-new", "username": "New"}}
    sessions: list[str] = []

    assert api._silo_prepare_sync_users(_cfg(), "default", wanted, sessions) == ""
    entry = api._build_sync_user_entry("silo", "p-new", wanted["p-new"], {}, 100)

    assert opened == ["5"] and sessions == ["new-a"]
    assert entry["account_user_id"] == "5" and entry["account_username"] == "newbie" and entry["profile_id"] == "p-new"
    assert entry["access_token"] == "new-a" and entry["refresh_token"] == "new-r" and entry["auth_error"] == ""
    assert entry["profile_token"] == "" and entry["profile_has_pin"] is False
    assert api._silo_prepare_sync_users(_cfg(), "default", {"p-x": {"key": "p-x", "username": "X"}}) == "not_found"


def test_saving_again_reuses_the_live_session_and_keeps_rotated_tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import providerInstancesAPI as api
    from providers.auth import _auth_SILO as silo

    monkeypatch.setattr(silo.SiloClient, "list_profiles", lambda self: [])
    monkeypatch.setattr(silo.SiloClient, "list_other_account_profiles", lambda self: [{"id": "p-other", "name": "Other", "is_primary": True, "is_child": False,
                                                                                        "account_user_id": "2", "account_username": "other"}])
    monkeypatch.setattr(silo.SiloClient, "open_account_session", lambda self, uid: pytest.fail("session must be reused"))
    wanted: dict[str, Any] = {"p-other": {"key": "p-other", "username": "Other"}}
    sessions: list[str] = []

    assert api._silo_prepare_sync_users(_account_cfg(), "default", wanted, sessions) == ""
    latest = {**_account_cfg()["silo"]["sync_users"]["p-other"], "access_token": "rotated-a", "refresh_token": "rotated-r"}
    entry = api._build_sync_user_entry("silo", "p-other", wanted["p-other"], latest, 100)

    assert sessions == [] and entry["access_token"] == "rotated-a" and entry["refresh_token"] == "rotated-r"
    assert api._silo_session_tokens({"p-other": entry, "p-kid": _cfg()["silo"]["sync_users"]["p-kid"]}) == {"p-other": "rotated-a"}


def test_failed_save_ends_the_sessions_it_opened(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import providerInstancesAPI as api
    from providers.auth import _auth_SILO as silo

    ended: list[str] = []
    monkeypatch.setattr(silo.SiloClient, "list_profiles", lambda self: [])
    monkeypatch.setattr(silo.SiloClient, "list_other_account_profiles", lambda self: [
        {"id": "p-a", "name": "A", "is_primary": True, "is_child": False, "account_user_id": "5", "account_username": "a"},
        {"id": "p-b", "name": "B", "is_primary": True, "is_child": False, "account_user_id": "6", "account_username": "b"}])

    def open_session(self: Any, uid: str) -> dict[str, Any]:
        if uid == "6":
            raise silo.SiloAuthError("permission_denied")
        return {"access_token": f"tok-{uid}", "refresh_token": "r", "expires_at": 1, "session_refreshed_at": 1}

    monkeypatch.setattr(silo.SiloClient, "open_account_session", open_session)
    monkeypatch.setattr(silo.SiloClient, "end_session", lambda self, token: ended.append(token) or True)
    monkeypatch.setattr(api, "load_config", lambda: _cfg())
    monkeypatch.setattr(api, "_admin_required_response", lambda request, cfg: None)

    res = api.api_provider_sync_users_update("silo", "default", {"users": [{"key": "p-a", "username": "A"}, {"key": "p-b", "username": "B"}]})

    assert res == {"ok": False, "error": "silo_account_session_failed"}
    assert ended == ["tok-5"]


def test_account_identity_uses_its_own_session_and_profile() -> None:
    from providers.auth import _auth_SILO as silo

    block = silo.provider_block(_account_cfg(), "default~p-other")

    assert block["access_token"] == "acc-a1" and block["refresh_token"] == "acc-r1"
    assert silo.is_account_entry(block) and silo.is_configured(block)
    headers = silo.base_headers(block, token=block["access_token"], profile=True)
    assert headers["Authorization"] == "Bearer acc-a1" and headers["X-Profile-Id"] == "p-other" and "X-Profile-Token" not in headers
    assert not silo.is_account_entry(silo.provider_block(_account_cfg(), "default~p-kid"))


def test_account_refresh_writes_only_its_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _account_cfg()
    cfg["silo"]["sync_users"]["p-other"]["expires_at"] = 1
    holder = _holder(monkeypatch, cfg)
    sent: list[dict[str, Any]] = []

    class Session:
        def post(self, url: str, json: Any = None, **kw: Any) -> ResponseStub:
            assert url.endswith("/api/v2/auth/refresh")
            sent.append(json)
            return ResponseStub(200, {"access_token": "acc-a2", "refresh_token": "acc-r2", "expires_in": 28800, "user": {"id": "2", "username": "other"}})

    res = silo.SiloClient(cfg, instance_id="default~p-other", session=Session()).refresh_token(cfg)

    root = holder["cfg"]["silo"]
    entry = root["sync_users"]["p-other"]
    assert res["ok"] is True and sent == [{"refresh_token": "acc-r1"}]
    assert entry["access_token"] == "acc-a2" and entry["refresh_token"] == "acc-r2" and entry["username"] == "Other"
    assert root["access_token"] == "a1" and root["refresh_token"] == "r1"


@pytest.mark.parametrize("reopen", [True, False])
def test_ended_account_session_is_reopened_through_the_admin(monkeypatch: pytest.MonkeyPatch, reopen: bool) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _account_cfg()
    holder = _holder(monkeypatch, cfg)
    calls: list[str] = []

    class Session:
        def post(self, url: str, **kw: Any) -> ResponseStub:
            return ResponseStub(401, {"type": silo.PROBLEM_PREFIX + "session_expired"})

    def open_session(self: Any, uid: str) -> dict[str, Any]:
        calls.append(f"{self.instance_id}:{uid}")
        if not reopen:
            raise silo.SiloAuthError("permission_denied")
        return {"access_token": "re-a", "refresh_token": "re-r", "expires_at": 4_000_000_000, "session_refreshed_at": 5}

    monkeypatch.setattr(silo.SiloClient, "open_account_session", open_session)

    res = silo.SiloClient(cfg, instance_id="default~p-other", session=Session()).refresh_token(cfg, force=True)

    entry = holder["cfg"]["silo"]["sync_users"]["p-other"]
    assert calls == ["default:2"]
    if reopen:
        assert res["ok"] is True and entry["access_token"] == "re-a" and entry["refresh_token"] == "re-r" and entry["auth_error"] == ""
    else:
        assert res["reconnect_required"] is True and entry["auth_error"] == "reconnect_required"
    assert holder["cfg"]["silo"]["access_token"] == "a1"


def test_reopened_session_is_ended_when_the_identity_was_removed_meanwhile(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _account_cfg()
    holder = _holder(monkeypatch, cfg)
    ended: list[str] = []

    class Session:
        def post(self, url: str, **kw: Any) -> ResponseStub:
            if url.endswith("/auth/logout"):
                ended.append(kw["headers"]["Authorization"])
                return ResponseStub(204, None)
            return ResponseStub(401, {"type": silo.PROBLEM_PREFIX + "session_expired"})

    def open_session(self: Any, uid: str) -> dict[str, Any]:
        removed = copy.deepcopy(holder["cfg"])
        removed["silo"]["sync_users"].pop("p-other")
        holder["cfg"] = removed
        return {"access_token": "orphan-a", "refresh_token": "orphan-r", "expires_at": 4_000_000_000, "session_refreshed_at": 5}

    monkeypatch.setattr(silo.SiloClient, "open_account_session", open_session)

    res = silo.SiloClient(cfg, instance_id="default~p-other", session=Session()).refresh_token(cfg, force=True)

    assert res["ok"] is False and ended == ["Bearer orphan-a"]
    assert "p-other" not in holder["cfg"]["silo"]["sync_users"]


@pytest.mark.parametrize("accounts", [True, False])
def test_users_endpoint_lists_other_accounts_only_when_asked(monkeypatch: pytest.MonkeyPatch, accounts: bool) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from api.authenticationAPI import register_auth
    from providers.auth import _auth_SILO as silo

    calls: list[str] = []
    monkeypatch.setattr("api.authenticationAPI.load_config", lambda: _account_cfg())
    monkeypatch.setattr(silo.SiloClient, "list_profiles", lambda self: [{"id": "p-owner", "name": "Alice", "has_pin": True, "is_child": False, "is_primary": True}])

    def others(self: Any) -> list[dict[str, Any]]:
        calls.append("others")
        return [{"id": "p-other", "name": "Other", "is_child": False, "is_primary": True, "account_user_id": "2", "account_username": "other"}]

    monkeypatch.setattr(silo.SiloClient, "list_other_account_profiles", others)
    app = FastAPI()
    register_auth(app)

    res = TestClient(app).get("/api/silo/users" + ("?accounts=1" if accounts else "")).json()

    names = [(u["username"], u.get("account")) for u in res["users"]]
    assert names == ([("Alice", None), ("Other", "other")] if accounts else [("Alice", None)])
    assert calls == (["others"] if accounts else [])


def test_disconnect_ends_account_sessions_and_keepalive_covers_them(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_SILO as silo

    cfg = _account_cfg()
    parent = copy.deepcopy(cfg["silo"])
    assert silo.detach_account_sessions(parent) == ["acc-a1"]
    gone = parent["sync_users"]["p-other"]
    assert gone["access_token"] == "" and gone["refresh_token"] == "" and gone["auth_error"] == "reconnect_required"
    assert parent["sync_users"]["p-kid"]["profile_token"] == "kid-token"

    holder = _holder(monkeypatch, cfg)
    refreshed: list[str] = []
    monkeypatch.setattr(silo.SiloClient, "refresh_token", lambda self, c=None, force=False, **kw: refreshed.append(self.instance_id) or {"status": "ok"})
    cfg["silo"]["session_refreshed_at"] = silo.now()
    silo.refresh_all()
    assert refreshed == ["default~p-other"]
    assert holder["cfg"] is cfg


@pytest.mark.parametrize("identities", [True, False])
def test_watcher_picks_up_plays_of_another_account(monkeypatch: pytest.MonkeyPatch, identities: bool) -> None:
    from providers.scrobble.silo import watch

    cfg = _account_cfg()
    cfg["silo"]["sync_users"]["p-other"]["account_primary_profile_id"] = "p-other-main"
    calls: list[tuple[str, str | None]] = []
    row = {"id": "s-9", "media_item_id": "movie-tmdb-100", "media_type": "movie", "media_title": "Alpha", "profile_id": "p-other",
           "profile_name": "Other", "position_seconds": 600.0, "file_duration": 6000, "is_paused": False, "started_at": "2026-11-20T20:00:00.000Z"}

    def request(adapter: Any, method: str, path: str, **kw: Any) -> ResponseStub:
        inst = getattr(adapter, "instance_id", None) or adapter._instance_id
        calls.append((inst, (kw.get("headers") or {}).get("X-Profile-Id")))
        if inst == "default~p-other":
            return ResponseStub(200, {"items": [row]})
        return ResponseStub(200, {"items": []})

    class Dispatch:
        def __init__(self) -> None:
            self.events: list[Any] = []

        def needs_user_resolution(self) -> bool:
            return identities

        def dispatch(self, ev: Any) -> bool:
            self.events.append(ev)
            return True

    monkeypatch.setattr(watch, "silo_request", request)
    monkeypatch.setattr(watch, "item_ids", lambda *a, **k: {"tmdb": "100"})
    monkeypatch.setattr(watch, "_cw_update", lambda *a, **k: None)
    monkeypatch.setattr(watch.SiloWatchService, "_poll_manual_marks", lambda self, cfg, block: None)
    got = Dispatch()
    svc = watch.SiloWatchService(dispatcher=got, cfg_provider=lambda: cfg)

    assert svc._tick() is identities

    if identities:
        assert ("default~p-other", "p-other-main") in calls
        ev = got.events[0]
        assert ev.action == "start" and ev.account == "Other" and ev.raw["_cw_session_identity"]["user_id"] == "p-other"
    else:
        assert calls == [("default", None)] and got.events == []


def test_failed_account_poll_keeps_its_plays_running(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.silo import watch

    cfg = _account_cfg()
    row = {"id": "s-9", "media_item_id": "movie-tmdb-100", "media_type": "movie", "media_title": "Alpha", "profile_id": "p-other",
           "profile_name": "Other", "position_seconds": 600.0, "file_duration": 6000, "is_paused": False, "started_at": "2026-11-20T20:00:00.000Z"}
    state = {"other": ResponseStub(200, {"items": [row]})}

    def request(adapter: Any, method: str, path: str, **kw: Any) -> ResponseStub:
        inst = getattr(adapter, "instance_id", None) or adapter._instance_id
        return state["other"] if inst == "default~p-other" else ResponseStub(200, {"items": []})

    class Dispatch:
        events: list[str] = []

        def needs_user_resolution(self) -> bool:
            return True

        def dispatch(self, ev: Any) -> bool:
            self.events.append(ev.action)
            return True

    monkeypatch.setattr(watch, "silo_request", request)
    monkeypatch.setattr(watch, "item_ids", lambda *a, **k: {"tmdb": "100"})
    monkeypatch.setattr(watch, "_cw_update", lambda *a, **k: None)
    monkeypatch.setattr(watch, "_cw_update_payload", lambda *a, **k: None)
    monkeypatch.setattr(watch.SiloWatchService, "_poll_manual_marks", lambda self, cfg, block: None)
    monkeypatch.setattr(watch, "COMPLETION_RECHECK_SECONDS", 0.0)
    monkeypatch.setattr(watch.SiloWatchService, "_completed_in_session", lambda self, session: False)
    got = Dispatch()
    svc = watch.SiloWatchService(dispatcher=got, cfg_provider=lambda: cfg)

    svc._tick()
    state["other"] = ResponseStub(503, {"type": "x/service_unavailable"})
    svc._tick()
    assert got.events == ["start"] and "s-9" in svc._sessions

    state["other"] = ResponseStub(200, {"items": []})
    svc._tick()
    assert got.events == ["start", "stop"] and "s-9" not in svc._sessions


def test_redaction_masks_sync_user_tokens() -> None:
    from cw_platform.config_base import redact_config

    red = redact_config(copy.deepcopy(_account_cfg()))["silo"]
    assert red["sync_users"]["p-other"]["access_token"] != "acc-a1" and red["sync_users"]["p-other"]["refresh_token"] != "acc-r1"
    assert red["sync_users"]["p-kid"]["profile_token"] != "kid-token"
    assert red["sync_users"]["p-kid"]["username"] == "Kid"
    assert red["profile_token"] != "owner-pin-token"
