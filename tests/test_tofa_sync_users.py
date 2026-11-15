# CrossWatch test scripts
from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from typing import Any

import pytest

SERVER = "http://tofa.local:33333"


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
        "tofa": {
            "server_url": SERVER, "server_id": "srv-1", "access_token": "a1", "refresh_token": "r1", "expires_at": 4_000_000_000,
            "user_id": "u-1", "profile_id": "p-owner", "profile_name": "Alice", "profile_locked": True, "profile_primary": True,
            "profile_token": "owner-pin-token", "profile_token_expires_at": 4_000_000_000,
            "sync_users": {
                "p-kid": {"profile_id": "p-kid", "profile_name": "Kid", "username": "Kid", "user": "Kid", "profile_locked": True,
                          "profile_primary": False, "profile_token": "kid-token", "profile_token_expires_at": 4_000_000_000},
                "p-guest": {"profile_id": "p-guest", "profile_name": "Guest", "username": "Guest", "user": "Guest", "profile_locked": False,
                            "profile_primary": False, "profile_token": ""},
            },
        },
        "scrobble": {"enabled": True, "sources": {"watcher": True}},
    }


def test_sync_user_block_uses_parent_tokens_and_own_profile() -> None:
    from cw_platform.provider_instances import build_provider_config_view, list_sync_user_ids
    from providers.auth import _auth_TOFA as tofa

    cfg = _cfg()
    assert sorted(list_sync_user_ids(cfg, "tofa")) == ["default~p-guest", "default~p-kid"]
    kid = tofa.provider_block(cfg, "default~p-kid")
    assert kid["access_token"] == "a1" and kid["profile_id"] == "p-kid" and kid["profile_token"] == "kid-token"
    guest = tofa.provider_block(cfg, "default~p-guest")
    assert guest["profile_id"] == "p-guest" and guest["profile_token"] == "" and guest["profile_locked"] is False
    assert guest["profile_primary"] is False and tofa.is_configured(guest) and not tofa.needs_profile_pin(guest)
    view = build_provider_config_view(cfg, "tofa", "default~p-kid")
    assert tofa.provider_block(view, "default~p-kid")["profile_id"] == "p-kid"
    assert tofa.provider_block(cfg, "default~p-nobody") == {}
    headers = tofa.base_headers(kid, token="a1", profile=True)
    assert headers["X-Profile-Id"] == "p-kid" and headers["X-Profile-Token"] == "kid-token"


def test_refresh_for_sync_user_writes_parent_tokens_only(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    cfg = _cfg()
    cfg["tofa"]["expires_at"] = 1
    holder = {"cfg": cfg}
    monkeypatch.setattr(tofa, "save_config", lambda c: holder.__setitem__("cfg", c))
    monkeypatch.setattr(tofa, "load_config", lambda: holder["cfg"])
    monkeypatch.setattr(tofa, "_cloud_limiter", lambda: type("L", (), {"wait": staticmethod(lambda key: 0.0)})())

    class Session:
        def post(self, url: str, **kw: Any) -> ResponseStub:
            assert url == "https://api.tofa.tv/servers/srv-1/device-token/refresh"
            return ResponseStub(200, {"access_token": "a2", "refresh_token": "r2", "expires_in": 2592000})

    res = tofa.TofaClient(cfg, instance_id="default~p-kid", session=Session()).refresh_token(cfg)  # type: ignore[arg-type]

    block = holder["cfg"]["tofa"]
    assert res["ok"] is True and res["instance"] == "default" and block["access_token"] == "a2" and block["refresh_token"] == "r2"
    assert "instances" not in block
    assert block["sync_users"]["p-kid"]["profile_token"] == "kid-token" and "access_token" not in block["sync_users"]["p-kid"]
    assert tofa.provider_block(holder["cfg"], "default~p-kid")["access_token"] == "a2"


def test_stale_pin_token_is_flagged_on_the_user_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    cfg = _cfg()
    holder = {"cfg": cfg}
    monkeypatch.setattr(tofa, "save_config", lambda c: holder.__setitem__("cfg", c))
    monkeypatch.setattr(tofa, "load_config", lambda: holder["cfg"])

    tofa.TofaClient(cfg, instance_id="default~p-kid")._flag_profile_pin()

    entry = holder["cfg"]["tofa"]["sync_users"]["p-kid"]
    assert entry["auth_error"] == "profile_pin_required" and entry["profile_token"] == ""
    assert "auth_error" not in holder["cfg"]["tofa"] and holder["cfg"]["tofa"]["profile_token"] == "owner-pin-token"
    assert tofa.status_for_block(tofa.provider_block(holder["cfg"], "default~p-kid"))["profile_pin_required"] is True
    assert tofa.status_for_block(tofa.provider_block(holder["cfg"], "default"))["profile_pin_required"] is False


def test_sync_users_api_entry_verifies_pin_and_never_stores_it(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import providerInstancesAPI as api
    from providers.auth import _auth_TOFA as tofa

    calls: list[Any] = []

    def profiles(self: Any) -> list[dict[str, Any]]:
        return [{"id": "p-kid", "name": "Kid", "has_pin": True, "is_primary": False}, {"id": "p-guest", "name": "Guest", "has_pin": False, "is_primary": False}]

    def verify(self: Any, pid: str, pin: Any) -> tuple[str, int]:
        calls.append((pid, pin))
        if pin != "3113":
            raise tofa.TofaProfileError("invalid_pin")
        return "fresh-kid-token", 4_100_000_000

    monkeypatch.setattr(tofa.TofaClient, "list_profiles", profiles)
    monkeypatch.setattr(tofa.TofaClient, "verify_pin", verify)
    wanted: dict[str, Any] = {"p-kid": {"key": "p-kid", "username": "Kid", "home_pin": "3113"}, "p-guest": {"key": "p-guest", "username": "Guest"}}

    assert api._tofa_prepare_sync_users(_cfg(), "default", wanted) == ""
    kid = api._build_sync_user_entry("tofa", "p-kid", wanted["p-kid"], {}, 100)
    guest = api._build_sync_user_entry("tofa", "p-guest", wanted["p-guest"], {}, 100)
    assert kid["profile_token"] == "fresh-kid-token" and kid["profile_locked"] is True and kid["profile_token_expires_at"] == 4_100_000_000
    assert "home_pin" not in kid and "3113" not in json.dumps(kid)
    assert guest["profile_token"] == "" and guest["profile_id"] == "p-guest" and guest["profile_primary"] is False and guest["added_at"] == 100
    assert calls == [("p-kid", "3113")]
    assert "tofa" in api._SYNC_USER_PROVIDERS
    bad = {"p-kid": {"key": "p-kid", "username": "Kid", "home_pin": "0000"}}
    assert api._tofa_prepare_sync_users(_cfg(), "default", bad) == "invalid_pin"
    assert api._tofa_prepare_sync_users(_cfg(), "default", {"p-x": {"key": "p-x", "username": "X"}}) == "not_found"

    rows = api._sync_user_rows({**_cfg(), "tofa": {**_cfg()["tofa"], "sync_users": {"p-kid": {**_cfg()["tofa"]["sync_users"]["p-kid"], "profile_token": ""}}}}, "tofa", "default")
    assert rows[0]["key"] == "p-kid" and rows[0]["needs_pin"] is True and rows[0]["has_pin"] is False


def test_pairs_and_routes_link_tofa_identities() -> None:
    from cw_platform.identity_links import IDENTITY_PROVIDERS
    from cw_platform.orchestrator._pairs import _expand_sync_user_pair
    from providers.scrobble.routes import sync_user_endpoints_supported, sync_user_matches

    cfg = _cfg()
    cfg["jellyfin"] = {"server": "http://jf", "access_token": "t", "user_id": "admin",
                       "sync_users": {"u-kid": {"user_id": "u-kid", "username": "kid", "user": "kid"}}}
    pairs = _expand_sync_user_pair(cfg, {"id": "P1", "source": "TOFA", "target": "JELLYFIN", "match_users": True, "user_matches": {"p-kid": "u-kid"}})
    assert [(p["source_instance"], p["target_instance"]) for p in pairs] == [("default~p-kid", "default~u-kid")]
    assert "tofa" in IDENTITY_PROVIDERS
    assert sync_user_endpoints_supported("tofa", "default", "jellyfin", "default")
    matches = sync_user_matches(cfg, "tofa", "default", "jellyfin", "default", {"p-kid": "u-kid"})
    assert matches == [{"key": "p-kid", "username": "Kid", "sink_instance": "default~u-kid", "ids": ["p-kid"]}]


def test_redaction_masks_sync_user_tokens() -> None:
    from cw_platform.config_base import redact_config

    red = redact_config(copy.deepcopy(_cfg()))["tofa"]
    assert red["sync_users"]["p-kid"]["profile_token"] != "kid-token"
    assert red["sync_users"]["p-kid"]["username"] == "Kid"
    assert red["profile_token"] != "owner-pin-token"


class Dispatch:
    def __init__(self, needs: bool) -> None:
        self.needs = needs
        self.events: list[Any] = []

    def needs_user_resolution(self) -> bool:
        return self.needs

    def dispatch(self, ev: Any) -> bool:
        self.events.append(ev)
        return True


FILES = [{"media_file_id": "f-alpha", "media_id": "m-alpha", "media_type": "movie", "title": "Alpha", "edition": None, "tmdb_id": 100, "imdb_id": "tt100"}]


@pytest.fixture()
def server(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> list[dict[str, Any]]:
    from providers.scrobble.tofa import watch
    from providers.sync.tofa import _common

    calls: list[dict[str, Any]] = []
    state = {"completed": True}

    def fake(session: Any, method: str, url: str, **kw: Any) -> ResponseStub:
        path = url.split("/api/v1", 1)[1]
        calls.append({"method": method, "path": path, "instance": kw.get("instance_id"), "params": kw.get("params") or {}})
        if path == "/libraries":
            return ResponseStub(200, [{"id": "lib-1", "name": "Movies", "media_type": "movie"}])
        if path == "/media/files":
            return ResponseStub(200, {"items": FILES, "next_after": None})
        if path == "/media/m-alpha":
            return ResponseStub(200, {"files": [{"id": "f-alpha", "duration_ms": 6_000_000}]})
        if path == "/media/f-alpha/progress":
            import time as _t
            return ResponseStub(200, {"completed": state["completed"], "completed_at": _t.strftime("%Y-%m-%dT%H:%M:%SZ", _t.gmtime())})
        if path == "/users/me/watch-state":
            return ResponseStub(200, {"items": [], "next_after": None, "changes_cursor": "c-0"})
        if path == "/users/me/state-changes":
            pid = "p-kid" if kw.get("instance_id") == "default~p-kid" else "p-owner"
            feed = [{"kind": "watched", "media_file_id": "f-alpha", "completed": True, "completed_at": "2026-11-05T10:00:00Z"}] if pid == "p-kid" else []
            return ResponseStub(200, {"changes": feed, "next_cursor": "c-1", "has_more": False, "requires_full_sync": False})
        raise AssertionError(f"unexpected {method} {path}")

    monkeypatch.setattr(_common, "request_with_auth", fake)
    monkeypatch.setattr(_common, "STATE_DIR", tmp_path)
    monkeypatch.setattr(watch, "_cw_update", lambda *a, **k: None)
    monkeypatch.setattr(watch, "_cw_update_payload", lambda *a, **k: None)
    monkeypatch.setattr(watch, "COMPLETION_RECHECK_SECONDS", 0.0)
    _common._LIBRARIES.clear()
    return calls


def _started(profile: str | None, sid: str = "s-1") -> str:
    return json.dumps({"type": "StreamStarted", "session_id": sid, "user_id": "u-1", "profile_id": profile, "profile_name": "Kid",
                       "media_id": "m-alpha", "media_file_id": "f-alpha", "media_title": "Alpha"})


def test_watcher_routes_identity_playback_with_its_own_profile(monkeypatch: pytest.MonkeyPatch, server: list[dict[str, Any]]) -> None:
    from providers.scrobble.tofa import watch

    cfg = _cfg()
    plain = watch.TofaWatchService(dispatcher=Dispatch(False), cfg_provider=lambda: cfg)
    assert [inst for inst, _b in plain._profiles(cfg)] == ["default"]
    plain.handle_message(_started("p-kid"))
    assert plain._sessions == {}

    got = Dispatch(True)
    matched = watch.TofaWatchService(dispatcher=got, cfg_provider=lambda: cfg)
    assert sorted(inst for inst, _b in matched._profiles(cfg)) == ["default", "default~p-guest", "default~p-kid"]
    matched.handle_message(_started("p-kid"))
    matched.handle_message(json.dumps({"type": "StreamProgress", "session_id": "s-1", "progress_pct": 88.0, "position_ticks": 52_800_000_000, "is_paused": False}))
    matched.handle_message(json.dumps({"type": "StreamStopped", "session_id": "s-1"}))

    assert [e.action for e in got.events] == ["start", "stop"]
    stop = got.events[-1]
    assert stop.progress == 100.0 and stop.raw["_cw_session_identity"]["user_id"] == "p-kid"
    check = next(c for c in server if c["path"] == "/media/f-alpha/progress")
    assert check["instance"] == "default~p-kid"

    matched.handle_message(_started(None, sid="s-2"))
    assert matched._sessions["s-2"]["instance"] == "default"


def test_manual_marks_are_read_per_identity(monkeypatch: pytest.MonkeyPatch, server: list[dict[str, Any]]) -> None:
    from providers.scrobble.tofa import watch

    monkeypatch.setattr(watch, "wants_marks", lambda dispatch: True)
    monkeypatch.setattr(watch, "written_by_crosswatch", lambda *a, **k: False)
    cfg = _cfg()
    got = Dispatch(True)
    svc = watch.TofaWatchService(dispatcher=got, cfg_provider=lambda: cfg)
    svc._poll_manual_marks(cfg)
    assert svc._mark_cursors == {"p-owner": "c-0", "p-kid": "c-0", "p-guest": "c-0"}
    svc._marks._next_poll = 0.0
    svc._poll_manual_marks(cfg)
    assert len(got.events) == 1 and got.events[0].raw["_cw_session_identity"]["user_id"] == "p-kid"
    feed_calls = {c["instance"] for c in server if c["path"] == "/users/me/state-changes"}
    assert feed_calls == {"default", "default~p-kid", "default~p-guest"}


def test_sync_runs_with_the_identity_profile(server: list[dict[str, Any]]) -> None:
    from providers.sync._mod_TOFA import TOFAModule

    index = TOFAModule({**_cfg(), "_cw_provider_instance": "default~p-kid"}).build_index("collection")
    assert "tmdb:100" in index
    assert {c["instance"] for c in server if c["path"] == "/media/files"} == {"default~p-kid"}


def _shared_cfg() -> dict[str, Any]:
    cfg = _cfg()
    cfg["tofa"]["is_admin"] = True
    cfg["tofa"]["sync_users"]["p-cen"] = {
        "profile_id": "p-cen", "profile_name": "cenodude", "username": "cenodude", "user": "cenodude", "profile_locked": False,
        "profile_primary": True, "profile_token": "", "auth_error": "", "shared": True, "user_id": "u-cen",
        "sync_key_id": "k-1", "sync_key_token": "tofa_wsk_secret",
    }
    return cfg


class Recorder:
    def __init__(self, routes: dict[tuple[str, str], Any]) -> None:
        self.routes = routes
        self.calls: list[dict[str, Any]] = []

    def request(self, method: str, url: str, **kw: Any) -> ResponseStub:
        path = url.split("/api/v1", 1)[1]
        self.calls.append({"method": method.upper(), "path": path, "headers": dict(kw.get("headers") or {}), "json": kw.get("json")})
        hit = self.routes.get((method.upper(), path))
        if hit is None:
            raise AssertionError(f"unexpected {method} {path}")
        return hit if isinstance(hit, ResponseStub) else ResponseStub(200, hit)

    def post(self, url: str, **kw: Any) -> ResponseStub:
        raise AssertionError("a watch sync key must never trigger a token refresh")


def _store(monkeypatch: pytest.MonkeyPatch, cfg: dict[str, Any]) -> dict[str, Any]:
    from providers.auth import _auth_TOFA as tofa

    holder = {"cfg": cfg}
    monkeypatch.setattr(tofa, "save_config", lambda c: holder.__setitem__("cfg", c))
    monkeypatch.setattr(tofa, "load_config", lambda: holder["cfg"])
    return holder


def test_shared_user_uses_its_sync_key_and_never_the_admin_session() -> None:
    from providers.auth import _auth_TOFA as tofa

    cfg = _shared_cfg()
    block = tofa.provider_block(cfg, "default~p-cen")
    assert tofa.is_shared_user(block) and tofa.is_configured(block) and not tofa.needs_profile_pin(block)
    session = Recorder({("GET", "/users/me"): {"id": "u-cen", "username": "cenodude"}})
    client = tofa.TofaClient(cfg, instance_id="default~p-cen", session=session)  # type: ignore[arg-type]
    assert client.access_token() == "tofa_wsk_secret"
    client.account()
    headers = session.calls[0]["headers"]
    assert headers["Authorization"] == "Bearer tofa_wsk_secret" and headers["X-Profile-Id"] == "p-cen" and "X-Profile-Token" not in headers
    assert not tofa.is_shared_user(tofa.provider_block(cfg, "default")) and not tofa.is_shared_user(tofa.provider_block(cfg, "default~p-kid"))
    cfg["tofa"]["sync_users"]["p-cen"]["sync_key_token"] = ""
    assert not tofa.is_configured(tofa.provider_block(cfg, "default~p-cen"))
    with pytest.raises(tofa.TofaSyncKeyInvalid):
        tofa.TofaClient(cfg, instance_id="default~p-cen").access_token()


@pytest.mark.parametrize("status,code", [(401, "api_key_invalid"), (403, "api_key_access_lost")])
def test_rejected_sync_key_flags_only_that_identity(monkeypatch: pytest.MonkeyPatch, status: int, code: str) -> None:
    from providers.auth import _auth_TOFA as tofa

    holder = _store(monkeypatch, _shared_cfg())
    session = Recorder({("GET", "/users/me"): ResponseStub(status, {"code": code})})
    with pytest.raises(tofa.TofaSyncKeyInvalid):
        tofa.TofaClient(holder["cfg"], instance_id="default~p-cen", session=session).account()  # type: ignore[arg-type]
    users = holder["cfg"]["tofa"]["sync_users"]
    assert users["p-cen"]["auth_error"] == "sync_key_invalid"
    assert holder["cfg"]["tofa"].get("auth_error", "") == "" and users["p-kid"].get("auth_error", "") == ""
    assert tofa.status_for_block(tofa.provider_block(holder["cfg"], "default~p-cen"))["reconnect_required"] is True
    assert tofa.is_configured(tofa.provider_block(holder["cfg"], "default"))


def test_request_with_auth_never_refreshes_for_a_sync_key(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    holder = _store(monkeypatch, _shared_cfg())
    session = Recorder({})
    seen: list[dict[str, Any]] = []

    def call(sess: Any, method: str, url: str, **kw: Any) -> ResponseStub:
        seen.append(kw["headers"])
        return ResponseStub(401, {"code": "api_key_invalid"})

    resp = tofa.request_with_auth(session, "GET", f"{SERVER}/api/v1/users/me/watch-state", cfg=holder["cfg"],  # type: ignore[arg-type]
                                  instance_id="default~p-cen", request_func=call)
    assert resp.status_code == 401 and len(seen) == 1 and seen[0]["Authorization"] == "Bearer tofa_wsk_secret"
    assert holder["cfg"]["tofa"]["sync_users"]["p-cen"]["auth_error"] == "sync_key_invalid"


def test_shared_profiles_lists_other_users_for_admins_only() -> None:
    from providers.auth import _auth_TOFA as tofa

    routes: dict[tuple[str, str], Any] = {
        ("GET", "/system/info"): {"version": "0.13.3", "capabilities": ["sync.user_state", "auth.watch_sync_keys"]},
        ("GET", "/users/list"): [{"id": "u-1", "username": "owner", "email": None}, {"id": "u-cen", "username": "cenodude", "email": None},
                                 {"id": "u-two", "username": "duo", "email": None}],
        ("GET", "/users/u-cen/profiles"): [{"id": "p-cen", "name": "cenodude", "is_primary": True, "is_locked": True}],
        ("GET", "/users/u-two/profiles"): [{"id": "p-d1", "name": "Main", "is_primary": True}, {"id": "p-d2", "name": "Kids", "is_kids": True}],
    }
    cfg = _shared_cfg()
    rows = tofa.TofaClient(cfg, session=Recorder(routes)).shared_profiles()  # type: ignore[arg-type]
    assert [(r["id"], r["user_id"], r["username"], r["has_pin"]) for r in rows] == [
        ("p-cen", "u-cen", "cenodude", False), ("p-d1", "u-two", "duo (Main)", False), ("p-d2", "u-two", "duo (Kids)", False)]
    assert rows[0]["shared"] is True and rows[0]["is_primary"] is True
    cfg["tofa"]["is_admin"] = False
    assert tofa.TofaClient(cfg, session=Recorder({})).shared_profiles() == []  # type: ignore[arg-type]
    cfg["tofa"]["is_admin"] = True
    old = dict(routes)
    old[("GET", "/system/info")] = {"capabilities": ["sync.user_state"]}
    assert tofa.TofaClient(cfg, session=Recorder(old)).shared_profiles() == []  # type: ignore[arg-type]


def test_sync_key_create_and_revoke_calls() -> None:
    from providers.auth import _auth_TOFA as tofa

    routes: dict[tuple[str, str], Any] = {
        ("POST", "/users/u-cen/watch-sync-keys"): ResponseStub(201, {"id": "k-9", "name": "CrossWatch", "secret": "s-9", "expires_at": None}),
        ("DELETE", "/users/u-cen/watch-sync-keys/k-9"): ResponseStub(204, None),
        ("DELETE", "/users/u-cen/watch-sync-keys/k-gone"): ResponseStub(404, {"code": "not_found"}),
        ("DELETE", "/users/u-cen/watch-sync-keys/k-err"): ResponseStub(500, {"code": "boom"}),
    }
    session = Recorder(routes)
    client = tofa.TofaClient(_shared_cfg(), session=session)  # type: ignore[arg-type]
    assert client.create_sync_key("u-cen") == ("s-9", "k-9")
    create = session.calls[0]
    assert create["json"] == {"name": "CrossWatch"} and create["headers"]["Authorization"] == "Bearer a1"
    assert client.revoke_sync_key("u-cen", "k-9") and client.revoke_sync_key("u-cen", "k-gone")
    assert client.revoke_sync_key("u-cen", "k-err") is False
    routes[("POST", "/users/u-cen/watch-sync-keys")] = ResponseStub(201, {"id": "k-10"})
    with pytest.raises(tofa.TofaInvalidResponse):
        client.create_sync_key("u-cen")


def _guests(self: Any) -> list[dict[str, Any]]:
    return [{"id": "p-cen", "name": "cenodude", "username": "cenodude", "user_id": "u-cen", "is_primary": True, "has_pin": False, "is_child": False, "shared": True},
            {"id": "p-d1", "name": "Main", "username": "duo (Main)", "user_id": "u-two", "is_primary": True, "has_pin": False, "is_child": False, "shared": True},
            {"id": "p-d2", "name": "Kids", "username": "duo (Kids)", "user_id": "u-two", "is_primary": False, "has_pin": False, "is_child": True, "shared": True}]


def _household(self: Any) -> list[dict[str, Any]]:
    return [{"id": "p-kid", "name": "Kid", "has_pin": True, "is_primary": False}, {"id": "p-guest", "name": "Guest", "has_pin": False, "is_primary": False}]


def test_prepare_creates_one_key_per_shared_user(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import providerInstancesAPI as api
    from providers.auth import _auth_TOFA as tofa

    made: list[str] = []

    def create(self: Any, uid: str) -> tuple[str, str]:
        made.append(uid)
        return f"secret-{uid}", f"key-{uid}"

    monkeypatch.setattr(tofa.TofaClient, "list_profiles", _household)
    monkeypatch.setattr(tofa.TofaClient, "shared_profiles", _guests)
    monkeypatch.setattr(tofa.TofaClient, "create_sync_key", create)
    wanted: dict[str, Any] = {"p-cen": {"key": "p-cen", "username": "x"}, "p-d1": {"key": "p-d1", "username": "x"}, "p-d2": {"key": "p-d2", "username": "x"},
                              "p-guest": {"key": "p-guest", "username": "Guest", "shared": True, "sync_key_token": "forged", "user_id": "u-evil"}}
    created: list[tuple[str, str]] = []
    assert api._tofa_prepare_sync_users(_shared_cfg(), "default", wanted, created) == ""
    assert made == ["u-two"] and created == [("u-two", "key-u-two")]
    assert wanted["p-cen"]["sync_key_token"] == "tofa_wsk_secret" and wanted["p-cen"]["sync_key_id"] == "k-1"
    assert wanted["p-d1"]["sync_key_token"] == wanted["p-d2"]["sync_key_token"] == "secret-u-two"
    entry = api._build_sync_user_entry("tofa", "p-d2", wanted["p-d2"], {}, 100)
    assert entry["shared"] is True and entry["user_id"] == "u-two" and entry["username"] == "duo (Kids)" and entry["profile_locked"] is False
    assert entry["profile_primary"] is False and entry["sync_key_id"] == "key-u-two"
    household = api._build_sync_user_entry("tofa", "p-guest", wanted["p-guest"], {}, 100)
    assert "shared" not in household and "sync_key_token" not in household and "user_id" not in household

    def broken(self: Any, uid: str) -> tuple[str, str]:
        raise tofa.TofaServerError("forbidden")

    monkeypatch.setattr(tofa.TofaClient, "create_sync_key", broken)
    cfg = _shared_cfg()
    cfg["tofa"]["sync_users"]["p-cen"]["auth_error"] = "sync_key_invalid"
    assert api._tofa_prepare_sync_users(cfg, "default", {"p-cen": {"key": "p-cen", "username": "x"}}) == "tofa_sync_key_failed"
    assert api._tofa_prepare_sync_users(cfg, "default", {"p-nobody": {"key": "p-nobody", "username": "x"}}) == "not_found"


def _api(monkeypatch: pytest.MonkeyPatch, store: dict[str, Any]) -> Any:
    from api import providerInstancesAPI as api

    def fake_load() -> dict[str, Any]:
        return copy.deepcopy(store)

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    def fake_update(mutator: Any) -> tuple[dict[str, Any], Any]:
        cfg = fake_load()
        result = mutator(cfg)
        fake_save(cfg)
        return cfg, result

    monkeypatch.setattr(api, "load_config", fake_load)
    monkeypatch.setattr(api, "save_config", fake_save)
    monkeypatch.setattr(api, "update_config", fake_update)
    monkeypatch.setattr(api, "_admin_required_response", lambda request, cfg: None)
    monkeypatch.setattr(api, "_invalidate_provider_cache", lambda provider: None)
    return api


def _raise_forbidden(self: Any, uid: str) -> tuple[str, str]:
    from providers.auth import _auth_TOFA as tofa

    raise tofa.TofaServerError("forbidden")


def test_sync_users_api_revokes_keys_nobody_uses(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    store = _shared_cfg()
    api = _api(monkeypatch, store)
    revoked: list[tuple[str, str]] = []
    monkeypatch.setattr(tofa.TofaClient, "list_profiles", _household)
    monkeypatch.setattr(tofa.TofaClient, "shared_profiles", _guests)
    monkeypatch.setattr(tofa.TofaClient, "create_sync_key", lambda self, uid: (f"secret-{uid}", f"key-{uid}"))
    monkeypatch.setattr(tofa.TofaClient, "revoke_sync_key", lambda self, uid, kid: revoked.append((uid, kid)) or True)

    three = [{"key": "p-cen", "username": "cenodude"}, {"key": "p-d1", "username": "duo"}, {"key": "p-d2", "username": "duo"}]
    res = api.api_provider_sync_users_update("tofa", "default", {"users": three})
    assert res["ok"] and revoked == []
    rows = {r["key"]: r for r in res["users"]}
    assert rows["p-cen"]["shared"] is True and rows["p-cen"]["key_invalid"] is False and "secret" not in json.dumps(res)
    res = api.api_provider_sync_users_update("tofa", "default", {"users": three[::2]})
    assert res["ok"] and revoked == []
    res = api.api_provider_sync_users_update("tofa", "default", {"users": three[:1]})
    assert res["ok"] and revoked == [("u-two", "key-u-two")]
    assert set(store["tofa"]["sync_users"]) == {"p-cen"}

    monkeypatch.setattr(tofa.TofaClient, "create_sync_key", _raise_forbidden)
    revoked.clear()
    store["tofa"]["sync_users"]["p-cen"]["auth_error"] = "sync_key_invalid"
    assert api.api_provider_sync_users("tofa", "default")["users"][0]["key_invalid"] is True
    res = api.api_provider_sync_users_update("tofa", "default", {"users": three[:1]})
    assert res == {"ok": False, "error": "tofa_sync_key_failed"} and revoked == []
    monkeypatch.setattr(tofa.TofaClient, "create_sync_key", lambda self, uid: ("secret-new", "k-2"))
    res = api.api_provider_sync_users_update("tofa", "default", {"users": three[:1]})
    assert res["ok"] and revoked == [("u-cen", "k-1")]
    entry = store["tofa"]["sync_users"]["p-cen"]
    assert entry["sync_key_token"] == "secret-new" and entry["auth_error"] == ""

    res = api.api_provider_sync_users_update("tofa", "default", {"users": []})
    assert res["ok"] and revoked[-1] == ("u-cen", "k-2") and "sync_users" not in store["tofa"]


def test_failed_save_revokes_the_keys_it_created(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    store = _shared_cfg()
    store["tofa"]["sync_users"].pop("p-cen")
    api = _api(monkeypatch, store)
    revoked: list[tuple[str, str]] = []
    monkeypatch.setattr(tofa.TofaClient, "list_profiles", _household)
    monkeypatch.setattr(tofa.TofaClient, "shared_profiles", _guests)
    monkeypatch.setattr(tofa.TofaClient, "create_sync_key", lambda self, uid: (f"secret-{uid}", f"key-{uid}"))
    monkeypatch.setattr(tofa.TofaClient, "revoke_sync_key", lambda self, uid, kid: revoked.append((uid, kid)) or True)
    monkeypatch.setattr(api, "_max_sync_users_per_connection", lambda cfg=None: 2)
    res = api.api_provider_sync_users_update("tofa", "default", {"users": [{"key": "p-cen", "username": "a"}, {"key": "p-d1", "username": "b"},
                                                                           {"key": "p-kid", "username": "Kid"}]})
    assert res["ok"] is False and res["error"] == "sync_user_limit_reached"
    assert sorted(revoked) == [("u-cen", "key-u-cen"), ("u-two", "key-u-two")]


def test_instance_delete_and_disconnect_revoke_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    store = _shared_cfg()
    store["tofa"]["instances"] = {"TOFA-P01": copy.deepcopy(store["tofa"])}
    api = _api(monkeypatch, store)
    monkeypatch.setattr(api, "usage_conflict_response", lambda cfg, provider, inst: None)
    revoked: list[tuple[str, str, str]] = []
    monkeypatch.setattr(tofa.TofaClient, "revoke_sync_key", lambda self, uid, kid: revoked.append((self.instance_id, uid, kid)) or True)
    assert api.api_provider_instances_delete("tofa", "TOFA-P01")["ok"]
    assert revoked == [("TOFA-P01", "u-cen", "k-1")] and "TOFA-P01" not in store["tofa"]["instances"]

    holder = _store(monkeypatch, store)
    tofa.TofaClient(holder["cfg"]).disconnect(holder["cfg"])
    entry = holder["cfg"]["tofa"]["sync_users"]["p-cen"]
    assert revoked[-1] == ("default", "u-cen", "k-1")
    assert entry["sync_key_token"] == "" and entry["auth_error"] == "sync_key_invalid"
    assert holder["cfg"]["tofa"]["access_token"] == ""


def test_redaction_masks_sync_keys() -> None:
    from cw_platform.config_base import _is_sensitive_path, redact_config

    red = redact_config(copy.deepcopy(_shared_cfg()))["tofa"]["sync_users"]["p-cen"]
    assert red["sync_key_token"] != "tofa_wsk_secret" and red["sync_key_id"] == "k-1"
    assert _is_sensitive_path(("tofa", "sync_users", "p-cen", "sync_key_token"))


def test_health_for_a_shared_user_checks_its_own_account(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync import _mod_TOFA as mod
    from providers.sync.tofa import _common

    paths: list[str] = []

    def fake(session: Any, method: str, url: str, **kw: Any) -> ResponseStub:
        path = url.split("/api/v1", 1)[1]
        paths.append(path)
        if path == "/users/me":
            return ResponseStub(200, {"id": "u-cen"})
        return ResponseStub(403, {"code": "forbidden"})

    monkeypatch.setattr(_common, "request_with_auth", fake)
    res = mod.TOFAModule({**_shared_cfg(), "_cw_provider_instance": "default~p-cen"}).health()
    assert res["ok"] is True and paths == ["/users/me"]


def test_watcher_routes_shared_user_playback_by_user_id(server: list[dict[str, Any]]) -> None:
    from providers.scrobble.tofa import watch

    cfg = _shared_cfg()
    svc = watch.TofaWatchService(dispatcher=Dispatch(True), cfg_provider=lambda: cfg)
    assert svc._instance_for({"user_id": "u-cen"}, cfg) == "default~p-cen"
    assert svc._instance_for({"user_id": "u-cen", "profile_id": "p-cen"}, cfg) == "default~p-cen"
    assert svc._instance_for({"user_id": "u-1"}, cfg) == "default"
    cfg["tofa"]["sync_users"]["p-cen"]["profile_primary"] = False
    assert svc._instance_for({"user_id": "u-cen"}, cfg) == ""


def _two_profile_cfg() -> dict[str, Any]:
    cfg = _shared_cfg()
    users = cfg["tofa"]["sync_users"]
    users.pop("p-cen")
    for pid, primary in (("p-d1", True), ("p-d2", False)):
        users[pid] = {"profile_id": pid, "profile_name": pid, "username": pid, "user": pid, "profile_locked": False, "profile_primary": primary,
                      "profile_token": "", "auth_error": "", "shared": True, "user_id": "u-two", "sync_key_id": "k-old", "sync_key_token": "old-secret"}
    return cfg


def test_rejected_key_flags_every_profile_that_shares_it(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    holder = _store(monkeypatch, _two_profile_cfg())
    session = Recorder({("GET", "/users/me"): ResponseStub(401, {"code": "api_key_invalid"})})
    with pytest.raises(tofa.TofaSyncKeyInvalid):
        tofa.TofaClient(holder["cfg"], instance_id="default~p-d1", session=session).account()  # type: ignore[arg-type]
    users = holder["cfg"]["tofa"]["sync_users"]
    assert users["p-d1"]["auth_error"] == users["p-d2"]["auth_error"] == "sync_key_invalid"
    assert users["p-kid"].get("auth_error", "") == ""


def test_resave_never_reuses_a_key_rejected_on_a_sibling_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    store = _two_profile_cfg()
    store["tofa"]["sync_users"]["p-d1"]["auth_error"] = "sync_key_invalid"
    api = _api(monkeypatch, store)
    revoked: list[tuple[str, str]] = []
    monkeypatch.setattr(tofa.TofaClient, "list_profiles", _household)
    monkeypatch.setattr(tofa.TofaClient, "shared_profiles", _guests)
    monkeypatch.setattr(tofa.TofaClient, "create_sync_key", lambda self, uid: ("new-secret", "k-new"))
    monkeypatch.setattr(tofa.TofaClient, "revoke_sync_key", lambda self, uid, kid: revoked.append((uid, kid)) or True)
    res = api.api_provider_sync_users_update("tofa", "default", {"users": [{"key": "p-d1", "username": "a"}, {"key": "p-d2", "username": "b"}]})
    assert res["ok"]
    users = store["tofa"]["sync_users"]
    assert users["p-d1"]["sync_key_token"] == users["p-d2"]["sync_key_token"] == "new-secret"
    assert revoked == [("u-two", "k-old")]


def test_unexpected_save_error_still_revokes_new_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TOFA as tofa

    store = _shared_cfg()
    store["tofa"]["sync_users"].pop("p-cen")
    api = _api(monkeypatch, store)
    revoked: list[tuple[str, str]] = []
    monkeypatch.setattr(tofa.TofaClient, "list_profiles", _household)
    monkeypatch.setattr(tofa.TofaClient, "shared_profiles", _guests)
    monkeypatch.setattr(tofa.TofaClient, "create_sync_key", lambda self, uid: (f"secret-{uid}", f"key-{uid}"))
    monkeypatch.setattr(tofa.TofaClient, "revoke_sync_key", lambda self, uid, kid: revoked.append((uid, kid)) or True)

    def disk_full(mutator: Any) -> Any:
        raise OSError("disk full")

    monkeypatch.setattr(api, "update_config", disk_full)
    with pytest.raises(OSError):
        api.api_provider_sync_users_update("tofa", "default", {"users": [{"key": "p-cen", "username": "a"}]})
    assert revoked == [("u-cen", "key-u-cen")]
