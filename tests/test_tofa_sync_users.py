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
