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


def test_pairs_and_routes_match_silo_users_by_name() -> None:
    from cw_platform.orchestrator._pairs import _expand_sync_user_pair
    from providers.scrobble.routes import sync_user_endpoints_supported, sync_user_matches

    cfg = _cfg()
    cfg["jellyfin"] = {"server": "http://jf", "access_token": "t", "user_id": "admin",
                       "sync_users": {"u-kid": {"user_id": "u-kid", "username": "kid", "user": "kid"}}}
    pairs = _expand_sync_user_pair(cfg, {"id": "P1", "source": "SILO", "target": "JELLYFIN", "match_users": True})
    assert [(p["source_instance"], p["target_instance"]) for p in pairs] == [("default~p-kid", "default~u-kid")]

    assert sync_user_endpoints_supported("silo", "default", "jellyfin", "default")
    matches = sync_user_matches(cfg, "silo", "default", "jellyfin", "default")
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


def test_redaction_masks_sync_user_tokens() -> None:
    from cw_platform.config_base import redact_config

    red = redact_config(copy.deepcopy(_cfg()))["silo"]
    assert red["sync_users"]["p-kid"]["profile_token"] != "kid-token"
    assert red["sync_users"]["p-kid"]["username"] == "Kid"
    assert red["profile_token"] != "owner-pin-token"
