# tests/test_webhook_config_view_persist.py
# CrossWatch - Webhook config views never reach config.json
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import importlib
from typing import Any

import pytest

from cw_platform.config_base import cleanup_obsolete_config_keys
from providers.scrobble.routes import build_route_cfg_by_id
from providers.webhooks.config import sink_configured


def _plex_cfg() -> dict[str, Any]:
    plex = {
        "server_url": "http://pms:32400",
        "pms_token": "tok",
        "account_token": "tok",
        "sync_users": {"1": {"username": "owner", "account_id": 1}, "322620": {"username": "kid", "account_id": 322620}},
    }
    return {
        "plex": plex,
        "silo": {
            "server_url": "http://silo",
            "access_token": "a",
            "profile_id": "p1",
            "sync_users": {"p1": {"profile_id": "p1", "username": "owner"}, "p2": {"profile_id": "p2", "username": "kid"}},
        },
        "scrobble": {"watch": {"routes": [{
            "id": "R2", "enabled": True, "provider": "silo", "provider_instance": "default",
            "sink": "plex", "sink_instance": "default", "match_users": True,
            "user_matches": {"p1": "1", "p2": "322620"}, "filters": {},
        }]}},
    }


def test_cleanup_drops_saved_scrobble_views() -> None:
    cfg = _plex_cfg()
    cfg["_cw_scrobble_provider_configs"] = {"plex": {"sync_users": {"2028816": {"account_id": 2028816}}}}
    cfg["_cw_scrobble_sink"] = {"provider": "plex", "instance": "default", "config": {}}

    changed = cleanup_obsolete_config_keys(cfg)

    assert "_cw_scrobble_provider_configs" not in cfg
    assert "_cw_scrobble_sink" not in cfg
    assert {"_cw_scrobble_provider_configs", "_cw_scrobble_sink"} <= set(changed)


def test_linked_plex_users_configured_after_cleanup() -> None:
    cfg = _plex_cfg()
    cfg["_cw_scrobble_provider_configs"] = {"plex": {**cfg["plex"], "sync_users": {"2028816": {"account_id": 2028816}}}}
    stale = build_route_cfg_by_id(cfg, "R2~p1")
    assert stale is not None
    assert sink_configured(stale, "plex", "default~1") is False

    cleanup_obsolete_config_keys(cfg)

    for rid, inst in (("R2~p1", "default~1"), ("R2~p2", "default~322620")):
        built = build_route_cfg_by_id(cfg, rid)
        assert built is not None
        assert sink_configured(built, "plex", inst) is True


def test_user_probe_filter_is_not_logged(monkeypatch: pytest.MonkeyPatch) -> None:
    import providers.scrobble.scrobble as scrobble_mod
    from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent

    messages: list[str] = []
    monkeypatch.setattr(scrobble_mod, "_log", lambda msg, lvl="INFO": messages.append(str(msg)))
    cfg = {"scrobble": {"watch": {"route_id": "R2~p1", "route_provider": "silo", "filters": {"username_whitelist": ["id:p1"]}}}}
    disp = Dispatcher([], cfg_provider=lambda: cfg)

    def event(raw: dict[str, Any]) -> ScrobbleEvent:
        return ScrobbleEvent(action="stop", media_type="movie", ids={}, title=None, year=None, season=None, number=None,
                             progress=100.0, account="kid", server_uuid=None, session_key=None, raw=raw)

    identity = {"_cw_session_identity": {"user_id": "p2", "name": "kid"}, "_cw_manual_mark": True}
    assert disp.accepts_user(event({**identity, "_cw_user_probe": True})) is False
    assert messages == []
    assert disp.accepts_user(event(identity)) is False
    assert [m for m in messages if "reason=username_whitelist" in m]


@pytest.mark.parametrize("provider", ["plex", "emby", "jellyfin"])
def test_webhook_never_saves_passed_view(monkeypatch: pytest.MonkeyPatch, provider: str) -> None:
    mod = importlib.import_module(f"providers.webhooks.{provider}")
    saved: list[dict[str, Any]] = []
    monkeypatch.setattr(mod, "save_config", lambda cfg: saved.append(cfg))
    view = {"scrobble": {"sources": {"webhook": False}}, "_cw_scrobble_provider_configs": {provider: {}}}

    mod.process_webhook({}, {}, cfg=view)

    assert saved == []


@pytest.mark.parametrize("provider", ["plex", "emby", "jellyfin"])
def test_webhook_defaults_still_saved_for_loaded_config(monkeypatch: pytest.MonkeyPatch, provider: str) -> None:
    mod = importlib.import_module(f"providers.webhooks.{provider}")
    saved: list[dict[str, Any]] = []
    monkeypatch.setattr(mod, "save_config", lambda cfg: saved.append(cfg))
    monkeypatch.setattr(mod, "_load_config", lambda: {"scrobble": {"sources": {"webhook": False}}})

    mod.process_webhook({}, {})

    assert len(saved) == 1
