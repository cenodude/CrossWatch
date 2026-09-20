# tests/test_provider_endpoint_regressions.py
# CrossWatch - Provider endpoint identity regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import pytest

from cw_platform.provider_instances import build_config_view


SYNC_PROVIDERS = (
    "ANILIST", "CROSSWATCH", "EMBY", "FLICKLIST", "FLOPPY", "JELLYFIN", "KODI", "MDBLIST",
    "NUVIO", "PLEX", "PUBLICMETADB", "PUNCHPLAY", "SCROB", "SIMKL", "STREMIO", "TAUTULLI",
    "TMDB", "TRACEARR", "TRAKT",
)


@pytest.mark.parametrize("provider", [
    "anilist", "emby", "jellyfin", "mdblist", "plex", "publicmetadb", "punchplay", "simkl", "tautulli", "tmdb", "trakt",
])
def test_provider_state_files_stay_with_the_endpoint(monkeypatch, tmp_path, provider):
    from cw_platform.orchestrator._scope import provider_call

    common = import_module(f"providers.sync.{provider}._common")
    state_attr = "_STATE_DIR" if provider == "emby" else "STATE_DIR"
    monkeypatch.setattr(common, state_attr, tmp_path)
    for name in ("CW_PAIR_SCOPE", "CW_PAIR_KEY", "CW_SYNC_PAIR", "CW_PAIR"):
        monkeypatch.setenv(name, "cw2_" + "a" * 64)
    paths = []
    for instance in ("default", "P01", "P02"):
        cfg = {"_cw_provider_instance": instance, "_cw_pair_scope": "cw2_" + "a" * 64}
        path = Path(provider_call(lambda cfg: common.state_file(f"{provider}_history.shadow.json"), cfg))
        path.write_text(instance, encoding="utf-8")
        paths.append(path)
    assert len(set(paths)) == 3
    assert [path.read_text(encoding="utf-8") for path in paths] == ["default", "P01", "P02"]


@pytest.mark.parametrize("provider", SYNC_PROVIDERS)
@pytest.mark.parametrize("source,target", [("default", "P01"), ("P01", "default"), ("P01", "P02")])
def test_all_provider_adapters_keep_endpoint_credentials(monkeypatch, tmp_path, provider, source, target):
    import requests

    from cw_platform.orchestrator._pairs_utils import pair_endpoint_config

    module = import_module(f"providers.sync._mod_{provider}")
    client = getattr(module, f"{provider}Client", None)
    if client is not None and hasattr(client, "connect"):
        monkeypatch.setattr(client, "connect", lambda self: self)
    if provider == "SIMKL":
        monkeypatch.setattr(module, "_fresh_access_token", lambda block: block["access_token"])
    if provider == "PLEX":
        monkeypatch.setattr(module, "configure_plex_context", lambda **kw: None)
    monkeypatch.setattr(requests.Session, "request", lambda *args, **kw: pytest.fail("Unexpected network request"))
    for side, value in {"SRC": provider, "DST": provider, "SRC_INSTANCE": source, "DST_INSTANCE": target}.items():
        monkeypatch.setenv(f"CW_PAIR_{side}", value)

    def block(instance):
        return {
            "server": f"http://{instance}", "server_url": f"http://{instance}", "baseurl": f"http://{instance}",
            "base_url": f"http://{instance}", "token": f"token-{instance}", "account_token": f"token-{instance}",
            "access_token": f"token-{instance}", "api_token": f"token-{instance}", "api_key": f"token-{instance}",
            "auth_key": f"token-{instance}", "client_id": f"client-{instance}", "user_id": instance,
            "username": instance, "password": "password", "session_id": f"session-{instance}",
            "account_id": "1" if instance == "default" else "2", "root_dir": str(tmp_path / instance),
            "connection_verified": True,
        }

    key = "tmdb_sync" if provider == "TMDB" else provider.lower()
    cfg = {key: {**block("default"), "instances": {i: block(i) for i in ("P01", "P02")}}}
    blocks = {i: build_config_view(cfg, {provider: i})[key] for i in (source, target)}
    pair_cfg = {**cfg, "_cw_pair_instance_blocks": {provider: blocks}}
    for instance in (source, target):
        view = pair_endpoint_config(pair_cfg, provider, instance)
        adapter = getattr(module, f"{provider}Module")(view)
        if provider in {"EMBY", "JELLYFIN", "KODI"}:
            assert adapter.cfg.server.rstrip("/") == f"http://{instance}"
            assert adapter.instance_id == instance
            if provider == "KODI":
                assert adapter.cfg.username == instance
            else:
                assert adapter.cfg.access_token == f"token-{instance}"
        elif provider == "PLEX":
            assert adapter.cfg.baseurl == f"http://{instance}"
            assert adapter.cfg.token == f"token-{instance}"
        elif provider in {"ANILIST", "SIMKL", "TRAKT"}:
            assert adapter.cfg.access_token == f"token-{instance}"
        elif provider in {"MDBLIST", "PUBLICMETADB", "TAUTULLI", "TRACEARR"}:
            assert adapter.cfg.api_key == f"token-{instance}"
        elif provider == "TMDB":
            assert adapter.cfg.session_id == f"session-{instance}"
        elif provider == "CROSSWATCH":
            assert adapter.cfg.root_dir == str(tmp_path / instance)
        elif provider == "FLICKLIST":
            assert adapter.instance_id == instance
            assert adapter._section()["access_token"] == f"token-{instance}"
        elif provider == "FLOPPY":
            assert adapter.instance_id == instance
            assert adapter.client.api_token == f"token-{instance}"
        elif provider == "NUVIO":
            assert adapter.instance_id == instance
            assert adapter.client.block["access_token"] == f"token-{instance}"
        elif provider == "STREMIO":
            assert adapter.instance_id == instance
            assert adapter.client.auth_key() == f"token-{instance}"
        elif provider == "PUNCHPLAY":
            from providers.auth._auth_PUNCHPLAY import provider_block

            assert adapter.instance_id == instance
            assert provider_block(adapter.config, adapter.instance_id)["access_token"] == f"token-{instance}"
        elif provider == "SCROB":
            from providers.sync.scrob._common import cfg_section

            assert adapter.instance_id == instance
            assert cfg_section(adapter)["username"] == instance


@pytest.mark.parametrize("provider", ["EMBY", "JELLYFIN"])
@pytest.mark.parametrize("instance", ["default", "P01"])
@pytest.mark.parametrize("status", [204, 500])
def test_media_server_progress_labels_follow_the_adapter(monkeypatch, provider, instance, status):
    module = import_module(f"providers.sync._mod_{provider}")
    progress = import_module(f"providers.sync.{provider.lower()}._progress")
    monkeypatch.setenv("CW_PAIR_SRC", provider)
    monkeypatch.setenv("CW_PAIR_DST", provider)
    monkeypatch.setenv("CW_PAIR_SRC_INSTANCE", "default")
    monkeypatch.setenv("CW_PAIR_DST_INSTANCE", "P01")
    block = {"server": "http://test", "access_token": "test-token", "user_id": "test-user"}
    adapter = getattr(module, f"{provider}Module")({provider.lower(): block, "_cw_provider_instance": instance})
    calls = []
    adapter.client = SimpleNamespace(post=lambda path, **kw: calls.append(path) or SimpleNamespace(status_code=status))
    monkeypatch.setattr(progress, "_active_item_ids", lambda *args: set())
    monkeypatch.setattr(progress, "resolve_item_ids", lambda *args, **kw: ["11"])
    monkeypatch.setattr(progress, "_target_state", lambda *args: {"duration_ms": 600000, "progress_ms": 0})
    item = {"type": "movie", "ids": {"tmdb": "1"}, "progress_ms": 60000, "progress_at": "2026-09-20T12:00:00Z"}

    count, unresolved = progress.add(adapter, [item])

    assert count == int(status == 204)
    assert bool(unresolved) == (status == 500)
    assert len(calls) == 1
    assert adapter._progress_write_results[0]["provider_instance"] == instance


def test_plex_cached_adapters_keep_distinct_instance_context(monkeypatch):
    from providers.sync import _mod_PLEX as plex

    monkeypatch.setattr(plex, "_ADAPTER_CACHE", {})
    monkeypatch.setattr(plex, "_ADAPTER_CACHE_ENABLED", True)
    monkeypatch.setattr(plex, "configure_plex_context", lambda **kw: None)
    monkeypatch.setattr(plex, "PLEXModule", lambda cfg: SimpleNamespace(
        config=cfg, cfg=SimpleNamespace(baseurl="http://plex", pms_token="token", token="token")))
    cfg = {"plex": {"baseurl": "http://plex", "token": "token"}, "_cw_pair_scope": "same-pair"}
    default = plex.OPS._adapter({**cfg, "_cw_provider_instance": "default"})
    named = plex.OPS._adapter({**cfg, "_cw_provider_instance": "P01"})

    assert default is not named
    assert default.config["_cw_provider_instance"] == "default"
    assert named.config["_cw_provider_instance"] == "P01"
    assert plex.OPS._adapter({**cfg, "_cw_provider_instance": "default"}) is default
    assert plex.OPS._adapter({**cfg, "_cw_provider_instance": "P01"}) is named


@pytest.mark.parametrize("source,target", [("default", "P01"), ("P01", "default"), ("P01", "P02")])
def test_scrob_refresh_uses_each_endpoint_account(monkeypatch, source, target):
    from providers.auth import _auth_SCROB as auth
    from providers.sync import _mod_SCROB as scrob
    from providers.sync.scrob import _common as common

    def block(instance):
        return {"server_url": f"http://{instance}", "api_key": f"key-{instance}", "username": instance,
                "password": "password", "access_token": f"old-{instance}", "expires_at": 1}

    stored = {"scrob": {**block("default"), "instances": {i: block(i) for i in ("P01", "P02")}}}
    saves = []
    logins = []
    monkeypatch.setattr(auth, "load_config", lambda: deepcopy(stored))
    monkeypatch.setattr(auth, "save_config", lambda cfg: saves.append(deepcopy(cfg)))
    monkeypatch.setattr(auth, "login", lambda server, api_key, username, password, **kw:
                        logins.append((server, api_key, username)) or {"access_token": f"new-{username}", "expires_at": 4102444800})
    for instance in (source, target):
        cfg = build_config_view(stored, {"SCROB": instance})
        cfg["_cw_provider_instance"] = instance
        adapter = scrob.OPS._adapter(cfg)
        assert adapter.instance_id == instance
        section = common.cfg_section(adapter)
        assert section["server_url"] == f"http://{instance}"
        token = auth.access_token_for(adapter.config, instance_id=common.instance_id(adapter))
        assert token == f"new-{instance}"
        saved = saves[-1]["scrob"]
        for other in ("default", "P01", "P02"):
            row = saved if other == "default" else saved["instances"][other]
            assert row["access_token"] == (f"new-{other}" if other == instance else f"old-{other}")
    assert logins == [(f"http://{i}", f"key-{i}", i) for i in (source, target)]


def test_scrob_explicit_argument_takes_precedence_over_endpoint_marker():
    from providers.sync import _mod_SCROB as scrob

    block = {"server_url": "http://test", "api_key": "key", "username": "user", "password": "password"}
    adapter = scrob.SCROBModule({"scrob": block, "_cw_provider_instance": "P01"}, instance_id="default")
    assert adapter.instance_id == "default"
