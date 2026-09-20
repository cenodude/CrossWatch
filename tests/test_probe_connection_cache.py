# tests/test_probe_connection_cache.py
# CrossWatch - Shared provider probe cache regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from api import probesAPI as probes
from cw_platform import connection_status as connections


@pytest.fixture(autouse=True)
def isolated(config_base, monkeypatch):
    monkeypatch.setattr(probes, "PROBE_CACHE", {p: (0.0, False) for p in probes.PROVIDERS})
    monkeypatch.setattr(probes, "PROBE_DETAIL_CACHE", {})
    monkeypatch.setattr(probes, "_USERINFO_CACHE", {})
    monkeypatch.setattr(probes, "_BUST_SEEN", set())
    monkeypatch.setattr(probes, "STATUS_CACHE", {"ts": 0.0, "data": None})
    monkeypatch.setattr(probes, "STATUS_SCOPE_CACHE", {})


@pytest.mark.parametrize("provider", [p for p in probes.DETAIL_PROBES if p not in {"SIMKL", "CROSSWATCH"}])
def test_verified_providers_check_once_after_restart_then_stop_polling(provider, monkeypatch):
    calls = []
    monkeypatch.setattr(probes, "USERINFO_FNS", {})
    name = provider.lower()
    cfg = {name: {"access_token": "secret"}}

    @probes._persistent_probe(name)
    def verify(cfg, max_age_sec=0):
        calls.append(True)
        return True, ""

    assert verify(cfg) == (True, "")
    state = connections.read(name, cfg)
    connections.update(name, cfg, checked_at=state["checked_at"] - 86400)
    probes._BUST_SEEN.clear()
    probes.PROBE_CACHE[name] = (0.0, False)
    probes.PROBE_DETAIL_CACHE.clear()
    assert verify(cfg) == (True, "")
    assert len(calls) == 1
    monkeypatch.setattr(connections, "_BOOT_ID", "restarted")
    assert verify(cfg) == (True, "")
    assert verify(cfg) == (True, "")
    assert len(calls) == 2
    assert verify(cfg, max_age_sec=0) == (True, "")
    assert len(calls) == 3
    probes.invalidate_provider_caches(name)
    assert verify(cfg) == (True, "")
    assert len(calls) == 4


def test_trakt_refresh_shares_settings_and_does_not_fetch_counts(monkeypatch):
    calls = []
    cfg = {"trakt": {"client_id": "client", "access_token": "token"}}

    def get(url, **kwargs):
        calls.append(url)
        return 200, json.dumps({"user": {"vip": True}, "limits": {"watchlist": {"item_count": 100}}}).encode()

    monkeypatch.setattr(probes, "_http_get", get)
    monkeypatch.setattr(probes, "_trakt_limits_used", lambda *a: pytest.fail("probe fetched lists"))
    assert probes._probe_trakt_detail(cfg, max_age_sec=0) == (True, "")
    info = probes.trakt_user_info(cfg)
    assert info["vip"] is True
    assert info["limits"]["watchlist"] == {"item_count": 100}
    assert calls == ["https://api.trakt.tv/users/settings"]
    assert probes._probe_trakt_detail(cfg) == (True, "")
    assert len(calls) == 1


def test_mdblist_refresh_shares_user_response(monkeypatch):
    from providers.auth import runtime

    calls = []
    cfg = {"mdblist": {"api_key": "secret"}}

    def request(*args, **kwargs):
        calls.append(args)
        return SimpleNamespace(status_code=200, text=json.dumps({"user_id": 12, "username": "Test"}))

    monkeypatch.setattr(runtime, "request_with_auth", request)
    assert probes._probe_mdblist_detail(cfg, max_age_sec=0) == (True, "")
    assert probes.mdblist_user_info(cfg)["user_id"] == 12
    assert len(calls) == 1


def test_anilist_refresh_shares_viewer_response(monkeypatch):
    calls = []
    cfg = {"anilist": {"access_token": "token"}}

    def post(*args, **kwargs):
        calls.append(args)
        return 200, b'{"data":{"Viewer":{"id":123,"name":"Test"}}}'

    monkeypatch.setattr(probes, "_http_post", post)
    assert probes._probe_anilist_detail(cfg, max_age_sec=0) == (True, "")
    assert probes.anilist_user_info(cfg)["user"]["id"] == 123
    assert len(calls) == 1


def test_sync_health_replaces_stale_status_without_probe(monkeypatch):
    from cw_platform.orchestrator import _pairs

    cfg = {"trakt": {"client_id": "client", "access_token": "token"}}
    connections.update("trakt", cfg, connected=True, checked_at=time.time())
    monkeypatch.setattr(probes, "_http_get", lambda *a, **kw: pytest.fail("sync status triggered a probe"))
    monkeypatch.setattr(_pairs, "inject_ctx_into_provider", lambda *a: None)
    health = {"ok": False, "status": "down", "details": "unauthorized"}
    ctx = SimpleNamespace(config=cfg, emit=lambda *a, **kw: None,
                          providers={"TRAKT": SimpleNamespace(health=lambda *a, **kw: health)})
    _pairs._collect_health_for_run(ctx, [{"source": "TRAKT", "target": "TRAKT"}])
    assert probes._probe_trakt_detail(cfg) == (False, "unauthorized")
    health.update(ok=True, status="ok", details=None)
    _pairs._collect_health_for_run(ctx, [{"source": "TRAKT", "target": "TRAKT"}])
    assert probes._probe_trakt_detail(cfg) == (True, "")


def test_changed_credentials_and_profiles_do_not_share_state():
    cfg = {"nuvio": {"access_token": "first", "profile_id": "1"}}
    connections.update("nuvio", cfg, connected=True, checked_at=time.time())
    assert connections.read("nuvio", {"nuvio": {"access_token": "second", "profile_id": "1"}}) == {}
    assert connections.read("nuvio", {"nuvio": {"access_token": "first", "profile_id": "2"}}) == {}
    serialized = "".join(p.read_text() for p in connections._root().glob("*.json"))
    assert "first" not in serialized


def test_concurrent_cold_probes_share_one_verification(monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    calls = []
    cfg = {"publicmetadb": {"api_key": "key"}}

    @probes._persistent_probe("publicmetadb")
    def verify(cfg, max_age_sec=0):
        calls.append(True)
        entered.set()
        assert release.wait(3)
        return True, ""

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(verify, cfg)
        assert entered.wait(3)
        second = pool.submit(verify, cfg)
        release.set()
        assert first.result(timeout=3) == second.result(timeout=3) == (True, "")
    assert len(calls) == 1


def test_expired_token_is_reported_without_refreshing_it(monkeypatch):
    cfg = {"trakt": {"client_id": "client", "access_token": "token", "expires_at": 1}}
    connections.update("trakt", cfg, connected=True, checked_at=time.time())
    monkeypatch.setattr(probes, "_http_get", lambda *a, **kw: pytest.fail("expired token polled"))
    assert probes._probe_trakt_detail(cfg) == (False, "trakt: access token expired")


def test_authenticated_calls_update_only_the_matching_profile(monkeypatch):
    from providers.auth import runtime

    cfg = {"mdblist": {"api_key": "first", "instances": {"second": {"api_key": "second"}}}}
    result = SimpleNamespace(status_code=401)
    monkeypatch.setattr(runtime, "_backend", lambda _: SimpleNamespace(request_with_auth=lambda *a, **kw: result))
    runtime.request_with_auth("mdblist", None, "GET", "https://api.mdblist.com/user", cfg=cfg, instance_id="second")
    view = runtime.build_provider_config_view(cfg, "mdblist", "second")
    assert connections.read("mdblist", view)["connected"] is False
    assert connections.read("mdblist", cfg) == {}
    result.status_code = 200
    runtime.request_with_auth("mdblist", None, "GET", "https://api.mdblist.com/user", cfg=cfg, instance_id="second")
    assert connections.read("mdblist", view)["connected"] is True
    result.status_code = 429
    runtime.request_with_auth("mdblist", None, "GET", "https://api.mdblist.com/user", cfg=cfg, instance_id="second")
    assert connections.read("mdblist", view)["connected"] is True


def test_simkl_concurrent_startup_reads_verify_once_even_with_saved_settings(monkeypatch, tmp_path):
    from providers.sync.simkl import _common

    monkeypatch.setattr(_common, "STATE_DIR", tmp_path)
    monkeypatch.setattr(probes, "SIMKL_AUTH", None)
    monkeypatch.setattr(probes, "_SIMKL_SETTINGS_CACHE", {})
    monkeypatch.setattr(probes, "_SIMKL_VERIFY_AFTER", 0.0)
    monkeypatch.setattr(probes, "last_outcome", lambda _: True)
    cfg = {"simkl": {"client_id": "client", "access_token": "token"}}
    _common.account_settings_store(_common.account_cache_key("token"), {"account": {"type": "free"}})
    calls = []

    def settings(*args, **kwargs):
        calls.append(True)
        return 200, b'{"account":{"type":"pro"}}'

    monkeypatch.setattr(probes, "_simkl_settings_post", settings)
    for boot in ("first-start", "after-restart"):
        monkeypatch.setattr(connections, "_BOOT_ID", boot)
        with ThreadPoolExecutor(max_workers=4) as pool:
            assert list(pool.map(probes._probe_simkl_detail, [cfg] * 4)) == [(True, "")] * 4
        assert probes.simkl_user_info(cfg)["account_type"] == "pro"
    assert len(calls) == 2


def test_failed_startup_verification_is_not_retried_by_status_reads(monkeypatch):
    cfg = {"publicmetadb": {"api_key": "key"}}
    connections.update("publicmetadb", cfg, connected=True, checked_at=time.time())
    monkeypatch.setattr(connections, "_BOOT_ID", "restarted")
    calls = []

    @probes._persistent_probe("publicmetadb")
    def verify(cfg, max_age_sec=0):
        calls.append(True)
        return False, "unreachable"

    assert verify(cfg) == (False, "unreachable")
    assert verify(cfg) == (False, "unreachable")
    assert len(calls) == 1


@pytest.mark.parametrize("provider", [
    "../escape", "..\\escape", "simkl/../../escape", "simkl\\..\\..\\escape",
    "/tmp/escape", "C:\\temp\\escape", "C:escape", "\\\\server\\share\\escape",
    "%2e%2e%2fescape", "simkl:stream", "simkl\x00", "CON", "", "unknown",
])
@pytest.mark.parametrize("operation", ["identity", "lock_for", "read", "update", "invalidate"])
def test_connection_cache_rejects_unsafe_providers_before_file_access(monkeypatch, provider, operation):
    monkeypatch.setattr(connections, "_read", lambda *a: pytest.fail("invalid provider reached a file read"))
    monkeypatch.setattr(connections, "_write", lambda *a: pytest.fail("invalid provider reached a file write"))
    monkeypatch.setattr(connections, "_LOCKS", {})
    call = getattr(connections, operation)
    with pytest.raises(ValueError, match="Unsupported connection status provider"):
        if operation == "invalidate":
            call(provider)
        else:
            call(provider, {})
    assert connections._LOCKS == {}


@pytest.mark.parametrize("provider", [*probes.PROVIDERS, "CW"])
def test_known_provider_names_keep_compatible_cache_paths(provider):
    cfg = {"simkl": {"access_token": "token"}}
    canonical = connections.provider_key(provider)
    if canonical == "tmdb_sync":
        canonical = "tmdb"
    normalized = f" {provider.upper()} "
    key = connections.identity(normalized, cfg)
    assert key.startswith(f"{canonical}.")
    assert key == connections.identity(canonical, cfg)
    connections.update(normalized, cfg, connected=True, checked_at=time.time())
    assert connections.read(canonical, cfg)["connected"] is True
    assert (connections._root() / f"{key}.json").is_file()
    connections.invalidate(normalized)
    assert (connections._root() / f"{canonical}.invalidated.json").is_file()
    assert connections.read(canonical, cfg) == {}


def test_credential_values_cannot_change_the_cache_directory():
    cfg = {"simkl": {"access_token": "../../token", "root_dir": "C:\\escape", "username": "../user"}}
    connections.update("simkl", cfg, connected=True, checked_at=time.time())
    files = list(connections._root().iterdir())
    assert len(files) == 1
    assert files[0].parent == connections._root()
    assert files[0].name == f"{connections.identity('simkl', cfg)}.json"


def test_instance_api_invalidation_rejects_path_traversal(monkeypatch):
    from api.providerInstancesAPI import _invalidate_provider_cache

    monkeypatch.setattr(connections, "_write", lambda *a: pytest.fail("instance API wrote an unsafe cache path"))
    _invalidate_provider_cache("..\\escape")


@pytest.mark.parametrize("group,prefix", [("AUTH", "_auth_"), ("SYNC", "_mod_")])
def test_connection_cache_accepts_new_registry_providers(monkeypatch, group, prefix):
    from cw_platform.modules_registry import MODULES, provider_names

    monkeypatch.setitem(MODULES[group], f"{prefix}NEWPROVIDER", "providers.example")
    assert "NEWPROVIDER" in provider_names()
    cfg = {"newprovider": {"access_token": "token"}}
    connections.update("NEWPROVIDER", cfg, connected=True, checked_at=time.time())
    assert connections.read("newprovider", cfg)["connected"] is True
    connections.invalidate("newprovider")
    assert connections.read("NEWPROVIDER", cfg) == {}


@pytest.mark.parametrize("provider", ["../escape", "..\\escape", "/tmp/escape", "c:escape"])
def test_registry_entries_must_still_be_safe_filename_components(monkeypatch, provider):
    from cw_platform.modules_registry import MODULES

    monkeypatch.setitem(MODULES["AUTH"], f"_auth_{provider.upper()}", "providers.example")
    monkeypatch.setattr(connections, "_write", lambda *a: pytest.fail("unsafe registry entry reached a file write"))
    with pytest.raises(ValueError, match="Unsupported connection status provider"):
        connections.invalidate(provider)
