# CrossWatch test scripts
from __future__ import annotations

import sys
import types

import pytest

from cw_platform import token_refresh


@pytest.fixture
def providers(monkeypatch):
    calls: list[str] = []

    def module(name: str, result):
        mod = types.ModuleType(name)

        def refresh_all():
            calls.append(name)
            if isinstance(result, Exception):
                raise result
            return result

        mod.refresh_all = refresh_all
        monkeypatch.setitem(sys.modules, name, mod)
        return name

    monkeypatch.setattr(token_refresh, "PROVIDERS", {
        "ONE": module("cw_test_refresh_one", {"default": "ok"}),
        "BROKEN": module("cw_test_refresh_broken", RuntimeError("boom")),
        "IDLE": module("cw_test_refresh_idle", {}),
        "TWO": module("cw_test_refresh_two", {"second": "refresh_failed:400"}),
    })
    return calls


def test_refresh_all_runs_every_provider_and_survives_a_failure(providers):
    assert token_refresh.refresh_all() == {"ONE": {"default": "ok"}, "TWO": {"second": "refresh_failed:400"}}
    assert providers == ["cw_test_refresh_one", "cw_test_refresh_broken", "cw_test_refresh_idle", "cw_test_refresh_two"]


def test_registered_providers_expose_refresh_all():
    import importlib

    for module_path in token_refresh.PROVIDERS.values():
        assert callable(importlib.import_module(module_path).refresh_all)


def test_worker_starts_once_and_stops(providers, monkeypatch):
    monkeypatch.setattr(token_refresh, "_worker", None)
    monkeypatch.setattr(token_refresh, "INTERVAL_S", 30)
    try:
        assert token_refresh.start_worker() is True
        assert token_refresh.start_worker() is False
    finally:
        token_refresh.stop_worker()
        worker = token_refresh._worker
        if worker is not None:
            worker.join(timeout=5)
    assert worker is not None and not worker.is_alive()
    assert providers[:1] == ["cw_test_refresh_one"]


def test_simkl_worker_skips_profiles_that_need_a_reconnect(monkeypatch):
    import time

    from providers.auth import _auth_SIMKL as simkl

    soon = int(time.time()) + 3600
    token = {"access_token": "simkl_at_a", "refresh_token": "r", "token_expires_at": soon, "auth_version": 2}
    cfg = {"simkl": {**token, "instances": {
        "dead": {**token, "auth_error": "reconnect_required"},
        "later": {**token, "token_expires_at": soon + 5 * 86400},
    }}}
    refreshed: list[str] = []

    def refresh(_cfg=None, *, instance_id=None, margin_s=0):
        refreshed.append(str(instance_id))
        return {"ok": True, "status": "ok"}

    monkeypatch.setattr(simkl, "load_config", lambda: cfg)
    monkeypatch.setattr(simkl.PROVIDER, "refresh", refresh)

    assert simkl.refresh_all() == {"default": "ok"}
    assert refreshed == ["default"]
