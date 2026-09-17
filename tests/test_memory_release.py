from __future__ import annotations

import types

from cw_platform import memory_release
from cw_platform.orchestrator import _pairs_utils


def test_clear_caches_runs_every_registered_clear(monkeypatch):
    monkeypatch.setattr(memory_release, "_CACHES", {})
    cleared: list[str] = []

    def broken() -> None:
        raise RuntimeError("boom")

    memory_release.register_cache("a", lambda: cleared.append("a"))
    memory_release.register_cache("broken", broken)
    memory_release.register_cache("b", lambda: cleared.append("b"))
    memory_release.clear_caches()
    assert cleared == ["a", "b"]


def test_registered_app_caches_are_cleared():
    from api import insightAPI, wallAPI
    from services import analyzer

    analyzer._STATE_CACHE[("k",)] = ("state",)
    analyzer._ANALYSIS_CACHE[("k",)] = {"problems": []}
    wallAPI._WALL_CACHE[("k",)] = {"items": []}
    insightAPI._DERIVED_CACHE[("k",)] = {"rows": []}
    memory_release.clear_caches()
    assert not analyzer._STATE_CACHE
    assert not analyzer._ANALYSIS_CACHE
    assert not wallAPI._WALL_CACHE
    assert not insightAPI._DERIVED_CACHE


def test_release_memory_is_safe_everywhere():
    memory_release.release_memory()


def test_release_ctx_restores_module_and_ops_globals(monkeypatch):
    monkeypatch.setattr(_pairs_utils, "_CTX_TARGETS", {})
    mod = types.ModuleType("fake_mod_TEST")
    setattr(mod, "ctx", "original")
    common = types.ModuleType("fake_mod_common")

    class Ops:
        pass

    Ops.__module__ = "fake_mod_TEST"
    ops = Ops()
    modules = {"fake_mod_TEST": mod, "fake_mod_common": common}
    monkeypatch.setattr(_pairs_utils.importlib, "import_module", lambda name: modules[name])

    run_ctx = types.SimpleNamespace(snap_cache={"big": 1})
    _pairs_utils.inject_ctx_into_provider(ops, run_ctx)
    assert getattr(mod, "ctx") is run_ctx and getattr(common, "ctx") is run_ctx and getattr(ops, "ctx") is run_ctx

    _pairs_utils.release_ctx_from_providers(run_ctx)
    assert getattr(mod, "ctx") == "original"
    assert not hasattr(common, "ctx")
    assert not hasattr(ops, "ctx")
    assert _pairs_utils._CTX_TARGETS == {}


def test_release_ctx_leaves_newer_ctx_in_place(monkeypatch):
    monkeypatch.setattr(_pairs_utils, "_CTX_TARGETS", {})
    mod = types.ModuleType("fake_mod_OTHER")
    setattr(mod, "ctx", None)

    class Ops:
        pass

    Ops.__module__ = "fake_mod_OTHER"
    modules = {"fake_mod_OTHER": mod}
    monkeypatch.setattr(_pairs_utils.importlib, "import_module", lambda name: modules[name])

    first = types.SimpleNamespace()
    second = types.SimpleNamespace()
    _pairs_utils.inject_ctx_into_provider(Ops(), first)
    _pairs_utils.inject_ctx_into_provider(Ops(), second)
    _pairs_utils.release_ctx_from_providers(first)
    assert getattr(mod, "ctx") is second
    _pairs_utils.release_ctx_from_providers(second)
    assert getattr(mod, "ctx") is None
