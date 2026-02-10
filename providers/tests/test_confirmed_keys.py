from __future__ import annotations

import importlib

import pytest


MODULES_WITH_HELPER = (
    "sync._mod_ANILIST",
    "sync._mod_CROSSWATCH",
    "sync._mod_EMBY",
    "sync._mod_JELLYFIN",
    "sync._mod_MDBLIST",
    "sync._mod_SIMKL",
    "sync._mod_TMDB",
    "sync._mod_TRAKT",
)


@pytest.mark.parametrize("module_path", MODULES_WITH_HELPER)
def test_confirmed_keys_filters_unresolved_and_dupes(module_path: str):
    mod = importlib.import_module(module_path)
    fn = getattr(mod, "_confirmed_keys")

    items = [
        {"id": "a"},
        {"id": "b"},
        {"id": "b"},
        {"id": "c"},
    ]
    key_of = lambda it: it.get("id")

    # Keep unresolved shapes compatible across all modules (TMDB's helper is stricter).
    unresolved = [{"key": "b"}, {"key": "c"}, "missing"]
    out = fn(key_of, items, unresolved)
    assert out == ["a"]


@pytest.mark.parametrize("module_path", MODULES_WITH_HELPER)
def test_confirmed_keys_accepts_string_unresolved_keys(module_path: str):
    mod = importlib.import_module(module_path)
    fn = getattr(mod, "_confirmed_keys")

    items = [{"id": "x"}, {"id": "y"}, {"id": "z"}]
    key_of = lambda it: it.get("id")

    out = fn(key_of, items, ["y"])
    assert out == ["x", "z"]
