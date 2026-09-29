# tests/test_media_identity_blocking.py
# CrossWatch - Movie/show identity in blackbox, frozen items, phantoms and dropped shows
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import json
from importlib import import_module

import pytest

from cw_platform.orchestrator import _pairs_blocklist, _phantoms, _unresolved
from cw_platform.orchestrator._pairs_oneway import _matches_dropped_show


MOVIE = {"type": "movie", "title": "The Two Towers", "year": 2002, "ids": {"tmdb": "121", "imdb": "tt0167261"}}
SHOW = {"type": "show", "title": "Doctor Who", "year": 1963, "ids": {"tmdb": "121", "imdb": "tt0056751"}}


def _apply(monkeypatch, blackbox, feature="watchlist"):
    monkeypatch.setattr(_pairs_blocklist, "load_unresolved_keys", lambda *a, **kw: set())
    monkeypatch.setattr(_pairs_blocklist, "load_blackbox_keys", lambda *a, **kw: set(blackbox))
    monkeypatch.setattr(_pairs_blocklist, "keys_for_feature", lambda *a, **kw: {})
    rows = [copy.deepcopy(MOVIE), copy.deepcopy(SHOW)]
    return [row["type"] for row in _pairs_blocklist.apply_blocklist(None, rows, dst="EMBY", feature=feature)]


@pytest.mark.parametrize("feature", ["watchlist", "ratings", "history"])
@pytest.mark.parametrize("blackbox,remaining", [({"tmdb:121"}, ["show"]), ({"tmdb:121#show"}, ["movie"])])
def test_blackbox_entry_blocks_only_its_media_type(monkeypatch, feature, blackbox, remaining):
    assert _apply(monkeypatch, blackbox, feature) == remaining


def test_blackbox_non_tmdb_keys_are_unchanged(monkeypatch):
    assert _apply(monkeypatch, {"imdb:tt0056751"}) == ["movie"]


def test_orchestrator_reads_hint_records_with_typed_keys(tmp_path, monkeypatch):
    monkeypatch.setattr(_unresolved, "STATE_DIR", tmp_path)
    path = _unresolved._blocking_path("EMBY", "watchlist")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"tmdb:121": {"hint": SHOW}, "tmdb:5": {"hint": {"type": "movie", "ids": {"tmdb": "5"}}}}))
    assert set(_unresolved.load_unresolved_keys("EMBY", "watchlist", cross_features=False)) == {"tmdb:121#show", "tmdb:5"}
    assert _unresolved.clear_unresolved("EMBY", "watchlist", ["tmdb:121#show"])["count"] == 1


@pytest.mark.parametrize("module_name", [
    "providers.sync.emby._watchlist",
    "providers.sync.emby._ratings",
    "providers.sync.jellyfin._watchlist",
    "providers.sync.jellyfin._ratings",
])
def test_legacy_frozen_show_is_thawed_by_its_typed_key(tmp_path, monkeypatch, module_name):
    module = import_module(module_name)
    path = tmp_path / "unresolved.json"
    path.write_text(json.dumps({"tmdb:121": {"hint": SHOW}, "tmdb:5": {"hint": {"type": "movie", "ids": {"tmdb": "5"}}}}))
    monkeypatch.setattr(module, "_unresolved_path", lambda: str(path))
    monkeypatch.setattr(module, "_is_capture_mode", lambda: False)
    monkeypatch.setattr(module, "_pair_scope", lambda: "pair", raising=False)
    monkeypatch.setattr(module, "_UNRES_CACHE", {})
    monkeypatch.setattr(module, "_UNRES_DIRTY", set())
    assert set(module._load()) == {"tmdb:121#show", "tmdb:5"}
    module._thaw_if_present(["tmdb:121#show"])
    assert set(module._load()) == {"tmdb:5"}


def _guard(tmp_path, monkeypatch):
    monkeypatch.setattr(_phantoms, "_DIR", str(tmp_path))
    return _phantoms.PhantomGuard("TRAKT", "EMBY", "watchlist", ttl_days=30, enabled=True)


def _filter(guard, items):
    blackboxed = []
    store = type("Store", (), {"blackbox_put": lambda self, pair, key, reason: blackboxed.append(key)})()
    kept, blocked = guard.filter_adds(items, lambda it: it["key"], lambda it: dict(it), lambda *a, **kw: None, store, "TRAKT-EMBY")
    return [row["key"] for row in kept], blackboxed


def test_legacy_success_entry_does_not_phantom_block_the_movie(tmp_path, monkeypatch):
    guard = _guard(tmp_path, monkeypatch)
    guard._lf.write_text(json.dumps({"tmdb:121": guard._now(), "imdb:tt1": guard._now()}))
    kept, blackboxed = _filter(guard, [{"key": "tmdb:121"}, {"key": "imdb:tt1"}])
    assert kept == ["tmdb:121"]
    assert blackboxed == ["imdb:tt1"]


def test_versioned_success_entries_keep_phantom_detection(tmp_path, monkeypatch):
    guard = _guard(tmp_path, monkeypatch)
    guard.record_success(["tmdb:121", "tmdb:121#show"])
    assert json.loads(guard._lf.read_text())["version"] == 2
    kept, blackboxed = _filter(guard, [{"key": "tmdb:121"}, {"key": "tmdb:121#show"}, {"key": "tmdb:7"}])
    assert kept == ["tmdb:7"]
    assert sorted(blackboxed) == ["tmdb:121", "tmdb:121#show"]


def test_legacy_success_file_is_upgraded_on_next_success(tmp_path, monkeypatch):
    guard = _guard(tmp_path, monkeypatch)
    guard._lf.write_text(json.dumps({"tmdb:121": 1, "imdb:tt1": 1}))
    guard.record_success(["tmdb:7"])
    assert set(json.loads(guard._lf.read_text())["items"]) == {"imdb:tt1", "tmdb:7"}


@pytest.mark.parametrize("item,dropped", [
    (MOVIE, False),
    (SHOW, True),
    ({"type": "episode", "show_ids": {"tmdb": "121"}, "season": 1, "episode": 2}, True),
])
def test_dropped_show_does_not_drop_movie_with_same_tmdb(item, dropped):
    assert _matches_dropped_show(item, {"tmdb:121", "tmdb:121#show"}) is dropped
