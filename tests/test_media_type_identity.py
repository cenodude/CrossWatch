# tests/test_media_type_identity.py
# CrossWatch - Movie/show identity collisions and existing state compatibility
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import json
from importlib import import_module
from types import SimpleNamespace

import pytest

from cw_platform.id_map import canonical_key, migrate_media_index, migrate_media_key, minimal
from cw_platform.orchestrator._planner import diff
from cw_platform.orchestrator._snapshots import canonicalize_index
from cw_platform.orchestrator.facade import Orchestrator
from providers.sync.crosswatch import _watchlist as cw_watchlist
from providers.sync.wetrakr import _watchlist as we_watchlist
from test_orchestrator_oneway_watchlist import FakeOps


MOVIE = {"type": "movie", "title": "Movie", "ids": {"tmdb": "121", "imdb": "tt0167261"}}
SHOW = {"type": "show", "title": "Show", "ids": {"tmdb": "121", "imdb": "tt0056751", "tvdb": "76107"}}


def test_wetrakr_watchlist_preserves_movie_and_show(monkeypatch):
    rows = {"movie": [{**MOVIE, "id": 95}], "show": [{**SHOW, "id": 1390810}]}
    monkeypatch.setattr(we_watchlist, "tracking_rows", lambda *a, **kw: rows)
    index = we_watchlist.build_index.__wrapped__(SimpleNamespace(config={}))
    assert set(index) == {"tmdb:121", "tmdb:121#show"}
    assert index["tmdb:121"]["type"] == "movie"
    assert index["tmdb:121#show"]["type"] == "show"
    assert len(canonicalize_index(index, feature="watchlist")) == 2


def test_wetrakr_ratings_preserve_movie_and_show(monkeypatch):
    from providers.sync.wetrakr import _ratings

    rows = {kind: [{**item, "interactions": {"user": {"rating": {"rating": value}}}}]
            for kind, item, value in [("movie", MOVIE, 8.5), ("show", SHOW, 6.2)]}
    monkeypatch.setattr(_ratings, "tracking_rows", lambda *a, **kw: rows)
    index = _ratings.build_index.__wrapped__(SimpleNamespace(config={}))
    assert {key: row["rating"] for key, row in index.items()} == {"tmdb:121": 8.5, "tmdb:121#show": 6.2}


@pytest.mark.parametrize("schema", [None, 2])
@pytest.mark.parametrize("operation", ["add", "remove"])
def test_publicmetadb_watchlist_refreshes_untyped_legacy_cache(monkeypatch, schema, operation):
    from providers.sync.publicmetadb import _watchlist

    current = {"tmdb:121": "movie-row", "tmdb:121#show": "show-row"}
    old = {"tmdb:121": "show-row"}
    fetched, deleted, saved = [], [], []
    monkeypatch.setattr(_watchlist, "_ensure_watchlist", lambda *a: "watchlist")
    monkeypatch.setattr(_watchlist, "_shadow_load", lambda: {"items": current if schema == 2 else old, "identity_schema": schema})
    monkeypatch.setattr(_watchlist, "_fetch_all_items", lambda *a: (fetched.append(True), dict(current)))
    monkeypatch.setattr(_watchlist, "_shadow_save", lambda items, **kw: saved.append(dict(items)))
    client = SimpleNamespace(delete=lambda path: (deleted.append(path), SimpleNamespace(status_code=200))[1])
    result = getattr(_watchlist, operation)(SimpleNamespace(client=client), [SHOW])
    assert len(fetched) == (0 if schema == 2 else 1)
    assert result == ((1 if operation == "remove" else 0), [])
    assert deleted == (["/api/external/lists/watchlist/items/show-row"] if operation == "remove" else [])
    assert saved == [{"tmdb:121": "movie-row"} if operation == "remove" else current]


@pytest.mark.parametrize("feature", ["watchlist", "collection", "ratings"])
def test_snapshot_preserves_types_and_normalizes_legacy_show_key(feature):
    index = canonicalize_index({"tmdb:121": SHOW, "imdb:tt0167261": MOVIE}, feature=feature)
    assert set(index) == {"tmdb:121", "tmdb:121#show"}
    assert diff({"tmdb:121": MOVIE}, {"tmdb:121#show": SHOW}) == ([minimal(MOVIE)], [minimal(SHOW)])


def test_episode_and_season_keys_are_unchanged():
    from providers.sync.simkl._common import key_of

    for typ, expected in [("episode", "tmdb:121#s01e02"), ("season", "tmdb:121#season:1")]:
        item = {"type": typ, "show_ids": {"tmdb": "121"}, "season": 1, "episode": 2}
        assert canonical_key(item) == expected
        assert key_of(item) == expected
    assert migrate_media_key("tmdb:121@123456", SHOW) == "tmdb:121#show@123456"
    assert migrate_media_key("tmdb:121", MOVIE) == "tmdb:121"


@pytest.mark.parametrize("reverse", [False, True])
def test_already_migrated_cache_entry_wins_over_legacy_duplicate(reverse):
    current = {**SHOW, "rating": 8}
    previous = {**SHOW, "rating": 6}
    rows = [("tmdb:121#show", current), ("tmdb:121", previous)]
    if reverse:
        rows.reverse()
    assert migrate_media_index(dict(rows)) == {"tmdb:121#show": current}


@pytest.mark.parametrize("removed,remaining", [(MOVIE, SHOW), (SHOW, MOVIE)])
def test_new_deletion_markers_do_not_block_other_media_type(removed, remaining):
    from cw_platform.orchestrator._tombstones import filter_with, media_tombstone_tokens

    markers = media_tombstone_tokens(removed)
    assert "tmdb:121" not in markers
    assert filter_with(None, [removed, remaining], extra_block=markers) == [remaining]
    assert filter_with(None, [removed], extra_block={"tmdb:121"}) == []


@pytest.mark.parametrize("provider,feature,loader,path_name", [
    ("trakt", "_watchlist", "_shadow_load", "_shadow_path"),
    ("trakt", "_ratings", "_load_cache_doc", "_cache_path"),
    ("trakt", "_collection", "_shadow_load", "_shadow_path"),
    ("simkl", "_watchlist", "_shadow_load", "_shadow_path"),
])
def test_legacy_provider_caches_keep_show_identity(tmp_path, monkeypatch, provider, feature, loader, path_name):
    module = import_module(f"providers.sync.{provider}.{feature}")
    path = tmp_path / "cache.json"
    doc = {"schema": getattr(module, "_SHADOW_SCHEMA", 1), "items": {"tmdb:121": SHOW}}
    path.write_text(json.dumps(doc), encoding="utf-8")
    monkeypatch.setenv("CW_PAIR_SCOPE", "collision")
    monkeypatch.setattr(module, path_name, lambda: path)
    monkeypatch.setattr(module, "_is_capture_mode", lambda: False)
    if hasattr(module, "_pair_scope"):
        monkeypatch.setattr(module, "_pair_scope", lambda: "collision")
    assert getattr(module, loader)()["items"] == {"tmdb:121#show": SHOW}


def test_nested_rating_shadows_migrate_without_losing_remote_ids(monkeypatch):
    from providers.sync.simkl import _ratings as simkl
    from providers.sync.publicmetadb import _ratings as publicmetadb

    entry = {"id": "remote-show", "item": {**SHOW, "rating": 8}, "label": "Overall"}
    monkeypatch.setattr(simkl, "_load_json", lambda *a: {"items": {"tmdb:121": copy.deepcopy(entry)}})
    assert simkl._rshadow_load()["items"] == {"tmdb:121#show": entry}
    monkeypatch.setattr(publicmetadb, "read_json", lambda *a: {"items": {"tmdb:121#rating:overall": copy.deepcopy(entry)}})
    assert publicmetadb._shadow_load()["items"] == {"tmdb:121#show#rating:overall": entry}


def test_crosswatch_legacy_playlist_order_is_preserved():
    from providers.sync.crosswatch import _playlists

    movie = {**MOVIE, "ids": {"tmdb": "55"}}
    row = _playlists._clean_list_row("test", {
        "items": {"tmdb:55": movie, "tmdb:121": SHOW},
        "order": ["tmdb:121", "tmdb:55"],
    })
    assert row["order"] == ["tmdb:121#show", "tmdb:55"]


@pytest.mark.parametrize("existing", [MOVIE, SHOW])
@pytest.mark.parametrize("removed", [MOVIE, SHOW])
def test_crosswatch_upgrades_legacy_state_without_merging_types(tmp_path, monkeypatch, existing, removed):
    monkeypatch.setenv("CW_CROSSWATCH_PAIR_SCOPED", "1")
    monkeypatch.setenv("CW_PAIR_SCOPE", "collision")
    monkeypatch.setenv("CW_PAIR_SRC", "WETRAKR")
    monkeypatch.setattr(cw_watchlist, "_ensure_tmdb_for_item", lambda *a: False)
    path = tmp_path / "watchlist.collision.json"
    path.write_text(json.dumps({"ts": 1, "items": {"tmdb:121": existing}}), encoding="utf-8")
    adapter = SimpleNamespace(cfg=SimpleNamespace(base_path=str(tmp_path)))
    assert set(cw_watchlist.build_index(adapter)) == {canonical_key(existing)}
    cw_watchlist.add(adapter, [MOVIE, SHOW])
    assert set(cw_watchlist.build_index(adapter)) == {"tmdb:121", "tmdb:121#show"}
    assert cw_watchlist.add(adapter, [MOVIE, SHOW]) == (0, [])
    assert cw_watchlist.remove(adapter, [removed]) == (1, [])
    remaining = SHOW if removed == MOVIE else MOVIE
    assert set(cw_watchlist.build_index(adapter)) == {canonical_key(remaining)}


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("providers", [("WETRAKR", "CROSSWATCH"), ("TRAKT", "MDBLIST"), ("SIMKL", "CROSSWATCH")])
@pytest.mark.parametrize("removed", [MOVIE, SHOW])
@pytest.mark.parametrize("legacy", [False, True])
def test_sync_collision_repeat_and_independent_removal(config_base, monkeypatch, mode, providers, removed, legacy):
    src_name, dst_name = providers
    rows = [SHOW] if legacy else [MOVIE, SHOW]
    src = FakeOps(src_name, {canonical_key(row): copy.deepcopy(row) for row in rows})
    dst = FakeOps(dst_name, {})
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {src_name: src, dst_name: dst})
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda *a: True)
    cfg = {
        "runtime": {"snapshot_ttl_sec": 0, "apply_chunk_size": 0, "apply_chunk_pause_ms": 0},
        "sync": {"dry_run": False, "enable_add": True, "enable_remove": True, "include_observed_deletes": True, "allow_mass_delete": True},
        "pairs": [{"id": "collision", "enabled": True, "source": src_name, "target": dst_name,
                   "mode": mode, "feature": "watchlist", "features": {"watchlist": {"enable": True, "add": True, "remove": True}}}],
    }
    orchestrator = Orchestrator(cfg)
    orchestrator.run()
    if legacy:
        from cw_platform.orchestrator import _pairs_oneway, _pairs_twoway

        load = _pairs_oneway.load_feature_state
        migrated_reads = []

        def legacy_state(*args):
            state = copy.deepcopy(load(*args))
            for provider in state.get("providers", {}).values():
                items = provider.get("watchlist", {}).get("baseline", {}).get("items", {})
                if "tmdb:121#show" in items and "tmdb:121" not in items:
                    items["tmdb:121"] = items.pop("tmdb:121#show")
                    migrated_reads.append(True)
            return state

        monkeypatch.setattr(_pairs_oneway, "load_feature_state", legacy_state)
        monkeypatch.setattr(_pairs_twoway, "load_feature_state", legacy_state)
        orchestrator.run()
        assert migrated_reads
        assert sum(map(len, dst.add_calls)) == 1
        assert not src.add_calls and not src.remove_calls and not dst.remove_calls
        src.index["tmdb:121"] = copy.deepcopy(MOVIE)
        orchestrator.run()
    assert set(dst.index) == {"tmdb:121", "tmdb:121#show"}
    assert sum(map(len, dst.add_calls)) == 2
    orchestrator.run()
    assert sum(map(len, dst.add_calls)) == 2
    assert not src.add_calls and not src.remove_calls and not dst.remove_calls
    src.index.pop(canonical_key(removed))
    orchestrator.run()
    expected = canonical_key(SHOW if removed == MOVIE else MOVIE)
    assert set(src.index) == set(dst.index) == {expected}
    assert [row["type"] for batch in dst.remove_calls for row in batch] == [removed["type"]]
    orchestrator.run()
    assert set(src.index) == set(dst.index) == {expected}
    assert sum(map(len, dst.add_calls)) == 2
