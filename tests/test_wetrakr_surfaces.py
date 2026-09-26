# tests/test_wetrakr_surfaces.py
# CrossWatch - WeTrakr analyzer, capture, editor and cleanup integration tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import sqlite3

import pytest

from test_wetrakr_sync import EPISODE, LATER, MOVIE, SHOW, WHEN, env, item
from test_wetrakr_ratings_progress import SEASON, features


@pytest.fixture
def surfaces(features, config_base, monkeypatch):
    from api import editorAPI
    from services import snapshots
    from cw_platform.orchestrator import _unresolved, _blackbox

    monkeypatch.setattr(snapshots, "CONFIG", config_base)
    monkeypatch.setattr(editorAPI, "_STATE_BASE", config_base)
    monkeypatch.setattr(editorAPI, "load_config", lambda: copy.deepcopy(features.cfg))
    monkeypatch.setattr(editorAPI, "_emit_editor_operation", lambda *a, **kw: None)
    monkeypatch.setattr(_unresolved, "STATE_DIR", config_base / ".cw_state")
    monkeypatch.setattr(_blackbox, "STATE_DIR", config_base / ".cw_state")
    return features


def test_profile_only_discovery_and_cleanup_capabilities(surfaces):
    from api import editorAPI
    from services import snapshots

    provider = next(row for row in snapshots.snapshot_manifest(surfaces.cfg) if row["id"] == "WETRAKR")
    assert provider["configured"] and provider["label"] == "WeTrakr"
    assert {row["id"] for row in provider["instances"] if row["configured"]} == {"P01"}
    expected = {"watchlist", "history", "ratings", "progress"}
    assert {key for key, value in provider["features"].items() if value} == expected
    assert {key for key, value in provider["cleanup_features"].items() if value} == expected
    for feature in (*expected, "collection"):
        targets = [row for row in editorAPI._editor_send_targets(surfaces.cfg, feature) if row["provider"] == "WETRAKR"]
        assert [row["instance"] for row in targets] == (["P01"] if feature in expected else [])


@pytest.mark.parametrize("feature,media", [
    ("watchlist", MOVIE), ("watchlist", SHOW), ("history", MOVIE), ("history", EPISODE),
    ("ratings", MOVIE), ("ratings", SHOW), ("ratings", SEASON), ("ratings", EPISODE),
    ("progress", MOVIE), ("progress", EPISODE),
])
def test_editor_send_capture_and_cleanup(surfaces, feature, media):
    from api import editorAPI
    from services import snapshots

    source = {**item(media), "watched_at": WHEN, "rating": 7, "progress_percent": 40, "progress_at": WHEN}
    result = editorAPI.api_editor_send({"kind": feature, "items": [source],
        "providers": [{"provider": "WETRAKR", "instance": "P01"}]}, request=None)
    assert result["ok"], result
    assert result["confirmed"] == 1
    saved = editorAPI._load_state_items(feature, "WETRAKR", "P01")
    assert len(saved) == 1
    assert next(iter(saved.values()))["ids"]["wetrakr"] == str(media["id"])
    if feature == "ratings":
        result = editorAPI.api_editor_send({"kind": feature, "items": [{**source, "rating": 9}],
            "providers": [{"provider": "WETRAKR", "instance": "P01"}]}, request=None)
        assert result["ok"]
        assert next(iter(surfaces.adapter.build_index(feature, force_refresh=True).values()))["rating"] == 9
    capture = snapshots.create_snapshot("WETRAKR", feature, instance_id="P01", cfg=surfaces.cfg)
    assert capture["ok"]
    loaded = snapshots.read_snapshot(capture["path"])
    assert loaded["instance"] == "P01" and loaded["stats"]["count"] == 1
    key, captured = next(iter(loaded["items"].items()))
    assert key.startswith("wetrakr:")
    assert captured["ids"]["wetrakr"] == str(media["id"])
    if media["type"] in {"episode", "season"}:
        assert captured["show_ids"]["wetrakr"] == str(SHOW["id"])
        assert key.startswith(f"wetrakr:{SHOW['id']}#")
    cleanup = snapshots.clear_provider_features("WETRAKR", [feature], instance_id="P01", cfg=surfaces.cfg)
    assert cleanup["ok"], cleanup
    assert cleanup["results"][feature]["remaining"] == 0
    assert not surfaces.adapter.build_index(feature, force_refresh=True)
    restored = snapshots.restore_snapshot(capture["path"], instance_id="P01", cfg=surfaces.cfg)
    assert restored["ok"], restored
    assert len(surfaces.adapter.build_index(feature, force_refresh=True)) == 1


def test_cleanup_removes_all_plays_and_skips_collections(surfaces):
    from services import snapshots

    for media in (MOVIE, EPISODE):
        for stamp in (WHEN, LATER):
            surfaces.server.play(media, stamp)
    result = snapshots.clear_provider_features("WETRAKR", ["history", "collection"],
        instance_id="P01", cfg=surfaces.cfg)
    assert result["ok"], result
    assert not surfaces.server.history
    assert result["results"]["collection"]["skipped"]
    assert any(path == "/sync/tracking/remove/all" for _, path, _ in surfaces.server.calls)


def test_cleanup_missing_profile_cannot_use_configured_account(surfaces):
    from services import snapshots

    surfaces.server.planning.append(copy.deepcopy(MOVIE))
    with pytest.raises(ValueError, match="not configured"):
        snapshots.clear_provider_features("WETRAKR", ["watchlist"], instance_id="missing", cfg=surfaces.cfg)
    assert not surfaces.server.calls
    assert surfaces.server.planning


def test_editor_removes_only_selected_rewatch(surfaces):
    from services import editor_removal
    from providers.sync._mod_WETRAKR import WETRAKRModule

    for stamp in (WHEN, LATER):
        surfaces.server.play(EPISODE, stamp)
    adapter = WETRAKRModule({**surfaces.view, "_cw_history_rewatches": True})
    watches = adapter.build_index("history")
    key, selected = next(iter(watches.items()))
    result = editor_removal._remove_viewings(surfaces.cfg, "history",
        {"provider": "WETRAKR", "instance": "P01", "viewings": {key: selected}}, dry_run=False)
    assert result["ok"] and result["removed"] == 1, result
    remaining = adapter.build_index("history", force_refresh=True)
    assert len(remaining) == 1
    assert next(iter(remaining.values()))["_wetrakr_history_id"] != selected["_wetrakr_history_id"]


def test_analyzer_classifies_tracker_and_matches_native_episode_ids():
    from services import analyzer

    assert analyzer._is_tracker_to_media_server("WETRAKR@P01", ["PLEX@P02"])
    assert not analyzer._is_tracker_to_media_server("WETRAKR@P01", ["TRAKT"])
    native = {"type": "episode", "season": 1, "episode": 1,
        "ids": {"wetrakr": "174658"}, "show_ids": {"wetrakr": "1391953"}}
    aliases = analyzer._alias_keys(native)
    assert "wetrakr:1391953#s01e01" in aliases
    assert "ep:wetrakr:174658" in aliases
    assert "wetrakr:174658#s01e01" not in aliases
    provider = "WETRAKR@P01"
    state = {"providers": {
        "WETRAKR": {"instances": {"P01": {"history": {"baseline": {"items": {"native-row": native}}}}}},
        "PLEX": {"history": {"baseline": {"items": {"plex-row": {**native, "ids": {"plex": "55"}}}}}},
    }}
    cfg = {"pairs": [{"id": "native", "enabled": True, "source": "WETRAKR", "source_instance": "P01",
        "target": "PLEX", "mode": "one-way", "features": {"history": {"enable": True}}}]}
    context = analyzer._analysis_context(state, cfg)
    assert analyzer._target_peer_match(context, provider, "history", "native-row", native, "PLEX") == "alias"


def test_native_ids_survive_existing_database_upgrade():
    from cw_platform.local_db import schema

    tables = ("baseline_items", "pair_baseline_items", "manual_policy_add_items", "sync_run_spotlight_items")
    conn = sqlite3.connect(":memory:")
    try:
        schema.apply_schema(conn)
        for table in tables:
            conn.execute(f"ALTER TABLE {table} DROP COLUMN ids_wetrakr")
            conn.execute(f"ALTER TABLE {table} DROP COLUMN show_ids_wetrakr")
        conn.execute("DELETE FROM schema_migrations WHERE version = ?", (schema.SCHEMA_VERSION,))
        conn.execute("INSERT INTO schema_migrations VALUES (3, 1, 'local_state')")
        conn.execute("INSERT INTO state_meta(key, value_text, value_type, updated_at) VALUES ('preserved', 'old data', 'text', 1)")
        schema.apply_schema(conn)
        schema.apply_schema(conn)
        for table in tables:
            columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
            assert {"ids_wetrakr", "show_ids_wetrakr"} <= columns
        assert conn.execute("SELECT value_text FROM state_meta WHERE key = 'preserved'").fetchone()[0] == "old data"
        assert conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == schema.SCHEMA_VERSION
    finally:
        conn.close()


def test_saved_editor_rewatches_keep_native_event_identity(surfaces):
    from api import editorAPI
    from providers.sync._mod_WETRAKR import WETRAKRModule

    for _ in range(2):
        surfaces.server.play(EPISODE, WHEN)
    adapter = WETRAKRModule({**surfaces.view, "_cw_history_rewatches": True})
    watches = adapter.build_index("history")
    editorAPI._save_state_items("history", "WETRAKR", watches, "P01")
    saved = editorAPI._load_state_items("history", "WETRAKR", "P01")
    assert set(saved) == set(watches)
    assert {row["provider_event_id"] for row in saved.values()} == {"play-1", "play-2"}
    assert all(row["show_ids"]["wetrakr"] == str(SHOW["id"]) for row in saved.values())


def test_mapping_catalog_and_episode_suggestions_use_selected_profile(surfaces):
    from services.interactive_sync_catalogs import search_catalogs
    from services.interactive_sync_episodes import metadata_key

    cfg = {**surfaces.cfg, "tmdb": {"api_key": "metadata-test-key"}}
    row = {"provider": "WETRAKR", "instance": "P01", "item": item(EPISODE)}
    assert search_catalogs(cfg, row)["catalogs"] == [{"id": "tmdb", "label": "TMDb metadata"}]
    assert metadata_key(cfg, row) == "metadata-test-key"
    assert search_catalogs(surfaces.cfg, row)["catalogs"] == []
    for instance in ("default", "missing"):
        assert search_catalogs(cfg, {**row, "instance": instance})["catalogs"] == []
        assert metadata_key(cfg, {**row, "instance": instance}) is None


@pytest.mark.parametrize("media", [MOVIE, EPISODE])
@pytest.mark.parametrize("scope", ["", "wetrakr-pair"])
def test_mapping_prepare_persist_and_apply(surfaces, media, scope):
    from api import editorAPI
    from api.interactiveSyncAPI import prepare_mapping
    from cw_platform.id_map import canonical_key
    from cw_platform.mapping_policy import effective_policy

    original = {**item(media), "ids": {"wetrakr": "999"}, "watched_at": WHEN, "_wetrakr_history_id": "old-play"}
    if media["type"] == "episode":
        original["show_ids"] = {"wetrakr": "998"}
    old_key = canonical_key(original)
    corrected = item(media)
    row = {"provider": "WETRAKR", "instance": "P01", "feature": "history", "key": old_key, "item": original}
    key, prepared, blocks = prepare_mapping(row, corrected)
    assert "_wetrakr_history_id" not in prepared
    assert prepared["watched_at"] == WHEN
    assert prepared["ids"] == corrected["ids"]
    assert old_key in blocks
    editorAPI._save_policy_manual_batch([("history", "WETRAKR", {key: prepared}, blocks, "P01")], pair_id=scope,
        mappings={("history", "WETRAKR", "P01"): {key: {"original_key": old_key, "original": original, "origin": "editor"}}})
    policy = editorAPI._load_policy()
    node = effective_policy(policy, scope)["providers"]["WETRAKR"]
    assert "history" not in node
    saved = node["instances"]["P01"]["history"]["adds"]["items"][key]
    assert saved["ids"] == corrected["ids"]
    if media["type"] == "episode":
        assert saved["show_ids"] == corrected["show_ids"]
    if scope:
        assert "WETRAKR" not in effective_policy(policy, "another-pair")["providers"]
    assert surfaces.adapter.add("history", [saved])["ok"]
    written = next(iter(surfaces.adapter.build_index("history", force_refresh=True).values()))
    assert written["ids"]["wetrakr"] == str(media["id"])


def test_mapping_search_uses_tmdb_for_wetrakr_destination(surfaces, monkeypatch):
    import requests
    from fastapi import HTTPException
    from services.interactive_sync_mapping import search_candidates
    from test_wetrakr_sync import Response

    cfg = {**surfaces.cfg, "tmdb": {"api_key": "metadata-test-key"}}
    row = {"provider": "WETRAKR", "instance": "P01", "item": item(MOVIE)}
    calls = []
    def get(session, url, **kwargs):
        calls.append((url, kwargs))
        return Response({"results": [{"id": 155, "title": "The Dark Knight", "release_date": "2008-07-16"}]})
    monkeypatch.setattr(requests.Session, "get", get)
    result = search_candidates(cfg, row, "The Dark Knight", catalog="tmdb")
    assert result["results"][0]["ids"] == {"tmdb": "155"}
    assert calls[0][0] == "https://api.themoviedb.org/3/search/movie"
    assert calls[0][1]["params"]["api_key"] == "metadata-test-key"
    with pytest.raises(HTTPException) as exc:
        search_candidates(cfg, row, "The Dark Knight", catalog="destination")
    assert exc.value.status_code == 409


def test_watchlist_native_id_grouping_and_removal(surfaces):
    from services import watchlist

    native = {"type": "movie", "title": "Movie", "ids": {"wetrakr": str(MOVIE["id"])}}
    external = {**native, "ids": {**native["ids"], "tmdb": "155"}}
    refs = [("wetrakr:126", "WETRAKR", "P01", native), ("tmdb:155", "CROSSWATCH", "default", external)]
    assert len(watchlist._group_watchlist_refs(refs)) == 1
    assert watchlist._ids_from_key_or_item("wetrakr:126", {}) == native["ids"]
    assert watchlist._preferred_watchlist_key([], native, "movie") == "wetrakr:126"
    assert surfaces.adapter.add("watchlist", [native])["ok"]
    watchlist._delete_on_ops_watchlist_batch("WETRAKR", [{"key": "wetrakr:126", "type": "movie"}], surfaces.view)
    assert not surfaces.adapter.build_index("watchlist", force_refresh=True)


def test_dashboard_and_export_keep_native_identity(surfaces, monkeypatch):
    from services import dashboard_widgets, export

    native = {"type": "episode", "title": "Pilot", "season": 1, "episode": 1,
        "watched_at": WHEN, "ids": {"wetrakr": "174658"}, "show_ids": {"wetrakr": "1391953"}}
    aliases = dashboard_widgets._media_id_aliases("history", native)
    assert "history|id|episode|wetrakr:1391953|s1|e1" in aliases
    assert not set(aliases) & set(dashboard_widgets._media_id_aliases("history", {**native, "episode": 2}))
    assert export._match_query("tmdb:62085", native, "wetrakr:174658")
    assert export._norm_ids(native["ids"]) == native["ids"]


def test_native_id_title_lookup(surfaces, config_base):
    from api import editorAPI
    from cw_platform.local_db.title_index import history_title_maps

    rows = {
        "tmdb:155": {"type": "movie", "title": "The Dark Knight", "ids": {"tmdb": "155", "wetrakr": "126"}},
        "tmdb:1396#s01e01": {"type": "episode", "series_title": "Breaking Bad", "title": "Pilot",
            "ids": {"wetrakr": "174658"}, "show_ids": {"wetrakr": "1391953"}, "season": 1, "episode": 1},
    }
    editorAPI._save_state_items("history", "WETRAKR", rows, "P01")
    _, movies, shows = history_title_maps(config_base)
    assert movies["wetrakr:126"][0] == "The Dark Knight"
    assert shows["wetrakr:1391953"] == "Breaking Bad"


@pytest.mark.parametrize("media", [SEASON, EPISODE])
def test_manual_mapping_show_native_id_is_not_used_as_child_rating_id(surfaces, media):
    source = {**item(media), "ids": {"wetrakr": str(SHOW["id"])}, "rating": 8}
    assert surfaces.adapter.add("ratings", [source])["ok"]
    payload = next(kw["json"] for method, path, kw in surfaces.server.calls if method == "POST" and path == "/sync/ratings")
    assert payload == {media["type"] + "s": [{"id": media["id"], "rating": 8}]}


@pytest.mark.parametrize("media", [SEASON, EPISODE])
def test_numbering_mapping_discards_stale_native_child_id(surfaces, media):
    from api.interactiveSyncAPI import prepare_mapping
    from cw_platform.id_map import canonical_key

    original = {**item(media), "season": 2, "ids": {"wetrakr": "999"}, "rating": 8}
    corrected = {**original, "season": 1}
    _, prepared, _ = prepare_mapping({"item": original, "key": canonical_key(original)}, corrected)
    assert "wetrakr" not in prepared["ids"]
    assert surfaces.adapter.add("ratings", [prepared])["ok"]
    payload = next(kw["json"] for method, path, kw in surfaces.server.calls if method == "POST" and path == "/sync/ratings")
    assert payload == {media["type"] + "s": [{"id": media["id"], "rating": 8}]}


@pytest.mark.parametrize("episode", [False, True])
def test_quick_add_uses_profile_and_real_history_adapter(surfaces, monkeypatch, episode):
    import json
    from api import manualAPI
    from cw_platform import config_base

    monkeypatch.setattr(config_base, "load_config", lambda: surfaces.cfg)
    monkeypatch.setattr(manualAPI, "_manual_external_ids", lambda *a: item(SHOW if episode else MOVIE)["ids"])
    response = manualAPI.api_manual_watched({"item": {"type": "show" if episode else "movie", "tmdb": 1396 if episode else 155},
        "episodes": [{"season": 1, "episode": 1}] if episode else [], "episode_source": "tmdb",
        "actions": {"history": True}, "providers": [{"provider": "WETRAKR", "instance": "P01"}]}, request=None)
    result = json.loads(response.body)
    assert result["ok"], result
    assert result["results"][0]["instance"] == "P01"
    written = next(iter(surfaces.adapter.build_index("history", force_refresh=True).values()))
    assert written["type"] == ("episode" if episode else "movie")
