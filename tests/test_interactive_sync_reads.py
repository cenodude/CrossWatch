# tests/test_interactive_sync_reads.py
# CrossWatch - Interactive sync provider read budgets
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from copy import deepcopy
from unittest.mock import Mock

import pytest

from api import editorAPI, syncAPI
from cw_platform.interactive_reads import ReviewReads, read_once, use_review_reads
from services import interactive_sync as svc
from test_interactive_sync_features import FEATURES, feature_item, feature_setup


@pytest.mark.parametrize("feature", FEATURES)
@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_remapping_and_apply_read_each_provider_once(config_base, monkeypatch, feature, mode):
    original = feature_item(feature, 1)
    cfg, src, dst = feature_setup(config_base, monkeypatch, feature, [original, feature_item(feature, 3)], [], mode)
    cfg["runtime"]["apply_chunk_size"] = 1
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: deepcopy(cfg), lambda *_: None))
    monkeypatch.setattr(editorAPI, "_STATE_BASE", config_base)
    for ops in (src, dst):
        monkeypatch.setattr(ops, "build_index", Mock(wraps=ops.build_index))
        monkeypatch.setattr(ops, "health", Mock(wraps=ops.health))
        monkeypatch.setattr(ops, "activities", Mock(return_value={"updated_at": "2026-01-01"}), raising=False)
    session = svc.Session(pair_id="p1", owner="local")
    try:
        svc.refresh(session, cfg, {})
        inputs_path = session.reads.path
        corrected = feature_item(feature, 2)
        editorAPI._save_policy_manual(feature, "SRC", {"imdb:tt0000002": corrected}, ["imdb:tt0000001"], merge=True)
        svc.refresh_mappings(session, cfg, {}, ())
        svc.recalculate(session, cfg, {})
        assert {row["key"] for row in session.plan.rows.values()} == {"imdb:tt0000002", "imdb:tt0000003"}
        assert not dst.add_calls
        svc.apply(session, cfg, set(session.plan.rows))
        assert session.status == "complete", session.message
        assert len(dst.add_calls) == 2
        assert not inputs_path.exists()
        for ops in (src, dst):
            assert ops.build_index.call_count == 1
            assert ops.health.call_count == 1
            assert ops.activities.call_count == 1
    finally:
        session.close()


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_empty_review_reads_once(config_base, monkeypatch, mode):
    cfg, src, dst = feature_setup(config_base, monkeypatch, "watchlist", [], [], mode)
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: deepcopy(cfg), lambda *_: None))
    for ops in (src, dst):
        monkeypatch.setattr(ops, "build_index", Mock(wraps=ops.build_index))
    session = svc.Session(pair_id="p1", owner="local")
    try:
        svc.refresh(session, cfg, {})
        assert session.status == "complete"
        assert src.build_index.call_count == dst.build_index.call_count == 1
        assert not src.add_calls and not dst.add_calls
    finally:
        session.close()


def test_explicit_refresh_fetches_new_inventory(config_base, monkeypatch):
    cfg, src, dst = feature_setup(config_base, monkeypatch, "watchlist", [feature_item("watchlist", 1)], [])
    monkeypatch.setattr(src, "build_index", Mock(wraps=src.build_index))
    session = svc.Session(pair_id="p1", owner="local")
    try:
        svc.refresh(session, cfg, {})
        old_path = session.reads.path
        src.index["imdb:tt0000002"] = feature_item("watchlist", 2)
        svc.recalculate(session, cfg, {})
        assert len(session.plan.rows) == 1
        svc.refresh(session, cfg, {})
        assert len(session.plan.rows) == 2
        assert src.build_index.call_count == 2
        assert not old_path.exists()
        assert not dst.add_calls
    finally:
        session.close()


def test_replay_never_fetches_missing_inputs_and_does_not_leak():
    reads = ReviewReads()
    fetch = Mock(return_value={"item": {"rating": 8}})
    try:
        with use_review_reads(reads, collecting=True):
            observed = read_once("index", fetch)
            observed["item"]["rating"] = 2
        with use_review_reads(reads):
            assert read_once("index", fetch)["item"]["rating"] == 8
            with pytest.raises(RuntimeError, match="missing provider data"):
                read_once("missing", fetch)
        assert fetch.call_count == 1
        read_once("index", fetch)
        assert fetch.call_count == 2
    finally:
        reads.close()


def test_settings_change_requires_explicit_refresh(config_base, monkeypatch):
    cfg, src, dst = feature_setup(config_base, monkeypatch, "watchlist", [feature_item("watchlist", 1)], [])
    monkeypatch.setattr(src, "build_index", Mock(wraps=src.build_index))
    session = svc.Session(pair_id="p1", owner="local")
    try:
        svc.refresh(session, cfg, {})
        cfg["pairs"][0]["source_instance"] = "other"
        svc.apply(session, cfg, set(session.plan.rows))
        assert session.status == "review"
        assert "Settings changed" in session.message
        assert src.build_index.call_count == 1
        assert not dst.add_calls
    finally:
        session.close()


def test_playlist_observations_are_separate_for_provider_instances():
    from types import SimpleNamespace
    from cw_platform.interactive_reads import retained_read

    def fetch(adapter):
        return adapter.instance_id
    fetch.__module__ = "example._playlists"
    captured = retained_read(fetch)
    reads = ReviewReads()
    try:
        with use_review_reads(reads, collecting=True):
            assert captured(SimpleNamespace(instance_id="home")) == "home"
            assert captured(SimpleNamespace(instance_id="remote")) == "remote"
        with use_review_reads(reads):
            assert captured(SimpleNamespace(instance_id="remote")) == "remote"
            assert captured(SimpleNamespace(instance_id="home")) == "home"
    finally:
        reads.close()


def test_simkl_anime_review_renormalizes_native_coordinates_locally(monkeypatch):
    from types import SimpleNamespace
    from providers.sync.simkl import _history
    from cw_platform.anime_mapping.episodes import SourceCoordinate

    native = dict(show_ids={"simkl": "100"}, season=1, episode=27,
                  alias=None, tvdb={"season": 2, "episode": 1})
    item = dict(type="episode", show_ids={"simkl": "100"}, season=2, episode=1,
                watched_at="2026-01-01T00:00:00Z", _cw_simkl_native=native)
    adapter = SimpleNamespace()
    monkeypatch.setattr(_history, "_anibridge_release_tag", lambda _: "test")
    monkeypatch.setattr(_history, "_anime_source_coordinate",
                        lambda *_: SourceCoordinate("tmdb", "200", 3, 5, "user_override", "test"))
    updated = next(iter(_history.replay_index(adapter, {"old": item}).values()))
    assert (updated["season"], updated["episode"]) == (3, 5)
    assert updated["show_ids"]["tmdb"] == "200"
    monkeypatch.setattr(_history, "_anime_source_coordinate", lambda *_: None)
    restored = next(iter(_history.replay_index(adapter, {"old": item}).values()))
    assert (restored["season"], restored["episode"]) == (2, 1)
    assert "_cw_simkl_native" not in restored
    assert item["_cw_simkl_native"] == native


def test_plex_review_retains_native_catalog_for_write_adapter(monkeypatch):
    from types import SimpleNamespace
    from providers.sync.plex import _history

    reads = ReviewReads()
    adapter = SimpleNamespace()
    catalog = _history.HistoryCatalog()
    catalog.by_rk["42"] = {"type": "movie", "ids": {"tmdb": "1"}}
    monkeypatch.setattr(_history, "_catalog_cache_key", lambda *_: ("preview",))
    try:
        with use_review_reads(reads, collecting=True):
            _history._store_history_catalog(adapter, set(), catalog)
        monkeypatch.setattr(_history, "_index_cache_context", Mock(side_effect=AssertionError("write must use captured catalog")))
        with use_review_reads(reads):
            result = _history._get_history_catalog(SimpleNamespace(), set(), force=True)
        assert result.by_rk == catalog.by_rk
    finally:
        reads.close()


@pytest.mark.parametrize("provider", ["emby", "jellyfin"])
def test_interactive_favorite_writes_skip_live_verification(monkeypatch, provider):
    from importlib import import_module
    from types import SimpleNamespace

    watchlist = import_module(f"providers.sync.{provider}._watchlist")
    adapter = SimpleNamespace(cfg=SimpleNamespace(user_id="user", watchlist_write_delay_ms=0), client=Mock())
    adapter.client.get.side_effect = AssertionError("unexpected reread")
    monkeypatch.setattr(watchlist, "resolve_item_id", lambda *_: "42")
    monkeypatch.setattr(watchlist, "_verify_favorite", Mock(side_effect=AssertionError("unexpected verification")))
    monkeypatch.setattr(watchlist, "_thaw_if_present", lambda *_: None)
    write = Mock(return_value=True)
    monkeypatch.setattr(watchlist, "mark_favorite", write)
    reads = ReviewReads()
    try:
        with use_review_reads(reads):
            assert watchlist._add_favorites(adapter, [feature_item("watchlist", 1)]) == (1, [])
            assert watchlist._remove_favorites(adapter, [feature_item("watchlist", 1)]) == (1, [])
        assert write.call_count == 2
        adapter.client.get.assert_not_called()
    finally:
        reads.close()


def test_local_recalculation_without_captured_inputs_does_not_read(config_base, monkeypatch):
    cfg, src, dst = feature_setup(config_base, monkeypatch, "watchlist", [feature_item("watchlist", 1)], [])
    monkeypatch.setattr(src, "build_index", Mock(side_effect=AssertionError("unexpected read")))
    session = svc.Session(pair_id="p1", owner="local")
    try:
        svc.recalculate(session, cfg, {})
        assert session.reads is None
        assert "Refresh provider data" in session.message
    finally:
        session.close()


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_dropped_show_state_is_captured_for_recalculation_and_apply(config_base, monkeypatch, mode):
    cfg, src, dst = feature_setup(config_base, monkeypatch, "history", [feature_item("history", 1)], [], mode)
    src.provider = "SIMKL"
    cfg["pairs"][0]["source"] = "SIMKL"
    cfg["simkl"] = {"history_ignore_dropped_shows": True}
    dropped = Mock(return_value=set())
    monkeypatch.setattr(src, "dropped_show_tokens", dropped, raising=False)
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SIMKL": src, "DST": dst})
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: deepcopy(cfg), lambda *_: None))
    session = svc.Session(pair_id="p1", owner="local")
    try:
        svc.refresh(session, cfg, {})
        svc.recalculate(session, cfg, {})
        svc.apply(session, cfg, set(session.plan.rows))
        assert session.status == "complete", session.message
        assert dropped.call_count == 1
        assert len(dst.add_calls) == 1
    finally:
        session.close()


def test_failed_apply_invalidates_inputs_before_recalculation_or_retry(config_base, monkeypatch):
    item = feature_item("history", 1)
    cfg, src, dst = feature_setup(config_base, monkeypatch, "history", [item], [])
    writes = Mock()
    def failing_apply(*args, **kwargs):
        writes()
        raise RuntimeError("failed after a write")
    monkeypatch.setattr(syncAPI, "_run_pairs_thread_entry", failing_apply)
    session = svc.Session(pair_id="p1", owner="local")
    try:
        svc.refresh(session, cfg, {})
        path = session.reads.path
        selected = set(session.plan.rows)
        with pytest.raises(RuntimeError, match="after a write"):
            svc.apply(session, cfg, selected)
        assert session.reads is None and session.plan.reads is None
        assert not path.exists()
        svc.recalculate(session, cfg, {})
        assert session.status == "error"
        svc.apply(session, cfg, selected)
        assert session.status == "error"
        assert "Refresh provider data" in session.message
        writes.assert_called_once()
    finally:
        session.close()


@pytest.mark.parametrize("provider", ["emby", "jellyfin"])
def test_native_history_state_protects_latest_watch_despite_different_public_ids(monkeypatch, provider):
    from importlib import import_module
    from types import SimpleNamespace
    from cw_platform.interactive_reads import replace_retained

    history = import_module(f"providers.sync.{provider}._history")
    common = import_module(f"providers.sync.{provider}._common")
    adapter = SimpleNamespace(client=Mock(), cfg=SimpleNamespace(user_id="user"))
    source = {"type": "movie", "ids": {"imdb": "tt1234567"}, "watched_at": "2026-09-02T00:00:00Z"}
    captured = {str(n): {"type": "movie", "ids": {"tmdb": "99"}, f"{provider}_item_id": "native-1", "watched_at": stamp}
                for n, stamp in enumerate(["2026-09-03T00:00:00Z", None, "2026-09-01T00:00:00Z"])}
    for name in ("_shadow_load", "_bb_load"):
        monkeypatch.setattr(history, name, lambda: {})
    for name in ("_shadow_save", "_bb_save", "_unres_flush", "_thaw_if_present"):
        monkeypatch.setattr(history, name, lambda *args: None)
    monkeypatch.setattr(history, "_history_limit", lambda *_: 25)
    monkeypatch.setattr(history, "_history_delay_ms", lambda *_: 0)
    monkeypatch.setattr(history, "_dst_user_states", Mock(side_effect=AssertionError("unexpected reread")))
    write = Mock(side_effect=AssertionError("must not overwrite the newer watch"))
    monkeypatch.setattr(history, "_mark_played", write)
    if provider == "emby":
        monkeypatch.setattr(common, "provider_index", lambda *_: {})
        monkeypatch.setattr(history, "_prepare_mids", lambda *_: ({"imdb:tt1234567": source}, [("imdb:tt1234567", "native-1")], []))
    else:
        lookup = import_module("providers.sync.jellyfin._id_lookup")
        monkeypatch.setattr(lookup, "prepare", lambda *_: None)
        monkeypatch.setattr(history, "_try_resolve_iid", lambda *_: "native-1")
    reads = ReviewReads()
    try:
        with use_review_reads(reads, collecting=True):
            replace_retained(history.build_index, captured)
        with use_review_reads(reads):
            assert history.add(adapter, [source]) == (0, [])
        write.assert_not_called()
        assert history._dst_user_states.call_count == 0
        assert adapter._history_write_meta["results"][0]["reason"] == "existing_newer"
    finally:
        reads.close()


def test_simkl_cached_anime_does_not_force_inventory_download(monkeypatch):
    from types import SimpleNamespace
    from providers.sync.simkl import _history as history

    cached = {"event": dict(type="episode", show_ids={"simkl": "100"}, season=2, episode=1,
                            _simkl_episode_number=27, watched_at="2026-01-01T00:00:00Z")}
    monkeypatch.setattr(history, "_cache_load", lambda: deepcopy(cached))
    monkeypatch.setattr(history, "_cache_doc_is_stale", lambda *_: False)
    monkeypatch.setattr(history, "normalize_flat_watermarks", lambda: None)
    monkeypatch.setattr(history, "get_watermark", lambda *_: "2026-09-01T00:00:00Z")
    monkeypatch.setattr(history, "fetch_activities", lambda *args, **kwargs: ({}, None))
    monkeypatch.setattr(history, "_headers", lambda *args, **kwargs: {})
    monkeypatch.setattr(history, "_fetch_all_items", Mock(side_effect=AssertionError("unexpected full read")))
    adapter = SimpleNamespace(client=SimpleNamespace(session=object()), cfg=SimpleNamespace(timeout=5), config={})
    reads = ReviewReads()
    try:
        with use_review_reads(reads, collecting=True):
            assert history.build_index(adapter) == cached
        history._fetch_all_items.assert_not_called()
    finally:
        reads.close()


def test_simkl_capture_and_replay_preserve_native_alias_identity_and_keep_cache_clean(monkeypatch):
    from types import SimpleNamespace
    from providers.sync.simkl import _history as history
    from cw_platform.anime_mapping.episodes import SourceCoordinate

    adapter = SimpleNamespace()
    native = dict(show_ids={"simkl": "100"}, season=1, episode=27,
                  alias={"show_ids": {"tmdb": "200"}, "season": 2, "episode": 1}, tvdb=None)
    original = dict(type="episode", show_ids={"simkl": "100", "tmdb": "200"}, ids={"tvdb": "999"},
                    title="Original episode", season=2, episode=1, _simkl_episode_number=27,
                    watched_at="2026-01-01T00:00:00Z", _cw_simkl_native=native)
    monkeypatch.setattr(history, "_anibridge_release_tag", lambda *_: "test")
    monkeypatch.setattr(history, "_load_anime_episode_map_cache", lambda: {"100": [{"episode": 27, "tvdb": {"season": 2, "episode": 1}}]})
    monkeypatch.setattr(history, "_anime_source_coordinate", lambda *_: None)
    capture = history.capture_index(adapter, {"original": original})
    unchanged = history.replay_index(adapter, capture)
    assert unchanged == {"original": {key: value for key, value in original.items() if key != "_cw_simkl_native"}}
    monkeypatch.setattr(history, "_anime_source_coordinate", lambda *_: SourceCoordinate("tmdb", "300", 3, 5, "user_override", "test"))
    changed = next(iter(history.replay_index(adapter, capture).values()))
    assert changed["show_ids"] == {"simkl": "100", "tmdb": "300"}
    assert changed["ids"] == {}
    assert changed["title"] == "S03E05"
    save = Mock()
    monkeypatch.setattr(history, "_save_json", save)
    history._cache_save(capture, rewatches=False)
    assert "_cw_simkl_native" not in save.call_args.args[1]["items"]["original"]
    assert "_cw_simkl_native" in capture["original"]


def test_retained_reads_normalize_keywords_without_colliding_query_scopes():
    from types import SimpleNamespace
    from cw_platform.interactive_reads import retained_read

    calls = []
    @retained_read
    def fetch(adapter, kind="movies", *, limit=None, force_refresh=False):
        calls.append((kind, limit))
        return [kind, limit]
    reads = ReviewReads()
    adapter = SimpleNamespace()
    try:
        with use_review_reads(reads, collecting=True):
            assert fetch(adapter, "movies", limit=1) == ["movies", 1]
            assert fetch(adapter, kind="shows", limit=2) == ["shows", 2]
        with use_review_reads(reads):
            assert fetch(adapter=adapter, kind="movies", limit=1, force_refresh=True) == ["movies", 1]
            assert fetch(adapter, "shows", limit=2) == ["shows", 2]
            with pytest.raises(RuntimeError, match="missing provider data"):
                fetch(adapter, "movies", limit=2)
        assert calls == [("movies", 1), ("shows", 2)]
    finally:
        reads.close()


def test_removing_anime_override_uses_cached_native_episode_data(monkeypatch):
    from types import SimpleNamespace
    from providers.sync.simkl import _history as history
    from cw_platform.anime_mapping.episodes import SourceCoordinate

    adapter = SimpleNamespace()
    legacy = dict(type="episode", season=3, episode=5, show_ids={"simkl": "100", "tmdb": "300"},
                  ids={"tvdb": "999"}, _simkl_episode_number=27, watched_at="2026-01-01T00:00:00Z")
    monkeypatch.setattr(history, "_anibridge_release_tag", lambda *_: "test")
    monkeypatch.setattr(history, "_load_anime_episode_map_cache", lambda: {"100": [{"episode": 27, "tvdb": {"season": 2, "episode": 1}}]})
    monkeypatch.setattr(history, "_anime_source_coordinate", lambda *_: SourceCoordinate("tmdb", "300", 3, 5, "user_override", "test"))
    capture = history.capture_index(adapter, {"original": legacy})
    monkeypatch.setattr(history, "_anime_source_coordinate", lambda *_: None)
    replay = next(iter(history.replay_index(adapter, capture).values()))
    assert (replay["season"], replay["episode"]) == (2, 1)
    assert replay["show_ids"] == {"simkl": "100"}
    assert replay["ids"] == {}
    assert legacy["show_ids"]["tmdb"] == "300"


@pytest.mark.parametrize("operation", ["add", "update", "remove"])
@pytest.mark.parametrize("interactive,dry_run", [(True, False), (False, False), (True, True)])
def test_successful_writes_without_adapter_receipts_are_reported_as_unverified(operation, interactive, dry_run):
    from types import SimpleNamespace
    from cw_platform.orchestrator import _applier
    from services.interactive_sync_report import SyncReport

    item = feature_item("watchlist", 1)
    write = Mock(return_value=dict(ok=True, count=1, confirmed_keys=["imdb:tt0000001"]))
    provider = SimpleNamespace(add=write, remove=write)
    report = SyncReport()
    def emit(event, **fields):
        report.event(dict(event=event, **fields))
    reads = ReviewReads()
    try:
        with use_review_reads(reads if interactive else None):
            getattr(_applier, "apply_" + operation)(
                dst_ops=provider, cfg={}, dst_name="EMBY", feature="watchlist", items=[item],
                dry_run=dry_run, emit=emit, dbg=lambda *_: None, chunk_size=100, chunk_pause_ms=0)
        assert sum(report.accepted_unverified.values()) == int(interactive and not dry_run)
    finally:
        reads.close()
