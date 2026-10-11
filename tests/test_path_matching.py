# tests/test_path_matching.py
# CrossWatch - Pair path matching regressions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace

import pytest

from api import syncAPI as api
from cw_platform.id_map import canonical_key, minimal
from cw_platform.path_matching import PathAliases, PathIndex, media_paths, pair_enabled, path_keys, rekey_by_path
from providers.sync import _path_matching as lookup


def movie(path="/media/movies/Alien (1979)/Alien.mkv", **extra):
    return {"type": "movie", "title": "Alien", "year": 1979, "ids": {}, "media_paths": [path], **extra}


def test_roots_separators_and_case():
    assert path_keys(movie()) == path_keys(movie("Z:\\Movies\\Alien (1979)\\ALIEN.mkv"))
    assert path_keys(movie()) != path_keys(movie("/movies/Alien (1979)/Alien-remux.mkv"))
    assert not path_keys(movie("/Alien.mkv"))
    assert not path_keys(movie("https://host/movies/Alien.mkv"))
    assert not path_keys(movie("C:\\Alien.mkv"))


def test_episode_coordinates_parts_and_types():
    episode = movie("/tv/Show/Season 1/Episode.mkv", type="episode", season=1, episode=1)
    assert path_keys(episode)
    assert path_keys(episode) != path_keys({**episode, "episode": 2})
    assert not path_keys({**episode, "season": None})
    assert not path_keys({**episode, "type": "show"})
    assert path_keys(movie(part=1)) != path_keys(movie(part=2))
    assert path_keys(movie(part=1)) != path_keys(movie())


def test_native_path_extraction_and_minimal():
    obj = SimpleNamespace(media=[SimpleNamespace(parts=[SimpleNamespace(file="/movies/A/A.mkv")])])
    assert media_paths(obj) == ["/movies/A/A.mkv"]
    assert media_paths({"Media": [{"Part": [{"file": "/movies/A/A.mkv"}]}]}) == media_paths(obj)
    assert media_paths({"MediaSources": [{"Path": "/movies/A/A.mkv"}]}) == media_paths(obj)
    assert minimal(movie())["media_paths"] == movie()["media_paths"]


def test_ambiguity_and_conflicting_ids_are_not_matches():
    item = movie(ids={"tmdb": "1"})
    assert PathIndex([("a", movie())]).resolve(item) == ("a", "path_match")
    assert PathIndex([("a", movie()), ("b", movie())]).resolve(item) == (None, "path_ambiguous")
    assert PathIndex([("a", movie(ids={"tmdb": "2"}))]).resolve(item) == (None, "path_id_conflict")
    assert PathIndex([("a", movie(show_ids={"tvdb": "2"}))]).resolve(movie(show_ids={"tvdb": "1"}))[1] == "path_id_conflict"


def test_pair_support_and_boolean_coercion():
    pair = {"source": "PLEX", "target": "JELLYFIN", "path_matching": True}
    assert pair_enabled(pair)
    assert not pair_enabled({**pair, "target": "TRAKT"})
    assert not pair_enabled({**pair, "path_matching": "false"})
    assert not pair_enabled({"source": "PLEX", "target": "EMBY"})


def test_api_roundtrip_and_unsupported_pair(monkeypatch):
    cfg = {"pairs": []}
    monkeypatch.setattr(api, "_env", lambda: (lambda: cfg, lambda value: None))
    result = api.api_pairs_add(api.PairIn(source="PLEX", target="JELLYFIN", path_matching=True))
    assert result["ok"] and cfg["pairs"][0]["path_matching"]
    assert api.api_pairs_update(result["id"], api.PairPatch(path_matching=False))["ok"]
    assert cfg["pairs"][0]["path_matching"] is False
    assert api.api_pairs_update(result["id"], api.PairPatch(path_matching=True, target="TRAKT"))["ok"]
    assert cfg["pairs"][0]["path_matching"] is False


def test_fallback_is_opt_in_and_ids_win(monkeypatch):
    calls = []
    adapter = SimpleNamespace(path_matching=False, cfg=SimpleNamespace(strict_id_matching=False))
    monkeypatch.setattr(lookup, "_catalog", lambda *args: pytest.fail("Unexpected catalogue read"))

    @lookup.path_fallback("JELLYFIN")
    def resolve(ad, item, **kwargs):
        calls.append(ad.cfg.strict_id_matching)
        return "id-hit"

    assert resolve(adapter, movie()) == "id-hit"
    assert calls == [False]
    adapter.path_matching = True
    assert resolve(adapter, movie()) == "id-hit"
    assert calls == [False, True]
    assert adapter.cfg.strict_id_matching is False


@pytest.mark.parametrize("provider", ["PLEX", "JELLYFIN", "EMBY"])
@pytest.mark.parametrize("duplicate,conflict,expected", [(False, False, "a"), (True, False, None), (False, True, None)])
def test_fallback_does_not_bypass_conflicts_with_title_search(monkeypatch, provider, duplicate, conflict, expected):
    adapter = SimpleNamespace(path_matching=True, cfg=SimpleNamespace(strict_id_matching=False))
    rows = [("a", movie(ids={"tmdb": "2"} if conflict else {}))]
    if duplicate:
        rows.append(("b", movie()))
    monkeypatch.setattr(lookup, "_catalog", lambda *args: PathIndex(rows))

    @lookup.path_fallback(provider)
    def resolve(ad, item, **kwargs):
        return None if ad.cfg.strict_id_matching else "title-hit"

    assert resolve(adapter, movie(ids={"tmdb": "1"})) == expected


@pytest.mark.parametrize("provider", ["JELLYFIN", "EMBY"])
def test_catalog_is_scoped_and_indexes_unwatched_items(monkeypatch, provider):
    common = __import__(f"providers.sync.{provider.lower()}._common", fromlist=["normalize"])
    select = "jf_selected_library_ids" if provider == "JELLYFIN" else "emby_selected_library_ids"
    monkeypatch.setattr(common, select, lambda cfg, feature: {"selected"})
    calls = []

    def get(route, params):
        calls.append((route, params))
        return SimpleNamespace(status_code=200, json=lambda: {"Items": [{
            "Id": "native", "Type": "Movie", "Name": "Alien", "ProviderIds": {},
            "Path": "/other/Alien (1979)/Alien.mkv", "UserData": {"Played": False},
        }]})

    adapter = SimpleNamespace(path_matching=True, cfg=SimpleNamespace(user_id="user"), client=SimpleNamespace(get=get))
    assert lookup.resolve_path(adapter, movie(), provider, "history") == ("native", "path_match")
    assert calls[0][1]["ParentId"] == "selected"
    assert "Filters" not in calls[0][1]
    assert lookup.resolve_path(adapter, movie(), provider, "history")[0] == "native"
    assert len(calls) == 1


def test_snapshot_aliases_require_unique_compatible_paths():
    left = {"plex:1": movie(ids={"plex": "1"})}
    right = {"imdb:tt2": movie(ids={"imdb": "tt2"})}
    assert set(rekey_by_path(right, left)) == {"plex:1"}
    assert PathAliases((left, right), True).tokens(left["plex:1"])
    assert not PathAliases((left, right), False).tokens(left["plex:1"])
    duplicate = {**right, "other": movie()}
    assert rekey_by_path(duplicate, left) == duplicate
    assert not PathAliases((left, duplicate), True).tokens(left["plex:1"])


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("feature", ["history", "ratings", "progress"])
@pytest.mark.parametrize("enabled", [False, True])
def test_pair_planner_recognizes_existing_path_match(config_base, monkeypatch, mode, feature, enabled):
    from cw_platform.orchestrator import Orchestrator
    from test_interactive_sync_features import feature_item
    from test_orchestrator_dry_run_no_side_effects import FakeOps, _cfg, _install

    left = {**feature_item(feature, 1), **movie(ids={"plex": "1"})}
    right = {**feature_item(feature, 1), **movie(ids={"imdb": "tt2"}, title="Other metadata")}
    src = FakeOps("PLEX", {canonical_key(left): left})
    dst = FakeOps("JELLYFIN", {canonical_key(right): right})
    _install(monkeypatch, src, dst, config_base / ".cw_state")
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"PLEX": src, "JELLYFIN": dst})
    for ops in (src, dst):
        ops.features = lambda: {feature: True}
        ops.capabilities = lambda: {"features": {feature: True}, "index_semantics": "present"}
        ops.health = lambda *_a, **_kw: {"ok": True, "status": "ok", "features": {feature: True}}
    cfg = _cfg(False)
    cfg["pairs"][0].update(source="PLEX", target="JELLYFIN", mode=mode, path_matching=enabled,
                           feature=feature, features={feature: {"enable": True, "add": True, "remove": False}})
    for _ in range(2):
        result = Orchestrator(cfg).run()
        assert not result["errors"]
    if enabled:
        assert not src.add_calls and not dst.add_calls
    else:
        assert dst.add_calls


def test_multiple_files_cannot_alias_multiple_destination_records():
    first, second = movie(), movie("/movies/Alien (1979)/Alien-extended.mkv")
    source = {"a": {**first, "media_paths": first["media_paths"] + second["media_paths"]}}
    target = {"b": first, "c": second}
    assert not PathAliases((source, target), True).allowed


@pytest.mark.parametrize("provider", ["PLEX", "JELLYFIN", "EMBY"])
def test_library_only_accepts_path_only_items(monkeypatch, provider):
    adapter = SimpleNamespace(path_matching=True)
    monkeypatch.setattr(lookup, "_catalog", lambda *args: PathIndex([("native", movie())]))
    result = lookup.path_presence(adapter, [movie()], [{"status": "absent"}], provider, "history")
    assert result == [{"status": "present", "item_id": "native", "reason": "path_match"}]
    adapter.path_matching = False
    assert lookup.path_presence(adapter, [movie()], [{"status": "absent"}], provider, "history") == [{"status": "absent"}]


def test_plex_catalog_respects_libraries_and_home_scope(monkeypatch):
    from providers.sync.plex import _common as common

    switches = []
    monkeypatch.setattr(common, "home_scope_enter", lambda ad: (True, True, None, None))
    monkeypatch.setattr(common, "home_scope_exit", lambda ad, switched: switches.append(switched))
    monkeypatch.setattr(common, "plex_feature_library_ids", lambda ad, feature: {"1"})
    monkeypatch.setattr(common, "normalize", lambda obj: movie())
    selected = SimpleNamespace(key=1, type="movie", all=lambda **kwargs: [SimpleNamespace(ratingKey=42)])
    excluded = SimpleNamespace(key=2, type="movie", all=lambda **kwargs: pytest.fail("Excluded library scanned"))
    adapter = SimpleNamespace(path_matching=True, client=SimpleNamespace(server=SimpleNamespace(
        library=SimpleNamespace(sections=lambda: [selected, excluded]))))
    assert lookup.resolve_path(adapter, movie(), "PLEX", "history") == ("42", "path_match")
    assert switches == [True]


def test_failed_catalog_does_not_cache_partial_data(monkeypatch):
    from providers.sync.jellyfin import _common as common

    monkeypatch.setattr(common, "jf_selected_library_ids", lambda cfg, feature: set())
    adapter = SimpleNamespace(path_matching=True, cfg=SimpleNamespace(user_id="u"),
                              client=SimpleNamespace(get=lambda *args, **kwargs: SimpleNamespace(status_code=503)))
    with pytest.raises(RuntimeError, match="path_catalog_http_503"):
        lookup.resolve_path(adapter, movie(), "JELLYFIN", "history")
    assert not adapter._media_path_catalogs


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_path_match_propagates_source_deletion(config_base, monkeypatch, mode):
    from cw_platform.orchestrator import Orchestrator
    from test_orchestrator_dry_run_no_side_effects import FakeOps, _cfg, _install

    left = movie(ids={"plex": "1"}, watched_at="2024-01-01T12:00:00Z")
    right = movie(ids={"imdb": "tt2"}, title="Other metadata", watched_at="2024-01-01T12:00:00Z")
    src = FakeOps("PLEX", {canonical_key(left): left})
    dst = FakeOps("JELLYFIN", {canonical_key(right): right})
    _install(monkeypatch, src, dst, config_base / ".cw_state")
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"PLEX": src, "JELLYFIN": dst})
    for ops in (src, dst):
        ops.features = lambda: {"history": True}
        ops.capabilities = lambda: {"features": {"history": True}, "index_semantics": "present"}
        ops.health = lambda *_a, **_kw: {"ok": True, "status": "ok", "features": {"history": True}}
    removed = []

    def remove(cfg, items, **kwargs):
        batch = list(items)
        removed.extend(batch)
        return {"ok": True, "count": len(batch)}

    dst.remove = remove
    cfg = _cfg(False)
    cfg["sync"].update(enable_remove=True, include_observed_deletes=True, drop_guard=False)
    cfg["pairs"][0].update(source="PLEX", target="JELLYFIN", mode=mode, path_matching=True,
                           feature="history", features={"history": {"enable": True, "add": True, "remove": True}})
    for _ in range(4):
        assert not Orchestrator(cfg).run()["errors"]
        assert not removed
    src.index.clear()
    events = []
    result = Orchestrator(cfg, on_progress=events.append).run()
    assert not result["errors"]
    assert len(removed) == 1, "\n".join(events)


@pytest.mark.parametrize("provider", ["JELLYFIN", "EMBY"])
def test_public_resolver_uses_opt_in_fallback(monkeypatch, provider):
    from importlib import import_module

    common = import_module(f"providers.sync.{provider.lower()}._common")
    adapter = SimpleNamespace(path_matching=True, config={}, cfg=SimpleNamespace(
        user_id="user", server="server", access_token="token", strict_id_matching=True, targeted_lookup=True),
        client=SimpleNamespace(get=lambda *a, **k: SimpleNamespace(status_code=200, json=lambda: {"Items": []})))
    if provider == "JELLYFIN":
        monkeypatch.setattr(common, "_targeted_lookup_item_id", lambda *a, **k: None)
    monkeypatch.setattr(lookup, "_catalog", lambda *args: PathIndex([("native", movie())]))
    assert common.resolve_item_ids(adapter, movie()) == ["native"]


def test_public_plex_history_resolver_uses_opt_in_fallback(monkeypatch):
    from providers.sync.plex import _history

    adapter = SimpleNamespace(path_matching=True, config={"plex": {}}, cfg=SimpleNamespace(strict_id_matching=False),
                              client=SimpleNamespace(server=SimpleNamespace()))
    monkeypatch.setattr(lookup, "_catalog", lambda *args: PathIndex([("42", movie())]))
    assert _history._resolve_rating_key(adapter, movie()) == "42"


def test_baseline_preserves_paths_and_parts(tmp_path):
    from cw_platform.orchestrator._state_store import StateStore

    item = movie(part=2)
    state = {"providers": {"PLEX": {"history": {"baseline": {"items": {canonical_key(item): item}}}}}}
    store = StateStore(tmp_path).for_pair("paths", pair_id="pair")
    store.save_feature_blocks({("PLEX", "default", "history"): state["providers"]["PLEX"]["history"]})
    loaded = store.load_state()["providers"]["PLEX"]["history"]["baseline"]["items"][canonical_key(item)]
    assert loaded["media_paths"] == item["media_paths"]
    assert loaded["part"] == 2


def test_existing_schema_gets_path_columns():
    import sqlite3
    from cw_platform.local_db.schema import apply_schema

    conn = sqlite3.connect(":memory:")
    apply_schema(conn)
    for table in ("baseline_items", "pair_baseline_items"):
        conn.execute(f"ALTER TABLE {table} DROP COLUMN path_metadata_json")
    apply_schema(conn)
    for table in ("baseline_items", "pair_baseline_items"):
        assert "path_metadata_json" in {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    conn.close()


@pytest.mark.parametrize("feature", ["history", "ratings", "progress"])
@pytest.mark.parametrize("change", ["none", "disabled", "ambiguous"])
@pytest.mark.parametrize("kind", ["movie", "episode", "multipart"])
def test_two_way_path_baselines_stay_native_without_phantom_deletes(config_base, monkeypatch, feature, change, kind):
    from cw_platform.orchestrator import Orchestrator
    from cw_platform.orchestrator._state_store import StateStore
    from cw_platform.pair_scope import pair_feature_scope
    from test_interactive_sync_features import feature_item
    from test_orchestrator_dry_run_no_side_effects import FakeOps, _cfg, _install

    left = {**feature_item(feature, 1), **movie(ids={"plex": "1"})}
    right = {**feature_item(feature, 1), **movie(ids={"imdb": "tt2"}, title="Other metadata")}
    if kind == "episode":
        for item in (left, right):
            item.update(type="episode", season=1, episode=2, media_paths=["/tv/Show/Season 1/Episode 2.mkv"])
    elif kind == "multipart":
        left["part"] = right["part"] = 2
    src = FakeOps("PLEX", {canonical_key(left): left})
    dst = FakeOps("JELLYFIN", {canonical_key(right): right})
    _install(monkeypatch, src, dst, config_base / ".cw_state")
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"PLEX": src, "JELLYFIN": dst})
    removed = []
    for ops in (src, dst):
        ops.features = lambda: {feature: True}
        ops.capabilities = lambda: {"features": {feature: True}, "index_semantics": "present"}
        ops.health = lambda *_a, **_kw: {"ok": True, "status": "ok", "features": {feature: True}}
        ops.remove = lambda cfg, items, **kwargs: removed.extend(items) or {"ok": True, "count": 0}
    cfg = _cfg(False)
    cfg["sync"].update(enable_remove=True, include_observed_deletes=True, drop_guard=False)
    pair = cfg["pairs"][0]
    pair.update(source="PLEX", target="JELLYFIN", mode="two-way", path_matching=True, feature=feature,
                features={feature: {"enable": True, "add": True, "remove": True}})
    store = StateStore(config_base).for_pair(pair_feature_scope(cfg, pair, feature))
    for _ in range(4):
        assert not Orchestrator(cfg).run()["errors"]
        baseline = store.load_state()["providers"]["JELLYFIN"][feature]["baseline"]["items"]
        assert set(baseline) == {canonical_key(right)}
        assert not (store.load_tomb() or {}).get("keys")
        assert not removed and not src.add_calls and not dst.add_calls
    if change == "disabled":
        pair["path_matching"] = False
    elif change == "ambiguous":
        duplicate = {**right, "ids": {"imdb": "tt3"}}
        dst.index[canonical_key(duplicate)] = duplicate
    for _ in range(2):
        assert not Orchestrator(cfg).run()["errors"]
        assert not removed
        assert not (store.load_tomb() or {}).get("keys")
        if feature == "progress":
            assert all(item.get("progress_ms", 1) != 0 for batch in src.add_calls + dst.add_calls for item in batch)


@pytest.mark.parametrize("provider", ["PLEX", "JELLYFIN", "EMBY"])
def test_disabled_matching_strips_snapshot_paths_without_mutating_cache(monkeypatch, provider):
    item = movie(ids={provider.lower(): "1"})
    index = {canonical_key(item): item}
    monkeypatch.setattr(lookup, "_catalog", lambda *args: pytest.fail("Disabled matching scanned a catalog"))
    stripped = lookup.enrich_snapshot(SimpleNamespace(path_matching=False), index, provider, "history")
    assert "media_paths" not in next(iter(stripped.values()))
    assert item["media_paths"]
    enabled = lookup.enrich_snapshot(SimpleNamespace(path_matching=True), index, provider, "history")
    assert next(iter(enabled.values()))["media_paths"] == item["media_paths"]
