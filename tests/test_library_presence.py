# tests/test_library_presence.py
# CrossWatch - Destination Library Presence Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from cw_platform import library_presence as lp
from cw_platform.id_map import canonical_key
from cw_platform.orchestrator._interactive import InteractivePlan
from services.interactive_sync_store import ReviewStore
from test_interactive_sync import item, run, setup_ops
from test_orchestrator_dry_run_no_side_effects import _cfg

WATCHED_AT = "2024-01-01T12:00:00Z"
SHOW_IDS = {"tmdb": "500", "tvdb": "600"}


def movie(number):
    return item(number, watched_at=WATCHED_AT)


def episode(season, number, show_ids=None):
    return dict(type="episode", title=f"S{season}E{number}", series_title="Show", season=season, episode=number,
                ids={}, show_ids=dict(show_ids or SHOW_IDS), watched_at=WATCHED_AT)


def history_setup(config_base, monkeypatch, source, target, *, mode="one-way", options=None):
    src, dst = setup_ops(config_base, monkeypatch, source, target)
    for ops in (src, dst):
        ops.features = lambda: {"history": True}
        ops.capabilities = lambda: {"features": {"history": True}, "index_semantics": "present"}
        ops.health = lambda *_a, **_k: {"ok": True, "status": "ok", "features": {"history": True}}
    cfg = _cfg(False)
    feature = {"enable": True, "add": True, "remove": False}
    feature.update(options or {})
    cfg["pairs"][0].update(mode=mode, feature="history", features={"history": feature})
    return cfg, src, dst


def enable_presence(ops, absent_keys, *, error=None):
    calls = []

    def presence(cfg, items, *, feature):
        rows = list(items)
        calls.append(rows)
        if error is not None:
            raise error
        return [lp.verdict(lp.ABSENT, reason=lp.REASON) if canonical_key(row) in absent_keys
                else lp.verdict(lp.PRESENT, item_id="1") for row in rows]

    ops.library_presence = presence
    ops.capabilities = lambda: {"features": {"history": True}, "index_semantics": "present",
                                "library_presence": {"features": ["history"]}}
    return calls


def test_split_absent_only_removes_absent_items():
    items = ["a", "b", "c"]
    ops = SimpleNamespace(library_presence=lambda cfg, rows, *, feature: [
        lp.verdict(lp.PRESENT, item_id=5), lp.verdict(lp.ABSENT), lp.unknown("ambiguous")])
    kept, skipped, error = lp.split_absent(ops, {}, "history", items)
    assert kept == ["a", "c"]
    assert skipped == [("b", lp.REASON)]
    assert error == ""


@pytest.mark.parametrize("result", [RuntimeError("down"), [lp.verdict(lp.ABSENT)], "absent", None])
def test_split_absent_fails_open(result):
    def presence(cfg, rows, *, feature):
        if isinstance(result, Exception):
            raise result
        return result

    kept, skipped, error = lp.split_absent(SimpleNamespace(library_presence=presence), {}, "history", ["a", "b"])
    assert kept == ["a", "b"]
    assert skipped == []
    assert error


def test_checkable_requires_usable_ids():
    assert lp.checkable(movie(1))
    assert not lp.checkable(dict(type="movie", title="No ids", ids={}))
    assert not lp.checkable(dict(type="movie", ids={"tmdb": "1", "jellyfin": "abc"}), native_keys=("jellyfin",))
    assert lp.checkable(episode(1, 1))
    assert not lp.checkable(dict(type="episode", season=1, episode=1, ids={"tvdb": "9"}))
    assert not lp.checkable(dict(type="episode", season=None, episode=1, show_ids=SHOW_IDS))


def test_requested_and_supported_follow_feature_and_capability():
    ops = SimpleNamespace(library_presence=lambda *a, **k: [], capabilities=lambda: {"library_presence": {"features": ["history"]}})
    assert lp.requested("history", {lp.OPTION: True})
    assert lp.requested("ratings", {lp.OPTION: True})
    assert lp.requested("progress", {lp.OPTION: True})
    assert lp.requested("watchlist", {lp.OPTION: True})
    assert not lp.requested("collection", {lp.OPTION: True})
    assert not lp.requested("playlists", {lp.OPTION: True})
    assert not lp.requested("history", {})
    assert lp.supported(ops, "history")
    assert not lp.supported(ops, "ratings")
    assert not lp.supported(SimpleNamespace(capabilities=lambda: {"library_presence": {"features": ["history"]}}), "history")


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_option_off_keeps_the_normal_plan(config_base, monkeypatch, mode):
    cfg, src, dst = history_setup(config_base, monkeypatch, [movie(1), movie(2)], [], mode=mode)
    calls = enable_presence(dst, {canonical_key(movie(2))})
    plan = InteractivePlan()
    run(cfg, plan)
    assert not calls
    assert sorted(row["result"] for row in plan.rows.values()) == ["add", "add"]


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_option_on_skips_absent_items_before_review(config_base, monkeypatch, mode):
    cfg, src, dst = history_setup(config_base, monkeypatch, [movie(1), movie(2)], [], mode=mode,
                                  options={lp.OPTION: True})
    calls = enable_presence(dst, {canonical_key(movie(2))})
    plan = InteractivePlan()
    run(cfg, plan)
    assert len(calls) == 1 and len(calls[0]) == 2
    by_key = {row["key"]: row for row in plan.rows.values()}
    assert by_key[canonical_key(movie(1))]["result"] == "add"
    hidden = by_key[canonical_key(movie(2))]
    assert hidden["result"] == lp.REASON
    assert hidden["selectable"] is False
    assert hidden["provider"] == "DST"
    assert not dst.add_calls


def test_absent_items_are_not_written_or_recorded_as_failures(config_base, monkeypatch):
    cfg, src, dst = history_setup(config_base, monkeypatch, [movie(1), movie(2)], [], options={lp.OPTION: True})
    enable_presence(dst, {canonical_key(movie(2))})
    from cw_platform.orchestrator import Orchestrator

    Orchestrator(cfg).run(dry_run=False, pair_scope_ids=["p1"], write_state_json=False)
    written = [canonical_key(row) for batch in dst.add_calls for row in batch]
    assert written == [canonical_key(movie(1))]
    state_dir = config_base / ".cw_state"
    recorded = "".join(path.read_text(encoding="utf-8") for path in state_dir.rglob("*.json")
                       if "unresolved" in path.name or "blackbox" in path.name or "flap" in path.name)
    assert canonical_key(movie(2)) not in recorded


def test_presence_failure_leaves_every_add_in_the_plan(config_base, monkeypatch):
    cfg, src, dst = history_setup(config_base, monkeypatch, [movie(1), movie(2)], [], options={lp.OPTION: True})
    calls = enable_presence(dst, {canonical_key(movie(2))}, error=RuntimeError("library unavailable"))
    plan = InteractivePlan()
    run(cfg, plan)
    assert len(calls) == 1
    assert sorted(row["result"] for row in plan.rows.values()) == ["add", "add"]


def test_two_way_only_checks_the_side_that_supports_it(config_base, monkeypatch):
    cfg, src, dst = history_setup(config_base, monkeypatch, [movie(1)], [movie(2)], mode="two-way",
                                  options={lp.OPTION: True})
    calls = enable_presence(dst, {canonical_key(movie(1))})
    plan = InteractivePlan()
    run(cfg, plan)
    assert [[canonical_key(row) for row in batch] for batch in calls] == [[canonical_key(movie(1))]]
    by_key = {row["key"]: row for row in plan.rows.values()}
    assert by_key[canonical_key(movie(1))]["result"] == lp.REASON
    assert by_key[canonical_key(movie(2))]["result"] == "add"
    assert by_key[canonical_key(movie(2))]["provider"] == "SRC"


@pytest.mark.parametrize("include_specials, expected", [(True, {0, 1}), (False, {1})])
def test_presence_runs_after_the_specials_option(config_base, monkeypatch, include_specials, expected):
    cfg, src, dst = history_setup(config_base, monkeypatch, [episode(0, 1), episode(1, 1)], [],
                                  options={lp.OPTION: True, "include_specials": include_specials})
    calls = enable_presence(dst, set())
    run(cfg, InteractivePlan())
    assert {row["season"] for batch in calls for row in batch} == expected


def test_review_store_hides_not_in_library_rows_by_default():
    store = ReviewStore()
    base = dict(feature="history", provider="DST", instance="default", operation="add", key="k")
    store.rows["a"] = dict(base, id="a", item=movie(1), result="add", selectable=True)
    store.rows["b"] = dict(base, id="b", item=movie(2), result=lp.REASON, selectable=False)
    store.finish()
    assert store.counts["changes"] == 1
    assert store.counts["not_in_library"] == 1
    assert store.counts["selected"] == 1
    default: dict[str, Any] = store.page()
    assert [row["id"] for row in default["items"]] == ["a"]
    assert default["total"] == 1
    hidden: dict[str, Any] = store.page(result=lp.REASON)
    assert [row["id"] for row in hidden["items"]] == ["b"]
    assert hidden["selectable"] == 0
    store.select(True)
    assert store.counts["selected"] == 1


def test_kodi_presence_uses_the_library_index(monkeypatch):
    from providers.sync.kodi import _presence

    found = {canonical_key(movie(1)): ({"_kodi_id": 7}, "ok"), canonical_key(movie(3)): (None, "ambiguous")}
    index = SimpleNamespace(resolve=lambda row: found.get(canonical_key(row), (None, "not_found")))
    monkeypatch.setattr(_presence, "library_index", lambda adapter, feature: index)
    rows = [movie(1), movie(2), movie(3), dict(type="movie", title="No ids", ids={}), dict(movie(4), _kodi_id=9)]
    result = _presence.presence(SimpleNamespace(), rows)
    assert [row["status"] for row in result] == [lp.PRESENT, lp.ABSENT, lp.UNKNOWN, lp.UNKNOWN, lp.UNKNOWN]
    assert result[0]["item_id"] == "7"


def test_kodi_presence_fails_open_when_the_index_is_unavailable(monkeypatch):
    from providers.sync.kodi import _presence

    def broken(adapter, feature):
        raise RuntimeError("kodi offline")

    monkeypatch.setattr(_presence, "library_index", broken)
    assert [row["status"] for row in _presence.presence(SimpleNamespace(), [movie(1), movie(2)])] == [lp.UNKNOWN, lp.UNKNOWN]


def plex_catalog(history):
    return history.build_catalog_from_entries([
        {"rk": "10", "type": "movie", "ids": {"imdb": "tt0000001"}, "watched": False},
        {"rk": "20", "type": "show", "ids": dict(SHOW_IDS), "watched": False},
    ])


def plex_setup(monkeypatch, catalog, leaves=None):
    from providers.sync.plex import _history as history
    from providers.sync.plex import _presence

    loads = []

    def populate(adapter, allow, cat):
        loads.append(1)
        for entry in leaves or []:
            cat.add(entry)
        return len(leaves or [])

    monkeypatch.setattr(_presence, "home_scope_enter", lambda adapter: (False, False, None, None))
    monkeypatch.setattr(_presence, "home_scope_exit", lambda adapter, switched: None)
    monkeypatch.setattr(_presence, "plex_feature_library_ids", lambda adapter, feature: set())
    monkeypatch.setattr(history, "_get_history_catalog", lambda adapter, allow, **kwargs: catalog)
    monkeypatch.setattr(history, "_populate_catalog_episode_leaves", populate)
    monkeypatch.setattr(history, "_store_history_catalog", lambda adapter, allow, cat: None)
    return _presence, loads


def test_plex_presence_checks_movies_and_episodes(monkeypatch):
    from providers.sync.plex import _history as history

    leaf = {"rk": "21", "type": "episode", "ids": {}, "show_ids": dict(SHOW_IDS), "show_rk": "20",
            "season": 1, "episode": 1, "watched": False}
    presence, loads = plex_setup(monkeypatch, plex_catalog(history), [leaf])
    adapter = SimpleNamespace(client=SimpleNamespace(server=object()))
    rows = [movie(1), movie(2), episode(1, 1), episode(1, 2), episode(1, 3), episode(1, 1, {"tvdb": "999"}),
            dict(type="movie", title="No ids", ids={}),
            dict(type="episode", season=1, episode=1, ids={"tvdb": "5"}, watched_at=WATCHED_AT)]
    result = presence.presence(adapter, rows)
    assert [row["status"] for row in result] == [
        lp.PRESENT, lp.ABSENT, lp.PRESENT, lp.ABSENT, lp.ABSENT, lp.ABSENT, lp.UNKNOWN, lp.UNKNOWN]
    assert result[0]["item_id"] == "10"
    assert result[2]["item_id"] == "21"
    assert loads == [1]


def test_plex_presence_fails_open(monkeypatch):
    from providers.sync.plex import _history as history

    presence, _loads = plex_setup(monkeypatch, plex_catalog(history))

    def broken(adapter, allow, **kwargs):
        raise RuntimeError("plex_guid_index_request_failed")

    monkeypatch.setattr(history, "_get_history_catalog", broken)
    adapter = SimpleNamespace(client=SimpleNamespace(server=object()))
    assert [row["status"] for row in presence.presence(adapter, [movie(1), movie(2)])] == [lp.UNKNOWN, lp.UNKNOWN]
    no_server = SimpleNamespace(client=SimpleNamespace(server=None))
    assert [row["status"] for row in presence.presence(no_server, [movie(1)])] == [lp.UNKNOWN]


def jellyfin_adapter(**overrides):
    from providers.sync._mod_JELLYFIN import JFConfig

    return SimpleNamespace(cfg=JFConfig(server="http://jf", access_token="token", user_id="user", **overrides),
                           client=None, config={})


def test_jellyfin_presence_resolves_ids_only(monkeypatch):
    from providers.sync.jellyfin import _common as common
    from providers.sync.jellyfin import _id_lookup, _presence

    seen = []

    def resolve(adapter, want, *, feature="history"):
        seen.append((adapter.cfg.strict_id_matching, want.get("type"), feature))
        return "jf-1" if (want.get("ids") or {}).get("imdb") == "tt0000001" or want.get("episode") == 1 else None

    monkeypatch.setattr(_presence, "_index_ready", lambda adapter, feature: True)
    monkeypatch.setattr(_id_lookup, "prepare", lambda adapter, feature, items: None)
    monkeypatch.setattr(common, "resolve_item_id", resolve)
    adapter = jellyfin_adapter()
    rows = [movie(1), movie(2), episode(1, 1), episode(1, 2), dict(type="movie", title="No ids", ids={}),
            dict(type="episode", season=1, episode=1, ids={"tvdb": "5"}),
            dict(movie(3), jellyfin_item_id="abc")]
    result = _presence.presence(adapter, rows)
    assert [row["status"] for row in result] == [
        lp.PRESENT, lp.ABSENT, lp.PRESENT, lp.ABSENT, lp.UNKNOWN, lp.UNKNOWN, lp.UNKNOWN]
    assert result[0]["item_id"] == "jf-1"
    assert len(seen) == 4
    assert all(strict for strict, _kind, _feature in seen)
    assert adapter.cfg.strict_id_matching is False


def test_jellyfin_presence_fails_open_without_an_index(monkeypatch):
    from providers.sync.jellyfin import _common as common
    from providers.sync.jellyfin import _presence

    def never(adapter, want, *, feature="history"):
        raise AssertionError("resolver must not run without an index")

    monkeypatch.setattr(common, "resolve_item_id", never)
    monkeypatch.setattr(_presence, "_index_ready", lambda adapter, feature: False)
    assert [row["status"] for row in _presence.presence(jellyfin_adapter(), [movie(1), movie(2)])] == [lp.UNKNOWN, lp.UNKNOWN]

    def broken(adapter, feature):
        raise RuntimeError("identity_index_http_500")

    monkeypatch.setattr(_presence, "_index_ready", broken)
    assert [row["status"] for row in _presence.presence(jellyfin_adapter(), [movie(1)])] == [lp.UNKNOWN]


def emby_adapter():
    from providers.sync._mod_EMBY import EMBYConfig

    return SimpleNamespace(cfg=EMBYConfig(server="http://emby", access_token="token", user_id="user"),
                           client=object(), config={})


def emby_library(monkeypatch, *, fail=False):
    from providers.sync.emby import _common as common

    library = [
        {"Id": "m1", "Type": "Movie", "ProductionYear": 2000, "ProviderIds": {"Imdb": "tt0000001"}},
        {"Id": "s1", "Type": "Series", "ProviderIds": {"Tmdb": "500", "Tvdb": "600"}},
    ]
    queries = []

    def direct(http, uid, pairs, include_types, scope):
        queries.append((tuple(pairs), include_types))
        if fail:
            return None
        wanted, types = set(pairs), set(include_types.split(","))
        out = []
        for row in library:
            row_pairs = {common.format_provider_pair(k, v) for k, v in common._ids_from_provider_ids(row["ProviderIds"]).items()}
            if row["Type"] in types and wanted & row_pairs:
                out.append(row)
        return out

    def episodes(http, uid, sid, start=0, limit=500):
        rows = [{"Id": "e1", "Type": "Episode", "SeriesId": "s1", "ParentIndexNumber": 1, "IndexNumber": 1}]
        return {"Items": rows if start == 0 else [], "TotalRecordCount": len(rows)}

    monkeypatch.setattr(common, "_direct_query_by_pairs", direct)
    monkeypatch.setattr(common, "get_series_episodes", episodes)
    return queries


def test_emby_presence_batches_id_queries(monkeypatch):
    from providers.sync.emby import _presence

    queries = emby_library(monkeypatch)
    rows = [movie(n) for n in range(1, 61)] + [episode(1, 1), episode(1, 2), episode(1, 1, {"tvdb": "999"}),
                                               dict(type="movie", title="No ids", ids={})]
    result = _presence.presence(emby_adapter(), rows)
    statuses = [row["status"] for row in result]
    assert statuses[0] == lp.PRESENT and result[0]["item_id"] == "m1"
    assert statuses[1:60] == [lp.ABSENT] * 59
    assert statuses[60:] == [lp.PRESENT, lp.ABSENT, lp.ABSENT, lp.UNKNOWN]
    assert result[60]["item_id"] == "e1"
    assert sorted(types for _pairs, types in queries) == ["Movie", "Series"]
    assert len(next(pairs for pairs, types in queries if types == "Movie")) == 60


def test_emby_presence_fails_open_when_the_id_query_fails(monkeypatch):
    from providers.sync.emby import _presence

    emby_library(monkeypatch, fail=True)
    assert [row["status"] for row in _presence.presence(emby_adapter(), [movie(1), movie(2)])] == [lp.UNKNOWN, lp.UNKNOWN]


def test_quiet_log_drops_debug_lines_only_inside_the_block(monkeypatch, capsys):
    from providers.sync import _log

    monkeypatch.setenv("CW_LOG_LEVEL", "debug")
    monkeypatch.setattr(_log, "_append_ui_log", lambda provider, line: None)
    _log.log("JELLYFIN", "common", "debug", "before")
    with _log.quiet():
        _log.log("JELLYFIN", "common", "debug", "hidden")
        _log.log("JELLYFIN", "common", "info", "shown")
    _log.log("JELLYFIN", "common", "debug", "after")
    out = capsys.readouterr().out
    assert "before" in out and "shown" in out and "after" in out
    assert "hidden" not in out


def test_jellyfin_presence_logs_one_summary_instead_of_per_item_lines(monkeypatch, capsys):
    from providers.sync import _log
    from providers.sync.jellyfin import _common as common
    from providers.sync.jellyfin import _id_lookup, _presence

    def resolve(adapter, want, *, feature="history"):
        common._dbg("resolve_miss", kind=want.get("type"))
        return None

    monkeypatch.setenv("CW_LOG_LEVEL", "debug")
    monkeypatch.setattr(_log, "_append_ui_log", lambda provider, line: None)
    monkeypatch.setattr(_presence, "_index_ready", lambda adapter, feature: True)
    monkeypatch.setattr(_id_lookup, "prepare", lambda adapter, feature, items: None)
    monkeypatch.setattr(common, "resolve_item_id", resolve)
    result = _presence.presence(jellyfin_adapter(), [movie(n) for n in range(1, 51)])
    out = capsys.readouterr().out
    assert [row["status"] for row in result] == [lp.ABSENT] * 50
    assert "resolve_miss" not in out
    assert out.count("presence_done") == 1


def test_plex_presence_loads_episodes_per_show_when_few_shows_are_involved(monkeypatch):
    from providers.sync.plex import _history as history

    leaf = {"rk": "21", "type": "episode", "ids": {}, "show_ids": dict(SHOW_IDS), "show_rk": "20",
            "season": 1, "episode": 1, "watched": False}
    presence, library_loads = plex_setup(monkeypatch, plex_catalog(history))
    show_loads = []

    def load_shows(adapter, cat, show_rks):
        show_loads.append(sorted(show_rks))
        cat.add(leaf)
        return 1

    monkeypatch.setattr(presence, "_show_copies", lambda adapter, allow: {"tmdb:500": {"20"}, "tvdb:600": {"20"}})
    monkeypatch.setattr(presence, "_load_show_leaves", load_shows)
    adapter = SimpleNamespace(client=SimpleNamespace(server=object()))
    result = presence.presence(adapter, [episode(1, 1), episode(1, 2)])
    assert [row["status"] for row in result] == [lp.PRESENT, lp.ABSENT]
    assert show_loads == [["20"]]
    assert library_loads == []


def test_plex_presence_falls_back_to_the_library_listing(monkeypatch):
    from providers.sync.plex import _history as history

    leaf = {"rk": "21", "type": "episode", "ids": {}, "show_ids": dict(SHOW_IDS), "show_rk": "20",
            "season": 1, "episode": 1, "watched": False}
    presence, library_loads = plex_setup(monkeypatch, plex_catalog(history), [leaf])
    monkeypatch.setattr(presence, "_show_copies", lambda adapter, allow: {"tmdb:500": {"20"}})
    monkeypatch.setattr(presence, "_load_show_leaves", lambda adapter, cat, show_rks: None)
    adapter = SimpleNamespace(client=SimpleNamespace(server=object()))
    result = presence.presence(adapter, [episode(1, 1), episode(1, 2)])
    assert [row["status"] for row in result] == [lp.PRESENT, lp.ABSENT]
    assert library_loads == [1]


def rated(number, rating, rated_at="2024-01-01T12:00:00Z"):
    return item(number, rating=rating, rated_at=rated_at)


def ratings_setup(config_base, monkeypatch, source, target, *, mode="one-way", options=None):
    src, dst = setup_ops(config_base, monkeypatch, source, target)
    for ops in (src, dst):
        ops.features = lambda: {"ratings": True}
        ops.capabilities = lambda: {"features": {"ratings": True}, "index_semantics": "present"}
        ops.health = lambda *_a, **_k: {"ok": True, "status": "ok", "features": {"ratings": True}}
    cfg = _cfg(False)
    feature = {"enable": True, "add": True, "remove": False, "types": ["movies", "shows"], "mode": "all"}
    feature.update(options or {})
    cfg["pairs"][0].update(mode=mode, feature="ratings", features={"ratings": feature})
    return cfg, src, dst


def enable_ratings_presence(ops, absent_keys):
    calls = []

    def presence(cfg, items, *, feature):
        rows = list(items)
        calls.append((feature, sorted(canonical_key(row) for row in rows)))
        return [lp.verdict(lp.ABSENT, reason=lp.REASON) if canonical_key(row) in absent_keys
                else lp.verdict(lp.PRESENT, item_id="1") for row in rows]

    ops.library_presence = presence
    ops.capabilities = lambda: {"features": {"ratings": True}, "index_semantics": "present",
                                "library_presence": {"features": ["history", "ratings"]}}
    return calls


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_ratings_option_skips_new_ratings_but_keeps_rating_changes(config_base, monkeypatch, mode):
    changed = rated(1, 7, "2024-06-01T12:00:00Z")
    source = [changed, rated(2, 8), rated(3, 9)]
    target = [rated(1, 6, "2024-01-01T12:00:00Z")]
    cfg, src, dst = ratings_setup(config_base, monkeypatch, source, target, mode=mode, options={lp.OPTION: True})
    every_key = {canonical_key(row) for row in source}
    calls = enable_ratings_presence(dst, every_key)
    plan = InteractivePlan()
    run(cfg, plan)
    assert calls == [("ratings", sorted([canonical_key(rated(2, 8)), canonical_key(rated(3, 9))]))]
    by_key = {row["key"]: row for row in plan.rows.values()}
    assert by_key[canonical_key(changed)]["result"] == "update"
    assert by_key[canonical_key(changed)]["item"]["rating"] == 7
    assert by_key[canonical_key(rated(2, 8))]["result"] == lp.REASON
    assert by_key[canonical_key(rated(3, 9))]["result"] == lp.REASON


def test_ratings_update_is_written_when_every_new_rating_is_skipped(config_base, monkeypatch):
    from cw_platform.orchestrator import Orchestrator

    changed = rated(1, 7, "2024-06-01T12:00:00Z")
    source = [changed, rated(2, 8), rated(3, 9)]
    cfg, src, dst = ratings_setup(config_base, monkeypatch, source, [rated(1, 6)], options={lp.OPTION: True})
    enable_ratings_presence(dst, {canonical_key(rated(2, 8))})
    Orchestrator(cfg).run(dry_run=False, pair_scope_ids=["p1"], write_state_json=False)
    written = {canonical_key(row): row.get("rating") for batch in dst.add_calls for row in batch}
    assert written == {canonical_key(changed): 7, canonical_key(rated(3, 9)): 9}


def test_ratings_option_off_keeps_the_normal_plan(config_base, monkeypatch):
    cfg, src, dst = ratings_setup(config_base, monkeypatch, [rated(1, 7), rated(2, 8)], [])
    calls = enable_ratings_presence(dst, {canonical_key(rated(2, 8))})
    plan = InteractivePlan()
    run(cfg, plan)
    assert not calls
    assert sorted(row["result"] for row in plan.rows.values()) == ["add", "add"]


def test_ratings_option_is_ignored_when_the_server_only_supports_history(config_base, monkeypatch):
    cfg, src, dst = ratings_setup(config_base, monkeypatch, [rated(1, 7)], [], options={lp.OPTION: True})
    calls = enable_ratings_presence(dst, {canonical_key(rated(1, 7))})
    dst.capabilities = lambda: {"features": {"ratings": True}, "index_semantics": "present",
                                "library_presence": {"features": ["history"]}}
    plan = InteractivePlan()
    run(cfg, plan)
    assert not calls
    assert [row["result"] for row in plan.rows.values()] == ["add"]


def test_kodi_presence_works_for_ratings(monkeypatch):
    from providers.sync.kodi import _presence

    seen = []
    found = {canonical_key(movie(1)): ({"_kodi_id": 7}, "ok")}
    index = SimpleNamespace(resolve=lambda row: found.get(canonical_key(row), (None, "not_found")))

    def library_index(adapter, feature):
        seen.append(feature)
        return index

    monkeypatch.setattr(_presence, "library_index", library_index)
    result = _presence.presence(SimpleNamespace(), [rated(1, 7), rated(2, 8)], feature="ratings")
    assert [row["status"] for row in result] == [lp.PRESENT, lp.ABSENT]
    assert seen == ["ratings"]


def plex_ratings_setup(monkeypatch, index, coordinates=None, *, shared=False):
    from providers.sync.plex import _history as history
    from providers.sync.plex import _presence, _ratings

    loads = []

    def find(kind, guids):
        table = index.get("shows" if kind == "show" else "movies", {})
        return next((table[guid] for guid in guids if guid in table), None)

    def show_coordinates(adapter, allow, show_rks):
        loads.append(sorted(show_rks))
        return coordinates

    monkeypatch.setattr(_presence, "home_scope_enter", lambda adapter: (False, False, None, None))
    monkeypatch.setattr(_presence, "home_scope_exit", lambda adapter, switched: None)
    monkeypatch.setattr(_presence, "plex_feature_library_ids", lambda adapter, feature: set())
    monkeypatch.setattr(_ratings, "_shared_user_scope_active", lambda adapter: shared)
    monkeypatch.setattr(history, "_build_guid_index", lambda adapter, allow, **kwargs: index)
    monkeypatch.setattr(history, "_pms_find_in_guid_index", find)
    monkeypatch.setattr(_presence, "_show_coordinates", show_coordinates)
    return _presence, loads


def test_plex_ratings_presence_checks_movies_shows_seasons_and_episodes(monkeypatch):
    index = {"movies": {"imdb://tt0000001": "10"}, "shows": {"tmdb://500": "20", "tvdb://600": "20"}}
    presence, loads = plex_ratings_setup(monkeypatch, index, {"20": {(1, 1), (1, 2)}})
    adapter = SimpleNamespace(client=SimpleNamespace(server=object()))
    show = dict(type="show", title="Show", ids=dict(SHOW_IDS), rating=8)
    season = lambda number: dict(type="season", season=number, ids={}, show_ids=dict(SHOW_IDS), rating=8)
    rows = [rated(1, 7), rated(2, 8), show, dict(show, ids={"tmdb": "999"}), dict(episode(1, 1), rating=9),
            dict(episode(1, 5), rating=9), season(1), season(4), dict(episode(1, 1, {"tvdb": "999"}), rating=9),
            dict(type="movie", title="No ids", ids={}, rating=5),
            dict(type="episode", season=1, episode=1, ids={"tvdb": "5"}, rating=5),
            dict(rated(3, 6), ids={"imdb": "tt0000003", "plex": "77"})]
    result = presence.presence(adapter, rows, feature="ratings")
    assert [row["status"] for row in result] == [
        lp.PRESENT, lp.ABSENT, lp.PRESENT, lp.ABSENT, lp.PRESENT, lp.ABSENT, lp.PRESENT, lp.ABSENT, lp.ABSENT,
        lp.UNKNOWN, lp.UNKNOWN, lp.UNKNOWN]
    assert result[0]["item_id"] == "10"
    assert loads == [["20"]]


def test_plex_ratings_presence_fails_open(monkeypatch):
    index = {"movies": {"imdb://tt0000001": "10"}, "shows": {"tmdb://500": "20"}}
    adapter = SimpleNamespace(client=SimpleNamespace(server=object()))
    presence, _loads = plex_ratings_setup(monkeypatch, index, None)
    result = presence.presence(adapter, [rated(1, 7), dict(episode(1, 1), rating=9)], feature="ratings")
    assert [row["status"] for row in result] == [lp.PRESENT, lp.UNKNOWN]
    presence, _loads = plex_ratings_setup(monkeypatch, index, {}, shared=True)
    result = presence.presence(adapter, [rated(1, 7), rated(2, 8)], feature="ratings")
    assert [row["status"] for row in result] == [lp.UNKNOWN, lp.UNKNOWN]


def resumed(number, percent, at="2024-01-01T12:00:00Z"):
    return item(number, progress_percent=percent, progress_ms=percent * 6000, duration_ms=600000, progress_at=at)


def progress_setup(config_base, monkeypatch, source, target, *, mode="one-way", options=None):
    src, dst = setup_ops(config_base, monkeypatch, source, target)
    for ops in (src, dst):
        ops.features = lambda: {"progress": True}
        ops.capabilities = lambda: {"features": {"progress": True}, "index_semantics": "present"}
        ops.health = lambda *_a, **_k: {"ok": True, "status": "ok", "features": {"progress": True}}
    cfg = _cfg(False)
    feature = {"enable": True, "add": True, "remove": False, "mode": "all"}
    feature.update(options or {})
    cfg["pairs"][0].update(mode=mode, feature="progress", features={"progress": feature})
    return cfg, src, dst


def enable_progress_presence(ops, absent_keys):
    calls = []

    def presence(cfg, items, *, feature):
        rows = list(items)
        calls.append((feature, sorted(canonical_key(row) for row in rows)))
        return [lp.verdict(lp.ABSENT, reason=lp.REASON) if canonical_key(row) in absent_keys
                else lp.verdict(lp.PRESENT, item_id="1") for row in rows]

    ops.library_presence = presence
    ops.capabilities = lambda: {"features": {"progress": True}, "index_semantics": "present",
                                "library_presence": {"features": ["history", "progress"]}}
    return calls


def test_split_absent_never_checks_items_the_destination_already_tracks():
    seen = []

    def presence(cfg, rows, *, feature):
        seen.append(list(rows))
        return [lp.verdict(lp.ABSENT) for _ in rows]

    ops = SimpleNamespace(library_presence=presence)
    kept, skipped, error = lp.split_absent(ops, {}, "progress", ["a", "b", "c"], known=lambda value: value == "b")
    assert seen == [["a", "c"]]
    assert kept == ["b"]
    assert [value for value, _reason in skipped] == ["a", "c"]
    assert error == ""
    kept, skipped, error = lp.split_absent(ops, {}, "progress", ["b"], known=lambda value: True)
    assert (kept, skipped, error) == (["b"], [], "")
    assert len(seen) == 1


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_progress_option_skips_new_positions_but_keeps_position_changes(config_base, monkeypatch, mode):
    moved = resumed(1, 60, "2024-06-01T12:00:00Z")
    source = [moved, resumed(2, 30), resumed(3, 40)]
    target = [resumed(1, 20, "2024-01-01T12:00:00Z")]
    cfg, src, dst = progress_setup(config_base, monkeypatch, source, target, mode=mode, options={lp.OPTION: True})
    calls = enable_progress_presence(dst, {canonical_key(row) for row in source})
    plan = InteractivePlan()
    run(cfg, plan)
    assert calls == [("progress", sorted([canonical_key(resumed(2, 30)), canonical_key(resumed(3, 40))]))]
    by_key = {row["key"]: row for row in plan.rows.values()}
    assert by_key[canonical_key(moved)]["result"] in ("add", "update")
    assert by_key[canonical_key(moved)]["item"]["progress_percent"] == 60
    assert by_key[canonical_key(resumed(2, 30))]["result"] == lp.REASON
    assert by_key[canonical_key(resumed(3, 40))]["result"] == lp.REASON


def test_progress_option_off_keeps_the_normal_plan(config_base, monkeypatch):
    cfg, src, dst = progress_setup(config_base, monkeypatch, [resumed(1, 30), resumed(2, 40)], [])
    calls = enable_progress_presence(dst, {canonical_key(resumed(2, 40))})
    plan = InteractivePlan()
    run(cfg, plan)
    assert not calls
    assert sorted(row["result"] for row in plan.rows.values()) == ["add", "add"]


def test_jellyfin_progress_presence_uses_the_progress_resolver(monkeypatch):
    from providers.sync.jellyfin import _common as common
    from providers.sync.jellyfin import _id_lookup, _presence

    seen = []

    def resolve_many(adapter, want, *, feature="history"):
        seen.append((adapter.cfg.strict_id_matching, feature, "watched_at" in want))
        return ["jf-9", "jf-10"] if (want.get("ids") or {}).get("imdb") == "tt0000001" else []

    def never(adapter, want, *, feature="history"):
        raise AssertionError("progress must use resolve_item_ids")

    monkeypatch.setattr(_presence, "_index_ready", lambda adapter, feature: True)
    monkeypatch.setattr(_id_lookup, "prepare", lambda adapter, feature, items: None)
    monkeypatch.setattr(common, "resolve_item_ids", resolve_many)
    monkeypatch.setattr(common, "resolve_item_id", never)
    show = dict(type="show", title="Show", ids=dict(SHOW_IDS))
    result = _presence.presence(jellyfin_adapter(), [resumed(1, 30), resumed(2, 30), show], feature="progress")
    assert [row["status"] for row in result] == [lp.PRESENT, lp.ABSENT, lp.UNKNOWN]
    assert result[0]["item_id"] == "jf-9"
    assert seen == [(True, "progress", False), (True, "progress", False)]


def test_emby_progress_presence_batches_id_queries(monkeypatch):
    from providers.sync.emby import _presence

    queries = emby_library(monkeypatch)
    rows = [resumed(n, 30) for n in range(1, 41)] + [dict(episode(1, 1), progress_percent=30),
                                                    dict(episode(1, 2), progress_percent=30)]
    result = _presence.presence(emby_adapter(), rows, feature="progress")
    statuses = [row["status"] for row in result]
    assert statuses[0] == lp.PRESENT and result[0]["item_id"] == "m1"
    assert statuses[1:40] == [lp.ABSENT] * 39
    assert statuses[40:] == [lp.PRESENT, lp.ABSENT]
    assert sorted(types for _pairs, types in queries) == ["Movie", "Series"]


def test_plex_progress_presence_checks_movies_and_episodes_only(monkeypatch):
    index = {"movies": {"imdb://tt0000001": "10"}, "shows": {"tmdb://500": "20", "tvdb://600": "20"}}
    presence, loads = plex_ratings_setup(monkeypatch, index, {"20": {(1, 1)}}, shared=True)
    adapter = SimpleNamespace(client=SimpleNamespace(server=object()))
    rows = [resumed(1, 30), resumed(2, 30), dict(episode(1, 1), progress_percent=30),
            dict(episode(1, 2), progress_percent=30), dict(episode(1, 1), type="anime", progress_percent=30),
            dict(type="show", title="Show", ids=dict(SHOW_IDS)), dict(type="movie", title="No ids", ids={})]
    result = presence.presence(adapter, rows, feature="progress")
    assert [row["status"] for row in result] == [
        lp.PRESENT, lp.ABSENT, lp.PRESENT, lp.ABSENT, lp.PRESENT, lp.UNKNOWN, lp.UNKNOWN]
    assert loads == [["20"]]


def watchlist_setup(config_base, monkeypatch, source, target, *, mode="one-way", options=None):
    src, dst = setup_ops(config_base, monkeypatch, source, target)
    cfg = _cfg(False)
    feature = {"enable": True, "add": True, "remove": False}
    feature.update(options or {})
    cfg["pairs"][0].update(mode=mode, feature="watchlist", features={"watchlist": feature})
    return cfg, src, dst


def enable_watchlist_presence(ops, absent_keys, features=("history", "progress", "watchlist")):
    calls = []

    def presence(cfg, items, *, feature):
        rows = list(items)
        calls.append((feature, sorted(canonical_key(row) for row in rows)))
        return [lp.verdict(lp.ABSENT, reason=lp.REASON) if canonical_key(row) in absent_keys
                else lp.verdict(lp.PRESENT, item_id="1") for row in rows]

    ops.library_presence = presence
    ops.capabilities = lambda: {"features": {"watchlist": True}, "index_semantics": "present",
                                "library_presence": {"features": list(features)}}
    return calls


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_watchlist_option_skips_titles_that_are_not_in_the_library(config_base, monkeypatch, mode):
    cfg, src, dst = watchlist_setup(config_base, monkeypatch, [item(1), item(2), item(3)], [item(1)], mode=mode,
                                    options={lp.OPTION: True})
    calls = enable_watchlist_presence(dst, {canonical_key(item(3))})
    plan = InteractivePlan()
    run(cfg, plan)
    assert calls == [("watchlist", sorted([canonical_key(item(2)), canonical_key(item(3))]))]
    by_key = {row["key"]: row for row in plan.rows.values()}
    assert by_key[canonical_key(item(2))]["result"] == "add"
    assert by_key[canonical_key(item(3))]["result"] == lp.REASON
    assert by_key[canonical_key(item(3))]["selectable"] is False
    assert canonical_key(item(1)) not in by_key


def test_watchlist_option_off_keeps_the_normal_plan(config_base, monkeypatch):
    cfg, src, dst = watchlist_setup(config_base, monkeypatch, [item(1), item(2)], [])
    calls = enable_watchlist_presence(dst, {canonical_key(item(2))})
    plan = InteractivePlan()
    run(cfg, plan)
    assert not calls
    assert sorted(row["result"] for row in plan.rows.values()) == ["add", "add"]


def test_watchlist_option_is_ignored_for_servers_without_a_library_watchlist(config_base, monkeypatch):
    cfg, src, dst = watchlist_setup(config_base, monkeypatch, [item(1)], [], options={lp.OPTION: True})
    calls = enable_watchlist_presence(dst, {canonical_key(item(1))}, features=("history", "ratings", "progress"))
    plan = InteractivePlan()
    run(cfg, plan)
    assert not calls
    assert [row["result"] for row in plan.rows.values()] == ["add"]


def test_only_jellyfin_and_emby_offer_the_watchlist_option():
    from providers.sync._mod_EMBY import _EmbyOPS
    from providers.sync._mod_JELLYFIN import _JellyfinOPS
    from providers.sync._mod_KODI import OPS as kodi
    from providers.sync._mod_PLEX import _PlexOPS

    support = {name: sorted(f for f in lp.FEATURES if lp.supported(ops, f)) for name, ops in (
        ("jellyfin", _JellyfinOPS()), ("emby", _EmbyOPS()), ("plex", _PlexOPS()), ("kodi", kodi))}
    assert support == {
        "jellyfin": ["history", "progress", "watchlist"],
        "emby": ["history", "progress", "watchlist"],
        "plex": ["history", "progress", "ratings"],
        "kodi": ["history", "progress", "ratings"],
    }


def test_jellyfin_watchlist_presence_checks_movies_and_shows(monkeypatch):
    from providers.sync.jellyfin import _common as common
    from providers.sync.jellyfin import _id_lookup, _presence

    seen = []

    def resolve(adapter, want, *, feature="history"):
        seen.append((adapter.cfg.strict_id_matching, feature, want.get("type")))
        ids = want.get("ids") or {}
        return "jf-1" if ids.get("imdb") == "tt0000001" or ids.get("tmdb") == "500" else None

    monkeypatch.setattr(_presence, "_index_ready", lambda adapter, feature: feature == "history")
    monkeypatch.setattr(_id_lookup, "prepare", lambda adapter, feature, items: None)
    monkeypatch.setattr(common, "resolve_item_id", resolve)
    show = dict(type="show", title="Show", ids=dict(SHOW_IDS))
    rows = [item(1), item(2), show, dict(show, ids={"tvdb": "999"}), dict(type="movie", title="No ids", ids={}),
            episode(1, 1), dict(type="season", title="Season", ids=dict(SHOW_IDS))]
    result = _presence.presence(jellyfin_adapter(), rows, feature="watchlist")
    assert [row["status"] for row in result] == [
        lp.PRESENT, lp.ABSENT, lp.PRESENT, lp.ABSENT, lp.UNKNOWN, lp.UNKNOWN, lp.UNKNOWN]
    assert seen == [(True, "history", "movie"), (True, "history", "movie"), (True, "history", "show"), (True, "history", "show")]


def test_emby_watchlist_presence_batches_id_queries(monkeypatch):
    from providers.sync.emby import _presence

    queries = emby_library(monkeypatch)
    show = dict(type="show", title="Show", ids=dict(SHOW_IDS))
    rows = [item(n) for n in range(1, 41)] + [show, dict(show, ids={"tvdb": "999"}), episode(1, 1)]
    result = _presence.presence(emby_adapter(), rows, feature="watchlist")
    statuses = [row["status"] for row in result]
    assert statuses[0] == lp.PRESENT and result[0]["item_id"] == "m1"
    assert statuses[1:40] == [lp.ABSENT] * 39
    assert statuses[40:] == [lp.PRESENT, lp.ABSENT, lp.UNKNOWN]
    assert sorted(types for _pairs, types in queries) == ["Movie", "Series"]

