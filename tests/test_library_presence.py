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


def test_requested_and_supported_are_history_only():
    ops = SimpleNamespace(library_presence=lambda *a, **k: [], capabilities=lambda: {"library_presence": {"features": ["history"]}})
    assert lp.requested("history", {lp.OPTION: True})
    assert not lp.requested("ratings", {lp.OPTION: True})
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
