# /tests/test_crosswatch_anime_history.py
# CrossWatch - CW tracker anime episode identity and deduplication
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace

import pytest

from cw_platform.anime_mapping import storage
from cw_platform.anime_mapping.history_coords import HistoryCoordinateAliases, coordinate_tokens
from cw_platform.id_map import canonical_key, minimal
from providers.sync.crosswatch import _history


def check_upgrade(monkeypatch, cfg, module, reader_module, name, root):
    from cw_platform.orchestrator.facade import Orchestrator
    from cw_platform.tracker_store import TrackerStore
    from providers.sync import _mod_CROSSWATCH as crosswatch

    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {name: module.OPS, "CROSSWATCH": crosswatch.OPS})
    with monkeypatch.context() as previous:
        previous.setattr(_history, "_anime_identity", lambda item, context, fresh=False: dict(item))
        initial = Orchestrator(cfg).run()
    assert initial["errors"] == 0
    assert TrackerStore(root).record_count() == 6
    for _ in range(2):
        result = Orchestrator(cfg).run()
        assert result["errors"] == result["unresolved"] == result["added"] == result["removed"] == 0
        assert TrackerStore(root).record_count() == 6
    monkeypatch.setattr(_history, "_ALIASES", {"key": None, "rows": None})
    monkeypatch.setattr(_history, "index_ready", lambda tag: False)
    result = Orchestrator(cfg).run()
    assert result["errors"] == result["added"] == result["removed"] == 0
    assert TrackerStore(root).record_count() == 6


def episode(provider="tvdb", number=1, watched_at="2026-10-07T00:00:00Z"):
    return {"type": "episode", "show_ids": {provider: "10" if provider == "tvdb" else "30"},
            "season": 3 if provider == "tvdb" else 1,
            "episode": number + 12 if provider == "tvdb" else number,
            "watched": True, "watched_at": watched_at}


def unavailable_index(monkeypatch):
    monkeypatch.setattr(_history, "_ALIASES", {"key": None, "rows": None})
    monkeypatch.setattr(_history, "index_ready", lambda tag: False)


@pytest.fixture
def adapter(config_base):
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps({
        "tvdb_show:10:s3": {"mal:1": {"13-24": "1-12"}},
        "tmdb_show:30:s1": {"mal:1": {"1-12": "1-12"}},
    }), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    return SimpleNamespace(config={"anime_mapping": {"enabled": True}},
                           cfg=SimpleNamespace(base_path=config_base / "tracker"))


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("separate_batches", [False, True])
def test_cw_stores_equivalent_coordinates_once(adapter, reverse, separate_batches):
    rows = [episode("tvdb", n) for n in (1, 2, 3)] + [episode("tmdb", n) for n in (1, 2, 3)]
    if reverse:
        rows.reverse()
    for batch in ([rows[:3], rows[3:]] if separate_batches else [rows]):
        _, unresolved = _history.add(adapter, batch)
        assert not unresolved
    stored = _history.build_index(adapter)
    assert len(stored) == 3
    assert all(len(coordinate_tokens(row)) == 2 and "_cw_anime_map" not in row for row in stored.values())
    assert all(len(row["show_ids"]) == 1 for row in stored.values())
    for batch in (rows, list(reversed(rows))):
        _history.add(adapter, batch)
        assert len(_history.build_index(adapter)) == 3


def test_newer_alias_updates_watch_without_mixing_numbering(adapter):
    _history.add(adapter, [episode()])
    _history.add(adapter, [episode("tmdb", watched_at="2026-10-08T00:00:00Z")])
    [row] = _history.build_index(adapter).values()
    assert row["show_ids"] == {"tvdb": "10"}
    assert (row["season"], row["episode"]) == (3, 13)
    assert row["watched_at"] == "2026-10-08T00:00:00Z"


@pytest.mark.parametrize("mode", ["global_off", "pair_off", "index_missing"])
def test_disabled_or_missing_mapping_does_not_guess_equivalence(adapter, monkeypatch, mode):
    if mode == "global_off":
        adapter.config["anime_mapping"]["enabled"] = False
    elif mode == "pair_off":
        adapter.config["_cw_pair_feature_options"] = {"use_anime_mapping": False}
    else:
        unavailable_index(monkeypatch)
    _history.add(adapter, [episode(), episode("tmdb")])
    rows = list(_history.build_index(adapter).values())
    assert len(rows) == 2
    assert all(_history.ALIAS_FIELD not in row for row in rows)


@pytest.mark.parametrize("mode", ["global_off", "pair_off"])
def test_saved_aliases_respect_mapping_switches(adapter, mode):
    _history.add(adapter, [episode()])
    if mode == "global_off":
        adapter.config["anime_mapping"]["enabled"] = False
    else:
        adapter.config["_cw_pair_feature_options"] = {"use_anime_mapping": False}
    _history.add(adapter, [episode("tmdb")])
    assert sorted(_history.build_index(adapter)) == ["tmdb:30#s01e01", "tvdb:10#s03e13"]
    assert all(_history.ALIAS_FIELD not in row for row in _history.build_index(adapter).values())
    assert _history.remove(adapter, [episode("tmdb")]) == (1, [])
    assert sorted(_history.build_index(adapter)) == ["tvdb:10#s03e13"]
    adapter.config["anime_mapping"]["enabled"] = True
    adapter.config.pop("_cw_pair_feature_options", None)
    [row] = _history.build_index(adapter).values()
    assert coordinate_tokens(row) == {"tvdb:10#s03e13", "tmdb:30#s01e01"}


def test_aliases_stay_out_of_downstream_output(adapter):
    _history.add(adapter, [episode()])
    [row] = _history.build_index(adapter).values()
    assert coordinate_tokens(row)
    assert "_cw_anime_map" not in row and "_cw_anime_map" not in minimal(row)


def test_mapping_failure_keeps_ordinary_history_working(adapter, monkeypatch):
    def broken(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(_history, "resolve_absolute", broken)
    assert _history.add(adapter, [episode(), episode("tmdb")]) == (2, [])
    assert len(_history.build_index(adapter)) == 2
    assert _history.remove(adapter, [episode("tmdb")]) == (1, [])
    assert sorted(_history.build_index(adapter)) == ["tvdb:10#s03e13"]


def test_only_incoming_items_are_resolved(adapter, monkeypatch):
    rows = [{"type": "episode", "show_ids": {"tvdb": str(1000 + n)}, "season": 1, "episode": 1,
             "watched": True, "watched_at": "2026-10-07T00:00:00Z"} for n in range(200)]
    _history.add(adapter, rows)
    calls = []
    real = _history.resolve_absolute
    monkeypatch.setattr(_history, "resolve_absolute", lambda item, **kw: calls.append(1) or real(item, **kw))
    for _ in range(3):
        _history.build_index(adapter)
    assert not calls
    _history.add(adapter, [episode()])
    _history.remove(adapter, rows[:20])
    assert len(calls) == 21


def shift_tmdb_mapping():
    storage.paths("v3")["mappings"].write_text(json.dumps({
        "tvdb_show:10:s3": {"mal:1": {"13-24": "1-12"}},
        "tmdb_show:30:s1": {"mal:1": {"2-13": "1-12"}},
    }), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")


def test_corrected_mapping_replaces_saved_aliases(adapter):
    _history.add(adapter, [episode()])
    shift_tmdb_mapping()
    [row] = _history.build_index(adapter).values()
    assert coordinate_tokens(row) == {"tvdb:10#s03e13", "tmdb:30#s01e02"}
    assert _history.remove(adapter, [episode("tmdb")]) == (0, [])
    assert _history.remove(adapter, [episode("tmdb", 2)]) == (1, [])
    assert not _history.build_index(adapter)


@pytest.mark.parametrize("operation", ["noop_add", "remove_other"])
def test_corrected_aliases_are_saved_without_a_watch_update(adapter, monkeypatch, operation):
    _history.add(adapter, [episode()])
    shift_tmdb_mapping()
    if operation == "noop_add":
        assert _history.add(adapter, [episode("tmdb", 2)]) == (0, [])
    else:
        assert _history.remove(adapter, [episode("tmdb", 9)]) == (0, [])
    unavailable_index(monkeypatch)
    assert _history.remove(adapter, [episode("tmdb")]) == (0, [])
    assert sorted(_history.build_index(adapter)) == ["tvdb:10#s03e13"]
    assert _history.remove(adapter, [episode("tmdb", 2)]) == (1, [])


def test_update_while_mapping_off_keeps_saved_aliases(adapter, monkeypatch):
    _history.add(adapter, [episode()])
    adapter.config["anime_mapping"]["enabled"] = False
    _history.add(adapter, [episode(watched_at="2026-10-08T00:00:00Z")])
    adapter.config["anime_mapping"]["enabled"] = True
    unavailable_index(monkeypatch)
    _history.add(adapter, [episode("tmdb")])
    [row] = _history.build_index(adapter).values()
    assert row["watched_at"] == "2026-10-08T00:00:00Z"
    assert coordinate_tokens(row) == {"tvdb:10#s03e13", "tmdb:30#s01e01"}


def test_failed_lookup_is_not_trusted_after_recovery(adapter, monkeypatch):
    with monkeypatch.context() as broken:
        broken.setattr("cw_platform.anime_mapping.episodes.query_edges", lambda *a, **kw: [])
        context = _history._alias_context(adapter)
        assert not any(_history.ALIAS_FIELD in _history._anime_identity(row, context) for row in (episode(), episode("tmdb")))
    _history.add(adapter, [episode(), episode("tmdb")])
    assert len(_history.build_index(adapter)) == 1
    monkeypatch.setattr(_history, "_MISS_SECONDS", 0.0)
    with monkeypatch.context() as broken:
        broken.setattr("cw_platform.anime_mapping.episodes.query_edges", lambda *a, **kw: [])
        _history._anime_identity(episode(number=2), _history._alias_context(adapter))
    assert coordinate_tokens(_history._anime_identity(episode(number=2), _history._alias_context(adapter)))


def test_stored_aliases_work_without_mapping_index(adapter, monkeypatch):
    _history.add(adapter, [episode()])
    unavailable_index(monkeypatch)
    _history.add(adapter, [episode("tmdb")])
    assert len(_history.build_index(adapter)) == 1
    [row] = _history.build_index(adapter).values()
    assert HistoryCoordinateAliases().tokens(row) == {"tvdb:10#s03e13", "tmdb:30#s01e01"}


def test_unrelated_or_unmapped_episodes_are_preserved(adapter):
    rows = [episode(), episode("tmdb", 2), {**episode("tmdb"), "show_ids": {"tmdb": "999"}},
            {**episode(), "season": 0}, {"type": "movie", "ids": {"tmdb": "30"}, "watched_at": "2026-10-07T00:00:00Z"}]
    _history.add(adapter, rows)
    assert len(_history.build_index(adapter)) == len(rows)


def test_ambiguous_same_axis_mapping_is_not_collapsed(adapter):
    paths = storage.paths("v3")
    paths["mappings"].write_text(json.dumps({
        "tvdb_show:10:s3": {"mal:1": {"13": "1", "14": "1"}},
        "tmdb_show:30:s1": {"mal:1": {"1": "1"}},
    }), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    _history.add(adapter, [episode(), episode(number=2), episode("tmdb")])
    assert len(_history.build_index(adapter)) == 3


def test_rewatches_remain_distinct_and_alias_removal_targets_one_viewing(adapter):
    adapter.config["_cw_history_rewatches"] = True
    early = [episode(), episode("tmdb")]
    late = [episode(watched_at="2026-10-08T00:00:00Z"), episode("tmdb", watched_at="2026-10-08T00:00:00Z")]
    _history.add(adapter, early + late)
    assert len(_history.build_index(adapter)) == 2
    assert _history.remove(adapter, [early[1]]) == (1, [])
    [row] = _history.build_index(adapter).values()
    assert row["watched_at"] == "2026-10-08T00:00:00Z"


def test_alternate_coordinate_removes_stored_episode(adapter):
    _history.add(adapter, [episode()])
    assert _history.remove(adapter, [episode("tmdb")]) == (1, [])
    assert not _history.build_index(adapter)


@pytest.mark.parametrize("metadata", [None, {}, "tvdb:10#s03e13", [None, 5, "", "imdb:tt1#s01e01", "tvdb:10#s3e1", "tvdb:#s03e13"]])
def test_invalid_coordinate_metadata_is_ignored(metadata):
    assert coordinate_tokens({_history.ALIAS_FIELD: metadata}) == set()
