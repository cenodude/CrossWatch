# tests/test_tracker_store.py
# CrossWatch - Local tracker storage contract
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import json
import os

import pytest

from api import maintenanceAPI
from cw_platform.tracker_store import TrackerStore, TrackerUnreadable, tracker_context

ITEM = {"type": "movie", "ids": {"tmdb": "1"}, "watched_at": "2026-09-19T12:00:00Z"}


def test_state_round_trips_and_missing_state_reads_as_none(tmp_path):
    store = TrackerStore(tmp_path)
    assert store.read_state("history") is None
    assert store.write_state("history", {"tmdb:1": ITEM}, ts=5)
    assert store.read_state("history").raw == {"ts": 5, "items": {"tmdb:1": ITEM}}
    assert store.has_state("history")


def test_unreadable_state_raises_instead_of_reading_empty(tmp_path):
    (tmp_path / "history.json").write_text("{broken", "utf-8")
    with pytest.raises(TrackerUnreadable) as exc:
        TrackerStore(tmp_path).read_state("history")
    assert exc.value.names == ["history.json"]
    assert str(exc.value) == "crosswatch_history_state_unreadable:history.json"


def test_missing_state_falls_back_to_the_newest_restore_point_only_when_allowed(tmp_path):
    store = TrackerStore(tmp_path)
    store.add_restore_point("history", {"tmdb:1": ITEM})
    found = store.read_state("history")
    assert found.raw["items"] == {"tmdb:1": ITEM}
    assert found.restored_from == store.restore_points("history")[0].name
    assert store.read_state("history", snapshot_fallback=False) is None


def test_restore_points_keep_the_newest_within_the_limit(tmp_path):
    store = TrackerStore(tmp_path)
    snaps = tmp_path / "snapshots"
    snaps.mkdir()
    for n in range(3):
        path = snaps / f"2026010{n + 1}T000000Z-history.json"
        path.write_text(json.dumps({"ts": n, "items": {}}), "utf-8")
        os.utime(path, (1_700_000_000 + n, 1_700_000_000 + n))
    store.add_restore_point("history", {"tmdb:1": ITEM}, retention_days=0, max_snapshots=2)
    names = [p.name for p in store.restore_points("history")]
    assert len(names) == 2
    assert "20260103T000000Z-history.json" in names
    assert store.read_restore_point("history", names[-1])["items"] == {"tmdb:1": ITEM}


def test_restore_point_reads_refuse_unknown_names(tmp_path):
    store = TrackerStore(tmp_path)
    with pytest.raises(ValueError):
        store.read_restore_point("history", "../config.json")
    with pytest.raises(ValueError):
        store.load_items("history", "../config.json")


def test_pair_scoped_state_stays_in_its_own_file(tmp_path):
    TrackerStore(tmp_path).write_state("watchlist", {"tmdb:1": ITEM})
    scoped = TrackerStore(tmp_path, scope="pair-a", chained=False)
    assert scoped.read_state("watchlist").raw["items"] == {"tmdb:1": ITEM}
    scoped.write_state("watchlist", {})
    assert TrackerStore(tmp_path).read_state("watchlist").raw["items"] == {"tmdb:1": ITEM}
    assert scoped.read_state("watchlist").raw["items"] == {}


def test_profile_roots_follow_the_configured_instances(tmp_path):
    cfg = {"crosswatch": {"root_dir": "tracker", "instances": {"CW-P01": {}}}}
    assert tracker_context(cfg, tmp_path) == ("default", tmp_path / "tracker")
    assert tracker_context(cfg, tmp_path, "CW-P01") == ("CW-P01", tmp_path / "tracker" / "profiles" / "CW-P01")
    assert tracker_context(cfg, tmp_path, "../outside") == ("default", tmp_path / "tracker")


def test_restore_points_endpoint_lists_names_per_feature_for_the_profile(tmp_path, monkeypatch):
    base = tmp_path / ".cw_provider"
    profile = base / "profiles" / "CW-P01"
    TrackerStore(base).add_restore_point("history", {"tmdb:1": ITEM})
    TrackerStore(profile).add_restore_point("ratings", {"tmdb:1": ITEM})
    (tmp_path / "config.json").write_text(json.dumps({"crosswatch": {"root_dir": str(base), "instances": {"CW-P01": {}}}}), "utf-8")
    monkeypatch.setattr(maintenanceAPI, "_cw", lambda: (tmp_path / "cache", tmp_path, tmp_path / ".cw_state", None, None, None))

    default = maintenanceAPI.crosswatch_tracker_restore_points(provider_instance="default")
    scoped = maintenanceAPI.crosswatch_tracker_restore_points(provider_instance="CW-P01")

    assert len(default["restore_points"]["history"]) == 1
    assert default["restore_points"]["ratings"] == []
    assert scoped["provider_instance"] == "CW-P01"
    assert scoped["restore_points"]["history"] == []
    assert len(scoped["restore_points"]["ratings"]) == 1


def _legacy(root, name, payload, mtime):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), "utf-8")
    os.utime(path, (mtime, mtime))
    return path


def test_first_open_imports_json_files_and_moves_them_to_a_backup(tmp_path):
    _legacy(tmp_path, "history.json", {"ts": 7, "items": {"tmdb:1": ITEM}}, 1_700_000_100)
    _legacy(tmp_path, "watchlist.pair-a.json", {"ts": 8, "items": {"tmdb:2": ITEM}}, 1_700_000_200)
    _legacy(tmp_path, "history.unresolved.json", {"items": {"x": {}}}, 1_700_000_100)
    _legacy(tmp_path, "playlists.json", {"version": 1, "lists": {}}, 1_700_000_100)
    _legacy(tmp_path, "snapshots/20260101T000000Z-history.json", {"ts": 1, "items": {}}, 1_700_000_000)
    _legacy(tmp_path, "snapshots/pair-a/20260102T000000Z-watchlist.json", {"ts": 2, "items": {}}, 1_700_000_050)
    (tmp_path / "profiles" / "CW-P01").mkdir(parents=True)

    store = TrackerStore(tmp_path)

    assert store.read_current("history") == {"ts": 7, "items": {"tmdb:1": ITEM}}
    assert store.read_meta("history.unresolved") == {"items": {"x": {}}}
    assert store.read_playlists() == {"version": 1, "lists": {}}
    assert [p.name for p in store.restore_points("history")] == ["20260101T000000Z-history.json"]
    scoped = TrackerStore(tmp_path, scope="pair-a", chained=False)
    assert scoped.read_current("watchlist")["items"] == {"tmdb:2": ITEM}
    assert [p.name for p in scoped.restore_points("watchlist")] == ["20260102T000000Z-watchlist.json"]
    assert not list(tmp_path.glob("*.json"))
    assert not (tmp_path / "snapshots").exists()
    backup = next(tmp_path.glob("json-backup-*"))
    assert (backup / "history.json").exists()
    assert (backup / "snapshots" / "pair-a" / "20260102T000000Z-watchlist.json").exists()
    assert (tmp_path / "profiles" / "CW-P01").is_dir()


def test_newer_pair_state_wins_for_a_chained_source_after_import(tmp_path):
    _legacy(tmp_path, "watchlist.downstream.json", {"items": {"tmdb:1": ITEM}}, 1_700_000_100)
    _legacy(tmp_path, "watchlist.upstream.json", {"items": {"tmdb:2": ITEM}}, 1_700_000_200)

    assert set(TrackerStore(tmp_path, scope="downstream", chained=True).read_state("watchlist").raw["items"]) == {"tmdb:2"}
    assert set(TrackerStore(tmp_path, scope="downstream", chained=False).read_state("watchlist").raw["items"]) == {"tmdb:1"}


@pytest.mark.parametrize("name", ["history.json", "playlists.json"])
def test_a_corrupt_json_file_blocks_the_import_and_stays_in_place(tmp_path, name):
    (tmp_path / name).write_text("{broken", "utf-8")
    _legacy(tmp_path, "ratings.json", {"items": {"tmdb:1": ITEM}}, 1_700_000_100)

    with pytest.raises(TrackerUnreadable):
        TrackerStore(tmp_path).read_state("ratings")

    assert (tmp_path / name).exists()
    assert (tmp_path / "ratings.json").exists()
    (tmp_path / name).write_text(json.dumps({"items": {}}), "utf-8")
    assert TrackerStore(tmp_path).read_state("ratings").raw["items"] == {"tmdb:1": ITEM}


def test_reading_an_empty_root_creates_no_database(tmp_path):
    store = TrackerStore(tmp_path / "missing")
    assert store.read_state("history") is None
    assert store.restore_points("history") == []
    assert store.stamp(["history"]) == [("history", 0, 0)]
    assert not (tmp_path / "missing").exists()


def test_profile_with_its_own_root_dir_uses_it(tmp_path):
    cfg = {"crosswatch": {"root_dir": str(tmp_path / "base"), "instances": {"CW-P02": {"root_dir": str(tmp_path / "own")}}}}
    assert tracker_context(cfg, tmp_path, "CW-P02") == ("CW-P02", tmp_path / "own")


def test_failed_writes_are_logged_not_raised(tmp_path, caplog):
    blocked = tmp_path / "not-a-folder"
    blocked.write_text("x", "utf-8")
    store = TrackerStore(blocked)
    assert store.write_state("history", {"tmdb:1": {"type": "movie"}}) is False
    store.add_restore_point("history", {"tmdb:1": {"type": "movie"}})
    assert "store_write_failed" in caplog.text


@pytest.mark.parametrize("feature", ["watchlist", "collection", "progress", "ratings", "history"])
def test_legacy_list_states_keep_real_keys_so_removes_still_work(tmp_path, feature):
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

    item = {"type": "movie", "title": "Heat", "year": 1995, "ids": {"tmdb": "949"}, "rating": 8,
            "watched_at": "2026-01-01T00:00:00Z", "collected_at": "2026-01-01T00:00:00Z",
            "progress_percent": 40, "progress_ms": 4000, "duration_ms": 10000}
    (tmp_path / f"{feature}.json").write_text(json.dumps([item]), "utf-8")
    expected = "tmdb:949@1767225600" if feature == "history" else "tmdb:949"
    assert list(TrackerStore(tmp_path).read_current(feature)["items"]) == [expected]
    adapter = CROSSWATCHModule({"CrossWatch": {"connected": True, "enabled": True, "root_dir": str(tmp_path)}})
    assert adapter.remove(feature, [item])["count"] == 1
    assert TrackerStore(tmp_path).read_current(feature)["items"] == {}


def test_a_corrupt_snapshot_blocks_the_move_instead_of_hiding_the_tracker(tmp_path):
    (tmp_path / "snapshots").mkdir()
    (tmp_path / "snapshots" / "20261001T000000Z-history.json").write_text("{broken", "utf-8")
    with pytest.raises(TrackerUnreadable) as exc:
        TrackerStore(tmp_path).read_state("history")
    assert exc.value.names == ["snapshots/20261001T000000Z-history.json"]
    assert (tmp_path / "snapshots" / "20261001T000000Z-history.json").is_file()
    assert not list(tmp_path.glob("json-backup-*"))


def test_a_corrupt_row_is_unreadable_everywhere(tmp_path):
    import sqlite3

    store = TrackerStore(tmp_path)
    store.write_state("history", {"tmdb:1": {"type": "movie", "title": "A", "ids": {"tmdb": "1"}}})
    with sqlite3.connect(tmp_path / "tracker.db") as conn:
        conn.execute("UPDATE items SET data='{bad' WHERE item_key='tmdb:1'")
    for read in (lambda: store.query("history"), lambda: store.load_items("history"), lambda: list(store.iter_records())):
        with pytest.raises(TrackerUnreadable):
            read()


def test_legacy_list_entries_without_identity_keep_their_position(tmp_path):
    item = {"type": "movie", "title": "Heat", "ids": {"tmdb": "949"}}
    (tmp_path / "watchlist.json").write_text(json.dumps([item, "junk", {"type": "movie"}]), "utf-8")
    assert set(TrackerStore(tmp_path).read_current("watchlist")["items"]) == {"tmdb:949", "1", "2"}


@pytest.mark.parametrize(("rewatches", "count"), [(True, 2), (False, 1)])
def test_legacy_history_list_keeps_every_watch_for_rewatch_mode(tmp_path, rewatches, count):
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

    movie = {"type": "movie", "title": "Heat", "ids": {"tmdb": "949"}}
    watches = [{**movie, "watched_at": "2026-01-01T00:00:00Z"}, {**movie, "watched_at": "2026-02-01T00:00:00Z"}]
    (tmp_path / "history.json").write_text(json.dumps(watches), "utf-8")
    assert len(TrackerStore(tmp_path).read_current("history")["items"]) == 2
    adapter = CROSSWATCHModule({"CrossWatch": {"connected": True, "enabled": True, "root_dir": str(tmp_path)}, "_cw_history_rewatches": rewatches})
    assert len(adapter.build_index("history")) == count


def test_a_tracker_edit_waits_for_an_adapter_write_instead_of_being_undone_by_it(tmp_path, monkeypatch):
    import threading
    import time

    from providers.sync import _mod_CROSSWATCH
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

    heat = {"type": "movie", "title": "Heat", "ids": {"tmdb": "949"}, "watched_at": "2026-01-01T00:00:00Z"}
    arrival = {"type": "movie", "title": "Arrival", "ids": {"tmdb": "329865"}, "watched_at": "2026-01-02T00:00:00Z"}
    TrackerStore(tmp_path).write_state("history", {"tmdb:949": heat})
    history = _mod_CROSSWATCH._FEATURES["history"]
    real_load = history._load_state
    ui = {}

    def load_then_ui_deletes(adapter):
        state = real_load(adapter)
        ui["thread"] = threading.Thread(
            target=lambda: TrackerStore(tmp_path).change_items("history", [("tmdb:949", None, None)], action="delete")
        )
        ui["thread"].start()
        time.sleep(0.3)
        ui["blocked"] = ui["thread"].is_alive()
        return state

    monkeypatch.setattr(history, "_load_state", load_then_ui_deletes)
    adapter = CROSSWATCHModule({"CrossWatch": {"connected": True, "enabled": True, "root_dir": str(tmp_path)}})
    assert adapter.add("history", [arrival])["ok"] is True
    ui["thread"].join(10)
    assert ui["blocked"] is True
    assert set(TrackerStore(tmp_path).read_current("history")["items"]) == {"tmdb:329865"}


def test_a_corrupt_sidecar_is_logged_and_kept_in_the_backup(tmp_path, caplog):
    (tmp_path / "history.json").write_text(json.dumps({"ts": 1, "items": {}}), "utf-8")
    (tmp_path / "watchlist.tombstones.json").write_text("{broken", "utf-8")
    TrackerStore(tmp_path).read_state("history")
    assert "legacy_sidecar_skipped" in caplog.text
    assert (next(tmp_path.glob("json-backup-*")) / "watchlist.tombstones.json").is_file()
