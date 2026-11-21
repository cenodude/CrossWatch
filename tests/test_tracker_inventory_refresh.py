# tests/test_tracker_inventory_refresh.py
# CrossWatch - Tracker changes keep the saved CrossWatch inventory read by the Editor and Hub current
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import logging
import sqlite3
import threading
import time
from contextlib import nullcontext

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import editorAPI as editor
from api import trackerAPI
from cw_platform.local_db import state as database
from cw_platform.modules_registry import load_sync_ops
from cw_platform.provider_instances import build_provider_config_view
from cw_platform import tracker_store
from cw_platform.tracker_store import TrackerStore
from services import editor_removal as removal
from services import profile_history, tracker_inventory

REF = {"provider": "CROSSWATCH", "instance": "default"}
MOVIE = {"type": "movie", "title": "Heat", "year": 1995, "ids": {"tmdb": 949, "imdb": "tt0113277"}}
OTHER = {"type": "movie", "title": "Ronin", "year": 1998, "ids": {"tmdb": 8195, "imdb": "tt0122690"}}
REAL_BUSY = tracker_inventory._busy
FIELDS = {
    "watchlist": {"added_at": "2026-11-01T20:00:00Z"},
    "history": {"watched_at": "2026-11-01T20:00:00Z"},
    "ratings": {"rating": 8, "rated_at": "2026-11-01T20:00:00Z"},
    "progress": {"progress_ms": 600000, "duration_ms": 6000000, "progress_at": "2026-11-01T20:00:00Z"},
    "collection": {"collected_at": "2026-11-01T20:00:00Z"},
}


@pytest.fixture
def env(config_base, monkeypatch):
    root = config_base / "tracker"
    cfg = {"pairs": [], "crosswatch": {"root_dir": str(root), "connected": True}}
    monkeypatch.setattr(editor, "_STATE_BASE", config_base)
    monkeypatch.setattr(editor, "load_config", lambda: cfg)
    monkeypatch.setattr(editor, "sync_provider_names", lambda **kwargs: ["CROSSWATCH"])
    monkeypatch.setattr(trackerAPI, "load_config", lambda: cfg)
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    monkeypatch.setattr(tracker_inventory, "_busy", lambda: False)
    monkeypatch.setattr(tracker_inventory, "_SEEN", {})
    monkeypatch.setattr(tracker_inventory, "_QUIET", {})
    monkeypatch.setattr(tracker_inventory, "_LOADED", set())
    monkeypatch.setattr(tracker_inventory, "_FAILED", {})
    monkeypatch.setattr(tracker_inventory, "_STOP", threading.Event())
    monkeypatch.setattr(removal, "_sync_guard", nullcontext)
    removal._LIVE_READS.clear()
    app = FastAPI()
    app.include_router(trackerAPI.router)
    return cfg, root, TestClient(app)


def saved(feature, instance="default"):
    state = database.load_state_features(editor._STATE_BASE, {feature})
    return removal._items(state, "CROSSWATCH", instance, feature)


def send(feature, item):
    out = editor.api_editor_send({"kind": feature, "providers": [REF], "items": [item]}, request=None)
    assert out["results"][0]["result"]["confirmed"] == 1


def tracked(root, feature):
    return sorted(TrackerStore(root).read_current(feature)["items"])


def settle(cfg):
    tracker_inventory.run_once(cfg)
    tracker_inventory.run_once(cfg)


def adapter_add(cfg, feature, item, instance="default"):
    load_sync_ops("CROSSWATCH").add(build_provider_config_view(cfg, "CROSSWATCH", instance), [item], feature=feature)


@pytest.mark.parametrize("feature", list(FIELDS))
def test_tracker_delete_clears_the_saved_inventory(env, feature):
    cfg, root, client = env
    send(feature, {**MOVIE, **FIELDS[feature]})
    settle(cfg)
    response = client.post("/api/tracker/delete", json={"feature": feature, "keys": tracked(root, feature)})
    assert response.status_code == 200, response.text
    assert saved(feature)
    settle(cfg)
    assert saved(feature) == {}
    assert editor._load_state_items(feature, "CROSSWATCH", "default") == {}


def test_title_removal_clears_the_hub_watches(env):
    cfg, root, client = env
    send("history", {**MOVIE, **FIELDS["history"]})
    preview = client.post("/api/tracker/titles/remove", json={"keys": ["movie:tmdb:949"], "scope": "history"}).json()
    assert client.post("/api/tracker/titles/remove", json={"keys": ["movie:tmdb:949"], "scope": "history", "token": preview["token"]}).status_code == 200
    settle(cfg)
    state = database.load_state_features(editor._STATE_BASE, {"history"})
    assert profile_history.title_play_summary(lambda: state, lambda: {}, {}, 949)["holders"] == []


def test_one_sent_watch_is_saved_once(env):
    cfg, _, _ = env
    send("history", {**MOVIE, **FIELDS["history"]})
    assert len(saved("history")) == 2
    settle(cfg)
    assert len(saved("history")) == 1


def test_undo_brings_the_saved_row_back(env):
    cfg, root, client = env
    send("ratings", {**MOVIE, **FIELDS["ratings"]})
    batch = client.post("/api/tracker/delete", json={"feature": "ratings", "keys": tracked(root, "ratings")}).json()["batch"]
    settle(cfg)
    assert saved("ratings") == {}
    assert client.post("/api/tracker/undo", json={"batch": batch}).status_code == 200
    settle(cfg)
    assert sorted(saved("ratings")) == tracked(root, "ratings") == ["tmdb:949"]


def test_rows_added_outside_the_editor_reach_it(env):
    cfg, root, client = env
    send("watchlist", {**MOVIE, **FIELDS["watchlist"]})
    adapter_add(cfg, "watchlist", {**OTHER, **FIELDS["watchlist"]})
    settle(cfg)
    assert sorted(editor._load_state_items("watchlist", "CROSSWATCH", "default")) == tracked(root, "watchlist") == ["tmdb:8195", "tmdb:949"]


def test_tracker_changes_never_create_a_saved_inventory(env):
    cfg, root, client = env
    adapter_add(cfg, "ratings", {**MOVIE, **FIELDS["ratings"]})
    settle(cfg)
    state = database.load_state_features(editor._STATE_BASE, {"ratings"})
    assert "CROSSWATCH" not in (state.get("providers") or {})


def test_unchanged_items_are_not_rebuilt(env, monkeypatch):
    cfg, root, client = env
    send("history", {**MOVIE, **FIELDS["history"]})
    calls = []
    real = tracker_inventory.refresh
    monkeypatch.setattr(tracker_inventory, "refresh", lambda *args: calls.append(args[1]) or real(*args))
    settle(cfg)
    assert calls == ["history"]
    assert client.post("/api/tracker/note", json={"key": "movie:tmdb:949", "status": "completed"}).status_code == 200
    TrackerStore(root).write_state("history", TrackerStore(root).read_current("history")["items"])
    settle(cfg)
    assert calls == ["history"]


def test_refresh_keeps_the_saved_checkpoint(env):
    cfg, root, client = env
    adapter_add(cfg, "watchlist", {**MOVIE, **FIELDS["watchlist"]})
    database.save_feature_baseline(editor._STATE_BASE, provider="CROSSWATCH", feature="watchlist", checkpoint=123,
                                   items={"tmdb:949": {**MOVIE, **FIELDS["watchlist"]}})
    assert client.post("/api/tracker/delete", json={"feature": "watchlist", "keys": tracked(root, "watchlist")}).status_code == 200
    settle(cfg)
    state = database.load_state_features(editor._STATE_BASE, {"watchlist"})
    assert state["providers"]["CROSSWATCH"]["watchlist"]["checkpoint"] == 123
    assert saved("watchlist") == {}


def test_failed_tracker_read_keeps_the_saved_inventory_and_retries(env, monkeypatch):
    cfg, _, _ = env
    send("watchlist", {**MOVIE, **FIELDS["watchlist"]})
    adapter_add(cfg, "watchlist", {**OTHER, **FIELDS["watchlist"]})

    def unreadable(*args, **kwargs):
        raise RuntimeError("unreadable")

    with monkeypatch.context() as patch:
        patch.setattr(editor, "read_tracker_index", unreadable)
        settle(cfg)
    assert sorted(saved("watchlist")) == ["tmdb:949"]
    settle(cfg)
    assert sorted(saved("watchlist")) == ["tmdb:8195", "tmdb:949"]


def test_nothing_is_refreshed_while_a_sync_runs(env, monkeypatch):
    cfg, root, client = env
    send("ratings", {**MOVIE, **FIELDS["ratings"]})
    client.post("/api/tracker/delete", json={"feature": "ratings", "keys": tracked(root, "ratings")})
    monkeypatch.setattr(tracker_inventory, "_busy", lambda: True)
    settle(cfg)
    assert sorted(saved("ratings")) == ["tmdb:949"]


def test_collapsed_history_block_keeps_its_format(env):
    cfg, root, client = env
    database.save_feature_baseline(editor._STATE_BASE, provider="CROSSWATCH", feature="history",
                                   items={"tmdb:8195": {**OTHER, **FIELDS["history"]}})
    adapter_add(cfg, "history", {**MOVIE, **FIELDS["history"]})
    settle(cfg)
    assert sorted(saved("history")) == ["tmdb:949"]


def test_other_instances_tag_their_rows(env, config_base):
    cfg, _, _ = env
    cfg["crosswatch"]["instances"] = {"second": {"root_dir": str(config_base / "second")}}
    adapter_add(cfg, "ratings", {**MOVIE, **FIELDS["ratings"]}, instance="second")
    database.save_feature_baseline(editor._STATE_BASE, provider="CROSSWATCH", instance="second", feature="ratings", items={})
    settle(cfg)
    assert [(key, row.get("_cw_instance")) for key, row in saved("ratings", "second").items()] == [("tmdb:949", "second")]


def test_item_revision_moves_only_when_items_change(tmp_path):
    store = TrackerStore(tmp_path)
    store.write_state("ratings", {"tmdb:949": {**MOVIE, **FIELDS["ratings"]}})
    first = store.item_revisions()
    store.write_state("ratings", {"tmdb:949": {**MOVIE, **FIELDS["ratings"]}})
    assert store.item_revisions() == first
    store.change_items("ratings", [("tmdb:949", None, None)], action="delete")
    assert store.item_revisions()["ratings"] != first["ratings"]
    assert set(store.item_revisions()) == {"ratings"}


def test_rebuild_waits_until_the_tracker_is_quiet(env):
    cfg, root, client = env
    send("ratings", {**MOVIE, **FIELDS["ratings"]})
    settle(cfg)
    client.post("/api/tracker/delete", json={"feature": "ratings", "keys": tracked(root, "ratings")})
    tracker_inventory.run_once(cfg)
    adapter_add(cfg, "ratings", {**OTHER, **FIELDS["ratings"]})
    tracker_inventory.run_once(cfg)
    assert sorted(saved("ratings")) == ["tmdb:949"]
    tracker_inventory.run_once(cfg)
    assert sorted(saved("ratings")) == ["tmdb:8195"]


def test_pair_scoped_trackers_are_left_alone(env, monkeypatch):
    cfg, _, _ = env
    send("ratings", {**MOVIE, **FIELDS["ratings"]})
    adapter_add(cfg, "ratings", {**OTHER, **FIELDS["ratings"]})
    monkeypatch.setattr(tracker_inventory, "_busy", REAL_BUSY)
    monkeypatch.setattr("services.tracker_automation.busy", lambda: False)
    monkeypatch.setenv("CW_CROSSWATCH_PAIR_SCOPED", "1")
    settle(cfg)
    assert sorted(saved("ratings")) == ["tmdb:949"]


def hub_remove(feature, item):
    removal._LIVE_READS.clear()
    out = editor.api_editor_send({"kind": feature, "operation": "remove", "direct": True, "providers": [REF],
                                  "items": [{**item, "key": "tmdb:949"}]}, request=None)
    return out["results"][0]


@pytest.mark.parametrize("feature", ["history", "ratings", "collection"])
def test_hub_remove_clears_a_leftover_the_tracker_no_longer_has(env, feature):
    cfg, root, _ = env
    item = {**MOVIE, **FIELDS[feature]}
    send(feature, item)
    TrackerStore(root).change_items(feature, [(k, None, None) for k in tracked(root, feature)], action="delete")
    row = hub_remove(feature, item)
    assert row["ok"] is True and row["result"]["removed"] == 1
    assert saved(feature) == {}


def test_hub_remove_leaves_titles_the_tracker_still_has_to_the_normal_path(env):
    cfg, root, _ = env
    item = {**MOVIE, **FIELDS["history"]}
    send("history", item)
    assert hub_remove("history", item)["error"] == "needs_live_match"
    assert len(saved("history")) == 2


def test_hub_remove_never_clears_leftovers_without_a_readable_tracker(env, config_base, monkeypatch):
    cfg, root, _ = env
    item = {**MOVIE, **FIELDS["ratings"]}
    send("ratings", item)
    TrackerStore(root).change_items("ratings", [("tmdb:949", None, None)], action="delete")
    with monkeypatch.context() as patch:
        patch.setattr(tracker_inventory, "_tracker_root", lambda cfg, inst: config_base / "missing")
        assert hub_remove("ratings", item)["error"] == "needs_live_match"

    def unreadable(*args, **kwargs):
        raise RuntimeError("unreadable")

    with monkeypatch.context() as patch:
        patch.setattr(removal, "_read_current", unreadable)
        assert hub_remove("ratings", item)["error"] == "needs_live_match"
    assert sorted(saved("ratings")) == ["tmdb:949"]


def test_restart_skips_rebuilding_an_unchanged_inventory(env, monkeypatch):
    cfg, root, _ = env
    send("ratings", {**MOVIE, **FIELDS["ratings"]})
    settle(cfg)
    calls = []
    real = tracker_inventory.refresh
    monkeypatch.setattr(tracker_inventory, "refresh", lambda *args: calls.append(args[1]) or real(*args))

    def restart():
        monkeypatch.setattr(tracker_inventory, "_SEEN", {})
        monkeypatch.setattr(tracker_inventory, "_QUIET", {})
        monkeypatch.setattr(tracker_inventory, "_LOADED", set())
        settle(cfg)

    restart()
    assert calls == []
    database.save_feature_baseline(editor._STATE_BASE, provider="CROSSWATCH", feature="ratings", items={})
    restart()
    assert calls == ["ratings"]
    assert sorted(saved("ratings")) == ["tmdb:949"]


def test_hub_remove_keeps_a_saved_row_the_tracker_still_holds_under_another_id(env):
    cfg, root, _ = env
    adapter_add(cfg, "ratings", {"type": "movie", "title": "Heat", "year": 1995, "ids": {"imdb": "tt0113277"}, **FIELDS["ratings"]})
    database.save_feature_baseline(editor._STATE_BASE, provider="CROSSWATCH", feature="ratings",
                                   items={"tmdb:949": {**MOVIE, **FIELDS["ratings"]}})
    item = {"type": "movie", "title": "Heat", "year": 1995, "ids": {"tmdb": 949}, **FIELDS["ratings"]}
    assert hub_remove("ratings", item)["error"] == "needs_live_match"
    assert sorted(saved("ratings")) == ["tmdb:949"]


def test_only_real_item_changes_wake_the_worker(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(tracker_store, "_ITEM_LISTENERS", {lambda: calls.append(1)})
    store = TrackerStore(tmp_path)
    store.write_state("ratings", {"tmdb:949": {**MOVIE, **FIELDS["ratings"]}})
    assert calls == [1]
    store.write_state("ratings", {"tmdb:949": {**MOVIE, **FIELDS["ratings"]}})
    store.save_note("movie:tmdb:949", {"type": "movie", "status": "completed"})
    assert calls == [1]
    store.change_items("ratings", [("tmdb:949", None, None)], action="delete")
    assert calls == [1, 1]
    TrackerStore._bump_items(sqlite3.connect(":memory:"), [])
    assert calls == [1, 1]


def test_check_reports_a_change_that_is_still_settling(env):
    cfg, root, client = env
    send("ratings", {**MOVIE, **FIELDS["ratings"]})
    assert tracker_inventory.run_once(cfg) is True
    assert tracker_inventory.run_once(cfg) is False


def test_read_only_tracker_reads_never_wait_for_writers(env):
    cfg, root, _ = env
    adapter_add(cfg, "ratings", {**MOVIE, **FIELDS["ratings"]})
    held, release = threading.Event(), threading.Event()

    def writer():
        with TrackerStore(root).mutation():
            held.set()
            release.wait(10)

    blocker = threading.Thread(target=writer)
    blocker.start()
    held.wait(5)
    result = {}
    reader = threading.Thread(target=lambda: result.update(rows=editor.read_tracker_index("ratings", "default")))
    reader.start()
    reader.join(5)
    finished = not reader.is_alive()
    release.set()
    blocker.join(5)
    reader.join(5)
    assert finished
    assert list(result["rows"]) == ["tmdb:949"]


def test_running_worker_catches_up_soon_after_a_tracker_change(env, monkeypatch):
    cfg, root, client = env
    monkeypatch.setattr(tracker_inventory, "INTERVAL", 0.2)
    monkeypatch.setattr(tracker_inventory, "QUIET", 0.05)
    send("ratings", {**MOVIE, **FIELDS["ratings"]})
    tracker_inventory.start(lambda: cfg)
    try:
        client.post("/api/tracker/delete", json={"feature": "ratings", "keys": tracked(root, "ratings")})
        deadline = time.monotonic() + 10
        while saved("ratings") and time.monotonic() < deadline:
            time.sleep(0.05)
        assert saved("ratings") == {}
    finally:
        tracker_inventory.stop()


def test_read_only_watchlist_reads_never_look_up_metadata(env, monkeypatch):
    cfg, root, _ = env
    from providers.sync.crosswatch import _watchlist
    adapter_add(cfg, "watchlist", {"type": "movie", "title": "Heat", "year": 1995, "ids": {"imdb": "tt0113277"}, **FIELDS["watchlist"]})
    lookups = []
    monkeypatch.setattr(_watchlist, "_ensure_tmdb_for_item", lambda item: lookups.append(item) or True)
    points = len(TrackerStore(root).restore_points("watchlist"))
    assert list(editor.read_tracker_index("watchlist", "default")) == ["imdb:tt0113277"]
    assert lookups == []
    assert len(TrackerStore(root).restore_points("watchlist")) == points


def test_one_broken_instance_does_not_stop_the_others(env, config_base, monkeypatch):
    cfg, _, _ = env
    cfg["crosswatch"]["instances"] = {"second": {"root_dir": str(config_base / "second")}}
    adapter_add(cfg, "ratings", {**MOVIE, **FIELDS["ratings"]}, instance="second")
    database.save_feature_baseline(editor._STATE_BASE, provider="CROSSWATCH", instance="second", feature="ratings", items={})
    real = tracker_inventory._tracker_root

    def root(cfg, inst):
        if inst == "default":
            raise OSError("broken")
        return real(cfg, inst)

    monkeypatch.setattr(tracker_inventory, "_tracker_root", root)
    settle(cfg)
    assert list(saved("ratings", "second")) == ["tmdb:949"]


def test_a_failing_refresh_is_logged_once_and_keeps_the_copy(env, monkeypatch, caplog):
    cfg, _, _ = env
    send("ratings", {**MOVIE, **FIELDS["ratings"]})
    adapter_add(cfg, "ratings", {**OTHER, **FIELDS["ratings"]})
    monkeypatch.setattr(load_sync_ops("CROSSWATCH"), "features", lambda: {"ratings": False})
    with caplog.at_level(logging.WARNING, logger=tracker_inventory.__name__):
        for _ in range(3):
            settle(cfg)
    assert sorted(saved("ratings")) == ["tmdb:949"]
    assert len([r for r in caplog.records if "Could not refresh" in r.getMessage()]) == 1


def test_hub_remove_leaves_pair_scoped_trackers_alone(env, monkeypatch):
    cfg, root, _ = env
    item = {**MOVIE, **FIELDS["ratings"]}
    send("ratings", item)
    TrackerStore(root).change_items("ratings", [("tmdb:949", None, None)], action="delete")
    monkeypatch.setenv("CW_CROSSWATCH_PAIR_SCOPED", "1")
    assert hub_remove("ratings", item)["error"] == "needs_live_match"
    monkeypatch.delenv("CW_CROSSWATCH_PAIR_SCOPED")
    assert sorted(saved("ratings")) == ["tmdb:949"]
