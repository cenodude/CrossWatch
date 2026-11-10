# tests/test_floppy_status_mapping.py
# CrossWatch - Floppy status mapping preservation and pair execution tests.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from cw_platform.orchestrator._status_mapping import run_status_mappings
from cw_platform.status_mapping import ReadState, Status
from cw_platform.status_mapping_store import StatusStore
from cw_platform.tracker_store import TrackerStore
from providers.sync._status_mapping import provider_caps
from providers.sync.floppy import _common, _status


def entry(item_id, cid, status=1):
    parts = item_id.split("/")
    return {"item_id": item_id, "consumption_id": cid, "item": {"source": parts[1], "media_id": parts[2], "title": "Title"},
            "status": status, "progress": 0, "score": 7.5, "scored_at": "old", "start_date": None,
            "end_date": None, "notes": "Keep this", "created_at": "old", "lists": []}


@pytest.fixture
def server(monkeypatch):
    movie = entry("movie/tmdb/1", 10)
    show = entry("tv/tmdb/2", 20)
    state = SimpleNamespace(rows={"media/movie": [movie, entry("movie/tmdb/3", 30, 0)], "media/tv": [show],
                                 "media/season": [entry("tv/tmdb/2/1", 21)],
                                 "media/episode": [entry("tv/tmdb/2/1/1", 22, 3)], "playback/progress": [],
                                 "lists": [{"id": 1, "name": "Watchlist"}]},
                            calls=[], writes=[], hook=None)
    for row in [*state.rows["media/movie"], show]:
        state.rows[f"media/{row['item_id']}/history"] = [{key: value for key, value in row.items() if key not in {"item", "item_id", "lists"}}]
    def change(key, **values):
        for rows in state.rows.values():
            for row in rows:
                if row.get("consumption_id") == key:
                    row.update(values)
    state.change = change
    def get(adapter, path, **kwargs):
        state.calls.append(("GET", path, deepcopy(kwargs)))
        if state.hook:
            result = state.hook("GET", path, kwargs)
            if result is not None:
                return result
        rows = state.rows[path]
        offset = kwargs["params"]["offset"]
        return {"count": len(rows), "results": deepcopy(rows[offset:offset+1])}
    def patch(adapter, path, **kwargs):
        state.writes.append((path, deepcopy(kwargs)))
        if state.hook:
            result = state.hook("PATCH", path, kwargs)
            if result is not None:
                return result
        cid = int(path.rsplit("/", 1)[1])
        change(cid, **kwargs["json"])
        return {"consumption_id": cid, **kwargs["json"]}
    monkeypatch.setattr(_common, "api_get", get)
    monkeypatch.setattr(_status, "api_patch", patch)
    monkeypatch.setattr(_status.StatusAdapter, "adapter", lambda self: SimpleNamespace(config=self.cfg, instance_id="default"))
    state.adapter = _status.StatusAdapter({})
    return state


def test_complete_reads_use_native_consumptions_and_keep_planned_and_lists_separate(server):
    server.rows["media/movie"][0]["lists"] = [{"list_id": 1, "list_item_id": 2}]
    rows = server.adapter.read()
    assert len(rows) == 3
    assert rows["movie/tmdb/1"].observation.status == Status.WATCHING
    assert rows["movie/tmdb/1"].observation.watchlisted is True
    assert rows["movie/tmdb/3"].observation.status == Status.PLANNED
    assert rows["movie/tmdb/3"].observation.watchlisted is True
    assert any(path == "media/movie" and opts["params"]["offset"] == 1 for _, path, opts in server.calls)


def test_exact_status_patch_preserves_other_consumption_fields(server):
    before = server.adapter.read()["movie/tmdb/1"]
    server.adapter.write(before, Status.DROPPED)
    after = server.adapter.read()["movie/tmdb/1"]
    assert server.writes == [("media/movie/tmdb/1/history/10", {"json": {"status": 4}})]
    assert after.observation.status == Status.DROPPED
    assert after.observation.protected_token == before.observation.protected_token
    server.change(10, notes="edited")
    assert server.adapter.read()["movie/tmdb/1"].observation.protected_token != before.observation.protected_token


@pytest.mark.parametrize("problem", ["multiple", "different_id", "different_status", "missing_field", "unknown_status"])
def test_ambiguous_consumptions_are_never_selected_by_order(server, problem):
    rows = server.rows["media/movie/tmdb/1/history"]
    if problem == "multiple":
        rows.append({**rows[0], "consumption_id": 99})
    elif problem == "different_id":
        rows[0]["consumption_id"] = 99
    elif problem == "different_status":
        rows[0]["status"] = 4
    elif problem == "missing_field":
        rows[0].pop("notes")
    else:
        server.change(10, status=9)
    row = server.adapter.read()["movie/tmdb/1"]
    assert row.observation.state == ReadState.UNKNOWN
    with pytest.raises(RuntimeError, match="transition_blocked"):
        server.adapter.write(row, Status.ON_HOLD)
    assert not server.writes


@pytest.mark.parametrize("seasons,desired,blocked", [([1], Status.DROPPED, True), ([], Status.WATCHING, True),
    ([4], Status.WATCHING, True), ([1, 4], Status.WATCHING, True), ([9], Status.ON_HOLD, True),
    ([1], Status.ON_HOLD, False), ([1], Status.WATCHING, False), ([2], Status.DROPPED, False)])
def test_show_transitions_cannot_cascade_or_create_seasons(server, seasons, desired, blocked):
    server.change(20, status=2)
    server.rows["media/season"] = [entry(f"tv/tmdb/2/{index+1}", 50+index, status) for index, status in enumerate(seasons)]
    before = deepcopy(server.rows["media/season"])
    row = server.adapter.read()["tv/tmdb/2"]
    assert bool(server.adapter.blockers(row, desired)) is blocked
    if blocked:
        with pytest.raises(RuntimeError, match="transition_blocked"):
            server.adapter.write(row, desired)
        assert not server.writes
    else:
        server.adapter.write(row, desired)
        assert server.adapter.read()["tv/tmdb/2"].observation.protected_token == row.observation.protected_token
    assert server.rows["media/season"] == before


def test_repeated_episode_plays_do_not_block_a_status_write(server):
    server.rows["media/episode"].append({**server.rows["media/episode"][0], "consumption_id": 23})
    row = server.adapter.read()["tv/tmdb/2"]
    assert row.observation.rewatching is False
    server.adapter.write(row, Status.ON_HOLD)
    assert server.writes


@pytest.mark.parametrize("protection", ["planned", "completed", "rewatching", "playback"])
def test_protected_entries_never_write(server, protection):
    if protection in {"planned", "completed"}:
        server.change(20, status=0 if protection == "planned" else 3)
    elif protection == "rewatching":
        server.rows["media/season"][0]["rewatch_started_at"] = "now"
    else:
        server.rows["playback/progress"] = [{"media_type": "episode", "ids": {"tmdb": 2}, "position_seconds": 100}]
    row = server.adapter.read()["tv/tmdb/2"]
    with pytest.raises(RuntimeError, match="transition_blocked"):
        server.adapter.write(row, Status.ON_HOLD)
    assert not server.writes


@pytest.mark.parametrize("problem", ["missing_total", "empty_page", "duplicate_child", "changed"])
def test_failed_snapshots_do_not_become_empty_successes(server, problem):
    if problem == "duplicate_child":
        server.rows["media/episode"] *= 2
    else:
        def hook(method, path, kwargs):
            if path == "media/movie" and problem == "missing_total":
                return {"results": []}
            if path == "media/movie" and kwargs["params"]["offset"] == 1 and problem == "empty_page":
                return {"count": 2, "results": []}
            if path == "media/tv/tmdb/2/history" and problem == "changed":
                server.change(10, status=4)
        server.hook = hook
    with pytest.raises(RuntimeError):
        server.adapter.read()
    assert not server.writes


@pytest.mark.parametrize("response", [{}, {"consumption_id": 999, "status": 2}, {"consumption_id": 10, "status": 1},
                                     {"consumption_id": 10, "status": 2, "errors": ["denied"]}])
def test_patch_response_requires_the_exact_entry_and_status(server, response):
    row = server.adapter.read()["movie/tmdb/1"]
    server.hook = lambda method, path, kwargs: response if method == "PATCH" else None
    with pytest.raises(RuntimeError, match="write_rejected"):
        server.adapter.write(row, Status.ON_HOLD)


def test_unvalidated_anime_and_non_tmdb_records_are_not_mapped(server):
    server.rows["media/tv"][0]["item"]["library_media_type"] = "anime"
    server.rows["media/movie"].append(entry("movie/imdb/tt123", 99))
    assert set(server.adapter.read()) == {"movie/tmdb/1", "movie/tmdb/3"}


@pytest.mark.parametrize("response", [[], [None], {"count": 1, "results": [None]}])
def test_malformed_page_is_not_a_complete_snapshot(server, response):
    server.hook = lambda method, path, kwargs: response if path == "media/movie" else None
    with pytest.raises(RuntimeError, match="invalid_response"):
        server.adapter.read()


def test_pair_custom_watchlist_name_is_respected(server):
    server.adapter.cfg = {"_pair_providers": {"floppy": {"watchlist_name": "Later"}}}
    server.rows["lists"].append({"id": 2, "name": "Later"})
    server.rows["media/movie"][0]["lists"] = [{"list_id": 2, "list_item_id": 1}]
    assert server.adapter.read()["movie/tmdb/1"].observation.watchlisted is True
    server.rows["media/movie"][0]["lists"] = [{"list_id": 1, "list_item_id": 1}]
    assert server.adapter.read()["movie/tmdb/1"].observation.watchlisted is False


def test_adapter_passes_narrowed_instance_config(monkeypatch):
    from providers.sync import _mod_FLOPPY
    cfg = {"_cw_provider_instance": "FLOPPY-P01", "floppy": {"server_url": "http://test"}}
    calls = []
    monkeypatch.setattr(_mod_FLOPPY, "FLOPPYModule", lambda value: calls.append(value))
    _status.StatusAdapter(cfg).adapter()
    assert calls == [cfg]
    assert _mod_FLOPPY._current_instance_id(cfg) == "FLOPPY-P01"


def test_registry_exposes_only_safe_destinations():
    for caps in provider_caps("FLOPPY")["media"].values():
        assert caps["writable"] == ["watching", "on_hold", "dropped"]
        assert "planned" not in caps["readable"]


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_pair_sync_verifies_writes_and_respects_direction(server, config_base, tmp_path, mode):
    tracker = TrackerStore(tmp_path / "tracker")
    tracker.save_note("movie:tmdb:1", {"type": "movie", "tmdb": "1", "status": "on_hold"})
    cfg = {"crosswatch": {"root_dir": str(tmp_path / "tracker")}, "floppy": {}}
    pair = {"id": "floppy-pair", "source": "CROSSWATCH", "target": "FLOPPY", "mode": mode,
            "features": {"history": {"enable": True, "use_source_status": True}},
            "status_mapping": {"version": 1, "a": {"provider": "crosswatch", "instance": "default"},
                "b": {"provider": "floppy", "instance": "default"}, "conflict": "prefer_a",
                "directions": {"a_to_b": {"movies": {"on_hold": {"include": True, "target": "on_hold"}}}}}}
    if mode == "two-way":
        pair["status_mapping"]["directions"]["b_to_a"] = {"movies": {"dropped": {"include": True, "target": "dropped"}}}
    ctx = SimpleNamespace(interactive=None, dry_run=False, emit=lambda *args, **kwargs: None)
    baselines = StatusStore(tmp_path / "status.sqlite3")
    assert run_status_mappings(ctx, [pair], cfg, {}, store=baselines)["updated"] == 1
    assert run_status_mappings(ctx, [pair], cfg, {}, store=baselines)["updated"] == 0
    server.change(10, status=4)
    run_status_mappings(ctx, [pair], cfg, {}, store=baselines)
    assert tracker.note("movie:tmdb:1")["status"] == ("dropped" if mode == "two-way" else "on_hold")
    assert len(server.writes) == (1 if mode == "two-way" else 2)


def _skipped(rows):
    return {key: row.raw["_cw_skipped"] for key, row in rows.items() if isinstance(row.raw, dict) and row.raw.get("_cw_skipped")}


def test_one_bad_title_is_skipped_and_the_rest_is_read(server):
    server.rows["media/movie"][0].pop("lists")
    rows = server.adapter.read()
    skipped = _skipped(rows)
    assert list(skipped.values()) == ["floppy_status_incomplete_lists"]
    assert any(row.observation.state.value == "present" for key, row in rows.items() if key not in skipped)
    assert not server.writes
