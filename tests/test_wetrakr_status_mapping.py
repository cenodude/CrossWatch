# tests/test_wetrakr_status_mapping.py
# CrossWatch - WeTrakr status mapping reads, preservation and sync integration.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from types import SimpleNamespace

import pytest

from cw_platform.orchestrator._status_mapping import run_status_mappings
from cw_platform.status_mapping import ReadState, Status
from cw_platform.status_mapping_store import StatusStore
from cw_platform.tracker_store import TrackerStore
from providers.sync._status_mapping import provider_caps
from providers.sync.wetrakr import _common, _status


def media(ident, kind="show"):
    return {"id": ident, "type": kind, "ids": {"tmdb": ident}, "title": f"Title {ident}"}


@pytest.fixture
def server(monkeypatch, config_base):
    _status._SNAPSHOTS.clear()
    state = SimpleNamespace(
        lists={"/sync/tracking/watching/shows": [media(1), media(2)],
               "/sync/tracking/dropped/movies": [media(3, "movie")],
               "/sync/tracking/watched/movies": [media(3, "movie")],
               "/sync/tracking/waiting/shows": [media(4)]},
        statuses={1: "watching", 2: "watching", 3: "dropped", 4: "waiting"},
        history=[{"id": "play1", "watched_at": "2025-01-01", "movie": media(3, "movie")}],
        notes=[{"id": 7, "target": "show", "text": "Keep private", "spoiler": False, "show": media(1), "note_updated_at": "old"}],
        ratings=[{**media(1), "interactions": {"user": {"rating": {"rating": 8, "rated_at": "old"}}}}],
        playback=[], calls=[], writes=[], activity="fixed", hook=None,
    )

    def request(adapter, method, path, **kwargs):
        state.calls.append((method, path, deepcopy(kwargs)))
        if state.hook:
            response = state.hook(method, path, kwargs)
            if response is not None:
                return response
        if method == "POST":
            state.writes.append(deepcopy(kwargs["json"]))
            group = "shows" if "shows" in kwargs["json"] else "movies"
            row = kwargs["json"][group][0]
            ident, native = row["id"], row["status"]
            for route, items in state.lists.items():
                if route.endswith("/" + group) and "/watched/" not in route:
                    items[:] = [item for item in items if item["id"] != ident]
            state.statuses[ident] = "paused" if native == "hold" else native
            state.lists.setdefault(f"/sync/tracking/{native}/{group}", []).append(media(ident, "show" if group == "shows" else "movie"))
            return SimpleNamespace(json=lambda: {"added": {"total": 1}, "notFound": {"shows": []}})
        if path == "/sync/last_activities":
            content = json.dumps([state.lists, state.statuses, state.history, state.notes, state.ratings, state.playback], sort_keys=True, default=str)
            return SimpleNamespace(json=lambda: {"all": f"{state.activity}:{hashlib.sha256(content.encode()).hexdigest()}"})
        if "/history/" in path:
            kind = "movie" if path.endswith("movies") else "episode"
            rows = [row for row in state.history if kind in row]
        elif "/playing/" in path:
            rows = [row for row in state.playback if row["type"] + "s" == path.rsplit("/", 1)[1]]
        elif path == "/sync/ratings/all":
            rows = state.ratings
        elif path == "/sync/notes/all":
            rows = state.notes
        else:
            assert kwargs["params"]["extended"] == "interactions"
            rows = [{**row, "interactions": {"user": {"tracking": {"last": {"status": state.statuses[row["id"]], "tracked_at": "now"}},
                                                       "favorite": {"value": True}}}} for row in state.lists.get(path, [])]
        rows = deepcopy(rows)
        page = kwargs["params"]["page"]
        return SimpleNamespace(json=lambda: rows[page - 1:page], headers={
            "X-Pagination-Page": str(page), "X-Pagination-Page-Count": str(max(1, len(rows))),
            "X-Pagination-Item-Count": str(len(rows)),
        })

    monkeypatch.setattr(_common, "request", request)
    monkeypatch.setattr(_status, "request", request)
    monkeypatch.setattr(_status.StatusAdapter, "adapter", lambda self: SimpleNamespace(config={"wetrakr": {"user_id": "test"}}, instance_id="default"))
    state.adapter = _status.StatusAdapter({})
    return state


def test_reads_all_pages_and_prefers_explicit_movie_status_over_historical_watches(server):
    rows = server.adapter.read()
    assert len(rows) == 4
    assert rows["movie:3"].observation.status == Status.DROPPED
    assert rows["show:4"].observation.state == ReadState.UNKNOWN
    assert rows["show:4"].observation.native_status == "waiting"
    assert any(path == "/sync/tracking/watching/shows" and opts["params"]["page"] == 2 for _, path, opts in server.calls)


def test_unchanged_activity_reuses_status_snapshot(server):
    first = server.adapter.read()
    server.calls.clear()
    assert server.adapter.read() == first
    assert [path for _, path, _ in server.calls] == ["/sync/last_activities"]
    server.notes[0]["text"] = "Changed"
    server.calls.clear()
    assert server.adapter.read()["show:1"].observation.protected_token != first["show:1"].observation.protected_token
    assert any(path == "/sync/notes/all" for _, path, _ in server.calls)


def test_moving_journal_boundary_does_not_invalidate_snapshot(server):
    def response(method, path, kwargs):
        if path == "/sync/last_activities":
            return SimpleNamespace(json=lambda: {"all": "fixed", "journal_visible_until": len(server.calls)})
    server.hook = response
    assert len(server.adapter.read()) == 4


@pytest.mark.parametrize("value", [None, [], {}, {"journal_visible_until": "now"}])
def test_invalid_activity_response_fails_closed(value):
    with pytest.raises(_common.WeTrakrSyncError, match="status_invalid_activities"):
        _status.activity_state(value)


def test_hold_alias_and_partial_write_preserve_ratings_notes_and_history(server):
    before = server.adapter.read()["show:1"]
    server.adapter.write(before, Status.ON_HOLD)
    assert server.writes == [{"shows": [{"id": 1, "status": "hold"}], "allow_rewatch": False}]
    after = server.adapter.read()["show:1"]
    assert after.observation.status == Status.ON_HOLD
    assert after.observation.protected_token == before.observation.protected_token
    server.notes[0]["text"] = "Changed"
    assert server.adapter.read()["show:1"].observation.protected_token != after.observation.protected_token


@pytest.mark.parametrize("state", ["planning", "dropped"])
def test_disagreeing_lists_are_not_resolved_by_timestamps(server, state):
    server.lists[f"/sync/tracking/{state}/shows"] = [media(1)]
    row = server.adapter.read()["show:1"]
    assert row.observation.state == ReadState.UNKNOWN


def test_planned_is_recognized_and_completed_show_cannot_be_overwritten(server):
    server.lists["/sync/tracking/planning/shows"] = [media(5)]
    server.statuses[5] = "planning"
    server.lists["/sync/tracking/watched/shows"] = [media(1)]
    rows = server.adapter.read()
    assert rows["show:5"].observation.status == Status.PLANNED
    assert rows["show:5"].observation.watchlisted is True
    assert "completion" in server.adapter.blockers(rows["show:1"], Status.DROPPED)[0]
    with pytest.raises(_common.WeTrakrSyncError, match="blocked"):
        server.adapter.write(rows["show:1"], Status.DROPPED)
    assert not server.writes


def test_playback_is_preserved_and_repeated_plays_are_not_a_rewatch(server):
    server.playback = [{**media(3, "movie"), "playback": {"status": "paused", "progress_percent": 50}}]
    row = server.adapter.read()["movie:3"]
    assert row.observation.status == Status.DROPPED
    assert "playback" in server.adapter.blockers(row, Status.DROPPED)[0]
    server.history.append({**server.history[0], "id": "play2"})
    assert server.adapter.read()["movie:3"].observation.rewatching is False


@pytest.mark.parametrize("broken", ["history", "rating", "notes", "activities", "paging"])
def test_incomplete_snapshots_fail_without_writes(server, broken):
    if broken == "history":
        server.history[0].pop("movie")
        server.history[0]["episode"] = {}
    elif broken == "rating":
        server.ratings[0]["interactions"] = {}
    elif broken == "notes":
        server.notes[0].pop("show")
    elif broken == "activities":
        def change(method, path, kwargs):
            if path == "/sync/notes/all":
                server.activity = "new"
        server.hook = change
    else:
        def empty(method, path, kwargs):
            if path == "/sync/tracking/watching/shows" and kwargs["params"]["page"] == 2:
                return SimpleNamespace(json=lambda: [], headers={"X-Pagination-Item-Count": "2", "X-Pagination-Page-Count": "2"})
        server.hook = empty
    with pytest.raises(_common.WeTrakrSyncError):
        server.adapter.read()
    assert not server.writes


@pytest.mark.parametrize("body", [{"added": {}, "errored": {"shows": [{"error": "rejected"}]}},
                                  {"added": {}, "notFound": {"shows": [{"id": 1}]}}, {}, []])
def test_http_success_with_missing_or_failed_item_is_rejected(server, body):
    row = server.adapter.read()["show:1"]
    server.hook = lambda method, path, kwargs: SimpleNamespace(json=lambda: body) if method == "POST" else None
    with pytest.raises(_common.WeTrakrSyncError, match="rejected"):
        server.adapter.write(row, Status.DROPPED)


def test_capabilities_limit_movies_and_never_offer_completion():
    caps = provider_caps("WETRAKR")["media"]
    assert caps["movies"]["writable"] == ["dropped"]
    assert caps["shows"]["writable"] == ["watching", "on_hold", "dropped"]


def test_adapter_uses_the_selected_instance(monkeypatch):
    from providers.sync import _mod_WETRAKR
    calls = []
    monkeypatch.setattr(_mod_WETRAKR, "WETRAKRModule", lambda cfg, instance_id: calls.append(instance_id))
    _status.StatusAdapter({"_cw_provider_instance": "P01"}).adapter()
    assert calls == ["P01"]


def test_native_identity_matching_preserves_wetrakr_boundaries(server):
    from cw_platform.orchestrator._status_mapping import matched_records
    from dataclasses import replace
    row = server.adapter.read()["show:1"]
    native = replace(row, item={**row.item, "ids": {"wetrakr": "1"}})
    assert list(matched_records({"a": native}, {"b": native})) == [(native, native)]
    conflict = replace(row, item={**row.item, "ids": {**row.item["ids"], "wetrakr": "999"}})
    assert not list(matched_records({"a": row}, {"b": conflict}))


def test_crosswatch_to_wetrakr_sync_is_verified_and_does_not_echo(server, config_base, tmp_path):
    store = TrackerStore(tmp_path / "tracker")
    store.save_note("show:tmdb:1", {"type": "show", "tmdb": "1", "status": "on_hold"})
    cfg = {"crosswatch": {"root_dir": str(tmp_path / "tracker")}, "wetrakr": {}}
    pair = {"id": "status-pair", "source": "CROSSWATCH", "target": "WETRAKR", "mode": "two-way",
            "features": {"history": {"enable": True, "use_source_status": True}},
            "status_mapping": {"version": 1, "a": {"provider": "crosswatch", "instance": "default"},
                               "b": {"provider": "wetrakr", "instance": "default"}, "conflict": "prefer_a",
                               "directions": {"a_to_b": {"shows": {"on_hold": {"include": True, "target": "on_hold"}}},
                                              "b_to_a": {"shows": {"dropped": {"include": True, "target": "dropped"}}}}}}
    ctx = SimpleNamespace(interactive=None, dry_run=False, emit=lambda *args, **kwargs: None)
    baselines = StatusStore(tmp_path / "statuses.sqlite3")
    assert run_status_mappings(ctx, [pair], cfg, {}, store=baselines)["updated"] == 1
    assert run_status_mappings(ctx, [pair], cfg, {}, store=baselines)["updated"] == 0
    assert len(server.writes) == 1
    server.statuses[1] = "dropped"
    server.lists["/sync/tracking/hold/shows"] = []
    server.lists["/sync/tracking/dropped/shows"] = [media(1)]
    assert run_status_mappings(ctx, [pair], cfg, {}, store=baselines)["updated"] == 1
    assert store.note("show:tmdb:1")["status"] == "dropped"
    assert len(server.writes) == 1
