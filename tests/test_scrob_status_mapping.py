# tests/test_scrob_status_mapping.py
# CrossWatch - Scrob status mapping, hidden playback and forwarding safeguards.
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
from providers.sync.scrob import _common, _status


def episode(ident):
    return {"id": ident + 1000, "type": "episode", "tmdb_id": ident + 2000, "show_tmdb_id": ident,
            "season_number": 1, "episode_number": 1}


def response(data, status=200):
    return SimpleNamespace(status_code=status, content=b"{}", text="{}", json=lambda: deepcopy(data))


@pytest.fixture
def server(monkeypatch):
    state = SimpleNamespace(data={
        "history": [{"id": i, "media": episode(i), "completed": True, "play_count": 1, "watched_at": "old"} for i in (1, 2, 3)],
        "history/dropped": {"shows": [], "movies": []}, "ratings": {"results": []},
        "history/continue-watching": {"continue_watching": []}, "history/now-playing": {"now_playing": []},
        "lists": {"lists": [{"id": 7, "name": "Watchlist", "item_count": 0}]},
        "lists/7": {"id": 7, "name": "Watchlist", "item_count": 0, "privacy_level": "private", "items": []},
        "auth/settings": {"trakt_push_dropped": False, "simkl_push_dropped": False, "mdblist_push_dropped": False}},
        shows={i: {"id": 100 + i, "tmdb_id": i, "watched": i != 1, "watch_started": True,
                   "watch_pct": 25 if i == 1 else 100, "rewatch": None, "dropped": False, "user_rating": 7.5,
                   "season_states": {"1": {"watch_pct": 25 if i == 1 else 100}}, "status": "Ended" if i == 2 else "Returning Series",
                   "episode_order": "tmdb:aired"} for i in (1, 2, 3)},
        calls=[], writes=[], hook=None, ignore_write=False)
    def request(adapter, method, path, **kwargs):
        state.calls.append((method, path, deepcopy(kwargs)))
        if state.hook:
            alternate = state.hook(method, path, kwargs)
            if alternate is not None:
                return alternate
        if method == "GET":
            if path == "history":
                page = kwargs["params"]["page"]
                rows = state.data[path]
                return response({"results": rows[page - 1:page], "page": page, "total_results": len(rows), "total_pages": len(rows)})
            if path.startswith("shows/"):
                return response(state.shows[int(path.split("/")[1])])
            if path == "history/continue-watching":
                assert kwargs["params"]["include_hidden"] == "true"
                return response({"continue_watching": state.data[path]["continue_watching"][:kwargs["params"]["limit"]]})
            return response(state.data[path])
        state.writes.append((method, path, deepcopy(kwargs)))
        if not state.ignore_write:
            if path == "history/drop/show":
                local = kwargs["params" if method == "DELETE" else "json"]["show_id"]
                ident = next(i for i, row in state.shows.items() if row["id"] == local)
                state.shows[ident]["dropped"] = method == "POST"
                state.data["history/dropped"]["shows"] = [r for r in state.data["history/dropped"]["shows"] if r["id"] != local]
                if method == "POST":
                    state.data["history/dropped"]["shows"].append({"id": local, "tmdb_id": ident})
            else:
                local = kwargs["json"]["media_id"]
                state.data["history/dropped"]["movies"].append({"id": local, "tmdb_id": 5})
        return response({"status": "ok"})
    monkeypatch.setattr(_common, "scrob_request", request)
    monkeypatch.setattr(_status, "scrob_request", request)
    state.module = SimpleNamespace(config={"scrob": {}}, instance_id="default")
    monkeypatch.setattr(_status.StatusAdapter, "adapter", lambda self: state.module)
    state.adapter = _status.StatusAdapter({})
    return state


def test_complete_history_and_status_precedence(server):
    rows = server.adapter.read()
    assert rows["show:1"].observation.status == Status.WATCHING
    assert rows["show:2"].observation.status == Status.COMPLETED
    assert rows["show:3"].observation.state == ReadState.UNKNOWN
    assert any(path == "history" and kw["params"]["page"] == 3 for _, path, kw in server.calls)


def test_long_show_with_rounded_zero_percent_reads_as_watching(server):
    server.shows[1].update(watch_pct=0, season_states={"10": {"watch_pct": 4}})
    assert server.adapter.read()["show:1"].observation.status == Status.WATCHING


def test_native_show_ids_and_preservation_round_trip(server):
    before = server.adapter.read()["show:1"]
    server.adapter.write(before, Status.DROPPED)
    dropped = server.adapter.read()["show:1"]
    assert dropped.observation.status == Status.DROPPED
    assert dropped.observation.protected_token == before.observation.protected_token
    server.adapter.write(dropped, Status.WATCHING)
    assert server.adapter.read()["show:1"].observation == before.observation
    assert server.writes == [("POST", "history/drop/show", {"json": {"show_id": 101}}),
                             ("DELETE", "history/drop/show", {"params": {"show_id": 101}})]


def test_cancellation_during_forwarding_preflight_prevents_mutation(server):
    from cw_platform.run_control import SyncCancelled, clear_cancel, request_cancel
    record = server.adapter.read()["show:1"]
    def hook(method, path, kwargs):
        if method == "GET" and path == "auth/settings":
            request_cancel()
    server.hook = hook
    clear_cancel()
    try:
        with pytest.raises(SyncCancelled):
            server.adapter.write(record, Status.DROPPED)
        assert server.writes == []
    finally:
        clear_cancel()


@pytest.mark.parametrize("case", ["rewatch", "repeat", "forwarding", "unknown_forwarding", "playback", "playing", "planned", "completed", "numbering", "zero_history"])
def test_guarded_transitions_never_write(server, case):
    if case == "rewatch":
        server.shows[1]["rewatch"] = {"started_at": "now", "watched": 0}
    elif case == "repeat":
        server.data["history"].append({**server.data["history"][0], "id": 99})
    elif case == "forwarding":
        server.data["auth/settings"]["trakt_push_dropped"] = True
    elif case == "unknown_forwarding":
        server.data["auth/settings"].pop("simkl_push_dropped")
    elif case in {"playback", "playing"}:
        path, bucket = ("history/continue-watching", "continue_watching") if case == "playback" else ("history/now-playing", "now_playing")
        server.data[path][bucket] = [{"id": 9, "media": episode(1), "progress_seconds": 400}]
    elif case == "planned":
        server.data["lists"]["lists"][0]["item_count"] = 1
        server.data["lists/7"].update(item_count=1, items=[{"id": 8, "media": {"id": 501, "type": "series", "tmdb_id": 1}}])
    elif case == "completed":
        server.shows[1].update(watched=True, watch_pct=100)
    elif case == "numbering":
        server.shows[1]["episode_order"] = "tvdb:official"
    else:
        server.data["history"] = server.data["history"][1:]
    server.shows[1]["dropped"] = True
    server.data["history/dropped"]["shows"] = [{"id": 101, "tmdb_id": 1}]
    row = server.adapter.read()["show:1"]
    with pytest.raises(RuntimeError, match="transition_blocked"):
        server.adapter.write(row, Status.WATCHING)
    assert not server.writes


def test_forwarding_is_rechecked_immediately_before_write(server):
    row = server.adapter.read()["show:1"]
    server.data["auth/settings"]["mdblist_push_dropped"] = True
    with pytest.raises(RuntimeError, match="forwarding_enabled"):
        server.adapter.write(row, Status.DROPPED)
    assert not server.writes


def test_hidden_resume_reads_grow_without_fixed_cap(server):
    rows = [{"id": i, "media": {"id": i, "type": "movie", "tmdb_id": i}} for i in range(1, 402)]
    server.data["history/continue-watching"]["continue_watching"] = rows
    assert _common.status_resume(server.module) == rows
    assert [kw["params"]["limit"] for _, path, kw in server.calls if path == "history/continue-watching"] == [100, 200, 400, 800]


def test_existing_unwatched_movie_can_be_dropped_without_creation(server):
    server.data["lists"]["lists"][0].update(name="Favourites", item_count=1)
    server.data["lists/7"].update(name="Favourites", item_count=1,
        items=[{"id": 8, "media": {"id": 505, "type": "movie", "tmdb_id": 5, "watched": False}}])
    row = server.adapter.read()["movie:5"]
    assert row.observation.state == ReadState.ABSENT
    server.adapter.write(row, Status.DROPPED)
    after = server.adapter.read()["movie:5"]
    assert after.observation.status == Status.DROPPED
    assert after.observation.protected_token == row.observation.protected_token
    assert server.writes == [("POST", "history/drop/movie", {"json": {"media_id": 505}})]
    with pytest.raises(RuntimeError, match="transition_blocked"):
        server.adapter.write(after, Status.WATCHING)


@pytest.mark.parametrize("case", ["missing_paging", "bad_row", "empty_page", "total", "page_count", "duplicate", "list_count", "show_id", "flag", "snapshot"])
def test_incomplete_or_inconsistent_reads_fail(server, case):
    if case == "duplicate":
        server.data["history"].append(deepcopy(server.data["history"][0]))
    elif case == "list_count":
        server.data["lists/7"]["item_count"] = 1
    elif case == "show_id":
        server.shows[1]["tmdb_id"] = 7
    elif case == "flag":
        server.shows[1]["dropped"] = True
    else:
        def hook(method, path, kwargs):
            if path == "shows/1" and case == "snapshot":
                server.data["history"][0]["watched_at"] = "new"
            if path == "history":
                page = kwargs["params"]["page"]
                data = {"results": server.data["history"][page-1:page], "page": page, "total_results": 3, "total_pages": 3}
                if case == "missing_paging": data.pop("total_pages")
                if page == 2:
                    if case == "bad_row": data["results"] = [None]
                    if case == "empty_page": data["results"] = []
                    if case == "total": data["total_results"] = 4
                    if case == "page_count": data["total_pages"] = 4
                return response(data)
        server.hook = hook
    with pytest.raises(RuntimeError):
        server.adapter.read()
    assert not server.writes


def test_changed_ratings_and_progress_change_protected_fingerprint(server):
    before = server.adapter.read()["show:1"].observation.protected_token
    server.shows[1]["user_rating"] = 8
    changed = server.adapter.read()["show:1"].observation.protected_token
    assert changed != before
    server.shows[1]["season_states"]["1"]["watch_pct"] = 50
    assert server.adapter.read()["show:1"].observation.protected_token != changed


def test_capabilities_keep_movie_restore_hold_and_completion_unavailable():
    caps = provider_caps("SCROB")["media"]
    assert caps["shows"]["writable"] == ["watching", "dropped"]
    assert caps["movies"]["writable"] == ["dropped"]


@pytest.mark.parametrize("body,status", [({"status": "error"}, 200), ({"status": "ok", "error": "failed"}, 200), ({}, 200), ({"status": "ok"}, 500)])
def test_failed_mutations_are_not_acknowledged(server, body, status):
    row = server.adapter.read()["show:1"]
    server.hook = lambda method, path, kwargs: response(body, status) if method != "GET" else None
    with pytest.raises(RuntimeError, match="write_"):
        server.adapter.write(row, Status.DROPPED)


def test_resume_prefix_changes_fail_instead_of_publishing_a_partial_snapshot(server):
    server.data["history/continue-watching"]["continue_watching"] = [{"id": i} for i in range(101)]
    def hook(method, path, kwargs):
        if path == "history/continue-watching" and kwargs["params"]["limit"] == 200:
            return response({"continue_watching": [{"id": i} for i in range(1, 102)]})
    server.hook = hook
    with pytest.raises(RuntimeError, match="resume_changed"):
        _common.status_resume(server.module)


def test_ratings_continuation_is_not_silently_ignored(server):
    server.data["ratings"]["next"] = "/ratings?page=2"
    with pytest.raises(RuntimeError, match="incomplete_rows"):
        server.adapter.read()


def test_empty_watchlist_is_not_created(server):
    server.data["lists"]["lists"] = []
    assert len(server.adapter.read()) == 3
    assert not server.writes


def test_tvdb_only_identity_is_not_guessed(server):
    server.data["history"].append({"id": 4, "media": {"id": 14, "type": "episode", "tvdb_id": 77, "show_tvdb_id": 88},
                                  "completed": True, "play_count": 1, "watched_at": "old"})
    assert len(server.adapter.read()) == 3
    assert not server.writes


def test_named_instance_config_reaches_the_existing_adapter(monkeypatch):
    from providers.sync import _mod_SCROB
    calls = []
    config = {"scrob": {}, "_cw_provider_instance": "secondary"}
    monkeypatch.setattr(_mod_SCROB, "SCROBModule", lambda cfg: calls.append(cfg))
    _status.StatusAdapter(config).adapter()
    assert calls == [config]


@pytest.mark.parametrize("mode,ignored", [("one-way", False), ("two-way", False), ("one-way", True)])
def test_runtime_readback_and_pending_writes(server, config_base, tmp_path, mode, ignored):
    tracker = TrackerStore(tmp_path / "tracker")
    tracker.save_note("show:tmdb:1", {"type": "show", "tmdb": "1", "status": "dropped"})
    cfg = {"crosswatch": {"root_dir": str(tmp_path / "tracker")}, "scrob": {}}
    pair = {"id": "scrob-status", "source": "CROSSWATCH", "target": "SCROB", "mode": mode,
            "features": {"history": {"enable": True, "use_source_status": True}},
            "status_mapping": {"version": 1, "a": {"provider": "crosswatch", "instance": "default"},
                               "b": {"provider": "scrob", "instance": "default"}, "conflict": "prefer_a",
                               "directions": {"a_to_b": {"shows": {"dropped": {"include": True, "target": "dropped"}}}}}}
    if mode == "two-way":
        pair["status_mapping"]["directions"]["b_to_a"] = {"shows": {"watching": {"include": True, "target": "watching"}}}
    ctx = SimpleNamespace(interactive=None, dry_run=False, emit=lambda *args, **kwargs: None)
    store = StatusStore(tmp_path / "status.sqlite3")
    server.ignore_write = ignored
    assert run_status_mappings(ctx, [pair], cfg, {}, store=store)["updated"] == (0 if ignored else 1)
    assert run_status_mappings(ctx, [pair], cfg, {}, store=store)["updated"] == 0
    assert len(server.writes) == 1
    if mode == "two-way":
        server.shows[1]["dropped"] = False
        server.data["history/dropped"]["shows"] = []
        assert run_status_mappings(ctx, [pair], cfg, {}, store=store)["updated"] == 1
        assert tracker.note("show:tmdb:1")["status"] == "watching"
