# tests/test_mdblist_status_mapping.py
# CrossWatch - MDBList status mappings, complete reads and preserved account state.
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
from providers.sync.mdblist import _status


def show(ident):
    return {"title": "Test show", "ids": {"tmdb": ident, "imdb": f"tt{ident}"}}


def response(value, status=200):
    def check():
        if status >= 400:
            raise RuntimeError("http_failed")
    return SimpleNamespace(json=lambda: deepcopy(value), headers={}, raise_for_status=check)


@pytest.fixture
def server(monkeypatch):
    state = SimpleNamespace(rows={
        "/sync/watched": [("shows", {"show": show(i), "last_watched_at": "old"}) for i in (1, 2, 3)],
        "plays": [("episodes", {"play_id": i, "watched_at": "old", "episode": {"show": show(i),
                   "ids": {"tmdb": 10 + i}, "season": 1, "number": 1}}) for i in (1, 2, 3)],
        "/watchlist/items": [], "/sync/dropped": [], "/sync/seasons/dropped": [],
        "/sync/ratings": [("shows", {"show": show(1), "rating_precise": 7.5, "rated_at": "old"})],
        "/sync/collection": [("shows", {"show": show(1), "collected_at": "old"})]},
        native={i: {"id": i, "watched": True, "last_watched_at": "old", "watchlist": False,
                    "completed": True if i == 2 else None, "inprogress": None if i == 2 else True,
                    "watched_episode_count": 1 if i == 1 else 10, "total_episodes_aired": 10,
                    "has_unwatched_aired": i == 1, "rating_precise": 7.5} for i in (1, 2, 3)},
        calls=[], writes=[], playback=[], hook=None, activity="old", ignore_write=False)

    def get(url, **kwargs):
        path = url.removeprefix(_status.BASE)
        state.calls.append((path, deepcopy(kwargs)))
        if state.hook:
            override = state.hook(path, kwargs)
            if override is not None:
                return override
        if path == "/sync/last_activities":
            return response({"watched_at": state.activity, "watchlisted_at": "old", "dropped_at": "old", "server_time": len(state.calls)})
        if path == "/sync/playback":
            return response(state.playback)
        params = kwargs["params"]
        offset = int(params.get("cursor", "0"))
        rows = state.rows["plays" if params.get("plays") == "all" else path]
        buckets = {name: [] for name in ("movies", "shows", "seasons", "episodes")}
        for bucket, row in rows[offset:offset + 1]:
            buckets[bucket].append(row)
        more = offset + 1 < len(rows)
        return response({**buckets, "pagination": {"total": len(rows), "has_more": more,
                                                   "next_cursor": str(offset + 1) if more else None}})

    def post(url, **kwargs):
        path = url.removeprefix(_status.BASE)
        if path.startswith("/sync/state/"):
            state.calls.append((path, deepcopy(kwargs)))
            return response({"type": path.split("/")[3], "provider": "tmdb", "not_found": [],
                             "items": [state.native[i] for i in kwargs["json"]["ids"]]})
        state.writes.append((path, deepcopy(kwargs)))
        ident = kwargs["json"]["shows"][0]["ids"]["tmdb"]
        if not state.ignore_write:
            state.rows["/sync/dropped"] = [(b, r) for b, r in state.rows["/sync/dropped"] if r["show"]["ids"]["tmdb"] != ident]
            if not path.endswith("/remove"):
                state.rows["/sync/dropped"].append(("shows", {"show": show(ident), "dropped_at": "new"}))
        return response({"deleted" if path.endswith("/remove") else "updated": {"shows": 1}, "not_found": {"shows": []}})

    state.client = SimpleNamespace(get=get, post=post)
    monkeypatch.setattr(_status.StatusAdapter, "adapter", lambda self: SimpleNamespace(client=state.client))
    state.adapter = _status.StatusAdapter({})
    return state


def test_clamped_cursor_pagination_and_native_states(server):
    rows = server.adapter.read()
    assert rows["show:1"].observation.status == Status.WATCHING
    assert rows["show:2"].observation.status == Status.COMPLETED
    assert rows["show:3"].observation.state == ReadState.UNKNOWN
    assert rows["show:3"].observation.native_status == "caught_up"
    assert any(kw.get("params", {}).get("cursor") == "2" for _, kw in server.calls)
    server.rows["/sync/dropped"] = [("shows", {"show": show(1)})]
    assert server.adapter.read()["show:1"].observation.status == Status.DROPPED
    server.rows["/watchlist/items"] = [("shows", show(1))]
    server.native[1]["watchlist"] = True
    assert server.adapter.read()["show:1"].observation.status == Status.PLANNED


@pytest.mark.parametrize("watched,flags,native", [
    (10, {"inprogress": True, "completed": None, "has_unwatched_aired": None}, "caught_up"),
    (12, {"inprogress": True, "completed": None, "has_unwatched_aired": None}, "caught_up"),
    (5, {"inprogress": True, "completed": None, "has_unwatched_aired": None}, "unknown"),
    (5, {"inprogress": True, "completed": None, "has_unwatched_aired": True}, "watching"),
    (10, {"inprogress": None, "completed": True, "has_unwatched_aired": None}, "completed"),
])
def test_null_progress_flags_read_as_not_set(server, watched, flags, native):
    server.native[3].update(watched_episode_count=watched, **flags)
    assert server.adapter.read()["show:3"].observation.native_status == native


def test_drop_restore_preserves_history_dates_ratings_collection_and_progress(server):
    before = server.adapter.read()["show:1"]
    server.adapter.write(before, Status.DROPPED)
    dropped = server.adapter.read()["show:1"]
    assert dropped.observation.status == Status.DROPPED
    assert dropped.observation.protected_token == before.observation.protected_token
    server.adapter.write(dropped, Status.WATCHING)
    assert server.adapter.read()["show:1"].observation == before.observation
    assert server.writes == [("/sync/dropped", {"json": {"shows": [{"ids": {"tmdb": 1}}]}, "max_retries": 0}),
                             ("/sync/dropped/remove", {"json": {"shows": [{"ids": {"tmdb": 1}}]}, "max_retries": 0})]


@pytest.mark.parametrize("case", ["null_progress", "zero_history", "completed", "planned", "season", "repeat", "playback", "caught_up"])
def test_protected_transitions_never_write(server, case):
    if case == "null_progress":
        server.native[1]["watched_episode_count"] = None
    elif case == "zero_history":
        server.rows["plays"] = server.rows["plays"][1:]
    elif case == "completed":
        server.native[1]["completed"] = True
    elif case == "planned":
        server.native[1]["watchlist"] = True
        server.rows["/watchlist/items"] = [("shows", show(1))]
    elif case == "season":
        server.rows["/sync/seasons/dropped"] = [("seasons", {"show": show(1), "season": {"number": 2}, "dropped_at": "old"})]
    elif case == "repeat":
        server.rows["plays"].append(("episodes", {**server.rows["plays"][0][1], "play_id": 99}))
    elif case == "playback":
        server.playback = [{"type": "episode", "show": show(1), "progress": 30}]
    else:
        server.native[1]["watched_episode_count"] = 10
    server.rows["/sync/dropped"] = [("shows", {"show": show(1)})]
    record = server.adapter.read()["show:1"]
    with pytest.raises(RuntimeError, match="transition_blocked"):
        server.adapter.write(record, Status.WATCHING)
    assert not server.writes


@pytest.mark.parametrize("case", ["missing_pagination", "empty_middle", "repeated_cursor", "repeated_page", "changed_total", "bad_row", "http", "activity", "missing_state", "duplicate_state", "duplicate_play"])
def test_incomplete_or_inconsistent_reads_fail(server, case):
    if case in {"missing_state", "duplicate_state"}:
        server.client.post = lambda *a, **k: response({"type": "show", "provider": "tmdb", "not_found": [],
             "items": [] if case == "missing_state" else [server.native[1], server.native[1], server.native[3]]})
    elif case == "duplicate_play":
        server.rows["plays"].append(deepcopy(server.rows["plays"][0]))
    else:
        def hook(path, kw):
            if path == "/sync/ratings" and case == "activity":
                server.activity = "new"
            if path != "/sync/watched" or kw["params"].get("plays"):
                return None
            offset = int(kw["params"].get("cursor", "0"))
            page = {"shows": [server.rows[path][offset][1]], "pagination": {"total": 3, "has_more": offset < 2,
                    "next_cursor": str(offset + 1) if offset < 2 else None}}
            if case == "missing_pagination":
                page.pop("pagination")
            if case == "http":
                return response({}, 500)
            if offset == 1:
                if case == "empty_middle": page["shows"] = []
                if case == "repeated_cursor": page["pagination"]["next_cursor"] = "1"
                if case == "repeated_page": page["shows"] = [server.rows[path][0][1]]
                if case == "changed_total": page["pagination"]["total"] = 4
                if case == "bad_row": page["shows"] = [None]
            return response(page)
        server.hook = hook
    with pytest.raises(RuntimeError):
        server.adapter.read()
    assert not server.writes


@pytest.mark.parametrize("error", ["not_found", "errors", "error"])
def test_item_errors_reject_success_counters(server, error):
    row = server.adapter.read()["show:1"]
    server.client.post = lambda *a, **k: response({"updated": {"shows": 1}, error: {"shows": [1]}})
    with pytest.raises(RuntimeError, match="write_rejected"):
        server.adapter.write(row, Status.DROPPED)


def test_capabilities_no_completion_hold_or_movie_writes():
    caps = provider_caps("MDBLIST")["media"]
    assert caps["shows"]["writable"] == ["watching", "dropped"]
    assert caps["movies"]["writable"] == []
    assert caps["shows"]["defaults"]["dropped"]["include"] is True
    assert caps["shows"]["defaults"]["watching"]["include"] is False


def test_state_batches_are_complete_without_a_total_title_cap(server):
    titles = {str(i): {"type": "show", "ids": {"tmdb": str(i)}} for i in range(1, 206)}
    server.native = {i: {**server.native[1], "id": i} for i in range(1, 206)}
    assert len(_status.states(server.client, titles)) == 205
    assert [len(kw["json"]["ids"]) for _, kw in server.calls] == [100, 100, 5]


@pytest.mark.parametrize("field,value", [("completed", "false"), ("inprogress", 1), ("watched_episode_count", -1)])
def test_invalid_progress_is_not_coerced(server, field, value):
    server.native[1][field] = value
    rows = server.adapter.read()
    assert _skipped(rows) == {"show:1": "mdblist_status_invalid_progress"}
    assert rows["show:1"].observation.status is None


def test_movie_completion_is_read_only_and_planned_stays_frozen(server):
    movie = {"title": "Movie", "ids": {"tmdb": 4}}
    server.rows["plays"].append(("movies", {"movie": movie, "play_id": 4, "watched_at": "old"}))
    server.native[4] = {"id": 4, "watched": True, "watchlist": False}
    row = server.adapter.read()["movie:4"]
    assert row.observation.status == Status.COMPLETED
    with pytest.raises(RuntimeError, match="transition_blocked"):
        server.adapter.write(row, Status.DROPPED)
    server.rows["/watchlist/items"] = [("movies", movie)]
    server.native[4]["watchlist"] = True
    assert server.adapter.read()["movie:4"].observation.status == Status.PLANNED


def test_dropped_without_history_is_readable_but_does_not_seed_remote_state(server):
    server.rows["/sync/dropped"] = [("shows", {"show": show(4)})]
    server.native[4] = {**server.native[1], "id": 4, "watched": False, "watched_episode_count": 0}
    row = server.adapter.read()["show:4"]
    assert row.observation.status == Status.DROPPED
    with pytest.raises(RuntimeError, match="transition_blocked"):
        server.adapter.write(row, Status.WATCHING)


def test_precise_rating_and_history_edits_change_protection_token(server):
    original = server.adapter.read()["show:1"].observation.protected_token
    server.rows["/sync/ratings"][0][1]["rating_precise"] = 8
    changed = server.adapter.read()["show:1"].observation.protected_token
    assert original != changed
    server.rows["plays"][0][1]["watched_at"] = "new"
    assert changed != server.adapter.read()["show:1"].observation.protected_token


def test_narrowed_instance_config_reaches_existing_transport(monkeypatch):
    from providers.sync import _mod_MDBLIST
    calls = []
    config = {"mdblist": {}, "_cw_provider_instance": "second"}
    monkeypatch.setattr(_mod_MDBLIST, "MDBLISTModule", lambda cfg: calls.append(cfg))
    _status.StatusAdapter(config).adapter()
    assert calls == [config]


@pytest.mark.parametrize("mode,ignored", [("one-way", False), ("two-way", False), ("one-way", True)])
def test_runtime_verifies_readback_and_keeps_uncertain_writes_pending(server, config_base, tmp_path, mode, ignored):
    tracker = TrackerStore(tmp_path / "tracker")
    tracker.save_note("show:tmdb:1", {"type": "show", "tmdb": "1", "status": "dropped"})
    cfg = {"crosswatch": {"root_dir": str(tmp_path / "tracker")}, "mdblist": {}}
    pair = {"id": "mdblist-status", "source": "CROSSWATCH", "target": "MDBLIST", "mode": mode,
            "features": {"history": {"enable": True, "use_source_status": True}},
            "status_mapping": {"version": 1, "a": {"provider": "crosswatch", "instance": "default"},
                               "b": {"provider": "mdblist", "instance": "default"}, "conflict": "prefer_a",
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
        server.rows["/sync/dropped"] = []
        assert run_status_mappings(ctx, [pair], cfg, {}, store=store)["updated"] == 1
        assert tracker.note("show:tmdb:1")["status"] == "watching"
        assert len(server.writes) == 1


def _skipped(rows):
    return {key: row.raw["_cw_skipped"] for key, row in rows.items() if isinstance(row.raw, dict) and row.raw.get("_cw_skipped")}


def test_watchlist_change_during_read_skips_only_that_title(server):
    server.native[1]["watchlist"] = True
    rows = server.adapter.read()
    assert _skipped(rows) == {"show:1": "mdblist_status_watchlist_changed"}
    assert len(rows) > 1
    assert not server.writes
