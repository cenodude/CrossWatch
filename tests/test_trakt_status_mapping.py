# tests/test_trakt_status_mapping.py
# CrossWatch - Trakt status mapping semantics, preservation and pair execution.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from types import SimpleNamespace

import pytest

from cw_platform.orchestrator._status_mapping import matched_records, run_status_mappings
from cw_platform.status_mapping import ReadState, Status
from cw_platform.status_mapping_store import StatusStore
from cw_platform.tracker_store import TrackerStore
from providers.sync._status_mapping import provider_caps
from providers.sync.trakt import _status


def show(ident, status="returning series"):
    return {"title": "Test show", "ids": {"trakt": ident, "tmdb": ident}, "status": status}


def response(value, headers=None, status=200):
    def check():
        if status >= 400:
            raise RuntimeError("http_failed")
    return SimpleNamespace(status_code=status, json=lambda: deepcopy(value), headers=headers or {}, raise_for_status=check)


def activities(state):
    stamp = f"{state.activity}:{hashlib.sha256(json.dumps(state.rows, sort_keys=True, default=str).encode()).hexdigest()}"
    sections = {}
    for _, _, fields in _status.LISTS.values():
        for section, field in fields:
            sections.setdefault(section, {})[field] = stamp
    sections.setdefault("shows", {})["hidden_at"] = stamp
    sections.setdefault("seasons", {})["hidden_at"] = stamp
    return {"all": stamp, **sections}


@pytest.fixture
def server(monkeypatch):
    _status._CACHE.clear()
    state = SimpleNamespace(rows={
        "/sync/history": [{"id": 1, "watched_at": "old", "show": show(1), "episode": {"ids": {"trakt": 11}, "season": 1, "number": 1}},
                          {"id": 2, "watched_at": "old", "show": show(2, "ended"), "episode": {"ids": {"trakt": 21}, "season": 1, "number": 1}},
                          {"id": 3, "watched_at": "old", "show": show(3), "episode": {"ids": {"trakt": 31}, "season": 1, "number": 1}}],
        "/sync/watchlist": [], "/users/hidden/dropped": [], "/sync/playback": [],
        "/sync/ratings": [{"show": show(1), "rating": 8, "rated_at": "old"}],
        "/users/me/notes/all": [{"id": 99, "notes": "Private note", "privacy": "private"}]},
        progress={1: {"aired": 10, "completed": 1, "reset_at": None, "seasons": []},
                  2: {"aired": 10, "completed": 10, "reset_at": None, "seasons": []},
                  3: {"aired": 10, "completed": 10, "reset_at": None, "seasons": []}},
        calls=[], writes=[], hook=None, activity="fixed", ignore_write=False)
    def get(url, **kwargs):
        path = url.removeprefix(_status.BASE)
        state.calls.append((path, deepcopy(kwargs)))
        if state.hook:
            result = state.hook(path, kwargs)
            if result is not None:
                return result
        if path == "/sync/last_activities":
            return response(activities(state))
        if path.startswith("/shows/"):
            assert kwargs["params"] == {"hidden": "true", "specials": "false"}
            return response(state.progress[int(path.split("/")[2])])
        rows = state.rows[path]
        page = kwargs["params"]["page"]
        return response(rows[page-1:page], {"X-Pagination-Page": str(page), "X-Pagination-Page-Count": str(len(rows)),
                                           "X-Pagination-Item-Count": str(len(rows))})
    def post(url, **kwargs):
        state.writes.append((url.removeprefix(_status.BASE), deepcopy(kwargs)))
        ident = kwargs["json"]["shows"][0]["ids"]["trakt"]
        remove = url.endswith("/remove")
        if not state.ignore_write:
            state.rows["/users/hidden/dropped"] = [row for row in state.rows["/users/hidden/dropped"] if row["show"]["ids"]["trakt"] != ident]
            if not remove:
                state.rows["/users/hidden/dropped"].append({"show": show(ident), "hidden_at": "new"})
        return response({"deleted" if remove else "added": {"shows": 1}, "not_found": {"shows": []}})
    state.client = SimpleNamespace(get=get, post=post)
    monkeypatch.setattr(_status.StatusAdapter, "adapter", lambda self: SimpleNamespace(client=state.client))
    state.adapter = _status.StatusAdapter({})
    return state


def fixed_activities(server, base=None, **moved):
    current = deepcopy(base) if base is not None else activities(server)
    for name, stamp in moved.items():
        section, field = name.split("__")
        current[section][field] = stamp
    server.hook = lambda path, kwargs: response(current) if path == "/sync/last_activities" else None
    return current


def test_unchanged_activity_reads_nothing_but_last_activities(server):
    first = server.adapter.read()
    server.calls.clear()
    assert server.adapter.read() == first
    assert [path for path, _ in server.calls] == ["/sync/last_activities"]


def test_dropped_change_rereads_only_the_dropped_list(server):
    base = fixed_activities(server)
    server.adapter.read()
    server.rows["/users/hidden/dropped"] = [{"show": show(1), "hidden_at": "new"}]
    fixed_activities(server, base, shows__dropped_at="moved")
    server.calls.clear()
    assert server.adapter.read()["show:1"].observation.status == Status.DROPPED
    assert {path for path, _ in server.calls} == {"/sync/last_activities", "/users/hidden/dropped"}


def test_hidden_season_change_refreshes_show_progress(server):
    base = fixed_activities(server)
    first = server.adapter.read()["show:1"].observation.protected_token
    server.progress[1]["hidden_seasons"] = [{"number": 1}]
    fixed_activities(server, base, seasons__hidden_at="moved")
    server.calls.clear()
    assert server.adapter.read()["show:1"].observation.protected_token != first
    assert "/shows/1/progress/watched" in {path for path, _ in server.calls}


def test_airing_progress_refreshes_daily_and_ended_shows_weekly(server, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(_status.time, "time", lambda: now[0])
    fixed_activities(server)
    server.adapter.read()
    progress_calls = lambda: sorted(path for path, _ in server.calls if path.startswith("/shows/"))
    now[0] += _status.MAX_AGE - 60
    server.calls.clear()
    server.adapter.read()
    assert progress_calls() == []
    now[0] += 120
    server.progress[1]["aired"] = 11
    server.calls.clear()
    server.adapter.read()
    assert progress_calls() == ["/shows/1/progress/watched", "/shows/3/progress/watched"]
    now[0] += _status.ENDED_PROGRESS_MAX_AGE
    server.calls.clear()
    server.adapter.read()
    assert "/shows/2/progress/watched" in progress_calls()


def test_complete_pagination_and_status_precedence(server):
    rows = server.adapter.read()
    assert rows["show:1"].observation.status == Status.WATCHING
    assert rows["show:2"].observation.status == Status.COMPLETED
    assert rows["show:3"].observation.state == ReadState.UNKNOWN
    assert any(path == "/sync/history" and kw["params"]["page"] == 3 for path, kw in server.calls)
    server.rows["/users/hidden/dropped"] = [{"show": show(1)}]
    assert server.adapter.read()["show:1"].observation.status == Status.DROPPED
    server.rows["/sync/watchlist"] = [{"show": show(1)}]
    row = server.adapter.read()["show:1"]
    assert row.observation.status == Status.PLANNED
    assert server.adapter.blockers(row, Status.DROPPED)


def test_drop_and_restore_preserve_all_observed_content(server):
    before = server.adapter.read()["show:1"]
    server.adapter.write(before, Status.DROPPED)
    dropped = server.adapter.read()["show:1"]
    assert dropped.observation.status == Status.DROPPED
    assert dropped.observation.protected_token == before.observation.protected_token
    server.adapter.write(dropped, Status.WATCHING)
    restored = server.adapter.read()["show:1"]
    assert restored.observation == before.observation
    assert server.writes == [("/users/hidden/dropped", {"json": {"shows": [{"ids": {"trakt": 1}}]}, "max_retries": 1}),
                             ("/users/hidden/dropped/remove", {"json": {"shows": [{"ids": {"trakt": 1}}]}, "max_retries": 1})]


def test_stale_early_counts_do_not_hide_later_history(server):
    def hook(path, kwargs):
        if path == "/sync/history":
            page = kwargs["params"]["page"]
            total = 2 if page < 3 else 3
            return response(server.rows[path][page-1:page], {"X-Pagination-Page": str(page),
                            "X-Pagination-Item-Count": str(total), "X-Pagination-Page-Count": str(total)})
    server.hook = hook
    assert len(server.adapter.read()) == 3
    assert any(path == "/sync/history" and kw["params"]["page"] == 4 for path, kw in server.calls)


@pytest.mark.parametrize("case", ["reset", "repeat", "playback", "planned", "completed", "zero_history"])
def test_unsafe_transitions_never_write(server, case):
    if case == "reset":
        server.progress[1]["reset_at"] = "now"
    elif case == "repeat":
        server.rows["/sync/history"].append({**server.rows["/sync/history"][0], "id": 4})
    elif case == "playback":
        server.rows["/sync/playback"] = [{"id": 9, "show": show(1), "progress": 50}]
    elif case == "planned":
        server.rows["/sync/watchlist"] = [{"show": show(1)}]
    elif case == "completed":
        server.rows["/users/hidden/dropped"] = [{"show": show(1, "ended")}]
        server.progress[1]["completed"] = 10
    else:
        server.rows["/users/hidden/dropped"] = [{"show": show(1)}]
        server.rows["/sync/history"] = server.rows["/sync/history"][1:]
        server.progress[1]["completed"] = 0
    row = server.adapter.read()["show:1"]
    with pytest.raises(RuntimeError, match="transition_blocked"):
        server.adapter.write(row, Status.WATCHING if case == "zero_history" else Status.DROPPED)
    assert not server.writes


@pytest.mark.parametrize("case", ["missing_header", "empty_page", "changed_total", "bad_row", "activity", "duplicate_history"])
def test_failed_reads_are_not_complete_snapshots(server, case):
    if case == "duplicate_history":
        server.rows["/sync/history"].append(deepcopy(server.rows["/sync/history"][0]))
    else:
        def hook(path, kwargs):
            if path == "/sync/ratings" and case == "activity":
                server.activity = "changed"
            if path == "/sync/history":
                page = kwargs["params"]["page"]
                headers = {"X-Pagination-Page": str(page), "X-Pagination-Page-Count": "3", "X-Pagination-Item-Count": "3"}
                if case == "missing_header":
                    return response([])
                if case == "bad_row":
                    return response([None], headers)
                if page == 2 and case == "empty_page":
                    return response([], headers)
                if page == 2 and case == "changed_total":
                    return response([server.rows[path][1]], {**headers, "X-Pagination-Item-Count": "4"})
        server.hook = hook
    with pytest.raises(RuntimeError):
        server.adapter.read()
    assert not server.writes


def test_movies_are_read_only_and_no_on_hold_destination():
    caps = provider_caps("TRAKT")["media"]
    assert caps["movies"]["writable"] == []
    assert caps["shows"]["writable"] == ["watching", "dropped"]


def test_episode_watchlist_entries_protect_their_parent_without_colliding(server):
    server.rows["/sync/watchlist"] = [{"show": show(1), "episode": {"number": number}} for number in (1, 2)]
    row = server.adapter.read()["show:1"]
    assert row.observation.status == Status.PLANNED
    assert row.observation.watchlisted is True


def test_notes_and_ratings_are_included_in_preservation_fingerprint(server):
    before = server.adapter.read()["show:1"]
    server.rows["/users/me/notes/all"][0]["privacy"] = "public"
    assert server.adapter.read()["show:1"].observation.protected_token != before.observation.protected_token


@pytest.mark.parametrize("body", [{}, {"added": {"shows": 1}, "not_found": {"shows": [{"ids": {"trakt": 1}}]}},
                                  {"added": {"shows": 1}, "errors": ["denied"]}])
def test_success_status_with_rejected_items_raises(server, body):
    row = server.adapter.read()["show:1"]
    server.client.post = lambda *args, **kwargs: response(body)
    with pytest.raises(RuntimeError, match="write_rejected"):
        server.adapter.write(row, Status.DROPPED)


def test_adapter_retains_explicit_instance_for_auth_refresh(monkeypatch):
    from providers.sync import _mod_TRAKT
    cfg = {"_cw_provider_instance": "TRAKT-P01", "trakt": {"client_id": "test", "access_token": "test"}}
    calls = []
    monkeypatch.setattr(_mod_TRAKT, "TRAKTModule", lambda config, connect: calls.append((config, connect)))
    _status.StatusAdapter(cfg).adapter()
    assert calls == [(cfg, False)]


def test_native_trakt_ids_match_without_external_ids(server):
    rows = server.adapter.read()
    row = rows["show:1"]
    row.item["ids"] = {"trakt": "1"}
    assert list(matched_records({"a": row}, {"b": row})) == [(row, row)]


@pytest.mark.parametrize("mode,ignored", [("one-way", False), ("two-way", False), ("one-way", True)])
def test_runtime_verifies_readback_instead_of_success_counters(server, config_base, tmp_path, mode, ignored):
    tracker = TrackerStore(tmp_path / "tracker")
    tracker.save_note("show:tmdb:1", {"type": "show", "tmdb": "1", "status": "dropped"})
    cfg = {"crosswatch": {"root_dir": str(tmp_path / "tracker")}, "trakt": {}}
    pair = {"id": "trakt-status", "source": "CROSSWATCH", "target": "TRAKT", "mode": mode,
            "features": {"history": {"enable": True, "use_source_status": True}},
            "status_mapping": {"version": 1, "a": {"provider": "crosswatch", "instance": "default"},
                               "b": {"provider": "trakt", "instance": "default"}, "conflict": "prefer_a",
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
        server.rows["/users/hidden/dropped"] = []
        assert run_status_mappings(ctx, [pair], cfg, {}, store=store)["updated"] == 1
        assert tracker.note("show:tmdb:1")["status"] == "watching"
        assert len(server.writes) == 1


def _skipped(rows):
    return {key: row.raw["_cw_skipped"] for key, row in rows.items() if isinstance(row.raw, dict) and row.raw.get("_cw_skipped")}


def test_invalid_show_progress_skips_only_that_show(server):
    server.progress[1].pop("reset_at")
    rows = server.adapter.read()
    skipped = _skipped(rows)
    assert list(skipped.values()) == ["trakt_status_invalid_progress"]
    assert len(rows) > len(skipped)
    assert not server.writes
