# tests/test_publicmetadb_status_mapping.py
# CrossWatch - PublicMetaDB status mapping progress, preservation and pair execution.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from cw_platform.orchestrator._status_mapping import run_status_mappings
from cw_platform.status_mapping import ReadState, Status
from cw_platform.status_mapping_store import StatusStore
from cw_platform.tracker_store import TrackerStore
from providers.sync._status_mapping import provider_caps
from providers.sync.publicmetadb import _status


def history(ident, episode=1):
    return {"id": f"{ident}-{episode}", "tmdb_id": ident, "media_type": "tv", "season": 1, "episode": episode, "watched_at": "old"}


@pytest.fixture
def server(monkeypatch):
    state = SimpleNamespace(rows={
        "watched": [history(1), history(2), history(2, 2), history(3), history(3, 2)],
        "watched/shows": [{"tmdb_id": i, "watched_episodes": 1 if i == 1 else 2, "last_watched_at": "old"} for i in (1, 2, 3)],
        "resume": [], "dropped": [], "lists": [{"id": "watch", "name": "Watchlist", "type": "watchlist", "is_public": False}],
        "lists/watch/items": []}, calls=[], writes=[], hook=None, ignore_write=False,
        details={i: {"status": "Ended" if i == 2 else "Returning Series", "number_of_seasons": 1, "number_of_episodes": 2,
                     "seasons": [{"season_number": 1, "episode_count": 2}]} for i in (1, 2, 3)},
        episodes={i: [{"episode_number": n, "air_date": "2020-01-01"} for n in (1, 2)] for i in (1, 2, 3)})
    def read_json(path, **kwargs):
        name = path.removeprefix(_status.BASE + "/")
        state.calls.append((name, deepcopy(kwargs)))
        page = kwargs["params"]["page"]
        rows = state.rows[name]
        data = {"items": deepcopy(rows[page - 1:page]), "page": page, "total": len(rows), "totalPages": len(rows)}
        if state.hook:
            alternate = state.hook(name, page, data)
            if alternate is not None:
                return alternate
        return data
    def request(method, path, **kwargs):
        state.writes.append((method, path, deepcopy(kwargs)))
        ident = kwargs["json"]["tmdb_id"] if method == "POST" else int(path.split("/")[-2])
        if not state.ignore_write:
            state.rows["dropped"] = [r for r in state.rows["dropped"] if r["tmdb_id"] != ident]
            if method == "POST":
                state.rows["dropped"].append({"id": f"drop-{ident}", "tmdb_id": ident, "media_type": "tv", "created": "new"})
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"success": True})
    def lookup(entity, ident, *, season=None, fresh):
        assert entity == "show" and fresh is True
        ident = int(ident)
        value = state.details[ident] if season is None else {"episodes": state.episodes[ident]}
        return SimpleNamespace(data=deepcopy(value))
    state.module = SimpleNamespace(config={"publicmetadb": {}, "tmdb": {"api_key": "test"}},
                                   client=SimpleNamespace(read_json=read_json, request=request))
    monkeypatch.setattr(_status.StatusAdapter, "adapter", lambda self: state.module)
    monkeypatch.setattr(_status, "_tmdb_metadata_provider", lambda adapter: SimpleNamespace(lookup=lookup))
    state.adapter = _status.StatusAdapter({})
    return state


def test_native_statuses_use_complete_history_and_metadata(server):
    rows = server.adapter.read()
    assert rows["show:1"].observation.status == Status.WATCHING
    assert rows["show:2"].observation.status == Status.COMPLETED
    assert rows["show:3"].observation.state == ReadState.UNKNOWN
    assert any(name == "watched/shows" and kw["params"]["page"] == 3 for name, kw in server.calls)
    server.rows["dropped"] = [{"tmdb_id": 4, "media_type": "tv"}]
    row = server.adapter.read()["show:4"]
    assert row.observation.status == Status.DROPPED
    with pytest.raises(RuntimeError, match="transition_blocked"):
        server.adapter.write(row, Status.WATCHING)


def test_drop_restore_preserve_snapshot_and_exact_write_payload(server):
    original = server.adapter.read()["show:1"]
    server.adapter.write(original, Status.DROPPED)
    dropped = server.adapter.read()["show:1"]
    assert dropped.observation.status == Status.DROPPED
    assert dropped.observation.protected_token == original.observation.protected_token
    server.adapter.write(dropped, Status.WATCHING)
    assert server.adapter.read()["show:1"].observation == original.observation
    assert server.writes == [("POST", "/api/external/dropped", {"max_retries": 1, "json": {"tmdb_id": 1, "media_type": "tv"}}),
                             ("DELETE", "/api/external/dropped/1/tv", {"max_retries": 1})]


@pytest.mark.parametrize("case", ["planned", "completed", "repeat", "playback", "missing_metadata", "future", "today", "missing_date", "invalid_coordinates", "caught_up"])
def test_unsafe_transitions_are_preserved(server, case):
    if case == "planned":
        server.rows["lists/watch/items"] = [{"tmdb_id": 1, "media_type": "tv"}]
    elif case in {"completed", "caught_up"}:
        server.episodes[1] = server.episodes[1][:1]
        server.details[1]["seasons"][0]["episode_count"] = 1
        server.details[1]["number_of_episodes"] = 1
        server.details[1]["status"] = "Ended" if case == "completed" else "Returning Series"
    elif case == "repeat":
        server.rows["watched"].append({**history(1), "id": "repeat", "watched_at": "later"})
    elif case == "playback":
        server.rows["resume"] = [{"id": "resume", "tmdb_id": 1, "media_type": "tv", "position_ms": 2000}]
    elif case == "missing_metadata":
        server.module.config["tmdb"] = {}
    elif case == "invalid_coordinates":
        server.episodes[1][0]["episode_number"] = 9
    else:
        server.episodes[1][1]["air_date"] = {"future": "2999-01-01", "today": datetime.now(timezone.utc).date().isoformat(), "missing_date": None}[case]
    server.rows["dropped"] = [{"tmdb_id": 1, "media_type": "tv"}]
    row = server.adapter.read()["show:1"]
    with pytest.raises(RuntimeError, match="transition_blocked"):
        server.adapter.write(row, Status.WATCHING)
    assert not server.writes


@pytest.mark.parametrize("case", ["missing_paging", "bad_row", "empty_middle", "changed_total", "repeated_page", "wrong_page", "history_duplicate", "snapshot_changed"])
def test_failed_reads_do_not_publish_partial_snapshots(server, case):
    if case == "history_duplicate":
        server.rows["watched"].append(deepcopy(server.rows["watched"][0]))
    else:
        def hook(name, page, data):
            if name == "watched":
                if case == "missing_paging": data.pop("totalPages")
                if page == 2:
                    if case == "bad_row": data["items"] = [None]
                    if case == "empty_middle": data["items"] = []
                    if case == "changed_total": data["total"] += 1
                    if case == "repeated_page": data["items"] = [deepcopy(server.rows["watched"][0])]
                    if case == "wrong_page": data["page"] = 1
            if case == "snapshot_changed" and name == "lists":
                server.rows["watched"][0]["watched_at"] = "new"
            return data
        server.hook = hook
    with pytest.raises(RuntimeError):
        server.adapter.read()
    assert not server.writes


def test_later_pages_can_omit_unchanged_page_counts(server):
    def hook(name, page, data):
        if page > 1:
            data.pop("totalPages")
        return data
    server.hook = hook
    assert len(server.adapter.read()) == 3


def test_no_watchlist_creation_and_custom_list_membership_is_preserved(server):
    server.rows["lists"] = [{"id": "custom", "name": "Favourites", "type": "custom", "is_public": False}]
    server.rows["lists/custom/items"] = [{"tmdb_id": 1, "media_type": "tv", "notes": "Private", "position": 2}]
    row = server.adapter.read()["show:1"]
    assert row.observation.status == Status.WATCHING
    server.rows["lists/custom/items"][0]["notes"] = "Edited"
    assert row.observation.protected_token != server.adapter.read()["show:1"].observation.protected_token
    server.module.config["publicmetadb"]["watchlist_list_id"] = "custom"
    assert server.adapter.read()["show:1"].observation.status == Status.PLANNED
    assert not server.writes


@pytest.mark.parametrize("case", ["missing", "ambiguous"])
def test_unresolved_watchlist_selection_fails_closed(server, case):
    if case == "missing":
        server.module.config["publicmetadb"]["watchlist_list_id"] = "missing"
    else:
        server.rows["lists"].append({"id": "second", "name": "Watchlist", "type": "custom"})
        server.rows["lists/second/items"] = []
    with pytest.raises(RuntimeError, match="watchlist"):
        server.adapter.read()


def test_movie_flags_do_not_enable_movie_writes(server):
    server.rows["watched"].append({"id": "movie", "tmdb_id": 5, "media_type": "movie", "watched_at": "old"})
    row = server.adapter.read()["movie:5"]
    assert row.observation.status == Status.COMPLETED
    with pytest.raises(RuntimeError, match="transition_blocked"):
        server.adapter.write(row, Status.DROPPED)
    caps = provider_caps("PUBLICMETADB")["media"]
    assert caps["shows"]["writable"] == ["watching", "dropped"]
    assert caps["movies"]["writable"] == []


@pytest.mark.parametrize("body", [{"success": False}, {"success": True, "error": "failed"}, {}, []])
def test_write_rejections_do_not_count_as_success(server, body):
    row = server.adapter.read()["show:1"]
    server.module.client.request = lambda *a, **k: SimpleNamespace(raise_for_status=lambda: None, json=lambda: body)
    with pytest.raises(RuntimeError, match="write_rejected"):
        server.adapter.write(row, Status.DROPPED)


def test_mutation_transport_uses_one_attempt(monkeypatch):
    from providers.sync import _mod_PUBLICMETADB as module
    calls = []
    monkeypatch.setattr(module, "request_with_retries", lambda *a, **k: calls.append((a, k)))
    client = object.__new__(module.PUBLICMETADBClient)
    client.cfg = SimpleNamespace(base_url="https://example.test", timeout=10, max_retries=3)
    client.session = object()
    client.request("DELETE", "/api/external/dropped/1/tv", max_retries=1)
    assert calls[0][1]["max_retries"] == 1
    client.get("/api/external/dropped")
    assert calls[1][1]["max_retries"] == 3


@pytest.mark.parametrize("case", ["season_count", "episode_count", "truncated_season", "duplicate_episode", "bad_date", "unknown_date"])
def test_incomplete_metadata_never_infers_completion(server, case):
    if case == "season_count":
        server.details[2]["number_of_seasons"] = 2
    elif case == "episode_count":
        server.details[2]["number_of_episodes"] = 3
    elif case == "truncated_season":
        server.episodes[2].pop()
    elif case == "duplicate_episode":
        server.episodes[2][1]["episode_number"] = 1
    else:
        server.episodes[2][1]["air_date"] = "invalid" if case == "bad_date" else None
    assert server.adapter.read()["show:2"].observation.state == ReadState.UNKNOWN


def test_status_cancellation_propagates_through_metadata(server, monkeypatch):
    from cw_platform.run_control import SyncCancelled
    def cancelled():
        raise SyncCancelled()
    monkeypatch.setattr(_status, "raise_if_cancelled", cancelled)
    with pytest.raises(SyncCancelled):
        server.adapter.read()


def test_overlapping_list_pages_cannot_hide_planned_membership(server):
    server.rows["lists/watch/items"] = [{"id": "first", "tmdb_id": 1, "media_type": "tv"},
                                         {"id": "different", "tmdb_id": 1, "media_type": "tv"}]
    with pytest.raises(RuntimeError, match="duplicate"):
        server.adapter.read()


def test_narrowed_instance_and_metadata_configuration_are_retained(monkeypatch):
    from providers.sync import _mod_PUBLICMETADB as module
    calls = []
    cfg = {"publicmetadb": {"api_key": "second"}, "_cw_provider_instance": "second", "tmdb": {"api_key": "metadata"}}
    monkeypatch.setattr(module, "PUBLICMETADBModule", lambda config: calls.append(config))
    _status.StatusAdapter(cfg).adapter()
    assert calls == [cfg]


@pytest.mark.parametrize("mode,ignored", [("one-way", False), ("two-way", False), ("one-way", True)])
def test_shared_runtime_verifies_reads_and_does_not_retry_uncertain_writes(server, config_base, tmp_path, mode, ignored):
    tracker = TrackerStore(tmp_path / "tracker")
    tracker.save_note("show:tmdb:1", {"type": "show", "tmdb": "1", "status": "dropped"})
    cfg = {"crosswatch": {"root_dir": str(tmp_path / "tracker")}, "publicmetadb": {}}
    pair = {"id": "publicmetadb-status", "source": "CROSSWATCH", "target": "PUBLICMETADB", "mode": mode,
            "features": {"history": {"enable": True, "use_source_status": True}},
            "status_mapping": {"version": 1, "a": {"provider": "crosswatch", "instance": "default"},
                               "b": {"provider": "publicmetadb", "instance": "default"}, "conflict": "prefer_a",
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
        server.rows["dropped"] = []
        assert run_status_mappings(ctx, [pair], cfg, {}, store=store)["updated"] == 1
        assert tracker.note("show:tmdb:1")["status"] == "watching"
        assert len(server.writes) == 1


def _skipped(rows):
    return {key: row.raw["_cw_skipped"] for key, row in rows.items() if isinstance(row.raw, dict) and row.raw.get("_cw_skipped")}


def test_inconsistent_show_progress_skips_only_that_show(server):
    server.rows["watched/shows"][0]["watched_episodes"] = 2
    rows = server.adapter.read()
    skipped = _skipped(rows)
    assert list(skipped.values()) == ["publicmetadb_status_inconsistent_progress"]
    assert len(rows) == 3
    assert not server.writes
