# tests/test_status_mapping_adapters.py
# CrossWatch - Native status adapter payloads and preservation boundaries.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import pytest

from cw_platform.status_mapping import Status, ReadState, Observation
from cw_platform.tracker_store import TrackerStore, TrackerConflict
from providers.sync._status_mapping import Record, provider_caps
from providers.sync.anilist._status import StatusAdapter as AniList
from providers.sync.kitsu._status import StatusAdapter as Kitsu
from providers.sync.myanimelist._status import StatusAdapter as MAL
from providers.sync.simkl._status import StatusAdapter as SIMKL
from providers.sync.crosswatch._status import StatusAdapter as CW


def test_mal_zero_progress_reads_and_partial_write(monkeypatch):
    entry = {"status": "dropped", "score": 8, "comments": "Keep", "num_episodes_watched": 0, "is_rewatching": False, "updated_at": "old",
             "num_times_rewatched": 0, "rewatch_value": 0, "priority": 0, "tags": []}
    media = {"id": 1, "title": "Anime", "media_type": "tv", "num_episodes": 12}
    writes = []
    client = SimpleNamespace(entries=lambda: [(deepcopy(entry), media)], media=lambda ident: {**media, "my_list_status": deepcopy(entry)}, save=lambda *args: writes.append(args))
    monkeypatch.setattr(MAL, "client", lambda self: client)
    adapter = MAL({})
    before = adapter.read()["1"]
    assert before.observation.status == Status.DROPPED
    adapter.write(before, Status.ON_HOLD)
    assert writes == [("1", entry, {"status": "on_hold"})]
    entry["updated_at"] = "new"
    assert adapter.read()["1"].observation == before.observation
    entry["comments"] = "Edited"
    assert adapter.read()["1"].observation.protected_token != before.observation.protected_token


def test_kitsu_dates_rewatch_and_progress_preserved(monkeypatch):
    attributes = {"status": "on_hold", "progress": 0, "reconsuming": False, "notes": "Keep", "ratingTwenty": 16, "startedAt": None,
                  "reconsumeCount": 0, "private": False, "finishedAt": None, "progressedAt": None}
    entry = {"id": "55", "attributes": attributes}
    media = {"id": "1", "attributes": {"subtype": "TV", "canonicalTitle": "Anime"}}
    writes = []
    client = SimpleNamespace(entries=lambda: [(deepcopy(entry), media)], save=lambda *args: writes.append(args))
    monkeypatch.setattr(Kitsu, "client", lambda self: client)
    adapter = Kitsu({})
    record = adapter.read()["1"]
    assert "start date" in adapter.blockers(record, Status.WATCHING)[0]
    adapter.write(record, Status.DROPPED)
    assert writes == [("1", entry, {"status": "dropped"})]
    attributes.update(progress=1, startedAt="2025-01-01", reconsuming=True)
    assert "history date" in adapter.blockers(adapter.read()["1"], Status.DROPPED)[0]
    attributes["progressedAt"] = "2025-01-02"
    assert "Rewatching" in adapter.blockers(adapter.read()["1"], Status.DROPPED)[0]


def test_anilist_exact_entry_mutation_and_history_date_guard(monkeypatch):
    entry = {"id": 123, "mediaId": 1, "status": "PAUSED", "progress": 1, "repeat": 2,
             "notes": "Keep", "score": 80, "private": True, "completedAt": {"year": None, "month": None, "day": None},
             "startedAt": {"year": None, "month": None, "day": None}, "hiddenFromStatusLists": False, "customLists": {}, "advancedScores": {},
             "media": {"id": 1, "idMal": 2, "format": "TV", "title": {"english": "Anime"}}}
    calls = []
    def gql(query, variables, **kwargs):
        calls.append((query, variables))
        return {"MediaListCollection": {"lists": [{"entries": [deepcopy(entry)]}]}}
    monkeypatch.setattr(AniList, "client", lambda self: SimpleNamespace(viewer=lambda: {"id": 9}, gql=gql))
    adapter = AniList({})
    record = adapter.read()["1"]
    assert record.item["ids"] == {"anilist": "1", "mal": "2"}
    assert "history date" in adapter.blockers(record, Status.DROPPED)[0]
    entry["progress"] = 0
    record = adapter.read()["1"]
    assert adapter.blockers(record, Status.DROPPED) == ()
    adapter.write(record, Status.DROPPED)
    assert calls[-1][1] == {"id": 123, "status": "DROPPED"}


def test_simkl_uses_exact_bucket_and_preserves_dates(monkeypatch):
    from providers.sync.simkl import _history
    data = {"movies": [], "shows": [{"status": "hold", "last_watched_at": "2026-01-01", "show": {"title": "Show", "ids": {"simkl": 1, "tmdb": 2}}}], "anime": []}
    requests = []
    def request(*args, **kwargs):
        requests.append((args, kwargs))
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: deepcopy(data))
    monkeypatch.setattr(SIMKL, "adapter", lambda self: SimpleNamespace(client=SimpleNamespace(session=SimpleNamespace(get=request, post=request)), cfg=SimpleNamespace(timeout=5)))
    monkeypatch.setattr(_history, "_headers", lambda *args, **kwargs: {})
    adapter = SIMKL({})
    record = adapter.read()["shows:1"]
    assert record.observation.status == Status.ON_HOLD
    adapter.write(record, Status.DROPPED)
    assert requests[-1][1]["json"] == {"shows": [{"ids": {"simkl": 1}, "status": "dropped"}]}
    data["not_found"] = {"shows": [{"ids": {"simkl": 1}}]}
    with pytest.raises(RuntimeError, match="not_found"):
        adapter.write(record, Status.DROPPED)
    del data["not_found"]
    data["shows"][0]["last_watched_at"] = "2026-01-02"
    assert adapter.read()["shows:1"].observation.protected_token != record.observation.protected_token
    del data["anime"]
    assert set(adapter.read()) == {"shows:1"}
    data["anime"] = None
    with pytest.raises(RuntimeError, match="incomplete"):
        adapter.read()
    data["anime"] = []
    data["error"] = "user_token_failed"
    with pytest.raises(RuntimeError, match="incomplete"):
        adapter.read()
    del data["error"]
    data["errors"] = ["temporarily unavailable"]
    with pytest.raises(RuntimeError, match="incomplete"):
        adapter.read()


def test_simkl_cancellation_during_header_refresh_prevents_mutation(monkeypatch):
    from cw_platform.run_control import SyncCancelled, clear_cancel, request_cancel
    from providers.sync.simkl import _history
    from providers.sync._status_mapping import Record
    writes = []
    monkeypatch.setattr(SIMKL, "adapter", lambda self: SimpleNamespace(client=SimpleNamespace(session=SimpleNamespace(post=lambda *a, **kw: writes.append(kw))), cfg=SimpleNamespace(timeout=5)))
    def headers(*args, **kwargs):
        request_cancel()
        return {}
    monkeypatch.setattr(_history, "_headers", headers)
    record = Record("shows:1", "shows", {}, Observation(ReadState.UNKNOWN), {"bucket": "shows"})
    clear_cancel()
    try:
        with pytest.raises(SyncCancelled):
            SIMKL({}).write(record, Status.DROPPED)
        assert writes == []
    finally:
        clear_cancel()


def test_crosswatch_note_only_entry_preserves_every_other_field(tmp_path):
    store = TrackerStore(tmp_path)
    key = "show:tmdb:1"
    store.save_note(key, {"type": "show", "title": "Title", "tmdb": "1", "status": "on_hold", "notes": "Keep this", "tags": ["tag"], "started": "2025-01-01"})
    before = store.note(key)
    adapter = CW({"crosswatch": {"root_dir": str(tmp_path)}})
    record = adapter.read()[key]
    assert record.observation.status == Status.ON_HOLD
    assert not store.has_state("history")
    adapter.write(record, Status.COMPLETED)
    assert store.note(key) == {**before, "status": "completed"}
    assert not store.has_state("history")
    with pytest.raises(TrackerConflict):
        adapter.write(record, Status.DROPPED)


def test_crosswatch_known_title_without_notes_can_receive_status(tmp_path):
    store = TrackerStore(tmp_path)
    original = {"tmdb:1": {"type": "movie", "ids": {"tmdb": "1"}, "title": "Movie", "rating": 8}}
    store.write_state("ratings", original)
    adapter = CW({"crosswatch": {"root_dir": str(tmp_path)}})
    record = adapter.read()["movie:tmdb:1"]
    assert record.observation.state == ReadState.ABSENT
    adapter.write(record, Status.DROPPED)
    after = adapter.read()["movie:tmdb:1"]
    assert after.observation.protected_token == record.observation.protected_token
    assert store.read_current("ratings")["items"] == original


def test_capabilities_do_not_expose_unimplemented_transitions():
    assert provider_caps("bingebase") == {}
    assert provider_caps("simkl")["media"]["movies"]["writable"] == ["dropped"]
    for provider in ("simkl", "anilist", "myanimelist", "kitsu"):
        assert "completed" not in provider_caps(provider)["media"]["shows"]["writable"]
    assert "completed" in provider_caps("crosswatch")["media"]["shows"]["writable"]


def test_crosswatch_can_receive_status_without_creating_history_or_watchlist(tmp_path):
    from providers.sync._status_mapping import Record
    from cw_platform.status_mapping import Observation
    adapter = CW({"crosswatch": {"root_dir": str(tmp_path)}})
    source = Record("foreign", "movies", {"type": "movie", "ids": {"tmdb": "1"}, "title": "Movie"}, Observation(ReadState.UNKNOWN), {})
    before = adapter.missing_record(source, adapter.read())
    assert before is not None
    adapter.write(before, Status.DROPPED)
    after = adapter.read()[before.native_id]
    assert after.observation.protected_token == before.observation.protected_token
    assert not adapter.store().has_state("history") and not adapter.store().has_state("watchlist")


def test_punchplay_planned_comes_from_watchlist_and_status_only_patch_preserves_state(monkeypatch):
    from providers.sync.punchplay import _status as module
    interactions = [{"tmdbId": 1, "kind": "show", "scope": "title", "showStatus": "ON_HOLD", "rating": 7, "isFavourite": True},
                    {"tmdbId": 2, "kind": "movie", "scope": "title", "showStatus": None, "wantToWatch": False}]
    snapshots = {"interaction": interactions, "history": [{"id": "play", "watchedAt": "2025-01-01"}], "playback": []}
    monkeypatch.setattr(module.StatusAdapter, "adapter", lambda self: object())
    monkeypatch.setattr(module, "snapshot_pages", lambda adapter, resource, **kwargs: iter([deepcopy(snapshots[resource])]))
    monkeypatch.setattr(module, "build_index", lambda adapter: {"tmdb:2": {"type": "movie", "ids": {"tmdb": "2"}}})
    writes = []
    monkeypatch.setattr(module, "punchplay_request", lambda *args, **kwargs: (writes.append((args, kwargs)) or
        SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"ok": True})))
    adapter = module.StatusAdapter({})
    rows = adapter.read()
    assert rows["movie:2"].observation.status == Status.PLANNED
    assert rows["movie:2"].observation.watchlisted is True
    adapter.write(rows["show:1"], Status.COMPLETED)
    assert writes[0][0][1:] == ("PATCH", module.PLATFORM_BASE + "/title/show/1/interact")
    assert writes[0][1] == {"json": {"showStatus": "WATCHED"}, "max_retries": 1}
    interactions[0].update(showStatus="WATCHED", updatedAt="now")
    assert adapter.read()["show:1"].observation.protected_token == rows["show:1"].observation.protected_token
    snapshots["history"].append({"id": "unexpected"})
    assert adapter.read()["show:1"].observation.protected_token != rows["show:1"].observation.protected_token
    assert "on_hold" not in provider_caps("punchplay")["media"]["movies"]["writable"]


def test_punchplay_status_creates_the_interaction_for_a_title_without_one(monkeypatch):
    from providers.sync.punchplay import _status as module
    interactions = [{"id": 1, "tmdbId": 1, "kind": "show", "scope": "title", "showStatus": None, "rating": 7}]
    snapshots = {"interaction": interactions, "history": [{"id": "play", "watchedAt": "2025-01-01"}], "playback": []}
    monkeypatch.setattr(module.StatusAdapter, "adapter", lambda self: object())
    monkeypatch.setattr(module, "snapshot_pages", lambda adapter, resource, **kwargs: iter([deepcopy(snapshots[resource])]))
    monkeypatch.setattr(module, "build_index", lambda adapter: {"tmdb:4": {"type": "show", "ids": {"tmdb": "4"}}})
    monkeypatch.setattr(module, "punchplay_request", lambda *args, **kwargs: SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"ok": True}))
    adapter = module.StatusAdapter({})
    assert adapter.creates_missing is True
    snapshot = adapter.read()
    source = Record("x", "shows", {"title": "One Piece", "ids": {"tmdb": "37854"}}, Observation(ReadState.PRESENT, Status.DROPPED), {})
    missing = adapter.missing_record(source, snapshot)
    assert missing.native_id == "show:37854" and missing.observation.state == ReadState.ABSENT
    adapter.write(missing, Status.DROPPED)
    interactions.append({"id": 1249369, "tmdbId": 37854, "kind": "show", "mediaSource": "tmdb", "sourceId": 37854, "scope": "title",
                         "season": 0, "episode": 0, "watchedAt": None, "rating": None, "isFavourite": False, "wantToWatch": False,
                         "showStatus": "DROPPED", "updatedAt": "2026-11-09T22:17:28.133Z"})
    created = adapter.read()["show:37854"].observation
    assert created.status == Status.DROPPED
    assert created.protected_token == missing.observation.protected_token
    interactions[1]["mediaSource"] = "tvdb"
    assert adapter.read()["show:37854"].observation.protected_token != missing.observation.protected_token
    listed = adapter.missing_record(replace(source, item={"ids": {"tmdb": "4"}}), snapshot)
    assert listed.observation.status == Status.PLANNED and listed.observation.watchlisted is True
    assert adapter.missing_record(replace(source, item={"ids": {"imdb": "tt1"}}), snapshot) is None


def _skipped(rows):
    return {key: row.raw["_cw_skipped"] for key, row in rows.items() if isinstance(row.raw, dict) and row.raw.get("_cw_skipped")}


def test_simkl_skips_duplicate_and_unidentified_rows(monkeypatch):
    from providers.sync.simkl import _history
    show = {"status": "hold", "show": {"title": "Show", "ids": {"simkl": 1, "tmdb": 2}}}
    other = {"status": "dropped", "show": {"title": "Other", "ids": {"simkl": 3, "tmdb": 4}}}
    data = {"movies": [], "shows": [show, deepcopy(show), other, {"status": "hold", "show": {"title": "No id", "ids": {}}}], "anime": []}
    monkeypatch.setattr(SIMKL, "adapter", lambda self: SimpleNamespace(client=SimpleNamespace(session=SimpleNamespace(
        get=lambda *a, **k: SimpleNamespace(raise_for_status=lambda: None, json=lambda: deepcopy(data)))), cfg=SimpleNamespace(timeout=5)))
    monkeypatch.setattr(_history, "_headers", lambda *args, **kwargs: {})
    adapter = SIMKL({})
    rows = adapter.read()
    assert _skipped(rows) == {"shows:1": "simkl_status_duplicate_entry"}
    assert rows["shows:3"].observation.status == Status.DROPPED
    assert adapter.unidentified == ["simkl_status_missing_identity"]


def test_anilist_skips_an_inconsistent_entry(monkeypatch):
    def entry(ident, status, progress=0):
        return {"id": ident * 10, "mediaId": ident, "status": status, "progress": progress, "repeat": 0, "notes": "", "score": 0,
                "private": False, "completedAt": {"year": None, "month": None, "day": None},
                "startedAt": {"year": None, "month": None, "day": None}, "hiddenFromStatusLists": False, "customLists": {},
                "advancedScores": {}, "media": {"id": ident, "format": "TV", "title": {"english": f"Anime {ident}"}}}
    lists = [{"entries": [entry(1, "PAUSED"), entry(2, "CURRENT")]}, {"entries": [entry(1, "DROPPED")]}]
    monkeypatch.setattr(AniList, "client", lambda self: SimpleNamespace(viewer=lambda: {"id": 9},
                        gql=lambda *a, **k: {"MediaListCollection": {"lists": deepcopy(lists)}}))
    rows = AniList({}).read()
    assert _skipped(rows) == {"1": "anilist_status_inconsistent_entry"}
    assert rows["2"].observation.status == Status.WATCHING


def test_punchplay_skips_duplicate_and_unidentified_interactions(monkeypatch):
    from providers.sync.punchplay import _status as module
    interactions = [{"tmdbId": 1, "kind": "show", "scope": "title", "showStatus": "ON_HOLD"},
                    {"tmdbId": 1, "kind": "show", "scope": "title", "showStatus": "DROPPED"},
                    {"tmdbId": 3, "kind": "show", "scope": "title", "showStatus": "DROPPED"},
                    {"kind": "show", "scope": "title", "showStatus": "DROPPED"}]
    snapshots = {"interaction": interactions, "history": [], "playback": []}
    monkeypatch.setattr(module.StatusAdapter, "adapter", lambda self: object())
    monkeypatch.setattr(module, "snapshot_pages", lambda adapter, resource, **kwargs: iter([deepcopy(snapshots[resource])]))
    monkeypatch.setattr(module, "build_index", lambda adapter: {})
    adapter = module.StatusAdapter({})
    rows = adapter.read()
    assert _skipped(rows) == {"show:1": "punchplay_status_duplicate_entry"}
    assert rows["show:3"].observation.status == Status.DROPPED
    assert adapter.unidentified == ["punchplay_status_missing_identity"]
