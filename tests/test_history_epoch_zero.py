# /tests/test_history_epoch_zero.py
# CrossWatch - Epoch-zero history read, write, and cache regressions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from providers.sync.emby import _common as emby_common, _history as emby
from providers.sync.jellyfin import _history as jellyfin, _id_lookup as jellyfin_lookup
from providers.sync.mdblist import _history as mdblist
from providers.sync.plex import _history as plex
from providers.sync.simkl import _history as simkl


EPOCH = "1970-01-01T00:00:00Z"
NEXT = "1970-01-01T00:00:01Z"
DATES = [(EPOCH, 0), (NEXT, 1), (None, None), ("", None), ("invalid", None)]


def response(body):
    return SimpleNamespace(status_code=200, json=lambda: body)


def watched_row(kind="Movie", date=EPOCH):
    return {
        "Id": "1", "Type": kind, "Name": "Sample", "ProviderIds": {"Tmdb": "123"},
        "ParentIndexNumber": 1, "IndexNumber": 1,
        "UserData": {"Played": True, "LastPlayedDate": date},
    }


@pytest.fixture(params=[jellyfin, emby], ids=["jellyfin", "emby"])
def media_history(request, monkeypatch):
    history = request.param
    for name in ("_shadow_load", "_bb_load"):
        monkeypatch.setattr(history, name, lambda: {})
    for name in ("_shadow_save", "_bb_save", "_thaw_if_present", "_unres_flush", "_freeze", "sleep_ms"):
        monkeypatch.setattr(history, name, lambda *a, **k: None)
    monkeypatch.setattr(history, "resolve_item_id", lambda *a, **k: "1")
    monkeypatch.setattr(jellyfin_lookup, "prepare", lambda *a, **k: None)
    monkeypatch.setattr(emby_common, "provider_index", lambda *a, **k: {})
    if history is jellyfin:
        monkeypatch.setattr(history, "_normalize_watched", lambda *a, **k: True)
    return history


@pytest.mark.parametrize("date,timestamp", DATES)
def test_destination_state_distinguishes_epoch_from_missing(media_history, date, timestamp):
    http = SimpleNamespace(get=lambda *a, **k: response({"Items": [watched_row(date=date)]}))
    assert media_history._dst_user_state(http, "user", "1") == (True, timestamp)
    assert media_history._dst_user_state(http, "user", "missing") == (False, None)


@pytest.mark.parametrize("kind", ["movie", "episode"])
@pytest.mark.parametrize("date,timestamp", DATES)
def test_history_writes_valid_dates_including_epoch(media_history, monkeypatch, kind, date, timestamp):
    history = media_history
    monkeypatch.setattr(history, "_dst_user_states", lambda *a: {})
    mark = Mock(return_value=True)
    monkeypatch.setattr(history, "_mark_played", mark)
    adapter = SimpleNamespace(client=object(), cfg=SimpleNamespace(user_id="user"))
    item = {"type": kind, "ids": {"tmdb": "123"}, "watched_at": date}
    if kind == "episode":
        item.update({"show_ids": {"tvdb": "456"}, "season": 1, "episode": 1})

    count, unresolved = history.add(adapter, [item])

    result = adapter._history_write_meta["results"][0]
    if timestamp is None:
        assert count == 0
        assert unresolved[0]["reason"] == "missing_watched_at"
        mark.assert_not_called()
        assert "requested_at" not in result
    else:
        assert count == 1
        assert unresolved == []
        mark.assert_called_once_with(adapter.client, "user", "1", date_played_iso=date)
        assert result["requested_at"] == date
        assert "destination_at" not in result


@pytest.mark.parametrize("source,destination,expected_reason,writes", [
    (EPOCH, EPOCH, "existing_newer", 0),
    (EPOCH, "2026-01-01T00:00:00Z", "existing_newer", 0),
    ("2026-01-01T00:00:00Z", EPOCH, "wrote", 1),
])
def test_epoch_destination_uses_dated_write_decisions(media_history, monkeypatch, source, destination, expected_reason, writes):
    history = media_history
    client = SimpleNamespace(get=lambda *a, **k: response({"Items": [watched_row(date=destination)]}))
    adapter = SimpleNamespace(client=client, cfg=SimpleNamespace(user_id="user"))
    mark = Mock(return_value=True)
    monkeypatch.setattr(history, "_mark_played", mark)

    count, unresolved = history.add(adapter, [{"type": "movie", "ids": {"tmdb": "123"}, "watched_at": source}])

    assert count == writes
    assert unresolved == []
    assert mark.call_count == writes
    result = adapter._history_write_meta["results"][0]
    assert result["reason"] == expected_reason
    assert result["requested_at"] == source
    if not writes:
        assert result["destination_at"] == destination


@pytest.mark.parametrize("kind", ["Movie", "Episode"])
@pytest.mark.parametrize("date,timestamp", DATES)
def test_emby_snapshot_preserves_epoch_and_untimed_presence(monkeypatch, kind, date, timestamp):
    monkeypatch.setattr(emby, "_emby_library_roots", lambda *a: {})
    monkeypatch.setattr(emby, "_shadow_load", lambda: {})
    monkeypatch.setattr(emby, "_bb_load", lambda: {})
    monkeypatch.delenv("CW_EMBY_HISTORY_BACKFILL", raising=False)
    row = watched_row(kind, date)

    def get(path, *, params):
        types = params.get("IncludeItemTypes", "").split(",")
        return response({"Items": [row] if kind in types or params.get("Ids") else []})

    adapter = SimpleNamespace(client=SimpleNamespace(get=get), cfg=SimpleNamespace(user_id="user"))
    snapshot = emby.build_index(adapter)

    assert len(snapshot) == 1
    key, item = next(iter(snapshot.items()))
    assert item["watched"] is True
    if timestamp is None:
        assert "watched_at" not in item
        assert "@" not in key
    else:
        assert key.endswith(f"@{timestamp}")
        assert item["watched_at"] == date
        assert emby.build_index(adapter, since=2) == {}


@pytest.mark.parametrize("date,timestamp", DATES)
def test_emby_date_lookup_preserves_epoch(date, timestamp):
    row = watched_row(date=date)
    http = SimpleNamespace(get=lambda path, **k: response(row if path.endswith("/1") else {"Items": [row]}))
    cache = {}
    emby._prefetch_played_ts(http, "user", ["1", "missing"], cache)
    assert cache == {"1": timestamp, "missing": None}
    assert emby._played_ts_backfill(http, "user", row) == timestamp
    failed = SimpleNamespace(get=lambda *a, **k: SimpleNamespace(status_code=500))
    assert emby._played_ts_backfill(failed, "user", row) is None


@pytest.fixture
def simkl_state(monkeypatch):
    store = {}
    monkeypatch.setattr(simkl, "state_file", lambda name: name)
    monkeypatch.setattr(simkl, "_load_json", lambda path: store.get(path, {}))
    monkeypatch.setattr(simkl, "_save_json", lambda path, data: store.__setitem__(path, data))
    return store


def simkl_rows(kind, date):
    if kind == "movie":
        return [{"movie": {"title": "Sample", "ids": {"tmdb": "123"}}, "last_watched_at": date}], [], []
    row = {"show": {"title": "Sample", "ids": {"tvdb": "456", "simkl": "789"}},
           "seasons": [{"number": 1, "episodes": [{"number": 1, "watched_at": date}]}]}
    if kind == "episode":
        return [], [row], []
    if kind.startswith("anime_movie"):
        row["show"]["anime_type"] = "movie"
        if kind == "anime_movie":
            row["last_watched_at"] = date
            row.pop("seasons")
    return [], [], [row]


@pytest.mark.parametrize("kind", ["movie", "episode", "anime_episode", "anime_movie", "anime_movie_fallback"])
@pytest.mark.parametrize("date,timestamp", DATES)
def test_simkl_reader_preserves_epoch(simkl_state, kind, date, timestamp):
    snapshot, _, movies, shows, anime, _, _ = simkl._parse_rows(*simkl_rows(kind, date), limit=None)
    if timestamp is None:
        assert snapshot == {}
        return
    assert len(snapshot) == 1
    key, item = next(iter(snapshot.items()))
    assert key.endswith(f"@{timestamp}")
    assert item["watched_at"] == date
    assert timestamp in (movies, shows, anime)
    simkl._apply_since_limit(snapshot, since=2, limit=None)
    assert snapshot == {}


@pytest.mark.parametrize("date,timestamp", DATES)
def test_simkl_watched_coordinates_preserve_epoch(date, timestamp):
    _, rows, _ = simkl_rows("episode", date)
    assert simkl._watched_raw_coordinates(rows[0]) == ({(1, 1)} if timestamp is not None else set())


@pytest.mark.parametrize("kind", ["movie", "episode"])
@pytest.mark.parametrize("date,timestamp", DATES)
def test_simkl_cache_injection_preserves_epoch(simkl_state, kind, date, timestamp):
    item = {"type": kind, "ids": {"tmdb": "123"}, "watched_at": date}
    if kind == "episode":
        item.update({"show_ids": {"tvdb": "456"}, "season": 1, "episode": 1})
    simkl._inject_adds_into_cache([item])
    cached = simkl._cache_load()
    if timestamp is None:
        assert cached == {}
    else:
        assert len(cached) == 1
        key, saved = next(iter(cached.items()))
        assert key.endswith(f"@{timestamp}")
        assert saved["watched_at"] == date


def test_simkl_old_cache_forces_full_fetch(simkl_state, monkeypatch):
    simkl_state[simkl._cache_path()] = {"schema": 4, "items": {}}
    monkeypatch.setattr(simkl, "normalize_flat_watermarks", lambda: None)
    monkeypatch.setattr(simkl, "get_watermark", lambda *a: "2026-01-01T00:00:00Z")
    monkeypatch.setattr(simkl, "update_watermark_if_new", lambda *a: None)
    monkeypatch.setattr(simkl, "fetch_activities", lambda *a, **k: ({}, None))
    monkeypatch.setattr(simkl, "_headers", lambda *a, **k: {})
    monkeypatch.setattr(simkl, "_unfreeze", lambda *a: None)
    movies, _, _ = simkl_rows("movie", EPOCH)
    fetch = Mock(return_value={"movies": movies})
    monkeypatch.setattr(simkl, "_fetch_all_items", fetch)
    adapter = SimpleNamespace(client=SimpleNamespace(session=object()), cfg=SimpleNamespace(timeout=5))

    snapshot = simkl.build_index(adapter)

    assert fetch.call_args.kwargs["since_iso"] is None
    assert len(snapshot) == 1
    assert next(iter(snapshot.values()))["watched_at"] == EPOCH
    assert simkl._cache_load() == snapshot


@pytest.mark.parametrize("date,timestamp", DATES)
def test_plex_presence_keeps_missing_dates_separate(date, timestamp):
    catalog = plex.HistoryCatalog()
    catalog.add({"rk": "1", "type": "movie", "ids": {"tmdb": "123", "plex": "1"},
                 "watched": True, "view_count": 1, "last_viewed_at": timestamp})
    item = next(iter(catalog.presence().values()))
    assert item["watched_at"] == (date if timestamp is not None else None)
    assert bool(item.get("watched_at_missing")) == (timestamp is None)


@pytest.mark.parametrize("source,destination,status", [
    (0, 0, plex.DATE_EXACT), (0, 1000, plex.DATE_MISMATCH),
    (1000, 0, plex.DATE_MISMATCH), (0, None, plex.DATE_NO_DATE),
])
def test_plex_date_comparison_preserves_epoch(source, destination, status):
    assert plex._date_status(source, destination, 300)[0] == status


@pytest.mark.parametrize("kind", ["movie", "episode"])
@pytest.mark.parametrize("date,timestamp", DATES)
def test_plex_writes_epoch_and_rejects_missing_date(monkeypatch, kind, date, timestamp):
    catalog = plex.HistoryCatalog()
    catalog.add({"rk": "1", "type": kind, "ids": {"tmdb": "123", "plex": "1"},
                 "show_ids": {"tvdb": "456"}, "season": 1, "episode": 1, "watched": False})
    monkeypatch.setattr(plex, "home_scope_enter", lambda *a: (False, False, None, None))
    monkeypatch.setattr(plex, "home_scope_exit", lambda *a: None)
    monkeypatch.setattr(plex, "plex_feature_library_ids", lambda *a: set())
    monkeypatch.setattr(plex, "_get_history_catalog", lambda *a, **k: catalog)
    monkeypatch.setattr(plex, "_shadow_add_batch", lambda *a: None)
    monkeypatch.setattr(plex, "_ensure_session_pool", lambda *a: None)
    write = Mock(return_value=True)
    monkeypatch.setattr(plex, "_scrobble_with_date", write)
    adapter = SimpleNamespace(client=SimpleNamespace(server=object()), config={})
    item = {"type": kind, "ids": {"tmdb": "123"}, "watched_at": date}
    if kind == "episode":
        item.update({"show_ids": {"tvdb": "456"}, "season": 1, "episode": 1})

    count, unresolved = plex.add(adapter, [item])

    if timestamp is None:
        assert count == 0
        assert unresolved[0]["reason"] == "missing_watched_at"
        write.assert_not_called()
    else:
        assert count == 1
        assert unresolved == []
        write.assert_called_once_with(adapter.client.server, "1", timestamp)


@pytest.mark.parametrize("kind", ["movie", "episode"])
@pytest.mark.parametrize("date,timestamp", DATES)
def test_mdblist_history_merge_preserves_epoch(kind, date, timestamp):
    item = {"type": kind, "ids": {"tmdb": "123"}, "watched_at": date}
    if kind == "episode":
        item.update({"show_ids": {"tvdb": "456"}, "season": 1, "episode": 1})
    snapshot = {}
    key = mdblist._merge_event(snapshot, item)
    if timestamp is None:
        assert snapshot == {}
    else:
        assert key.endswith(f"@{timestamp}")
        assert snapshot[key]["watched_at"] == date
        migrated, _ = mdblist._migrate_cache(snapshot)
        assert migrated[key]["watched_at"] == date


@pytest.mark.parametrize("failed_first", [False, True])
def test_mdblist_old_cache_recovers_epoch_despite_unchanged_activities(monkeypatch, failed_first):
    existing = {"type": "movie", "ids": {"tmdb": "999"}, "watched_at": NEXT}
    store = {"cache": {"items": {"tmdb:999@1": existing}}}
    monkeypatch.setattr(mdblist, "_cache_path", lambda: "cache")
    monkeypatch.setattr(mdblist, "read_json", lambda path: store.get(path, {}))
    monkeypatch.setattr(mdblist, "write_json", lambda path, doc: store.__setitem__(path, doc))
    monkeypatch.setattr(mdblist, "_cfg", lambda *a: {"api_key": "test"})
    monkeypatch.setattr(mdblist, "_fetch_last_activities", lambda *a, **k: {"watched_at": NEXT, "journal_at": NEXT})
    monkeypatch.setattr(mdblist, "get_watermark", lambda *a: NEXT)
    monkeypatch.setattr(mdblist, "update_watermark_if_new", lambda *a: None)
    request = Mock(return_value=SimpleNamespace(status_code=200, text="{}", json=lambda: {
        "movies": [{"movie": {"ids": {"tmdb": "123"}}, "watched_at": EPOCH}],
        "pagination": {"has_more": False},
    }))
    monkeypatch.setattr(mdblist, "mdblist_request", request)
    adapter = SimpleNamespace(client=SimpleNamespace(session=object()), cfg=SimpleNamespace(timeout=5, max_retries=0))
    if failed_first:
        request.side_effect = RuntimeError("unavailable")
        assert mdblist.build_index(adapter) == {"tmdb:999@1": existing}
        assert mdblist._cache_is_stale()
        request.side_effect = None
        request.reset_mock()

    snapshot = mdblist.build_index(adapter)

    assert list(snapshot) == ["tmdb:123@0"]
    assert snapshot["tmdb:123@0"]["watched_at"] == EPOCH
    assert not mdblist._cache_is_stale()
    assert mdblist.build_index(adapter) == snapshot
    request.assert_called_once()
