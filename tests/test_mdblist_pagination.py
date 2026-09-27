# tests/test_mdblist_pagination.py
# CrossWatch - MDBList cursor pagination and incomplete read safeguards
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import importlib
import json
from types import SimpleNamespace

import pytest

from providers.sync.mdblist._common import CursorPager, MDBListFetchError
from providers.sync import _mod_MDBLIST as mdblist


WHEN = "2026-09-01T12:00:00Z"


def response(data, headers=None, status=200):
    return SimpleNamespace(status_code=status, text=json.dumps(data), json=lambda: data, headers=headers or {})


def setup_reader(monkeypatch, tmp_path, feature):
    module = importlib.import_module(f"providers.sync.mdblist._{feature}")
    adapter = SimpleNamespace(config={"mdblist": {"api_key": "test-key"}},
                              cfg=SimpleNamespace(timeout=1, max_retries=0), client=SimpleNamespace(session=None))
    writes = []
    for name in ("save_watermark", "update_watermark_if_new", "_save_cache", "_shadow_save"):
        if hasattr(module, name):
            monkeypatch.setattr(module, name, lambda *a, **kw: writes.append((a, kw)))
    for name in ("_cache_path", "_shadow_path"):
        if hasattr(module, name):
            monkeypatch.setattr(module, name, lambda: tmp_path / "cache.json")
    for name in ("get_watermark", "_unfreeze_keys_if_present"):
        if hasattr(module, name):
            monkeypatch.setattr(module, name, lambda *a: None)
    if hasattr(module, "_fetch_last_activities"):
        monkeypatch.setattr(module, "_fetch_last_activities", lambda *a, **kw: {})
    if feature == "history":
        monkeypatch.setattr(module, "_rewatches_enabled", lambda a: False)
    if feature == "playlists":
        monkeypatch.setattr(module, "_find_owned_resource", lambda *a: None)
    return module, adapter, writes


def row(number):
    return {"type": "movie", "mediatype": "movie", "id": number, "ids": {"tmdb": number},
            "movie": {"ids": {"tmdb": number}}, "rating": 8, "rating_precise": 7.5,
            "rated_at": WHEN, "watched_at": WHEN, "collected_at": WHEN}


def read(module, adapter, feature):
    if feature == "playlists":
        return module.get_snapshot(adapter, "123").items
    return module.build_index(adapter)


@pytest.mark.parametrize("feature", ["watchlist", "ratings", "history", "collection", "playlists"])
def test_readers_follow_cursors_even_on_short_pages(monkeypatch, tmp_path, feature):
    module, adapter, _ = setup_reader(monkeypatch, tmp_path, feature)
    calls = []

    def request(*a, **kw):
        params = kw["params"]
        assert "offset" not in params
        calls.append(params)
        number = len(calls)
        assert params.get("cursor") == (None if number == 1 else "opaque+/token==")
        cursor = "opaque+/token==" if number == 1 else None
        if feature in ("watchlist", "playlists"):
            return response([row(number)], {"X-Next-Cursor": cursor, "X-Has-More": str(bool(cursor)).lower()})
        return response({"movies": [row(number)], "pagination": {"next_cursor": cursor}})

    monkeypatch.setattr(module, "mdblist_request", request)
    result = read(module, adapter, feature)
    assert len(result) == 2
    assert len(calls) == 2
    assert all(p["limit"] == 1000 for p in calls)
    if feature == "ratings":
        assert calls[0]["since"] == calls[1]["since"]
        assert all(item["rating"] == 7.5 for item in result.values())


@pytest.mark.parametrize("feature", ["watchlist", "ratings", "history", "collection", "playlists"])
@pytest.mark.parametrize("failure", ["missing_cursor", "repeated_cursor", "http"])
def test_incomplete_read_does_not_save_partial_cache_or_watermark(monkeypatch, tmp_path, feature, failure):
    module, adapter, writes = setup_reader(monkeypatch, tmp_path, feature)
    calls = []

    def request(*a, **kw):
        calls.append(kw)
        cursor = "next"
        if len(calls) == 2:
            if failure == "http":
                return response({}, status=503)
            if failure == "missing_cursor":
                cursor = None
        return response({"movies": [row(len(calls))], "pagination": {"next_cursor": cursor, "has_more": True}})

    monkeypatch.setattr(module, "mdblist_request", request)
    with pytest.raises((MDBListFetchError, getattr(module, "MDBListPlaylistError", MDBListFetchError))):
        read(module, adapter, feature)
    assert len(calls) == 2
    assert writes == []


@pytest.mark.parametrize("feature", ["ratings", "history", "collection"])
def test_page_cap_with_more_items_does_not_save(monkeypatch, tmp_path, feature):
    module, adapter, writes = setup_reader(monkeypatch, tmp_path, feature)
    monkeypatch.setattr(module, "mdblist_request", lambda *a, **kw:
                        response({"movies": [row(1)], "pagination": {"next_cursor": "more"}}))
    with pytest.raises(MDBListFetchError, match="page limit"):
        module.build_index(adapter, max_pages=1)
    assert writes == []


def test_rewatch_pages_keep_distinct_plays(monkeypatch, tmp_path):
    module, adapter, _ = setup_reader(monkeypatch, tmp_path, "history")
    monkeypatch.setattr(module, "_rewatches_enabled", lambda a: True)
    calls = []

    def request(*a, **kw):
        calls.append(kw["params"])
        number = len(calls)
        item = row(1)
        item["watched_at"] = f"2026-09-0{number}T12:00:00Z"
        item["id"] = number
        return response({"movies": [item], "pagination": {"next_cursor": "second" if number == 1 else None}})

    monkeypatch.setattr(module, "mdblist_request", request)
    result = module.build_index(adapter)
    assert len(result) == 2
    assert all(p["plays"] == "all" for p in calls)
    assert calls[1]["cursor"] == "second"


def test_cursor_limit_and_final_page_at_safety_cap():
    pager = CursorPager(5000, max_pages=1)
    assert pager.params() == {"limit": 1000}
    assert not pager.advance({"pagination": {"next_cursor": None}}, response({}))


def test_empty_collection_response(monkeypatch, tmp_path):
    module, adapter, _ = setup_reader(monkeypatch, tmp_path, "collection")
    monkeypatch.setattr(module, "mdblist_request", lambda *a, **kw: response([], {"X-Has-More": "false"}))
    assert module.build_index(adapter) == {}


@pytest.mark.parametrize("stale", [False, True])
@pytest.mark.parametrize("failed_page", [1, 2])
@pytest.mark.parametrize("failure", ["http", "network"])
def test_failed_history_read_never_publishes_existing_cache(monkeypatch, tmp_path, stale, failed_page, failure):
    module, adapter, writes = setup_reader(monkeypatch, tmp_path, "history")
    cached = {"tmdb:99@1788264000": {"type": "movie", "ids": {"tmdb": "99"}, "watched_at": WHEN}}
    monkeypatch.setattr(module, "_load_cache", lambda: dict(cached))
    monkeypatch.setattr(module, "_cache_is_stale", lambda: stale)
    calls = []

    def request(*a, **kw):
        calls.append(kw)
        if len(calls) == failed_page:
            if failure == "network":
                raise ConnectionError("unavailable")
            return response({}, status=503)
        return response({"movies": [row(1)], "pagination": {"next_cursor": "second"}})

    monkeypatch.setattr(module, "mdblist_request", request)
    with pytest.raises(MDBListFetchError):
        module.build_index(adapter)
    assert len(calls) == failed_page
    assert writes == []
    assert len(cached) == 1


@pytest.mark.parametrize("payload", [
    {"movies": [], "shows": [], "seasons": [], "episodes": [], "pagination": {"has_more": False}},
    {"movies": [], "pagination": {"next_cursor": None}},
])
def test_successful_empty_history_clears_persisted_cache(monkeypatch, tmp_path, payload):
    module = importlib.import_module("providers.sync.mdblist._history")
    save_cache = module._save_cache
    module, adapter, writes = setup_reader(monkeypatch, tmp_path, "history")
    monkeypatch.setattr(module, "_save_cache", save_cache)
    old = {}
    module._merge_event(old, {"type": "movie", "ids": {"tmdb": "99"}, "watched_at": WHEN})
    save_cache(old, complete=True)
    assert module._load_cache() == old
    monkeypatch.setattr(module, "mdblist_request", lambda *a, **kw: response(payload))

    assert module.build_index(adapter) == {}
    assert module._load_cache() == {}
    saved = json.loads((tmp_path / "cache.json").read_text())
    assert saved["items"] == {}
    assert saved["version"] == 1


@pytest.mark.parametrize("payload", [{}, {"error": "unavailable"}, {"movies": None}, {"movies": {}}])
def test_invalid_history_response_cannot_clear_persisted_cache(monkeypatch, tmp_path, payload):
    module = importlib.import_module("providers.sync.mdblist._history")
    save_cache = module._save_cache
    module, adapter, writes = setup_reader(monkeypatch, tmp_path, "history")
    monkeypatch.setattr(module, "_save_cache", save_cache)
    old = {}
    module._merge_event(old, {"type": "movie", "ids": {"tmdb": "99"}, "watched_at": WHEN})
    save_cache(old, complete=True)
    before = (tmp_path / "cache.json").read_bytes()
    monkeypatch.setattr(module, "mdblist_request", lambda *a, **kw: response(payload))

    with pytest.raises(MDBListFetchError):
        module.build_index(adapter)
    assert (tmp_path / "cache.json").read_bytes() == before
    assert writes == []


def test_explicit_last_page_ignores_leftover_cursor():
    pager = CursorPager(1000, max_pages=1)
    assert not pager.advance({"pagination": {"next_cursor": "leftover", "has_more": False}}, response({}))


@pytest.mark.parametrize("failure", ["http", "missing_cursor", "repeated_cursor", "network"])
def test_ratings_mixed_journal_retries_from_unchanged_watermark(monkeypatch, tmp_path, failure):
    module, adapter, writes = setup_reader(monkeypatch, tmp_path, "ratings")
    cached = {"tmdb:99": {"type": "movie", "ids": {"tmdb": "99"}, "rating": 8}}
    watermarks = {"ratings": WHEN, "ratings_journal": WHEN}
    newer = "2026-09-05T12:00:00Z"
    monkeypatch.setattr(module, "_load_cache", lambda: dict(cached))
    monkeypatch.setattr(module, "_cache_version", lambda: 2)
    monkeypatch.setattr(module, "get_watermark", watermarks.get)
    monkeypatch.setattr(module, "coalesce_since", lambda *a, **kw: watermarks["ratings"])
    monkeypatch.setattr(module, "update_watermark_if_new", lambda k, v: watermarks.update({k: v}))
    monkeypatch.setattr(module, "_fetch_last_activities", lambda *a, **kw: {"rated_at": newer, "journal_at": newer})
    adapter.fetch_journal = lambda **kw: {"rows": [
        {"category": "rated", "item_type": "movie", "ids": {"tmdb": 99}, "status": "removed", "action_at": newer},
        {"category": "rated", "item_type": "movie", "ids": {"tmdb": 1}, "status": "added", "rating_precise": 7.5,
         "value_at": "2026-09-02T12:00:00Z", "action_at": newer},
    ]}
    requests = []

    def fail(*a, **kw):
        requests.append(kw["params"])
        if failure == "network":
            raise ConnectionError("unavailable")
        if failure == "http":
            return response({}, status=503)
        cursor = "same" if failure == "repeated_cursor" else None
        return response({"movies": [row(1)], "pagination": {"has_more": True, "next_cursor": cursor}})

    monkeypatch.setattr(module, "mdblist_request", fail)
    with pytest.raises((MDBListFetchError, ConnectionError)):
        module.build_index(adapter)
    assert writes == []
    assert watermarks == {"ratings": WHEN, "ratings_journal": WHEN}

    def succeed(*a, **kw):
        assert kw["params"]["since"] == requests[0]["since"]
        return response({"movies": [row(1)], "pagination": {"has_more": False}})

    monkeypatch.setattr(module, "mdblist_request", succeed)
    result = module.build_index(adapter)
    assert list(result) == ["tmdb:1"]
    assert result["tmdb:1"]["rating"] == 7.5
    assert writes[-1][0][0] == result
    assert watermarks == {"ratings": newer, "ratings_journal": newer}


def journal_client(responses):
    calls = []
    pages = iter(responses)

    def get(url, **kw):
        calls.append(kw["params"])
        result = next(pages)
        if isinstance(result, Exception):
            raise result
        return result

    client = SimpleNamespace(cfg=SimpleNamespace(api_key="test-key"), BASE="https://api.mdblist.com", get=get)
    client.journal_page = lambda **kw: mdblist.MDBLISTClient.journal_page(client, **kw)
    return client, calls


def test_journal_headers_and_category_filter_keep_paging():
    client, calls = journal_client([
        response({"journal": [{"category": "watched", "action_at": WHEN}]},
                 {"X-Next-Cursor": "next", "X-Has-More": "true"}),
        response({"journal": [{"category": "rated", "action_at": WHEN}], "pagination": {"has_more": False}}),
    ])
    result = mdblist.MDBLISTModule.fetch_journal(SimpleNamespace(client=client), since=WHEN, limit=5000, category="rated")
    assert len(result["rows"]) == 1
    assert result["rows"][0]["category"] == "rated"
    assert calls == [{"apikey": "test-key", "limit": 1000, "since": WHEN},
                     {"apikey": "test-key", "limit": 1000, "cursor": "next"}]


@pytest.mark.parametrize("failure", ["http", "network", "invalid_json", "missing_rows", "missing_cursor", "repeated_cursor", "page_cap"])
def test_incomplete_journal_never_returns_partial_rows(monkeypatch, failure):
    first = response({"journal": [{"category": "rated", "action_at": WHEN}], "pagination": {"next_cursor": "next"}})
    bad = response({"journal": [], "pagination": {"has_more": True}})
    if failure == "http":
        bad = response({}, status=503)
    elif failure == "network":
        bad = ConnectionError("unavailable")
    elif failure == "invalid_json":
        bad = response({})
        bad.json = lambda: json.loads("invalid")
    elif failure == "missing_rows":
        bad = response({})
    elif failure in ("repeated_cursor", "page_cap"):
        bad = response({"journal": [], "pagination": {"next_cursor": "next" if failure == "repeated_cursor" else "third"}})
    if failure == "page_cap":
        monkeypatch.setattr(mdblist, "CursorPager", lambda limit, max_pages: CursorPager(limit, 2))
    client, calls = journal_client([first, bad])
    with pytest.raises(MDBListFetchError):
        mdblist.MDBLISTModule.fetch_journal(SimpleNamespace(client=client), since=WHEN)
    assert len(calls) == 2


@pytest.mark.parametrize("status", [200, 409])
def test_expired_journal_requests_full_sync(status):
    client, _ = journal_client([response({"requires_full_sync": True, "reason": "sync_window_expired"}, status=status)])
    assert mdblist.MDBLISTModule.fetch_journal(SimpleNamespace(client=client), since=WHEN) == {
        "rows": [], "requires_full_sync": True,
    }


@pytest.mark.parametrize("feature", ["ratings", "history"])
@pytest.mark.parametrize("expired", [False, True])
def test_feature_does_not_commit_partial_journal(monkeypatch, tmp_path, feature, expired):
    module, adapter, writes = setup_reader(monkeypatch, tmp_path, feature)
    monkeypatch.setattr(module, "_load_cache", lambda: {"tmdb:99": {"type": "movie", "ids": {"tmdb": "99"}, "rating": 8, "watched_at": WHEN}})
    monkeypatch.setattr(module, "get_watermark", lambda *a: WHEN)
    newer = "2026-09-05T12:00:00Z"
    monkeypatch.setattr(module, "_fetch_last_activities", lambda *a, **kw:
                        {"rated_at": newer, "watched_at": newer, "journal_at": newer})
    if feature == "ratings":
        monkeypatch.setattr(module, "_cache_version", lambda: 2)
    else:
        monkeypatch.setattr(module, "_cache_is_stale", lambda: False)
    first = response({"journal": [{"category": "rated" if feature == "ratings" else "watched", "status": "removed",
                                   "item_type": "movie", "ids": {"tmdb": 99}, "action_at": newer}],
                      "pagination": {"next_cursor": "next"}})
    client, _ = journal_client([response({"requires_full_sync": True})] if expired else [first, response({}, status=503)])
    adapter.fetch_journal = lambda **kw: mdblist.MDBLISTModule.fetch_journal(SimpleNamespace(client=client), **kw)
    reads = []

    def request(*a, **kw):
        reads.append(kw)
        return response({"movies": [row(1)], "pagination": {"has_more": False}})

    monkeypatch.setattr(module, "mdblist_request", request)
    if expired:
        result = module.build_index(adapter)
        assert len(reads) == 1
        assert len(result) == 1
        assert str(next(iter(result.values()))["ids"]["tmdb"]) == "1"
        assert writes
    else:
        with pytest.raises(MDBListFetchError):
            module.build_index(adapter)
        assert reads == []
        assert writes == []
