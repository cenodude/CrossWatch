# /tests/test_provider_complete_reads.py
# Provider reads beyond former pagination limits and continuation safeguards
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import importlib
import json
from types import SimpleNamespace

import pytest

from providers.sync._pagination import PageGuard, page_numbers


def response(data, headers=None):
    return SimpleNamespace(status_code=200, text=json.dumps(data), json=lambda: data, headers=headers or {})


@pytest.fixture
def active_sync_cancel(monkeypatch):
    from cw_platform import run_control

    monkeypatch.setattr(run_control, "_STATE", dict(run_control._STATE))
    run_control.request_cancel()
    assert run_control.cancel_requested()


def test_non_sync_pagination_ignores_sync_cancel_but_keeps_repeat_detection(active_sync_cancel):
    from cw_platform.run_control import SyncCancelled
    from providers.sync.trakt._common import TraktPager, TraktPaginationError

    assert next(page_numbers(check_cancel=False)) == 1
    guard = PageGuard(ValueError, "repeated", check_cancel=False)
    guard.check([1])
    with pytest.raises(ValueError, match="repeated"):
        guard.check([1])
    pager = TraktPager("progress", check_cancel=False)
    pager.read(response([{"id": 1}]), 1)
    with pytest.raises(TraktPaginationError, match="repeated_page"):
        pager.read(response([{"id": 1}]), 2)
    with pytest.raises(SyncCancelled):
        next(page_numbers())
    with pytest.raises(SyncCancelled):
        TraktPager("history").read(response([]), 1)


def test_tracearr_user_picker_ignores_sync_cancel(monkeypatch, active_sync_cancel):
    from providers.auth import _auth_TRACEARR as module

    replies = iter([{"data": [{"id": "1"}], "meta": {"nextCursor": "next"}}, {"data": [{"id": "2"}]}])
    monkeypatch.setattr(module, "_get", lambda *args, **kwargs: response(next(replies)))
    users, error = module.list_users("http://test", "test")
    assert not error
    assert [user["id"] for user in users] == ["1", "2"]


def test_trakt_playback_ignores_sync_cancel(monkeypatch, active_sync_cancel):
    from services.playback_progress.adapters import trakt as module

    calls = []

    def get(path, params):
        calls.append((path, params["page"]))
        return response([{"id": path}] if params["page"] == 1 else [])

    monkeypatch.setattr(module, "_module", lambda cfg: SimpleNamespace(client=SimpleNamespace(BASE="", get=get)))
    adapter = module.TraktPlaybackAdapter()
    monkeypatch.setattr(adapter, "_normalize", lambda *args: None)
    result = adapter.list_progress({}, instance_id="default", instance_label="Test")
    assert result.ok
    assert len(calls) == 4


@pytest.mark.parametrize("provider,class_name", [("jellyfin", "JellyfinWatchService"), ("emby", "EmbyWatchService")])
def test_manual_mark_worker_survives_sync_cancel(monkeypatch, active_sync_cancel, provider, class_name):
    from concurrent.futures import ThreadPoolExecutor

    module = importlib.import_module(f"providers.scrobble.{provider}.watch")
    service = object.__new__(getattr(module, class_name))
    calls = []

    def get(path, cfg):
        calls.append(path)
        return {"Items": [{"Id": f"{len(calls)}-{i}", "UserData": {"LastPlayedDate": "2026-01-01T00:00:00Z"}}
                          for i in range(50)]}

    monkeypatch.setattr(service, "_mark_get", get)
    with ThreadPoolExecutor(max_workers=1) as pool:
        for _ in range(2):
            assert len(pool.submit(service._mark_played_rows, "user", {}, 1.0).result(timeout=5)) == 200
    assert len(calls) == 8


@pytest.mark.parametrize("later_pages,ok", [(None, True), (3, False)])
def test_publicmetadb_playback_preserves_known_page_count(monkeypatch, active_sync_cancel, later_pages, ok):
    from services.playback_progress.adapters import publicmetadb as module

    calls = []

    def get(path, params):
        page = params["page"]
        calls.append(page)
        data = {"items": [{"id": page}], "total": 2}
        if page == 1 or later_pages is not None:
            data["totalPages"] = 2 if page == 1 else later_pages
        return response(data)

    client = SimpleNamespace(get=get, safe_json=lambda resp: resp.json())
    monkeypatch.setattr(module, "PUBLICMETADBModule", lambda cfg: SimpleNamespace(cfg=SimpleNamespace(), client=client))
    monkeypatch.setattr(module, "tmdb_metadata_provider", lambda cfg: None)
    monkeypatch.setattr(module, "enrich_parallel", lambda rows, normalize: rows)
    result = module.PublicMetaDBPlaybackAdapter().list_progress({}, instance_id="default", instance_label="Test")
    assert result.ok is ok
    assert len(result.items) == (2 if ok else 0)
    assert calls == [1, 2]


@pytest.mark.parametrize("data", [{}, {"lists": None}, {"lists": []}, {"lists": None, "pagination": {"total_pages": 0, "total_items": 0}}])
def test_simkl_empty_custom_lists(monkeypatch, data):
    from providers.sync.simkl import _playlists as module

    monkeypatch.setattr(module, "_custom_get", lambda *args, **kwargs: data)
    assert module._user_lists(SimpleNamespace(), "1", "1") == []


@pytest.mark.parametrize("data", [{"lists": "bad"}, {"lists": None, "pagination": {"total_pages": 2}}, {"lists": [], "pagination": {"total_items": 1}}])
def test_simkl_invalid_or_incomplete_custom_lists_raise(monkeypatch, data):
    from providers.sync.simkl import _playlists as module

    monkeypatch.setattr(module, "_custom_get", lambda *args, **kwargs: data)
    with pytest.raises(module.SIMKLFetchError):
        module._user_lists(SimpleNamespace(), "1", "1")


@pytest.mark.parametrize("rows", [None, []])
def test_simkl_empty_later_custom_list_page_raises(monkeypatch, rows):
    from providers.sync.simkl import _playlists as module

    monkeypatch.setattr(module, "_custom_get", lambda *args, **kwargs: {
        "lists": [{"id": 1}] if kwargs["page"] == 1 else rows, "pagination": {"total_pages": 2}})
    monkeypatch.setattr(module, "_custom_resource", lambda adapter, row, **kwargs: row)
    with pytest.raises(module.SIMKLFetchError):
        module._user_lists(SimpleNamespace(), "1", "1")


def test_wetrakr_playlist_empty_intermediate_page_raises(monkeypatch):
    from providers.sync.wetrakr import _playlists as module

    monkeypatch.setattr(module, "request", lambda *args, **kwargs: response(
        [{"id": 1}] if kwargs["params"]["page"] == 1 else [], {"X-Pagination-Page-Count": 3}))
    with pytest.raises(module.WeTrakrSyncError, match="incomplete_page"):
        module._paged(SimpleNamespace(), "/lists")


@pytest.mark.parametrize("rows,next_offset", [([], 1), ([{"id": 1}], "bad"), ([{"id": 1}], 0)])
def test_punchplay_watchlist_rejects_invalid_continuation(monkeypatch, rows, next_offset):
    from providers.sync.punchplay import _watchlist as module

    monkeypatch.setattr(module, "punchplay_request", lambda *args, **kwargs: response({"items": rows, "nextOffset": next_offset}))
    with pytest.raises(module.PunchPlayError, match="invalid_continuation"):
        module._index_dynamic_items(SimpleNamespace(), "1", {})


def test_page_guard_rejects_nonconsecutive_reordered_pages():
    guard = PageGuard(ValueError, "repeated")
    guard.check([{"id": 1}, {"id": 2}])
    guard.check([{"id": 3}])
    with pytest.raises(ValueError, match="repeated"):
        guard.check([{"id": 2}, {"id": 1}])


def test_pagination_can_be_cancelled_between_requests(monkeypatch):
    from providers.sync import _pagination
    from cw_platform.run_control import SyncCancelled

    calls = []

    def cancel():
        calls.append(True)
        if len(calls) == 2:
            raise SyncCancelled()

    monkeypatch.setattr(_pagination, "raise_if_cancelled", cancel)
    pages = page_numbers()
    assert next(pages) == 1
    with pytest.raises(SyncCancelled):
        next(pages)


@pytest.mark.parametrize("kind", ["history", "library", "progress"])
def test_nuvio_complete_reads_past_1000_pages(monkeypatch, kind):
    from providers.sync.nuvio import _common as module

    monkeypatch.setattr(module, "selected_profile_id", lambda adapter: 1)
    calls = []

    def rpc(adapter, endpoint, params):
        number = len(calls) + 1
        calls.append(params)
        if number > 1001:
            return []
        return [{"id": number, "last_watched": 1700000000000 + number}]

    monkeypatch.setattr(module, "rpc", rpc)
    reader = {"history": module.pull_watched_rows, "library": module.pull_library_rows, "progress": module.pull_watch_progress_rows}[kind]
    kwargs = {"page_size" if kind == "history" else "limit": 1}
    assert len(reader(SimpleNamespace(), **kwargs)) == 1001
    assert len(calls) == 1002
    calls.clear()
    assert len(reader(SimpleNamespace(), probe=True, **kwargs)) == 1
    assert len(calls) == 1


@pytest.mark.parametrize("kind", ["history", "library", "progress"])
def test_nuvio_repeated_pages_raise(monkeypatch, kind):
    from providers.sync.nuvio import _common as module

    monkeypatch.setattr(module, "selected_profile_id", lambda adapter: 1)
    monkeypatch.setattr(module, "rpc", lambda *args: [{"id": 1, "last_watched": 1700000000000}])
    reader = {"history": module.pull_watched_rows, "library": module.pull_library_rows, "progress": module.pull_watch_progress_rows}[kind]
    with pytest.raises(module.NuvioInvalidResponse):
        reader(SimpleNamespace(), **{"page_size" if kind == "history" else "limit": 1})


@pytest.mark.parametrize("provider,total", [("silo", 5001), ("tofa", 2001)])
def test_cursor_providers_read_beyond_old_guards(monkeypatch, provider, total):
    module = importlib.import_module(f"providers.sync.{provider}._common")
    calls = []

    def request(*args, **kwargs):
        number = len(calls) + 1
        calls.append(kwargs["params"])
        cursor = str(number) if number < total else None
        return response({"items": [{"id": number}], "next_after": cursor,
                         "page": {"has_more": bool(cursor), "next_cursor": cursor}})

    monkeypatch.setattr(module, f"{provider}_request", request)
    reader = module.pages if provider == "silo" else module.cursor_pages
    assert len(reader(SimpleNamespace(), "/items", feature="history")) == total
    assert len(calls) == total


def test_publicmetadb_reads_beyond_1000_pages():
    from providers.sync.publicmetadb._common import paged_items

    calls = []

    def read(path, params):
        calls.append(params["page"])
        return {"items": [{"id": params["page"]}], "total": 1001, "totalPages": 1001}

    adapter = SimpleNamespace(client=SimpleNamespace(read_json=read))
    assert len(paged_items(adapter, "/items", per_page=1, what="history")) == 1001
    assert calls == list(range(1, 1002))


def test_wetrakr_reads_beyond_10000_pages(monkeypatch):
    from providers.sync.wetrakr import _common as module

    def request(*args, **kwargs):
        page = kwargs["params"]["page"]
        return response([{"id": page}], {"X-Pagination-Page": page,
                        "X-Pagination-Page-Count": 10001, "X-Pagination-Item-Count": 10001})

    monkeypatch.setattr(module, "request", request)
    assert len(module.pages(SimpleNamespace(), "/items")) == 10001


def test_wetrakr_playlists_read_beyond_100_pages(monkeypatch):
    from providers.sync.wetrakr import _playlists as module

    monkeypatch.setattr(module, "request", lambda *args, **kwargs: response(
        [{"id": kwargs["params"]["page"]}], {"X-Pagination-Page-Count": 101}))
    assert len(module._paged(SimpleNamespace(), "/lists")) == 101


def test_simkl_list_discovery_reads_beyond_20_pages(monkeypatch):
    from providers.sync.simkl import _playlists as module

    monkeypatch.setattr(module, "_custom_get", lambda *args, **kwargs: {
        "lists": [{"id": kwargs["page"]}], "pagination": {"total_pages": 21}})
    monkeypatch.setattr(module, "_custom_resource", lambda adapter, row, **kwargs: row)
    assert len(module._user_lists(SimpleNamespace(), "1", "1")) == 21


def test_mdblist_liked_lists_read_beyond_20_pages(monkeypatch):
    from providers.sync.mdblist import _playlists as module

    monkeypatch.setattr(module, "_apikey", lambda adapter: "test")
    monkeypatch.setattr(module, "_resource_from_list", lambda adapter, row, **kwargs: SimpleNamespace(id=row["id"]))

    def request(*args, **kwargs):
        page = kwargs["params"]["offset"] // module._LIKED_PAGE + 1
        return response({"lists": [{"id": str(page)}], "pagination": {"has_more": page < 21}})

    monkeypatch.setattr(module, "mdblist_request", request)
    assert len(module._liked_resources(SimpleNamespace(), set())) == 21


def test_punchplay_snapshot_reads_beyond_10000_pages(monkeypatch):
    from providers.sync.punchplay import _common as module

    def request(*args, **kwargs):
        page = kwargs["params"]["after"] + 1
        return response({"items": [{"id": page}], "hasMore": page < 10001, "nextAfter": page})

    monkeypatch.setattr(module, "punchplay_request", request)
    assert sum(len(page) for page in module.snapshot_pages(SimpleNamespace(), "history", feature="history")) == 10001


def test_scrob_episode_ratings_lookup_has_no_500_item_cap(monkeypatch):
    from providers.sync.scrob import _ratings as module

    calls = []

    def request(adapter, method, path):
        calls.append(path)
        return response({"show_tmdb_id": 1, "season_number": 1, "episode_number": len(calls)})

    monkeypatch.setattr(module, "scrob_request", request)
    cache = {}
    for number in range(1, 502):
        assert module._episode_context(SimpleNamespace(), number, cache)["episode_number"] == number
    assert len(calls) == 501
    module._episode_context(SimpleNamespace(), 1, cache)
    assert len(calls) == 501


def test_obsolete_page_caps_are_removed_from_defaults_and_instances():
    from cw_platform.config_base import DEFAULT_CFG, cleanup_obsolete_config_keys

    cfg = {provider: {"history_max_pages": 1, "instances": {"test": {"ratings_max_pages": 2}}}
           for provider in ("trakt", "mdblist", "publicmetadb", "flicklist", "punchplay", "scrob")}
    cfg["tracearr"] = {"history": {"max_pages": 1}, "instances": {"test": {"history": {"max_pages": 2}}}}
    cfg["tautulli"] = {"history": {"max_pages": 1}}
    changed = cleanup_obsolete_config_keys(cfg)
    assert len(changed) == 15
    assert "max_pages" not in json.dumps(cfg)
    assert "max_pages" not in json.dumps(DEFAULT_CFG)


@pytest.mark.parametrize("incomplete", [False, True])
def test_publicmetadb_playback_reads_all_pages_or_reports_failure(monkeypatch, incomplete, active_sync_cancel):
    from services.playback_progress.adapters import publicmetadb as module

    calls = []

    def get(path, params):
        page = params["page"]
        calls.append(page)
        rows = [] if incomplete and page == 1001 else [{"id": page}]
        return response({"items": rows, "totalPages": 1001, "total": 1001})

    client = SimpleNamespace(get=get, safe_json=lambda resp: resp.json())
    monkeypatch.setattr(module, "PUBLICMETADBModule", lambda cfg: SimpleNamespace(cfg=SimpleNamespace(progress_max_pages=1), client=client))
    monkeypatch.setattr(module, "tmdb_metadata_provider", lambda cfg: None)
    monkeypatch.setattr(module, "enrich_parallel", lambda rows, normalize: rows)
    adapter = module.PublicMetaDBPlaybackAdapter()
    result = adapter.list_progress({}, instance_id="default", instance_label="Test")
    assert result.ok is not incomplete
    assert len(result.items) == (0 if incomplete else 1001)
    assert calls == list(range(1, 1002))


def test_punchplay_dynamic_list_reads_beyond_500_pages(monkeypatch):
    from providers.sync.punchplay import _playlists as module

    def request(*args, **kwargs):
        offset = kwargs["params"]["offset"]
        return response({"items": [{"id": offset}], "total": 501,
                         "nextOffset": offset + 1 if offset < 500 else None})

    monkeypatch.setattr(module, "punchplay_request", request)
    assert len(module._dynamic_rows(SimpleNamespace(), "1")) == 501


@pytest.mark.parametrize("cyclic", [False, True])
def test_tofa_feed_reads_past_2000_pages_and_rejects_cycles(monkeypatch, cyclic):
    from providers.sync.tofa import _common as module

    calls = []

    def request(*args, **kwargs):
        number = len(calls) + 1
        calls.append(kwargs["params"]["cursor"])
        return response({"changes": [{"id": number}], "has_more": number < 2001,
                         "next_cursor": "0" if cyclic and number == 3 else str(number)})

    monkeypatch.setattr(module, "tofa_request", request)
    monkeypatch.setattr(module, "valid_change", lambda row: True)
    if cyclic:
        with pytest.raises(module.TofaSyncError, match="invalid_continuation"):
            module.feed_changes(SimpleNamespace(), "0", feature="history")
        assert len(calls) == 3
    else:
        rows, cursor, reset = module.feed_changes(SimpleNamespace(), "0", feature="history")
        assert len(rows) == 2001 and cursor == "2001" and reset is False


def test_flicklist_history_continues_past_500_pages(monkeypatch):
    from providers.sync.flicklist import _history as module

    def request(*args, **kwargs):
        page = kwargs["params"]["page"]
        return response([{"id": page}], {"X-FlickList-Page-Count": 501, "X-FlickList-Item-Count": 501})

    monkeypatch.setattr(module, "flicklist_request", request)
    monkeypatch.setattr(module, "_row_to_minimal", lambda row: {"type": "movie", "ids": {"tmdb": str(row["id"])}})
    monkeypatch.setattr(module, "_supplement_watched_shows", lambda *args: (0, 0))
    result = module.build_index(SimpleNamespace(config={"flicklist": {"history_max_pages": 1}}))
    assert len(result) == 501


def test_plex_child_lookup_reads_past_100000_rows(monkeypatch):
    from providers.sync.plex import _discover_history as module

    writer = object.__new__(module.Writer)
    writer.cache = {}
    calls = []

    def metadata(path, *, params, headers):
        start = int(headers["X-Plex-Container-Start"])
        calls.append(start)
        return {"totalSize": 100001, "Metadata": [
            {"ratingKey": str(i), "type": "episode", "index": i}
            for i in range(start, min(start + 100, 100001))]}

    monkeypatch.setattr(writer, "_metadata", metadata)
    assert writer._child("show", "episode", 100000) == "100000"
    assert calls[-1] == 100000


def test_silo_manual_marks_stop_after_four_pages(monkeypatch, active_sync_cancel):
    from providers.scrobble.silo import watch as module

    calls = []

    def request(*args, **kwargs):
        number = len(calls) + 1
        calls.append(kwargs["params"])
        return response({"items": [{"media_item_id": str(number), "updated_at": "2026-01-01T00:00:00Z"}],
                         "page": {"next_cursor": str(number) if number < 5 else None}})

    service = SimpleNamespace(_active_cfg=lambda: {}, session=None, _stop=SimpleNamespace(is_set=lambda: False))
    monkeypatch.setattr(module, "silo_request", request)
    assert len(module.SiloWatchService._completed_rows(service, "default", 1.0)) == 4
    assert len(calls) == 4


def test_floppy_season_probe_continues_beyond_60(monkeypatch):
    from providers.sync.floppy import _common as module

    monkeypatch.setattr(module, "_LAYOUT_CACHE", {})
    monkeypatch.setattr(module, "show_seasons", lambda *args: set())
    monkeypatch.setattr(module, "_season_episodes", lambda adapter, show, season: [1] if season <= 61 else [])
    assert module.show_layout(SimpleNamespace(), "1") == [(season, 1) for season in range(1, 62)]


def test_plex_episode_fallback_reads_past_5000_rows(monkeypatch):
    from providers.sync.plex import _common as module

    entry = {"api": None, "children": None, "leaves": None}
    monkeypatch.setattr(module, "_show_episode_cache_entry", lambda show: entry)
    calls = []

    def get(url, *, params, timeout):
        start = params["X-Plex-Container-Start"]
        calls.append(start)
        rows = ''.join(f'<Video ratingKey="{number}" parentIndex="1" index="{number}" />'
                       for number in range(start + 1, min(start + 5001, 5002)))
        return SimpleNamespace(ok=True, text=f'<MediaContainer totalSize="5001">{rows}</MediaContainer>')

    server = SimpleNamespace(url=lambda path: path, _session=SimpleNamespace(get=get))
    show = SimpleNamespace(episodes=lambda: [], _server=server, ratingKey="show")
    assert module.episode_rating_key_from_show(show, 1, 5001) == "5001"
    assert calls == [0, 5000]


@pytest.mark.parametrize("failed", [False, True])
def test_jellyfin_episode_fallback_reads_past_10000_and_never_caches_partial(monkeypatch, failed):
    from providers.sync.jellyfin import _common as module

    calls = []
    series = {"Id": "series", "Type": "Series", "ProviderIds": {"Tmdb": "1"}}
    monkeypatch.setattr(module, "build_provider_index", lambda *args, **kwargs: {"tmdb.1": [series]})

    def get(path, *, params):
        assert path == "/Shows/series/Episodes"
        assert params["EnableTotalRecordCount"] is True
        start = params["StartIndex"]
        calls.append(start)
        if failed and start == 10000:
            return SimpleNamespace(status_code=503)
        rows = [{"Id": f"episode-{i}", "Type": "Episode", "ParentIndexNumber": 1, "IndexNumber": i + 1}
                for i in range(start, min(start + 100, 10001))]
        return response({"Items": rows, "TotalRecordCount": 10001})

    adapter = SimpleNamespace(client=SimpleNamespace(get=get), config={},
                              cfg=SimpleNamespace(user_id="user", targeted_lookup=False, strict_id_matching=False))
    item = {"type": "episode", "show_ids": {"tmdb": "1"}, "season": 1, "episode": 10001}
    if failed:
        with pytest.raises(RuntimeError, match="http_503"):
            module.resolve_item_id(adapter, item)
        assert "series" not in adapter._jellyfin_series_episodes_cache
    else:
        assert module.resolve_item_id(adapter, item) == "episode-10000"
        assert len(adapter._jellyfin_series_episodes_cache["series"]) == 10001
    assert calls == list(range(0, 10001, 100))
