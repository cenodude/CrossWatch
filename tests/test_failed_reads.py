# CrossWatch test scripts
from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from cw_platform.orchestrator import _snapshots


class Resp:
    def __init__(self, status: int = 200, body: Any = None, *, text: str | None = None, ctype: str = "application/json"):
        self.status_code = status
        self.ok = 200 <= status < 300
        self.headers = {"Content-Type": ctype, "content-type": ctype}
        self.text = text if text is not None else ("" if body is None else json.dumps(body))
        self.content = self.text.encode("utf-8")

    def json(self) -> Any:
        return json.loads(self.text)


def _sequence(responses: list[Resp]):
    calls: list[dict[str, Any]] = []

    def request(_adapter: Any, method: str, url: str, **kwargs: Any) -> Resp:
        calls.append({"method": method, "url": url, **kwargs})
        return responses[min(len(calls) - 1, len(responses) - 1)]

    return request, calls


def test_require_snapshot_raises_for_missing_provider():
    with pytest.raises(RuntimeError, match="snapshot_unavailable:PLEX:history"):
        _snapshots.require_snapshot({"SIMKL": {}}, "PLEX", "history")
    assert _snapshots.require_snapshot({"PLEX": {}}, "PLEX", "history") == {}


def test_snapshot_rejects_invalid_provider_result(monkeypatch):
    ops = SimpleNamespace(features=lambda: {"history": True}, build_index=lambda *a, **k: None)
    monkeypatch.setattr(_snapshots, "provider_configured", lambda *a: True)
    monkeypatch.setattr(_snapshots, "allowed_providers_for_feature", lambda *a: set())
    callbacks: list[Any] = []
    with pytest.raises(RuntimeError, match="snapshot_invalid_result:FAKE:history:NoneType"):
        _snapshots.build_snapshots_for_feature(feature="history", config={}, providers={"FAKE": ops},
            snap_cache={}, snap_ttl_sec=300, dbg=lambda *a, **k: None, emit_info=lambda *a: None,
            on_snapshot=lambda *args: callbacks.append(args))
    assert callbacks == []


@pytest.mark.parametrize("responses,reason", [
    ([Resp(503, {"error": "down"})], "snapshot_http_503"),
    ([Resp(200, text="<html>proxy</html>", ctype="text/html")], "snapshot_invalid_response"),
    ([Resp(200, {"items": [{"id": 1}], "hasMore": True, "nextAfter": 5}), Resp(500, {})], "snapshot_http_500"),
    ([Resp(200, {"items": [{"id": 1}], "hasMore": True, "nextAfter": 0})], "snapshot_invalid_continuation"),
])
def test_punchplay_snapshot_pages_fail_instead_of_ending(monkeypatch, responses, reason):
    from providers.sync.punchplay import _common

    request, _calls = _sequence(responses)
    monkeypatch.setattr(_common, "punchplay_request", request)
    with pytest.raises(_common.PunchPlayError, match=reason):
        list(_common.snapshot_pages(SimpleNamespace(), "history", feature="history"))


def test_punchplay_snapshot_pages_valid_empty_still_succeeds(monkeypatch):
    from providers.sync.punchplay import _common

    request, _calls = _sequence([Resp(200, {"items": [], "hasMore": False})])
    monkeypatch.setattr(_common, "punchplay_request", request)
    assert list(_common.snapshot_pages(SimpleNamespace(), "history", feature="history")) == []


@pytest.mark.parametrize("responses,reason", [
    ([Resp(502, {"detail": "bad gateway"})], "scrob_history_http_502"),
    ([Resp(200, {"results": [{"id": 1}], "total_pages": 2}), Resp(500, {})], "scrob_history_http_500"),
    ([Resp(200, text="<html></html>", ctype="text/html")], "scrob_history_invalid_response"),
    ([Resp(200, {"results": [{"id": n}], "total_pages": 9}) for n in (1, 2, 3)], "scrob_history_page_cap_reached:2"),
])
def test_scrob_paged_get_fails_instead_of_returning_partial_rows(monkeypatch, responses, reason):
    from providers.sync.scrob import _common

    request, _calls = _sequence(responses)
    monkeypatch.setattr(_common, "scrob_request", request)
    with pytest.raises(_common.ScrobError, match=reason):
        _common.paged_get(SimpleNamespace(), "history", feature="history", page_size=10, max_pages=2)


@pytest.mark.parametrize("feature", ["watchlist", "ratings", "progress"])
def test_flicklist_index_http_failure_raises(monkeypatch, feature):
    import importlib

    module = importlib.import_module(f"providers.sync.flicklist._{feature}")
    request, _calls = _sequence([Resp(500, {"error": "boom"})])
    monkeypatch.setattr(module, "flicklist_request", request)
    with pytest.raises(RuntimeError, match=f"flicklist_{feature}_http_500"):
        module.build_index(SimpleNamespace(config={}))


def test_flicklist_history_page_cap_raises(monkeypatch):
    from providers.sync.flicklist import _history

    request, _calls = _sequence([Resp(200, [{"id": 1}])])
    monkeypatch.setattr(_history, "flicklist_request", request)
    monkeypatch.setattr(_history, "cfg_section", lambda adapter: {"history_max_pages": 2})
    monkeypatch.setattr(_history, "_response_page_count", lambda resp, data, page: 99)
    with pytest.raises(RuntimeError, match="flicklist_history_page_cap_reached:2"):
        _history.build_index(SimpleNamespace(config={}))


def test_publicmetadb_read_json_rejects_errors_and_unreadable_bodies():
    from providers.sync._mod_PUBLICMETADB import PUBLICMETADBClient, PUBLICMETADBError

    client = object.__new__(PUBLICMETADBClient)
    for resp, reason in ((Resp(500, {"error": "x"}), "publicmetadb_read_http_500"),
                         (Resp(200, text="<html></html>", ctype="text/html"), "publicmetadb_read_invalid_response")):
        client.get = lambda path, _resp=resp, **kw: _resp
        with pytest.raises(PUBLICMETADBError, match=reason):
            client.read_json("/api/external/watched", params={"page": 1})
    client.get = lambda path, **kw: Resp(200, {"items": []})
    assert client.read_json("/api/external/watched") == {"items": []}


def test_tautulli_call_rejects_missing_envelope(monkeypatch):
    from providers.sync import _mod_TAUTULLI as mod

    monkeypatch.setattr(mod, "request_with_retries", lambda *a, **k: Resp(200, text="<html></html>", ctype="text/html"))
    client = mod.TAUTULLIModule({"tautulli": {"server_url": "http://t:8181", "api_key": "k"}}).client
    with pytest.raises(RuntimeError, match="invalid_response"):
        client.call("get_history")


def test_tracearr_get_rejects_unreadable_body(monkeypatch):
    from providers.sync import _mod_TRACEARR as mod

    monkeypatch.setattr(mod._LIMITER, "wait", lambda key: 0.0)
    monkeypatch.setattr(mod, "request_with_retries", lambda *a, **k: Resp(200, text="<html></html>", ctype="text/html"))
    client = mod.TRACEARRModule({"tracearr": {"server_url": "http://t:3000", "api_key": "k"}}).client
    with pytest.raises(RuntimeError, match="invalid_response"):
        client.get("/history")


def test_plex_xml_without_media_container_is_not_an_empty_container():
    from providers.sync.plex import _common

    with pytest.raises(ValueError, match="plex_xml_missing_media_container"):
        _common._xml_to_container("<html><body>proxy error</body></html>")
    assert _common._xml_to_container('<MediaContainer size="0"></MediaContainer>')["MediaContainer"]["size"] == 0


@pytest.mark.parametrize("feature", ["history", "progress", "collection"])
def test_plex_unbound_server_raises(feature):
    import importlib

    module = importlib.import_module(f"providers.sync.plex._{feature}")
    adapter = SimpleNamespace(client=SimpleNamespace(server=None), cfg=SimpleNamespace(), config={})
    with pytest.raises(RuntimeError, match="PLEX server not bound"):
        module.build_index(adapter)


def _pmdb_adapter(pages: dict[int, Any]):
    def read_json(path, *, params):
        return pages[params["page"]]

    return SimpleNamespace(client=SimpleNamespace(read_json=read_json))


@pytest.mark.parametrize("pages,reason", [
    ({1: {"items": [{"id": "a"}], "total": 2, "totalPages": 2}, 2: {"items": [], "total": 2, "totalPages": 2}}, "incomplete_read:page 2"),
    ({1: {"items": [], "total": 3, "totalPages": 1}}, "incomplete_read:page 1"),
    ({1: {"items": [{"id": "a"}], "total": 2, "totalPages": 1}}, "incomplete_read:1/2"),
    ({1: {"items": [{"id": "a"}], "total": 2, "totalPages": 2}, 2: {"items": [{"id": "a"}], "total": 2, "totalPages": 2}}, "repeated_page"),
    ({1: {"items": [{"id": "a"}], "total": 2, "totalPages": 2}, 2: {"items": [{"id": "b"}], "total": 3, "totalPages": 2}}, "total_changed:2->3"),
    ({1: {"total": 0}}, "invalid_response"),
])
def test_publicmetadb_pager_rejects_incomplete_reads(pages, reason):
    from providers.sync.publicmetadb._common import paged_items

    with pytest.raises(RuntimeError, match=f"publicmetadb_history_{reason}"):
        paged_items(_pmdb_adapter(pages), "/api/external/watched", per_page=1, max_pages=10, what="history")


def test_publicmetadb_pager_accepts_empty_library_and_full_reads():
    from providers.sync.publicmetadb._common import paged_items

    assert paged_items(_pmdb_adapter({1: {"items": [], "total": 0, "totalPages": 0}}), "/x", per_page=5, max_pages=5, what="history") == []
    pages = {1: {"items": [{"id": "a"}], "total": 2, "totalPages": 2}, 2: {"items": [{"id": "b"}], "total": 2, "totalPages": 2}}
    assert [row["id"] for row in paged_items(_pmdb_adapter(pages), "/x", per_page=1, max_pages=5, what="history")] == ["a", "b"]


def test_flicklist_history_rejects_empty_advertised_last_page(monkeypatch):
    from providers.sync.flicklist import _history

    def page(rows: list[Any]) -> Resp:
        resp = Resp(200, rows)
        resp.headers.update({"X-FlickList-Page-Count": "2", "X-FlickList-Item-Count": "2"})
        return resp

    request, _calls = _sequence([page([{"id": 1}]), page([])])
    monkeypatch.setattr(_history, "flicklist_request", request)
    with pytest.raises(RuntimeError, match="flicklist_history_incomplete_read:page 2"):
        _history.build_index(SimpleNamespace(config={}))


@pytest.mark.parametrize("responses,reason", [
    ([Resp(200, {"results": [{"id": 1}], "total_pages": 2, "total_results": 2}), Resp(200, {})], "scrob_history_invalid_response"),
    ([Resp(200, {"results": [{"id": 1}], "total_pages": 2, "total_results": 2}), Resp(200, {"results": [], "total_pages": 2, "total_results": 2})], "scrob_history_incomplete_read"),
    ([Resp(200, {"results": [], "total_pages": 1, "total_results": 4})], "scrob_history_incomplete_read"),
])
def test_scrob_paged_get_rejects_malformed_later_pages(monkeypatch, responses, reason):
    from providers.sync.scrob import _common

    request, _calls = _sequence(responses)
    monkeypatch.setattr(_common, "scrob_request", request)
    with pytest.raises(_common.ScrobError, match=reason):
        _common.paged_get(SimpleNamespace(), "history", feature="history", page_size=1, max_pages=5)


def test_scrob_export_and_episode_lookup_reject_unreadable_bodies(monkeypatch):
    from providers.sync.scrob import _collection, _ratings
    from providers.sync.scrob._common import ScrobError

    with pytest.raises(ScrobError, match="scrob_collection_export_empty"):
        _collection._export_payload(Resp(200, text="", ctype="application/json"))
    with pytest.raises(ScrobError, match="scrob_collection_export_unreadable"):
        _collection._export_payload(Resp(200, {"detail": "maintenance"}))
    request, _calls = _sequence([Resp(200, ["not", "an", "object"])])
    monkeypatch.setattr(_ratings, "scrob_request", request)
    with pytest.raises(ScrobError, match="scrob_ratings_episode_lookup_invalid_response"):
        _ratings._episode_context(SimpleNamespace(), 123, {})


def test_plex_section_read_keeps_the_advertised_total():
    from providers.sync.plex import _common

    pages = [
        {"MediaContainer": {"totalSize": 3, "size": 2, "Metadata": [{"ratingKey": "1"}, {"ratingKey": "2"}]}},
        {"MediaContainer": {"size": 0, "Metadata": []}},
    ]

    def get(url, *, params, headers, timeout):
        return Resp(200, pages[0] if int(params["X-Plex-Container-Start"]) == 0 else pages[1])

    srv = SimpleNamespace(_baseurl="http://plex:32400", _session=SimpleNamespace(headers={}, get=get), token="t")
    with pytest.raises(RuntimeError, match="plex_guid_index_incomplete_page"):
        _common._fetch_section_guid_rows(srv, "1", 1, strict=True)


def test_emby_item_lookup_failure_is_not_a_missing_item():
    from providers.sync.emby import _history

    _history._item_meta_cache.clear()
    with pytest.raises(RuntimeError, match="emby_item_lookup_http_500"):
        _history._item_meta(SimpleNamespace(get=lambda path, params=None: Resp(500, {})), "x1")
    assert _history._item_meta(SimpleNamespace(get=lambda path, params=None: Resp(404, {})), "x2") is None


@pytest.mark.parametrize("result,ok", [
    ({"limits": {"total": 0}}, True),
    ({"limits": {"total": 2}}, False),
    ({"movies": [], "limits": {"total": 3}}, False),
    ({"movies": [{"movieid": 1}], "limits": {"total": 1}}, True),
    ({}, False),
])
def test_kodi_library_rows_match_the_reported_total(result, ok):
    from providers.auth._auth_KODI import KodiAuthError
    from providers.sync.kodi._common import library_result_rows

    if ok:
        assert isinstance(library_result_rows(result, "movies"), list)
    else:
        with pytest.raises(KodiAuthError):
            library_result_rows(result, "movies")


def test_kodi_batch_missing_response_raises(monkeypatch):
    from providers.auth import _auth_KODI as kodi

    reply = [{"jsonrpc": "2.0", "id": "0:VideoLibrary.GetMovies", "result": {"limits": {"total": 0}}}]
    monkeypatch.setattr(kodi.requests, "post", lambda *a, **k: Resp(200, reply))
    with pytest.raises(kodi.KodiAuthError, match="missing"):
        kodi.jsonrpc_batch_call("http://kodi:8080", [("VideoLibrary.GetMovies", {}), ("VideoLibrary.GetEpisodes", {})])


@pytest.mark.parametrize("pages,reason", [
    ({0: {"results": [{"id": 1}, {"id": 2}]}}, "floppy_missing_total"),
    ({0: {"results": [{"id": 1}, {"id": 2}], "pagination": {"total": 9}}}, "floppy_page_cap_reached"),
])
def test_floppy_paged_fails_on_caps_and_missing_totals(monkeypatch, pages, reason):
    from providers.sync.floppy import _common

    monkeypatch.setattr(_common, "PAGE_SIZE", 2)
    monkeypatch.setattr(_common, "api_get", lambda adapter, path, params=None: pages[int(params["offset"])])
    with pytest.raises(RuntimeError, match=reason):
        _common.paged(SimpleNamespace(), "media/movie", max_pages=1)


@pytest.mark.parametrize("prov", ["jellyfin", "emby"])
def test_playlist_pages_use_entry_identity(prov):
    import importlib

    common = importlib.import_module(f"providers.sync.{prov}._common")
    pages = {
        0: {"Items": [{"Id": "m1", "PlaylistItemId": "e1"}, {"Id": "m2", "PlaylistItemId": "e2"}]},
        2: {"Items": [{"Id": "m1", "PlaylistItemId": "e3"}]},
    }

    def get(path, params):
        return Resp(200, pages.get(int(params.get("StartIndex", 0)), {"Items": []}))

    rows = common.fetch_item_pages(SimpleNamespace(get=get), "/Playlists/p/Items", {}, reason="x", page_size=2, require_total=False, id_key="PlaylistItemId")
    assert [row["PlaylistItemId"] for row in rows] == ["e1", "e2", "e3"]


def test_mdblist_progress_rejects_empty_body(monkeypatch):
    from providers.sync.mdblist import _progress

    monkeypatch.setattr(_progress, "_request", lambda *a, **k: Resp(200, text=""))
    with pytest.raises(RuntimeError, match="mdblist_progress_empty_response"):
        _progress.build_index(SimpleNamespace(cfg=SimpleNamespace(api_key="k")))


@pytest.mark.parametrize("prov", ["jellyfin", "emby"])
@pytest.mark.parametrize("later", [False, True])
@pytest.mark.parametrize("bad_page", [{}, {"Items": [], "TotalRecordCount": 501}])
def test_provider_index_rejects_invalid_or_incomplete_pages(monkeypatch, prov, later, bad_page):
    import importlib

    common = importlib.import_module(f"providers.sync.{prov}._common")
    rows = [{"Id": str(i), "ProviderIds": {"Tmdb": str(i + 1)}} for i in range(500)]
    def get(path, *, params):
        if later and params["StartIndex"] == 0:
            return Resp(200, {"Items": rows, "TotalRecordCount": 501})
        return Resp(200, bad_page)
    adapter = SimpleNamespace(client=SimpleNamespace(get=get), cfg=SimpleNamespace(user_id="u"))
    with pytest.raises(RuntimeError, match=f"{prov}_provider_index_(invalid_page|incomplete_read)"):
        common.build_provider_index(adapter)
    assert getattr(adapter, "_provider_index_cache", None) is None


@pytest.mark.parametrize("prov", ["jellyfin", "emby"])
@pytest.mark.parametrize("id_key", ["Id", "PlaylistItemId"])
def test_media_server_pager_rejects_duplicate_entries_within_page(prov, id_key):
    import importlib

    common = importlib.import_module(f"providers.sync.{prov}._common")
    row = {"Id": "m1", "PlaylistItemId": "e1"}
    http = SimpleNamespace(get=lambda *a, **k: Resp(200, {"Items": [row, row], "TotalRecordCount": 2}))
    with pytest.raises(RuntimeError, match="duplicate_item"):
        common.fetch_item_pages(http, "/Items", {}, reason="test", id_key=id_key)


@pytest.mark.parametrize("member", [{"detail": "maintenance"}, 7, None])
def test_scrob_zip_rejects_invalid_collection_member(member):
    import io
    import zipfile
    from providers.sync.scrob import _collection

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("collection-movies.json", json.dumps([{"id": 1}]))
        archive.writestr("collection-shows.json", json.dumps(member))
    with pytest.raises(RuntimeError, match="export_member_unreadable"):
        _collection._export_payload(SimpleNamespace(content=buffer.getvalue()))


def test_scrob_zip_accepts_explicit_empty_collection():
    import io
    import zipfile
    from providers.sync.scrob import _collection

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("collection-movies.json", "[]")
    assert _collection._export_payload(SimpleNamespace(content=buffer.getvalue())) == []


@pytest.mark.parametrize("error", [RuntimeError("floppy_missing_total"), TimeoutError("timed out")])
def test_floppy_history_builder_propagates_per_movie_read_failure(monkeypatch, error):
    from providers.sync.floppy import _history

    def paged(adapter, path, **kwargs):
        if path == "media/movie":
            return [{"item_id": "movie/tmdb/123", "media_type": "movie", "source": "tmdb",
                     "media_id": "123", "status": _history.COMPLETED, "end_date": "2026-10-01T12:00:00Z"}]
        raise error
    monkeypatch.setattr(_history, "paged", paged)
    monkeypatch.setattr(_history, "_rewatches_enabled", lambda a: True)
    with pytest.raises(type(error), match=str(error)):
        _history.build_index(SimpleNamespace())


@pytest.mark.parametrize("helper", ["episode_history", "season_numbers", "layout", "bucket"])
def test_floppy_secondary_read_failures_are_not_cached_as_absence(monkeypatch, helper):
    from providers.sync.floppy import _common, _history

    def fail(*a, **k):
        raise RuntimeError("floppy_missing_total")
    monkeypatch.setattr(_common, "paged", fail)
    monkeypatch.setattr(_history, "paged", fail)
    monkeypatch.setattr(_history, "show_bucket", fail)
    monkeypatch.setattr(_history, "_SEASON_EPISODE_CACHE", {})
    monkeypatch.setattr(_common, "_LAYOUT_CACHE", {})
    calls = {"episode_history": lambda: _history._episode_history(SimpleNamespace(), "1", 1, 1),
             "season_numbers": lambda: _history._season_episode_numbers(SimpleNamespace(), "1", 1),
             "layout": lambda: _common.show_layout(SimpleNamespace(), "1"),
             "bucket": lambda: _history._existing_bucket(SimpleNamespace(), "1")}
    with pytest.raises(RuntimeError, match="floppy_missing_total"):
        calls[helper]()
    assert _history._SEASON_EPISODE_CACHE == {}
    assert _common._LAYOUT_CACHE == {}


@pytest.mark.parametrize("status", [404, 500])
@pytest.mark.parametrize("helper", ["movie", "episode", "season", "layout"])
def test_floppy_secondary_reads_only_allow_not_found(monkeypatch, status, helper):
    from providers.auth._auth_FLOPPY import FloppyAuthError
    from providers.sync.floppy import _common, _history

    def fail(*a, **k):
        raise FloppyAuthError("lookup failed", status_code=status)
    monkeypatch.setattr(_common, "paged", fail)
    monkeypatch.setattr(_history, "paged", fail)
    monkeypatch.setattr(_history, "_SEASON_EPISODE_CACHE", {})
    calls = {"movie": lambda: _history._movie_history(SimpleNamespace(), "1"),
             "episode": lambda: _history._episode_history(SimpleNamespace(), "1", 1, 1),
             "season": lambda: _history._season_episode_numbers(SimpleNamespace(), "1", 1),
             "layout": lambda: _common._season_episodes(SimpleNamespace(), "1", 1)}
    if status == 404:
        assert calls[helper]() == (None if helper == "season" else [])
    else:
        with pytest.raises(FloppyAuthError):
            calls[helper]()
