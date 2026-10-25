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
